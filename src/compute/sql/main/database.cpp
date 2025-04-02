/*
 * Copyright (c) GBA-NCTI-ISDC. 2022-2024.
 *
 * openGauss embedded is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITFOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * database.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/main/database.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "main/database.h"

#include <functional>
#include <iostream>
#include <stdexcept>

#include "common/memory/memory_manager.h"
#include "function/function.h"
#include "main/base_storage.h"
#include "storage/gstor/zekernel/common/cm_error.h"
#include "storage/gstor/zekernel/common/cm_file.h"

std::mutex IntarkDB::mutex_;
std::unordered_map<std::string, std::weak_ptr<BaseStorage>> IntarkDB::instance_map_ = {};

class IntarkDBInstance : public IntarkDB {
   public:
    IntarkDBInstance(std::shared_ptr<BaseStorage> storage, const std::string& path)
        : IntarkDB(path), storage_(storage) {}

    virtual ~IntarkDBInstance() {}

    virtual void DestoryHandle(void* handle);
    virtual status_t AllocHandle(void** handle);
    virtual bool IsDBAvailable();
    virtual int GetId(void* handle);

   private:
    std::shared_ptr<BaseStorage> storage_;
};

class IntarkDBWeakInstance : public IntarkDB {
   public:
    IntarkDBWeakInstance(std::weak_ptr<BaseStorage> storage, const std::string& path)
        : IntarkDB(path), storage_(storage) {}

    virtual ~IntarkDBWeakInstance() {}

    virtual void DestoryHandle(void* handle);
    virtual status_t AllocHandle(void** handle);
    virtual bool IsDBAvailable();
    virtual int GetId(void* handle);
    
   private:
    std::weak_ptr<BaseStorage> storage_;
};

IntarkDB::IntarkDB(const std::string& path) : path_(path) {
    full_dbpath_ = path + constant_db_name;
    intarkdb::FunctionContext::Init();
}

std::shared_ptr<BaseStorage> IntarkDB::GetStorage(const std::string& in_path) {
    auto iter = instance_map_.find(in_path);
    if (iter != instance_map_.end()) {
        auto instance = iter->second.lock();
        if (instance != nullptr) {
            return instance;
        }
    }

    // double check
    std::lock_guard<std::mutex> lock(mutex_);
    iter = instance_map_.find(in_path);
    if (iter != instance_map_.end()) {
        auto instance = iter->second.lock();
        if (instance != nullptr) {
            return instance;
        }
    }
    // 限制同时打开的数据库实例的数量   MAX_DB_INSTANCE_COUNT
    if (instance_map_.size() >= MAX_DB_INSTANCE_COUNT) {
        // try to find a weak_ptr that has expired
        for (auto it = instance_map_.begin(); it != instance_map_.end();) {
            if (it->second.expired()) {
                it = instance_map_.erase(it);
            } else {
                ++it;
            }
        }
        if (instance_map_.size() >= MAX_DB_INSTANCE_COUNT) {
            GS_LOG_RUN_ERR("No more than 32 databases can be opened simultaneously!");
            throw std::runtime_error("No more than 32 databases can be opened simultaneously!");
        }
    }
    auto instance = std::make_shared<BaseStorage>();
    instance->Open(const_cast<char*>(in_path.c_str()));
    instance_map_[in_path] = instance;
    intarkdb::MemoryManager::GetInstance(instance->get_sql_engine_memory_limit());
    return instance;
}

static bool IsAbsolutePath(const std::string& path) {
#ifdef __linux__
    return path.length() > 0 && path[0] == '/';
#else
    return true;  // windwos 平台暂都返回true
#endif
}

const char* DEFAULT_PATH = "./";
constexpr size_t DEFAULT_PATH_LEN = 2;

std::shared_ptr<IntarkDB> IntarkDB::GetWeakInstance(std::weak_ptr<BaseStorage> storage, const std::string& path) {
    return std::make_shared<IntarkDBWeakInstance>(storage, path);
}

std::shared_ptr<IntarkDB> IntarkDB::GetInstance(const char* path) {
    // get current path
    char current_path[1024] = {0};
    getcwd(current_path, sizeof(current_path));
    // path is aoosolute path or relative path
    std::string in_path = "";
    if (path == nullptr || strcmp(path, DEFAULT_PATH) == 0) {
        in_path = current_path;
    } else {
        if (IsAbsolutePath(path)) {
            in_path = path;
        } else {
            size_t len = strlen(path);
            if (len >= DEFAULT_PATH_LEN && strncmp(path, DEFAULT_PATH, DEFAULT_PATH_LEN) == 0) {
                in_path = current_path;
                in_path += "/";
                in_path += path + DEFAULT_PATH_LEN;
            } else {
                in_path = current_path;
                in_path += "/";
                in_path += path;
            }
        }
    }
    auto storage = GetStorage(in_path);
    if (storage == nullptr) {
        GS_LOG_RUN_ERR("Get database instance err!");
        throw std::runtime_error("Get database instance err!");
    }
    return std::make_shared<IntarkDBInstance>(storage, in_path);
}

void IntarkDBInstance::DestoryHandle(void* handle) { storage_->db_handle_free(handle); }

status_t IntarkDBInstance::AllocHandle(void** handle) { return storage_->db_handle_alloc(handle); }

bool IntarkDBInstance::IsDBAvailable() { return storage_->db_is_available(); }

int IntarkDBInstance::GetId(void* handle) { return storage_->get_handle_id(handle); }

void IntarkDBWeakInstance::DestoryHandle(void* handle) {
    auto storage = storage_.lock();
    if (storage == nullptr) {
        throw intarkdb::IntarkDBInValidException("base storage is invalid");
    }
    storage->db_handle_free(handle);
}

status_t IntarkDBWeakInstance::AllocHandle(void** handle) {
    auto storage = storage_.lock();
    if (storage == nullptr) {
        throw intarkdb::IntarkDBInValidException("base storage is invalid");
    }
    return storage->db_handle_alloc(handle);
}

bool IntarkDBWeakInstance::IsDBAvailable() {
    auto storage = storage_.lock();
    if (storage == nullptr) {
        return false;
    }
    return storage->db_is_available();
}

int IntarkDBWeakInstance::GetId(void* handle) {
    auto storage = storage_.lock();
    if (storage == nullptr) {
        throw intarkdb::IntarkDBInValidException("base storage is invalid");
    }
    return storage->get_handle_id(handle);
}
