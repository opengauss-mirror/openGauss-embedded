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
 * database.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/main/database.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include "common/winapi.h"
#include "storage/gstor/zekernel/common/cm_defs.h"
#include "storage/gstor/zekernel/common/cm_spinlock.h"

const uint32_t MAX_DB_INSTANCE_COUNT = 32;
const std::string constant_db_name = "intarkdb";

class BaseStorage;

namespace intarkdb {
class IntarkDBInValidException : public std::runtime_error {
   public:
    IntarkDBInValidException(const std::string& msg) : std::runtime_error(msg) {}
};
};  // namespace intarkdb

struct DbStorageWrapper {
    std::weak_ptr<BaseStorage> db_storage;
    std::string path;
};

class IntarkDB {
   private:
    IntarkDB() = delete;
    IntarkDB(const IntarkDB& db) = delete;
    IntarkDB& operator=(IntarkDB& db) = delete;

   protected:
    IntarkDB(const std::string& path);

   public:
    EXPORT_API virtual ~IntarkDB() {}
    // TODO: remove
    EXPORT_API void Init() {}
    EXPORT_API bool HasInit() { return true; }

    EXPORT_API static std::shared_ptr<IntarkDB> GetInstance(const char* path);
    EXPORT_API static std::shared_ptr<IntarkDB> GetWeakInstance(std::weak_ptr<BaseStorage> storage,
                                                                const std::string& path);

    const std::string& GetDbPath() { return path_; }
    const std::string& GetFullDbPath() { return full_dbpath_; }

    EXPORT_API virtual void DestoryHandle(void* handle) = 0;
    EXPORT_API virtual status_t AllocHandle(void** handle) = 0;
    EXPORT_API virtual bool IsDBAvailable() = 0;

    virtual int GetId(void* handle) = 0;

    void SetLastInsertRowid(int64_t rowid) {
        cm_spin_lock(&last_insert_rowid_lock, NULL);
        last_insert_rowid = rowid;
        cm_spin_unlock(&last_insert_rowid_lock);
    }
    int64_t LastInsertRowid() { return last_insert_rowid; }

   private:
    EXPORT_API static std::shared_ptr<BaseStorage> GetStorage(const std::string& in_path);

   private:
    std::string path_;
    std::string full_dbpath_;
    static std::mutex mutex_;
    static std::unordered_map<std::string, std::weak_ptr<BaseStorage>> instance_map_;

    int64_t last_insert_rowid{0};
    spinlock_t last_insert_rowid_lock{0};
};
