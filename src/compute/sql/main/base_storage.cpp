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
 * base_storage.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/main/base_storage.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "main/base_storage.h"

#ifndef _WIN32
#include <sys/stat.h>
#endif

#include <fmt/format.h>

#include <iostream>
#include <stdexcept>
#include <thread>

#include "storage/gstor/zekernel/common/cm_error.h"
#include "storage/gstor/zekernel/common/cm_utils.h"
#include "storage/gstor/zekernel/kernel/common/knl_session.h"

BaseStorage::BaseStorage() {}

BaseStorage::~BaseStorage() { 
    // notify out     
    db_shutdown(); 
}

void BaseStorage::Open(char *path) {
    if (has_open_) {
        return;
    }

    prepare_path(path);

    if (db_startup(path) != GS_SUCCESS) {
        int32 err_code;
        const char *message = nullptr;
        cm_get_error(&err_code, &message, NULL);
        GS_LOG_RUN_ERR("startup fail!! errno = %d, message = %s\n", err_code, message);
        if (err_code == ERR_LOCK_FILE) {
            GS_LOG_RUN_ERR("database is locked!! (maybe in use.)");
            throw std::runtime_error("database is locked!! (maybe in use.)");
        }
        std::string msg = message;
        throw std::runtime_error(msg);
    }

    has_open_ = true;
}

status_t BaseStorage::db_startup(char *path) {
    char real_data_path[GS_FILE_NAME_BUFFER_SIZE] = {0};

    if (CM_IS_EMPTY_STR(path)) {
        GS_LOG_RUN_ERR("[STG] data path is empty");
        return GS_ERROR;
    }
    GS_RETURN_IFERR(realpath_file(path, real_data_path, GS_FILE_NAME_BUFFER_SIZE));

    db_storage_ = std::make_unique<db_storage_t>();
    if (db_storage_ == nullptr) {
        GS_LOG_RUN_ERR("[STG] db_storage memory failed");
        return GS_ERROR;
    }

    if (gstor_startup(&db_storage_->instance, real_data_path) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[STG] db %u startup failed", 0);
        return GS_ERROR;
    }

    storage_instance_ = db_storage_->instance;
    init_g_handle_pool();

    return GS_SUCCESS;
}

constexpr uint32_t DB_PATH_MODE = 0755;
int BaseStorage::mkdir_multi(const char *path) {
    int ret = 0;
#ifndef _WIN32
    char *tmp = strdup(path);
    char *p = tmp;
    int len;

    len = strlen(tmp);
    if (tmp[len - 1] == '/')
        tmp[len - 1] = 0;

    for (p = tmp + (p[0] == '/' ? 1 : 0); *p; p++) {
        if (*p == '/') {
            *p = 0;
            if (mkdir(tmp, DB_PATH_MODE) != 0 && errno != EEXIST) {
                free(tmp);
                return -1;
            }
            *p = '/';
        }
    }

    if (mkdir(tmp, DB_PATH_MODE) != 0 && errno != EEXIST) {
        ret = -1;
    }

    free(tmp);
#endif
    return ret;
}

void BaseStorage::prepare_path(char *path) {
#ifndef _WIN32
    struct stat st;

    // check if path exists
    if (stat(path, &st) == 0) {
        if (S_ISDIR(st.st_mode)) {
            return;
        }
    }
    
    // create path
    if (mkdir_multi(path) == 0) {
        return;
    } else {
        throw std::runtime_error("Error creating directory!");
    }
#endif
}

void BaseStorage::db_shutdown() {
    deinit_g_handle_pool();
    gstor_shutdown((st_instance *)storage_instance_);
    storage_instance_ = nullptr;
}

status_t BaseStorage::db_handle_alloc(void **handle) {
    if (!storage_instance_) {
        return GS_ERROR;
    }

    if (reuse_handle((db_handle_t **)handle)) {
        return GS_SUCCESS;
    }

    db_handle_t *db_handle = (db_handle_t *)malloc(sizeof(db_handle_t));
    if (db_handle == NULL) {
        GS_LOG_RUN_ERR("[STG] alloc memory failed");
        return GS_ERROR;
    }
    if (gstor_alloc((st_instance *)storage_instance_, &db_handle->handle) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[STG] alloc handle from db failed");
        CM_FREE_PTR(db_handle);
        return GS_ERROR;
    }
    db_handle->next = db_handle->prev = NULL;

    cm_spin_lock(&handle_pool.lock, NULL);
    auto max_conn_num = gstor_get_max_connections(db_handle->handle);
    if (handle_pool.hwm >= max_conn_num) {
        GS_LOG_RUN_ERR("[STG] alloc handle failed, the maximum connection count(%u) is reached!",max_conn_num);
        gstor_free(db_handle->handle);
        CM_FREE_PTR(db_handle);
        cm_spin_unlock(&handle_pool.lock);
        return GS_FULL_CONN;
    }   
    handle_pool.handles[handle_pool.hwm++] = db_handle;
    cm_spin_unlock(&handle_pool.lock);
    GS_LOG_RUN_INF("[STG] alloc handle success, hwm = %d", handle_pool.hwm);
    *handle = db_handle;
    return GS_SUCCESS;
}

void BaseStorage::db_handle_free(void *handle) {
    db_handle_t *db_handle = ((db_handle_t *)handle);

    gstor_clean(db_handle->handle);

    return_free_handle(db_handle);
}

bool BaseStorage::db_is_available() { return has_open_ && db_is_open(storage_instance_); }

uint64_t BaseStorage::get_sql_engine_memory_limit() { return gstor_get_sql_engine_memory_limit(storage_instance_); }

int BaseStorage::get_handle_id(void *handle) {
    if(handle == nullptr) {
        return -1;
    }
    db_handle_t *db_handle = (db_handle_t *)handle;
    ec_handle_t* ec_handle = (ec_handle_t*)db_handle->handle;
    if(ec_handle == nullptr) {
        return -1;
    }
    auto session = ec_handle->session;
    if(session == nullptr) {
        return -1;
    }
    return session->id;
}
