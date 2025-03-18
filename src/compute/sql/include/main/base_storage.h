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
 * base_storage.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/main/base_storage.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <cstring>
#include <memory>
#include <string>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <functional>

#include "common/winapi.h"
#include "storage/db_handle.h"
#include "storage/gstor/gstor_executor.h"

struct StreamAggRunContext {
    std::mutex m;
    std::condition_variable cv;
    std::atomic<bool> exit_flag = false;
};

class BaseStorage {
   public:
    explicit BaseStorage();
    ~BaseStorage();

    // startup database
    void Open(char *path);

    // create a database operation handle
    // NOTE: 是否线程安全？多个相同数据库实例，可能出现不同线程同时调用该函数
    status_t db_handle_alloc(void **handle);

    // free a database operation handle
    void db_handle_free(void *handle);

    bool db_is_available();

    int get_handle_id(void *handle);


    uint64_t get_sql_engine_memory_limit();

    using StreamAggFunc = std::function<void(StreamAggRunContext&)>;
  
   private:
    int mkdir_multi(const char *path);
    void prepare_path(char *path);
    void db_shutdown();

    status_t db_startup(char *path);

    inline void init_g_handle_pool(void) {
        handle_pool.hwm = 0;
        handle_pool.lock = 0;
        biqueue_init(&handle_pool.idle_list);
    }

    inline void destroy_db_handle(db_handle_t *db_handle) {
        gstor_free(db_handle->handle);
        CM_FREE_PTR(db_handle);
    }

    inline void deinit_g_handle_pool(void) {
        for (uint32 i = 0; i < handle_pool.hwm; ++i) {
            db_handle_t *db_handle = handle_pool.handles[i];
            if (db_handle == NULL) {
                continue;
            }
            destroy_db_handle(db_handle);
        }
    }

    inline void return_free_handle(db_handle_t *db_handle) {
        cm_spin_lock(&handle_pool.lock, NULL);
        biqueue_add_tail(&handle_pool.idle_list, QUEUE_NODE_OF(db_handle));
        cm_spin_unlock(&handle_pool.lock);
    }

    inline bool32 reuse_handle(db_handle_t **handle) {
        if (biqueue_empty(&handle_pool.idle_list)) {
            return GS_FALSE;
        }

        cm_spin_lock(&handle_pool.lock, NULL);
        biqueue_node_t *node = biqueue_del_head(&handle_pool.idle_list);
        if (node == NULL) {
            cm_spin_unlock(&handle_pool.lock);
            return GS_FALSE;
        }

        *handle = OBJECT_OF(db_handle_t, node);
        cm_spin_unlock(&handle_pool.lock);
        return GS_TRUE;
    }

   private:
    bool has_open_ = false;

    std::unique_ptr<db_storage_t> db_storage_;
    // free in gstor_shutdown
    void *storage_instance_;

    // database operation handle pool
    handle_pool_t handle_pool;
    
};
