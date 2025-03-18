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
 * kv_connection.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/kv/kv_connection.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#ifdef _WIN32
#define EXP_KV_API __declspec(dllexport)
#else
#define EXP_KV_API __attribute__((visibility("default")))
#endif

#include <string>
#include <memory>

class IntarkDB;

class KvReplyInternal {
   public:
    int type;   /* return type */
    size_t len; /* Length of string */
    char *str;  /* err or value*/
};

class KvConnection {
   public:
    EXP_KV_API explicit KvConnection(std::shared_ptr<IntarkDB> instance);
    EXP_KV_API ~KvConnection();

    EXP_KV_API void Init();

    EXP_KV_API int OpenTable(const char* table_name);
    EXP_KV_API int OpenMemoryTable(const char* table_name);

    EXP_KV_API KvReplyInternal * Set(const char* key, const char* val);
    EXP_KV_API KvReplyInternal * Get(const char* key);
    EXP_KV_API KvReplyInternal * Del(const char* key);

    EXP_KV_API int Begin();
    EXP_KV_API int Commit();
    EXP_KV_API int Rollback();

   public:
    EXP_KV_API int32_t GetRetCode() { return ret_code_; }
    EXP_KV_API const std::string& GetRetMsg() { return ret_msg_; }
    EXP_KV_API KvReplyInternal * GetReply() { return &reply; }

   private:
    std::weak_ptr<IntarkDB> instance_;
    void* handle_{NULL};

    bool is_multi_ = false;

    KvReplyInternal reply;
    int32_t ret_code_{0};
    std::string ret_msg_{"success"};

    std::string kv_table{"SYS_KV"};
};
