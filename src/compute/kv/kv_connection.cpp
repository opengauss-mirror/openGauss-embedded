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
 * connection.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/kv/kv_connection.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "kv_connection.h"

#include "main/database.h"
#include "storage/db_handle.h"
#include "storage/gstor/gstor_executor.h"

KvConnection::KvConnection(std::shared_ptr<IntarkDB> instance) : instance_(instance) {} 

KvConnection::~KvConnection() {
    if (handle_) {
        auto instance_ptr = instance_.lock();
        if (instance_ptr != nullptr) {
            instance_ptr->DestoryHandle(handle_);
        }
        handle_ = nullptr;
    }
}

void KvConnection::Init() {
    if (instance_.lock() == nullptr || !instance_.lock()->HasInit()) {
        throw std::runtime_error("Connection init error, The database instance must be initialized first!");
    }

    // get the database operate handle
    auto instance_ptr = instance_.lock();
    if (instance_ptr == nullptr) {
        throw std::runtime_error("db_handle_alloc error, the database has closed!");
    }

    auto ret = instance_ptr->AllocHandle(&handle_);
    if (ret != GS_SUCCESS) {
        if (ret == GS_FULL_CONN) {
            throw std::runtime_error("The number of connections has reached the upper limit!");
        }
        throw std::runtime_error("get database handle fail");
    }

    // open default kv table
    OpenTable(kv_table.c_str());
}

int KvConnection::OpenTable(const char* table_name) {
    return gstor_open_table(((db_handle_t*)handle_)->handle, table_name);
}

int KvConnection::OpenMemoryTable(const char* table_name) {
    return gstor_open_mem_table(((db_handle_t*)handle_)->handle, table_name);
}

KvReplyInternal * KvConnection::Set(const char* key, const char* val) {
    ret_code_ = GS_SUCCESS;
    ret_msg_ = "success";
    memset(&reply, 0, sizeof(reply));

    if (key == nullptr || val == nullptr) {
        ret_code_ = GS_ERROR;
        ret_msg_ = "key or val is NULL!";
        reply.type = ret_code_;
        reply.len = ret_msg_.length();
        reply.str = (char *)ret_msg_.c_str();
        return &reply;
    }

    text_t text_key, text_val;
    text_key.len = strlen(key);
    text_key.str = (char*)key;
    text_val.len = strlen(val);
    text_val.str = (char*)val;
    ret_code_ = gstor_put(((db_handle_t*)handle_)->handle, text_key.str, text_key.len, text_val.str, text_val.len);

    if (ret_code_ == GS_SUCCESS) {
        if (!is_multi_) {
            gstor_commit(((db_handle_t*)handle_)->handle);
        }
    } else {
        if (!is_multi_) {
            gstor_rollback(((db_handle_t*)handle_)->handle);
        }
        int32_t err_code;
        const char* message = nullptr;
        cm_get_error(&err_code, &message, nullptr);

        ret_code_ = GS_ERROR;
        ret_msg_ = std::string(message);
        cm_reset_error();
    }

    reply.type = ret_code_;
    reply.len = ret_msg_.length();
    reply.str = (char *)ret_msg_.c_str();
    return &reply;
}

KvReplyInternal * KvConnection::Get(const char* key) {
    ret_code_ = GS_SUCCESS;
    ret_msg_ = "success";
    memset(&reply, 0, sizeof(reply));

    if (key == nullptr) {
        ret_code_ = GS_ERROR;
        ret_msg_ = "key is NULL!";
        reply.type = ret_code_;
        reply.len = ret_msg_.length();
        reply.str = (char *)ret_msg_.c_str();
        return &reply;
    }

    text_t text_key, text_val;
    text_key.len = strlen(key);
    text_key.str = (char*)key;
    bool32 eof;
    text_val.str = nullptr;
    text_val.len = 0;
    ret_code_ = gstor_get(((db_handle_t*)handle_)->handle, text_key.str, text_key.len, &text_val.str, &text_val.len, &eof);

    if (ret_code_ == GS_SUCCESS) {
        ret_msg_ = std::string(text_val.str, text_val.len);
    } else {
        int32_t err_code;
        const char* message = nullptr;
        cm_get_error(&err_code, &message, nullptr);

        ret_code_ = GS_ERROR;
        ret_msg_ = std::string(message);
        cm_reset_error();
    }

    reply.type = ret_code_;
    reply.len = ret_msg_.length();
    reply.str = (char *)ret_msg_.c_str();
    return &reply;
}

KvReplyInternal * KvConnection::Del(const char* key) {
    ret_code_ = GS_SUCCESS;
    ret_msg_ = "success";
    memset(&reply, 0, sizeof(reply));

    if (key == nullptr) {
        ret_code_ = GS_ERROR;
        ret_msg_ = "key is NULL!";
        reply.type = ret_code_;
        reply.len = ret_msg_.length();
        reply.str = (char *)ret_msg_.c_str();
        return &reply;
    }

    text_t text_key;
    text_key.len = strlen(key);
    text_key.str = (char*)key;
    unsigned int prefix = 0;
    unsigned int count = 0;
    ret_code_ = gstor_del(((db_handle_t*)handle_)->handle, text_key.str, text_key.len, prefix, &count);

    if (ret_code_ == GS_SUCCESS) {
        if (!is_multi_) {
            gstor_commit(((db_handle_t*)handle_)->handle);
        }
    } else {
        if (!is_multi_) {
            gstor_rollback(((db_handle_t*)handle_)->handle);
        }
        int32_t err_code;
        const char* message = nullptr;
        cm_get_error(&err_code, &message, nullptr);

        ret_code_ = GS_ERROR;
        ret_msg_ = std::string(message);
        cm_reset_error();
    }

    reply.type = ret_code_;
    reply.len = ret_msg_.length();
    reply.str = (char *)ret_msg_.c_str();
    return &reply;
}

int KvConnection::Begin() {
    is_multi_ = true;
    return gstor_begin(((db_handle_t*)handle_)->handle);
}

int KvConnection::Commit() {
    is_multi_ = false;
    return gstor_commit(((db_handle_t*)handle_)->handle);
}

int KvConnection::Rollback() {
    is_multi_ = false;
    return gstor_rollback(((db_handle_t*)handle_)->handle);
}