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
 * intarkdb_kv-c.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/kv/intarkdb_kv-c.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <vector> 

#include "intarkdb_kv.h"
#include "intarkdb_kv.hpp"

#include "compute/kv/kv_connection.h"
#include "intarkdb_sql.h"

struct DatabaseWrapper_kv {
    std::shared_ptr<IntarkDB> instance;
};

KvReply default_reply = { KV_ERROR, 20, (char *)"KvConnection is NULL" };

int intarkdb_open_kv(const char *path, intarkdb_database_kv *db) {
    return intarkdb_open(path, (intarkdb_database *)db);
}

void intarkdb_close_kv(intarkdb_database_kv *db) {
    intarkdb_close((intarkdb_database *)db);
}

int intarkdb_connect_kv(intarkdb_database_kv database, intarkdb_connection_kv *kvconn) {
    if (!database || !kvconn) {
        return -1;
    }
    auto wrapper = (DatabaseWrapper_kv *)database;
    KvConnection *connection = nullptr;
    try {
        connection = new KvConnection(wrapper->instance);

        connection->Init();
    } catch (...) {
        if (connection) {
            delete connection;
            connection = nullptr;
        }
        return -1;
    }
    *kvconn = (intarkdb_connection_kv)connection;
    return 0;
}

void intarkdb_disconnect_kv(intarkdb_connection_kv *kvconn) {
    if (kvconn && *kvconn) {
        KvConnection *connection = (KvConnection *)*kvconn;
        delete connection;
        *kvconn = nullptr;
    }
}

int intarkdb_open_table_kv(intarkdb_connection_kv kvconn, const char *table_name) {
    if (!kvconn) {
        return -1;
    }

    KvConnection *conn = (KvConnection *)kvconn;
    return conn->OpenTable(table_name);
}

int intarkdb_open_memtable_kv(intarkdb_connection_kv kvconn, const char *table_name) {
    if (!kvconn) {
        return -1;
    }

    KvConnection *conn = (KvConnection *)kvconn;
    return conn->OpenMemoryTable(table_name);
}

void * intarkdb_set(intarkdb_connection_kv kvconn, const char *key, const char *val) {
    if (!kvconn) {
        return &default_reply;
    }

    KvConnection *conn = (KvConnection *)kvconn;
    return conn->Set(key, val);
}

void * intarkdb_get(intarkdb_connection_kv kvconn, const char *key) {
    if (!kvconn) {
        return &default_reply;
    }

    KvConnection *conn = (KvConnection *)kvconn;
    return conn->Get(key);
}

void * intarkdb_del(intarkdb_connection_kv kvconn, const char *key) {
    if (!kvconn) {
        return &default_reply;
    }

    KvConnection *conn = (KvConnection *)kvconn;
    return conn->Del(key);
}

intarkdb_state_kv intarkdb_begin(intarkdb_connection_kv kvconn) {
    if (!kvconn) {
        return KV_ERROR;
    }

    KvConnection *conn = (KvConnection *)kvconn;
    return (intarkdb_state_kv)conn->Begin();
}

intarkdb_state_kv intarkdb_commit(intarkdb_connection_kv kvconn) {
    if (!kvconn) {
        return KV_ERROR;
    }

    KvConnection *conn = (KvConnection *)kvconn;
    return (intarkdb_state_kv)conn->Commit();
}

intarkdb_state_kv intarkdb_rollback(intarkdb_connection_kv kvconn) {
    if (!kvconn) {
        return KV_ERROR;
    }

    KvConnection *conn = (KvConnection *)kvconn;
    return (intarkdb_state_kv)conn->Rollback();
}

intarkdb_state_kv intarkdb_multi(intarkdb_connection_kv kvconn) {
    return intarkdb_begin(kvconn);
}

intarkdb_state_kv intarkdb_exec(intarkdb_connection_kv kvconn) {
    return intarkdb_commit(kvconn);
}

intarkdb_state_kv intarkdb_discard(intarkdb_connection_kv kvconn) {
    return intarkdb_rollback(kvconn);
}

// --------------------------------------------------------------------------------------------------
#include <fmt/core.h>
#include <fmt/ranges.h>

#include "common/exception.h"
#include "common/string_util.h"

KvIntarkDB::KvIntarkDB() {};

KvIntarkDB::~KvIntarkDB() {
    if (conn_) {
        intarkdb_disconnect_kv(&conn_);
    }

    if (db_) {
        intarkdb_close_kv(&db_);
    }
}

void KvIntarkDB::Connect(std::string db_path) {
    if (!db_) {
        if (intarkdb_open_kv(db_path.c_str(), &db_) != KV_SUCCESS) {
            throw intarkdb::Exception(ExceptionType::CONNECTION, fmt::format("open database error!"));
        }
    }

    if (!conn_) {
        if (intarkdb_connect_kv(db_, &conn_) != KV_SUCCESS) {
            throw intarkdb::Exception(ExceptionType::CONNECTION, fmt::format("connect error!"));
        }
    }
}

int KvIntarkDB::OpenTable(const char* table_name) {
    return intarkdb_open_table_kv(conn_, table_name);
}

int KvIntarkDB::OpenMemoryTable(const char* table_name) {
    return intarkdb_open_memtable_kv(conn_, table_name);
}

Reply * KvIntarkDB::Set(const char* key, const char* val) {
    return (Reply *)intarkdb_set(conn_, key, val);
}

Reply * KvIntarkDB::Get(const char* key) {
    return (Reply *)intarkdb_get(conn_, key);
}

Reply * KvIntarkDB::Del(const char* key) {
    return (Reply *)intarkdb_del(conn_, key);
}

int KvIntarkDB::Begin() {
    return intarkdb_begin(conn_);
}

int KvIntarkDB::Commit() {
    return intarkdb_commit(conn_);
}

int KvIntarkDB::Rollback() {
    return intarkdb_rollback(conn_);
}