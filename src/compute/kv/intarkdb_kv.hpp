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
* intarkdb_kv.hpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/kv/intarkdb_kv.hpp
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

class Reply {
   public:
    int type;
    size_t len;
    char *str;
};

class KvIntarkDB {
   public:
    EXP_KV_API KvIntarkDB();
    EXP_KV_API ~KvIntarkDB();

    EXP_KV_API void Connect(std::string db_path);

    EXP_KV_API int OpenTable(const char* table_name);
    EXP_KV_API int OpenMemoryTable(const char* table_name);

    EXP_KV_API Reply * Set(const char* key, const char* val);
    EXP_KV_API Reply * Get(const char* key);
    EXP_KV_API Reply * Del(const char* key);

    EXP_KV_API int Begin();
    EXP_KV_API int Commit();
    EXP_KV_API int Rollback();

   private:
    struct st_intarkdb_database_kv * db_{nullptr};
    struct st_intarkdb_connection_kv * conn_{nullptr};
};
