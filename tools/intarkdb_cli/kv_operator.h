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
* kv_operator.h
*
* IDENTIFICATION
* openGauss-embedded/tools/intarkdb_cli/kv_operator.h
*
* -------------------------------------------------------------------------
*/

#pragma once

#include <vector>
#include <string>
#include <map>

#include "compute/kv/kv_connection.h"

class KvOperator
{
public:
    KvOperator(std::unique_ptr<KvConnection> kvconn);
    ~KvOperator();

    typedef std::string(KvOperator::*kv_func)(const std::vector<std::string>& strList);
    //kv_func();
    std::string kv_set(const std::vector<std::string>& strList);
    std::string kv_get(const std::vector<std::string>& strList);
    std::string kv_del(const std::vector<std::string>& strList);
    std::string kv_change_table(const std::vector<std::string>& strList); //change table, create if not exist

    std::string kv_begin(const std::vector<std::string>& strList);
    std::string kv_commit(const std::vector<std::string>& strList);
    std::string kv_rollback(const std::vector<std::string>& strList);

    static std::string show_kv_help();

    std::map<std::string, kv_func> kv_func_map; // <cmd, func>
    std::string getKVTable() {return kv_table;}



private:
    std::unique_ptr<KvConnection> kv_connection_;
    bool auto_commit;
    std::string kv_table{"SYS_KV"};
};
