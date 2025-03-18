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
* kv_operator.cpp
*
* IDENTIFICATION
* openGauss-embedded/tools/intarkdb_cli/kv_operator.cpp
*
* -------------------------------------------------------------------------
*/

#include "kv_operator.h"

#include <sstream>

#include "storage/gstor/zekernel/common/cm_defs.h"

static std::string KV_USEAGE =
    "KV Usage: \n"
    "    set key value : set key's value \n"
    "    get key : get key's value \n"
    "    del key : delete key \n"
    "    kvtb tablename : change table, create if not exist \n"
    "    multi : start a transaction \n"
    "    exec : commit current transaction \n"
    "    discard : discard current transaction \n"
    "    help kv: show kv usage \n";

// kv_operator::kv_operator(std::shared_ptr<IntarkDB> instance) 
KvOperator::KvOperator(std::unique_ptr<KvConnection> kvconn) : kv_connection_(std::move(kvconn)), auto_commit(true)
{
    kv_func_map.insert(std::make_pair<std::string, KvOperator::kv_func>("set", &KvOperator::kv_set));
    kv_func_map.insert(std::make_pair<std::string, KvOperator::kv_func>("get", &KvOperator::kv_get));
    kv_func_map.insert(std::make_pair<std::string, KvOperator::kv_func>("del", &KvOperator::kv_del));
    kv_func_map.insert(std::make_pair<std::string, KvOperator::kv_func>("kvtb", &KvOperator::kv_change_table));
    //事务 仿照redis命令
    kv_func_map.insert(std::make_pair<std::string, KvOperator::kv_func>("multi", &KvOperator::kv_begin));
    kv_func_map.insert(std::make_pair<std::string, KvOperator::kv_func>("exec", &KvOperator::kv_commit));
    kv_func_map.insert(std::make_pair<std::string, KvOperator::kv_func>("discard", &KvOperator::kv_rollback));
    //事务
    kv_func_map.insert(std::make_pair<std::string, KvOperator::kv_func>("begin", &KvOperator::kv_begin));
    kv_func_map.insert(std::make_pair<std::string, KvOperator::kv_func>("commit", &KvOperator::kv_commit));
    kv_func_map.insert(std::make_pair<std::string, KvOperator::kv_func>("rollback", &KvOperator::kv_rollback));
}

KvOperator::~KvOperator()
{
    // 
}

std::string KvOperator::kv_set(const std::vector<std::string>& strList)
{
    if(strList.size() != 3) {
        return "cmd err usage: set key value;\n";
    }

    std::stringstream res;
    auto reply = kv_connection_->Set(strList[1].c_str(), strList[2].c_str());
    if (reply->type == GS_SUCCESS) {
        if (auto_commit) {
            kv_commit({});
            res << "Success";
        } else {
            res << "Success in this Transaction";
        }
    } else {
        res << "kv set failed key:" << strList[1] << " value:" << strList[2];
    }
    res << std::endl;
    return res.str();
}

std::string KvOperator::kv_get(const std::vector<std::string>& strList)
{
    if(strList.size() != 2) {
        return "cmd err usage: get key;\n";
    }

    std::stringstream res;
    auto reply = kv_connection_->Get(strList[1].c_str());
    if (reply->type == GS_SUCCESS) {
        if (reply->len > 0) {
            res << reply->str;
        } else {
            res << "key: " << strList[1] << " not exist";
        }
    } else {
        printf("get failed...\r\n");
    }
    res << std::endl;
    return res.str();
}

std::string KvOperator::kv_del(const std::vector<std::string>& strList)
{
    if(strList.size() != 2) {
        return "cmd err usage: del key;\n";
    }

    std::stringstream res;
    auto reply = kv_connection_->Del(strList[1].c_str());
    if (reply->type == GS_SUCCESS) {
        if (auto_commit) {
            kv_commit({});
            res << "Success";
        } else {
            res << "Success in this Transaction";
        }
    } else {
        res << "kv del failed key:" << strList[1];
    }
    res << std::endl;
    return res.str();
}

std::string KvOperator::kv_change_table(const std::vector<std::string>& strList)
{
    if(strList.size() != 2) {
        return "cmd err usage: kvtb tablename;\n";
    }
    
    std::stringstream res;
    if (kv_connection_->OpenTable(strList[1].c_str()) != GS_SUCCESS) {
        res << "kv change table: "<< strList[1] << " fail";
    } else {
        res << "kv change table: "<< strList[1] << " success";
        kv_table = strList[1];
    }

    res << std::endl;
    return res.str();
}

std::string KvOperator::kv_begin(const std::vector<std::string>& strList)
{
    auto_commit = false;
    kv_connection_->Begin();
    return "";
}

std::string KvOperator::kv_commit(const std::vector<std::string>& strList)
{
    auto_commit = true;
    kv_connection_->Commit();
    return "";
}

std::string KvOperator::kv_rollback(const std::vector<std::string>& strList)
{
    auto_commit = true;
    kv_connection_->Rollback();
    return "";
}

std::string KvOperator::show_kv_help()
{
    return KV_USEAGE;
}
