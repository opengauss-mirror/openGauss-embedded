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
* show_exec.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/physical_plan/show_exec.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/physical_plan/show_exec.h"

#include <fmt/core.h>

#include "type/type_str.h"
#include "common/default_value.h"
#include "common/string_util.h"
#include "monitor/status.h"
#include "include/intarkdb.h"
#include "storage/gstor/gstor_executor.h"
#include "storage/gstor/gstor_instance.h"
#include "storage/gstor/zekernel/kernel/common/knl_context.h"
#include "storage/gstor/zekernel/kernel/common/knl_session.h"

auto ShowExec::Execute() const -> RecordBatch {
    return RecordBatch(Schema());
}

void ShowExec::Execute(RecordBatch &rb_out) {
    switch (show_type_) {
        case ShowType::SHOW_TYPE_DATABASES: {
            throw intarkdb::Exception(ExceptionType::CATALOG,
                                      fmt::format("show type {} is not supported", show_type_));
            break;
        }
        case ShowType::SHOW_TYPE_TABLES: {
            ShowTables(rb_out);
            break;
        }
        case ShowType::SHOW_TYPE_SPECIFIC_TABLE: {
            DescribeTable(rb_out);
            break;
        }
        case ShowType::SHOW_TYPE_ALL: {
            ShowAll(rb_out);
            break;
        }
        case ShowType::SHOW_TYPE_VARIABLES: {
            ShowVariables(rb_out);
            break;
        }
        case ShowType::SHOW_TYPE_USERS: {
            ShowUsers(rb_out);
            break;
        }
        default: {
            throw intarkdb::Exception(ExceptionType::CATALOG,
                                      fmt::format("show type {} is not supported", int(show_type_)));
        }
    }
}

void ShowExec::ShowTables(RecordBatch &rb_out) {
    auto schema = child_->GetSchema();
    rb_out = RecordBatch(schema);
    while (true) {
        auto [r, _, eof] = child_->Next();
        if (eof) {
            break;
        }
        rb_out.AddRecord(r);
    }
}

void ShowExec::DescribeTable(RecordBatch &rb_out) {
    auto schema = child_->GetSchema();
    rb_out = RecordBatch(schema);
    while (true) {
        auto [r, _, eof] = child_->Next();
        if (eof) {
            break;
        }
        rb_out.AddRecord(r);
    } 
}

void ShowExec::ShowAll(RecordBatch &rb_out) {
    std::vector<SchemaColumnInfo> columns = { 
            { {"__show_tables_expanded", "name"}, "", GS_TYPE_VARCHAR, 0}, 
            { {"__show_tables_expanded", "value"}, "", GS_TYPE_VARCHAR, 1}, 
            { {"__show_tables_expanded", "description"}, "", GS_TYPE_VARCHAR, 2}, };
    auto schema = Schema(std::move(columns));
    rb_out = RecordBatch(schema);
#ifndef WIN32
    std::string split = "/";
#else
    std::string split = "\\";
#endif
    std::string full_db_path = db_path + split + "intarkdb" + split;
    // cpu
    {
        std::vector<Value> row_values;
        auto val_cpu = get_self_cpu();
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "cpu"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, std::to_string(val_cpu)));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "process cpu"));
        rb_out.AddRecord(Record(std::move(row_values)));
    }

    // memory
    {
        std::vector<Value> row_values;
        auto val_memory = get_memory();
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "memory"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, std::to_string(val_memory)));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "process memory(k)"));
        rb_out.AddRecord(Record(std::move(row_values)));
    }

    // db size
    {
        std::vector<Value> row_values;
        auto val_dir_size = get_dir_size(full_db_path.c_str());
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "dir_size"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, std::to_string(val_dir_size)));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "the size of db dir(byte)"));
        rb_out.AddRecord(Record(std::move(row_values)));
    }

    // db path
    {
        std::vector<Value> row_values;
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "db_path"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, full_db_path));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "the path of db"));
        rb_out.AddRecord(Record(std::move(row_values)));
    }

    // log path
    {
        std::vector<Value> row_values;
        std::string log_path = full_db_path + "log" + split + "run" + split;
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "log_path"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, log_path));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "the path of db run log"));
        rb_out.AddRecord(Record(std::move(row_values)));
    }

    // db version
    {
        std::vector<Value> row_values;
        std::string db_version = get_version();
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "db_version"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, db_version));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "the version of db"));
        rb_out.AddRecord(Record(std::move(row_values)));
    }

    // db starttime
    {
        std::vector<Value> row_values;
        auto start_time = get_start_time();
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "start_time"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, std::to_string(start_time)));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "db start time"));
        rb_out.AddRecord(Record(std::move(row_values)));
    }

    // os infomation
    {
        std::vector<Value> row_values;
        char osinfo[1024];
        get_os_info(osinfo, 1024);
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "osinfo"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, osinfo));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "operating system infomation(sysname, release, machine)"));
        rb_out.AddRecord(Record(std::move(row_values)));
    }
}

void ShowExec::ShowVariables(RecordBatch &rb_out) {
    std::vector<SchemaColumnInfo> columns = { 
            { {"__variables_show", "Variable_name"}, "", GS_TYPE_VARCHAR, 0}, 
            { {"__variables_show", "Value"}, "", GS_TYPE_VARCHAR, 1}, };
    auto schema = Schema(std::move(columns));
    rb_out = RecordBatch(schema);

    auto storage = catalog_.GetStorageHandle();

    auto variable_name = intarkdb::StringUtil::Lower(variable_name_);

    if (variable_name == "max_connections") {
        auto max_conn = gstor_get_max_connections(storage->handle);

        std::vector<Value> row_values;
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "max_connections"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, std::to_string(max_conn)));
        rb_out.AddRecord(Record(std::move(row_values)));
    } else if (variable_name == "synchronous_commit") {
        knl_session_t *session = EC_SESSION(catalog_.GetStorageHandle()->handle);
        std::string sync_commit = session->kernel->attr.synchronous_commit ? "on" : "off";

        std::vector<Value> row_values;
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "synchronous_commit"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, sync_commit));
        rb_out.AddRecord(Record(std::move(row_values)));
    } else if(variable_name == "resources") {
        // cpu
        std::vector<Value> row_values;
        auto val_cpu = get_self_cpu();
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "cpu"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, std::to_string(val_cpu)));
        rb_out.AddRecord(Record(std::move(row_values)));

        // memory
        row_values.clear();
        auto val_memory = get_memory();
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "memory"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, std::to_string(val_memory)));
        rb_out.AddRecord(Record(std::move(row_values)));
    } else if(variable_name == "info"){
        #ifndef WIN32
            std::string split = "/";
        #else
            std::string split = "\\";
        #endif
            std::string full_db_path = db_path + split + "intarkdb" + split;
        
        // dir_size
        std::vector<Value> row_values;
        auto val_dir_size = get_dir_size(full_db_path.c_str());
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "dir_size"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, std::to_string(val_dir_size)));
        rb_out.AddRecord(Record(std::move(row_values)));

        // db_path
        row_values.clear();
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "db_path"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, full_db_path));
        rb_out.AddRecord(Record(std::move(row_values)));

        // log path
        row_values.clear();
        std::string log_path = full_db_path + "log" + split + "run" + split;
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "log_path"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, log_path));
        rb_out.AddRecord(Record(std::move(row_values)));

        // db version
        row_values.clear();
        std::string db_version = get_version();
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "db_version"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, db_version));
        rb_out.AddRecord(Record(std::move(row_values)));

        // db starttime
        row_values.clear();
        auto start_time = get_start_time();
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "start_time"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, std::to_string(start_time)));
        rb_out.AddRecord(Record(std::move(row_values)));

        // os infomation
        row_values.clear();
        char osinfo[1024];
        get_os_info(osinfo, 1024);
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, "osinfo"));
        row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, osinfo));
        rb_out.AddRecord(Record(std::move(row_values)));      
    }
    else {
        throw intarkdb::Exception(ExceptionType::CATALOG,
                                      fmt::format("show variable name {} is not supported", variable_name));
    }
}

void ShowExec::ShowUsers(RecordBatch &rb_out) {
    std::vector<SchemaColumnInfo> columns = { 
            { {"__users_show", "user_name"}, "", GS_TYPE_VARCHAR, 0} };
    auto schema = Schema(std::move(columns));
    rb_out = RecordBatch(schema);

    knl_session_t *session = EC_SESSION(catalog_.GetStorageHandle()->handle);
    dc_context_t dc_ctx = session->kernel->dc_ctx;
    for (uint32 i = 0; i < dc_ctx.user_hwm; i++) {
        dc_user_t *user = dc_ctx.users[i];

        std::vector<Value> row_values;
        if (catalog_.GetUserID() == user->desc.id) {
            row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, user->desc.name));
            rb_out.AddRecord(Record(std::move(row_values)));
        } else if (variable_name_ == "__show_all_users") {
            row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, user->desc.name));
            rb_out.AddRecord(Record(std::move(row_values)));
        }
    }
}
