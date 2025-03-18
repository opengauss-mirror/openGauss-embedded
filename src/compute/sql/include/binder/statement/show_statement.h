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
* show_statement.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/statement/show_statement.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/bound_statement.h"
#include "binder/statement/select_statement.h"
#include "catalog/table_info.h"

enum class ShowType : int8_t {
    SHOW_TYPE_UNKNOWN = 0,
    SHOW_TYPE_DATABASES = 1,       // show databases;
    SHOW_TYPE_TABLES = 2,          // show tabls;
    SHOW_TYPE_SPECIFIC_TABLE = 3,  // describe <table name>;
    SHOW_TYPE_ALL = 4,             // show all;
    SHOW_TYPE_CREATE_TABLE = 5,    // show create table;
    SHOW_TYPE_VARIABLES = 6,       // show VARIABLES like;
    SHOW_TYPE_USERS = 7,           // show USERS;
};

class ShowStatement : public BoundStatement {
   public:
    explicit ShowStatement() : BoundStatement(StatementType::SHOW_STATEMENT) {}

    void SetPragma(bool is_pragma) { is_pragma_ = is_pragma; }
    bool GetPragma() const { return is_pragma_; }

   public:
    ShowType show_type;
    std::string name;
    std::string table_name;                 // show_type_ == SHOW_TYPE_SPECIFIC_TABLE 时有效
    std::unique_ptr<TableInfo> table_info;  // table_name_ 的元数据
    std::unique_ptr<SelectStatement> stmt;
    std::string db_path;
    std::string variable_name;
   private:
    bool is_pragma_{false};  // 由于系统表中的字段全为大写，需要该标志转换
};

template <>
struct fmt::formatter<ShowType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(ShowType c, FormatContext &ctx) const {
        std::string_view name;
        switch (c) {
            case ShowType::SHOW_TYPE_DATABASES:
                name = "SHOW_TYPE_DATABASES";
                break;
            case ShowType::SHOW_TYPE_TABLES:
                name = "SHOW_TYPE_TABLES";
                break;
            case ShowType::SHOW_TYPE_SPECIFIC_TABLE:
                name = "SHOW_TYPE_SPECIFIC_TABLE";
                break;
            case ShowType::SHOW_TYPE_ALL:
                name = "SHOW_TYPE_ALL";
                break;
            case ShowType::SHOW_TYPE_CREATE_TABLE:
                name = "SHOW_TYPE_CREATE_TABLE";
                break;
            case ShowType::SHOW_TYPE_VARIABLES:
                name = "SHOW_TYPE_VARIABLES";
                break;
            case ShowType::SHOW_TYPE_USERS:
                name = "SHOW_TYPE_USERS";
                break;
            default:
                name = "SHOW_TYPE_UNKNOWN";
                break;
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};
