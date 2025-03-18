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
 * statement_type.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/statement_type.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>

#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <sstream>

enum class StatementType : uint8_t {
    INVALID_STATEMENT,        // invalid statement type
    SELECT_STATEMENT,         // select statement type
    INSERT_STATEMENT,         // insert statement type
    UPDATE_STATEMENT,         // update statement type
    CREATE_STATEMENT,         // create statement type
    DELETE_STATEMENT,         // delete statement type
    EXPLAIN_STATEMENT,        // explain statement type
    DROP_STATEMENT,           // drop statement type
    INDEX_STATEMENT,          // index statement type
    PREPARE_STATEMENT,        // prepare statement type
    EXECUTE_STATEMENT,        // execute statement type
    ALTER_STATEMENT,          // alter statement type
    TRANSACTION_STATEMENT,    // transaction statement type,
    COPY_STATEMENT,           // copy type
    ANALYZE_STATEMENT,        // analyze type
    VARIABLE_SET_STATEMENT,   // variable set statement type
    VARIABLE_SHOW_STATEMENT,  // show variable statement type
    CREATE_FUNC_STATEMENT,    // create func statement type
    EXPORT_STATEMENT,         // EXPORT statement type
    PRAGMA_STATEMENT,         // PRAGMA statement type
    SHOW_STATEMENT,           // SHOW statement type
    VACUUM_STATEMENT,         // VACUUM statement type    
    SET_STATEMENT,            // SET statement type
    LOAD_STATEMENT,           // LOAD statement type
    RELATION_STATEMENT,
    EXTENSION_STATEMENT,
    LOGICAL_PLAN_STATEMENT,
    ATTACH_STATEMENT,
    DETACH_STATEMENT,
    MULTI_STATEMENT,
    CTAS_STATEMENT,         // create table as select statement type
    SEQUENCE_STATEMENT,     // sequence statement type
    CREATE_VIEW_STATEMENT,  // CREATE VIEW statement type
    CHECKPOINT_STATEMENT,
    COMMENT_STATEMENT,
    CREATE_ROLE_STATEMENT,
    ALTER_ROLE_STATEMENT,
    DROP_ROLE_STATEMENT,
    GRANT_ROLE_STATEMENT,
    GRANT_STATEMENT,
    SYNONYM_STATEMENT,
};

template <>
struct fmt::formatter<StatementType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(StatementType c, FormatContext &ctx) const {
        std::string_view name;
        switch (c) {
            case StatementType::INVALID_STATEMENT:
                name = "Invalid";
                break;
            case StatementType::SELECT_STATEMENT:
                name = "Select";
                break;
            case StatementType::INSERT_STATEMENT:
                name = "Insert";
                break;
            case StatementType::UPDATE_STATEMENT:
                name = "Update";
                break;
            case StatementType::CREATE_STATEMENT:
                name = "Create";
                break;
            case StatementType::DELETE_STATEMENT:
                name = "Delete";
                break;
            case StatementType::EXPLAIN_STATEMENT:
                name = "Explain";
                break;
            case StatementType::DROP_STATEMENT:
                name = "Drop";
                break;
            case StatementType::INDEX_STATEMENT:
                name = "Index";
                break;
            case StatementType::PREPARE_STATEMENT:
                name = "Prepare";
                break;
            case StatementType::EXECUTE_STATEMENT:
                name = "Execute";
                break;
            case StatementType::ALTER_STATEMENT:
                name = "Alter";
                break;
            case StatementType::TRANSACTION_STATEMENT:
                name = "Transaction";
                break;
            case StatementType::COPY_STATEMENT:
                name = "Copy";
                break;
            case StatementType::ANALYZE_STATEMENT:
                name = "Analyze";
                break;
            case StatementType::VARIABLE_SET_STATEMENT:
                name = "VariableSet";
                break;
            case StatementType::VARIABLE_SHOW_STATEMENT:
                name = "VariableShow";
                break;
            case StatementType::CREATE_FUNC_STATEMENT:
                name = "CreateFunc";
                break;
            case StatementType::EXPORT_STATEMENT:
                name = "Export";
                break;
            case StatementType::PRAGMA_STATEMENT:
                name = "Pragma";
                break;
            case StatementType::SHOW_STATEMENT:
                name = "Show";
                break;
            case StatementType::VACUUM_STATEMENT:
                name = "Vacuum";
                break;
            case StatementType::SET_STATEMENT:
                name = "Set";
                break;
            case StatementType::LOAD_STATEMENT:
                name = "Load";
                break;
            case StatementType::RELATION_STATEMENT:
                name = "Relation";
                break;
            case StatementType::EXTENSION_STATEMENT:
                name = "Extension";
                break;
            case StatementType::LOGICAL_PLAN_STATEMENT:
                name = "LogicalPlan";
                break;
            case StatementType::ATTACH_STATEMENT:
                name = "Attach";
                break;
            case StatementType::DETACH_STATEMENT:
                name = "Detach";
                break;
            case StatementType::MULTI_STATEMENT:
                name = "Multi";
                break;
            case StatementType::CTAS_STATEMENT:
                name = "Ctas";
                break;
            case StatementType::SEQUENCE_STATEMENT:
                name = "Sequence";
                break;
            case StatementType::CREATE_VIEW_STATEMENT:
                name = "CreateView";
                break;
            case StatementType::CHECKPOINT_STATEMENT:
                name = "Checkpoint";
                break;
            case StatementType::COMMENT_STATEMENT:
                name = "Comment";
                break;           
            case StatementType::CREATE_ROLE_STATEMENT:
                name = "CreateRole";
                break;
            case StatementType::DROP_ROLE_STATEMENT:
                name = "DropRole";
                break;
            case StatementType::GRANT_ROLE_STATEMENT:
                name = "GrantRole";
                break;
            case StatementType::GRANT_STATEMENT:
                name = "Grant";
                break;
            default:
                name = std::to_string(static_cast<uint8_t>(c));
                break;
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};

namespace StatementProps {

enum class TableType {
    TABLE_TYPE_NORMAL = 0,
    TABLE_TYPE_TIMESCALE = 1,
};

struct TableInfo {
    std::string table_name;
    TableType table_type;
};

};


struct StatementProperty {
    std::unordered_map<std::string, StatementProps::TableInfo> read_tables;
    std::unordered_map<std::string, StatementProps::TableInfo> modify_tables;
    std::vector<StatementProps::TableType> GetTableTypes() {
        std::vector<StatementProps::TableType> v_type;
        for (const auto &item : read_tables) {
            v_type.push_back(item.second.table_type);
        }
        for (const auto &item : modify_tables) {
            v_type.push_back(item.second.table_type);
        }
        return v_type;
    }
    std::string GetStrTableTypes() {
        std::vector<StatementProps::TableType> v_type = GetTableTypes();
        std::ostringstream os_type;
        for (size_t i = 0; i < v_type.size(); i++) {
            if (i > 0) 
                os_type << ",";
            os_type << i;
        }
        return os_type.str();
    }
};
