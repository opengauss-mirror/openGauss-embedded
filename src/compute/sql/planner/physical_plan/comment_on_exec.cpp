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
* comment_on_exec.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/physical_plan/comment_on_exec.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/physical_plan/comment_on_exec.h"

#include "binder/expressions/bound_constant.h"

auto CommentOnExec::Execute() const -> RecordBatch {
    exp_comment_def_t def;
    def.owner = { (char*)user_name.c_str(), (uint32)user_name.length() };
    def.name = { (char*)table_name.c_str(), (uint32)table_name.length() };
    def.column = { (char*)column_name.c_str(), (uint32)column_name.length() };
    def.comment = { (char*)comment.c_str(), (uint32)comment.length() };

    if (object_type_ == PG_OBJECT_COLUMN) {
        def.type = EXP_COMMENT_ON_COLUMN;
        auto table_info = catalog_->GetTable(user_name, table_name);
        if (table_info) {
            auto meta_info = table_info->GetTableMetaInfo();
            def.uid = meta_info.uid;
            def.id = meta_info.id;
            exp_column_def_t *columns_list = meta_info.columns;
            if (!columns_list) {
                throw std::invalid_argument(fmt::format("Get column info failed! column : ") + column_name);
            }
            bool col_is_found = false;
            for (uint32_t i = 0; i < meta_info.column_count; i++) {
                exp_column_def_t *col = columns_list + i;
                if (std::string(col->name.str) == column_name) {
                    col_is_found = true;
                    def.column_id = col->col_slot;
                    break;
                }
            }
            if (!col_is_found) {
                throw std::invalid_argument(fmt::format("column not found!"));
            }
        } else {
            throw std::invalid_argument(fmt::format("Get table info failed! table : ") + table_name);
        }
    } else if (object_type_ == PG_OBJECT_TABLE) {
        def.type = EXP_COMMENT_ON_TABLE;
        auto table_info = catalog_->GetTable(user_name, table_name);
        if (table_info) {
            auto meta_info = table_info->GetTableMetaInfo();
            def.uid = meta_info.uid;
            def.id = meta_info.id;
            def.column_id = GS_INVALID_ID32;
        } else {
            throw std::invalid_argument(fmt::format("Get table info failed! table : ") + table_name);
        }
    } else {
        throw std::invalid_argument(fmt::format("physical plan comment_on, object_type not support yet"));
    }

    RecordBatch rb(schema_);
    auto status = catalog_->CommentOn(&def);
    rb.SetRetCode(status);
    if(status != GS_SUCCESS) {
        int err_code;
        const char *message = nullptr;
        cm_get_error(&err_code, &message, NULL);
        std::string err_msg = fmt::format("CommentOn failed!! errno = {}, message = {}",
                                    err_code, message);
        cm_reset_error();
        GS_LOG_RUN_INF("%s", err_msg.c_str());
        rb.SetRetMsg(err_msg);
    }
    return rb;
}
