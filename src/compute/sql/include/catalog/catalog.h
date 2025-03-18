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
 * catalog.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/catalog/catalog.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "binder/statement/alter_statement.h"
#include "binder/statement/create_index_statement.h"
#include "binder/statement/create_sequence_statement.h"
#include "binder/statement/create_statement.h"
#include "binder/table_ref/bound_base_table.h"
#include "catalog/column.h"
#include "catalog/index.h"
#include "catalog/schema.h"
#include "catalog/table_info.h"
#include "datasource/table_datasource.h"
#include "storage/db_handle.h"
#include "storage/gstor/gstor_executor.h"
#include "storage/gstor/zekernel/common/cm_error.h"

class Catalog {
   public:
    explicit Catalog(std::string user, void *handle) : user_(user), handle_(handle) {}

    int CreateTable(const std::string &table_name, const std::vector<Column> &columns, const CreateStatement &stmt);

    auto GetTable(const std::string &schema_name, const std::string &table_name) const -> std::unique_ptr<TableInfo>;

    int CreateIndex(const std::string &table_name, const CreateIndexStatement &stmt);

    int AlterTable(const std::string &table_name, const AlterStatement &stmt);

    int CreateSequence(const CreateSequenceStatement &stmt);

    int64_t GetSequenceCurrVal(const std::string &seq_name) const;

    int64_t GetSequenceNextVal(const std::string &seq_name) const;

    auto IsSequenceExists(const std::string &seq_name) const -> bool;

    std::unique_ptr<TableDataSource> CreateTableDataSource(std::unique_ptr<BoundBaseTable> table_ref,
                                                           scan_action_t action, size_t table_idx) const;

    int CreateView(const std::string &viewName, const std::vector<Column> &columns, const std::string &query,
                   bool ignoreConflict);

    int forceCheckpoint();

    status_t CommentOn(exp_comment_def_t *def);

   public:
    db_handle_t *GetStorageHandle() const { return (db_handle_t *)handle_; }

    std::string GetUser() const { return user_; }
    void SetUser(const std::string& user) { user_ = user; }
    uint32_t GetUserID() const;
    uint32_t GetUserID(const std::string& user_name) const;
    std::string GetUserName(uint32_t uid) const;

    bool32 IsRole(const std::string& role_name) const;
    uint32_t GetRoleID(const std::string& role_name) const;

    bool32 CheckSysPrivilege(uint32 priv_id) const;
    bool32 CheckPrivilege(const std::string& objuser, const std::string& objname, object_type_t objtype, uint32 priv_id) const;

   private:
    std::string user_;
    void *handle_;  // db object

   public:
    bool need_lock_dc_ = false;

    // static
    static std::mutex dc_mutex_;
};
