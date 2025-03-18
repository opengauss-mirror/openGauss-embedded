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
 * table_datasource.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/datasource/table_datasource.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <tuple>

#include "binder/table_ref/bound_base_table.h"
#include "catalog/schema.h"
#include "datasource/datasource.h"
#include "planner/expressions/column_value_expression.h"
#include "planner/expressions/expression.h"
#include "planner/logical_plan/logical_plan.h"
#include "storage/gstor/gstor_executor.h"

enum class IndexOperType : uint8_t {
    CMP_GT,
    CMP_LT,
    CMP_GE,
    CMP_LE,
    CMP_EQ,
    CMP_OPEN_INTERVAL,
    CMP_CLOSE_INTERVAL,
    CMP_FRONT_OPEN_END_CLOSE_INTERVAL,
    CMP_FRONT_CLOSE_END_OPENE_INTERVAL,
    CMP_IN,
};

struct IndexMatchColumnInfo {
    uint16_t col_id;
    GStorDataType data_type;
    Expression* upper = nullptr;
    Expression* lower = nullptr;
};

struct IndexMatchInfo {
    bool use_index{false};
    uint32_t index_slot{GS_INVALID_ID32};
    std::vector<IndexMatchColumnInfo> index_columns;
    int32_t total_index_column{0};  // 索引所有字段数量
};

class TableDataSource : public DataSource {
   public:
    TableDataSource(void* handle, std::unique_ptr<BoundBaseTable> table, scan_action_t action, size_t idx)
        : handle_(handle), table_(std::move(table)), first_(true), action_(action), idx_(idx) {
        // 构建schema ，使列名都包含表名
        const auto& columns = table_->GetTableInfo().columns;
        std::vector<SchemaColumnInfo> schema_columns;
        size_t slot = 0;
        for (auto& col : columns) {
            SchemaColumnInfo info;
            info.col_name = std::vector<std::string>{table_->GetTableNameOrAlias(), col.Name()};
            info.col_type = intarkdb::NewLogicalType(col.GetRaw());
            info.slot = slot++;
            schema_columns.push_back(info);
        }
        row_column_list_ = std::make_unique<exp_column_def_t[]>(columns.size());
        schema_ = Schema(std::move(schema_columns));

        lock_clause_.is_select_for_update = GS_FALSE;
    }

    void* GetStorageHandle() { return handle_; }

    virtual const Schema& GetSchema() const override;

    status_t OpenStorageTable(std::string table_name);
    virtual void Insert(const std::vector<Column>& columns);
    void BatchInsert(std::vector<std::vector<Column>>& insert_rows,
                    const std::string &part_name, uint32_t part_no = 0,
                    bool32 is_ignore = GS_FALSE);
    int32_t AutoAddPartition(std::string table_name, std::string part_key, part_type_t part_type);
    bool ReflashPartition(std::string table_name, std::string part_name, uint32_t& part_no);
    status_t AutoIncrementNextValue(uint32_t slot, int64_t* nextval);
    status_t AlterIncrementValue(uint32_t slot, int64_t nextval);
    int64_t SeqNextValue(std::string seq_name);
    int64_t SeqCurrValue(std::string seq_name);

    virtual void Delete();

    virtual void Update(int column_count, exp_column_def_t* column_list);

    virtual std::tuple<intarkdb::RowContainerPtr, knl_cursor_t*, bool> Next() override;

    std::string GetTableName() const { return table_->GetTableNameOrAlias(); }

    int64_t Rows() const;  // 返回表的总行数

    const BoundBaseTable& GetTableRef() const { return *table_; }

    void SetIndexInfo(IndexMatchInfo& info) { index_bind_data_ = info; }

    auto Init() -> void;

    auto IsParitionTable() const -> bool;

    auto IsParitionIndex(uint32_t idx_slot) const -> bool;

    auto NeedParitionScan() const -> bool;

    auto PartitionTableNext() -> std::tuple<intarkdb::RowContainerPtr, knl_cursor_t*, bool>;

    auto CommonTableNext() -> std::tuple<intarkdb::RowContainerPtr, knl_cursor_t*, bool>;

    void Reset() {
        first_ = true;
        scan_count_ = 0;
        scan_partition_no_ = 0;
    }

    std::string GetUser() { return user_; }
    void SetUser(std::string user) { user_ = user; }

   public:
    // select for update
    lock_clause_t lock_clause_;

   private:
    void* handle_;
    std::unique_ptr<BoundBaseTable> table_;
    bool first_;
    std::vector<condition_def_t> conditions_;
    std::vector<Value> cache_values_;
    IndexMatchInfo index_bind_data_;
    uint32_t idx_slot_{GS_INVALID_ID32};
    int index_column_count_{0};
    int condition_count_{0};
    int scan_count_{0};  // 扫描行数
    scan_action_t action_;
    Schema schema_;
    std::unique_ptr<exp_column_def_t[]> row_column_list_;

    // for partition table
    uint64_t scan_partition_no_{0};
    size_t idx_{0};

    // user
    std::string user_;
};
