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
* insert_exec.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/physical_plan/insert_exec.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <map>
#include <unordered_set>

#include "binder/statement/insert_statement.h"
#include "datasource/table_datasource.h"
#include "planner/expressions/expression.h"
#include "planner/physical_plan/physical_plan.h"

class Column;

class InsertExec : public PhysicalPlan {
   public:
    InsertExec(PhysicalPlanPtr child, std::unique_ptr<TableDataSource> data_source,
               const std::vector<Column>& bound_columns,
               const std::vector<Column>& unbound_defaults,
               const std::vector<Column>& bound_defaults,
               InsertOnConflictActionAlias opt_or_action)
        : child_(child),
          source_(std::move(data_source)),
          bound_columns_(bound_columns),
          unbound_defaults_(unbound_defaults),
          bound_defaults_(bound_defaults),
          opt_or_action_(opt_or_action) {}
    virtual Schema GetSchema() const override;

    virtual auto Execute() const -> RecordBatch override;
    void Execute(RecordBatch &rb_out) override;

    virtual std::vector<PhysicalPlanPtr> Children() const override;

    virtual std::string ToString() const override;

    virtual void SetNeedResultSetEx(bool need) override { is_need_insert_result_ = need; }

    virtual bool NeedResultSetEx() override { return is_need_insert_result_; }

    void SetAutoCommit(bool auto_commit) override { is_auto_commit_ = auto_commit; }
    bool IsAutoCommit() override { return is_auto_commit_; }

    // useless
    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override { return {}; }

    void GetTableDef(const TableInfo &table_info, const exp_table_meta &meta_info) ;

    void MakeSchema(RecordBatch &rb_out) ;

    void GetPartitionKey() ;

    void GetBoundValue(std::vector<Value> &row_in, std::vector<Column> &column_list, std::vector<Value> &autoincrement_list,
      std::vector<Value>& values) ;

    void GetDefaultValue(std::vector<Column> &column_list, std::vector<Value> &default_value_list,
      std::vector<Value>& values) ;
  
    void GetAutoIncrement(std::vector<Column> &column_list, std::vector<Value> &autoincrement_list,
      std::vector<Value>& values, Value &bound_value) ;

    void GroupRowsByKey(std::vector<Column> &&column_list,
      std::map<std::string, std::unique_ptr<std::vector<std::vector<Column>>>> &insert_rows_map);
    void Insert(const std::map<std::string, std::unique_ptr<std::vector<std::vector<Column>>>> &insert_rows_map) ;

   private:
    PhysicalPlanPtr child_;
    std::unique_ptr<TableDataSource> source_;
    std::vector<Column> bound_columns_;
    mutable std::vector<Column> unbound_defaults_;
    std::vector<Column> bound_defaults_;
    InsertOnConflictActionAlias opt_or_action_ = ONCONFLICT_ALIAS_NONE;

    std::string m_table_name;

    // autoincrement
    bool32 m_auto_increment = GS_FALSE;
    bool32 m_autoincrement_col_is_bound = GS_FALSE;
    exp_column_def_t m_autoincrement_col_;

    // part
    bool32 m_is_parted = GS_FALSE;
    bool32 m_auto_addpart = GS_FALSE;
    bool32 m_is_crosspart = GS_FALSE;
    int32_t m_part_key_col_slot = -1;
    std::string m_part_interval;
    part_type_t m_part_type;

    std::string m_part_key_key_ = "-1";
    Value m_part_key_value_;

    std::unordered_map<std::string, uint32_t> m_part_name_map_;

   private:
    bool is_need_insert_result_ = false;
    int64_t last_insert_rowid {0};

    bool is_auto_commit_ = false;

    bool32 is_insert_or_ignore = GS_FALSE;
};
