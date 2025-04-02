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
 * binder.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/binder.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <assert.h>

#include <memory>
#include <optional>
#include <set>
#include <vector>

#include "binder/binder_context.h"
#include "binder/bound_expression.h"
#include "binder/bound_sort.h"
#include "binder/bound_statement.h"
#include "binder/expressions/bound_alias.h"
#include "binder/expressions/bound_case.h"
#include "binder/expressions/bound_column_def.h"
#include "binder/expressions/bound_interval.h"
#include "binder/expressions/bound_parameter.h"
#include "binder/statement/alter_statement.h"
#include "binder/statement/checkpoint_statement.h"
#include "binder/statement/comment_statement.h"
#include "binder/statement/constraint.h"
#include "binder/statement/copy_statement.h"
#include "binder/statement/create_index_statement.h"
#include "binder/statement/create_sequence_statement.h"
#include "binder/statement/create_statement.h"
#include "binder/statement/create_view.h"
#include "binder/statement/ctas_statement.h"
#include "binder/statement/delete_statement.h"
#include "binder/statement/drop_statement.h"
#include "binder/statement/explain_statement.h"
#include "binder/statement/insert_statement.h"
#include "binder/statement/role_statement.h"
#include "binder/statement/select_statement.h"
#include "binder/statement/set_statement.h"
#include "binder/statement/show_statement.h"
#include "binder/statement/synonym_statement.h"
#include "binder/statement/transaction_statement.h"
#include "binder/statement/update_statement.h"
#include "binder/table_ref/bound_base_table.h"
#include "binder/table_ref/bound_join.h"
#include "binder/table_ref/bound_subquery.h"
#include "binder/over_clause.h"
#include "catalog/catalog.h"
#include "nodes/parsenodes.hpp"
#include "nodes/pg_list.hpp"
#include "pg_definitions.hpp"
#include "postgres_parser.hpp"

static const std::string RETENTION_DEFAULT = "7d";

constexpr int32_t INVALID_IDX = INT32_MAX;

class Binder {
   public:
    Binder(const Catalog &catalog, duckdb::PostgresParser &parse)
        : catalog_(catalog), parser_(parse), parent_binder_(nullptr) {}
    Binder(Binder *parent_binder)
        : catalog_(parent_binder->catalog_),
          parser_(parent_binder->parser_),
          parent_binder_(parent_binder),
          user_(parent_binder->user_),
          is_system_command_(parent_binder->is_system_command_) {}
    ~Binder() {}

    // 解析 sql query 语句 为 statement序列
    void ParseSQL(const std::string &query);

    void SaveParseResult(duckdb_libpgquery::PGList *tree);

    std::string GetUser() { return user_; }
    void SetUser(std::string user) { user_ = user; }

    // bind statement
    auto BindSQLStmt(duckdb_libpgquery::PGNode *stmt) -> std::unique_ptr<BoundStatement>;
    auto BindColumnRef(duckdb_libpgquery::PGColumnRef *node) -> std::unique_ptr<BoundExpression>;
    auto BindColumnRef(const std::vector<std::string>& col_name,bool root) -> std::unique_ptr<BoundExpression>;
    auto BindColumnRefAlias(const std::vector<std::string>& column_name,const std::unordered_map<std::string, AliasIdx>& alias_binding) -> std::unique_ptr<BoundExpression>;
    auto BindCorrelatedColumn(const std::vector<std::string>& col_name , Binder* binder,BinderContext& ctx,bool root) -> std::unique_ptr<BoundExpression>;

    auto BindColumnWtihDef(duckdb_libpgquery::PGColumnDef &cdef, uint16_t slot, std::vector<Constraint> &constraints)
        -> Column;
    auto BindCreateColumnList(duckdb_libpgquery::PGList *tableElts, const std::string &table_name,
                              std::vector<Column> &columns, std::vector<Constraint> &constraints) -> void;

    auto BindCreateStmt(duckdb_libpgquery::PGCreateStmt *pg_stmt) -> std::unique_ptr<CreateStatement>;
    auto BindCreateIndex(duckdb_libpgquery::PGIndexStmt *pg_stmt) -> std::unique_ptr<CreateIndexStatement>;
    auto BindSelectStmt(duckdb_libpgquery::PGSelectStmt *pg_stmt) -> std::unique_ptr<SelectStatement>;
    auto BindSelectNoSetOp(duckdb_libpgquery::PGSelectStmt *pg_stmt) -> std::unique_ptr<SelectStatement>;
    auto BindSelectSetOp(duckdb_libpgquery::PGSelectStmt *pg_stmt) -> std::unique_ptr<SelectStatement>;

    auto BindDistinctOnList(SelectStatement &stmt, duckdb_libpgquery::PGList *distinct_clause) -> void;
    auto BindFromClause(duckdb_libpgquery::PGList *list) -> std::unique_ptr<BoundTableRef>;
    auto BindTableRef(const duckdb_libpgquery::PGNode &node) -> std::unique_ptr<BoundTableRef>;
    auto BindRangeVarTableRef(const duckdb_libpgquery::PGRangeVar &table_ref, bool is_select = true)
        -> std::unique_ptr<BoundTableRef>;
    auto BindJoinTableRef(const duckdb_libpgquery::PGJoinExpr &node) -> std::unique_ptr<BoundTableRef>;
    auto BindRangeSubselectTableRef(const duckdb_libpgquery::PGRangeSubselect &node) -> std::unique_ptr<BoundTableRef>;
    auto BindBaseTableRef(const std::string &schema_name, const std::string &table_name,
                          std::optional<std::string> &&alias, bool if_exists = false)
        -> std::unique_ptr<BoundBaseTable>;
    auto BindValueList(duckdb_libpgquery::PGList *list, std::vector<std::unique_ptr<BoundExpression>> &select_list)
        -> std::unique_ptr<BoundTableRef>;
    auto BindSubqueryTableRef(duckdb_libpgquery::PGSelectStmt *node, const std::string &sub_query_alias)
        -> std::unique_ptr<BoundSubquery>;

    auto BindTableAllColumns(const std::string &table_name) -> std::vector<std::unique_ptr<BoundExpression>>;
    auto BindSelectListExprs(duckdb_libpgquery::PGList *list) -> std::vector<std::unique_ptr<BoundExpression>>;
    auto BindAllColumnRefs(const char *expect_relation_name) -> std::vector<std::unique_ptr<BoundExpression>>;
    auto BindExprResTarget(duckdb_libpgquery::PGResTarget *root, int depth) -> std::unique_ptr<BoundExpression>;
    auto BindStarExpr(duckdb_libpgquery::PGAStar *node) -> std::unique_ptr<BoundExpression>;
    auto BindColumnRefFromTableBinding(const std::vector<std::string> &col_name, bool root = true)
        -> std::unique_ptr<BoundExpression>;

    auto BindWhereClause(duckdb_libpgquery::PGNode *root) -> std::unique_ptr<BoundExpression>;

    auto BindValue(duckdb_libpgquery::PGValue *node) -> std::unique_ptr<BoundExpression>;
    auto BindConstanExpr(duckdb_libpgquery::PGAConst *node) -> std::unique_ptr<BoundExpression>;
    auto BindGroupByClause(duckdb_libpgquery::PGList *list) -> std::vector<std::unique_ptr<BoundExpression>>;
    auto BindGroupByExpression(duckdb_libpgquery::PGNode *node) -> std::unique_ptr<BoundExpression>;
    auto BindHavingClause(duckdb_libpgquery::PGNode *root) -> std::unique_ptr<BoundExpression>;
    auto BindLimitClause(duckdb_libpgquery::PGNode *limit, duckdb_libpgquery::PGNode *offset) -> std::unique_ptr<LimitClause>;
    auto BindLimitOffset(duckdb_libpgquery::PGNode *root) -> std::unique_ptr<BoundExpression>;
    auto BindLimitCount(duckdb_libpgquery::PGNode *root) -> std::unique_ptr<BoundExpression>;
    auto BindSortItems(duckdb_libpgquery::PGList *list, const std::vector<std::unique_ptr<BoundExpression>> &)
        -> std::vector<std::unique_ptr<BoundSortItem>>;
    auto BindSortExpression(duckdb_libpgquery::PGNode *node,
                            const std::vector<std::unique_ptr<BoundExpression>> &select_list)
        -> std::unique_ptr<BoundExpression>;
    auto BindInterval(duckdb_libpgquery::PGIntervalConstant *node) -> std::unique_ptr<BoundExpression>;
    auto BindCase(duckdb_libpgquery::PGCaseExpr *root, int depth) -> std::unique_ptr<BoundExpression>;
#ifdef ENABLE_PG_QUERY
    auto BindLockingClause(duckdb_libpgquery::PGList *list) -> std::unique_ptr<LockClause>;
#endif
    auto BindOverClause(const struct duckdb_libpgquery::PGWindowDef& over) -> std::unique_ptr<intarkdb::OverClause>;

    std::unique_ptr<BoundExpression> BindExpression(duckdb_libpgquery::PGNode *node,int depth);
    std::vector<std::unique_ptr<BoundExpression>> BindExpressionList(duckdb_libpgquery::PGList *list, int depth);
    std::unique_ptr<BoundExpression> BindAExpr(duckdb_libpgquery::PGAExpr *root, int depth);
    std::unique_ptr<BoundExpression> BindFuncExpression(duckdb_libpgquery::PGFuncCall *root, int depth);

    auto BindInExpr(duckdb_libpgquery::PGAExpr *root, int depth) -> std::unique_ptr<BoundExpression>;
    auto BindBoolExpr(duckdb_libpgquery::PGBoolExpr *root, int depth) -> std::unique_ptr<BoundExpression>;
    auto BindNullTest(duckdb_libpgquery::PGNullTest *root, int depth) -> std::unique_ptr<BoundExpression>;
    auto BindTypeCast(duckdb_libpgquery::PGTypeCast *root, int depth) -> std::unique_ptr<BoundExpression>;

    auto BindSubQueryExpr(duckdb_libpgquery::PGSubLink *root) -> std::unique_ptr<BoundExpression>;

    auto BindInsertStmt(duckdb_libpgquery::PGInsertStmt *pg_stmt) -> std::unique_ptr<InsertStatement>;

    auto BindTransaction(duckdb_libpgquery::PGTransactionStmt *pg_stmt) -> std::unique_ptr<TransactionStatement>;

    auto BindDropStmt(duckdb_libpgquery::PGDropStmt *stmt) -> std::unique_ptr<DropStatement>;

    auto BindSequence(duckdb_libpgquery::PGCreateSeqStmt *stmt) -> std::unique_ptr<CreateSequenceStatement>;

    auto BindDeleteStmt(duckdb_libpgquery::PGDeleteStmt *stmt) -> std::unique_ptr<DeleteStatement>;

    auto BindUpdateStmt(duckdb_libpgquery::PGUpdateStmt *stmt) -> std::unique_ptr<UpdateStatement>;
    auto BindUpdateItems(duckdb_libpgquery::PGList *target_list, const BoundBaseTable &table)
        -> std::vector<UpdateSetItem>;

    auto BindVariableSet(duckdb_libpgquery::PGVariableSetStmt *stmt) -> std::unique_ptr<SetStatement>;

    static auto ConvertNodeTagToString(duckdb_libpgquery::PGNodeTag type) -> std::string;

    const std::vector<duckdb_libpgquery::PGNode *> &GetStatementNodes() const { return pg_statements_; }

    auto BindName(const std::string &name) -> void;
    // 绑定单列约束
    auto BindSingleColConstraint(duckdb_libpgquery::PGListCell *cell, Column &col) -> std::unique_ptr<Constraint>;
    // 绑定多列约束
    auto BindMultiColConstraint(const duckdb_libpgquery::PGConstraint &constraint) -> Constraint;
    // 绑定约束列表
    auto BindSingleColConstraintList(duckdb_libpgquery::PGList *list, Column &column,
                                     std::vector<Constraint> &constraints) -> void;
    auto BoundExpressionToDefaultValue(BoundExpression &expr, Column &column) -> std::vector<uint8_t>;

    auto BindAlter(duckdb_libpgquery::PGAlterTableStmt *stmt) -> std::unique_ptr<AlterStatement>;
    auto BindRename(duckdb_libpgquery::PGRenameStmt *stmt) -> std::unique_ptr<AlterStatement>;
    auto BindDefault(duckdb_libpgquery::PGNode *node) -> std::unique_ptr<BoundExpression>;
    auto BindComment(duckdb_libpgquery::PGNode *node) -> std::string;
    auto BindVariableShow(duckdb_libpgquery::PGVariableShowStmt *stmt) -> std::unique_ptr<ShowStatement>;
    auto BindShowDataBase() -> std::unique_ptr<ShowStatement>;
    auto BindShowTables() -> std::unique_ptr<ShowStatement>;
    auto BindShowSpecificTable(const std::string &lname) -> std::unique_ptr<ShowStatement>;
    auto BindShowAll(const std::string &lname) -> std::unique_ptr<ShowStatement>;
    auto BindShowCreateTable(const std::string &lname) -> std::unique_ptr<ShowStatement>;
    auto BindShowVariables(const std::string &lname) -> std::unique_ptr<ShowStatement>;

    auto BindCtas(duckdb_libpgquery::PGCreateTableAsStmt *stmt) -> std::unique_ptr<CtasStatement>;
    auto BindCreateViewStmt(duckdb_libpgquery::PGViewStmt *stmt) -> std::unique_ptr<CreateViewStatement>;

    auto BindCopyOptions(CopyInfo &info, duckdb_libpgquery::PGList *options) -> void;
    auto BindCopy(duckdb_libpgquery::PGCopyStmt *stmt) -> std::unique_ptr<CopyStatement>;
    auto BindCopyFrom(const duckdb_libpgquery::PGCopyStmt &stmt, CopyInfo &info) -> void;
    auto BindCopyTo(const duckdb_libpgquery::PGCopyStmt &stmt, CopyInfo &info) -> std::unique_ptr<SelectStatement>;
    auto BindCopySelectList(const duckdb_libpgquery::PGCopyStmt &stmt, const BoundBaseTable &table)
        -> std::vector<std::unique_ptr<BoundExpression>>;
    auto BindCopyOptionWithArg(const std::string &option_name, CopyInfo &info, duckdb_libpgquery::PGDefElem *def_elem)
        -> void;
    auto BindCopyOptionWithNoArg(const std::string &option_name, CopyInfo &info) -> void;

    auto BindCheckPoint(duckdb_libpgquery::PGCheckPointStmt *stmt) -> std::unique_ptr<CheckpointStatement>;
    auto BindExplainStmt(duckdb_libpgquery::PGExplainStmt *stmt) -> std::unique_ptr<ExplainStatement>;
    auto BindCommentOn(duckdb_libpgquery::PGCommentStmt *stmt) -> std::unique_ptr<CommentStatement>;
    
    auto BindCreateRole(duckdb_libpgquery::PGCreateRoleStmt *stmt) -> std::unique_ptr<CreateRoleStatement>;
    auto BindDropRole(duckdb_libpgquery::PGDropRoleStmt *stmt) -> std::unique_ptr<DropRoleStatement>;
    auto BindGrantRole(duckdb_libpgquery::PGGrantRoleStmt *stmt) -> std::unique_ptr<GrantRoleStatement>;
    auto BindGrant(duckdb_libpgquery::PGGrantStmt *stmt) -> std::unique_ptr<GrantStatement>;
    auto BindAlterRole(duckdb_libpgquery::PGAlterRoleStmt *stmt) -> std::unique_ptr<AlterRoleStatement>;

    auto BindCreateSynonym(duckdb_libpgquery::PGSynonymStmt *stmt) -> std::unique_ptr<SynonymStatement>;

    // 记录SQL Stmt中涉及的表名
    auto AddRelatedTables(const std::string &name, bool is_timescale, bool is_modified) -> void;

    auto GetNextUniversalID() -> int {
        auto root_binder = RootBinder();
        return root_binder->universal_id_++;
    }

    auto ParamCount() -> uint32_t {
        auto root_binder = RootBinder();
        return root_binder->n_param_;
    }

    auto SetParamCount(uint32_t num) -> void {
        auto root_binder = RootBinder();
        root_binder->n_param_ = num;
    }

    auto BindParamRef(duckdb_libpgquery::PGParamRef *node) -> std::unique_ptr<BoundExpression>;

   private:
    auto IsLikeFunction(const std::string &name) -> bool;
    auto IsLikeExpression(duckdb_libpgquery::PGAExpr_Kind kind) -> bool;

    auto RootBinder() -> Binder * {
        if (parent_binder_ == nullptr) {
            return this;
        }
        return parent_binder_->RootBinder();
    }

    auto CheckTablePrivilege(const TableInfo &table_info) -> void;

   private:
    const Catalog &catalog_;
    duckdb::PostgresParser &parser_;

    std::vector<duckdb_libpgquery::PGNode *> pg_statements_;  // save parse result
    int universal_id_{0};
    std::unordered_map<std::string, std::unique_ptr<BoundExpression>> alias_map_;

    uint32_t n_param_ = 0;

    Binder *parent_binder_ = nullptr;

    bool check_column_exist_{true};  // FIXME: 应该显示在BindExpression函数中指定

    // user
    std::string user_;
    bool is_system_command_{false};

   public:
    BinderContext ctx;

    int32_t bind_location;

    StatementProperty stmt_props;
};

inline auto Binder::IsLikeFunction(const std::string &funcname) -> bool {
    // function_name is low case
    return funcname == "like_escape" || funcname == "ilike_escape" || funcname == "not_like_escape" ||
           funcname == "not_ilike_escape";
}

inline auto Binder::IsLikeExpression(duckdb_libpgquery::PGAExpr_Kind kind) -> bool {
    return kind == duckdb_libpgquery::PG_AEXPR_LIKE || kind == duckdb_libpgquery::PG_AEXPR_ILIKE;
}
