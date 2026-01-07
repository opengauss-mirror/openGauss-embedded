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
 * planner.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/planner.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <stack>

#include "binder/bound_statement.h"
#include "binder/expressions/bound_position_ref_expr.h"
#include "binder/expressions/bound_sub_query.h"
#include "binder/statement/comment_statement.h"
#include "binder/statement/copy_statement.h"
#include "binder/statement/role_statement.h"
#include "binder/statement/delete_statement.h"
#include "binder/statement/drop_statement.h"
#include "binder/statement/insert_statement.h"
#include "binder/statement/select_statement.h"
#include "binder/statement/set_statement.h"
#include "binder/statement/show_statement.h"
#include "binder/statement/synonym_statement.h"
#include "binder/statement/transaction_statement.h"
#include "binder/statement/update_statement.h"
#include "binder/table_ref/bound_base_table.h"
#include "binder/table_ref/bound_join.h"
#include "binder/table_ref/bound_subquery.h"
#include "catalog/catalog.h"
#include "common/util.h"
#include "function/function.h"
#include "planner/expressions/column_param_expression.h"
#include "planner/expressions/expression.h"
#include "planner/logical_plan/logical_plan.h"
#include "planner/logical_plan/nested_loop_join_plan.h"
#include "planner/logical_plan/projection_plan.h"
#include "planner/physical_plan/physical_plan.h"

struct PlannerContext {
    std::unique_ptr<BoundExpression> where_clause;
};

// Planner take statement and translate it into Plan tree
class Planner {
   public:
    Planner(const Catalog& catalog) : catalog_(catalog) {}

    void PlanQuery(BoundStatement& statement);
    auto PlanSelect(SelectStatement& statement) -> LogicalPlanPtr;
    auto PlanWhereClause(SelectStatement& statement, LogicalPlanPtr& plan) -> LogicalPlanPtr;
    auto PlanGroupByClause(SelectStatement& statement, int origin_column_size, LogicalPlanPtr& plan) -> LogicalPlanPtr;
    auto PlanWindowClause(SelectStatement& statement, LogicalPlanPtr& plan) -> LogicalPlanPtr;
    auto PlanSetOpSelect(SelectStatement& statement) -> LogicalPlanPtr;
    auto PlanInsert(InsertStatement& statement) -> LogicalPlanPtr;
    auto PlanInsert(std::unique_ptr<BoundQuerySource> table_ref, std::unique_ptr<CreateStatement> create_stmt,
                    LogicalPlanPtr& plan) -> LogicalPlanPtr;
    auto PlanValues(SelectStatement& statement) -> LogicalPlanPtr;

    auto PlanUpdate(UpdateStatement& statement) -> LogicalPlanPtr;
    auto PlanDelete(DeleteStatement& statement) -> LogicalPlanPtr;
    auto PlanTransaction(void* handle, TransactionStatement& statement) -> LogicalPlanPtr;
    auto PlanDrop(DropStatement& statement) -> LogicalPlanPtr;
    auto PlanTableRef(SelectStatement& statement,
        std::unique_ptr<BoundQuerySource> table, scan_action_t action)-> LogicalPlanPtr;
    auto PlanSubqueryTableRef(BoundSubquery& subquery, scan_action_t action) -> LogicalPlanPtr;
    auto PlanJoinTableRef(SelectStatement& statement, BoundJoin& join, scan_action_t action) -> LogicalPlanPtr;

    auto PlanCommentOn(Catalog* catalog, CommentStatement& statement) -> LogicalPlanPtr;
    auto PlanShow(ShowStatement& statement) -> LogicalPlanPtr;
    auto PlanSet(SetStatement& statement) -> LogicalPlanPtr;
    auto PlanCreateRole(CreateRoleStatement& statement) -> LogicalPlanPtr;
    auto PlanAlterRole(AlterRoleStatement& statement) -> LogicalPlanPtr;
    auto PlanDropRole(DropRoleStatement& statement) -> LogicalPlanPtr;
    auto PlanGrantRole(GrantRoleStatement& statement) -> LogicalPlanPtr;
    auto PlanGrant(GrantStatement& statement) -> LogicalPlanPtr;

    auto PlanSynonym(SynonymStatement& statement) -> LogicalPlanPtr;

    // 子查询plan
    auto PlanSubqueryExprs(std::vector<std::unique_ptr<BoundExpression>>& exprs) -> void;
    auto PlanSubquery(BoundSubqueryExpr& expr) -> std::unique_ptr<BoundExpression>;
    void PlanSubqueries(std::unique_ptr<BoundExpression>* expr);
    auto PlanScalarSubqueryExprs(std::vector<std::unique_ptr<BoundExpression>>& exprs, LogicalPlanPtr plan)
        -> LogicalPlanPtr;
    auto PlanScalarSubqueryExpr(std::unique_ptr<BoundExpression>& expr,
                                std::vector<std::unique_ptr<BoundExpression>>& pullup_predicate) -> LogicalPlanPtr;

    auto CreatePhysicalExpression(BoundExpression& logical_expr, const LogicalPlanPtr& plan)
        -> std::unique_ptr<Expression>;

    auto CreatePhysicalPlan(const LogicalPlanPtr& plan) -> PhysicalPlanPtr;
    auto CreateProjectionExec(std::shared_ptr<ProjectionPlan> plan) -> PhysicalPlanPtr;
    auto CreateJoinExec(std::shared_ptr<NestedLoopJoinPlan>& plan) -> PhysicalPlanPtr;

    auto GetPrepareParams() const -> const std::vector<const ColumnParamExpression*>& { return prepare_params_cols_; }

    auto IsInSubqueryPlanning() const -> bool { return !subquery_planner_ctxs_.empty(); }
    auto EnterSbuqueryPlanning() -> void { subquery_planner_ctxs_.push_back(PlannerContext{}); }
    auto ExitSubqueryPlanning() -> void { subquery_planner_ctxs_.pop_back(); }

   private:
    const Catalog& catalog_;

    // for subquery plan
    std::vector<PlannerContext> subquery_planner_ctxs_;

    std::stack<LogicalPlanPtr> outer_plans;  // 子查询plan
    std::vector<const ColumnParamExpression*> params_cols;

    size_t table_idx_{0};

    // for prepare placeholder
    std::vector<const ColumnParamExpression*> prepare_params_cols_;
};

auto BinaryOpFactory(const std::string& op_name, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right,
                     const std::optional<intarkdb::Function>& func) -> std::unique_ptr<Expression>;

auto UnaryOpFactory(const std::string& op_name, std::unique_ptr<Expression> expr) -> std::unique_ptr<Expression>;

auto LikeOpFactory(const std::string& op_name, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right,
                   std::unique_ptr<Expression> escape) -> std::unique_ptr<Expression>;

auto FunctionFactory(const std::string& funcname, std::vector<std::unique_ptr<Expression>> args)
    -> std::unique_ptr<Expression>;
