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
 * bound_join.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/table_ref/bound_join.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>

#include <cstdint>

#include "binder/bound_expression.h"
#include "binder/bound_table_ref.h"
#include "common/join_type.h"

class BoundJoin : public BoundTableRef {
   public:
    explicit BoundJoin(JoinType type, std::unique_ptr<BoundTableRef> l, std::unique_ptr<BoundTableRef> r,
                       std::unique_ptr<BoundExpression> cond)
        : BoundTableRef(DataSourceType::JOIN_RESULT),
          left(std::move(l)),
          right(std::move(r)),
          join_type(type),
          on_condition(std::move(cond)) {}

    auto ToString() const -> std::string override {
        return fmt::format("Join {{ join type={}, left={}, right={}, condi={} }}", join_type, left->ToString(),
                           right->ToString(), on_condition);
    }

    std::unique_ptr<BoundTableRef> left;

    std::unique_ptr<BoundTableRef> right;

    JoinType join_type;

    std::unique_ptr<BoundExpression> on_condition;
};
