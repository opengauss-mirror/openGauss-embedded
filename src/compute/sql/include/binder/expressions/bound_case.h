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
 * bound_case.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_case.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#include "binder/bound_expression.h"
#include "common/hash_util.h"
#include "type/type_id.h"

struct BoundCaseCheck {
	std::unique_ptr<BoundExpression> when_expr;
	std::unique_ptr<BoundExpression> then_expr;
};

class BoundCase : public BoundExpression {
   public:
    BoundCase() : BoundExpression(ExpressionType::CASE) {}

    std::vector<BoundCaseCheck> bound_case_checks;
	std::unique_ptr<BoundExpression> else_expr;

   public:
    std::string ToString() const override { return "BoundCase"; }

    virtual hash_t Hash() const override {
        static const char *name = "BoundCase";
        hash_t h = HashUtil::HashBytes(name, strlen(name));
        for (auto &check : bound_case_checks) {
            h = HashUtil::CombineHash(h, check.when_expr->Hash());
            h = HashUtil::CombineHash(h, check.then_expr->Hash());
        }
        if (else_expr) {
            h = HashUtil::CombineHash(h, else_expr->Hash());
        }
        return h;
    }

    virtual std::unique_ptr<BoundExpression> Copy() const override {
        auto copy = std::make_unique<BoundCase>();
        for (auto &check : bound_case_checks) {
            BoundCaseCheck new_check;
            new_check.when_expr = check.when_expr->Copy();
            new_check.then_expr = check.then_expr->Copy();
            copy->bound_case_checks.push_back(std::move(new_check));
        }
        if (else_expr) {
            copy->else_expr = else_expr->Copy();
        } else {
            copy->else_expr = nullptr;
        }
        return std::move(copy);
    }

    virtual auto Equals(const BoundExpression &other) const -> bool override {
        if (other.Type() != ExpressionType::CASE) {
            return false;
        }

        const auto& other_case = static_cast<const BoundCase&>(other);
        if (bound_case_checks.size() != other_case.bound_case_checks.size()) {
		    return false;
        }
        for (size_t i = 0; i < bound_case_checks.size(); i++) {
            if (!bound_case_checks[i].when_expr->Equals(*other_case.bound_case_checks[i].when_expr)) {
                return false;
            }
            if (!bound_case_checks[i].then_expr->Equals(*other_case.bound_case_checks[i].then_expr)) {
                return false;
            }
        }

        if (else_expr && other_case.else_expr) {
            if (!else_expr->Equals(*other_case.else_expr)) {
                return false;
            }
        } else if ((!else_expr && other_case.else_expr) || (else_expr && !other_case.else_expr)) {
            return false;
        }
        return true;
    }

    virtual LogicalType ReturnType() const override {
        if (bound_case_checks.size() > 0) {
            LogicalType the_first_type = bound_case_checks[0].then_expr->ReturnType();
            for (size_t i = 0; i < bound_case_checks.size(); i++) {
                auto the_next_type = bound_case_checks[i].then_expr->ReturnType();
                if (the_next_type.type != the_first_type.type) {
                    return intarkdb::NewLogicalType(GS_TYPE_VARCHAR);
                }
            }
            return the_first_type;
        }
        return intarkdb::NewLogicalType(GS_TYPE_NULL);
    }

    virtual bool HasSubQuery() const override { return false; }

    virtual bool HasAggregation() const override { return false; }

    virtual bool HasParameter() const override { return false; }

    virtual bool HasWindow() const override { return false; }
};
