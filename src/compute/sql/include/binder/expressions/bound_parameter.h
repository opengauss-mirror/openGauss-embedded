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
 * bound_parameter.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_parameter.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#include "binder/bound_expression.h"
#include "common/hash_util.h"
#include "type/type_id.h"

class BoundParameter : public BoundExpression {
   public:
    BoundParameter() : BoundExpression(ExpressionType::BOUND_PARAM){};

    uint32_t parameter_nr;

   public:
    std::string ToString() const override { return "BoundParameter"; }

    virtual hash_t Hash() const override {
        static const char *name = "BoundParameter";
        hash_t h = HashUtil::HashBytes((const char *)(&parameter_nr), sizeof(parameter_nr));
        h = HashUtil::CombineHash(h, HashUtil::HashBytes(name, strlen(name)));
        return h;
    }

    virtual std::unique_ptr<BoundExpression> Copy() const override {
        auto copy = std::make_unique<BoundParameter>();
        copy->parameter_nr = parameter_nr;
        return copy;
    }

    virtual auto Equals(const BoundExpression &other) const -> bool override {
        if (other.Type() != ExpressionType::BOUND_PARAM) {
            return false;
        }
        auto &other_param = static_cast<const BoundParameter &>(other);
        return parameter_nr == other_param.parameter_nr;
    }

    virtual LogicalType ReturnType() const override { return intarkdb::NewLogicalType(GS_TYPE_PARAM); }

    virtual bool HasSubQuery() const override { return false; }

    virtual bool HasAggregation() const override { return false; }

    virtual bool HasParameter() const override { return true; }

    virtual bool HasWindow() const override { return false; }
};
