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
 * bound_interval.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_interval.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#include "binder/bound_expression.h"
#include "common/hash_util.h"
#include "type/type_id.h"

class BoundInterval : public BoundExpression { 
   public:
    explicit BoundInterval(std::vector<Value>& vals) : BoundExpression(ExpressionType::INTERVAL), values(vals) {}

    uint32_t value_nr;
    std::vector<Value> values;

   public:
    std::string ToString() const override { return "BoundInterval"; }

    virtual hash_t Hash() const override {
        static const char *name = "BoundInterval";
        hash_t h = HashUtil::HashBytes((const char *)(&value_nr), sizeof(value_nr));
        h = HashUtil::CombineHash(h, HashUtil::HashBytes(name, strlen(name)));
        return h;
    }

    virtual auto Equals(const BoundExpression &other) const -> bool override {
        return false;
    }

    virtual LogicalType ReturnType() const override { return intarkdb::NewLogicalType(GS_TYPE_VARCHAR); }

    virtual bool HasSubQuery() const override { return false; }

    virtual bool HasAggregation() const override { return false; }

    virtual bool HasParameter() const override { return false; }

    virtual bool HasWindow() const override { return false; }
};
