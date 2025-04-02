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
* bound_conjunctive.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/expressions/bound_conjunctive.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/bound_expression.h"
#include "cm_defs.h"

class BoundConjunctive : public BoundExpression {
   public:
    explicit BoundConjunctive(std::vector<std::unique_ptr<BoundExpression>>&& exprs)
        : BoundExpression(ExpressionType::CONJUNCTIVE), items(std::move(exprs)) {}

    std::string ToString() const override {
        std::string result;
        for (size_t i = 0; i < items.size(); i++) {
            result += items[i]->ToString();
            if (i != items.size() - 1) {
                result += " AND ";
            }
        }
        return result;
    }

    virtual auto GetName() const -> std::vector<std::string> override { return {ToString()}; }

    virtual bool HasAggregation() const override {
        for (auto& item : items) {
            if (item->HasAggregation()) {
                return true;
            }
        }
        return false;
    }

    virtual bool HasSubQuery() const override {
        for (auto& item : items) {
            if (item->HasSubQuery()) {
                return true;
            }
        }
        return false;
    }

    virtual bool HasParameter() const override {
        for (auto& item : items) {
            if (item->HasParameter()) {
                return true;
            }
        }
        return false;
    }

    virtual bool HasWindow() const override {
        for (auto& item : items) {
            if (item->HasWindow()) {
                return true;
            }
        }
        return false;
    }

    virtual auto Equals(const BoundExpression& other) const -> bool override {
        for (size_t i = 0; i < items.size(); i++) {
            if (!items[i]->Equals(*static_cast<const BoundConjunctive&>(other).items[i])) {
                return false;
            }
        }
        return true;
    }


    virtual LogicalType ReturnType() const override { return GS_TYPE_BOOLEAN; }

    virtual hash_t Hash() const override {
        hash_t h = 0;
        for (auto& item : items) {
            h = HashUtil::CombineHash(h, item->Hash());
        }
        return h;
    }

    virtual std::unique_ptr<BoundExpression> Copy() const override {
        std::vector<std::unique_ptr<BoundExpression>> new_items;
        for (auto& item : items) {
            new_items.push_back(item->Copy());
        }
        return std::make_unique<BoundConjunctive>(std::move(new_items));
    }

    size_t ItemCount() const { return items.size(); }

    std::vector<std::unique_ptr<BoundExpression>> items;
};
