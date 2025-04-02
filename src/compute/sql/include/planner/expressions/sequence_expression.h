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
 * sequence_expression.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/expressions/sequence_expression.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "catalog/catalog.h"
#include "common/exception.h"
#include "planner/expressions/expression.h"

class SequenceExpression : public Expression {
   public:
    SequenceExpression(const std::string& funcname, std::unique_ptr<Expression> sequence_name, const Catalog& catalog)
        : Expression(GS_TYPE_BIGINT),
          funcname_(funcname),
          sequence_name_(std::move(sequence_name)),
          catalog_(catalog) {}

    virtual auto Evaluate(const Record& record) const -> Value {
        if (funcname_ == "currval") {
            auto val = sequence_name_->Evaluate(record);
            if (val.IsNull()) {
                return ValueFactory::ValueNull();
            }
            int64_t cur_val = catalog_.GetSequenceCurrVal(val.GetCastAs<std::string>());
            return ValueFactory::ValueBigInt(cur_val);
        } else if (funcname_ == "nextval") {
            auto val = sequence_name_->Evaluate(record);
            if (val.IsNull()) {
                return ValueFactory::ValueNull();
            }
            int64_t next_val = catalog_.GetSequenceNextVal(val.GetCastAs<std::string>());
            return ValueFactory::ValueBigInt(next_val);
        } else {
            throw intarkdb::Exception(ExceptionType::EXECUTOR, fmt::format("unkown funcname={}", funcname_));
        }
    }

    virtual auto ToString() const -> std::string { return funcname_; }

    virtual auto Reset() -> void { sequence_name_->Reset(); }

   private:
    std::string funcname_;
    std::unique_ptr<Expression> sequence_name_;
    const Catalog& catalog_;
};
