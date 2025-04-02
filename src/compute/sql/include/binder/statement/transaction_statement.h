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
* transaction_statement.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/statement/transaction_statement.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <memory>
#include <vector>

#include "binder/bound_statement.h"

enum class TransactionType : uint8_t { INVALID, BEGIN_TRANSACTION, COMMIT, ROLLBACK };

class TransactionStatement : public BoundStatement {
   public:
    explicit TransactionStatement(TransactionType trans_type)
        : BoundStatement(StatementType::TRANSACTION_STATEMENT), type(trans_type) {}

    TransactionType type;

    auto ToString() const -> std::string override { return "TransactionStatement"; }
};
