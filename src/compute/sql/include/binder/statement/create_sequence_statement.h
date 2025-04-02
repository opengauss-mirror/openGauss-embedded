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
* create_sequence_statement.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/statement/create_sequence_statement.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/bound_statement.h"

enum class SequenceInfo : uint8_t {
    // Sequence start
    SEQ_START,
    // Sequence increment
    SEQ_INC,
    // Sequence minimum value
    SEQ_MIN,
    // Sequence maximum value
    SEQ_MAX,
    // Sequence cycle option
    SEQ_CYCLE,
    // Sequence owner table
    SEQ_OWN
};

class CreateSequenceStatement : public BoundStatement {
   public:
    explicit CreateSequenceStatement()
        : BoundStatement(StatementType::SEQUENCE_STATEMENT),
          name(std::string()),
          increment(1),
          min_value(1),
          max_value(NumericLimits<int64_t>::Maximum()),
          start_value(1),
          cycle(false) {}

    auto ToString() const -> std::string override { return "CreateSequenceStatement"; }

    //! schema name
    std::string schema_name;
    //! Sequence name to create
    std::string name;
    //! The increment value
    int64_t increment;
    //! The minimum value of the sequence
    int64_t min_value;
    //! The maximum value of the sequence
    int64_t max_value;
    //! The start value of the sequence
    int64_t start_value;
    //! Whether or not the sequence cycles
    bool cycle;
    // conflict policy
    bool ignore_conflict{false};
};
