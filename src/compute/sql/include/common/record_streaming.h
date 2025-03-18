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
 * record_streaming.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/record_streaming.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "common/record_iterator.h"
#include "planner/physical_plan/physical_plan.h"

class RecordStreaming : public RecordIterator {
   public:
    EXPORT_API RecordStreaming(const Schema& schema, PhysicalPlanPtr plan)
        : RecordIterator(RecordIteratorType::Streaming), schema_(schema), plan_{plan} {}

    EXPORT_API virtual ~RecordStreaming() {}

    EXPORT_API virtual uint32_t ColumnCount() const { return schema_.GetColumnInfos().size(); }

    EXPORT_API virtual const Schema& GetSchema() const { return schema_; }

   public:
    EXPORT_API virtual std::tuple<Record,bool> Next() {
        auto&& [r, _, eof] = plan_->Next();
        return {std::move(r), eof};
    }

   private:
    Schema schema_;
    PhysicalPlanPtr plan_;
};
