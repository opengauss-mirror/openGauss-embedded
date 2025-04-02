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
* over_clause.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/over_clause.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <memory>
#include <vector>

#include "binder/bound_expression.h"
#include "binder/bound_sort.h"
#include "common/hash_util.h"

namespace intarkdb {
struct OverClause {
    OverClause() = default;
    auto Hash() const -> hash_t {
        hash_t h = HashUtil::HashBytes("over", sizeof("over"));
        for (auto &p : partition_by) {
            h = HashUtil::CombineHash(h, p->Hash());
        }
        for (auto &o : order_by) {
            h = HashUtil::CombineHash(h, o->Hash());
        }
        return h;
    }

    auto operator==(const OverClause &other) const -> bool {
        return partition_by == other.partition_by && order_by == other.order_by;
    }

    std::unique_ptr<OverClause> Copy() const {
        auto r = std::make_unique<OverClause>();
        for (auto &p : partition_by) {
            r->partition_by.push_back(p->Copy());
        }
        for (auto &o : order_by) {
            r->order_by.push_back(o->Copy());
        }
        return r;
    }

    std::vector<std::unique_ptr<BoundExpression>> partition_by;
    std::vector<std::unique_ptr<BoundSortItem>> order_by;
};
}  // namespace intarkdb
