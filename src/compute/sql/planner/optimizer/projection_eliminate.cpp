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
* projection_eliminate.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/optimizer/projection_eliminate.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/optimizer/projection_eliminate.h"

namespace intarkdb {

ProjectionEliminate::ProjectionEliminate(Optimizer& optimizer) : optimizer_(optimizer) {}

LogicalPlanPtr ProjectionEliminate::Rewrite(LogicalPlanPtr& op) {
    GS_LOG_RUN_INF("projection eliminate op type = %d \n", static_cast<int>(op->Type()));
    switch (op->Type()) {
        case LogicalPlanType::Projection:
            return CheckProjectionChild(op);
        default:
            return Default(op);
    }
}

static auto EqualSchema(const Schema& schema1, const Schema& schema2) {
    const auto& column_list1 = schema1.GetColumns();
    const auto& column_list2 = schema2.GetColumns();

    if (column_list1.size() != column_list2.size()) {
        return false;
    }

    for (size_t i = 0; i < column_list1.size(); i++) {
        if (column_list1[i].Name() != column_list2[i].Name()) {
            return false;
        }
        if (!(column_list1[i].GetLogicalType() == column_list2[i].GetLogicalType())) {
            return false;
        }
    }
    return true;
}

LogicalPlanPtr ProjectionEliminate::CheckProjectionChild(LogicalPlanPtr& op) {
    auto child = op->Children()[0];
    // get schema from child
    const auto& child_schema = child->GetSchema();
    const auto& schema = op->GetSchema();

    if (EqualSchema(child_schema, schema)) {
        // eliminate projection
        return Rewrite(child);
    }
    return Default(op);
}

LogicalPlanPtr ProjectionEliminate::Default(LogicalPlanPtr& op) {
    std::vector<LogicalPlanPtr> new_children;
    auto children = op->Children();
    new_children.reserve(children.size());
    
    for (auto& child : children) {
        new_children.push_back( Rewrite(child) );
    }
    op->SetChildren(new_children);
    return op;
}

}  // namespace intarkdb
