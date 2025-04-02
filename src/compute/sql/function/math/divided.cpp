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
* divided.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/function/math/divided.cpp
*
* -------------------------------------------------------------------------
*/
#include "function/math/divided.h"

#include "type/operator/decimal_cast.h"
#include "type/type_id.h"
#include "type/value.h"

namespace intarkdb {

auto DividedSet::Register(FunctionContext& context) -> void {
    context.RegisterFunction("/", {{GS_TYPE_REAL, GS_TYPE_REAL}, GS_TYPE_REAL}, [](const std::vector<Value>& args) {
        auto left = args[0].GetCastAs<double>();
        auto right = args[1].GetCastAs<double>();
        if (right == 0) {
            return Value(GS_TYPE_REAL);
        }
        return ValueFactory::ValueDouble(left / right);
    });
}

}  // namespace intarkdb
