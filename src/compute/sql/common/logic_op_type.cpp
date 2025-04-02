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
* logic_op_type.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/common/logic_op_type.cpp
*
* -------------------------------------------------------------------------
*/
#include "common/logic_op_type.h"

#include "common/string_util.h"

const char* const OR_FUNC_NAME = "or";
const char* const AND_FUNC_NAME = "and";
const char* const NOT_FUNC_NAME = "!";

namespace intarkdb {

bool IsBinaryLogicalOp(const std::string& op_name) {
    return intarkdb::StringUtil::IsEqualIgnoreCase(op_name, AND_FUNC_NAME) ||
           intarkdb::StringUtil::IsEqualIgnoreCase(op_name, OR_FUNC_NAME);
}

LogicOpType ToLogicOpType(const std::string& op_name) {
    if (intarkdb::StringUtil::IsEqualIgnoreCase(op_name, AND_FUNC_NAME)) {
        return LogicOpType::And;
    }
    if (intarkdb::StringUtil::IsEqualIgnoreCase(op_name, OR_FUNC_NAME)) {
        return LogicOpType::Or;
    }
    if (op_name == NOT_FUNC_NAME) {
        return LogicOpType::Not;
    }
    return LogicOpType::InValid;
}

}  // namespace intarkdb
