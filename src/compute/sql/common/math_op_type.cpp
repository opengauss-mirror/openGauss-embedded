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
* math_op_type.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/common/math_op_type.cpp
*
* -------------------------------------------------------------------------
*/
#include "common/math_op_type.h"

MathOpType ToMathOpType(const std::string& op_name) {
    if (op_name == "+") {
        return MathOpType::Plus;
    }
    if (op_name == "-") {
        return MathOpType::Minus;
    }
    if (op_name == "*") {
        return MathOpType::Multiply;
    }
    if (op_name == "/") {
        return MathOpType::Divide;
    }
    if (op_name == "%") {
        return MathOpType::Mod;
    }
    return MathOpType::Invalid;
}

bool IsMathOp(const std::string& op_name) {
    return ToMathOpType(op_name) != MathOpType::Invalid;
}