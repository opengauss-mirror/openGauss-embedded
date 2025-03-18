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
* like_operator.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/type/operator/like_operator.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "planner/expressions/like_expression.h"
#include "type/type_system.h"
#include "type/value.h"


Value LikeOperator(LikeType type, const Value& left, const Value& right, const Value& escape);