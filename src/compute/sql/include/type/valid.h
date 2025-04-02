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
* valid.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/type/valid.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <math.h>
#include "common/winapi.h"

template <typename T>
EXPORT_API bool IsFinite(T value) {
    return true;
}

// 使用模板来避免重载中的歧义
template <>
EXPORT_API bool IsFinite(double value);

template <>
EXPORT_API bool IsFinite(float value);

template <typename T>
EXPORT_API bool IsNan(T value) {
    return false;
}

template <>
EXPORT_API bool IsNan(double value);

template <>
EXPORT_API bool IsNan(float value);
