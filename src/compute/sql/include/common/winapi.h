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
* winapi.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/common/winapi.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#ifdef WIN32
#define EXPORT_API __declspec(dllexport)
#else
#ifndef EXPORT_API
#define EXPORT_API __attribute__((visibility("default")))
#endif
#endif

#ifndef INTARKDB_EXTENSION_API
#ifdef _WIN32
#ifdef INTARKDB_BUILD_LOADABLE_EXTENSION
#define INTARKDB_EXTENSION_API __declspec(dllexport)
#else
#define INTARKDB_EXTENSION_API
#endif
#else
#define INTARKDB_EXTENSION_API __attribute__((visibility("default")))
#endif
#endif
