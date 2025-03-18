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
 * pragma_version.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/function/version/pragma_version.h
 *
 * -------------------------------------------------------------------------
 */
#include <cstdint>

#include "common/winapi.h"
namespace intarkdb {

EXPORT_API const char *SourceID();

EXPORT_API const char *LibraryVersion();

} // namespace intarkdb
