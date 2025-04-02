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
 * pragma_version.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/function/version/pragma_version.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "function/version/pragma_version.h"

namespace intarkdb {

const char *SourceID() {
	return INTARKDB_SOURCE_ID;
}

const char *LibraryVersion() {
	return INTARKDB_VERSION;
}

} // namespace intarkdb
