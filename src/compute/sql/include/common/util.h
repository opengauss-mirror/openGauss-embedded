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
* util.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/common/util.h
*
* -------------------------------------------------------------------------
*/

#pragma once

#include <string>
#include <cstdint>
#include <memory>
#include <cstring>

#define DESCRIBE_COLUMN_NAME 0
#define DESCRIBE_COLUMN_DATATYPE 1
#define DESCRIBE_COLUMN_SIZE 2
#define DESCRIBE_COLUMN_PRECISION 3
#define DESCRIBE_COLUMN_SCALE 4
#define DESCRIBE_COLUMN_NULLABLE 5
#define DESCRIBE_COLUMN_DEFAULT 6
#define DESCRIBE_COLUMN_KEY 7
#define DESCRIBE_COLUMN_EXTRA 8
#define DESCRIBE_COLUMN_COMMENT 9
#define DESCRIBE_COLUMN_ID 10
#define DESCRIBE_COLUMN_TABLEID 11
#define DESCRIBE_COLUMN_FLAGS 12

#define COLUMN_ATTR_DATATYPE ("datatype")
#define COLUMN_ATTR_PRECISION ("precision")
#define COLUMN_ATTR_SCALE ("scale")
#define COLUMN_ATTR_NULLABLE ("nullable")
#define COLUMN_ATTR_DEFAULT ("default")

#define COLUMN_PARAM_USERID ("0")

// 用户空间 和3rd/gstor/src/storage/gstor/zekernel/kernel/include/persist_defs.h里的 SPACE_TYPE_USERS 保持一致
#define SQL_SPACE_TYPE_USERS 3

#define SYS_TABLE_COLUMN_NAME_DATATYPE ("DATATYPE")
#define SYS_TABLE_COLUMN_NAME_DEFAULT_TEXT ("DEFAULT_TEXT")
#define SYS_TABLE_COLUMN_FLAGS ("FLAGS")

const int32_t COLUMN_DELETED = 0x00000002;
const uint32_t COLUMN_SERIAL = 0x00000008;  // KNL_COLUMN_FLAG_SERIAL

const uint16_t CYCLE_WAIT_TIMES = 100;  // 连接数据库时循环等待次数
const uint16_t WAIT_TIME_MS_50 = 50;  // 每次等待时间，50ms

#define SHOWTABLE_NAME_START_IDX (1)

#define COLUMN_VARCHAR_SIZE_DEFAULT (65535)

static const char DEFAULT_ESCAPE = '\0';

//! data pointers
typedef uint8_t data_t;
typedef data_t *data_ptr_t;
typedef const data_t *const_data_ptr_t;

typedef uint64_t idx_t;
static constexpr const idx_t INVALID_INDEX = idx_t(-1);

static constexpr const uint8_t PART_NAME_SUFFIX_DAY_TIME_LEN = 8; // e.g. 20230822
static constexpr const uint8_t PART_NAME_SUFFIX_HOUR_TIME_LEN = 10; // e.g. 2023082209

template <typename T>
const T Load(const_data_ptr_t ptr) {
	T ret;
	memcpy(&ret, ptr, sizeof(ret));
	return ret;
}

template <typename T>
void Store(const T val, data_ptr_t ptr) {
	memcpy(ptr, (void *)&val, sizeof(val));
}
