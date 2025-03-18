# /*
# * Copyright (c) GBA-NCTI-ISDC. 2022-2024.
# *
# * openGauss embedded is licensed under Mulan PSL v2.
# * You can use this software according to the terms and conditions of the Mulan PSL v2.
# * You may obtain a copy of Mulan PSL v2 at:
# *
# * http://license.coscl.org.cn/MulanPSL2
# *
# * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# * MERCHANTABILITY OR FITFOR A PARTICULAR PURPOSE.
# * See the Mulan PSL v2 for more details.
# * -------------------------------------------------------------------------
# *
# * model.py
# *
# * IDENTIFICATION
# * openGauss-embedded/interface/python/intarkdb/module/model.py
# *
# * -------------------------------------------------------------------------
#  */

from ctypes import *

SQL_FALSE = 0
SQL_TRUE = 1

# class intarkdb_state_t(Enum):
SQL_ERROR = -1
SQL_SUCCESS = 0
SQL_TIMEDOUT = 1

# class EN_ASSIGN_TYPE(Enum):
ASSIGN_TYPE_EQUAL = 0  # 等于
ASSIGN_TYPE_LESS = 0 + 1  # 小于
ASSIGN_TYPE_MORE = 0 + 2  # 大于
ASSIGN_TYPE_LESS_EQUAL = 0 + 3  # 小于等于
ASSIGN_TYPE_MORE_EQUAL = 0 + 4  # 大于等于
ASSIGN_TYPE_UNEQUAL = 0 + 5  # 不等于

# class EN_SQL_TYPE(Enum):
SQL_TYPE_UNKNOWN = -1
SQL_TYPE_BASE = 20000
SQL_TYPE_INTEGER = 20000 + 1  # /# * native 32 bits integer # */
SQL_TYPE_BIGINT = 20000 + 2  # /# * native 64 bits integer # */
SQL_TYPE_REAL = 20000 + 3  # /# * 8-byte native double # */
SQL_TYPE_NUMBER = 20000 + 4  # /# * number # */
SQL_TYPE_DECIMAL = 20000 + 5  # /# * decimal  internal used # */
SQL_TYPE_DATE = 20000 + 6  # /# * datetime # */
SQL_TYPE_TIMESTAMP = 20000 + 7  # /# * timestamp # */
SQL_TYPE_CHAR = 20000 + 8  # /# * char(n) # */
SQL_TYPE_VARCHAR = 20000 + 9  # /# * varchar  varchar2 # */
SQL_TYPE_STRING = 20000 + 10  # /# * native char # * # */
SQL_TYPE_BINARY = 20000 + 11  # /# * binary # */
SQL_TYPE_VARBINARY = 20000 + 12  # /# * varbinary # */
SQL_TYPE_CLOB = 20000 + 13  # /# * clob # */
SQL_TYPE_BLOB = 20000 + 14  # /# * blob # */
SQL_TYPE_CURSOR = 20000 + 15  # /# * resultset  for stored procedure # */
SQL_TYPE_COLUMN = 20000 + 16  # /# * column type  internal used # */
SQL_TYPE_BOOLEAN = 20000 + 17
SQL_TYPE_TIMESTAMP_TZ_FAKE = 20000 + 18
SQL_TYPE_TIMESTAMP_LTZ = 20000 + 19
SQL_TYPE_INTERVAL = 20000 + 20  # /# * interval of Postgre style  no use # */
SQL_TYPE_INTERVAL_YM = 20000 + 21  # /# * interval YEAR TO MONTH # */
SQL_TYPE_INTERVAL_DS = 20000 + 22  # /# * interval DAY TO SECOND # */
SQL_TYPE_RAW = 20000 + 23  # /# * raw # */
SQL_TYPE_IMAGE = 20000 + 24  # /# * image  equals to longblob # */
SQL_TYPE_UINT32 = 20000 + 25  # /# * unsigned integer # */
SQL_TYPE_UINT64 = 20000 + 26  # /# * unsigned bigint # */
SQL_TYPE_SMALLINT = 20000 + 27  # /# * 16-bit integer # */
SQL_TYPE_USMALLINT = 20000 + 28  # /# * unsigned 16-bit integer # */
SQL_TYPE_TINYINT = 20000 + 29  # /# * 8-bit integer # */
SQL_TYPE_UTINYINT = 20000 + 30  # /# * unsigned 8-bit integer # */
SQL_TYPE_FLOAT = 20000 + 31  # /# * 4-byte float # */
SQL_TYPE_TIMESTAMP_TZ = 20000 + 32  # /# * timestamp with time zone # */
SQL_TYPE_ARRAY = 20000 + 33  # /# * array # */

SQL_TYPE_OPERAND_CEIL = 20000 + 40    # ceil of operand type
SQL_TYPE_RECORD = 20000 + 41
SQL_TYPE_COLLECTION = 20000 + 42
SQL_TYPE_OBJECT = 20000 + 43
SQL_TYPE__DO_NOT_USE = 20000 + 44

SQL_TYPE_FUNC_BASE = 20000 + 200
SQL_TYPE_TYPMODE = 20000 + 200 + 1
SQL_TYPE_VM_ROWID = 20000 + 200 + 2
SQL_TYPE_ITVL_UNIT = 20000 + 200 + 3
SQL_TYPE_UNINITIALIZED = 20000 + 200 + 4
SQL_TYPE_NATIVE_DATE = 20000 + 200 + 5       # native datetime  internal used
SQL_TYPE_NATIVE_TIMESTAMP = 20000 + 200 + 6  # native timestamp  internal used
SQL_TYPE_LOGIC_TRUE = 20000 + 200 + 7       # native true  internal

INTARKDB_NUM = {}

class COL_TEXT(Structure):
    _fields_ = [
        ("str", c_char_p),
        ("len", c_uint),
        ("assign", c_int),
    ]


class EXP_COLUMN_DEF(Structure):
    _fields_ = [
        ("name", COL_TEXT),
        ("col_type", c_int),
        ("col_slot", c_ushort),
        ("size", c_ushort),
        ("nullable", c_uint),
        ("is_primary", c_uint),
        ("is_default", c_uint),
        ("default_val", COL_TEXT),
        ("crud_value", COL_TEXT),
        ("precision", c_ushort),
        ("comment", COL_TEXT),
    ]


class EXP_INDEX_DEF(Structure):
    _fields_ = [
        ("name", COL_TEXT),
        ("cols", POINTER(COL_TEXT)),
        ("col_count", c_uint),
        ("is_unique", c_uint),
        ("is_primary", c_uint),
    ]


class RES_ROW_DEF(Structure):
    _fields_ = [
        ("column_count", c_int),
        ("row_column_list", POINTER(EXP_COLUMN_DEF))
    ]


class INTARKDB_REPLY(Structure):
    _fields_ = [
        ("type", c_int),
        ("len", c_size_t),
        ("str", c_char_p),
    ]


class INTARKDB_DATABASE(Structure):
    _fields_ = [
        ("db", c_void_p),
    ]


class INTARKDB_CONNECTION(Structure):
    _fields_ = [
        ("conn", c_void_p),
    ]


class INTARKDB_PREPARED_STATEMENT(Structure):
    _fields_ = [
        ("prep_stmt", c_void_p),
    ]


class API_TEXT_T(Structure):
    _fields_ = [
        ("str", c_char_p),
        ("len", c_longlong),
        ("data_type", c_longlong),
    ]


class INTARKDB_RES_DEF(Structure):
    _fields_ = [
        ("row_count", c_longlong),
        ("is_select", c_bool),
        ("res_row", c_void_p),
        ("column_count", c_longlong),
        ("column_names", POINTER(API_TEXT_T)),
        ("msg", c_char_p),
        ("value_ptr", c_char_p),
        ("row_idx", c_longlong),
    ]  
