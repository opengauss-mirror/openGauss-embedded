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
# * sql.py
# *
# * IDENTIFICATION
# * openGauss-embedded/interface/python/intarkdb/module/sql.py
# *
# * -------------------------------------------------------------------------
#  */

from ctypes import *
from intarkdb.module.model import *
from intarkdb.dbapi2 import Date,Timestamp
from intarkdb.module.StandardError import OperationalError
import decimal, sys


class Dynamic:
    def __init__(self, network):
        if network == False:
            if sys.platform.startswith('linux'):
                self._intarkdb_sql_clib = CDLL("libintarkdb.so")
            elif sys.platform.startswith('win'):
                self._intarkdb_sql_clib = CDLL("intarkdb.dll")
            else:
                raise OperationalError()
        # if network == True: 
        else:    
            if sys.platform.startswith('linux'):
                self._intarkdb_sql_clib = CDLL("libnetwork_client_dynamic.so")
            elif sys.platform.startswith('win'):
                self._intarkdb_sql_clib = CDLL("network_client_dynamic.dll")
            else:
                raise OperationalError()

        if network == False:
            # init cdll function signature
            self._intarkdb_sql_clib.intarkdb_open.argtypes = (c_char_p, POINTER(POINTER(INTARKDB_DATABASE)))
            self._intarkdb_sql_clib.intarkdb_open.restype = c_int

            self._intarkdb_sql_clib.intarkdb_close.argtypes = (POINTER(POINTER(INTARKDB_DATABASE)),)
            self._intarkdb_sql_clib.intarkdb_close.restype = None

            self._intarkdb_sql_clib.intarkdb_query.argtypes = (POINTER(INTARKDB_CONNECTION), c_char_p, POINTER(INTARKDB_RES_DEF))
            self._intarkdb_sql_clib.intarkdb_query.restype = c_int

            self._intarkdb_sql_clib.intarkdb_row_count.argtypes = (POINTER(INTARKDB_RES_DEF),)
            self._intarkdb_sql_clib.intarkdb_row_count.restype = c_longlong

            self._intarkdb_sql_clib.intarkdb_column_count.argtypes = (POINTER(INTARKDB_RES_DEF),)
            self._intarkdb_sql_clib.intarkdb_column_count.restype = c_longlong

            self._intarkdb_sql_clib.intarkdb_column_name.argtypes = (POINTER(INTARKDB_RES_DEF), c_longlong)
            self._intarkdb_sql_clib.intarkdb_column_name.restype = c_char_p

            self._intarkdb_sql_clib.intarkdb_column_type.argtypes = (POINTER(INTARKDB_RES_DEF), c_longlong)
            self._intarkdb_sql_clib.intarkdb_column_type.restype = c_int

            self._intarkdb_sql_clib.intarkdb_value_varchar.argtypes = (POINTER(INTARKDB_RES_DEF), c_longlong, c_longlong)
            self._intarkdb_sql_clib.intarkdb_value_varchar.restype = c_char_p

            self._intarkdb_sql_clib.intarkdb_value_uint32.argtypes = (POINTER(INTARKDB_RES_DEF), c_longlong, c_longlong)
            self._intarkdb_sql_clib.intarkdb_value_uint32.restype = c_uint32

            self._intarkdb_sql_clib.intarkdb_value_uint64.argtypes = (POINTER(INTARKDB_RES_DEF), c_longlong, c_longlong)
            self._intarkdb_sql_clib.intarkdb_value_uint64.restype = c_uint64

            self._intarkdb_sql_clib.intarkdb_value_int32.argtypes = (POINTER(INTARKDB_RES_DEF), c_longlong, c_longlong)
            self._intarkdb_sql_clib.intarkdb_value_int32.restype = c_int32

            self._intarkdb_sql_clib.intarkdb_value_int64.argtypes = (POINTER(INTARKDB_RES_DEF), c_longlong, c_longlong)
            self._intarkdb_sql_clib.intarkdb_value_int64.restype = c_int64

            self._intarkdb_sql_clib.intarkdb_value_double.argtypes = (POINTER(INTARKDB_RES_DEF), c_longlong, c_longlong)
            self._intarkdb_sql_clib.intarkdb_value_double.restype = c_double

            self._intarkdb_sql_clib.intarkdb_init_result.argtypes = None
            self._intarkdb_sql_clib.intarkdb_init_result.restype = POINTER(INTARKDB_RES_DEF)

            self._intarkdb_sql_clib.intarkdb_free_row.argtypes = (POINTER(INTARKDB_RES_DEF),)
            self._intarkdb_sql_clib.intarkdb_free_row.restype = None

            self._intarkdb_sql_clib.intarkdb_destroy_result.argtypes = (POINTER(INTARKDB_RES_DEF),)
            self._intarkdb_sql_clib.intarkdb_destroy_result.restype = None

            self._intarkdb_sql_clib.intarkdb_result_msg.argtypes = (POINTER(INTARKDB_RES_DEF),)
            self._intarkdb_sql_clib.intarkdb_result_msg.restype = c_char_p

            self._intarkdb_sql_clib.intarkdb_connect.argtypes = (POINTER(INTARKDB_DATABASE), POINTER(POINTER(INTARKDB_CONNECTION)))
            self._intarkdb_sql_clib.intarkdb_connect.restype = c_int

            self._intarkdb_sql_clib.intarkdb_disconnect.argtypes = (POINTER(POINTER(INTARKDB_CONNECTION)),)
            self._intarkdb_sql_clib.intarkdb_disconnect.restype = None

            self._intarkdb_sql_clib.intarkdb_prepare.argtypes = (POINTER(INTARKDB_CONNECTION), c_char_p, POINTER(POINTER(INTARKDB_PREPARED_STATEMENT)))
            self._intarkdb_sql_clib.intarkdb_prepare.restype = c_int

            self._intarkdb_sql_clib.intarkdb_execute_prepared.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), POINTER(INTARKDB_RES_DEF))
            self._intarkdb_sql_clib.intarkdb_execute_prepared.restype = c_int

            self._intarkdb_sql_clib.intarkdb_prepare_errmsg.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT),)
            self._intarkdb_sql_clib.intarkdb_prepare_errmsg.restype = c_char_p

            self._intarkdb_sql_clib.intarkdb_destroy_prepare.argtypes = (POINTER(POINTER(INTARKDB_PREPARED_STATEMENT)),)
            self._intarkdb_sql_clib.intarkdb_destroy_prepare.restype = None

            self._intarkdb_sql_clib.intarkdb_bind_int8.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_int8)
            self._intarkdb_sql_clib.intarkdb_bind_int8.restype = c_int

            self._intarkdb_sql_clib.intarkdb_bind_int16.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_int16)
            self._intarkdb_sql_clib.intarkdb_bind_int16.restype = c_int

            self._intarkdb_sql_clib.intarkdb_bind_int32.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_int32)
            self._intarkdb_sql_clib.intarkdb_bind_int32.restype = c_int

            self._intarkdb_sql_clib.intarkdb_bind_int64.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_int64)
            self._intarkdb_sql_clib.intarkdb_bind_int64.restype = c_int

            self._intarkdb_sql_clib.intarkdb_bind_boolean.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_int8)
            self._intarkdb_sql_clib.intarkdb_bind_boolean.restype = c_int

            self._intarkdb_sql_clib.intarkdb_bind_double.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_double)
            self._intarkdb_sql_clib.intarkdb_bind_double.restype = c_int

            self._intarkdb_sql_clib.intarkdb_bind_date.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_char_p)
            self._intarkdb_sql_clib.intarkdb_bind_date.restype = c_int

            self._intarkdb_sql_clib.intarkdb_bind_varchar.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_char_p)
            self._intarkdb_sql_clib.intarkdb_bind_varchar.restype = c_int

            self._intarkdb_sql_clib.intarkdb_bind_null.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint)
            self._intarkdb_sql_clib.intarkdb_bind_null.restype = c_int

            self._intarkdb_sql_clib.intarkdb_connect_kv.argtypes = (POINTER(INTARKDB_DATABASE), POINTER(POINTER(INTARKDB_CONNECTION)))
            self._intarkdb_sql_clib.intarkdb_connect_kv.restype = c_int

            self._intarkdb_sql_clib.intarkdb_disconnect_kv.argtypes = (POINTER(POINTER(INTARKDB_CONNECTION)),)
            self._intarkdb_sql_clib.intarkdb_disconnect_kv.restype = None

            self._intarkdb_sql_clib.intarkdb_open_table_kv.argtypes = (POINTER(INTARKDB_CONNECTION), c_char_p)
            self._intarkdb_sql_clib.intarkdb_open_table_kv.restype = c_int

            self._intarkdb_sql_clib.intarkdb_open_memtable_kv.argtypes = (POINTER(INTARKDB_CONNECTION), c_char_p)
            self._intarkdb_sql_clib.intarkdb_open_memtable_kv.restype = c_int

            self._intarkdb_sql_clib.intarkdb_set.argtypes =  (POINTER(INTARKDB_CONNECTION), c_char_p, c_char_p)
            self._intarkdb_sql_clib.intarkdb_set.restype = POINTER(INTARKDB_REPLY)

            self._intarkdb_sql_clib.intarkdb_get.argtypes = (POINTER(INTARKDB_CONNECTION), c_char_p)
            self._intarkdb_sql_clib.intarkdb_get.restype = POINTER(INTARKDB_REPLY)

            self._intarkdb_sql_clib.intarkdb_del.argtypes = (POINTER(INTARKDB_CONNECTION), c_char_p)
            self._intarkdb_sql_clib.intarkdb_del.restype = POINTER(INTARKDB_REPLY)

            self._intarkdb_sql_clib.intarkdb_multi.argtypes = (POINTER(INTARKDB_CONNECTION),)
            self._intarkdb_sql_clib.intarkdb_multi.restype = c_int

            self._intarkdb_sql_clib.intarkdb_exec.argtypes = (POINTER(INTARKDB_CONNECTION),)
            self._intarkdb_sql_clib.intarkdb_exec.restype = c_int

            self._intarkdb_sql_clib.intarkdb_discard.argtypes = (POINTER(INTARKDB_CONNECTION),)
            self._intarkdb_sql_clib.intarkdb_discard.restype = c_int
        # if network == True:
        else:
            # init cdll function signature   
            self._intarkdb_sql_clib.intarkdb_client_query.argtypes = (POINTER(INTARKDB_DATABASE), c_char_p, POINTER(INTARKDB_RES_DEF))
            self._intarkdb_sql_clib.intarkdb_client_query.restype = c_int    

            self._intarkdb_sql_clib.intarkdb_client_row_count.argtypes = (POINTER(INTARKDB_RES_DEF),)
            self._intarkdb_sql_clib.intarkdb_client_row_count.restype = c_longlong        

            self._intarkdb_sql_clib.intarkdb_client_column_count.argtypes = (POINTER(INTARKDB_RES_DEF),)
            self._intarkdb_sql_clib.intarkdb_client_column_count.restype = c_longlong

            self._intarkdb_sql_clib.intarkdb_client_column_name.argtypes = (POINTER(INTARKDB_RES_DEF), c_longlong)
            self._intarkdb_sql_clib.intarkdb_client_column_name.restype = c_char_p

            self._intarkdb_sql_clib.intarkdb_client_column_type.argtypes = (POINTER(INTARKDB_RES_DEF), c_longlong)
            self._intarkdb_sql_clib.intarkdb_client_column_type.restype = c_int

            self._intarkdb_sql_clib.intarkdb_client_value_varchar.argtypes = (POINTER(INTARKDB_RES_DEF), c_longlong, c_longlong)
            self._intarkdb_sql_clib.intarkdb_client_value_varchar.restype = c_char_p

            self._intarkdb_sql_clib.intarkdb_client_value_uint32.argtypes = (POINTER(INTARKDB_RES_DEF), c_longlong, c_longlong)
            self._intarkdb_sql_clib.intarkdb_client_value_uint32.restype = c_uint32

            self._intarkdb_sql_clib.intarkdb_client_value_uint64.argtypes = (POINTER(INTARKDB_RES_DEF), c_longlong, c_longlong)
            self._intarkdb_sql_clib.intarkdb_client_value_uint64.restype = c_uint64

            self._intarkdb_sql_clib.intarkdb_client_value_double.argtypes = (POINTER(INTARKDB_RES_DEF), c_longlong, c_longlong)
            self._intarkdb_sql_clib.intarkdb_client_value_double.restype = c_double

            self._intarkdb_sql_clib.intarkdb_client_init_result.argtypes = None
            self._intarkdb_sql_clib.intarkdb_client_init_result.restype = POINTER(INTARKDB_RES_DEF)

            self._intarkdb_sql_clib.intarkdb_client_free_row.argtypes = (POINTER(INTARKDB_RES_DEF),)
            self._intarkdb_sql_clib.intarkdb_client_free_row.restype = None

            self._intarkdb_sql_clib.intarkdb_client_destroy_result.argtypes = (POINTER(INTARKDB_RES_DEF),)
            self._intarkdb_sql_clib.intarkdb_client_destroy_result.restype = None

            self._intarkdb_sql_clib.intarkdb_client_result_msg.argtypes = (POINTER(INTARKDB_RES_DEF),)
            self._intarkdb_sql_clib.intarkdb_client_result_msg.restype = c_char_p

            self._intarkdb_sql_clib.intarkdb_client_connect.argtypes = (c_char_p, c_int, c_char_p, c_char_p, c_char_p, POINTER(POINTER(INTARKDB_DATABASE)))
            self._intarkdb_sql_clib.intarkdb_client_connect.restype = c_int

            self._intarkdb_sql_clib.intarkdb_client_disconnect.argtypes = (POINTER(POINTER(INTARKDB_DATABASE)),)
            self._intarkdb_sql_clib.intarkdb_client_disconnect.restype = c_int

            self._intarkdb_sql_clib.intarkdb_client_prepare.argtypes = (POINTER(INTARKDB_DATABASE), c_char_p, POINTER(POINTER(INTARKDB_PREPARED_STATEMENT)))
            self._intarkdb_sql_clib.intarkdb_client_prepare.restype = c_int

            self._intarkdb_sql_clib.intarkdb_client_execute_prepared.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), POINTER(INTARKDB_RES_DEF))
            self._intarkdb_sql_clib.intarkdb_client_execute_prepared.restype = c_int

            self._intarkdb_sql_clib.intarkdb_client_prepare_errmsg.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT),)
            self._intarkdb_sql_clib.intarkdb_client_prepare_errmsg.restype = c_char_p

            self._intarkdb_sql_clib.intarkdb_client_destroy_prepare.argtypes = (POINTER(POINTER(INTARKDB_PREPARED_STATEMENT)),)
            self._intarkdb_sql_clib.intarkdb_client_destroy_prepare.restype = None

            self._intarkdb_sql_clib.intarkdb_client_bind_int8.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_int8)
            self._intarkdb_sql_clib.intarkdb_client_bind_int8.restype = c_int

            self._intarkdb_sql_clib.intarkdb_client_bind_int16.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_int16)
            self._intarkdb_sql_clib.intarkdb_client_bind_int16.restype = c_int

            self._intarkdb_sql_clib.intarkdb_client_bind_int32.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_int32)
            self._intarkdb_sql_clib.intarkdb_client_bind_int32.restype = c_int

            self._intarkdb_sql_clib.intarkdb_client_bind_int64.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_int64)
            self._intarkdb_sql_clib.intarkdb_client_bind_int64.restype = c_int

            self._intarkdb_sql_clib.intarkdb_client_bind_boolean.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_int8)
            self._intarkdb_sql_clib.intarkdb_client_bind_boolean.restype = c_int

            self._intarkdb_sql_clib.intarkdb_client_bind_double.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_double)
            self._intarkdb_sql_clib.intarkdb_client_bind_double.restype = c_int

            self._intarkdb_sql_clib.intarkdb_client_bind_date.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint, c_char_p)
            self._intarkdb_sql_clib.intarkdb_client_bind_date.restype = c_int

            self._intarkdb_sql_clib.intarkdb_client_bind_null.argtypes = (POINTER(INTARKDB_PREPARED_STATEMENT), c_uint)
            self._intarkdb_sql_clib.intarkdb_client_bind_null.restype = c_int


    def get_intarkdb_sql_clib(self):
        return self._intarkdb_sql_clib


class SQL:

    STMT_ID = 1

    def __init__(self, database, network = False):
        self._dynamic = Dynamic(network)
        self._db = database


    def open_db(self, path):
        db_ptr = POINTER(INTARKDB_DATABASE)()
        ret = self._dynamic.get_intarkdb_sql_clib().intarkdb_open(path.encode(), byref(db_ptr))
        self._db = db_ptr.contents
        return ret, db_ptr.contents


    def close_db(self):
        db_ptr = pointer(self._db)
        self._dynamic.get_intarkdb_sql_clib().intarkdb_close(byref(db_ptr))


    def intarkdb_query(self, conn, sql_str, intarkdb_result, network = False):
        if(network == False):
            ret = self._dynamic.get_intarkdb_sql_clib().intarkdb_query(byref(conn), sql_str.encode(), intarkdb_result)
        # if(network == True):
        else:
            ret = self._dynamic.get_intarkdb_sql_clib().intarkdb_client_query(byref(conn), sql_str.encode(), intarkdb_result)
        return ret


    def intarkdb_row_count(self, intarkdb_result, network = False):
        if(network == False):
            rowcount = self._dynamic.get_intarkdb_sql_clib().intarkdb_row_count(intarkdb_result)
        # if(network == True):
        else:
            rowcount = self._dynamic.get_intarkdb_sql_clib().intarkdb_client_row_count(intarkdb_result)
        return rowcount


    def intarkdb_column_count(self, intarkdb_result, network = False):
        if(network == False):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_column_count(intarkdb_result)
        # if(network == True):
        else:
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_client_column_count(intarkdb_result)


    def intarkdb_column_name(self, intarkdb_result, col_index, network = False) -> str:
        if(network == False):
            name = self._dynamic.get_intarkdb_sql_clib().intarkdb_column_name(intarkdb_result, col_index)
        # if(network == True):
        else:
            name = self._dynamic.get_intarkdb_sql_clib().intarkdb_client_column_name(intarkdb_result, col_index)
        if name == None:
            return None
        return name.decode()

    
    def intarkdb_column_type(self, intarkdb_result, col_index, network = False):
        if(network == False):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_column_type(intarkdb_result, col_index)
        # if(network == True):
        else:
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_client_column_type(intarkdb_result, col_index)
    

    def intarkdb_value(self, intarkdb_result, row_index, col_index, code_type, network = False):
        if(network == False):
            # 转化为字符串类型
            if code_type != 0:
                res = self._dynamic.get_intarkdb_sql_clib().intarkdb_value_varchar(intarkdb_result, row_index, col_index)
                if(res != None):
                    res = res.decode()
                else:
                    return None
            else:
                return None

            # 根据字段类型，转化到对应类型
            if(res in ['-infinity', 'infinity','-nan']):
                pass
            elif (res == ""):
                return None
            elif code_type in [SQL_TYPE_UTINYINT, SQL_TYPE_USMALLINT, SQL_TYPE_UINT32]:
                res = self._dynamic.get_intarkdb_sql_clib().intarkdb_value_uint32(intarkdb_result, row_index, col_index)
            elif code_type == SQL_TYPE_UINT64:
                res = self._dynamic.get_intarkdb_sql_clib().intarkdb_value_uint64(intarkdb_result, row_index, col_index)
            elif(code_type in [SQL_TYPE_REAL, SQL_TYPE_FLOAT]):
                res = self._dynamic.get_intarkdb_sql_clib().intarkdb_value_double(intarkdb_result, row_index, col_index)
        # if(network == True):
        else:
            # 转化为字符串类型
            if code_type!= 0:
                res = self._dynamic.get_intarkdb_sql_clib().intarkdb_client_value_varchar(intarkdb_result, row_index, col_index)
                if(res!= None):
                    res = res.decode()
                else:
                    return None
            else:
                return None

            # 根据字段类型，转化到对应类型
            if(res in ['-infinity', 'infinity','-nan']):
                pass
            elif (res == ""):
                return None
            elif code_type in [SQL_TYPE_UTINYINT, SQL_TYPE_USMALLINT, SQL_TYPE_UINT32]:
                res = self._dynamic.get_intarkdb_sql_clib().intarkdb_client_value_uint32(intarkdb_result, row_index, col_index)
            elif code_type == SQL_TYPE_UINT64:
                res = self._dynamic.get_intarkdb_sql_clib().intarkdb_client_value_uint64(intarkdb_result, row_index, col_index)
            elif(code_type in [SQL_TYPE_REAL, SQL_TYPE_FLOAT]):
                res = self._dynamic.get_intarkdb_sql_clib().intarkdb_client_value_double(intarkdb_result, row_index, col_index)
            
        if(code_type in [SQL_TYPE_INTEGER, SQL_TYPE_TINYINT, SQL_TYPE_SMALLINT, SQL_TYPE_BIGINT]):
            res = int(res)
        elif(code_type == SQL_TYPE_BOOLEAN):
            res = int(res)
        elif(code_type == SQL_TYPE_DECIMAL):
            decimal.getcontext().prec = 3
            res = decimal.Decimal(res)
        elif(code_type == SQL_TYPE_DATE):
            # 字符串为日期形式 "YYYY-MM-DD"
            date_parts = res.split("-")
            year = int(date_parts[0])
            month = int(date_parts[1])
            day = int(date_parts[2])
            res = Date(year, month, day)
            res = str(res)
        elif(code_type == SQL_TYPE_TIMESTAMP):
            # 字符串为日期形式 '%Y-%m-%d %H:%M:%S.%f'
            parts = res.split()
            # %Y-%m-%d
            date_parts = parts[0].split("-")
            year = int(date_parts[0])
            month = int(date_parts[1])
            day = int(date_parts[2])
            # %H:%M:%S.%f
            time_parts = parts[1].split(":")
            hour = int(time_parts[0])
            minute = int(time_parts[1])
            second_part = time_parts[2].split(".")
            second = int(second_part[0])
            microsecond = 0
            if(len(second_part)>1):
                microsecond = int(second_part[1])
            res = Timestamp(year, month, day, hour, minute, second, microsecond)
            res = res.strftime('%Y-%m-%d %H:%M:%S.%f')            
        
        # 返回结果
        return res
    

    def intarkdb_init_result(self, network = False):
        if (network == False):
            result = self._dynamic.get_intarkdb_sql_clib().intarkdb_init_result()
        # if (network == True):
        else:
            result = self._dynamic.get_intarkdb_sql_clib().intarkdb_client_init_result()
        return result


    def intarkdb_free_result(self, intarkdb_result, network = False):
        if (network == False):
            self._dynamic.get_intarkdb_sql_clib().intarkdb_free_result(intarkdb_result)
        # if (network == True):
        else:
            self._dynamic.get_intarkdb_sql_clib().intarkdb_client_free_result(intarkdb_result)


    def intarkdb_destory_result(self, intarkdb_result, network = False):
        if (network == False):
            self._dynamic.get_intarkdb_sql_clib().intarkdb_destroy_result(intarkdb_result)
        # if (network == True):
        else:
            self._dynamic.get_intarkdb_sql_clib().intarkdb_client_destroy_result(intarkdb_result)


    def intarkdb_result_msg(self, intarkdb_result, network = False):
        if (network == False):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_result_msg(intarkdb_result)
        # if (network == True):
        else:
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_client_result_msg(intarkdb_result)
    

    def intarkdb_connect(self, kv = False):
        connection_ptr = POINTER(INTARKDB_CONNECTION)()
        if (kv == False):
            ret = self._dynamic.get_intarkdb_sql_clib().intarkdb_connect(byref(self._db), byref(connection_ptr))
        # if (kv == True):
        else:
            ret = self._dynamic.get_intarkdb_sql_clib().intarkdb_connect_kv(byref(self._db), byref(connection_ptr))

        if connection_ptr:
            return ret, connection_ptr.contents
        else:
            return ret, None


    def intarkdb_client_connect(self, database, host, port, user, password):
        db_ptr = POINTER(INTARKDB_DATABASE)()
        ret = self._dynamic.get_intarkdb_sql_clib().intarkdb_client_connect(
            host.encode(), port, database.encode(), user.encode(), password.encode(), byref(db_ptr))
        
        if db_ptr:
            return ret, db_ptr.contents
        else:
            return ret, None


    def intarkdb_disconnect(self, conn, network = False, kv = False):
        conn_ptr = pointer(conn)
        if(network == False):
            if(kv == False):
                self._dynamic.get_intarkdb_sql_clib().intarkdb_disconnect(byref(conn_ptr))
            # if(kv == True):
            else:
                self._dynamic.get_intarkdb_sql_clib().intarkdb_disconnect_kv(byref(conn_ptr))
        # if(network == True):
        else:
            self._dynamic.get_intarkdb_sql_clib().intarkdb_client_disconnect(byref(conn_ptr))


    def intarkdb_prepare(self, conn, sql_str):
        prepared_statement = POINTER(INTARKDB_PREPARED_STATEMENT)()
        ret = self._dynamic.get_intarkdb_sql_clib().intarkdb_prepare(
            byref(conn), sql_str.encode(), byref(prepared_statement))
        return ret , prepared_statement


    def intarkdb_client_prepare(self, conn, sql_str):
        prepared_statement = POINTER(INTARKDB_PREPARED_STATEMENT)()
        ret = self._dynamic.get_intarkdb_sql_clib().intarkdb_client_prepare(
            byref(conn), sql_str.encode(), byref(prepared_statement), SQL.STMT_ID)
        SQL.STMT_ID += 1
        return ret, prepared_statement


    def intarkdb_execute_prepared(self, prepared_statement, intarkdb_result, network = False):
        if(network == False):
            ret = self._dynamic.get_intarkdb_sql_clib().intarkdb_execute_prepared(prepared_statement, intarkdb_result)
        # if(network == True):
        else:
            ret = self._dynamic.get_intarkdb_sql_clib().intarkdb_client_execute_prepared(prepared_statement, intarkdb_result)
        return ret
    

    def intarkdb_prepare_errmsg(self, prepared_statement, network = False):
        if(network == False):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_prepare_errmsg(prepared_statement)
        # if(network == True):
        else:
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_client_prepare_errmsg(prepared_statement)
    

    def intarkdb_destroy_prepare(self, prepared_statement, network = False):
        if(network == False):
            self._dynamic.get_intarkdb_sql_clib().intarkdb_destroy_prepare(byref(prepared_statement))
        # if(network == True):
        else:
            self._dynamic.get_intarkdb_sql_clib().intarkdb_client_destroy_prepare(byref(prepared_statement))


    def intarkdb_bind_value(self, prepared_statement, param_idx, val, code_type):
        if(code_type == SQL_TYPE_INTEGER):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_bind_int32(prepared_statement, param_idx, val)
        elif(code_type == SQL_TYPE_BIGINT):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_bind_int64(prepared_statement, param_idx, val)
        elif(code_type == SQL_TYPE_TINYINT):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_bind_int8(prepared_statement, param_idx, val)
        elif(code_type == SQL_TYPE_SMALLINT):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_bind_int16(prepared_statement, param_idx, val)
        elif(code_type == SQL_TYPE_BOOLEAN):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_bind_boolean(prepared_statement, param_idx, val)
        elif(code_type in [SQL_TYPE_REAL, SQL_TYPE_FLOAT]):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_bind_double(
                prepared_statement, param_idx, c_double(val).value)
        elif(code_type in [SQL_TYPE_DATE, SQL_TYPE_TIMESTAMP]):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_bind_date(prepared_statement, param_idx, val)
        elif(code_type == SQL_TYPE_VARCHAR):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_bind_varchar(prepared_statement, param_idx, val.encode())
        elif(code_type == None):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_bind_null(prepared_statement, param_idx)
        else:
            return SQL_ERROR
        

    def intarkdb_client_bind_value(self, prepared_statement, param_idx, val, code_type):
        if(code_type == SQL_TYPE_INTEGER):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_client_bind_int32(prepared_statement, param_idx, val)
        elif(code_type == SQL_TYPE_BIGINT):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_client_bind_int64(prepared_statement, param_idx, val)
        elif(code_type == SQL_TYPE_TINYINT):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_client_bind_int8(prepared_statement, param_idx, val)
        elif(code_type == SQL_TYPE_SMALLINT):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_client_bind_int16(prepared_statement, param_idx, val)
        elif(code_type == SQL_TYPE_BOOLEAN):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_client_bind_boolean(prepared_statement, param_idx, val)
        elif(code_type in [SQL_TYPE_REAL, SQL_TYPE_FLOAT]):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_client_bind_double(
                prepared_statement, param_idx, c_double(val).value)
        elif(code_type in [SQL_TYPE_DATE, SQL_TYPE_TIMESTAMP]):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_client_bind_date(prepared_statement, param_idx, val) 
        elif(code_type == SQL_TYPE_VARCHAR):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_client_bind_varchar(prepared_statement, param_idx, val.encode())
        elif(code_type == None):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_client_bind_null(prepared_statement, param_idx)
        else:
            return SQL_ERROR


    def intarkdb_open_table(self, conn, table_name, is_memory = False):
        if(is_memory == False):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_open_table_kv(byref(conn), table_name.encode())
        if(is_memory == True):
            return self._dynamic.get_intarkdb_sql_clib().intarkdb_open_memtable_kv(byref(conn), table_name.encode(), is_memory)
        
    
    def intarkdb_set(self, conn, key, value):
        return self._dynamic.get_intarkdb_sql_clib().intarkdb_set(byref(conn), key.encode(), value.encode())


    def intarkdb_get(self, conn, key):
        return self._dynamic.get_intarkdb_sql_clib().intarkdb_get(byref(conn), key.encode())
    

    def intarkdb_del(self, conn, key):
        return self._dynamic.get_intarkdb_sql_clib().intarkdb_del(byref(conn), key.encode())


    def intarkdb_multi(self, conn):
        return self._dynamic.get_intarkdb_sql_clib().intarkdb_multi(byref(conn))
    

    def intarkdb_exec(self, conn):
        return self._dynamic.get_intarkdb_sql_clib().intarkdb_exec(byref(conn))
    

    def intarkdb_discard(self, conn):
        return self._dynamic.get_intarkdb_sql_clib().intarkdb_discard(byref(conn))
