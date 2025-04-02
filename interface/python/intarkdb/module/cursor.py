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
# * cursor.py
# *
# * IDENTIFICATION
# * openGauss-embedded/interface/python/intarkdb/module/cursor.py
# *
# * -------------------------------------------------------------------------
#  */

from intarkdb.module.model import *
from intarkdb.module.sql import SQL
from intarkdb.module.StandardError import ProgrammingError
from intarkdb.dbapi2 import Date, Timestamp
from decimal import Decimal
import threading


class Cursor:

    CURSOR_LOCK = threading.Lock()

    def __init__(self, conn, database, network, kv = False):
        self.description = None
        self.rowcount = -1
        self.colcount = -1
        self.rownumber = 0
        self.connection = conn
        self._db = database
        self.__statement = None
        self._network = network
        self.__sql = SQL(self._db, network)
        self._kv = kv

        # 判断游标是否开启
        self.__status = True
        # 初始化结果集
        self.__resultset = self.__sql.intarkdb_init_result(network)
    

    def __del__(self):
        pass


    def execute(self, operation, parameters=None):
        # 判断游标是否开启
        if (self.__status == False):
            raise ProgrammingError(SQL_ERROR,"cursor already closed")
        if (self._kv == True):
            raise ProgrammingError(SQL_ERROR, "use get, set or del instead!")

        # 连接对象
        conn = self.connection
        
        with Cursor.CURSOR_LOCK:
            # 使用占位符
            if(parameters != None):
                # 准备prepared_statement
                if(self._network == False):
                    ret ,self.__statement = self.__sql.intarkdb_prepare(conn._conn, operation)
                else:
                    ret ,self.__statement = self.__sql.intarkdb_client_prepare(conn._conn, operation)

                if SQL_SUCCESS == ret:
                    print("intarkdb prepare success")
                else:
                    print("intarkdb prepare fail")
                    msg = self.__sql.intarkdb_prepare_errmsg(self.__statement, self._network)
                    raise ProgrammingError(ret, msg)

                # 绑定数据
                for col in range(len(parameters)):
                    param_idx = col + 1
                    val = parameters[col]
                    type_code = self.__typecode(val)

                    if(self._network == False):
                        if (type_code == SQL_TYPE_DATE or type_code == SQL_TYPE_TIMESTAMP):
                            val = str(val)
                            bind_code = SQL_TYPE_VARCHAR
                            ret = self.__sql.intarkdb_bind_value(self.__statement, param_idx, val, bind_code)
                        else:
                            ret = self.__sql.intarkdb_bind_value(self.__statement, param_idx, val, type_code)
                    # if(self._network == True):
                    else:
                        if (type_code == SQL_TYPE_DATE or type_code == SQL_TYPE_TIMESTAMP):
                            val = str(val)
                            bind_code = SQL_TYPE_VARCHAR
                            ret = self.__sql.intarkdb_client_bind_value(self.__statement, param_idx, val, bind_code)
                        else:
                            ret = self.__sql.intarkdb_client_bind_value(self.__statement, param_idx, val, type_code)                           
                        
                    if(ret == SQL_ERROR):
                        print("intarkdb bind value fail")
                        msg = self.__sql.intarkdb_prepare_errmsg(self.__statement, self._network)
                        raise ProgrammingError(ret, msg)
                    
                # 执行 SQL
                ret = self.__sql.intarkdb_execute_prepared(
                    self.__statement, self.__resultset, self._network)
                if SQL_SUCCESS == ret:
                    print("intarkdb execute prepared success")
                else:
                    print("intarkdb execute prepared fail")
                    msg = self.__sql.intarkdb_prepare_errmsg(self.__statement, self._network)
                    raise ProgrammingError(ret, msg)

                # 销毁 prepare
                self.__sql.intarkdb_destroy_prepare(self.__statement, self._network)
                print("intarkdb destroy prepare success")

            # 不使用占位符
            else:
                ret = self.__sql.intarkdb_query(conn._conn, operation, self.__resultset, self._network)

            if (SQL_SUCCESS == ret):
                # 从结果集获取行数
                self.rowcount = self.__sql.intarkdb_row_count(self.__resultset, self._network)
                # 从结果集获取列数
                self.colcount = self.__sql.intarkdb_column_count(self.__resultset, self._network)

                if(self.colcount > 0):
                    # 更新description
                    description = []
                    for col in range(self.colcount):
                        result = []
                        col_name = self.__sql.intarkdb_column_name(self.__resultset, col, self._network)
                        result.append(col_name)
                        col_type = self.__sql.intarkdb_column_type(self.__resultset, col, self._network)
                        result.append(col_type)
                        result += [None, None, None, None, None]
                        description.append(tuple(result))
                    self.description = tuple(description)
                
                # 更新rownumber
                self.rownumber = 0

                return self

            # 查询失败
            else:
                print("intarkdb query fail")
                msg = self.__sql.intarkdb_result_msg(self.__resultset, self._network)
                raise ProgrammingError(ret, msg)


    def executemany(self, operation, seq_of_parameters):
        # 判断游标是否开启
        if (self.__status == False):
            raise ProgrammingError(SQL_ERROR, "cursor already closed")
        if (self._kv == True):
            raise ProgrammingError(SQL_ERROR, "use get, set or del instead!")

        # 连接对象
        conn = self.connection
        
        with Cursor.CURSOR_LOCK:
            # 准备prepared_statement
            if(self._network == False):
                ret ,self.__statement = self.__sql.intarkdb_prepare(conn._conn, operation)
            else:
                ret ,self.__statement = self.__sql.intarkdb_client_prepare(conn._conn, operation)
            if SQL_SUCCESS == ret:
                print("intarkdb prepare success")
            else:
                print("intarkdb prepare fail")
                msg = self.__sql.intarkdb_prepare_errmsg(self.__statement, self._network)
                raise ProgrammingError(ret, msg)

            # 绑定数据
            for row in range(len(seq_of_parameters)):
                for col in range(len(seq_of_parameters[row])):
                    param_idx = col + 1
                    val = seq_of_parameters[row][col]
                    type_code = self.__typecode(val)
                    
                    if(self._network == False):
                        if (type_code == SQL_TYPE_DATE or type_code == SQL_TYPE_TIMESTAMP):
                            val = str(val)
                            bind_code = SQL_TYPE_VARCHAR
                            ret = self.__sql.intarkdb_bind_value(self.__statement, param_idx, val, bind_code)
                        else:
                            ret = self.__sql.intarkdb_bind_value(self.__statement, param_idx, val, type_code)
                    # if(self._network == True):
                    else:
                        if (type_code == SQL_TYPE_DATE or type_code == SQL_TYPE_TIMESTAMP):
                            val = str(val)
                            bind_code = SQL_TYPE_VARCHAR
                            ret = self.__sql.intarkdb_client_bind_value(self.__statement, param_idx, val, bind_code)
                        else:
                            ret = self.__sql.intarkdb_client_bind_value(self.__statement, param_idx, val, type_code) 

                    if(ret == SQL_ERROR):
                        print("intarkdb client bind value fail")
                        msg = self.__sql.intarkdb_prepare_errmsg(self.__statement, self._network)
                        raise ProgrammingError(ret, msg)

                # 执行 SQL
                ret = self.__sql.intarkdb_execute_prepared(self.__statement, self.__resultset, self._network)
                if SQL_SUCCESS == ret:
                    print("intarkdb execute prepared success, row is", row + 1)
                else:
                    print("intarkdb execute prepared fail, row is", row + 1)
                    msg = self.__sql.intarkdb_prepare_errmsg(self.__statement, self._network)
                    raise ProgrammingError(ret, msg)

            # 销毁 prepare
            self.__sql.intarkdb_destroy_prepare(self.__statement, self._network)
            print("intarkdb destroy prepare success")

            if (SQL_SUCCESS == ret):
                print("intarkdb query(many) success")

                # 从结果集获取行数
                self.rowcount = self.__sql.intarkdb_row_count(self.__resultset, self._network)
                # 从结果集获取列数
                self.colcount = self.__sql.intarkdb_column_count(self.__resultset, self._network)

                # 更新description
                description = []
                for col in range(self.colcount):
                    result = []
                    col_name = self.__sql.intarkdb_column_name(self.__resultset, col, self._network)
                    result.append(col_name)
                    col_type = self.__sql.intarkdb_column_type(self.__resultset, col, self._network)
                    result.append(col_type)
                    result += [None, None, None, None, None]
                    description.append(tuple(result))
                self.description = tuple(description)

                # 更新rownumber
                self.rownumber = 0

                return self

            # 查询失败
            else:
                print("intarkdb query(many) fail")
                msg = self.__sql.intarkdb_result_msg(self.__resultset, self._network)
                raise ProgrammingError(ret, msg)


    # 取结果集的一行
    def fetchone(self):
        if(self.__status == False):
            raise ProgrammingError(SQL_ERROR, "cursor already closed")
        if (self._kv == True):
            raise ProgrammingError(SQL_ERROR, "use get instead!")
        if(self.rownumber >= self.rowcount):
            return None

        ls = []
        for col in range(self.colcount):
            col_value = self.__sql.intarkdb_value(
                self.__resultset, self.rownumber, col, self.description[col][1], self._network)
            ls.append(col_value)

        self.rownumber += 1
        result = tuple(ls)
        return result


    # 取结果集的剩余所有行
    def fetchall(self):
        if(self.__status == False):
            raise ProgrammingError(SQL_ERROR, "cursor already closed")
        if (self._kv == True):
            raise ProgrammingError(SQL_ERROR, "use get instead!")
        if(self.rownumber >= self.rowcount):
            return []

        result_all = []

        while(self.rownumber < self.rowcount):
            ls = []
            for col in range(self.colcount):
                col_value = self.__sql.intarkdb_value(
                    self.__resultset, self.rownumber, col, self.description[col][1], self._network)
                ls.append(col_value)

            self.rownumber += 1
            result = tuple(ls)
            result_all.append(result) 

        return result_all
    

    # 取结果集的若干行
    def fetchmany(self, arraysize=1):
        if(self.__status == False):
            raise ProgrammingError(SQL_ERROR, "cursor already closed")
        if (self._kv == True):
            raise ProgrammingError(SQL_ERROR, "use get instead!")
        if(self.rownumber >= self.rowcount):
            return []
        if(arraysize >= self.rowcount - self.rownumber):
            return self.fetchall()

        result_many = []
        for row in range(arraysize):
            ls = []
            for col in range(self.colcount):
                col_value = self.__sql.intarkdb_value(
                    self.__resultset, self.rownumber, col, self.description[col][1], self._network)
                ls.append(col_value)

            self.rownumber += 1
            result = tuple(ls)
            result_many.append(result)
        return result_many


    # 关闭游标
    def close(self):
        if self.__status:
            # 清空并销毁结果集
            self.__sql.intarkdb_destory_result(self.__resultset, self._network)          

            # 游标状态设置为关闭
            self.__status = False
            print("cursor shutdown")
        else:
            print("Warning: cursor closed!")


    # 要求的方法，在这里不做任何事情
    def setinputsizes(self, sizes):
        pass


    # 要求的方法，在这里不做任何事情
    def setoutputsizes(self, size, column=None):
        pass


    # 打开表，is_memory为true表示内存表，false表示磁盘表
    def open_table(self, table_name, is_memory = False):
        if(self._kv == False):
            raise ProgrammingError(SQL_ERROR, "use execute instead!")

        # 连接对象
        conn = self.connection
        # 打开表
        self.__sql.intarkdb_open_table(conn._conn, table_name, is_memory)


    # 设置键值对
    def set(self, key, value):
        if(self._kv == False):
            raise ProgrammingError(SQL_ERROR, "use execute instead!")
        
        # 连接对象
        conn = self.connection
        # 设置键值对
        ret = self.__sql.intarkdb_set(conn._conn, key, value)    

        if(ret.contents.type != SQL_SUCCESS):
            raise ProgrammingError(ret.contents.type, "intarkdb set fail")
        
        return ret.contents.type


    # 获取键值对
    def get(self, key):
        if(self._kv == False):
            raise ProgrammingError(SQL_ERROR, "use execute instead!")

        # 连接对象
        conn = self.connection
        # 获取键值对
        ret = self.__sql.intarkdb_get(conn._conn, key)
        
        if(ret.contents.type!= SQL_SUCCESS):
            raise ProgrammingError(ret.contents.type, "intarkdb get fail")
        
        if ret.contents.str:
            return ret.contents.str.decode('utf-8')
        else:
            return None


    # 删除键值对
    def delete(self, key):
        if(self._kv == False):
            raise ProgrammingError(SQL_ERROR, "use execute instead!")
        
        # 连接对象
        conn = self.connection
        # 删除键值对
        ret = self.__sql.intarkdb_del(conn._conn, key)

        if(ret.contents.type!= SQL_SUCCESS):
            raise ProgrammingError(ret.contents.type, "intarkdb delete fail")
        
        return ret.contents.type


    # 类型转换
    def __typecode(self, val):
        if isinstance(val, int):
            if(val >= -128 and val <= 127):
                type_code = SQL_TYPE_TINYINT
            elif(val >= -32768 and val <= 32767):
                type_code = SQL_TYPE_SMALLINT
            elif(val >= -2147483648 and val <= 2147483647):
                type_code = SQL_TYPE_INTEGER
            else:
                type_code = SQL_TYPE_BIGINT
        if isinstance(val, int):
            type_code = SQL_TYPE_BOOLEAN
        elif isinstance(val, (float, Decimal)):
            type_code = SQL_TYPE_REAL
        elif isinstance(val, str):
            type_code = SQL_TYPE_VARCHAR
        elif isinstance(val, Date):
            type_code = SQL_TYPE_DATE
        elif isinstance(val, Timestamp):
            type_code = SQL_TYPE_TIMESTAMP
        else:
            type_code = None
        return type_code
