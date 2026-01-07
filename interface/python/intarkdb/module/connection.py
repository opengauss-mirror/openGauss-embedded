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
# * connection.py
# *
# * IDENTIFICATION
# * openGauss-embedded/interface/python/intarkdb/module/connection.py
# *
# * -------------------------------------------------------------------------
#  */

from intarkdb.module.StandardError import InterfaceError, DatabaseError, Error, ProgrammingError
from intarkdb.module.model import INTARKDB_DATABASE, SQL_SUCCESS, SQL_ERROR
from intarkdb.module.sql import SQL
from intarkdb.module.cursor import Cursor
from intarkdb.module.model import INTARKDB_NUM
import threading


class Connection:

    DATABASE = INTARKDB_DATABASE()
    CONNECTION_LOCK = threading.Lock()

    def __init__(self, database = ".", host = None, port = None, user = None, password = None):
        self._path = database   
        self._conn = None
        self._kv = False

        if (host == None and port == None and user == None and password == None):
            self._network = False
            self.__sql = SQL(Connection.DATABASE, self._network)
        elif (host and port and user and password):
            self._network = True
            self._host = host
            self._port = port
            self._user = user
            self._password = password
            self.__sql = SQL(Connection.DATABASE, self._network)
            Connection.DATABASE = database
        else:
            raise InterfaceError(SQL_ERROR, "host and port and user and password must be all set")

            
    def __del__(self):
        pass


    # 打开数据库
    def _open(self):
        with Connection.CONNECTION_LOCK:
            # 打开本地数据库
            ret, Connection.DATABASE = self.__sql.open_db(self._path)
            if SQL_SUCCESS == ret:
                print("open database success, path = {}".format(self._path))
            else:
                raise InterfaceError(ret, "open database fail")
        

    # 连接数据库
    def _connect(self):
        with Connection.CONNECTION_LOCK:
            # 连接本地数据库
            if (self._network == False):
                ret ,self._conn = self.__sql.intarkdb_connect()
            # 连接远程数据库
            if (self._network == True):
                ret ,self._conn = self.__sql.intarkdb_client_connect(
                    self._path, self._host, self._port, self._user, self._password)

        if SQL_SUCCESS == ret:
            print("intarkdb connect success")
        else:
            raise InterfaceError(SQL_ERROR, "intarkdb connect fail")
        

    # 连接KV数据库
    def _connect_kv(self):
        with Connection.CONNECTION_LOCK:
            kv = True
            ret ,self._conn = self.__sql.intarkdb_connect(kv)

        if SQL_SUCCESS == ret:
            self._kv = True
            print("intarkdb connect kv success")
        else:
            raise InterfaceError(SQL_ERROR, "intarkdb connect kv fail")


    # 打开游标
    def cursor(self):
        cur = Cursor(conn = self, database = Connection.DATABASE, network = self._network, kv = self._kv)
        if cur:
            print("create cursor success")
            return cur  
        else:
            raise Error(SQL_ERROR, "create cursor fail")

            
    # 启动事务，数据库默认自动开启事务
    def begin(self):
        if(self._kv == True):
            raise ProgrammingError(SQL_ERROR, "use multi instead!")

        operation = "begin"
        ret = self.__sql.intarkdb_query(self._conn, operation, None, self._network)
        if SQL_SUCCESS == ret:
            print("transaction begin success")
            return ret
        else:
            raise DatabaseError(ret, "transaction begin fail")

        
    # 提交事务
    def commit(self):
        if(self._kv == True):
            raise ProgrammingError(SQL_ERROR, "use exec instead!")

        operation = "commit"
        ret = self.__sql.intarkdb_query(self._conn, operation, None, self._network)
        if SQL_SUCCESS == ret:
            print("intarkdb commit success")
            return ret
        else:
            raise DatabaseError(ret, "intarkdb commit fail")
 

    # 回滚事务
    def rollback(self):
        if(self._kv == True):
            raise ProgrammingError(SQL_ERROR, "use discard instead!")

        operation = "rollback"
        ret = self.__sql.intarkdb_query(self._conn, operation, None, self._network)
        if SQL_SUCCESS == ret:
            print("intarkdb rollback success")
            return ret
        else:
            raise DatabaseError(ret, "intarkdb rollback fail")
        

    # 启动kv事务
    def multi(self):
        if(self._kv == False):
            raise ProgrammingError(SQL_ERROR, "use begin instead!")

        ret = self.__sql.intarkdb_multi(self._conn)
        if SQL_SUCCESS == ret:
            print("transaction multi success")
            return ret
        else:
            raise DatabaseError(ret, "transaction multi fail")
        

    # 提交kv事务
    def exec(self):
        if(self._kv == False):
            raise ProgrammingError(SQL_ERROR, "use commit instead!")

        ret = self.__sql.intarkdb_exec(self._conn)
        if SQL_SUCCESS == ret:
            print("intarkdb exec success")
            return ret
        else:
            raise DatabaseError(ret, "intarkdb exec fail")
        
    
    # 回滚kv事务
    def discard(self):
        if(self._kv == False):
            raise ProgrammingError(SQL_ERROR, "use rollback instead!")

        ret = self.__sql.intarkdb_discard(self._conn)
        if SQL_SUCCESS == ret:
            print("intarkdb discard success")
            return ret
        else:
            raise DatabaseError(ret, "intarkdb discard fail")


    # 关闭连接
    def close(self):
        if (self._kv == True):
            raise ProgrammingError(SQL_ERROR, "use close_kv instead!")

        with Connection.CONNECTION_LOCK:
            self.__sql.intarkdb_disconnect(self._conn, self._network)
            self._conn = None
            print("intarkdb disconnect success")
            if(self._network == False):
                INTARKDB_NUM[self._path] -= 1

            # 关闭数据库
            if (self._network == False and INTARKDB_NUM[self._path] == 0):
                self.__sql.close_db()  
                print("close db success!")  

    
    # 关闭kv连接
    def close_kv(self):
        if (self._kv == False):
            raise ProgrammingError(SQL_ERROR, "use close instead!")
        
        with Connection.CONNECTION_LOCK:
            self.__sql.intarkdb_disconnect(self._conn, self._network, self._kv)
            self._conn = None
            print("intarkdb disconnect kv success")

            # 关闭数据库
            if (INTARKDB_NUM[self._path] == 0):
                self.__sql.close_db()  
                print("close db success!")  
                

    # 执行SQL语句
    def execute(self, operation, parameters = None):
        if(self._kv == True):
            raise ProgrammingError(SQL_ERROR, "use set, get or del instead!")

        cur = Cursor(conn = self, database = Connection.DATABASE, network = self._network)  
        cur.execute(operation, parameters)   
        return cur  


    # 执行多条SQL语句
    def executemany(self, operation, seq_of_parameters):  
        if(self._kv == True):
            raise ProgrammingError(SQL_ERROR, "use set, get or del instead!")

        cur = Cursor(conn = self, database = Connection.DATABASE, network = self._network)   
        cur.executemany(operation, seq_of_parameters)
        return cur  


    # 打开表
    def open_table(self, table_name, is_memory):
        if(self._kv == False):
            raise ProgrammingError(SQL_ERROR, "use execute instead!")
        
        cur = Cursor(conn = self, database = Connection.DATABASE, network = self._network, kv = self._kv)
        cur.open_table(table_name, is_memory)
        return cur
    

    # 设置值
    def set(self, key, value):
        if(self._kv == False):
            raise ProgrammingError(SQL_ERROR, "use execute instead!")
        
        cur = Cursor(conn = self, database = Connection.DATABASE, network = self._network, kv = self._kv)
        cur.set(key, value)
        return cur
    

    # 获取值
    def get(self, key):
        if(self._kv == False):
            raise ProgrammingError(SQL_ERROR, "use execute instead!")
        
        cur = Cursor(conn = self, database = Connection.DATABASE, network = self._network, kv = self._kv)
        cur.get(key)
        return cur


    # 删除值
    def delete(self, key):
        if(self._kv == False):
            raise ProgrammingError(SQL_ERROR, "use execute instead!")

        cur = Cursor(conn = self, database = Connection.DATABASE, network = self._network, kv = self._kv)
        cur.delete(key)
        return cur
