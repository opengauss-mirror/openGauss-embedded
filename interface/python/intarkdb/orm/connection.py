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
# * openGauss-embedded/interface/python/intarkdb/orm/connection.py
# *
# * -------------------------------------------------------------------------
#  */

from sqlalchemy.engine import default
from intarkdb.orm.cursor import IntarkDBCursor
from sqlalchemy.engine.base import RootTransaction


class IntarkDBConnection(default.Connection):
    def __init__(self, connection):
        self.connection = connection


    def cursor(self):
        return IntarkDBCursor(self.connection)
    

    def execute(self, statement, *args, **kwargs):
        # 实现执行 SQL 语句的逻辑
        cursor = self.connection.cursor()
        cursor.execute(statement, *args)
        return cursor


    def begin(self):
        # 开始事务
        self.connection.begin()
    

    def commit(self):
        # 提交事务
        self.connection.commit()


    def rollback(self):
        # 回滚事务
        self.connection.rollback()


    def close(self):
        # 关闭连接
        self.connection.close()


    def scalar(self, statement, *args, **kwargs):
        result = self.execute(statement, *args, **kwargs).fetchone()
        return result[0] if result else None


    def fetchone(self):
        return self.cursor.fetchone()


    def fetchall(self):
        return self.cursor.fetchall()


    def execute_many(self, statement, parameters):
        cursor = self.connection.cursor()
        cursor.executemany(statement, parameters)
        return cursor
    
