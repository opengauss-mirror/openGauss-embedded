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
# * dialect.py
# *
# * IDENTIFICATION
# * openGauss-embedded/interface/python/intarkdb/orm/dialect.py
# *
# * -------------------------------------------------------------------------
#  */

from sqlalchemy.engine import default
from sqlalchemy import text
from intarkdb.module.StandardError import DatabaseError
from intarkdb import intarkdb
from intarkdb.orm.connection import IntarkDBConnection
from types import ModuleType


class IntarkDBDialect(default.DefaultDialect):

    name = "IntarkDB"
    supports_statement_cache = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.driver = kwargs.get('driver', 'default_driver')  # 设置默认值

    @classmethod
    def import_dbapi(cls) -> ModuleType:
        return intarkdb


    def create_engine(self, url, **kwargs):
        return default.DefaultDialect.create_engine(self, url, **kwargs)
    

    def create_connect_args(self, url):
        # 返回位置参数和关键字参数
        return ([], {
            'user': url.username,
            'password': url.password,
            'host': url.host,
            'port': url.port,
            'database': url.database,
        })


    def raw_connection(self, url):
        # 获取连接参数
        args, kwargs = self.create_connect_args(url)

        user = kwargs.get('user')
        password = kwargs.get('password')
        host = kwargs.get('host')
        port = kwargs.get('port')
        database = kwargs.get('database')

        # 创建并返回 DBAPI 连接
        return intarkdb.connect(database = database, host = host, port = port, user = user, password = password)
    

    def create_connection(self, connection_record):
        # 从连接记录中获取 URL
        url = connection_record.connection_url
        # 创建 DBAPI 连接
        dbapi_connection = self.raw_connection(url)

        # 返回自定义连接对象
        return IntarkDBConnection(dbapi_connection)


    def is_disconnect(self, exception, error=None, connection=None):
        if isinstance(exception, (DatabaseError)):
            return True
        return False


    def do_execute(self, cursor, statement, parameters, context=None):
        cursor.execute(statement, parameters)


    def do_fetchone(self, cursor):
        return cursor.fetchone()


    def do_fetchall(self, cursor):
        return cursor.fetchall()


    def has_table(self, connection, table_name, schema=None):
        # 实现检查表存在的逻辑
        result = connection.execute(
            text("SELECT name FROM 'SYS_TABLES' WHERE name=:table_name"),
            {"table_name": table_name}
        ).fetchone()
        return result is not None
    

    def get_schema_names(self, connection=None):
        # 实现获取模式名称的逻辑
        return []
