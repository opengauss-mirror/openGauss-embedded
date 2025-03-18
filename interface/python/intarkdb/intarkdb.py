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
# * intarkdb.py
# *
# * IDENTIFICATION
# * openGauss-embedded/interface/python/intarkdb/intarkdb.py
# *
# * -------------------------------------------------------------------------
#  */

from intarkdb.module.connection import Connection
from intarkdb.module.StandardError import InterfaceError, Error
from intarkdb.module.model import INTARKDB_NUM 

paramstyle = "qmark"
threadsafety = 1
apilevel = "2.0"

# 用户建立与数据库的连接
# 本地访问数据库可以不填写任何参数
# 远程访问数据库必须填写database, host, port, user, password
def connect(database = ".", host = None, port = None, user = None, password = None):

    conn = Connection(database=database, host=host, port=port, user=user, password=password)

    if (host == None and port == None and user == None and password == None):
        if database not in INTARKDB_NUM or INTARKDB_NUM[database] == 0:
            conn._open()
            INTARKDB_NUM[database] = 1
        else:
            INTARKDB_NUM[database] += 1
        conn._connect()
    elif (host and port and user and password):
        conn._connect()
    else:
        raise InterfaceError(-1, "host and port and user and password must be all set")

    return conn


# 用户建立与KV数据库的连接
def connect_kv(database = "."):

    conn = Connection(database=database)

    if database not in INTARKDB_NUM or INTARKDB_NUM[database] == 0:
        conn._open()
        INTARKDB_NUM[database] = 1
    else:
        INTARKDB_NUM[database] += 1

    conn._connect_kv()

    return conn
