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
# * openGauss-embedded/interface/python/intarkdb/orm/cursor.py
# *
# * -------------------------------------------------------------------------
#  */


class IntarkDBCursor:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = connection.cursor()


    def execute(self, statement, *args, **kwargs):
        self.cursor.execute(statement, *args, **kwargs)
        return self
    

    def fetchone(self):
        return self.cursor.fetchone()
    

    def fetchall(self):
        return self.cursor.fetchall()


    def fetchmany(self, size=None):
        return self.cursor.fetchmany(size)
    

    def close(self):
        self.cursor.close()


    def setinputsizes(self, *args):
        self.cursor.setinputsizes(*args)


    def setoutputsizes(self, *args):
        self.cursor.setoutputsizes(*args)

