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
# * example_connection.py
# *
# * IDENTIFICATION
# * openGauss-embedded/examples/example-python/example_connection.py
# *
# * -------------------------------------------------------------------------
#  */

from intarkdb import intarkdb

def connection_example():
    print("1. 连接intarkdb, 可设置isolation_level为None, 使得事务手动开启")
    conn = intarkdb.connect(database='.', isolation_level = None)
    print("-------------------------------")

    print("2. connection对象的cursor方法")
    conn.cursor()
    print("-------------------------------")

    print("3. 开启事务")
    conn.begin()
    print("-------------------------------")

    print("4. connection对象的execute方法")
    sql_str1 = "DROP TABLE IF EXISTS test_table;"
    print("执行:",sql_str1)
    conn.execute(sql_str1)
    print("*****************")

    sql_str2 = "CREATE TABLE test_table (ID INTEGER, ACCTID INTEGER, CHGAMT VARCHAR(20), BAK1 VARCHAR(20), BAK2 VARCHAR(20), PRIMARY KEY (ID,ACCTID));"
    print("执行:",sql_str2)
    conn.execute(sql_str2)
    print("*****************")

    sql_str3 = "INSERT INTO test_table VALUES (1, 11, 'AAA', 'b1', 'b2'), (2, 22, 'BBB', 'b1', 'b2');"
    print("执行:",sql_str3)
    c = conn.execute(sql_str3)
    print("*****************")

    sql_str4 = "SELECT ID,ACCTID,CHGAMT,BAK1,BAK2 FROM test_table;"
    print("执行:",sql_str4)
    c = conn.execute(sql_str4)
    print("调用fetch方法, 得到的结果集为:",c.fetchall())
    print("*****************")

    sql_str5 = "UPDATE test_table SET CHGAMT = 'ABC' WHERE ID = 1;"
    print("执行:",sql_str5)
    conn.execute(sql_str5)
    print("接着执行:",sql_str4)
    c = conn.execute(sql_str4)
    print("调用fetch方法, 得到的结果集为:",c.fetchall())  
    print("*****************")   

    sql_str6 = "DELETE FROM test_table WHERE ID = 2;"
    print("执行:",sql_str6)
    conn.execute(sql_str6)
    print("接着执行:",sql_str4)
    c = conn.execute(sql_str4)
    print("调用fetch方法, 得到的结果集为:",c.fetchall())  
    print("*****************")   

    sql_str7 = "INSERT INTO test_table VALUES(?,?,?,?,?);"
    rows = [(3,33,'CCC','b1','b2'), (4,44,'DDD','b1','b2'),(5,55,'EEE','b1','b2'), (6,66,'FFF','b1','b2')]
    print("执行:",sql_str7,"参数为",rows)
    conn.executemany(sql_str7,rows)
    print("接着执行:",sql_str4)
    c = conn.execute(sql_str4)
    print("调用fetch方法, 得到的结果集为:",c.fetchall())  
    print("-------------------------------")

    print("5. 提交事务")
    conn.commit()
    print("-------------------------------")

    print("6. 回滚事务")
    conn.rollback()
    print("-------------------------------")

    print("7. 关闭连接，关闭数据库")
    conn.close()
    print("-------------------------------")


if __name__ == "__main__":
    print("-----connection连接对象example-----")
    connection_example()
    