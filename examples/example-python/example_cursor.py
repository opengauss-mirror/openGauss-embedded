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
# * example_cursor.py
# *
# * IDENTIFICATION
# * openGauss-embedded/examples/example-python/example_cursor.py
# *
# * -------------------------------------------------------------------------
#  */

from intarkdb import intarkdb

def cursor_example():
    print("1. 连接intarkdb")
    conn = intarkdb.connect()
    print("-------------------------------")

    print("2. 创建 cursor 对象")
    c = conn.cursor()
    print("-------------------------------")

    print("3. execute方法")
    sql_str1 = "DROP TABLE IF EXISTS TEST_TABLE;"
    print("执行:",sql_str1)
    c.execute(sql_str1)
    print("*****************")

    sql_str2 = "CREATE TABLE TEST_TABLE (ID INTEGER, NAME VARCHAR(20), AGE FLOAT, GENDER BOOLEAN, PRIMARY KEY (ID));"
    print("执行:",sql_str2)
    c.execute(sql_str2)
    print("*****************")

    sql_str3 = "INSERT INTO TEST_TABLE VALUES (1, 'TOM', 23.0, True), (2, 'AMY', 25.0, False);"
    print("执行:",sql_str3)
    c.execute(sql_str3)
    print("*****************")

    sql_str4 = "SELECT ID,NAME,AGE,GENDER FROM TEST_TABLE;"
    print("执行:",sql_str4)
    c.execute(sql_str4)
    print("调用fetchall方法, 得到的结果集为:",c.fetchall())
    print("*****************")

    sql_str5 = "UPDATE TEST_TABLE SET AGE = 30 WHERE ID = 1;"
    print("执行:",sql_str5)
    c.execute(sql_str5)
    print("接着执行:",sql_str4)
    c.execute(sql_str4)
    print("调用fetchall方法, 得到的结果集为:",c.fetchall())    
    print("*****************")

    sql_str6 = "DELETE FROM TEST_TABLE WHERE ID = 1;"
    print("执行:",sql_str6)
    c.execute(sql_str6)
    print("接着执行:",sql_str4)
    c.execute(sql_str4)
    print("调用fetchall方法, 得到的结果集为:",c.fetchall()) 
    print("*****************")   

    sql_str7 = "INSERT INTO TEST_TABLE VALUES(?,?,?,?);"
    rows = [(3, 'MIKE', 18.0, True), (4, 'lily', 35.3, False),(5, 'JACK', 60.1, True), (6, 'BOBO', 19.0, False),(7, 'lee', 55.0, True)]
    print("执行:",sql_str7,"参数为",rows)
    c.executemany(sql_str7,rows)
    print("接着执行:",sql_str4)
    c.execute(sql_str4)
    print("调用fetchall方法, 得到的结果集为:",c.fetchall())     
    print("-------------------------------")

    print("4. fetch方法")
    print("首先执行SELETE语句, 使得rownumner归零")
    print("执行:",sql_str4)
    c.execute(sql_str4)
    print("*****************")

    print("A. fetchone方法")
    result = c.fetchone()
    print("结果为",result)
    print("*****************")  

    print("B. fetchmany方法")
    print("取arraysize为2")
    result2 = c.fetchmany(2)
    print("结果为",result2)
    print("*****************")  

    print("C. fetchall方法")
    result3 = c.fetchall()
    print("结果为",result3)
    print("-------------------------------")

    print("5. 关闭游标")
    c.close()
    print("-------------------------------")   

    print("6. 关闭连接，关闭数据库")
    conn.close()
    print("-------------------------------")

if __name__ == "__main__":
    print("-----cursor游标对象example-----")
    cursor_example()