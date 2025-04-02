/*
 * Copyright (c) GBA-NCTI-ISDC. 2022-2024.
 *
 * openGauss embedded is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITFOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * select_test.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/test/select_test.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <gtest/gtest.h>

#include <ctime>
#include <iomanip>
#include <iostream>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "main/connection.h"
#include "main/database.h"

class ConnectionForTest : public ::testing::Test {
   protected:
    ConnectionForTest() {}
    ~ConnectionForTest() {}
    static void SetUpTestSuite() {
        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance->Init();
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();

        // // prepare data
        conn->Query("create table select_test_t1 (sid integer , name varchar(20), age smallint , gpa decimal(5,2))");
        conn->Query(
            "insert into select_test_t1 values "
            "(1,'Kanye',20,3.5),(2,'Alice',20,3.6),(3,'Bob',25,3.7),(4,'Cindy',30,3.5),(5,'David',24,3.5)");
        conn->Query(
            "CREATE TABLE employees ( employee_id INT PRIMARY KEY, employee_name VARCHAR(50),department "
            "VARCHAR(50),salary DECIMAL(10, 2))");
        conn->Query(
            "INSERT INTO employees (employee_id, employee_name, department, salary) VALUES (1, 'Alice', 'Sales', "
            "5000.00),(2, 'Bob', 'Sales', 6000.00),(3, 'Charlie', 'Marketing', 5500.00),(4, 'David', 'Marketing', "
            "7000.00),(5, 'Eve', 'Finance', 8000.00),(6, 'Frank', 'Finance', 6500.00)");

        conn->Query("CREATE TABLE select_test_t2 (a INTEGER, b INTEGER)");
        conn->Query("INSERT INTO select_test_t2 VALUES (11, 22), (13, 22), (12, 21)");
    }

    // Per-test-suite tear-down.
    // Called after the last test in this test suite.
    // Can be omitted if not needed.
    static void TearDownTestSuite() {}

    void SetUp() override {}

    // void TearDown() override {}

    static std::shared_ptr<IntarkDB> db_instance;
    static std::unique_ptr<Connection> conn;
};

std::shared_ptr<IntarkDB> ConnectionForTest::db_instance = nullptr;
std::unique_ptr<Connection> ConnectionForTest::conn = nullptr;

TEST_F(ConnectionForTest, SelectInValidTable) {
    auto r = conn->Query("select * from select_test_invalid");
    EXPECT_EQ(r->GetRetCode(), -1);
}

TEST_F(ConnectionForTest, SelectResultCount) {
    auto r = conn->Query("select * from select_test_t1");
    EXPECT_EQ(r->RowCount(), 5);
    // with where
    r = conn->Query("select * from select_test_t1 where sid > 3");
    EXPECT_EQ(r->RowCount(), 2);
}

TEST_F(ConnectionForTest, SelectWithWhereFilter) {
    // multi condition
    auto r = conn->Query("select * from select_test_t1 where sid > 3 and age < 30");
    EXPECT_EQ(r->RowCount(), 1);

    r = conn->Query("select * from select_test_t1 where sid < 3 or sid > 4");
    EXPECT_EQ(r->RowCount(), 3);

    conn->Query("drop table if exists integers");
    conn->Query("CREATE TABLE integers(i INTEGER)");
    conn->Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)");
    r = conn->Query("SELECT i FROM integers WHERE (i=1 OR 1=0 OR i=1) AND (0=1 OR 1=0 OR 1=1) ORDER BY i");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);

    // 测试 条件合并优化
    r = conn->Query("select i from integers where i >= 2 and i > 2");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 3);

    r = conn->Query("select i from integers where i = 2 and i >= 2");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);

    r = conn->Query("select i from integers where i = 2 and i > 2");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 0);

    r = conn->Query("select 1 as k , sum(i) from integers group by k +1 order by 2");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 6);

    r = conn->Query("drop table if exists users");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("create table users (user_id int PRIMARY KEY, name varchar(10), age int)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("insert into users values (1, 'john_doe', 25), (2, 'jane_doe', 30), (3, 'bob_smith', 22)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select * from users where user_id > 1 and name <> 'jane_doe'");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "bob_smith");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 22);

    r = conn->Query("drop table if exists orders");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("create table orders (id int PRIMARY KEY, user_id int, product varchar,order_date date)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query(
        "insert into orders values (1, 1, 'shoes', '2019-01-01'), (2, 1, 'socks', '2019-01-01'), (3, 2, 'shoes', "
        "'2019-01-02'), (4, 3, 'shoes', '2019-01-02')");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query(
        "select * from users , orders where users.user_id = orders.user_id and users.user_id > 1 and orders.user_id > "
        "2");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "bob_smith");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 22);
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->Row(0).Field(4).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(0).Field(5).GetCastAs<std::string>(), "shoes");
    EXPECT_EQ(r->Row(0).Field(6).GetCastAs<std::string>(), "2019-01-02");

    // is null in where
    conn->Query("drop table if exists t5");
    conn->Query("CREATE TABLE t5(a text, b text, c text)");
    conn->Query("insert into t5 values (null,null,null),(null,'x','1'),('1','x','1')");
    r = conn->Query("select * from t5 where a is null and b='x'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "x");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "1");

    conn->Query("drop table if exists t6");
    conn->Query("create table t6( a int , b int , c int)");
    conn->Query("insert into t6 values (1,2,3),(2,3,4),(3,4,5)");
    conn->Query("create index t6_idx on t6(a,b)");
    conn->Query("create index t6_idx2 on t6(b)");
    r = conn->Query("select * from t6 where b = 2");
    ASSERT_EQ(r->GetRetCode(), 0);
    // TODO CHEKC IS USE INDEX
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 3);
}

TEST_F(ConnectionForTest, SelectWithWhereBewteen) {
    auto r = conn->Query("select sid , age from select_test_t1 where sid between 2 and 4");
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 4);
}

TEST_F(ConnectionForTest, SelectWithWhereIn) {
    auto r = conn->Query("select sid , age from select_test_t1 where sid in (2 ,3, 4)");
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 4);
}

TEST_F(ConnectionForTest, SelectWithLimit) {
    // limit 测试
    auto r = conn->Query("select * from select_test_t1 limit 2");
    EXPECT_EQ(r->RowCount(), 2);
}

TEST_F(ConnectionForTest, SelectWithKeyWordColumnName) {
    // 存在关键字列名，解析器不会把该列名转换为小写
    conn->Query("create table key_word_column_name_tbl(copy int , name int)");
    auto r = conn->Query("select COPY,NAME from key_word_column_name_tbl");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select copy,name from key_word_column_name_tbl");
    ASSERT_EQ(r->GetRetCode(), 0);
}

// 测试别名
TEST_F(ConnectionForTest, SelectWithAlias) {
    // table alias
    auto r = conn->Query("select a.age , gpa from select_test_t1 as a");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select * from select_test_t1 as a");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select * from select_test_t1 a");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query(
        "select age as b from select_test_t1 where select_test_t1.b > 0");  // 别名不能有表名修饰，否则认为是列名
    ASSERT_NE(r->GetRetCode(), 0);

    // table alias  and column alias
    r = conn->Query("SELECT e.employee_name AS name, e.salary AS employee_salary from employees as e");
    EXPECT_EQ(r->GetRetCode(), 0);

    // alias with aggregation
    r = conn->Query("select employee_name , salary + 10000 as new_salary from employees");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select employee_name , sum(salary) as total_salary from employees group by employee_name");
    EXPECT_EQ(r->GetRetCode(), 0);

    // alias with order by
    r = conn->Query(
        "select department , sum(salary) as total_salary from employees group by department order by "
        "total_salary");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "Sales");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<double>(), 11000.00);

    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<std::string>(), "Marketing");
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<double>(), 12500.00);

    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<std::string>(), "Finance");
    EXPECT_EQ(r->Row(2).Field(1).GetCastAs<double>(), 14500.00);

    // subquery with alias
    r = conn->Query("SELECT name FROM (SELECT employee_name as name FROM employees)");
    EXPECT_EQ(r->GetRetCode(), 0);

    // 子查询中包含虚拟列(通过计算生成)
    r = conn->Query("SELECT * FROM (SELECT employee_name , salary + 1000 , -salary FROM employees )");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("SELECT * FROM (SELECT employee_name , salary + 1000 as c FROM employees )");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select employees.employee_name from ( select employee_name from employees) as t");
    EXPECT_NE(r->GetRetCode(), 0);

    // subquery with alias and aggregation
    r = conn->Query(
        "SELECT * FROM (SELECT avg(salary) as avg_salary , department FROM employees GROUP BY department "
        "order by avg_salary)");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<double>(), 5500);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "Sales");
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<double>(), 6250);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<std::string>(), "Marketing");
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<double>(), 7250);
    EXPECT_EQ(r->Row(2).Field(1).GetCastAs<std::string>(), "Finance");

    r = conn->Query("SELECT SUM(salary) , employee_id + 1 as f FROM employees group by f order by f");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 6);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<double>(), 5000);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<double>(), 6000);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<double>(), 5500);
    EXPECT_EQ(r->RowRef(2).Field(1).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(3).Field(0).GetCastAs<double>(), 7000);
    EXPECT_EQ(r->RowRef(3).Field(1).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(4).Field(0).GetCastAs<double>(), 8000);
    EXPECT_EQ(r->RowRef(4).Field(1).GetCastAs<int32_t>(), 6);
    EXPECT_EQ(r->RowRef(5).Field(0).GetCastAs<double>(), 6500);
    EXPECT_EQ(r->RowRef(5).Field(1).GetCastAs<int32_t>(), 7);

    // 测试别名优先级（列名与别名出现冲突，不同子句中，使用别名 还是 使用列名)
    r = conn->Query("SELECT -salary as salary from employees order by salary");  // order by 优先使用别名
    EXPECT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 6);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<double>(), -8000);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<double>(), -7000);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<double>(), -6500);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<double>(), -6000);
    EXPECT_EQ(r->Row(4).Field(0).GetCastAs<double>(), -5500);
    EXPECT_EQ(r->Row(5).Field(0).GetCastAs<double>(), -5000);

    r = conn->Query(
        "select -salary as salary from employees where salary > 5000 order by salary");  // where中优先使用列名
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<double>(), -8000);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<double>(), -7000);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<double>(), -6500);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<double>(), -6000);
    EXPECT_EQ(r->Row(4).Field(0).GetCastAs<double>(), -5500);

    conn->Query("create table alias_t1(id int , name varchar(10))");
    conn->Query("create table alias_t2(id int , name varchar(10))");
    conn->Query("insert into alias_t1 values (1 , 'SQL')");
    conn->Query("insert into alias_t2 values (1 , 'Alias')");
    r = conn->Query("select alias_t1.id , alias_t1.name from alias_t1 t cross join alias_t2 alias_t1");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "Alias");

    r = conn->Query("select * as a from alias_t1");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);

    r = conn->Query("select * alias_t1");  // 解析器可以通过，需要在binder过程中报错
    EXPECT_NE(r->GetRetCode(), 0);

    // not support alias in from clause
    r = conn->Query("select id from alias_t1 as t1(a)");
    EXPECT_NE(r->GetRetCode(), 0);

    r = conn->Query("select t1.id , t2.id from alias_t1 as t1(a) , alias_t2 as t2(b)");
    EXPECT_NE(r->GetRetCode(), 0);

    r = conn->Query("select id , name from (select * from alias_t1 , alias_t2) as t(a,b,c,d)");
    EXPECT_NE(r->GetRetCode(), 0);
}

// 去重测试
TEST_F(ConnectionForTest, SelectWithDistinct) {
    auto r = conn->Query("select distinct age from select_test_t1");
    EXPECT_EQ(r->RowCount(), 4);
    r = conn->Query("select distinct gpa from select_test_t1");
    EXPECT_EQ(r->RowCount(), 3);
    r = conn->Query("select distinct age , gpa from select_test_t1");
    EXPECT_EQ(r->RowCount(), 5);

    // test distinct on
    r = conn->Query("select distinct(age) gpa from select_test_t1");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 4);
}

TEST_F(ConnectionForTest, SelectWithSort) {
    // 简单排序
    auto r = conn->Query("select * from select_test_t1 order by age");
    EXPECT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(1).Field(2).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(2).Field(2).GetCastAs<int32_t>(), 24);
    EXPECT_EQ(r->Row(3).Field(2).GetCastAs<int32_t>(), 25);
    EXPECT_EQ(r->Row(4).Field(2).GetCastAs<int32_t>(), 30);

    // 降序排序
    r = conn->Query("select * from select_test_t1 order by age desc");
    EXPECT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 30);
    EXPECT_EQ(r->Row(1).Field(2).GetCastAs<int32_t>(), 25);
    EXPECT_EQ(r->Row(2).Field(2).GetCastAs<int32_t>(), 24);
    EXPECT_EQ(r->Row(3).Field(2).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(4).Field(2).GetCastAs<int32_t>(), 20);

    // 多列排序
    r = conn->Query("select * from select_test_t1 order by age desc, gpa");
    EXPECT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 30);
    EXPECT_EQ(r->Row(1).Field(2).GetCastAs<int32_t>(), 25);
    EXPECT_EQ(r->Row(2).Field(2).GetCastAs<int32_t>(), 24);
    EXPECT_EQ(r->Row(3).Field(2).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(3).Field(3).GetCastAs<double>(), 3.5);
    EXPECT_EQ(r->Row(4).Field(2).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(4).Field(3).GetCastAs<double>(), 3.6);

    // 选择部分列进行排序
    r = conn->Query("select age from select_test_t1 order by age");
    EXPECT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 24);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 25);
    EXPECT_EQ(r->Row(4).Field(0).GetCastAs<int32_t>(), 30);

    r = conn->Query("select age , gpa from select_test_t1 order by 1");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 24);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 25);
    EXPECT_EQ(r->Row(4).Field(0).GetCastAs<int32_t>(), 30);

    r = conn->Query("select age as a , gpa from select_test_t1 order by 1");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 24);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 25);
    EXPECT_EQ(r->Row(4).Field(0).GetCastAs<int32_t>(), 30);

    r = conn->Query("select age + 1 , gpa from select_test_t1 order by 1");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 21);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 21);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 25);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 26);
    EXPECT_EQ(r->Row(4).Field(0).GetCastAs<int32_t>(), 31);

    r = conn->Query("select age , avg(gpa) as a from select_test_t1 group by age order by 2");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 24);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 30);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 25);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<double>(), 3.5);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<double>(), 3.5);
    EXPECT_EQ(r->Row(2).Field(1).GetCastAs<double>(), 3.55);
    EXPECT_EQ(r->Row(3).Field(1).GetCastAs<double>(), 3.7);

    // group by & order by & alias
    r = conn->Query("select age as zzz from select_test_t1 group by zzz order by select_test_t1.age");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 24);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 25);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 30);

    r = conn->Query("select age as zzz from select_test_t1 group by select_test_t1.age order by zzz");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 24);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 25);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 30);

    // 查询列表不在选择列表中
    r = conn->Query("select age from select_test_t1 order by gpa desc");
    EXPECT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 25);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 20);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 30);
    EXPECT_EQ(r->Row(4).Field(0).GetCastAs<int32_t>(), 24);

    // 子查询作为排序条件，且select list中包含聚合函数，主要测试对order by的 PlanSubqueries
    r = conn->Query(
        "SELECT test.b, SUM(a) FROM select_test_t2 test  GROUP BY test.b ORDER BY (SELECT SUM(a) FROM select_test_t2 t "
        "WHERE "
        "test.b=t.b) DESC");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 22);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 24);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 21);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<int32_t>(), 12);

    // 子查询作为排序条件，且select list中不含聚合函数，主要测试对order by的 PlanSubqueries
    r = conn->Query(
        "SELECT test.b FROM select_test_t2 test ORDER BY (SELECT SUM(a) FROM select_test_t2 t "
        "WHERE "
        "test.b=t.b) DESC");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 22);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 22);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 21);

    // sort by agg
    r = conn->Query("SELECT b, SUM(a) FROM select_test_t2 GROUP BY b ORDER BY COUNT(a)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 21);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 12);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 22);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<int32_t>(), 24);

    // sort by cast
    r = conn->Query("select a from select_test_t2 order by cast(b as real)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);

    conn->Query("create sequence agg_test_seq start 10");
    r = conn->Query(
        "SELECT MIN(value), MAX(value), 99 , now(), nextval('agg_test_seq') FROM (SELECT 1 AS value UNION ALL SELECT 2 "
        "UNION ALL SELECT 3) AS subquery");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);

    r = conn->Query(
        "SELECT MIN(value), MAX(value), nextval(s) FROM (SELECT 1 AS value , 'agg_test_seq' as s UNION ALL SELECT 2 , "
        "'abc' ) AS "
        "subquery");
    EXPECT_NE(r->GetRetCode(), 0);

    r = conn->Query(
        "SELECT MIN(value) , now() FROM (SELECT 1 as value UNION ALL SELECT 2 UNION ALL SELECT 3) as t group by "
        "now(),nextval('agg_test_seq')");
    EXPECT_EQ(r->GetRetCode(), 0);

    // 函数只有部分参数出现在group by clasuse
    r = conn->Query("SELECT min(s), substr(s,len) from (select 'abc' as s , 2 as len) as t group by s");
    EXPECT_NE(r->GetRetCode(), 0);

    r = conn->Query("SELECT min(s), substr(s,len) from (select 'abc' as s , 2 as len) as t group by s,len");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query(
        "select min(value) , now() from ( select 1 as value union select 2 ) having nextval('agg_test_seq') > 0 and 2 "
        "> 1");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select max(x) from (select 1 as x union select 2 ) having max(x) > 1");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select min(value) , now() from ( select 1 as value union select 2 ) order by now()");
    EXPECT_EQ(r->GetRetCode(), 0);

    // 测试 nulls first
    conn->Query("drop table if exists null_sort_t1");
    conn->Query("create table null_sort_t1 (a int , b int)");
    conn->Query("insert into null_sort_t1 values (1 , 2) , (null , 3) , (null , 4) , (2 , 5),(null,null)");
    r = conn->Query("select * from null_sort_t1 order by a nulls first,b nulls first");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(1).IsNull(), true);
    EXPECT_EQ(r->Row(1).Field(0).IsNull(), true);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(2).Field(0).IsNull(), true);
    EXPECT_EQ(r->Row(2).Field(1).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(3).Field(1).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(4).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(4).Field(1).GetCastAs<int32_t>(), 5);

    // test order by non-integer constant
    conn->Query("drop table if exists integers");
    conn->Query("CREATE TABLE integers(i INTEGER)");
    conn->Query("INSERT INTO integers VALUES (1), (NULL)");
    r = conn->Query("SELECT 10 AS j, i FROM integers ORDER BY j, i NULLS LAST");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 10);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 10);
    EXPECT_EQ(r->Row(1).Field(1).IsNull(), true);

    r = conn->Query("select * from integers order by '1' , 'abc'");
    ASSERT_EQ(r->GetRetCode(), 0);

    // order by complex expression
    r = conn->Query("select i from integers order by i + 1");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).IsNull(), true);
}

TEST_F(ConnectionForTest, SelectOrderByAlias) {
    conn->Query("drop table if exists tbl");
    conn->Query("create table tbl(a int , b int)");
    conn->Query("insert into tbl values (1,2),(2,3),(3,4)");
    auto r = conn->Query("select a , b  from tbl");
    ASSERT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select a + 1 as f from tbl");
    ASSERT_EQ(r->GetRetCode(), 0);
    const auto& schema = r->GetSchema();
    EXPECT_EQ(schema.GetColumnInfos()[0].GetColNameWithoutTableName(), "f");

    r = conn->Query("select a + 1 as f from tbl order by f desc");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 2);

    r = conn->Query("select a + 1 as f from tbl order by a+ 1 desc");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 2);

    r = conn->Query("select a from tbl order by a + 1 desc");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 1);

    r = conn->Query("select a + 1 as f from tbl order by f + 1 desc");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 2);
}

// 时间相关的转换
TEST_F(ConnectionForTest, SelectWithTimeCast) {
    // string to date
    auto r = conn->Query("select cast('2020-11-20 13:20:00' as date)");  // string -> date
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "2020-11-20");

    r = conn->Query("select '2000-01-01'::date");  // string -> date
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "2000-01-01");

    r = conn->Query("select '2000-01-01'::date::timestamp");  // string -> date -> timestamp
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "2000-01-01 00:00:00.000000");

    r = conn->Query("select unix_timestamp('2000-01-01'::date::timestamp)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int64_t>(), 946656000000000);

    r = conn->Query("select 1701938827000000::date::timestamp");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "2023-12-07 00:00:00.000000");

    // string -> timestamp
    r = conn->Query("select cast('2020-11-20 13:20:00' as timestamp)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "2020-11-20 13:20:00.000000");

    // string -> timestamp -> date
    r = conn->Query("select cast(cast('2020-11-20 13:20:00' as timestamp) as date)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "2020-11-20");

    // string -> timestamp -> unix timestamp
    r = conn->Query("select unix_timestamp(cast('2020-11-20 13:20:00' as timestamp))");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int64_t>(), 1605849600000000);

    // unix timestamp -> timestamp
    r = conn->Query("select cast(1605849600000000 as timestamp)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "2020-11-20 13:20:00.000000");

    // unix timestamp -> date , not allow
    r = conn->Query("select 1605849600000000::date");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "2020-11-20");

    // make_date -> unix timestamp
    r = conn->Query("select unix_timestamp(make_date(2020,11,20))");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int64_t>(), 1605801600000000);

    // make_date -> timestamp
    r = conn->Query("select cast(make_date(2020,11,20) as timestamp)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "2020-11-20 00:00:00.000000");

    r = conn->Query("select current_date()::timestamp");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>().substr(11), "00:00:00.000000");

    // timestamp compare with unix timestamp in where clause
    conn->Query("drop table if exists select_test_t3");
    conn->Query("create table select_test_t3 (id int , name varchar(10) , ts timestamp)");
    conn->Query("create index select_test_t3_idx on select_test_t3(ts)");  // 创建索引，测试存储引擎的索引检索
    conn->Query("insert into select_test_t3 values (1 , 'A' , '2020-11-20 13:20:00')");
    conn->Query("insert into select_test_t3 values (2 , 'B' , '2019-11-20 13:20:01')");
    r = conn->Query("select * from select_test_t3 where ts >= 1605849600000000");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "2020-11-20 13:20:00.000000");

    r = conn->Query("select * from select_test_t3 where ts < 1605809600000000");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "2019-11-20 13:20:01.000000");

    // compare with string
    r = conn->Query("select * from select_test_t3 where ts > '2020-11-20 10:20:00'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "2020-11-20 13:20:00.000000");
}

TEST_F(ConnectionForTest, SelectWithIsNull) {
    // 插入一行包含null值的数据
    conn->Query("insert into select_test_t1 values (6 , 'Eason' , null , 3.5)");
    // 测试 is null
    auto r = conn->Query("select * from select_test_t1 where age is null");
    EXPECT_EQ(r->RowCount(), 1);

    // 测试 is not null
    r = conn->Query("select * from select_test_t1 where age is not null");
    EXPECT_EQ(r->RowCount(), 5);
}

TEST_F(ConnectionForTest, SelectWithoutFrom) {
    auto r = conn->Query("select 1");
    EXPECT_EQ(r->RowCount(), 1);
}

TEST_F(ConnectionForTest, SelectWithJoin) {
    // prepare data for join
    conn->Query("create table PetTypes (PetTypeId int , PetType varchar(20))");
    conn->Query("create table Pets (PetId int , PetTypeId int , OwnerId int , PetName varchar(10), DOB timestamp)");

    conn->Query("insert into PetTypes values (1,'Bird'),(2,'Cat'),(3,'Dog'),(4,'Rabbit')");
    conn->Query(
        "insert into Pets values "
        "(1,2,3,'Fluffy','2020-11-20'),(2,3,3,'Fetch','2019-08-16'),(3,2,2,'Scratch','2018-10-01'),(4,3,3,'Wag','"
        "2020-03-15'),(5,1,1,'Tweet','2020-11-28'),(6,3,4,'Fluffy','2020-11-28'),(7,3,2,'Bark',null),(8,2,4,'Meow',"
        "null)");

    auto r = conn->Query(
        "SELECT Pets.PetName,PetTypes.PetType FROM Pets INNER JOIN PetTypes ON Pets.PetTypeId = PetTypes.PetTypeId");
    EXPECT_EQ(r->RowCount(), 8);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "Fluffy");
    EXPECT_STREQ(r->Row(0).Field(1).GetCastAs<std::string>().c_str(), "Cat");
    EXPECT_STREQ(r->Row(1).Field(0).GetCastAs<std::string>().c_str(), "Fetch");
    EXPECT_STREQ(r->Row(1).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(2).Field(0).GetCastAs<std::string>().c_str(), "Scratch");
    EXPECT_STREQ(r->Row(2).Field(1).GetCastAs<std::string>().c_str(), "Cat");
    EXPECT_STREQ(r->Row(3).Field(0).GetCastAs<std::string>().c_str(), "Wag");
    EXPECT_STREQ(r->Row(3).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(4).Field(0).GetCastAs<std::string>().c_str(), "Tweet");
    EXPECT_STREQ(r->Row(4).Field(1).GetCastAs<std::string>().c_str(), "Bird");
    EXPECT_STREQ(r->Row(5).Field(0).GetCastAs<std::string>().c_str(), "Fluffy");
    EXPECT_STREQ(r->Row(5).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(6).Field(0).GetCastAs<std::string>().c_str(), "Bark");
    EXPECT_STREQ(r->Row(6).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(7).Field(0).GetCastAs<std::string>().c_str(), "Meow");
    EXPECT_STREQ(r->Row(7).Field(1).GetCastAs<std::string>().c_str(), "Cat");

    // alias
    r = conn->Query("SELECT p.PetName,pt.PetType FROM Pets p INNER JOIN PetTypes pt ON p.PetTypeId = pt.PetTypeId");
    EXPECT_EQ(r->RowCount(), 8);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "Fluffy");
    EXPECT_STREQ(r->Row(0).Field(1).GetCastAs<std::string>().c_str(), "Cat");
    EXPECT_STREQ(r->Row(1).Field(0).GetCastAs<std::string>().c_str(), "Fetch");
    EXPECT_STREQ(r->Row(1).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(2).Field(0).GetCastAs<std::string>().c_str(), "Scratch");
    EXPECT_STREQ(r->Row(2).Field(1).GetCastAs<std::string>().c_str(), "Cat");
    EXPECT_STREQ(r->Row(3).Field(0).GetCastAs<std::string>().c_str(), "Wag");
    EXPECT_STREQ(r->Row(3).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(4).Field(0).GetCastAs<std::string>().c_str(), "Tweet");
    EXPECT_STREQ(r->Row(4).Field(1).GetCastAs<std::string>().c_str(), "Bird");
    EXPECT_STREQ(r->Row(5).Field(0).GetCastAs<std::string>().c_str(), "Fluffy");
    EXPECT_STREQ(r->Row(5).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(6).Field(0).GetCastAs<std::string>().c_str(), "Bark");
    EXPECT_STREQ(r->Row(6).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(7).Field(0).GetCastAs<std::string>().c_str(), "Meow");
    EXPECT_STREQ(r->Row(7).Field(1).GetCastAs<std::string>().c_str(), "Cat");

    // equal join
    r = conn->Query("SELECT p.PetName,pt.PetType FROM Pets p, PetTypes pt WHERE p.PetTypeId = pt.PetTypeId");
    EXPECT_EQ(r->RowCount(), 8);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "Fluffy");
    EXPECT_STREQ(r->Row(0).Field(1).GetCastAs<std::string>().c_str(), "Cat");
    EXPECT_STREQ(r->Row(1).Field(0).GetCastAs<std::string>().c_str(), "Fetch");
    EXPECT_STREQ(r->Row(1).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(2).Field(0).GetCastAs<std::string>().c_str(), "Scratch");
    EXPECT_STREQ(r->Row(2).Field(1).GetCastAs<std::string>().c_str(), "Cat");
    EXPECT_STREQ(r->Row(3).Field(0).GetCastAs<std::string>().c_str(), "Wag");
    EXPECT_STREQ(r->Row(3).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(4).Field(0).GetCastAs<std::string>().c_str(), "Tweet");
    EXPECT_STREQ(r->Row(4).Field(1).GetCastAs<std::string>().c_str(), "Bird");
    EXPECT_STREQ(r->Row(5).Field(0).GetCastAs<std::string>().c_str(), "Fluffy");
    EXPECT_STREQ(r->Row(5).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(6).Field(0).GetCastAs<std::string>().c_str(), "Bark");
    EXPECT_STREQ(r->Row(6).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(7).Field(0).GetCastAs<std::string>().c_str(), "Meow");
    EXPECT_STREQ(r->Row(7).Field(1).GetCastAs<std::string>().c_str(), "Cat");

    // left join
    r = conn->Query("SELECT p.PetName, pt.PetType FROM PetTypes pt LEFT JOIN Pets p ON p.PetTypeId = pt.PetTypeId");
    EXPECT_EQ(r->RowCount(), 9);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "Tweet");
    EXPECT_STREQ(r->Row(0).Field(1).GetCastAs<std::string>().c_str(), "Bird");
    EXPECT_STREQ(r->Row(1).Field(0).GetCastAs<std::string>().c_str(), "Fluffy");
    EXPECT_STREQ(r->Row(1).Field(1).GetCastAs<std::string>().c_str(), "Cat");
    EXPECT_STREQ(r->Row(2).Field(0).GetCastAs<std::string>().c_str(), "Scratch");
    EXPECT_STREQ(r->Row(2).Field(1).GetCastAs<std::string>().c_str(), "Cat");
    EXPECT_STREQ(r->Row(3).Field(0).GetCastAs<std::string>().c_str(), "Meow");
    EXPECT_STREQ(r->Row(3).Field(1).GetCastAs<std::string>().c_str(), "Cat");
    EXPECT_STREQ(r->Row(4).Field(0).GetCastAs<std::string>().c_str(), "Fetch");
    EXPECT_STREQ(r->Row(4).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(5).Field(0).GetCastAs<std::string>().c_str(), "Wag");
    EXPECT_STREQ(r->Row(5).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(6).Field(0).GetCastAs<std::string>().c_str(), "Fluffy");
    EXPECT_STREQ(r->Row(6).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_STREQ(r->Row(7).Field(0).GetCastAs<std::string>().c_str(), "Bark");
    EXPECT_STREQ(r->Row(7).Field(1).GetCastAs<std::string>().c_str(), "Dog");
    EXPECT_EQ(r->Row(8).Field(0).IsNull(), true);
    EXPECT_STREQ(r->Row(8).Field(1).GetCastAs<std::string>().c_str(), "Rabbit");
    // test statement property
    ASSERT_EQ(r->stmt_props.read_tables.size(), 2);
    ASSERT_EQ(r->stmt_props.modify_tables.size(), 0);
    auto iter = r->stmt_props.read_tables.begin();
    EXPECT_EQ(iter->second.table_type, StatementProps::TableType::TABLE_TYPE_NORMAL);
    iter++;
    EXPECT_EQ(iter->second.table_type, StatementProps::TableType::TABLE_TYPE_NORMAL);

    // cross product
    r = conn->Query("SELECT * FROM PetTypes , Pets");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 32);

    r = conn->Query("SELECT * FROM PetTypes CROSS JOIN Pets");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 32);

    // not support natural join
    r = conn->Query("SELECT * FROM PetTypes NATURAL JOIN Pets");
    ASSERT_NE(r->GetRetCode(), 0);

    // support join with using
    r = conn->Query("SELECT * FROM PetTypes JOIN Pets USING (PetTypeId)");
    // ASSERT_NE(r->GetRetCode(), 0);
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 8);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<std::string>(), "Bird");
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(0).Field(3).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<std::string>(), "Cat");
    EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(1).Field(3).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(2).Field(1).GetCastAs<std::string>(), "Cat");
    EXPECT_EQ(r->RowRef(2).Field(2).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(2).Field(3).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(3).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(3).Field(1).GetCastAs<std::string>(), "Cat");
    EXPECT_EQ(r->RowRef(3).Field(2).GetCastAs<int32_t>(), 8);
    EXPECT_EQ(r->RowRef(3).Field(3).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(4).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(4).Field(1).GetCastAs<std::string>(), "Dog");
    EXPECT_EQ(r->RowRef(4).Field(2).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(4).Field(3).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(5).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(5).Field(1).GetCastAs<std::string>(), "Dog");
    EXPECT_EQ(r->RowRef(5).Field(2).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(5).Field(3).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(6).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(6).Field(1).GetCastAs<std::string>(), "Dog");
    EXPECT_EQ(r->RowRef(6).Field(2).GetCastAs<int32_t>(), 6);
    EXPECT_EQ(r->RowRef(6).Field(3).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(7).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(7).Field(1).GetCastAs<std::string>(), "Dog");
    EXPECT_EQ(r->RowRef(7).Field(2).GetCastAs<int32_t>(), 7);
    EXPECT_EQ(r->RowRef(7).Field(3).GetCastAs<int32_t>(), 2);

    // self JOIN test
    r = conn->Query("select * from PetTypes , PetTypes");
    ASSERT_NE(r->GetRetCode(), 0);  // 表重名冲突

    r = conn->Query("select * from PetTypes a , PetTypes ");
    ASSERT_EQ(r->GetRetCode(), 0);  // 通过别名解决表重名冲突

    r = conn->Query("select PetTypes.* from PetTypes a , PetTypes");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->ColumnCount(), 2);  // * 添加前缀获取指定表的所有列

    r = conn->Query("select a.* from PetTypes a , PetTypes");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->ColumnCount(), 2);  // 别名也可以正确获取指定表的所有列

    r = conn->Query("select * from PetTypes cross join PetTypes");
    ASSERT_NE(r->GetRetCode(), 0);  // join 也需要正确检测到表重名冲突

    conn->Query("DROP TABLE IF EXISTS employees");
    conn->Query("CREATE TABLE employees ( emp_id BIGINT PRIMARY KEY, emp_name VARCHAR(100))");
    conn->Query("INSERT INTO employees (emp_id, emp_name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');");
    conn->Query("DROP TABLE IF EXISTS departments");
    conn->Query("CREATE TABLE departments ( dep_id INT PRIMARY KEY, dep_name VARCHAR(100))");  // test join on condition
                                                                                               // type not equal
    conn->Query("INSERT INTO departments (dep_id, dep_name) VALUES (1, 'HR'), (2, 'IT')");

    // test left join
    r = conn->Query(
        "SELECT employees.emp_id, employees.emp_name, departments.dep_name FROM employees LEFT JOIN departments ON "
        "employees.emp_id = departments.dep_id");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int64_t>(), 1);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<std::string>(), "Alice");
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<std::string>(), "HR");
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int64_t>(), 2);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<std::string>(), "Bob");
    EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<std::string>(), "IT");
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int64_t>(), 3);
    EXPECT_EQ(r->RowRef(2).Field(1).GetCastAs<std::string>(), "Charlie");
    EXPECT_EQ(r->RowRef(2).Field(2).IsNull(), true);

    r = conn->Query(
        "SELECT employees.emp_id, employees.emp_name, departments.dep_name FROM employees LEFT JOIN departments ON "
        "employees.emp_id > departments.dep_id");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int64_t>(), 1);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<std::string>(), "Alice");
    EXPECT_EQ(r->RowRef(0).Field(2).IsNull(), true);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int64_t>(), 2);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<std::string>(), "Bob");
    EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<std::string>(), "HR");
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int64_t>(), 3);
    EXPECT_EQ(r->RowRef(2).Field(1).GetCastAs<std::string>(), "Charlie");
    EXPECT_EQ(r->RowRef(2).Field(2).GetCastAs<std::string>(), "HR");
    EXPECT_EQ(r->RowRef(3).Field(0).GetCastAs<int64_t>(), 3);
    EXPECT_EQ(r->RowRef(3).Field(1).GetCastAs<std::string>(), "Charlie");
    EXPECT_EQ(r->RowRef(3).Field(2).GetCastAs<std::string>(), "IT");

    r = conn->Query(
        "SELECT employees.emp_id, employees.emp_name, departments.dep_name FROM employees LEFT JOIN "
        "departments ON employees.emp_id = departments.dep_id AND FALSE");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int64_t>(), 1);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<std::string>(), "Alice");
    EXPECT_EQ(r->RowRef(0).Field(2).IsNull(), true);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int64_t>(), 2);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<std::string>(), "Bob");
    EXPECT_EQ(r->RowRef(1).Field(2).IsNull(), true);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int64_t>(), 3);
    EXPECT_EQ(r->RowRef(2).Field(1).GetCastAs<std::string>(), "Charlie");
    EXPECT_EQ(r->RowRef(2).Field(2).IsNull(), true);

    r = conn->Query(
        "SELECT employees.emp_id, employees.emp_name, departments.dep_name FROM departments RIGHT JOIN employees ON "
        "employees.emp_id = departments.dep_id");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int64_t>(), 1);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<std::string>(), "Alice");
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<std::string>(), "HR");
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int64_t>(), 2);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<std::string>(), "Bob");
    EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<std::string>(), "IT");
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int64_t>(), 3);
    EXPECT_EQ(r->RowRef(2).Field(1).GetCastAs<std::string>(), "Charlie");
    EXPECT_EQ(r->RowRef(2).Field(2).IsNull(), true);

    r = conn->Query(
        "SELECT employees.emp_id, employees.emp_name, departments.dep_name FROM departments RIGHT JOIN employees ON "
        "employees.emp_id > departments.dep_id");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int64_t>(), 1);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<std::string>(), "Alice");
    EXPECT_EQ(r->RowRef(0).Field(2).IsNull(), true);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int64_t>(), 2);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<std::string>(), "Bob");
    EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<std::string>(), "HR");
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int64_t>(), 3);
    EXPECT_EQ(r->RowRef(2).Field(1).GetCastAs<std::string>(), "Charlie");
    EXPECT_EQ(r->RowRef(2).Field(2).GetCastAs<std::string>(), "HR");
    EXPECT_EQ(r->RowRef(3).Field(0).GetCastAs<int64_t>(), 3);
    EXPECT_EQ(r->RowRef(3).Field(1).GetCastAs<std::string>(), "Charlie");
    EXPECT_EQ(r->RowRef(3).Field(2).GetCastAs<std::string>(), "IT");

    r = conn->Query(
        "SELECT employees.emp_id, employees.emp_name, departments.dep_name FROM departments RIGHT JOIN "
        "employees ON employees.emp_id = departments.dep_id AND FALSE");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int64_t>(), 1);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<std::string>(), "Alice");
    EXPECT_EQ(r->RowRef(0).Field(2).IsNull(), true);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int64_t>(), 2);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<std::string>(), "Bob");
    EXPECT_EQ(r->RowRef(1).Field(2).IsNull(), true);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int64_t>(), 3);
    EXPECT_EQ(r->RowRef(2).Field(1).GetCastAs<std::string>(), "Charlie");
    EXPECT_EQ(r->RowRef(2).Field(2).IsNull(), true);

    // join with using clause 
    conn->Query("drop table if exists t1");
    conn->Query("create table t1 (c0 int)");
    r = conn->Query("select * from t1 join t1 t2 using (c0,c0)");
    ASSERT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select * from (t1 join t1 t2 using (c0)) , (select 42)");
    ASSERT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select * from (t1 join t1 t2 using (c0,c0)) , (select 42)");
    ASSERT_EQ(r->GetRetCode(), 0);

    conn->Query("drop table if exists tbl");
    conn->Query("create table tbl as select 42 as i");

    r = conn->Query("select * from tbl t1 join tbl t2 using (i) join tbl t3 using (i)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);

    r = conn->Query("select * from tbl t1 join tbl t2 using (i,i) join tbl t3 using (i,i,i)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);

    conn->Query("drop table tbl");
    conn->Query("create table tbl as select 42 as i, 84 as j");
    r = conn->Query("select * from tbl t1 join tbl t2 using (i,j) join tbl t3 using (i,j)");
    ASSERT_EQ(r->GetRetCode(),0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 84);

    r = conn->Query("select * from tbl t1 join tbl t2 using (i, j, i) join tbl t3 using (i, i, i, j, i)");
    ASSERT_EQ(r->GetRetCode(),0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 84);

    conn->Query("drop table if exists a");
    conn->Query("create table a as select 42 as i , 80 as j");
    conn->Query("drop table if exists b");
    conn->Query("create table b as select 43 as i , 84 as k");
    conn->Query("drop table if exists c");
    conn->Query("create table c as select 44 as i , 84 as l");

    r = conn->Query("select i , a.i , b.i from a inner join b using(i) ");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 0);

    r = conn->Query("select i , a.i , b.i from a left outer join b using(i) ");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 42);
    EXPECT_EQ(r->RowRef(0).Field(2).IsNull(),true);

    r = conn->Query("select i , a.i , b.i from a right outer join b using(i) ");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 43);
    EXPECT_EQ(r->RowRef(0).Field(1).IsNull(),true);
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<int32_t>(), 43);

    r = conn->Query("select * from a right join b using(i) ");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 43);
    EXPECT_EQ(r->RowRef(0).Field(1).IsNull(),true);
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<int32_t>(), 84);

    r = conn->Query("select * from a left join b using(i) right join c using(i)");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(),1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 44);
    EXPECT_EQ(r->RowRef(0).Field(1).IsNull(),true);
    EXPECT_EQ(r->RowRef(0).Field(2).IsNull(),true);
    EXPECT_EQ(r->RowRef(0).Field(3).GetCastAs<int32_t>(), 84);

    r = conn->Query("select i , a.i , b.i from a left join b using(i) right join c using(i)");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 44);
    EXPECT_EQ(r->RowRef(0).Field(1).IsNull(),true);
    EXPECT_EQ(r->RowRef(0).Field(2).IsNull(),true);

    // r = conn->Query("select i, a.i, b.i from a full outer join b using (i) order by 1");
    // ASSERT_EQ(r->GetRetCode(), 0);
    // ASSERT_EQ(r->RowCount(), 2);
    // EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
    // EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 42);
    // EXPECT_EQ(r->RowRef(0).Field(2).IsNull(), true);
    // EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 43);
    // EXPECT_EQ(r->RowRef(1).Field(1).IsNull(), true);
    // EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<int32_t>(), 43);

    // r = conn->Query("select * from a full outer join b using (i) order by 1");
    // ASSERT_EQ(r->GetRetCode(), 0);
    // ASSERT_EQ(r->RowCount(), 2);
    // EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
    // EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 80);
    // EXPECT_EQ(r->RowRef(0).Field(2).IsNull(), true);
    // EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 43);
    // EXPECT_EQ(r->RowRef(1).Field(1).IsNull(), true);
    // EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<int32_t>(), 84);

    // r = conn->Query("select i, a.i, b.i, c.i from a full outer join b using (i) full outer join c using (i) order by 1");
    // ASSERT_EQ(r->GetRetCode(), 0);
    // ASSERT_EQ(r->RowCount(), 3);
    // EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
    // EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 42);
    // EXPECT_EQ(r->RowRef(0).Field(2).IsNull(), true);
    // EXPECT_EQ(r->RowRef(0).Field(3).IsNull(), true);
    // EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 43);
    // EXPECT_EQ(r->RowRef(1).Field(1).IsNull(), true);
    // EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<int32_t>(), 43);
    // EXPECT_EQ(r->RowRef(1).Field(3).IsNull(), true);
    // EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 44);
    // EXPECT_EQ(r->RowRef(2).Field(1).IsNull(), true);
    // EXPECT_EQ(r->RowRef(2).Field(2).IsNull(), true);
    // EXPECT_EQ(r->RowRef(2).Field(3).GetCastAs<int32_t>(), 44);

    // r = conn->Query("select * from a full outer join b using (i) full outer join c using (i) order by 1");
    // ASSERT_EQ(r->GetRetCode(), 0);
    // ASSERT_EQ(r->RowCount(), 3);
    // EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
    // EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 80);
    // EXPECT_EQ(r->RowRef(0).Field(2).IsNull(), true);
    // EXPECT_EQ(r->RowRef(0).Field(3).IsNull(), true);
    // EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 43);
    // EXPECT_EQ(r->RowRef(1).Field(1).IsNull(), true);
    // EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<int32_t>(), 84);
    // EXPECT_EQ(r->RowRef(1).Field(3).IsNull(), true);
    // EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 44);
    // EXPECT_EQ(r->RowRef(2).Field(1).IsNull(), true);
    // EXPECT_EQ(r->RowRef(2).Field(2).IsNull(), true);
    // EXPECT_EQ(r->RowRef(2).Field(3).GetCastAs<int32_t>(), 84);
}

TEST_F(ConnectionForTest, SelectWithAgg) {
    conn->Query("CREATE TABLE score (id INT,name VARCHAR(50),subject VARCHAR(50),score INT)");
    conn->Query("INSERT INTO score VALUES (1, '张三', '数学', 81)");
    conn->Query("INSERT INTO score VALUES (2, '李四', '数学', 75)");
    conn->Query("INSERT INTO score VALUES (3, '王五', '语文', 90)");
    conn->Query("INSERT INTO score VALUES (4, '赵六', '语文', 65)");
    conn->Query("INSERT INTO score VALUES (5, '小明', '数学', 90)");
    conn->Query("INSERT INTO score VALUES (6, '小红', '物理', 85)");
    conn->Query("INSERT INTO score VALUES (7, '小刚', '物理', 78)");

    auto r = conn->Query("SELECT subject, MAX(score), MIN(score) FROM score GROUP BY subject");
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 90);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 75);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<int32_t>(), 85);
    EXPECT_EQ(r->Row(1).Field(2).GetCastAs<int32_t>(), 78);
    EXPECT_EQ(r->Row(2).Field(1).GetCastAs<int32_t>(), 90);
    EXPECT_EQ(r->Row(2).Field(2).GetCastAs<int32_t>(), 65);

    r = conn->Query("SELECT score * 2 FROM score GROUP BY score * 2");  // 投影不需要进行计算
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("SELECT score * 2 FROM score GROUP BY score");  // 投影需要进行计算
    EXPECT_EQ(r->GetRetCode(), 0);

    // test having
    r = conn->Query(
        "SELECT subject, COUNT(name) AS student_count FROM score GROUP BY subject HAVING student_count >= 3");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 3);

    // test count with null
    conn->Query("INSERT INTO score VALUES (8, '小坤', '语文', null)");
    r = conn->Query("SELECT COUNT(score) FROM score");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 7);

    // test invalid case 
    r = conn->Query("select min() , max() , sum() , avg() , mode() from score");
    EXPECT_NE(r->GetRetCode(), 0);

    r = conn->Query("select max(name) , min(name) , mode(name) from score");
    EXPECT_EQ(r->GetRetCode(), 0);
    

    // count(*) 不受null影响
    r = conn->Query("select count(*) from score");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 8);

    // count() 与 count(*)等价
    r = conn->Query("select count() from score");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 8);
    EXPECT_EQ(r->RowRef(0).Field(0).GetLogicalType().TypeId(), GS_TYPE_BIGINT);  // count 返回类型的是bigint

    r = conn->Query("select count(*) from score limit 1");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 8);
    EXPECT_EQ(r->RowRef(0).Field(0).GetLogicalType().TypeId(), GS_TYPE_BIGINT);  // count_start 返回类型的是bigint

    r = conn->Query("select count(*) from score where id > 3");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 5);

    r = conn->Query("select count(*) from score group by subject");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 3);

    r = conn->Query("select count(*) + 1 from score");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 9);

    // test fast scan
    r = conn->Query("select count(*) from (select * from score)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 8);

    r = conn->Query("select count(*) from (select count(*) from score)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);

    r = conn->Query("select count(*) + 1 from (select count(*) from score)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);

    // SUM AVG TEST
    r = conn->Query(
        "select subject,AVG(score),SUM(score),MIN(score),MAX(score) from score GROUP BY subject ORDER BY AVG(score) "
        "desc");
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_DOUBLE_EQ(r->Row(0).Field(1).GetCastAs<double>(), 82);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 246);
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<int32_t>(), 75);
    EXPECT_EQ(r->Row(0).Field(4).GetCastAs<int32_t>(), 90);

    EXPECT_DOUBLE_EQ(r->Row(1).Field(1).GetCastAs<double>(), 81.5);
    EXPECT_EQ(r->Row(1).Field(2).GetCastAs<int32_t>(), 163);
    EXPECT_EQ(r->Row(1).Field(3).GetCastAs<int32_t>(), 78);
    EXPECT_EQ(r->Row(1).Field(4).GetCastAs<int32_t>(), 85);

    EXPECT_DOUBLE_EQ(r->Row(2).Field(1).GetCastAs<double>(), 77.5);
    EXPECT_EQ(r->Row(2).Field(2).GetCastAs<int32_t>(), 155);
    EXPECT_EQ(r->Row(2).Field(3).GetCastAs<int32_t>(), 65);
    EXPECT_EQ(r->Row(2).Field(4).GetCastAs<int32_t>(), 90);

    // 测试 聚合函数中的distinct
    r = conn->Query("select count(distinct subject)  from score");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 3);

    // 测试 sum 和 AVG 的 distinct
    conn->Query("create table distinct_agg_table (a int, b int)");
    conn->Query(
        "insert into distinct_agg_table values (1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (3, "
        "1),(null,null),(null,1),(1,null)");  // 测试对null的支持
    r = conn->Query("select sum(distinct a), avg(distinct a) from distinct_agg_table");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 6);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 2);

    // 测试 groupby 字段问题
    r = conn->Query("select a,b from distinct_agg_table group by a , b");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 9);

    r = conn->Query("select distinct a , b from distinct_agg_table");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 9);

    r = conn->Query("select a * 2 from distinct_agg_table group by a * 2");  // select item 出现在group by中
    EXPECT_EQ(r->GetRetCode(), 0);

    // 需要支持
    // r = conn->Query("select a from distinct_agg_table group by a * 2");
    // EXPECT_EQ(r->GetRetCode(), 0);
    //
    r = conn->Query("select a * 2 + 1 from distinct_agg_table group by a * 2");  // select item 是 group by 的部分
    EXPECT_EQ(r->GetRetCode(), 0);

    // order having 等都有类似的限制
    r = conn->Query("select a * 2 from distinct_agg_table group by a * 2 order by a");  // no pass
    EXPECT_NE(r->GetRetCode(), 0);                                                      // 不等于0

    r = conn->Query("select a * 2 from distinct_agg_table group by a order by a * 2");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 4);

    // BUG : #709579644
    r = conn->Query("SELECT MAX(NULL::DECIMAL), MAX('0.1'::DECIMAL(4,1))::VARCHAR");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "0.1");
}

TEST_F(ConnectionForTest, SelectStringFunc) {
    // substr 测试用例
    auto r = conn->Query("select substr('Hello, World!', 1, 5)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "Hello");

    r = conn->Query("select substr('Quadratically',5)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "ratically");
    r = conn->Query("select substr('Sakila',1)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "Sakila");
    r = conn->Query("select substr('Sakila',2)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "akila");
    r = conn->Query("select substr('Sakila',-3)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "ila");
    r = conn->Query("select substr('Sakila',0)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "Sakila");
    r = conn->Query("select substr('Sakila',100)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    r = conn->Query("select substr('Sakila',-100)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "Sakila");
    r = conn->Query("select substr('Quadratically',5,6)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "ratica");
    r = conn->Query("select substr('Sakila',-5,3)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "aki");
    r = conn->Query("select substr('Sakila',2,0)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    r = conn->Query("select substr('Sakila',2,-1)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "S");
    r = conn->Query("select substr('Sakila',2,100)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "akila");
    r = conn->Query("select substr(null,2,3)");
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    r = conn->Query("select substr('Sakila',2,-100)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "S");
    r = conn->Query("select substr('Sakila',-100,100)");  // 起始位置超过字符串长度
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "Sakila");
    r = conn->Query("select substr('Sakila',-100,10)");  // 起始位置超过字符串长度
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    r = conn->Query("select substr('b',-2,2)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "b");

    r = conn->Query("select substr('Sakila',-3,-3)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "Sak");
    r = conn->Query("select substr('Sakila',-3,100)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "ila");
    r = conn->Query("select substr('Sakila',-3,-100)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "Sak");

    r = conn->Query("select substr('Sakila',null,3)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("select substr('Sakila',2,null)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    // test utf-8
    conn->Query("drop table if exists strings ");
    conn->Query("CREATE TABLE strings(s VARCHAR)");
    conn->Query("INSERT INTO strings VALUES ('twoñthree₡four🦆end')");
    r = conn->Query("SELECT substr(s,1,7) FROM strings");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "twoñthr");
    r = conn->Query("SELECT substr(s,10,7) FROM strings");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "₡four🦆e");
    r = conn->Query("SELECT substr(s,15,7) FROM strings");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "🦆end");
    r = conn->Query("SELECT substr(s, -4, 4) FROM strings");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "🦆end");
    r = conn->Query("SELECT substr(s, -1, -4) FROM strings");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "r🦆en");
    r = conn->Query("SELECT substr(s, 0, -4) FROM strings");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    r = conn->Query("SELECT substr(s, 0, 5) FROM strings");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "twoñ");
    r = conn->Query("SELECT substr(s, 5, -5) FROM strings");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "twoñ");
    r = conn->Query("SELECT substr(s, 5) FROM strings");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "three₡four🦆end");

    // test concat
    r = conn->Query("select concat('My', 'S', 'QL')");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "MySQL");
    r = conn->Query("select concat('My', NULL, 'QL')");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "MyQL");
    r = conn->Query("select concat(14.3)");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "14.3");
    r = conn->Query("select concat('你好',' ','世界')");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "你好 世界");

    // test concat_ws
    r = conn->Query("select concat_ws(',', 'First name', 'Second name', 'Last Name')");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "First name,Second name,Last Name");
    r = conn->Query("select concat_ws(',', 'First name', NULL, 'Last Name')");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "First name,Last Name");
    r = conn->Query("select concat_ws(',', NULL)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    r = conn->Query("select concat_ws(',', 'First name')");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "First name");
    r = conn->Query("select concat_ws()");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select concat_ws(',', NULL,2,3)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "2,3");
    r = conn->Query("select concat_ws(null, 1,2,3)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    r = conn->Query("select concat_ws('@', 1 , null)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "1");
    r = conn->Query("select concat_ws('@', null , 1)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "1");
    r = conn->Query("select concat_ws(',', '' , '','')");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), ",,");

    // test length
    r = conn->Query("select length('text')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 4);
    r = conn->Query("select length('text中文')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 6);
    r = conn->Query("select length('Hello🦆')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 6);
    r = conn->Query("select length(1,2)");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select length(null)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    r = conn->Query("select length()");
    ASSERT_NE(r->GetRetCode(), 0);

    // test repeat
    r = conn->Query("select repeat('text', 3)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "texttexttext");
    // repreat with 中文
    r = conn->Query("select repeat('text中文', 3)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "text中文text中文text中文");
    r = conn->Query("select repeat('text', 0)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    r = conn->Query("select repeat(1, 3)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "111");
    // test repeat with null
    r = conn->Query("select repeat(null, 3)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    r = conn->Query("select repeat('text', null)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    // test repeat with illegal params
    r = conn->Query("select repeat('text', -1)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    r = conn->Query("select repeat('text', 1.2)");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select repeat('text', '1')");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select repeat('text', 'a')");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select repeat('text', 1,2)");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select repeat()");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select repeat('abc',1,2)");
    ASSERT_NE(r->GetRetCode(), 0);

    // test trim
    r = conn->Query(
        "select LTRIM(''), LTRIM('Neither'), LTRIM(' Leading'), LTRIM('Trailing   '), LTRIM(' Both '), LTRIM(NULL), "
        "LTRIM('     '),LTRIM('mühleisen','mün'),LTRIM('W世界H','世W')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "Neither");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "Leading");
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<std::string>(), "Trailing   ");
    EXPECT_EQ(r->Row(0).Field(4).GetCastAs<std::string>(), "Both ");
    EXPECT_EQ(r->Row(0).Field(5).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(6).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(7).GetCastAs<std::string>(), "hleisen");
    EXPECT_EQ(r->Row(0).Field(8).GetCastAs<std::string>(), "界H");

    r = conn->Query(
        "select RTRIM(''), RTRIM('Neither'), RTRIM(' Leading'), RTRIM('Trailing   '), RTRIM(' Both '), RTRIM(NULL), "
        "RTRIM('    '),RTRIM('mühleisen','mün')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "Neither");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), " Leading");
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<std::string>(), "Trailing");
    EXPECT_EQ(r->Row(0).Field(4).GetCastAs<std::string>(), " Both");
    EXPECT_EQ(r->Row(0).Field(5).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(6).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(7).GetCastAs<std::string>(), "mühleise");

    r = conn->Query(
        "select TRIM(''), TRIM('Neither'), TRIM(' Leading'), TRIM('Trailing   '), TRIM(' Both '), TRIM(NULL), "
        "TRIM('    '),TRIM('mühleisen','mün')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "Neither");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "Leading");
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<std::string>(), "Trailing");
    EXPECT_EQ(r->Row(0).Field(4).GetCastAs<std::string>(), "Both");
    EXPECT_EQ(r->Row(0).Field(5).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(6).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(7).GetCastAs<std::string>(), "hleise");

    // test 自定义trim字符
    r = conn->Query(
        "select LTRIM('', 'ho'), LTRIM('hello', 'ho'), LTRIM('papapapa', 'pa'), LTRIM('blaHblabla', 'bla'), "
        "LTRIM('blabla', NULL), LTRIM(NULL, 'blabla'), LTRIM('blabla', '')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "ello");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<std::string>(), "Hblabla");
    EXPECT_EQ(r->Row(0).Field(5).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(5).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(6).GetCastAs<std::string>(), "blabla");

    r = conn->Query(
        "select RTRIM('', 'ho'), RTRIM('hello', 'ho'), RTRIM('papapapa', 'pa'), RTRIM('blaHblabla', 'bla'), "
        "RTRIM('blabla', NULL), RTRIM(NULL, 'blabla'), RTRIM('blabla', '')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "hell");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<std::string>(), "blaH");
    EXPECT_EQ(r->Row(0).Field(5).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(5).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(6).GetCastAs<std::string>(), "blabla");

    r = conn->Query(
        "select TRIM('', 'ho'), TRIM('hello', 'ho'), TRIM('papapapa', 'pa'), TRIM('blaHblabla', 'bla'), "
        "TRIM('blabla', NULL), TRIM(NULL, 'blabla'), TRIM('blabla', '')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "ell");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<std::string>(), "H");
    EXPECT_EQ(r->Row(0).Field(5).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(5).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(6).GetCastAs<std::string>(), "blabla");

    r = conn->Query("select trim()");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select trim('hello','World','aaa')");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select ltrim()");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select ltrim('hello','World','aaa')");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select rtrim()");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select rtrim('hello','World','aaa')");
    ASSERT_NE(r->GetRetCode(), 0);

    // test contains
    r = conn->Query(
        "SELECT CONTAINS('hello world', 'h'), \
       CONTAINS('hello world', 'he'), \
       CONTAINS('hello world', 'hel'), \
       CONTAINS('hello world', 'hell'), \
       CONTAINS('hello world', 'hello'), \
       CONTAINS('hello world', 'hello '), \
       CONTAINS('hello world', 'hello w'),  \
       CONTAINS('hello world', 'hello wo'), \
       CONTAINS('hello world', 'hello wor'),  \
       CONTAINS('hello world', 'hello worl')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(4).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(5).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(6).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(7).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(8).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(9).GetCastAs<int32_t>(), 1);

    r = conn->Query("select contains('hello', ''), contains('', ''), contains(NULL, '')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(2).IsNull(), true);

    r = conn->Query("select contains('abc',''),contains('abc',null),contains(null,'abc')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(2).IsNull(), true);

    r = conn->Query("select contains('abc','a','b')");
    ASSERT_NE(r->GetRetCode(), 0);

    // contains with utf-8
    conn->Query("drop table if exists strings ");
    conn->Query("CREATE TABLE strings(s VARCHAR)");
    conn->Query("insert into strings values ('átomo'),('olá mundo'),('你好世界'),('two ñ three ₡ four 🦆 end')");
    r = conn->Query("select contains(s, 'á') from strings");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 0);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 0);

    // test starts_with
    r = conn->Query(
        "SELECT STARTS_WITH('hello world', 'h'), \
       STARTS_WITH('hello world', 'he'), \
       STARTS_WITH('hello world', 'hel'), \
       STARTS_WITH('hello world', 'hell'), \
       STARTS_WITH('hello world', 'hello'), \
       STARTS_WITH('hello world', 'hello '), \
       STARTS_WITH('hello world', 'hello w'),  \
       STARTS_WITH('hello world', 'hello wo'), \
       STARTS_WITH('hello world', 'hello wor'),  \
       STARTS_WITH('hello world', 'hello worl')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(4).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(5).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(6).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(7).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(8).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(9).GetCastAs<int32_t>(), 1);

    r = conn->Query("select starts_with('hello', ''), starts_with('', ''), starts_with(NULL, '')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(2).IsNull(), true);

    r = conn->Query("select starts_with(s, 'á') from strings");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 0);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 0);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 0);

    // test repeat
    r = conn->Query("select REPEAT(NULL, NULL), REPEAT(NULL, 3), REPEAT('MySQL', NULL)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(1).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(2).IsNull(), true);

    r = conn->Query("select REPEAT('', 3), REPEAT('MySQL', 3), REPEAT('MotörHead', 2), REPEAT('Hello', -1)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "MySQLMySQLMySQL");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "MotörHeadMotörHead");
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<std::string>(), "");

    r = conn->Query("select repeat()");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select repeat('abc',1,2)");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select repeat(1)");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select repeat('abc',1.2)");
    ASSERT_NE(r->GetRetCode(), 0);

    // test replace
    r = conn->Query("select REPLACE('This is the main test string', NULL, 'ALT')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("select REPLACE(NULL, 'main', 'ALT')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("select REPLACE('This is the main test string', 'main', 'larger-main')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "This is the larger-main test string");

    r = conn->Query("select REPLACE('aaaaaaa', 'a', '0123456789')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(),
              "0123456789012345678901234567890123456789012345678901234567890123456789");

    r = conn->Query("select REPLACE(1, 2, 3, 4)");
    ASSERT_NE(r->GetRetCode(), 0);

    // test reverse
    r = conn->Query("select REVERSE(''), REVERSE('Hello'), REVERSE('MotörHead'), REVERSE(NULL)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "olleH");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "daeHrötoM");
    EXPECT_EQ(r->Row(0).Field(3).IsNull(), true);

    r = conn->Query("select REVERSE('hello', 'world')");
    ASSERT_NE(r->GetRetCode(), 0);

    // test tolower
    r = conn->Query(
        "select tolower(''), tolower('Hello'), tolower('HÉLLÖ'),tolower('ÉÈÊË'),tolower('HÉLLÖ WÔRLD'),toLOWER(NULL)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "hello");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "héllö");
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<std::string>(), "éèêë");
    EXPECT_EQ(r->Row(0).Field(4).GetCastAs<std::string>(), "héllö wôrld");
    EXPECT_EQ(r->Row(0).Field(5).IsNull(), true);

    // test toupper
    r = conn->Query(
        "select toupper(''), toupper('Hello'), toupper('Héllö'), toupper('éèêë'), toupper('Héllö Wôrld'), "
        "toUPPER(NULL)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "HELLO");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "HÉLLÖ");
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<std::string>(), "ÉÈÊË");
    EXPECT_EQ(r->Row(0).Field(4).GetCastAs<std::string>(), "HÉLLÖ WÔRLD");
    EXPECT_EQ(r->Row(0).Field(5).IsNull(), true);
}

TEST_F(ConnectionForTest, SelectWithDateFunc) {
    auto r = conn->Query("SELECT current_date()");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    // get date string
    auto t = std::time(nullptr);
    auto tm = *std::localtime(&t);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d");
    std::cout << r->Row(0).Field(0).GetCastAs<std::string>() << " " << r->Row(0).Field(0).GetType() << std::endl;
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), oss.str());

    conn->Query("CREATE TABLE date_test (id INT, name VARCHAR(50), birthday TIMESTAMP)");
    conn->Query("INSERT INTO date_test VALUES (1, '张三', '1990-01-01 12:00:01')");
    conn->Query("INSERT INTO date_test VALUES (2, '李四', '1991-02-02 13:45:00')");
    conn->Query("INSERT INTO date_test VALUES (3, '王五', '1992-03-03 14:15:16')");

    // test func year , month , day
    r = conn->Query(
        "SELECT YEAR(birthday) , MONTH(birthday) , DAY(birthday), HOUR(birthday),MINUTE(birthday) FROM date_test");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1990);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 1991);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 1992);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(2).Field(1).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(2).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(2).Field(2).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<int32_t>(), 12);
    EXPECT_EQ(r->Row(1).Field(3).GetCastAs<int32_t>(), 13);
    EXPECT_EQ(r->Row(2).Field(3).GetCastAs<int32_t>(), 14);
    EXPECT_EQ(r->Row(0).Field(4).GetCastAs<int32_t>(), 0);
    EXPECT_EQ(r->Row(1).Field(4).GetCastAs<int32_t>(), 45);
    EXPECT_EQ(r->Row(2).Field(4).GetCastAs<int32_t>(), 15);

    // test func year with illegal params
    r = conn->Query("SELECT YEAR(null)");
    ASSERT_NE(r->GetRetCode(), 0);

    r = conn->Query("SELECT YEAR('1990-01-01 00:00:00')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "1990");

    r = conn->Query("SELECT YEAR(id) from date_test");
    ASSERT_NE(r->GetRetCode(), 0);

    // make date
    r = conn->Query("SELECT MAKE_DATE(2017, 1, 1)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "2017-01-01");

    r = conn->Query("SELECT MAKE_DATE(1992,12)");
    ASSERT_NE(r->GetRetCode(), 0);

    r = conn->Query("SELECT MAKE_DATE(1992,null,1)");
    ASSERT_NE(r->GetRetCode(), 0);

    r = conn->Query("SELECT MAKE_DATE(0,12,1)");
    ASSERT_NE(r->GetRetCode(), 0);

    r = conn->Query("SELECT MAKE_DATE(2023,12,32)");
    ASSERT_NE(r->GetRetCode(), 0);

    r = conn->Query("select '0030-01-01'::Date");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "0030-01-01");

    r = conn->Query("select unix_timestamp('2000-01-01 00:00:00'::timestamp)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int64_t>(), 946656000000000);
}

TEST_F(ConnectionForTest, SelectSequence) {
    // create sequence
    auto r = conn->Query("CREATE SEQUENCE seq_1 START 10");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("SELECT nextval('seq_1')");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 10);
    r = conn->Query("SELECT nextval('seq_1')");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 11);
    // create table for insert sequence
    r = conn->Query("CREATE TABLE seq_test (id INT, name VARCHAR(50))");
    EXPECT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->stmt_props.read_tables.size(),0);
    ASSERT_EQ(r->stmt_props.modify_tables.size(),1);

    // insert data
    conn->Query("INSERT INTO seq_test VALUES (nextval('seq_1'), '小明')");
    // select data
    r = conn->Query("SELECT * FROM seq_test");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 12);

    // 测试参数为空
    r = conn->Query("select nextval(null)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    // 测试函数的参数为列数据
    r = conn->Query("drop table if exists strings");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->stmt_props.read_tables.size(),0);
    ASSERT_EQ(r->stmt_props.modify_tables.size(),1);

    conn->Query("CREATE TABLE strings(s VARCHAR)");
    conn->Query("INSERT INTO strings VALUES ('seq'), ('seq2')");
    conn->Query("CREATE SEQUENCE seq");
    conn->Query("CREATE SEQUENCE seq2");
    r = conn->Query("select s , nextval(s) from strings");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<int32_t>(), 1);
}

// 非关联子查询测试
TEST_F(ConnectionForTest, SelectWithInCorretedSubQuery) {
    conn->Query("create table subquery_t1 (a int)");
    conn->Query("create table subquery_t2 (a int , b varchar(20))");
    conn->Query("insert into subquery_t1 values (1),(2),(10)");

    auto r = conn->Query("select * from subquery_t1 where a in (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    r = conn->Query("select * from subquery_t1 where a not in (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);

    r = conn->Query("select * from subquery_t1 where a > ANY (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    r = conn->Query("select * from subquery_t1 where a > ALL (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);

    r = conn->Query("select * from subquery_t1 where a < ANY (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    r = conn->Query("select * from subquery_t1 where a < ALL (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);

    r = conn->Query("select * from subquery_t1 where exists (select * from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    conn->Query("insert into subquery_t2 values (null,'abc')");

    r = conn->Query("select * from subquery_t1 where a in (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    r = conn->Query("select * from subquery_t1 where a not in (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    r = conn->Query("select * from subquery_t1 where a > ANY (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    r = conn->Query("select * from subquery_t1 where a > ALL (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    r = conn->Query("select * from subquery_t1 where a < ANY (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    r = conn->Query("select * from subquery_t1 where a < ALL (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    r = conn->Query("select * from subquery_t1 where exists (select * from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);

    conn->Query("insert into subquery_t2 values (1,'abc'),(5,'bbc')");

    r = conn->Query("select * from subquery_t1 where a in (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);

    // 因为 null的存在，not int =>  not(a = null) = unkown , 结果是unkown 不会被选择
    r = conn->Query("select * from subquery_t1 where a not in (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    r = conn->Query("select * from subquery_t1 where a > ANY (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);

    r = conn->Query("select * from subquery_t1 where a > ALL (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);  // 含有null，无法认为是大于ALL

    r = conn->Query("select * from subquery_t1 where a < ANY (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);

    r = conn->Query("select * from subquery_t1 where a < ALL (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    // in 子查询不可返回多列
    r = conn->Query("select * from subquery_t1 where a in (select 1 , 2)");
    EXPECT_NE(r->GetRetCode(), 0);

    r = conn->Query("select * from subquery_t1 where exists (select * from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);

    conn->Query("delete from subquery_t2 where a is null");

    r = conn->Query("select * from subquery_t1 where a in (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);

    r = conn->Query("select * from subquery_t1 where a not in (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);

    r = conn->Query("select * from subquery_t1 where a > ANY (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);

    r = conn->Query("select * from subquery_t1 where a > ALL (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);

    r = conn->Query("select * from subquery_t1 where a < ANY (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);

    r = conn->Query("select * from subquery_t1 where a <= ALL (select a from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);

    r = conn->Query("select * from subquery_t1 where exists (select * from subquery_t2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);

    // subquery in select list
    r = conn->Query(
        "select a , (select a from subquery_t2) from subquery_t1");  // 子查询返回多行,強制返回第一行,參照duckdb
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);

    r = conn->Query("select a , (select a from subquery_t2 where a >100) from subquery_t1");  // 子查询返回空
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);

    r = conn->Query("select (select a from subquery_t2 where a > 100) ,a, (select 1) from subquery_t1");  // 多个子查询
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);

    r = conn->Query(
        "select (select (select a from subquery_t2 where a > 100) from subquery_t2 ) from subquery_t1");  // 多层子查询
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);
}

// 测试关联子查询
TEST_F(ConnectionForTest, SelectWithCorretedSubQuery) {
    conn->Query("drop table if exists Orders");
    conn->Query(
        "CREATE TABLE Orders (order_id INT PRIMARY KEY,cust_id INT,order_date DATE,order_amount DECIMAL(10,2))");
    conn->Query("drop table if exists Customers");
    conn->Query("CREATE TABLE Customers (cust_id int, cust_name varchar(20), cust_state varchar(20))");
    conn->Query(
        "INSERT INTO Customers (cust_id, cust_name, cust_state) VALUES (1, 'John Doe', 'California'),(2, 'Jane Smith', "
        "'New York'),(3, 'David Johnson', 'Texas'),(4,'Eric','ABC')");
    conn->Query(
        "INSERT INTO Orders (order_id, cust_id, order_date, order_amount) VALUES (101, 1, '2023-01-01', 100.00),(102, "
        "1, '2023-02-15', 150.00),(103, 2, '2023-03-10', 200.00),(104, 2, '2023-04-20', 75.00),(105, 3, '2023-05-05', "
        "300.00)");

    auto r = conn->Query(
        "SELECT cust_name, cust_state, (SELECT COUNT(*) FROM Orders WHERE Orders.cust_id = Customers.cust_id) AS "
        "orders FROM Customers ORDER BY cust_name");
    EXPECT_EQ(r->RowCount(), 4);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "David Johnson");
    EXPECT_STREQ(r->Row(1).Field(0).GetCastAs<std::string>().c_str(), "Eric");
    EXPECT_STREQ(r->Row(2).Field(0).GetCastAs<std::string>().c_str(), "Jane Smith");
    EXPECT_STREQ(r->Row(3).Field(0).GetCastAs<std::string>().c_str(), "John Doe");

    EXPECT_STREQ(r->Row(0).Field(1).GetCastAs<std::string>().c_str(), "Texas");
    EXPECT_STREQ(r->Row(1).Field(1).GetCastAs<std::string>().c_str(), "ABC");
    EXPECT_STREQ(r->Row(2).Field(1).GetCastAs<std::string>().c_str(), "New York");
    EXPECT_STREQ(r->Row(3).Field(1).GetCastAs<std::string>().c_str(), "California");

    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(2).GetCastAs<int32_t>(), 0);
    EXPECT_EQ(r->Row(2).Field(2).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(3).Field(2).GetCastAs<int32_t>(), 2);

    conn->Query(
        "CREATE TABLE employees_for_subquery (employee_id INT,employee_name VARCHAR(50),department_id INT,sales_amount "
        "DECIMAL(10,2))");
    conn->Query(
        "INSERT INTO employees_for_subquery (employee_id, employee_name, department_id, sales_amount) VALUES (1, "
        "'John', 1, "
        "2000.00),(2, 'Jane', 1, 2500.00),(3, 'Mike', 2, 1800.00),(4, 'Emily', 2, 2200.00),(5, 'David', 3, "
        "1900.00),(6, 'Sarah', 3, 2100.00)");
    conn->Query("CREATE TABLE departments (department_id INT,department_name VARCHAR(50),sales_amount DECIMAL(10,2))");
    conn->Query(
        "INSERT INTO departments (department_id, department_name, sales_amount) VALUES (1, 'Sales', 2250.00),(2, "
        "'Marketing', 2000.00),(3, 'Finance', 2050.00)");
    r = conn->Query(
        "SELECT ( SELECT AVG(sales_amount) FROM employees_for_subquery WHERE department_id = e.department_id) as A "
        "from employees_for_subquery "
        "e");
    EXPECT_EQ(r->RowCount(), 6);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<double>(), 2250);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<double>(), 2250);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<double>(), 2000);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<double>(), 2000);
    EXPECT_EQ(r->Row(4).Field(0).GetCastAs<double>(), 2000);
    EXPECT_EQ(r->Row(5).Field(0).GetCastAs<double>(), 2000);

    r = conn->Query(
        "SELECT employee_name, sales_amount FROM employees_for_subquery e WHERE sales_amount > ( SELECT "
        "AVG(sales_amount) FROM "
        "employees_for_subquery WHERE department_id = e.department_id)");
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "Jane");
    EXPECT_STREQ(r->Row(1).Field(0).GetCastAs<std::string>().c_str(), "Emily");
    EXPECT_STREQ(r->Row(2).Field(0).GetCastAs<std::string>().c_str(), "Sarah");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<double>(), 2500);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<double>(), 2200);
    EXPECT_EQ(r->Row(2).Field(1).GetCastAs<double>(), 2100);

    r = conn->Query(
        "SELECT employee_name, sales_amount FROM employees_for_subquery e WHERE sales_amount < ( SELECT "
        "AVG(sales_amount) FROM "
        "employees_for_subquery WHERE department_id = e.department_id)");
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "John");
    EXPECT_STREQ(r->Row(1).Field(0).GetCastAs<std::string>().c_str(), "Mike");
    EXPECT_STREQ(r->Row(2).Field(0).GetCastAs<std::string>().c_str(), "David");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<double>(), 2000);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<double>(), 1800);
    EXPECT_EQ(r->Row(2).Field(1).GetCastAs<double>(), 1900);

    r = conn->Query(
        "SELECT employee_name, sales_amount FROM employees_for_subquery e WHERE sales_amount > ANY ( SELECT "
        "AVG(sales_amount) FROM "
        "employees_for_subquery WHERE department_id = e.department_id)");
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "Jane");
    EXPECT_STREQ(r->Row(1).Field(0).GetCastAs<std::string>().c_str(), "Emily");
    EXPECT_STREQ(r->Row(2).Field(0).GetCastAs<std::string>().c_str(), "Sarah");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<double>(), 2500);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<double>(), 2200);
    EXPECT_EQ(r->Row(2).Field(1).GetCastAs<double>(), 2100);

    r = conn->Query(
        "SELECT employee_name, sales_amount FROM employees_for_subquery e WHERE sales_amount > ALL ( SELECT "
        "AVG(sales_amount) FROM "
        "employees_for_subquery WHERE department_id = e.department_id)");
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "Jane");
    EXPECT_STREQ(r->Row(1).Field(0).GetCastAs<std::string>().c_str(), "Emily");
    EXPECT_STREQ(r->Row(2).Field(0).GetCastAs<std::string>().c_str(), "Sarah");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<double>(), 2500);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<double>(), 2200);
    EXPECT_EQ(r->Row(2).Field(1).GetCastAs<double>(), 2100);

    r = conn->Query(
        "SELECT employee_name, sales_amount FROM employees_for_subquery e WHERE (SELECT COUNT(*) FROM "
        "employees_for_subquery WHERE department_id "
        "= e.department_id AND sales_amount >= e.sales_amount) <= 2");
    EXPECT_EQ(r->RowCount(), 6);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "John");
    EXPECT_STREQ(r->Row(1).Field(0).GetCastAs<std::string>().c_str(), "Jane");
    EXPECT_STREQ(r->Row(2).Field(0).GetCastAs<std::string>().c_str(), "Mike");
    EXPECT_STREQ(r->Row(3).Field(0).GetCastAs<std::string>().c_str(), "Emily");
    EXPECT_STREQ(r->Row(4).Field(0).GetCastAs<std::string>().c_str(), "David");
    EXPECT_STREQ(r->Row(5).Field(0).GetCastAs<std::string>().c_str(), "Sarah");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<double>(), 2000);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<double>(), 2500);
    EXPECT_EQ(r->Row(2).Field(1).GetCastAs<double>(), 1800);
    EXPECT_EQ(r->Row(3).Field(1).GetCastAs<double>(), 2200);
    EXPECT_EQ(r->Row(4).Field(1).GetCastAs<double>(), 1900);
    EXPECT_EQ(r->Row(5).Field(1).GetCastAs<double>(), 2100);

    // test column is from outer query alias name
    r = conn->Query(
        "SELECT employee_name, sales_amount as x FROM employees_for_subquery e WHERE (SELECT COUNT(*) FROM "
        "employees_for_subquery WHERE department_id "
        "= e.department_id AND sales_amount >= e.x) <= 2");
    ASSERT_NE(r->GetRetCode(), 0);  // not support by TiDB

    conn->Query("drop table if exists integers");
    conn->Query("create table integers (i int)");
    conn->Query("insert into integers values (1),(2),(3),(null)");
    // test correlatd subquery in select list with alias , 重点是 alias , alias的出现在 select list完成之前
    r = conn->Query("select i as k , (select i from integers where i = k) from integers");  //
    // ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_NE(r->GetRetCode(), 0);  // 暂时不支持子查询中引用外部的列别名
}

TEST_F(ConnectionForTest, SelectComplexSubquery) {
    conn->Query("drop table if exists departments");
    auto r =
        conn->Query("CREATE TABLE departments (department_id INT PRIMARY KEY, department_name VARCHAR(50) NOT NULL)");
    ASSERT_EQ(r->GetRetCode(), 0);
    conn->Query("drop table if exists employees");
    r = conn->Query(
        R"(CREATE TABLE employees (employee_id INT PRIMARY KEY, first_name VARCHAR(50) NOT NULL,last_name VARCHAR(50) NOT NULL,
                                   department_id int , manager_id int))");
    ASSERT_EQ(r->GetRetCode(), 0);
    conn->Query("drop table if exists orders");
    r = conn->Query("CREATE TABLE orders ( order_id INT PRIMARY KEY, order_date DATE)");
    ASSERT_EQ(r->GetRetCode(), 0);
    conn->Query(
        "INSERT INTO departments (department_id, department_name) VALUES (1, 'Sales'), (2, 'Marketing'), (3, 'IT')");
    conn->Query(
        R"(INSERT INTO employees (employee_id, first_name, last_name, department_id, manager_id) VALUES 
                                  (1, 'John', 'Doe', 1, NULL), 
                                  (2, 'Jane', 'Smith', 1, 1), 
                                  (3, 'Tom', 'Johnson', 2, NULL), 
                                  (4, 'Alice', 'Williams', 3, NULL),  
                                  (5, 'Bob', 'Brown', 3, 4))");
    conn->Query(
        R"(INSERT INTO orders (order_id, order_date) VALUES (1, '2022-01-01'), (2, '2022-02-15'), (3, '2022-03-20'))");

    r = conn->Query(
        "SELECT d.department_name, (SELECT COUNT(*) FROM employees e WHERE e.department_id = d.department_id) as "
        "total_employees FROM departments d");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "Sales");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<std::string>(), "Marketing");
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<std::string>(), "IT");
    EXPECT_EQ(r->Row(2).Field(1).GetCastAs<int32_t>(), 2);

    r = conn->Query("select employee_id , (select first_name from orders ) from employees");
    ASSERT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select employee_id , (select first_name , last_name from orders ) from employees");
    ASSERT_NE(r->GetRetCode(), 0);
}

// 测试 null相关的问题
TEST_F(ConnectionForTest, SelectWithNull) {
    conn->Query("CREATE TABLE test_null ( salary real, department VARCHAR(50))");

    // 空表的查询
    auto r = conn->Query("SELECT count(*) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 0);

    r = conn->Query("SELECT count(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 0);

    r = conn->Query("SELECT sum(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("SELECT avg(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("SELECT min(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("SELECT max(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    conn->Query("INSERT INTO test_null VALUES (null, 'IT')");
    r = conn->Query("SELECT min(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("SELECT sum(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("SELECT avg(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    conn->Query("INSERT INTO test_null VALUES (100, 'IT')");

    r = conn->Query("SELECT min(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 100);

    r = conn->Query("SELECT max(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 100);

    r = conn->Query("SELECT sum(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 100);

    r = conn->Query("SELECT avg(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<double>(), 100);

    conn->Query("INSERT INTO test_null VALUES (200, 'Marketing')");

    r = conn->Query("SELECT count(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);

    r = conn->Query("SELECT sum(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 300);

    r = conn->Query("SELECT avg(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<double>(), 150);

    r = conn->Query("SELECT min(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 100);

    r = conn->Query("SELECT max(salary) FROM test_null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 200);

    // order with null
    conn->Query("INSERT INTO test_null VALUES (null, 'Marketing')");
    r = conn->Query("SELECT * FROM test_null ORDER BY salary , department");
    EXPECT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 100);
    EXPECT_STREQ(r->Row(0).Field(1).GetCastAs<std::string>().c_str(), "IT");

    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 200);
    EXPECT_STREQ(r->Row(1).Field(1).GetCastAs<std::string>().c_str(), "Marketing");

    EXPECT_EQ(r->Row(2).Field(0).IsNull(), true);
    EXPECT_STREQ(r->Row(2).Field(1).GetCastAs<std::string>().c_str(), "IT");

    EXPECT_EQ(r->Row(3).Field(0).IsNull(), true);
    EXPECT_STREQ(r->Row(3).Field(1).GetCastAs<std::string>().c_str(), "Marketing");

    r = conn->Query("SELECT * FROM test_null ORDER BY salary desc, department");
    EXPECT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 200);
    EXPECT_STREQ(r->Row(0).Field(1).GetCastAs<std::string>().c_str(), "Marketing");

    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 100);
    EXPECT_STREQ(r->Row(1).Field(1).GetCastAs<std::string>().c_str(), "IT");

    EXPECT_EQ(r->Row(2).Field(0).IsNull(), true);
    EXPECT_STREQ(r->Row(2).Field(1).GetCastAs<std::string>().c_str(), "IT");

    EXPECT_EQ(r->Row(3).Field(0).IsNull(), true);
    EXPECT_STREQ(r->Row(3).Field(1).GetCastAs<std::string>().c_str(), "Marketing");

    r = conn->Query("SELECT * FROM test_null ORDER BY salary desc, department desc");
    EXPECT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->Row(2).Field(0).IsNull(), true);
    EXPECT_STREQ(r->Row(2).Field(1).GetCastAs<std::string>().c_str(), "Marketing");

    EXPECT_EQ(r->Row(3).Field(0).IsNull(), true);
    EXPECT_STREQ(r->Row(3).Field(1).GetCastAs<std::string>().c_str(), "IT");

    r = conn->Query("SELECT SUM(null),AVG(3),AVG(null)");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(0).Field(2).IsNull(), true);
}

// 类型转换测试
TEST_F(ConnectionForTest, SelectTypeCast) {
    auto r = conn->Query("select true");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("select try_cast('2022-08-21 12:12:12' as timestamp)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "2022-08-21 12:12:12.000000");

    r = conn->Query("select try_cast('2022-08-21 12:12:12' as date)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "2022-08-21");

    r = conn->Query("select try_cast('2022-08-32 12:12:12' as date)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("select cast('2022-08-32 12:12:12' as date)");
    EXPECT_NE(r->GetRetCode(), 0);

    // big year
    r = conn->Query("select try_cast('20222-08-21 12:12:12' as timestamp)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("select try_cast('20222-08-21 12:12:12' as date)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("select cast('20222-08-21 12:12:12' as timestamp)");
    EXPECT_NE(r->GetRetCode(), 0);

    r = conn->Query("select try_cast('2011-12-12 12:12:12+08' as timestamp)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "2011-12-12 04:12:12.000000");

    r = conn->Query("select try_cast('2011-12-12 12:12:12+08:30' as timestamp)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "2011-12-12 03:42:12.000000");

    r = conn->Query("select try_cast('2011-12-12 12:12:12 uTc' as timestamp)");  // 测试大小写混合的UTC
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("SELECT TIMESTAMP '2008-01-01 00:00:01.5'::VARCHAR");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "2008-01-01 00:00:01.500000");  // 测试保留毫秒位

    r = conn->Query("SELECT TIMESTAMP '2008-01-01 00:00:01.50'::VARCHAR");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "2008-01-01 00:00:01.500000");  // 测试保留毫秒位
                                                                                                      //
    r = conn->Query("SELECT TIMESTAMP '2008-01-01 00:00:01.52'::VARCHAR");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "2008-01-01 00:00:01.520000");  // 测试保留毫秒位

    r = conn->Query("select try_cast(1.2 as int)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);

    r = conn->Query("select try_cast(1.5 as int)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);

    r = conn->Query("select '0-01-01'::Date");
    EXPECT_NE(r->GetRetCode(), 0);

    r = conn->Query("select '100000-01-01'::Date");
    EXPECT_NE(r->GetRetCode(), 0);

    r = conn->Query("select 1.5 < 2");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);

    // 类型提升
    conn->Query("create table un ( a uint8 , b uint16, c uint32 , d uint64)");
    conn->Query("insert into un values (1,2,3,4)");
    r = conn->Query("select a - 1 from un");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetType(), GS_TYPE_INTEGER);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 0);

    // r = conn->Query("select -a from un");
    // EXPECT_EQ(r->GetRetCode(), 0);
    // EXPECT_EQ(r->RowCount(), 1);
    // EXPECT_EQ(r->Row(0).Field(0).GetType(), GS_TYPE_SMALLINT);
    // EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), -1);

    r = conn->Query("select b * 1 , d + 1 from un");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetType(), GS_TYPE_INTEGER);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(0).Field(1).GetType(), GS_TYPE_REAL);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int64_t>(), 5);

    //  字符串转换 int tinyint smallint
    conn->Query("drop table if exists int_strings");
    conn->Query("CREATE TABLE int_strings(s string)");
    conn->Query(
        "INSERT INTO int_strings VALUES(' '), ('blablabla'), ('-100000'), ('-32768'), ('0'), ('32767'), ('100000')");
    r = conn->Query("SELECT TRY_CAST(s AS SMALLINT) FROM int_strings");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 7);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    EXPECT_EQ(r->Row(1).Field(0).IsNull(), true);
    EXPECT_EQ(r->Row(2).Field(0).IsNull(), true);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), -32768);
    EXPECT_EQ(r->Row(4).Field(0).GetCastAs<int32_t>(), 0);
    EXPECT_EQ(r->Row(5).Field(0).GetCastAs<int32_t>(), 32767);
    EXPECT_EQ(r->Row(6).Field(0).IsNull(), true);

    // decimal -> double 测试
    conn->Query(
        "CREATE TABLE tpch_q1_agg (l_returnflag VARCHAR, l_linestatus VARCHAR, sum_qty INT, sum_base_price DOUBLE, "
        "sum_disc_price DOUBLE, sum_charge DOUBLE, avg_qty DOUBLE, avg_price DOUBLE, avg_disc DOUBLE, count_order "
        "BIGINT)");
    conn->Query(
        "INSERT INTO tpch_q1_agg VALUES('R', 'F', 3785523, 5337950526.47, 5071818532.9420, 5274405503.049367, "
        "25.5259438574251, 35994.029214030925, 0.04998927856184382, 148301)");
    r = conn->Query("select * from tpch_q1_agg");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "R");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "F");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 3785523);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "3785523");
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<double>(), 5337950526.47);
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<std::string>(), "5337950526.47");
    EXPECT_EQ(r->Row(0).Field(4).GetCastAs<double>(), 5071818532.942);
    EXPECT_EQ(r->Row(0).Field(5).GetCastAs<double>(), 5274405503.049367);
    EXPECT_EQ(r->Row(0).Field(5).GetCastAs<std::string>(), "5274405503.049367");
    EXPECT_EQ(r->Row(0).Field(6).GetCastAs<double>(), 25.5259438574251);
    EXPECT_EQ(r->Row(0).Field(6).GetCastAs<std::string>(), "25.5259438574251");
    EXPECT_EQ(r->Row(0).Field(7).GetCastAs<double>(), 35994.029214030925);
    EXPECT_EQ(r->Row(0).Field(7).GetCastAs<std::string>(), "35994.029214030925");
    EXPECT_EQ(r->Row(0).Field(8).GetCastAs<double>(), 0.04998927856184382);
    EXPECT_EQ(r->Row(0).Field(8).GetCastAs<std::string>(), "0.04998927856184382");
    EXPECT_EQ(r->Row(0).Field(9).GetCastAs<int64_t>(), 148301);
    EXPECT_EQ(r->Row(0).Field(9).GetCastAs<std::string>(), "148301");

    conn->Query("drop table if exists test_double");
    r = conn->Query("create table test_double (a double)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("insert into test_double values (35521.32691633466)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select * from test_double");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<double>(), 35521.32691633466);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "35521.32691633466");
}

TEST_F(ConnectionForTest, SelectBooleanOp) {
    auto r = conn->Query("select true and true");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), (int32_t)Trivalent::TRI_TRUE);

    r = conn->Query("select true and null");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    // null 不应该检查值
    // EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), (int32_t)Trivalent::UNKNOWN);

    r = conn->Query("select true or true");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), (int32_t)Trivalent::TRI_TRUE);

    r = conn->Query("select false and true");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), (int32_t)Trivalent::TRI_FALSE);

    r = conn->Query("select null and false");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), (int32_t)Trivalent::TRI_FALSE);

    r = conn->Query("select null or false");
    EXPECT_EQ(r->RowCount(), 1);
    // EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), (int32_t)Trivalent::UNKNOWN);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("select null or true");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), (int32_t)Trivalent::TRI_TRUE);

    r = conn->Query("select 1 and 2");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), (int32_t)Trivalent::TRI_TRUE);

    r = conn->Query("select 1 and 0");
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), (int32_t)Trivalent::TRI_FALSE);

    r = conn->Query("select try_cast( true as int)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
}

TEST_F(ConnectionForTest, SelectWithUnion) {
    auto r = conn->Query("select 1 , 2 union select 2 , 1");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    const auto& columns1 = r->GetSchema().GetColumnInfos();
    ASSERT_EQ(columns1.size(), 2);
    EXPECT_EQ(columns1[0].GetColNameWithoutTableName(), "1");
    EXPECT_EQ(columns1[1].GetColNameWithoutTableName(), "2");
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<int32_t>(), 1);

    // union & order by
    r = conn->Query("select 1 as a , 2 as b union select 2 as b, 1 as a");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    const auto& columns2 = r->GetSchema().GetColumnInfos();
    ASSERT_EQ(columns2.size(), 2);
    EXPECT_EQ(columns2[0].GetColNameWithoutTableName(), "a");
    EXPECT_EQ(columns2[1].GetColNameWithoutTableName(), "b");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<int32_t>(), 1);

    r = conn->Query("select 1 as a , 2 as b union select 2 as b, 1 as a order by a desc");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<int32_t>(), 2);

    r = conn->Query("select 1 as a , 2 as a union select 2 , 1 order by a desc");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<int32_t>(), 2);

    r = conn->Query("select 2 as a union select 1 as b order by b");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    const auto& columns3 = r->GetSchema().GetColumnInfos();
    ASSERT_EQ(columns3.size(), 1);
    EXPECT_EQ(columns3[0].GetColNameWithoutTableName(), "a");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 2);

    r = conn->Query("select 2 as a union select 1 as b order by a");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    const auto& columns4 = r->GetSchema().GetColumnInfos();
    ASSERT_EQ(columns4.size(), 1);
    EXPECT_EQ(columns4[0].GetColNameWithoutTableName(), "a");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 2);

    r = conn->Query("values (1) union values (2)");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 2);

    conn->Query("drop table if exists t1_a");
    conn->Query("drop table if exists t1_b");
    conn->Query("drop table if exists t1_c");
    conn->Query("CREATE TABLE t1_a(a INTEGER PRIMARY KEY, b TEXT)");
    conn->Query("CREATE TABLE t1_b(c INTEGER PRIMARY KEY, d TEXT)");
    conn->Query("CREATE TABLE t1_c(e INTEGER PRIMARY KEY, f TEXT)");
    conn->Query("INSERT INTO t1_a VALUES(1, 'one'), (4, 'four')");
    conn->Query("INSERT INTO t1_b VALUES(2, 'two'), (5, 'five')");
    conn->Query("INSERT INTO t1_c VALUES(3, 'three'), (6, 'six')");
    r = conn->Query("SELECT a, b FROM t1_a UNION ALL SELECT c, d FROM t1_b UNION ALL SELECT e, f FROM t1_c");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 6);
    r = conn->Query(
        "select * from (SELECT a, b FROM t1_a UNION ALL SELECT c, d FROM t1_b UNION ALL SELECT e, f FROM t1_c)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 6);

    r = conn->Query("select 1 ,2 union select 2 , 3 limit 1");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);

    r = conn->Query("select 1.00 , 2 union select 2 , null");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    auto type1 = r->RowRef(0).Field(0).GetLogicalType();
    EXPECT_EQ(type1.TypeId(), GS_TYPE_DECIMAL);
    EXPECT_EQ(static_cast<int32_t>(type1.Scale()), 2);
    EXPECT_EQ(static_cast<int32_t>(type1.Precision()), 12);
    auto type2 = r->RowRef(0).Field(1).GetLogicalType();
    EXPECT_EQ(type2.TypeId(), GS_TYPE_INTEGER);

    r = conn->Query("select '1' union select 1");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    // check type
    auto type = r->Row(0).Field(0).GetLogicalType();
    EXPECT_EQ(type.TypeId(), GS_TYPE_VARCHAR);
    EXPECT_EQ(type.Length() > 0, true);

    r = conn->Query("select a , b from t1_a union select b ,a from t1_a");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 4);
    // check type
    type = r->Row(0).Field(0).GetLogicalType();
    EXPECT_EQ(type.TypeId(), GS_TYPE_VARCHAR);
    EXPECT_EQ(type.Length() > 0, true);
    type = r->Row(0).Field(1).GetLogicalType();
    EXPECT_EQ(type.TypeId(), GS_TYPE_VARCHAR);
    EXPECT_EQ(type.Length() > 0, true);

    r = conn->Query("create table u_t1 as select a , b from t1_a union select b , a from t1_a");
    ASSERT_EQ(r->GetRetCode(), 0);

    // ambiguous column
    r = conn->Query("select a , 1 from t1_a union select b , 2 as a from t1_a order by a");
    // ASSERT_NE(r->GetRetCode(), 0);
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "1");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "1");
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<std::string>(), "4");
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<std::string>(), "1");
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<std::string>(), "four");
    EXPECT_EQ(r->Row(2).Field(1).GetCastAs<std::string>(), "2");
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<std::string>(), "one");
    EXPECT_EQ(r->Row(3).Field(1).GetCastAs<std::string>(), "2");

    conn->Query("drop table if exists t1_a");
    conn->Query("create table t1_a(x int , a int , b int , c int)");
    conn->Query("insert into t1_a values (1,2,3,4),(4,5,6,7),(7,8,9,10)");
    r = conn->Query("select a from t1_a union select b from t1_a order by b desc");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 6);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 9);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 8);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 6);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->Row(4).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(5).Field(0).GetCastAs<int32_t>(), 2);

    r = conn->Query("select (select null) union all select cast(1 as boolean)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->RowRef(0).Field(0).IsNull(), true);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 1);

    // FIX #709858705 , 两个union 连续
    r = conn->Query("drop table if exists u_t1");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("create table u_t1 (a int)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("drop table if exists u_t2");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("create table u_t2 (b int)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("insert into u_t1 values (1),(2),(3),(null)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("insert into u_t2 values (2),(3),(4),(null)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("SELECT a FROM u_t1 UNION SELECT b FROM u_t2 UNION SELECT b AS c FROM u_t2 ORDER BY c");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(3).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(4).Field(0).IsNull(), true);

    r = conn->Query("SELECT a FROM u_t1 UNION ALL SELECT b FROM u_t2");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 8);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(3).Field(0).IsNull(), true);
    EXPECT_EQ(r->RowRef(4).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(5).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(6).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(7).Field(0).IsNull(), true);

    r = conn->Query("select a - 10 as k from u_t1 union select b - 10 as i from u_t2 order by a - 10");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), -9);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), -8);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), -7);
    EXPECT_EQ(r->RowRef(3).Field(0).GetCastAs<int32_t>(), -6);
    EXPECT_EQ(r->RowRef(4).Field(0).IsNull(), true);

    r = conn->Query("SELECT a FROM u_t1 UNION SELECT b FROM u_t2 UNION SELECT b AS c FROM u_t2 ORDER BY b");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->Row(4).Field(0).IsNull(), true);

    r = conn->Query("SELECT a FROM u_t1 UNION SELECT b FROM u_t2 UNION SELECT 100 AS c FROM u_t2 ORDER BY b");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 6);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->Row(4).Field(0).GetCastAs<int32_t>(), 100);
    EXPECT_EQ(r->Row(5).Field(0).IsNull(), true);

    // sqlite 支持 ,duckdb 不支持
    r = conn->Query("SELECT a FROM u_t1 UNION SELECT b as d FROM u_t2 UNION SELECT 100 AS c FROM u_t2 ORDER BY b");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 6);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->Row(4).Field(0).GetCastAs<int32_t>(), 100);
    EXPECT_EQ(r->Row(5).Field(0).IsNull(), true);

    r = conn->Query("SELECT a FROM u_t1 UNION SELECT b as d from u_t2 ORDER BY b");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(3).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(4).Field(0).IsNull(), true);
}

TEST_F(ConnectionForTest, SelectUnionIntersectAndExcept) {
    conn->Query("drop table if exists stu");
    conn->Query("drop table if exists sc");
    conn->Query("create table stu(sno varchar(10),sname varchar(20),sage decimal(2),ssex varchar(5))");
    conn->Query(
        "create table sc(sno varchar(10),cno varchar(10),score decimal(4,2),constraint pk_sc primary key (sno,cno))");
    conn->Query(
        "insert into stu values ('s001','张三',23,'男'),('s002','李四',23,'男'),('s003','吴鹏',25,'男'),"
        "('s004','琴沁',20,'女'),('s005','王丽',20,'女'),('s006','李波',21,'男'),"
        "('s007','刘玉',21,'男'),('s001','萧蓉',21,'女'),('s002','陈萧晓',23,'女'),"
        "('s003','陈美',22,'女')");
    conn->Query(
        "insert into sc values ('s001','c001',78.9),('s002','c001',80.9),('s003','c001',81.9),"
        "('s004','c001',60.9),('s001','c002',82.9),('s002','c002',72.9),('s003','c002',81.9),"
        "('s001','c003',59)");
    auto r = conn->Query("select sno from stu intersect select sno from sc");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 4);

    r = conn->Query("select sno from stu intersect all select sno from sc");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 7);

    // test 对称性
    r = conn->Query("select sno from sc intersect all select sno from stu");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 7);

    r = conn->Query("select sno from sc except select sno from stu");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    r = conn->Query("select sno from stu except select sno from sc");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);

    r = conn->Query("select sno from sc except all select sno from stu");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "s001");

    r = conn->Query("select sno from stu except all select sno from sc");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);
}

TEST_F(ConnectionForTest, SelectTestWithSubqueryInFromClause) {
    conn->Query("drop table if exists t");
    conn->Query("drop table if exists t2");
    conn->Query("drop table if exists t0");
    conn->Query("drop table if exists t1_a");
    conn->Query("drop table if exists t1_b");

    conn->Query("create table t(a int , b int)");
    conn->Query("create table t2(a int , b int)");
    conn->Query("create table t0(c0 int)");

    conn->Query("insert into t values (1,1)");
    conn->Query("insert into t2 values (2,2)");

    conn->Query("drop view if exists t1_view");
    conn->Query("CREATE VIEW t1_view AS SELECT a, b FROM t UNION ALL SELECT a, b FROM t2");
    auto r = conn->Query("SELECT * FROM (SELECT t1_view.a, t1_view.b AS b, t0.c0 FROM t0, t1_view)");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select count(*) from t1_view");  // test fast scan with view
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);

    r = conn->Query("select count(*) from (select 1 union select 2)");  // test fast scan with subquery
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);

    // t(a ,b) , t2(a ,b )
    r = conn->Query("select * from (select avg(a) from t,t2)");
    EXPECT_NE(r->GetRetCode(), 0);

    // 测试表名不一致
    r = conn->Query("select t1.a from (select a from t) as t0");
    EXPECT_NE(r->GetRetCode(), 0);

    // 含有. 的列
    r = conn->Query("select * from (select avg(t.a) from t , t2)");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select * from (select 1.1)");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select * from ( select * from (select a from t))");
    EXPECT_EQ(r->GetRetCode(), 0);

    // values in from clause
    r = conn->Query("select * from (values (1))");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);

    r = conn->Query("select * from (values (1),(-1))");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), -1);

    r = conn->Query("select * from (values (1),(1,2))");  // values lists must all be the same length
    ASSERT_NE(r->GetRetCode(), 0);

    conn->Query("drop table if exists a");
    conn->Query("create table a( i int )");
    conn->Query("insert into a values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(100)");
    r = conn->Query("select count(*) , sum(i) from a , (select 100::int as j) b where i < j");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 11);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 66);

    // select subquery export same column name
    r = conn->Query("select * from (select 42 as a , 44 as a) tbl");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->ColumnCount(), 2);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 44);
}

TEST_F(ConnectionForTest, SelectOnlyValues) {
    auto r = conn->Query("values (1)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);

    // multi row
    r = conn->Query("values (1),(2)");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 2);
}

TEST_F(ConnectionForTest, SelectDecimal) {
    auto r = conn->Query("select 1.1");
    auto decimal = r->Row(0).Field(0);
    auto type = decimal.GetLogicalType();
    EXPECT_EQ(type.type, GS_TYPE_DECIMAL);
    EXPECT_EQ(type.scale, 1);
    EXPECT_EQ(type.precision, 2);

    r = conn->Query("select .34");
    ASSERT_EQ(r->GetRetCode(), 0);
    decimal = r->Row(0).Field(0);
    type = decimal.GetLogicalType();
    EXPECT_EQ(type.scale, 2);                                        // 小数点有效
    EXPECT_EQ(type.precision, 2);                                    // 总长度
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "0.34");  // 测试decimal打印效果

    conn->Query("drop table if exists bmsql_warehouse");
    conn->Query("create table bmsql_warehouse (d_tax decimal(4,4))");
    r = conn->Query("INSERT INTO bmsql_warehouse (d_tax) VALUES (0.1234)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("INSERT INTO bmsql_warehouse (d_tax) VALUES (0.12342)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("INSERT INTO bmsql_warehouse (d_tax) VALUES (1.1234)");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("INSERT INTO bmsql_warehouse (d_tax) VALUES (-.1234)");
    ASSERT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select '6.1111'::Decimal(4,3)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "6.111");

    r = conn->Query("select 3.5::Decimal(5,2)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "3.50");

    r = conn->Query("select .1::Decimal(2,2)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "0.10");

    r = conn->Query("select 3::Decimal(2,2)");
    ASSERT_NE(r->GetRetCode(), 0);

    r = conn->Query("select 3.5::Decimal(2,2)");
    ASSERT_NE(r->GetRetCode(), 0);

    r = conn->Query("select 3.5::Decimal(5,0)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "3");

    r = conn->Query("select 3::Decimal(5,2)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "3.00");

    r = conn->Query("select 123::Decimal(4,2)");
    ASSERT_NE(r->GetRetCode(), 0);

    conn->Query("drop table if exists tbl");
    conn->Query("drop table if exists decimals");
    conn->Query("create table tbl(i int)");
    conn->Query("insert into tbl values(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)");
    r = conn->Query(
        "CREATE TABLE decimals AS SELECT i::DECIMAL(4,1) AS d1, (i * i)::DECIMAL(9,1) AS d2, (i * i * "
        "i)::DECIMAL(18,1) AS d3 FROM tbl");
    ASSERT_EQ(r->GetRetCode(), 0);

    // TODO: agg 的 decimal 问题，后续支持
    // r = conn->Query("SELECT SUM(d1) , SUM(d2) , SUM(d3) FROM decimals");
    // ASSERT_EQ(r->GetRetCode(), 0);
    // auto value_type = r->RowRef(0).Field(0).GetLogicalType();
    // EXPECT_EQ(value_type.type, GS_TYPE_DECIMAL);
    // EXPECT_EQ(value_type.scale, 1);
    // EXPECT_EQ(value_type.precision, 38);

    // r = conn->Query("SELECT SUM(d1)::VARCHAR, SUM(d2)::VARCHAR, SUM(d3)::VARCHAR FROM decimals");
    // ASSERT_EQ(r->GetRetCode(), 0);
    // EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "45.0");
    // EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "285.0");
    // EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "2025.0");

    r = conn->Query("SELECT '0.1'::DECIMAL * '10.0'::DECIMAL");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<std::string>(), "1.000000");

    r = conn->Query("select 999999.9999::Decimal(18,3)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<std::string>(), "999999.999");

    // decimal to decimal , 其他类型 to decimal 有不同的规则
    r = conn->Query("select 999999.9999::double::Decimal(18,3)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<std::string>(), "1000000.000");
}

TEST_F(ConnectionForTest, SelectSpecialValue) {
    auto r = conn->Query("select try_cast('-infinity' as date)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    // EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "-infinity");
    r = conn->Query("select try_cast('infinity' as date)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    // EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "infinity");

    r = conn->Query(" select try_cast('infinity' as timestamp)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
    // EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "infinity");
}

TEST_F(ConnectionForTest, SelectMath) {
    auto r = conn->Query("select 1 / 2 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    auto v = r->Row(0).Field(0);
    EXPECT_EQ(v.GetCastAs<double>(), 0.5);
    EXPECT_EQ(v.GetType(), GS_TYPE_REAL);
    EXPECT_EQ(v.GetCastAs<std::string>(), "0.5");

    r = conn->Query("select 1::Decimal(5,2) / 2::Decimal(5,2)");
    ASSERT_EQ(r->GetRetCode(), 0);
    v = r->Row(0).Field(0);
    EXPECT_EQ(v.GetType(), GS_TYPE_REAL);
    EXPECT_EQ(v.GetCastAs<std::string>(), "0.5");

    r = conn->Query("select 1::Decimal(5,2) / 0::Decimal(5,2)");
    ASSERT_EQ(r->GetRetCode(), 0);
    v = r->Row(0).Field(0);
    // EXPECT_EQ(v.GetType(), GS_TYPE_REAL); // 后续支持
    EXPECT_EQ(v.IsNull(), true);

    r = conn->Query("select 1::double / 0::double");
    ASSERT_EQ(r->GetRetCode(), 0);
    v = r->Row(0).Field(0);
    EXPECT_EQ(v.GetType(), GS_TYPE_REAL);
    EXPECT_EQ(v.IsNull(), true);

    r = conn->Query("SELECT ('0.5'::DECIMAL(1,1) + 10000)::VARCHAR");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "10000.5");

    r = conn->Query("select 'nan'::float + 1");
    ASSERT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select 1 + null");
    ASSERT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select 'inf'::float == 'inf'::float");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    // test add but overflow
    r = conn->Query("select 1::int + 2147483647::int");
    ASSERT_NE(r->GetRetCode(), 0);

    r = conn->Query("select 200::UTINYINT + 200::UTINYINT");
    ASSERT_NE(r->GetRetCode(), 0);

    // test add with unspported type
    r = conn->Query("select 1::int + '1'::varchar");
    ASSERT_NE(r->GetRetCode(), 0);

    // test mul but overflow
    r = conn->Query("select 200::UTINYINT * 200::UTINYINT");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select 200::INT * 200::INT");
    ASSERT_EQ(r->GetRetCode(), 0);

    // mul with null
    r = conn->Query("select 200::INT * null");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    // test op with not number type
    r = conn->Query("select 200::INT * '1'::varchar");
    ASSERT_NE(r->GetRetCode(), 0);

    r = conn->Query("select 200::INT / '1'::varchar");
    ASSERT_NE(r->GetRetCode(), 0);

    r = conn->Query("select 200::INT % '1'::varchar");
    ASSERT_NE(r->GetRetCode(), 0);

    // test sum and avg with overflow
    r = conn->Query("create table test_agg_overflow (a tinyint)");
    ASSERT_EQ(r->GetRetCode(), 0);

    r = conn->Query("insert into test_agg_overflow values (100),(100),(100),(100),(100),(100),(100),(100),(100),(100)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select sum(a) from test_agg_overflow");
    ASSERT_EQ(r->GetRetCode(), 0);  // sum 会自动提升类型 为 INT64 或者 UINT64 , 最好应该提升到128bit的，但是目前不支持
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int64_t>(), 1000);

    r = conn->Query("select avg(a) from test_agg_overflow");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<double>(), 100);

    // test negative overflow
    r = conn->Query("CREATE TABLE minima (t TINYINT, s SMALLINT, i INTEGER, b BIGINT)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("INSERT INTO minima VALUES (-128, -32768, -2147483648, -9223372036854775808)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("SELECT -t FROM minima");
    ASSERT_NE(r->GetRetCode(), 0);

    // test minus op
    r = conn->Query("SELECT 1::UINT64 - 2");  // 类型提升
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetLogicalType().TypeId(), GS_TYPE_REAL);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int64_t>(), -1);

    r = conn->Query("SELECT 1 -1 ");
    ASSERT_EQ(r->GetRetCode(), 0);

    r = conn->Query("SELECT 1::TINYINT - 1::TINYINT ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 0);

    r = conn->Query("SELECT 1::UTINYINT - 2::UTINYINT ");
    ASSERT_NE(r->GetRetCode(), 0);

    r = conn->Query("SELECT 1::SMALLINT - 1::SMALLINT ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 0);

    r = conn->Query("SELECT 1::BIGINT - 2::BIGINT");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int64_t>(), -1);

    // 大整数处理（hugeint）
    r = conn->Query("SELECT  9223372036854775809+1");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "9223372036854775810");

    r = conn->Query("SELECT -9223372036854775809-1");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "-9223372036854775810");
}

TEST_F(ConnectionForTest, SelectWithInf) {
    auto r = conn->Query("select 'inf'::float == 'inf'::float");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("select 'inf'::float > '-inf'::float");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("select 'inf'::float < '-inf'::float");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("select 'inf'::float > 1.0");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("select 'inf'::float < 1.0");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("select 1.0 < 'inf'::float");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("select 1.0 > 'inf'::float");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("select 1.0 > '-inf'::float");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    conn->Query("drop table if exists floats");
    conn->Query("create table floats (a float)");
    conn->Query("insert into floats values ('inf'),(1),('-inf')");
    conn->Query("create index f1 on floats(a)");
    r = conn->Query("select * from floats where a > 0");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    r = conn->Query("select * from floats where a = 1");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);

    conn->Query("insert into floats values ('nan')");
    r = conn->Query("select * from floats order by a desc");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 4);
    EXPECT_EQ(std::isnan(r->Row(0).Field(0).GetCastAs<double>()), true);
    EXPECT_EQ(std::isinf(r->Row(1).Field(0).GetCastAs<double>()), true);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<double>(), 1);
    EXPECT_EQ(std::isinf(r->Row(3).Field(0).GetCastAs<double>()), true);
}

TEST_F(ConnectionForTest, SelectWithStar) {
    conn->Query("drop table if exists t");
    conn->Query("create table t(a int , b int)");
    conn->Query("insert into t values (1,1)");
    auto r = conn->Query("select not_t.* from t");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select t.* from t");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select *,a from t ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->ColumnCount(), 3);
    r = conn->Query("select a,* from t");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->ColumnCount(), 3);
    // ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select *,* from t");
    // ASSERT_NE(r->GetRetCode(), 0);
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->ColumnCount(), 4);
    r = conn->Query("select tx.* from t as tx");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select tb.* from t as tx , t as ty");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("select tx.* from t as tx , t as ty");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->ColumnCount(), 2);

    // error statement
    r = conn->Query("select * + 1 from t");
    ASSERT_NE(r->GetRetCode(), 0);

    // not support exclude
    r = conn->Query("select * exclude(b) from t");
    ASSERT_NE(r->GetRetCode(), 0);

    // not support replace
    r = conn->Query("select * replace(b as z) from t");
    ASSERT_NE(r->GetRetCode(), 0);

    // not support * with lambda
    r = conn->Query("select COLUMNS([x for x in (*) if x <> 't.a']) FROM t");
    ASSERT_NE(r->GetRetCode(), 0);
}

TEST_F(ConnectionForTest, SelectWithIndex) {
    // 测试多字段索引的选择是否正确,保证不会因选错索引，导致查询结果异常
    conn->Query("drop table if exists t");
    conn->Query("create table t(a int , b int , c int, d int)");
    conn->Query("create index t_idx_test1 on t(a,b)");
    conn->Query("create index t_idx_test2 on t(a,b,c,d)");
    conn->Query("insert into t values (1,1,1,1),(2,2,2,2),(3,3,3,3)");
    auto r = conn->Query("select * from t where a = 1 and b = 1");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<int32_t>(), 1);

    r = conn->Query("select * from t where b >= 2");  // should not use index
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);

    r = conn->Query("select * from t where b = 1 and d = 1 and a  >= 1");  // use index
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);

    r = conn->Query("select * from t where b = 1 and c = 1 and d = 1");  // not use index
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
}

TEST_F(ConnectionForTest, FixBug710145225) {
    // 子查询中存在别名时出现问题
    conn->Query("CREATE TABLE subquery_with_alias (x int,a int,b int,c int)");
    conn->Query("INSERT INTO subquery_with_alias VALUES(1,2,3,4)");
    conn->Query("INSERT INTO subquery_with_alias VALUES(4,5,6,7)");
    conn->Query("INSERT INTO subquery_with_alias VALUES(7,8,9,10)");
    auto r = conn->Query(
        "SELECT max(cnt) AS mx FROM (SELECT a%2 AS eo, count(*) AS cnt FROM subquery_with_alias GROUP BY eo)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);

    r = conn->Query("SELECT * FROM (SELECT a%2 AS eo, count(*) AS cnt FROM subquery_with_alias GROUP BY eo)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 0);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<int32_t>(), 1);

    // TODO: 需要支持
    // r = conn->Query("SELECT * FROM (SELECT a%2 AS eo, count(*) AS cnt FROM subquery_with_alias GROUP BY eo + 1)");
    // ASSERT_EQ(r->GetRetCode(), 0);
    // EXPECT_EQ(r->RowCount(), 2);
    // EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 0);
    // EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 2);
    // EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 1);
    // EXPECT_EQ(r->Row(1).Field(1).GetCastAs<int32_t>(), 1);
}

TEST_F(ConnectionForTest, Fix709854461) {
    conn->Query("drop table if exists t1");
    conn->Query("drop table if exists t2");
    conn->Query("CREATE TABLE t1(c text)");
    conn->Query("CREATE TABLE t2(x text PRIMARY KEY, y text)");
    conn->Query("INSERT INTO t1(c) VALUES(123)");
    conn->Query("INSERT INTO t2(x) VALUES(123)");
    auto r = conn->Query(
        "SELECT 1 FROM t1, t2 WHERE (SELECT 3 FROM (SELECT x FROM t2 WHERE x=c OR x=(SELECT x FROM (VALUES(0)))) WHERE "
        "x>c OR x=c)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 1);
}

TEST_F(ConnectionForTest, SelectTestInBook) {
    // 表结构
    //  empno 员工编号
    //  ename 员工姓名
    //  job 员工职位
    //  sal 基本工资
    //  mgr 上级经理的员工编号
    //  hiredate 入职日期
    //  comm 奖金
    //  deptno 部门编号
    conn->Query(
        "CREATE TABLE emp( empno INT, ename VARCHAR(50), job VARCHAR(50), sal DECIMAL(10, 2), mgr INT, hiredate "
        "DATE, comm DECIMAL(10, 2), deptno INT, PRIMARY KEY (empno) )");
    conn->Query(
        "INSERT INTO emp VALUES (1,'John Doe','Manager',5000.0,null,'2020-01-01',null,10),(2,'Jane "
        "Smith','Assistant',3000,1,'2020-02-15',500,10),(3,'Mike Johnson','Clerk',2000,2,'2020-03-10',null,20)");

    // 筛选行
    auto r = conn->Query("SELECT * FROM emp WHERE deptno=10");
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 2);

    // 多条件查询
    r = conn->Query("SELECT * from emp WHERE deptno = 10 or comm is not null or sal <= 2000 and deptno=20");
    EXPECT_EQ(r->RowCount(), 3);

    // 调整优先级
    r = conn->Query("SELECT * from emp WHERE (deptno = 10 or comm is not null or sal <= 2000) and deptno=20");
    EXPECT_EQ(r->RowCount(), 1);

    // 创建有意义的列名
    r = conn->Query("SELECT sal as salary , comm as commission from emp");
    EXPECT_EQ(r->GetRetCode(), 0);

    // 在where 中引用列别名
    r = conn->Query("SELECT sal as salary , comm as commission from emp where salary < 5000");
    EXPECT_EQ(r->RowCount(), 2);

    // 查找null值
    r = conn->Query("SELECT * from emp where comm is null");
    EXPECT_EQ(r->RowCount(), 2);

    // 查找匹配项
    r = conn->Query("SELECT * from emp where deptno in (10,20) and ename like 'J%'");
    EXPECT_EQ(r->RowCount(), 2);
}

TEST_F(ConnectionForTest, subquery_talbe_test_aliasing) {
    conn->Query("drop table if exists a");
    conn->Query("create table a (i int)");
    conn->Query("insert into a values (42)");
    auto r = conn->Query("select * from ( select i as j from a group by j) sql where j = 42");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);

    r = conn->Query("select * from ( select i as j from a group by i) sql where j = 42");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
}

TEST_F(ConnectionForTest, subquery_table_test_nested_table_subquerytest_slow) {
    conn->Query("drop table if exists test");
    conn->Query("create table test (i int,j int)");
    conn->Query("insert into test values (3,4),(4,5),(5,6)");
    auto r = conn->Query(
        "SELECT * FROM (SELECT i, j FROM (SELECT j AS i, i AS j FROM (SELECT j AS i, i AS j FROM test) AS a) AS a) AS "
        "a, (SELECT i+1 AS r,j FROM test) AS b, test WHERE a.i=b.r AND test.j=a.i ORDER BY 1");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(0).Field(3).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(0).Field(4).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(0).Field(5).GetCastAs<int32_t>(), 4);

    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<int32_t>(), 6);
    EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(1).Field(3).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(1).Field(4).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(1).Field(5).GetCastAs<int32_t>(), 5);

    // 100 层嵌套
    r = conn->Query(
        "SELECT i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM "
        "(SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 "
        "AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM "
        "(SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 "
        "AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM "
        "(SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 "
        "AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM "
        "(SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 "
        "AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM "
        "(SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 "
        "AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM "
        "(SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 "
        "AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM "
        "(SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 "
        "AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM "
        "(SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 "
        "AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM "
        "(SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 "
        "AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM "
        "(SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 "
        "AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM "
        "(SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 "
        "AS i FROM (SELECT i + 1 AS i FROM test) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS "
        "a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS "
        "a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS "
        "a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS "
        "a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS "
        "a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a) AS a");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 103);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 104);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 105);
}

TEST_F(ConnectionForTest, subquery_table_union) {
    auto r = conn->Query("select * from (select 42) sq1 union all select * from (select 43) sq2");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 43);
}

TEST_F(ConnectionForTest, subquery_table_test_table_subquery) {
    conn->Query("drop table if exists test");
    conn->Query("create table test (i int,j int)");
    conn->Query("insert into test values (3,4),(4,5),(5,6)");
    auto r = conn->Query("SELECT * FROM (SELECT i, j AS d FROM test ORDER BY i) AS b");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(2).Field(1).GetCastAs<int32_t>(), 6);

    // check column names for simple projection and aliases
    r = conn->Query("SELECT b.d FROM (SELECT i * 2 + j AS d FROM test) AS b");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 10);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 13);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 16);
    const auto& schema = r->GetSchema();
    EXPECT_EQ(schema.GetColumnInfos()[0].GetColNameWithoutTableName(), "d");

    // join with subquery
    r = conn->Query(
        "SELECT a.i,a.j,b.r,b.j FROM (SELECT i, j FROM test) AS a INNER JOIN (SELECT i+1 AS r,j FROM test) AS b ON "
        "a.i=b.r ORDER BY 1");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(0).Field(3).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<int32_t>(), 6);
    EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(1).Field(3).GetCastAs<int32_t>(), 5);

    // check that * is in the correct order
    r = conn->Query(
        "SELECT * FROM (SELECT i, j FROM test) AS a, (SELECT i+1 AS r,j FROM test) AS b, test WHERE a.i=b.r AND "
        "test.j=a.i ORDER BY 1");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(0).Field(3).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(0).Field(4).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(0).Field(5).GetCastAs<int32_t>(), 4);

    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<int32_t>(), 6);
    EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(1).Field(3).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(1).Field(4).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(1).Field(5).GetCastAs<int32_t>(), 5);

    // # subquery group cols are visible
    r = conn->Query("select sum(x) from (select i as x from test group by i) sq");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 12);

    // subquery group aliases are visible
    r = conn->Query("select sum(x) from (select i+1 as x from test group by x) sq");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 15);
}

TEST_F(ConnectionForTest, subquery_table_test_unnamed_subquery) {
    auto r = conn->Query("SELECT a FROM (SELECT 42 a)");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);

    r = conn->Query("SELECT * FROM (SELECT 42 a), (SELECT 43 b)");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 43);

    r = conn->Query("SELECT * FROM (VALUES (42, 43))");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 43);

    r = conn->Query("SELECT * FROM (SELECT 42 a), (SELECT 43 b), (SELECT 44 c), (SELECT 45 d)");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 43);
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<int32_t>(), 44);
    EXPECT_EQ(r->RowRef(0).Field(3).GetCastAs<int32_t>(), 45);

    r = conn->Query(
        "SELECT * FROM (FROM (SELECT 42 a), (SELECT 43 b)) JOIN (SELECT 44 c) ON (true) JOIN (SELECT 45 d) ON (true)");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 43);
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<int32_t>(), 44);
    EXPECT_EQ(r->RowRef(0).Field(3).GetCastAs<int32_t>(), 45);
}

TEST_F(ConnectionForTest, int_cast_test) {
    auto r = conn->Query("select -42::tinyint::utinyint");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<uint8_t>(), 214);

    r = conn->Query("SELECT -42::TINYINT::USMALLINT");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<uint16_t>(), 65494);

    r = conn->Query("SELECT -42::TINYINT::UINTEGER");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<uint32_t>(), 4294967254);

    r = conn->Query("SELECT -42::TINYINT::UBIGINT");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<uint64_t>(), 18446744073709551574U);
}

TEST_F(ConnectionForTest, cast_boolean_autocast) {
    auto r = conn->Query("SELECT true=1");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("SELECT true=0");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("SELECT false=0");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("SELECT false=1");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("SELECT 1=true");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("SELECT 0=true");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("SELECT 0=false");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("SELECT 1=false");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    // boolean -> string
    r = conn->Query("SELECT true='1'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("SELECT true='0'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("SELECT false='0'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("SELECT false='1'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("SELECT true='true'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("SELECT true='false'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("SELECT false='false'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("SELECT false='true'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    conn->Query("drop table if exists bool_test");
    conn->Query("create table bool_test (a boolean , b int , c varchar)");
    conn->Query("insert into bool_test values (true,1,'true'),(false,0,'0'),(true,1,'123'),(false,0,'false')");
    r = conn->Query("select * from bool_test where a");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);

    r = conn->Query("select * from bool_test where b");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);

    r = conn->Query("select * from bool_test where c");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<std::string>(), "true");
    EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<std::string>(), "123");
}

TEST_F(ConnectionForTest, ComplexFilter) {
    conn->Query("drop table if exists t");
    conn->Query("create table t(a int , b int)");
    conn->Query("insert into t values (1,1),(2,2),(3,3),(4,4),(5,5)");
    auto r = conn->Query("select * from t , t as t2 where t.a > t2.a and t.a < t2.a and t2.a = 1");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);
}

TEST_F(ConnectionForTest, FixBug710259557) {
    // string to boolean
    conn->Query("CREATE TABLE type_boolean01 (datev boolean)");
    auto r = conn->Query("INSERT INTO type_boolean01 VALUES ('true')");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("INSERT INTO type_boolean01 VALUES ('t')");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("INSERT INTO type_boolean01 VALUES ('y')");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("INSERT INTO type_boolean01 VALUES ('yes')");
    ASSERT_NE(r->GetRetCode(), 0);
    r = conn->Query("INSERT INTO type_boolean01 VALUES ('n')");
    ASSERT_NE(r->GetRetCode(), 0);
}

TEST_F(ConnectionForTest, FixBug710259552) {
    // 优化器bug
    conn->Query("drop table if exists t2");
    conn->Query("CREATE TABLE t2(k INTEGER PRIMARY KEY, v TEXT)");
    conn->Query("INSERT INTO t2 VALUES(5, 'v'), (4, 'iv'), (3, 'iii'), (2, 'ii')");
    auto r = conn->Query("SELECT * FROM t2 AS x1, t2 AS x2 WHERE x1.k=x2.k+1");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 3);
    ASSERT_EQ(r->ColumnCount(), 4);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<std::string>(), "v");
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(0).Field(3).GetCastAs<std::string>(), "iv");
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<std::string>(), "iv");
    EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(1).Field(3).GetCastAs<std::string>(), "iii");
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(2).Field(1).GetCastAs<std::string>(), "iii");
    EXPECT_EQ(r->RowRef(2).Field(2).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(2).Field(3).GetCastAs<std::string>(), "ii");
}

TEST_F(ConnectionForTest, FixBug710259550) {
    // 补充 uint64 -> Trivalent
    conn->Query("CREATE TABLE bigints(i bigint)");
    auto r = conn->Query("INSERT INTO bigints VALUES (-9223372036854775808), (0), (9223372036854775807)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("SELECT i::BOOL FROM bigints order by i");
    ASSERT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select '123.1'::bool");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("select '123'::bool");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);
}

TEST_F(ConnectionForTest, FixBug710259544) {
    conn->Query("drop table if exists dates");
    conn->Query("drop table if exists timestamp");
    conn->Query("CREATE TABLE dates(i DATE)");
    conn->Query("CREATE TABLE timestamp(i TIMESTAMP)");
    conn->Query("INSERT INTO dates VALUES ('1993-08-14')");
    conn->Query("INSERT INTO timestamp VALUES ('1993-08-14 00:00:01')");
    auto r = conn->Query("select count(*) from dates inner join timestamp on (timestamp.i::DATE = dates.i)");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 1);
}

TEST_F(ConnectionForTest, JoinOnKeyWithNull) {
    conn->Query("drop table if exists t1");
    conn->Query("drop table if exists t2");
    conn->Query("CREATE TABLE t1 (a int , b int)");
    conn->Query("CREATE TABLE t2 (a int , b int)");
    conn->Query("INSERT INTO t1 VALUES (1, 1), (2, 2), (3, null)");
    conn->Query("INSERT INTO t2 VALUES (1, 1), (2, 2), (3, null)");
    auto r = conn->Query("SELECT * FROM t1 JOIN t2 ON t1.b = t2.b");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
}

TEST_F(ConnectionForTest, CompareWithStringType) {
    conn->Query("drop table if exists t1");
    conn->Query("CREATE TABLE t1 (a int , b string)");  // string 类型的比较
    conn->Query("INSERT INTO t1 VALUES (1, '1'), (2, '2'), (3, '3')");
    auto r = conn->Query("SELECT * FROM t1 as x , t1 as y WHERE x.b = y.b");
    ASSERT_EQ(r->GetRetCode(), 0);
}

TEST_F(ConnectionForTest, DecimalMathTest) {
    auto r = conn->Query("SELECT CAST('5.0' AS DECIMAL(4,3)) + CAST('5.0' AS DECIMAL(4,2))");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<std::string>(), "10.000");
    EXPECT_EQ(r->RowRef(0).Field(0).scale, 3);
    EXPECT_EQ(r->RowRef(0).Field(0).precision, 6);

    r = conn->Query("SELECT CAST('5.0' AS DECIMAL(4,3)) + CAST('5.0' AS DECIMAL(4,3))");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).scale, 3);
    EXPECT_EQ(r->RowRef(0).Field(0).precision, 5);

    r = conn->Query("SELECT CAST('5.0' AS DECIMAL(5,3)) + CAST('5.0' AS DECIMAL(5,4))");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).scale, 4);
    EXPECT_EQ(r->RowRef(0).Field(0).precision, 7);

    r = conn->Query("SELECT CAST('5.0' AS DECIMAL(5,3)) + CAST('5.0' AS DECIMAL(5,1))");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).scale, 3);
    EXPECT_EQ(r->RowRef(0).Field(0).precision, 8);

    r = conn->Query("SELECT CAST('5.0' AS DECIMAL(5,3)) * CAST('5.0' AS DECIMAL(5,2))");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).scale, 5);
    EXPECT_EQ(r->RowRef(0).Field(0).precision, 10);
}

TEST_F(ConnectionForTest, TestStringOperator) {
    conn->Query("drop table if exists t8");
    conn->Query("CREATE TABLE t8(a INTEGER PRIMARY KEY, b TEXT UNIQUE, c CHAR(100))");
    auto r = conn->Query("INSERT INTO t8(a,b) values (1,'one'),(4,'four')");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("SELECT x.a || '/' || y.a from t8 x , t8 y order by x.a");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<std::string>(), "1/1");
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<std::string>(), "1/4");
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<std::string>(), "4/1");
    EXPECT_EQ(r->RowRef(3).Field(0).GetCastAs<std::string>(), "4/4");
}

TEST_F(ConnectionForTest, TestWithIn) {
    conn->Query("drop table if exists t1");
    conn->Query("CREATE TABLE t1 (a int , b int)");
    conn->Query("INSERT INTO t1 VALUES (1, 1), (2, 2), (3, 3)");
    auto r = conn->Query("SELECT * FROM t1 WHERE a IN (1, 2) and a = 3");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    r = conn->Query("SELECT * FROM t1 WHERE a IN (1, 2) and a = 2");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 2);

    r = conn->Query("SELECT * from t1 where a not in (1, 2)");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 3);

    r = conn->Query("select a , a in (1,null) from t1 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<bool>(), true);
    EXPECT_EQ(r->RowRef(1).Field(1).IsNull(), true);
    EXPECT_EQ(r->RowRef(2).Field(1).IsNull(), true);

    r = conn->Query("select a , a not in (1,null) from t1 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<bool>(), false);
    EXPECT_EQ(r->RowRef(1).Field(1).IsNull(), true);
    EXPECT_EQ(r->RowRef(2).Field(1).IsNull(), true);
}

TEST_F(ConnectionForTest, TestCaseWhen) {
    conn->Query("drop table if exists test_case_when");
    conn->Query("create table test_case_when(id int, name varchar(20), score int)");
    conn->Query(
        "insert into test_case_when values(1, 'xiao a', 90),(2, 'xiao b', 80),"
        "(3, 'xiao c', 60),(4, 'xiao d', 50),(5, 'xiao f', 85);");

    auto r = conn->Query(
        "select case when score>=90 then 'A+' when score>=85 then 'A' "
        "when score>=80 then 'B+' when score>=60 then 'B' else 'C' end as level from test_case_when");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<std::string>(), "A+");
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<std::string>(), "B+");
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<std::string>(), "B");
    EXPECT_EQ(r->RowRef(3).Field(0).GetCastAs<std::string>(), "C");
    EXPECT_EQ(r->RowRef(4).Field(0).GetCastAs<std::string>(), "A");

    r = conn->Query(
        "select case score when 90 then 'A+' when 85 then 'A' "
        "when 80 then 'B+' when 60 then 'B' else 'C' end as level from test_case_when");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 5);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<std::string>(), "A+");
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<std::string>(), "B+");
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<std::string>(), "B");
    EXPECT_EQ(r->RowRef(3).Field(0).GetCastAs<std::string>(), "C");
    EXPECT_EQ(r->RowRef(4).Field(0).GetCastAs<std::string>(), "A");
}

TEST_F(ConnectionForTest, Bug710382881) {
    conn->Query("drop table if exists integers");
    conn->Query("CREATE TABLE integers(i INTEGER)");
    conn->Query("INSERT INTO integers VALUES (1), (2), (3), (null)");

    auto r = conn->Query("SELECT EXISTS(SELECT i FROM integers WHERE i=i1.i) AS g, COUNT(*) FROM integers i1 GROUP BY g ORDER BY g");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<bool>(), true);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<int32_t>(), 3);

    r = conn->Query("SELECT i, EXISTS(SELECT i FROM integers WHERE i IS NULL OR i>i1.i*10) FROM integers i1 ORDER BY i");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 4);
    // 因为 结果含有 null 且有排序，这里不对顺序进行判断了（null的排序规则可能后面会变化，避免后面再修改用例了)
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<bool>(), true);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<bool>(), true);
    EXPECT_EQ(r->RowRef(2).Field(1).GetCastAs<bool>(), true);
    EXPECT_EQ(r->RowRef(3).Field(1).GetCastAs<bool>(), true);
}

TEST_F(ConnectionForTest , OperatorPrority) {
    auto r = conn->Query("select 1 == 5 > 6 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("select 0 == 0 = 0");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    // select 5 == 2 > 1 
    r = conn->Query("select 5 == 2 > 1 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    // select 22 != 45 < 66
    r = conn->Query("select 22 != 45 < 66 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    // select 0 || 1 OR -1 
    r = conn->Query("select 0 || 1 OR -1 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 1);

    r = conn->Query("SELECT 0 = 1 < -1"); // < 优先级高于 =
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    // select 22 = 45 <= 66
    r = conn->Query("select 22 = 45 <= 66 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    // select 22 = 45 > 66 
    r = conn->Query("select 22 = 45 > 66 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    // select 22 = 45 < 66 
    r = conn->Query("select 22 = 45 < 66 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    // select 22 == 45 >= 66 
    r = conn->Query("select 22 == 45 >= 66 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    // select 22 == 45 < 66
    r = conn->Query("select 22 == 45 < 66 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    // select 22 != 45 != 66 
    r = conn->Query("select 22 != 45 != 66 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    // select 22 != 45 <> 66 
    r = conn->Query("select 22 != 45 <> 66 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    // select 22 <> 45 < 66
    r = conn->Query("select 22 <> 45 < 66 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    // select 22 <> 45 >= 66
    r = conn->Query("select 22 <> 45 >= 66 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    // select 22 <> 45 = 66
    r = conn->Query("select 22 <> 45 = 66 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    // select 22 <> 45 != 66 
    r = conn->Query("select 22 <> 45 != 66 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    // select 22 LIKE 45 < 66 
    r = conn->Query("select 22 LIKE 45 < 66 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    // select 22 LIKE 45 LIKE 66 
    r = conn->Query("select 22 LIKE 45 LIKE 66 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    // select 1 and 0 || 1
    r = conn->Query("select 1 and 0 || 1 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 1);

    // select 0 and 0 and 0 
    r = conn->Query("select 0 and 0 and 0 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 0);

    // select 0 or 0 || 0
    r = conn->Query("select 0 or 0 || 0 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 0);

    // select 0 < 2 like 1 , (0 < 2 ) like 1 , 0 < ( 2 like 1) 
    r = conn->Query("select 0 < 2 like 1 , (0 < 2 ) like 1 , 0 < ( 2 like 1) ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<bool>(), true);
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<bool>(), false);

    // select 0 <= 1 || -1 
    r = conn->Query("select 0 <= 1 || -1 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 1);

    // select 1 = 5 like 5 
    r = conn->Query("select 1 = 5 like 5 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    // select 5 = 5 like 1 
    r = conn->Query("select 5 = 5 like 1 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), true);

    // select 0 = 1 like -1 
    r = conn->Query("select 0 = 1 like -1 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);

    // select 1 <> 0 || 1 
    r = conn->Query("select 1 <> 0 || 1 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    // NOTE: 这个用例 sqlite结果为 1 , duckdb 结果为 0 
    // 原因在于 在比较 1 <> '01' 时的规则不一致，但是将数据存入表中时，sqlite的表现又不一样
    // 因为sqlite规则的混乱，这里采用duckdb的结果( duckdb不支持 数字进行|| ，这里只讨论 1 <> '01' 的结果) 
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 0);

    // select 1 like 5 == 5 
    r = conn->Query("select 1 like 5 == 5 ");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);
}

#include <fmt/format.h>

TEST_F(ConnectionForTest, TestMoveConstants) {
    struct MoveConstantsTypeInfo {
        std::string type_name;
        std::string max;
        std::string min;
    };

    std::vector<MoveConstantsTypeInfo> type_infos = {
        {"tinyint", "127", "-128"},
        {"smallint", "32767", "-32768"},
        {"integer", "2147483647", "-2147483648"},
        {"bigint", "9223372036854775807", "-9223372036854775808"},
    };
    for (auto& type_info : type_infos) {
        conn->Query("drop table if exists vals");
        conn->Query(fmt::format("create table vals (v {})", type_info.type_name).c_str());
        conn->Query("insert into vals values (2),(null)");
        auto r = conn->Query(fmt::format("select * from vals where v + ({} - 10)::{} = -100::{}", type_info.max,
                                         type_info.type_name, type_info.type_name)
                                 .c_str());
        ASSERT_EQ(r->GetRetCode(), 0);
        EXPECT_EQ(r->RowCount(), 0);

        r = conn->Query(fmt::format("SELECT v+({}-10)::{}=-100::{} FROM vals", type_info.max, type_info.type_name,
                                    type_info.type_name)
                            .c_str());
        ASSERT_EQ(r->GetRetCode(), 0);
        ASSERT_EQ(r->RowCount(), 2);
        EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<bool>(), false);
        EXPECT_EQ(r->RowRef(1).Field(0).IsNull(), true);

        r = conn->Query(fmt::format("SELECT * FROM vals WHERE v-{}::{}={}::{}", type_info.max, type_info.type_name,
                                    type_info.max, type_info.type_name)
                            .c_str());
        ASSERT_EQ(r->GetRetCode(), 0);
        ASSERT_EQ(r->RowCount(), 0);

        r = conn->Query(fmt::format("select * from vals where ({} + 100)::{} - v = {}::{}", type_info.min,
                                    type_info.type_name, type_info.max, type_info.type_name)
                            .c_str());
        ASSERT_EQ(r->GetRetCode(), 0);
        ASSERT_EQ(r->RowCount(), 0);

        r = conn->Query(
            fmt::format("SELECT * FROM vals WHERE v*0::{}=1::{}", type_info.type_name, type_info.type_name).c_str());
        ASSERT_EQ(r->GetRetCode(), 0);
        ASSERT_EQ(r->RowCount(), 0);

        r = conn->Query(
            fmt::format("SELECT * FROM vals WHERE v*0::{}=0::{}", type_info.type_name, type_info.type_name).c_str());
        ASSERT_EQ(r->GetRetCode(), 0);
        ASSERT_EQ(r->RowCount(), 1);
        EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 2);

        r = conn->Query(fmt::format("SELECT * FROM vals WHERE v*(-1)::{}>({})::{}", type_info.type_name, type_info.min,
                                    type_info.type_name)
                            .c_str());
        ASSERT_EQ(r->GetRetCode(), 0);
        ASSERT_EQ(r->RowCount(), 1);
        EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 2);

        r = conn->Query(fmt::format("SELECT * FROM vals WHERE v*(-2)::{}>({})::{}", type_info.type_name, type_info.min,
                                    type_info.type_name)
                            .c_str());
        ASSERT_EQ(r->GetRetCode(), 0);
        ASSERT_EQ(r->RowCount(), 1);
        EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 2);

        r = conn->Query(fmt::format("select * from vals where v - 1::{} < {}::{}", type_info.type_name, type_info.max,
                                    type_info.type_name)
                            .c_str());
        ASSERT_EQ(r->GetRetCode(), 0);
        ASSERT_EQ(r->RowCount(), 1);
        EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 2);
    }
}
