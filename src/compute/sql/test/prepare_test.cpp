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
 * prepare_test.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/test/prepare_test.cpp
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

class ConnectionForPrepare : public ::testing::Test {
   protected:
    ConnectionForPrepare() {}
    ~ConnectionForPrepare() {}
    static void SetUpTestSuite() {
        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance->Init();
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();
    }

    static void TearDownTestSuite() {}

    void SetUp() override {}

    static std::shared_ptr<IntarkDB> db_instance;
    static std::unique_ptr<Connection> conn;
};

std::shared_ptr<IntarkDB> ConnectionForPrepare::db_instance = nullptr;
std::unique_ptr<Connection> ConnectionForPrepare::conn = nullptr;

TEST_F(ConnectionForPrepare, PrepareWithInsert) {
    conn->Query("drop table if exists test_prepare");
    conn->Query("create table test_prepare (id int, name varchar(20))");
    auto stmt = conn->Prepare("insert into test_prepare values (?, ?)");
    ASSERT_EQ(stmt->HasError(), false);
    auto r = stmt->Execute({ValueFactory::ValueInt(1), ValueFactory::ValueVarchar("hello")});
    EXPECT_EQ(r->GetRetCode(), 0);
    r = stmt->Execute({ValueFactory::ValueInt(2), ValueFactory::ValueVarchar("world")});
    EXPECT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select * from test_prepare");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_STREQ(r->Row(0).Field(1).GetCastAs<std::string>().c_str(), "hello");
    EXPECT_STREQ(r->Row(1).Field(1).GetCastAs<std::string>().c_str(), "world");

    conn->Query("drop table if exists AAAA_TEST_TABLE");
    conn->Query(
        "CREATE TABLE AAAA_TEST_TABLE(LOGID BIGINT, ACCTID INTEGER,CHGAMT VARCHAR(20), BAK1 VARCHAR(20), CHGTIME "
        "TIMESTAMP, BIN_TMP BLOB)");
    conn->Query("CREATE UNIQUE INDEX IDX_AAAA_TEST_TABLE_1 on AAAA_TEST_TABLE (LOGID)");

    stmt = conn->Prepare("INSERT INTO AAAA_TEST_TABLE (LOGID,ACCTID,CHGAMT,BAK1,CHGTIME) values (?, ?, ?, ?, ?)");
    ASSERT_EQ(stmt->HasError(), false);
    r = stmt->Execute({ValueFactory::ValueInt(-1), ValueFactory::ValueInt(11), ValueFactory::ValueVarchar("AAA"),
                       ValueFactory::ValueVarchar("bak1"), ValueFactory::ValueVarchar("2023-08-01")});
    EXPECT_EQ(r->GetRetCode(), 0);
    r = stmt->Execute({ValueFactory::ValueInt(-2), ValueFactory::ValueInt(22), ValueFactory::ValueVarchar("BBB"),
                       ValueFactory::ValueVarchar("bak1"), ValueFactory::ValueVarchar("2023-8-2")});
    EXPECT_EQ(r->GetRetCode(), 0);
    r = stmt->Execute({ValueFactory::ValueInt(-2), ValueFactory::ValueInt(33), ValueFactory::ValueVarchar("CCC"),
                       ValueFactory::ValueVarchar("bak1"), ValueFactory::ValueVarchar("2023-8-3")});
    EXPECT_NE(r->GetRetCode(), 0);  // 冲突，应该写入失败
    r = conn->Query("select * from AAAA_TEST_TABLE");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->ColumnCount(), 6);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int64_t>(), -1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int64_t>(), -2);

    EXPECT_EQ(r->Row(0).Field(5).IsNull(), true);
    EXPECT_EQ(r->Row(1).Field(5).IsNull(), true);
}

TEST_F(ConnectionForPrepare, PrepareWithSelect) {
    auto stmt = conn->Prepare("select * from AAAA_TEST_TABLE where LOGID=? and BAK1=? and CHGTIME=?");
    ASSERT_EQ(stmt->HasError(), false);
    auto r = stmt->Execute(
        {ValueFactory::ValueInt(-2), ValueFactory::ValueVarchar("bak1"), ValueFactory::ValueVarchar("2023-08-02")});
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);

    // prepare with avg function
    conn->Query("drop table if exists score");
    conn->Query("CREATE TABLE score (id INT,name VARCHAR(50),subject VARCHAR(50),score INT)");
    conn->Query("INSERT INTO score VALUES (1, '张三', '数学', 81)");
    conn->Query("INSERT INTO score VALUES (2, '李四', '数学', 75)");
    conn->Query("INSERT INTO score VALUES (3, '王五', '语文', 90)");
    conn->Query("INSERT INTO score VALUES (4, '赵六', '语文', 65)");
    conn->Query("INSERT INTO score VALUES (5, '小明', '数学', 90)");
    conn->Query("INSERT INTO score VALUES (6, '小红', '物理', 85)");
    conn->Query("INSERT INTO score VALUES (7, '小刚', '物理', 78)");

    stmt = conn->Prepare(
        "SELECT subject, COUNT(name) AS student_count FROM score GROUP BY subject HAVING student_count >= ?");
    ASSERT_EQ(stmt->HasError(), false);
    r = stmt->Execute({ValueFactory::ValueInt(2)});
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);
    r = stmt->Execute({ValueFactory::ValueInt(3)});
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 3);

    // prepare with limit
    stmt = conn->Prepare("select * from score limit ? offset ?");
    ASSERT_EQ(stmt->HasError(), false);
    r = stmt->Execute({ValueFactory::ValueInt(2), ValueFactory::ValueInt(2)});
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 4);

    r = stmt->Execute({ValueFactory::ValueInt(3), ValueFactory::ValueInt(4)});
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 6);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 7);

    // prepare with sort , 不允许使用参数
    stmt = conn->Prepare("select * from score order by ? desc");
    ASSERT_EQ(stmt->HasError(), true);
}

TEST_F(ConnectionForPrepare, PrepareWithLike) {
    conn->Query("CREATE TABLE products (id INT,name VARCHAR(100),description VARCHAR(255))");
    conn->Query(
        "INSERT INTO products (id, name, description) VALUES \
      (1, 'Apple iPhone 12', 'The latest iPhone model'), \
    (2, 'Samsung Galaxy S21', 'Powerful Android phone'), \
    (3, 'Google Pixel 5', 'High-quality camera phone'), \
    (4, 'Apple MacBook Pro', 'Premium laptop with Retina display'), \
    (5, 'Dell XPS 13', 'Thin and lightweight laptop')");
    auto stmt = conn->Prepare("SELECT * FROM products WHERE name LIKE ?");
    ASSERT_EQ(stmt->HasError(), false);
    auto r = stmt->Execute({ValueFactory::ValueVarchar("%Apple%")});
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "Apple iPhone 12");
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<std::string>(), "Apple MacBook Pro");

    r = stmt->Execute({ValueFactory::ValueVarchar("%Google%")});
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "Google Pixel 5");

    stmt = conn->Prepare("SELECT * FROM products WHERE name LIKE ? escape ?");
    ASSERT_EQ(stmt->HasError(), false);
    r = stmt->Execute({ValueFactory::ValueVarchar("%\\Apple%"), ValueFactory::ValueVarchar("\\")});
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "Apple iPhone 12");
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<std::string>(), "Apple MacBook Pro");

    r = stmt->Execute({ValueFactory::ValueVarchar("%\\Google%"), ValueFactory::ValueVarchar("\\")});
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
}

TEST_F(ConnectionForPrepare, PrepareWithSelectUsingIndex) {
    conn->Query("drop table if exists employees");
    conn->Query(
        "CREATE TABLE employees  (employee_id INT PRIMARY KEY,  first_name VARCHAR(40),  last_name VARCHAR(40),  "
        "hire_date DATE)");
    conn->Query(
        "INSERT INTO employees (employee_id, first_name, last_name, hire_date)  VALUES (1, 'John', 'Doe', "
        "'2022-01-01'),  (2, 'Jane', 'Smith', '2023-02-05'),  (3, 'Bob', 'Johnson', '2021-12-31')");
    // create prepare
    auto stmt = conn->Prepare("select * from employees where employee_id=?");
    ASSERT_EQ(stmt->HasError(), false);
    auto r = stmt->Execute({ValueFactory::ValueInt(1)});
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "John");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "Doe");
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<std::string>(), "2022-01-01");

    r = stmt->Execute({ValueFactory::ValueInt(2)});
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "Jane");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "Smith");
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<std::string>(), "2023-02-05");

    stmt = conn->Prepare("select * from employees where employee_id > ?");
    ASSERT_EQ(stmt->HasError(), false);
    r = stmt->Execute({ValueFactory::ValueInt(1)});
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "Jane");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "Smith");
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<std::string>(), "2023-02-05");
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<std::string>(), "Bob");
    EXPECT_EQ(r->Row(1).Field(2).GetCastAs<std::string>(), "Johnson");
    EXPECT_EQ(r->Row(1).Field(3).GetCastAs<std::string>(), "2021-12-31");

    // test 参数 超出字段范围，应无法使用索引，退化为全表扫描
    // TODO: 因为 compare func在 prepare 时就固定下来，不可传类型不匹配的参数,观察看看有没有变类型的需求
    // r = stmt->Execute({ValueFactory::ValueBigInt(1000000000000)});
    // std::cout << r->GetRetMsg() << std::endl;
    // ASSERT_EQ(r->GetRetCode(), 0);
    // ASSERT_EQ(r->RowCount(), 0);

    // r = stmt->Execute({ValueFactory::ValueBigInt(-1000000000000)});
    // ASSERT_EQ(r->GetRetCode(), 0);
    // ASSERT_EQ(r->RowCount(), 3);
}

TEST_F(ConnectionForPrepare, PrepareWithCast) {
    auto stmt = conn->Prepare("select cast(? as varchar(20))");
    if (stmt->HasError()) {
        std::cout << stmt->ErrorMsg() << std::endl;
    }
    ASSERT_EQ(stmt->HasError(), false);
    auto r = stmt->Execute({ValueFactory::ValueInt(1)});
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "1");
}

TEST_F(ConnectionForPrepare, PrepareWithCreateAs) {
    auto stmt = conn->Prepare("create table praram_test as select ?");
    ASSERT_EQ(stmt->HasError(), true);
}

TEST_F(ConnectionForPrepare, PrepareWithMathOperator) {
    auto stmt = conn->Prepare("select ? - ?");
    ASSERT_EQ(stmt->HasError(), false);
    auto r = stmt->Execute({ValueFactory::ValueInt(1), ValueFactory::ValueInt(2)});
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), -1);

    stmt = conn->Prepare("select ? + ?");
    ASSERT_EQ(stmt->HasError(), false);
    r = stmt->Execute({ValueFactory::ValueInt(1), ValueFactory::ValueInt(2)});
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 3);
}

TEST_F(ConnectionForPrepare, PrepareUpdateWithIndex) {
    conn->Query("create table update_prepare_test (a int primary key, b int)");
    conn->Query("insert into update_prepare_test values (1,2)");
    auto stmt = conn->Prepare("update update_prepare_test set b = 3 where a = ?");
    ASSERT_EQ(stmt->HasError(), false);
    ASSERT_EQ(stmt->ParamCount(), 1);
    // FIXME: CHECK IT USE INDEX OR NOT 
    auto r = stmt->Execute({ValueFactory::ValueInt(1)});
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select * from update_prepare_test");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 3);
}

TEST_F(ConnectionForPrepare , PrepareCheckParamCount) {
    auto stmt = conn->Prepare(R"(select ID ,NAME from "SYS"."SYS_COLUMNS" 
        where "TABLE#"=( select id from "SYS"."SYS_TABLES" WHERE name=?))");
    ASSERT_EQ(stmt->HasError(), false);
    ASSERT_EQ(stmt->ParamCount(), 1); 
}

TEST_F(ConnectionForPrepare , PrepareReExecuteWithSubquery) {
    conn->Query("drop table if exists users");
    conn->Query("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER)");
    conn->Query("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 20), (2, 'Bob', 25), (3, 'Charlie', 30)");
    auto stmt = conn->Prepare("SELECT * FROM users WHERE id = (SELECT id FROM users WHERE name = ?)"); 
    ASSERT_EQ(stmt->HasError(), false);
    auto r = stmt->Execute({ValueFactory::ValueVarchar("Alice")});
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "Alice");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 20);
    
    r = stmt->Execute({ValueFactory::ValueVarchar("Bob")});
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "Bob");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 25);
}

TEST_F(ConnectionForPrepare , PrepareWithAgg) {
    conn->Query("drop table if exists test_agg");
    conn->Query("create table test_agg (id int, name varchar(20), score int)");
    conn->Query("insert into test_agg values (1, 'Alice', 80), (2, 'Bob', 90), (3, 'Charlie', 85)");
    auto stmt = conn->Prepare("select count() from test_agg where score > ?");
    ASSERT_EQ(stmt->HasError(), false);
    auto r = stmt->Execute({ValueFactory::ValueInt(85)});
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
}

TEST_F(ConnectionForPrepare , PrepareWithBinaryOp) {
    auto stmt = conn->Prepare("select 1 + ?");
    ASSERT_EQ(stmt->HasError(), false);
    auto r = stmt->Execute({ValueFactory::ValueInt(1)});
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);

    stmt = conn->Prepare("select 1 > ?");
    ASSERT_EQ(stmt->HasError(), false);
    r = stmt->Execute({ValueFactory::ValueInt(2)});
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);

    stmt = conn->Prepare("select 1::Decimal(10,2) + ?");
    ASSERT_EQ(stmt->HasError(), false);
    r = stmt->Execute({ValueFactory::ValueInt(1)});
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<std::string>(), "2.00");

    conn->Query("drop table if exists bmsql_warehouse");
    conn->Query("create table bmsql_warehouse ( w_id        integer   not null, w_ytd       decimal(12,2), w_tax       decimal(4,4), w_name      varchar(10), w_street_1  varchar(20), w_street_2  varchar(20), w_city      varchar(20), w_state     char(2), w_zip       char(9), primary key (w_id) )");
    conn->Query("insert into bmsql_warehouse values (1, 1000.00, 1, 'name', 'street1', 'street2', 'city', 'st', 'zip')");
    stmt = conn->Prepare("UPDATE bmsql_warehouse SET w_ytd = w_ytd + ?, w_tax = ? WHERE w_id = ?");
    ASSERT_EQ(stmt->HasError(), false);

    r = stmt->Execute({ValueFactory::ValueDouble(100.22), ValueFactory::ValueInt(1), ValueFactory::ValueInt(1)});
    ASSERT_EQ(r->GetRetCode(), 0);

}
