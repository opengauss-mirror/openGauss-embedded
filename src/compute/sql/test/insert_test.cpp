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
 * insert_test.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/test/insert_test.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <gtest/gtest.h>

#include <iostream>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "main/connection.h"
#include "main/database.h"

class ConnectionForInsertTest : public ::testing::Test {
   protected:
    ConnectionForInsertTest() {}
    ~ConnectionForInsertTest() {}
    static void SetUpTestSuite() {
        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance->Init();
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();

        // sleep for a while to wait for db to start
        sleep(1);

        // data has prepare in select_test
        conn->Query("create table insert_test_t1 (sid integer , name varchar(20), age smallint , gpa decimal(5,2))");
        conn->Query("create table insert_test_t2 (sid integer , name varchar(20), age smallint , gpa decimal(5,2))");
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

std::shared_ptr<IntarkDB> ConnectionForInsertTest::db_instance = nullptr;
std::unique_ptr<Connection> ConnectionForInsertTest::conn = nullptr;

TEST_F(ConnectionForInsertTest, TestInsert) {
    // normal insert
    // EXPECT_NO_THROW(conn->Query("insert into insert_test_t1 values (1, 'a', 1, 1.1)"));
    auto rb1 = conn->Query("insert into insert_test_t1 values (1, 'a', 1, 1.1)");
    auto ret = rb1->GetRetCode();
    EXPECT_EQ(ret, 0);

    // specific column
    // EXPECT_NO_THROW(conn->Query("insert into insert_test_t1 (sid, name) values (2, 'b')"));
    auto rb2 = conn->Query("insert into insert_test_t1 (sid, name) values (2, 'b')");
    auto ret2 = rb2->GetRetCode();
    EXPECT_EQ(ret2, 0);

    // speicifi all column
    // EXPECT_NO_THROW(conn->Query("insert into insert_test_t1 (sid, name, age, gpa) values (3, 'c', 3, 3.3)"));
    auto rb3 = conn->Query("insert into insert_test_t1 (sid, name, age, gpa) values (3, 'c', 3, 3.3)");
    auto ret3 = rb3->GetRetCode();
    EXPECT_EQ(ret3, 0);

    // insert not exist table
    // EXPECT_THROW(conn->Query("insert into insert_test_t3 values (1, 'a', 1, 1.1)"), std::invalid_argument);
    auto rb4 = conn->Query("insert into insert_test_t3 values (1, 'a', 1, 1.1)");
    auto ret4 = rb4->GetRetCode();
    EXPECT_NE(ret4, 0);

    // insert not exist column
    // EXPECT_THROW(conn->Query("insert into insert_test_t1 (sid, name, age, gpa, not_exist) values (3, 'c',
    // 3, 3.3, 3.3)"), std::runtime_error);
    auto rb5 = conn->Query("insert into insert_test_t1 (sid, name, age, gpa, not_exist) values (3, 'c', 3, 3.3, 3.3)");
    auto ret5 = rb5->GetRetCode();
    EXPECT_NE(ret5, 0);

    // test insert into select
    // EXPECT_NO_THROW(conn->Query("insert into insert_test_t2 select * from insert_test_t1"));
    auto rb6 = conn->Query("insert into insert_test_t2 select * from insert_test_t1");
    auto ret6 = rb6->GetRetCode();
    EXPECT_EQ(ret6, 0);

    // test insert values has non constant expression
    // EXPECT_NO_THROW(conn->Query("insert into insert_test_t1 values (1+1,'b', 1- 1 , 3 / 2)"));
    auto rb7 = conn->Query("insert into insert_test_t1 values (1+1,'b', 1- 1 , 3 / 2)");
    auto ret7 = rb7->GetRetCode();
    EXPECT_EQ(ret7, 0);

    conn->Query("drop table if exists integers");
    conn->Query("create table integers (a int)");
    auto r = conn->Query("insert into integers select 42");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("insert into integers select cast(null as varchar)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select * from integers");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 42);
    EXPECT_EQ(r->RowRef(1).Field(0).IsNull(), true);

    conn->Query("drop table if exists integers");
    conn->Query("create table integers (i int)");
    conn->Query("INSERT INTO integers VALUES (1), (2), (3), (4)");
    r = conn->Query("insert into integers select * from integers");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("SELECT COUNT(*) FROM integers WHERE i=1");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 2);

    conn->Query("drop table if exists colb_test5");
    conn->Query("create table colb_test5 (id int , clob text)");
    conn->Query("insert into colb_test5 values (1, 'hello')");
    r = conn->Query("insert into colb_test5(clob) select clob from colb_test5");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select * from colb_test5");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<std::string>(), "hello");
    EXPECT_EQ(r->RowRef(1).Field(0).IsNull(), true);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<std::string>(), "hello");

    r = conn->Query("insert into insert_test_t1 (sid, sid) values (3, 3)");
    ASSERT_NE(r->GetRetCode(), 0);
}

TEST_F(ConnectionForInsertTest, TestInsertTypeTrans) {
    conn->Query("drop table if exists insert_test");
    conn->Query("create table insert_test(a int )");
    auto r = conn->Query("insert into insert_test (a) values (1.5)");
    EXPECT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select * from insert_test");
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);
}

TEST_F(ConnectionForInsertTest, TestWithDifferentType) {
    conn->Query("create table string_test ( a string )");
    auto r = conn->Query("insert into string_test values ('hello'),('world')");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select * from string_test");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
}

TEST_F(ConnectionForInsertTest, TestInsertNotSupported) {
    conn->Query("drop table if exists insert_test");
    conn->Query("create table insert_test(a int )");
    auto r = conn->Query("insert into insert_test (a) values (1) returning a");
    ASSERT_NE(r->GetRetCode(), 0);
}

TEST_F(ConnectionForInsertTest, TestValuesWithSubquery) {
    conn->Query("CREATE TABLE insert_with_subquery (a int,b int,c int)");
    conn->Query("INSERT INTO insert_with_subquery VALUES(1+2+3,4,5)");
    auto r = conn->Query("INSERT INTO insert_with_subquery VALUES((SELECT b FROM insert_with_subquery WHERE a=0),6,7)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("SELECT * FROM insert_with_subquery");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 6);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->Row(1).Field(0).IsNull(), true);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<int32_t>(), 6);
    EXPECT_EQ(r->Row(1).Field(2).GetCastAs<int32_t>(), 7);
}

TEST_F(ConnectionForInsertTest, TestDefaultValue) {
    conn->Query("create table defalut_value_tbl( a int , b varchar default '' , c varchar)");
    conn->Query("insert into defalut_value_tbl(a) values (1)");
    conn->Query("insert into defalut_value_tbl(a,b,c) values (2,null,'')");
    auto r = conn->Query("select * from defalut_value_tbl where b = ''");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->RowRef(0).Field(2).IsNull(), true);

    r = conn->Query("select * from defalut_value_tbl where b is null");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(0).Field(1).IsNull(), true);
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<std::string>(), "");
}

TEST_F(ConnectionForInsertTest, TestInsertWithRecordBatch) {
    conn->SetNeedResultSetEx(true);
    conn->Query("drop table if exists insert_test");
    auto r = conn->Query("create table insert_test(a int , b int autoincrement , c int default 10)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("insert into insert_test (a) values (1)");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    ASSERT_EQ(r->ColumnCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<int32_t>(), 10);

    r = conn->Query("insert into insert_test (a,b) values (2,2)");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    ASSERT_EQ(r->ColumnCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<int32_t>(), 10);

    r = conn->Query("insert into insert_test (c,a) values (3,11),(4,11),(5,11)");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 3);
    ASSERT_EQ(r->ColumnCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 3);  // 自增列都放在第一列的位置
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<int32_t>(), 11);
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(1).Field(1).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->RowRef(1).Field(2).GetCastAs<int32_t>(), 11);
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(2).Field(1).GetCastAs<int32_t>(), 5);
    EXPECT_EQ(r->RowRef(2).Field(2).GetCastAs<int32_t>(), 11);

    r = conn->Query("insert into insert_test (a,b,c) values (6,7,8)");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    ASSERT_EQ(r->ColumnCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 6);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 7);
    EXPECT_EQ(r->RowRef(0).Field(2).GetCastAs<int32_t>(), 8);

    r = conn->Query("insert into insert_test (b,c) values (9,2)");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 1);
    ASSERT_EQ(r->ColumnCount(), 2);  // 没插入不会返回
    // EXPECT_EQ(r->RowRef(0).Field(0).IsNull(), true);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<int32_t>(), 9);
    EXPECT_EQ(r->RowRef(0).Field(1).GetCastAs<int32_t>(), 2);
}

TEST_F(ConnectionForInsertTest, TestInsertBlob) {
    auto r = conn->Query("CREATE TABLE blobs (b BLOB)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("INSERT INTO blobs VALUES ('aaaaaaaaaa')");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query(
        "INSERT INTO blobs SELECT b||b||b||b||b||b||b||b||b||b FROM blobs WHERE b=(SELECT MAX(b) FROM blobs)");
    ASSERT_EQ(r->GetRetCode(), 0);
}

TEST_F(ConnectionForInsertTest, TestMulInsertStmt) {
    auto r = conn->Query(
        "CREATE TABLE mul_insert (a int, b int);insert into mul_insert values (1,2);insert into mul_insert values "
        "(3,4);");
    std::cout << r->GetRetMsg() << std::endl;
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("SELECT * FROM mul_insert");
    ASSERT_EQ(r->GetRetCode(), 0);
    ASSERT_EQ(r->RowCount(), 2);
}
