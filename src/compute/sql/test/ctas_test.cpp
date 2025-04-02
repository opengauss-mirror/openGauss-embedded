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
 * ctas_test.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/test/ctas_test.cpp
 *
 * -------------------------------------------------------------------------
 */
// test for create table as select
#include <gtest/gtest.h>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "common/default_value.h"
#include "main/connection.h"
#include "main/database.h"

class CtasTest : public ::testing::Test {
   protected:
    CtasTest() {}
    ~CtasTest() {}
    static void SetUpTestSuite() {
        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance->Init();
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();
    }

    // Per-test-suite tear-down.
    // Called after the last test in this test suite.
    // Can be omitted if not needed.
    static void TearDownTestSuite() { conn.reset(); }

    void SetUp() override {}

    // void TearDown() override {}

    static std::shared_ptr<IntarkDB> db_instance;
    static std::unique_ptr<Connection> conn;
};

std::shared_ptr<IntarkDB> CtasTest::db_instance = nullptr;
std::unique_ptr<Connection> CtasTest::conn = nullptr;

TEST_F(CtasTest, CreateTableAsSelectNoData) {
    std::string select_table("select_table1");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;
    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // create table no data
    std::string table_as_select1("table_as_select_no_data");
    std::string create1(
        fmt::format("create table {} AS SELECT id, name, age FROM {};", table_as_select1, select_table));
    std::cout << create1 << std::endl;
    auto result1 = conn->Query(create1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(table_as_select1);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 3);
    EXPECT_EQ(table_info1->GetIndexCount(), 0);
    auto name_col = table_info1->GetColumnByName("name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    EXPECT_EQ(name_col->nullable, GS_TRUE);  // ctas 不保持原来列的 nullable 属性
    auto age_col = table_info1->GetColumnByName("age");
    EXPECT_FALSE(age_col == NULL);
    EXPECT_EQ(age_col->col_type, GStorDataType::GS_TYPE_INTEGER);
    // 不保持原来列的 default 属性
}

TEST_F(CtasTest, CreateTableAsSelectUnderlineStart) {
    std::string select_table("select_table_underline");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);

    // create table no data
    std::string table_as_select1("_table_as_select_underline");
    std::string create1(
        fmt::format("create table {} AS SELECT id, name, age FROM {};", table_as_select1, select_table));
    std::cout << create1 << std::endl;
    auto result1 = conn->Query(create1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(table_as_select1);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 3);
    EXPECT_EQ(table_info1->GetIndexCount(), 0);
    auto name_col = table_info1->GetColumnByName("name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    auto age_col = table_info1->GetColumnByName("age");
    EXPECT_FALSE(age_col == NULL);
    EXPECT_EQ(age_col->col_type, GStorDataType::GS_TYPE_INTEGER);
}

TEST_F(CtasTest, CreateTableAsSelectDigitStart) {
    std::string select_table("select_table_digit");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // create table no data
    std::string table_as_select1("10table_as_select_digit");
    std::string create1(
        fmt::format("create table {} AS SELECT id, name, age FROM {};", table_as_select1, select_table));
    std::cout << create1 << std::endl;
    auto result1 = conn->Query(create1.c_str());
    EXPECT_FALSE(result1->GetRetCode() == GS_SUCCESS);
}

TEST_F(CtasTest, CreateTableAsSelectSpecialCharacterStart) {
    std::string select_table("select_table_special");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // create table no data
    std::string table_as_select1("&table_as_select_special");
    std::string create1(
        fmt::format("create table {} AS SELECT id, name, age FROM {};", table_as_select1, select_table));
    std::cout << create1 << std::endl;
    auto result1 = conn->Query(create1.c_str());
    EXPECT_FALSE(result1->GetRetCode() == GS_SUCCESS);
}

TEST_F(CtasTest, CreateTableAsSelectSpecialCharacterMid) {
    std::string select_table("select_table_specialmid");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // create table no data
    std::string table_as_select1("table$_as_select_specialmid");
    std::string create1(
        fmt::format("create table {} AS SELECT id, name, age FROM {};", table_as_select1, select_table));
    std::cout << create1 << std::endl;
    auto result1 = conn->Query(create1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(table_as_select1);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 3);
    EXPECT_EQ(table_info1->GetIndexCount(), 0);
    auto name_col = table_info1->GetColumnByName("name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    auto age_col = table_info1->GetColumnByName("age");
    EXPECT_FALSE(age_col == NULL);
    EXPECT_EQ(age_col->col_type, GStorDataType::GS_TYPE_INTEGER);
}

TEST_F(CtasTest, CreateTableAsSelectWithData) {
    std::string select_table("select_table2");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // insert data
    std::string insert_data1(
        fmt::format("insert into {} values(1, 99, 23, 'ruo'), (2, 98, 25, 'teng');", select_table));
    auto result1 = conn->Query(insert_data1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    // test
    auto r = conn->Query(fmt::format("select * from {}", select_table).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 2);
    // create table with data no condition
    std::string table_as_select2("table_as_select_data");
    std::string create2(
        fmt::format("create table {} AS SELECT id, name, age FROM {};", table_as_select2, select_table));
    auto result2 = conn->Query(create2.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(table_as_select2);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 3);
    EXPECT_EQ(table_info1->GetIndexCount(), 0);
    auto name_col = table_info1->GetColumnByName("name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    auto age_col = table_info1->GetColumnByName("age");
    EXPECT_FALSE(age_col == NULL);
    EXPECT_EQ(age_col->col_type, GStorDataType::GS_TYPE_INTEGER);
    // test
    auto r1 = conn->Query(fmt::format("select * from {}", table_as_select2).c_str());
    EXPECT_TRUE(r1->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r1->RowCount(), 2);
}

TEST_F(CtasTest, CreateTableAsSelectWithDataAndCond) {
    std::string select_table("select_table3");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // insert data
    std::string insert_data1(
        fmt::format("insert into {} values(1, 99, 23, 'ruo'), (2, 98, 25, 'teng');", select_table));
    auto result1 = conn->Query(insert_data1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    // test
    auto r = conn->Query(fmt::format("select * from {}", select_table).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 2);
    // create table with data and condition
    std::string table_as_select3("table_as_select3_data_cond");
    std::string create2(
        fmt::format("create table {} AS SELECT id, name, age FROM {} WHERE id = 1;", table_as_select3, select_table));
    std::cout << create2 << std::endl;
    auto result2 = conn->Query(create2.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(table_as_select3);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 3);
    EXPECT_EQ(table_info1->GetIndexCount(), 0);
    auto name_col = table_info1->GetColumnByName("name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    // EXPECT_EQ(name_col->nullable, GS_FALSE);
    auto age_col = table_info1->GetColumnByName("age");
    EXPECT_FALSE(age_col == NULL);
    EXPECT_EQ(age_col->col_type, GStorDataType::GS_TYPE_INTEGER);
    // test
    auto r1 = conn->Query(fmt::format("select * from {}", table_as_select3).c_str());
    EXPECT_TRUE(r1->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r1->RowCount(), 1);
}

TEST_F(CtasTest, CreateTableAsSelectWhereNotEqual) {
    std::string select_table("select_table101");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // insert data
    std::string insert_data1(
        fmt::format("insert into {} values(1, 99, 23, 'ruo'), (2, 98, 25, 'teng'), (3, 53, 31, 'ilike'), (4, 32, 22, "
                    "'like'), (5, 55, 26, 'notlike'), (6, 78, 30, 'gob'), (7, 99, 21, 'gad'), (8, 88, 28, 'gas'), (9,  "
                    "79, 29, 'handle'), (10, 100, 26, 'kingseq');",
                    select_table));
    std::cout << insert_data1 << std::endl;
    auto result1 = conn->Query(insert_data1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    // test
    auto r = conn->Query(fmt::format("select * from {}", select_table).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 10);
    // create table with data and condition
    std::string table_as_select3("table_as_select_table101");
    std::string create2(fmt::format("create table {} AS SELECT id, name, age FROM {} WHERE age != 28;",
                                    table_as_select3, select_table));
    std::cout << create2 << std::endl;
    auto result2 = conn->Query(create2.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(table_as_select3);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 3);
    EXPECT_EQ(table_info1->GetIndexCount(), 0);
    auto name_col = table_info1->GetColumnByName("name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    // EXPECT_EQ(name_col->nullable, GS_FALSE);
    auto age_col = table_info1->GetColumnByName("age");
    EXPECT_FALSE(age_col == NULL);
    EXPECT_EQ(age_col->col_type, GStorDataType::GS_TYPE_INTEGER);
    // test
    auto r1 = conn->Query(fmt::format("select * from {}", table_as_select3).c_str());
    EXPECT_TRUE(r1->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r1->RowCount(), 9);
}

TEST_F(CtasTest, CreateTableAsSelectWhereLargerThan) {
    std::string select_table("select_table102");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // insert data
    std::string insert_data1(
        fmt::format("insert into {} values(1, 99, 23, 'ruo'), (2, 98, 25, 'teng'), (3, 53, 31, 'ilike'), (4, 32, 22, "
                    "'like'), (5, 55, 26, 'notlike'), (6, 78, 30, 'gob'), (7, 99, 21, 'gad'), (8, 88, 28, 'gas'), (9,  "
                    "79, 29, 'handle'), (10, 100, 26, 'kingseq');",
                    select_table));
    std::cout << insert_data1 << std::endl;
    auto result1 = conn->Query(insert_data1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    // test
    auto r = conn->Query(fmt::format("select * from {}", select_table).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 10);
    // create table with data and condition
    std::string table_as_select3("table_as_select_table102");
    std::string create2(
        fmt::format("create table {} AS SELECT id, name, age FROM {} WHERE age > 28;", table_as_select3, select_table));
    std::cout << create2 << std::endl;
    auto result2 = conn->Query(create2.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(table_as_select3);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 3);
    EXPECT_EQ(table_info1->GetIndexCount(), 0);
    auto name_col = table_info1->GetColumnByName("name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    // EXPECT_EQ(name_col->nullable, GS_FALSE);
    auto age_col = table_info1->GetColumnByName("age");
    EXPECT_FALSE(age_col == NULL);
    // test
    auto r1 = conn->Query(fmt::format("select * from {}", table_as_select3).c_str());
    EXPECT_TRUE(r1->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r1->RowCount(), 3);
}

TEST_F(CtasTest, CreateTableAsSelectWhereLessThan) {
    std::string select_table("select_table103");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // insert data
    std::string insert_data1(
        fmt::format("insert into {} values(1, 99, 23, 'ruo'), (2, 98, 25, 'teng'), (3, 53, 31, 'ilike'), (4, 32, 22, "
                    "'like'), (5, 55, 26, 'notlike'), (6, 78, 30, 'gob'), (7, 99, 21, 'gad'), (8, 88, 28, 'gas'), (9,  "
                    "79, 29, 'handle'), (10, 100, 26, 'kingseq');",
                    select_table));
    std::cout << insert_data1 << std::endl;
    auto result1 = conn->Query(insert_data1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    // test
    auto r = conn->Query(fmt::format("select * from {}", select_table).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 10);
    // create table with data and condition
    std::string table_as_select3("table_as_select_table103");
    std::string create2(
        fmt::format("create table {} AS SELECT id, name, age FROM {} WHERE age< 28;", table_as_select3, select_table));
    std::cout << create2 << std::endl;
    auto result2 = conn->Query(create2.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(table_as_select3);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 3);
    EXPECT_EQ(table_info1->GetIndexCount(), 0);
    auto name_col = table_info1->GetColumnByName("name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    // EXPECT_EQ(name_col->nullable, GS_FALSE);
    auto age_col = table_info1->GetColumnByName("age");
    EXPECT_FALSE(age_col == NULL);
    // test
    auto r1 = conn->Query(fmt::format("select * from {}", table_as_select3).c_str());
    EXPECT_TRUE(r1->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r1->RowCount(), 6);
}

TEST_F(CtasTest, CreateTableAsSelectWhereLargerEqual) {
    std::string select_table("select_table104");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // insert data
    std::string insert_data1(
        fmt::format("insert into {} values(1, 99, 23, 'ruo'), (2, 98, 25, 'teng'), (3, 53, 31, 'ilike'), (4, 32, 22, "
                    "'like'), (5, 55, 26, 'notlike'), (6, 78, 30, 'gob'), (7, 99, 21, 'gad'), (8, 88, 28, 'gas'), (9,  "
                    "79, 29, 'handle'), (10, 100, 26, 'kingseq');",
                    select_table));
    auto result1 = conn->Query(insert_data1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    // test
    auto r = conn->Query(fmt::format("select * from {}", select_table).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 10);
    // create table with data and condition
    std::string table_as_select3("table_as_select_table104");
    std::string create2(
        fmt::format("create table {} AS SELECT id, name, age FROM {} WHERE age>=25;", table_as_select3, select_table));
    auto result2 = conn->Query(create2.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(table_as_select3);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 3);
    EXPECT_EQ(table_info1->GetIndexCount(), 0);
    auto name_col = table_info1->GetColumnByName("name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    // EXPECT_EQ(name_col->nullable, GS_FALSE);
    auto age_col = table_info1->GetColumnByName("age");
    EXPECT_FALSE(age_col == NULL);
    EXPECT_EQ(age_col->col_type, GStorDataType::GS_TYPE_INTEGER);
    // test
    auto r1 = conn->Query(fmt::format("select * from {}", table_as_select3).c_str());
    EXPECT_TRUE(r1->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r1->RowCount(), 7);
}

TEST_F(CtasTest, CreateTableAsSelectWhereLessEqual) {
    std::string select_table("select_table105");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // insert data
    std::string insert_data1(
        fmt::format("insert into {} values(1, 99, 23, 'ruo'), (2, 98, 25, 'teng'), (3, 53, 31, 'ilike'), (4, 32, 22, "
                    "'like'), (5, 55, 26, 'notlike'), (6, 78, 30, 'gob'), (7, 99, 21, 'gad'), (8, 88, 28, 'gas'), (9,  "
                    "79, 29, 'handle'), (10, 100, 26, 'kingseq');",
                    select_table));
    auto result1 = conn->Query(insert_data1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    // test
    auto r = conn->Query(fmt::format("select * from {}", select_table).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 10);
    // create table with data and condition
    std::string table_as_select3("table_as_select_table105");
    std::string create2(
        fmt::format("create table {} AS SELECT id, name, age FROM {} WHERE age<=25;", table_as_select3, select_table));
    auto result2 = conn->Query(create2.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(table_as_select3);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 3);
    EXPECT_EQ(table_info1->GetIndexCount(), 0);
    auto name_col = table_info1->GetColumnByName("name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    // EXPECT_EQ(name_col->nullable, GS_FALSE);
    auto age_col = table_info1->GetColumnByName("age");
    EXPECT_FALSE(age_col == NULL);
    EXPECT_EQ(age_col->col_type, GStorDataType::GS_TYPE_INTEGER);
    // test
    auto r1 = conn->Query(fmt::format("select * from {}", table_as_select3).c_str());
    EXPECT_TRUE(r1->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r1->RowCount(), 4);
}

TEST_F(CtasTest, CreateTableAsSelectTableExist) {
    std::string select_table("select_table_exists");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    std::string table_as_select3(select_table);
    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);

    std::string create2(
        fmt::format("create table {} AS SELECT id, name, age FROM {} WHERE age<=25;", table_as_select3, select_table));
    std::cout << create2 << std::endl;
    auto result1 = conn->Query(create2.c_str());
    EXPECT_FALSE(result1->GetRetCode() == GS_SUCCESS);
    EXPECT_STREQ(result1->GetRetMsg().c_str(),
                 fmt::format("The object table {} already exists.", table_as_select3).c_str());
}

TEST_F(CtasTest, CreateTableAsSelectColNameNotExist) {
    std::string select_table("select_table_colname_notexists");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    std::string table_as_select3("table_as_select_colname_notexists");
    std::string colname_notexist("sage");
    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);

    std::string create2(fmt::format("create table {} AS SELECT id, name, {} FROM {} WHERE age<=25;", table_as_select3,
                                    colname_notexist, select_table));
    std::cout << create2 << std::endl;
    auto result1 = conn->Query(create2.c_str());
    EXPECT_FALSE(result1->GetRetCode() == GS_SUCCESS);
    EXPECT_STREQ(result1->GetRetMsg().c_str(),
                 fmt::format("Binder Error: column {} not found", colname_notexist).c_str());
}

TEST_F(CtasTest, CreateTableAsSelectWithDataAndCondAndAlias) {
    std::string select_table("select_table4");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // insert data
    std::string insert_data1(
        fmt::format("insert into {} values(1, 99, 23, 'ruo'), (2, 98, 25, 'teng');", select_table));
    auto result1 = conn->Query(insert_data1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    // test
    auto r = conn->Query(fmt::format("select * from {}", select_table).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 2);
    // create table with alias
    std::string table_as_select4("table_as_select_alias");
    std::string create2(
        fmt::format("create table {} AS SELECT id as new_id, name as new_name, age as new_age FROM {} WHERE id = 2;",
                    table_as_select4, select_table));
    auto result2 = conn->Query(create2.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(table_as_select4);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 3);
    EXPECT_EQ(table_info1->GetIndexCount(), 0);
    auto name_col = table_info1->GetColumnByName("new_name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    // EXPECT_EQ(name_col->nullable, GS_FALSE);
    auto age_col = table_info1->GetColumnByName("new_age");
    EXPECT_FALSE(age_col == NULL);
    EXPECT_EQ(age_col->col_type, GStorDataType::GS_TYPE_INTEGER);
    // test
    auto r1 = conn->Query(fmt::format("select * from {}", table_as_select4).c_str());
    EXPECT_TRUE(r1->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r1->RowCount(), 1);
}

TEST_F(CtasTest, CreateTableAsSelectWithDataAndCondAndStar) {
    std::string select_table("select_table5");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // insert data
    std::string insert_data1(
        fmt::format("insert into {} values(1, 99, 23, 'ruo'), (2, 98, 25, 'teng');", select_table));
    auto result1 = conn->Query(insert_data1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    // test
    auto r = conn->Query(fmt::format("select * from {}", select_table).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 2);
    // create table with star
    std::string table_as_select4("table_as_select_star");
    std::string create2(
        fmt::format("create table {} AS SELECT * FROM {} WHERE id = 2;", table_as_select4, select_table));
    auto result2 = conn->Query(create2.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(table_as_select4);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 4);
    EXPECT_EQ(table_info1->GetIndexCount(), 0);
    auto name_col = table_info1->GetColumnByName("name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    // EXPECT_EQ(name_col->nullable, GS_FALSE);
    auto age_col = table_info1->GetColumnByName("age");
    EXPECT_FALSE(age_col == NULL);
    EXPECT_EQ(age_col->col_type, GStorDataType::GS_TYPE_INTEGER);
    // test
    auto r1 = conn->Query(fmt::format("select * from {}", table_as_select4).c_str());
    EXPECT_TRUE(r1->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r1->RowCount(), 1);
}

TEST_F(CtasTest, CreateTableAsSelectTableNotExist) {
    std::string select_table("not_exist_table");

    // create table ,the select table does not exists
    std::string table_as_select4("table_as_select_star");
    std::string create2(
        fmt::format("create table {} AS SELECT * FROM {} WHERE id = 2;", table_as_select4, select_table));
    auto result = conn->Query(create2.c_str());
    EXPECT_FALSE(result->GetRetCode() == GS_SUCCESS);
}

TEST_F(CtasTest, CreateTableAsSelectColumnNotExist) {
    std::string select_table("select_table6");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;
    std::string not_exist_col("not_exist_col");
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    // create table ,the select table column does not exists
    std::string table_as_select5("table_as_select_col_notexist");
    std::string create2(
        fmt::format("create table {} AS SELECT {} FROM {};", table_as_select5, not_exist_col, select_table));
    auto result1 = conn->Query(create2.c_str());
    EXPECT_FALSE(result1->GetRetCode() == GS_SUCCESS);
    EXPECT_STREQ(result1->GetRetMsg().c_str(), fmt::format("Binder Error: column {} not found", not_exist_col).c_str());
}

TEST_F(CtasTest, CreateTableAsSelectTheSameTableName) {
    std::string select_table("select_table7");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;
    std::string table_as_select6(select_table);
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    std::string create2(fmt::format("create table {} AS SELECT * FROM {};", table_as_select6, select_table));
    auto result1 = conn->Query(create2.c_str());
    EXPECT_FALSE(result1->GetRetCode() == GS_SUCCESS);
    EXPECT_STREQ(result1->GetRetMsg().c_str(),
                 fmt::format("The object table {} already exists.", table_as_select6).c_str());
}

TEST_F(CtasTest, CreateTableAsSelectTableNameNULL) {
    std::string select_table("select_table8");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;
    std::string table_as_select7;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    std::string create2(fmt::format("create table {} AS SELECT * FROM {};", table_as_select7, select_table));
    auto result1 = conn->Query(create2.c_str());
    EXPECT_FALSE(result1->GetRetCode() == GS_SUCCESS);
}

TEST_F(CtasTest, CreateTableAsSelectTableNameOneSingleQuote) {
    std::string select_table("select_table_one_single_quote");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    std::string create2(fmt::format("create table ' AS SELECT * FROM {};", select_table));
    auto result1 = conn->Query(create2.c_str());
    EXPECT_FALSE(result1->GetRetCode() == GS_SUCCESS);
}

TEST_F(CtasTest, CreateTableAsSelectTableNameOneDoubleQuote) {
    std::string select_table("select_table_one_double_quote");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    std::string create2(fmt::format("create table \" AS SELECT * FROM {};", select_table));
    auto result1 = conn->Query(create2.c_str());
    EXPECT_FALSE(result1->GetRetCode() == GS_SUCCESS);
}

TEST_F(CtasTest, CreateTableAsSelectTableNameContainSPace) {
    std::string select_table("select_table_contain_space");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // insert data
    std::string insert_data1(
        fmt::format("insert into {} values(1, 99, 23, 'ruo'), (2, 98, 25, 'teng'), (3, 53, 31, 'ilike'), (4, 32, 22, "
                    "'like'), (5, 55, 26, 'notlike'), (6, 78, 30, 'gob'), (7, 99, 21, 'gad'), (8, 88, 28, 'gas'), (9,  "
                    "79, 29, 'handle'), (10, 100, 26, 'kingseq');",
                    select_table));
    auto result1 = conn->Query(insert_data1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    // test
    auto r = conn->Query(fmt::format("select * from {}", select_table).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 10);
    // create table with data and condition
    std::string table_as_select3("space cont");
    std::string create2(fmt::format("create table \"{}\" AS SELECT id, name, age FROM {} WHERE age<=25;",
                                    table_as_select3, select_table));
    auto result2 = conn->Query(create2.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(table_as_select3);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 3);
    EXPECT_EQ(table_info1->GetIndexCount(), 0);
    auto name_col = table_info1->GetColumnByName("name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    // EXPECT_EQ(name_col->nullable, GS_FALSE);
    auto age_col = table_info1->GetColumnByName("age");
    EXPECT_FALSE(age_col == NULL);
    EXPECT_EQ(age_col->col_type, GStorDataType::GS_TYPE_INTEGER);
    // test
    auto r1 = conn->Query(fmt::format("select * from \"{}\"", table_as_select3).c_str());
    EXPECT_TRUE(r1->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r1->RowCount(), 4);
}

TEST_F(CtasTest, CreateTableAsSelectTableNameOutOfRange) {
    std::string select_table("select_table_name_outofrange");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;
    std::string table_as_select6("tablenameoutofrangetablenameoutofrangetablenameoutofrangetablenameoutofrange");
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    std::string create2(fmt::format("create table {} AS SELECT * FROM {};", table_as_select6, select_table));
    auto result1 = conn->Query(create2.c_str());
    EXPECT_FALSE(result1->GetRetCode() == GS_SUCCESS);
}

TEST_F(CtasTest, CreateTableAsSelectTableNameLength60) {
    std::string select_table("select_table_name_length60");
    std::string query(fmt::format(
        "create table {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, name varchar NOT NULL);",
        select_table));
    std::cout << query << std::endl;
    std::string table_as_select6("tablenameoutofrangetablenameoutofrangetablenameoutofranget60");
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    std::string create2(fmt::format("create table {} AS SELECT * FROM {};", table_as_select6, select_table));
    auto result1 = conn->Query(create2.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(table_as_select6);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 0);
    auto name_col = table_info->GetColumnByName("name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    // EXPECT_EQ(name_col->nullable, GS_FALSE);
    auto age_col = table_info->GetColumnByName("age");
    EXPECT_FALSE(age_col == NULL);
    EXPECT_EQ(age_col->col_type, GStorDataType::GS_TYPE_INTEGER);
}

TEST_F(CtasTest, CreateTableAsSelectConstant) {
    std::string table_as_select6("table_as_select_constant");
    std::string create2(fmt::format("create table {} AS SELECT '123456789';", table_as_select6));
    auto result = conn->Query(create2.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(table_as_select6);
    EXPECT_FALSE(table_info == nullptr);
    // test
    auto r = conn->Query(fmt::format("select * from \"{}\"", table_as_select6).c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 1);
}

TEST_F(CtasTest, CreateTableAsSelectTableJoin) {
    std::string table_as_select6("table_as_select_join");
    std::string select_join_table1("select_join_table1");
    std::string select_join_table2("select_join_table2");
    auto result = conn->Query(
        fmt::format("create table {} (id integer, score integer, age integer, name varchar);", select_join_table1)
            .c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto result1 = conn->Query(
        fmt::format("insert into {} values(1, 99, 25, 'ruo'),(2, 78, 23,'peo'),(3, 87, 24, 'erw'),(4, 99, 23, 'ads');",
                    select_join_table1)
            .c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto result2 = conn->Query(
        fmt::format("create table {} (id integer, gpa decimal, class_name varchar);", select_join_table2).c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto result3 = conn->Query(fmt::format("insert into {} values(1, 99.9, '语文'),(2, 78.0,'数学'),(3, 87.5, "
                                           "'英语'),(4, 99.5, '物理'),(5, 80.5, '历史'),(6, 72.5, '化学');",
                                           select_join_table2)
                                   .c_str());
    EXPECT_TRUE(result3->GetRetCode() == GS_SUCCESS);

    std::string create2(fmt::format("create table {} AS SELECT * from {} join {} on {}.id = {}.id;", table_as_select6,
                                    select_join_table1, select_join_table2, select_join_table1, select_join_table2));
    auto result4 = conn->Query(create2.c_str());
    EXPECT_TRUE(result4->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(table_as_select6);
    EXPECT_FALSE(table_info == nullptr);
    // test
    auto r = conn->Query(fmt::format("select * from \"{}\"", table_as_select6).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 4);

    r = conn->Query("create table select_join_no_cond as select * from select_join_table1 ,select_join_table2");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    r = conn->Query("select * from select_join_no_cond");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    ASSERT_EQ(r->RowCount(), 24);
}

TEST_F(CtasTest, CreateTableAsSelectTableJoinAlias) {
    std::string table_as_select6("table_as_select_join_alias");
    std::string select_join_table1("select_join_alias_table1");
    std::string select_join_table2("select_join_alias_table2");
    auto result = conn->Query(
        fmt::format("create table {} (id integer, score integer, age integer, name varchar);", select_join_table1)
            .c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto result1 = conn->Query(
        fmt::format("insert into {} values(1, 99, 25, 'ruo'),(2, 78, 23,'peo'),(3, 87, 24, 'erw'),(4, 99, 23, 'ads');",
                    select_join_table1)
            .c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto result2 = conn->Query(
        fmt::format("create table {} (id integer, gpa decimal, class_name varchar);", select_join_table2).c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);
    auto result3 = conn->Query(fmt::format("insert into {} values(1, 99.9, '语文'),(2, 78.0,'数学'),(3, 87.5, "
                                           "'英语'),(4, 99.5, '物理'),(5, 80.5, '历史'),(6, 72.5, '化学');",
                                           select_join_table2)
                                   .c_str());
    EXPECT_TRUE(result3->GetRetCode() == GS_SUCCESS);
    std::string create2(
        fmt::format("create table {} AS SELECT {}.name as stu_name, {}.class_name as class_name, {}.gpa as result from "
                    "{} join {} on {}.id = {}.id;",
                    table_as_select6, select_join_table1, select_join_table2, select_join_table2, select_join_table1,
                    select_join_table2, select_join_table1, select_join_table2));
    auto result4 = conn->Query(create2.c_str());
    EXPECT_TRUE(result4->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(table_as_select6);
    EXPECT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 3);
    auto stu_name_col = table_info->GetColumnByName("stu_name");
    EXPECT_FALSE(stu_name_col == NULL);
    auto class_name_col = table_info->GetColumnByName("class_name");
    EXPECT_FALSE(class_name_col == NULL);
    auto result_col = table_info->GetColumnByName("result");
    EXPECT_FALSE(result_col == NULL);
    // test
    auto r = conn->Query(fmt::format("select * from \"{}\"", table_as_select6).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 4);
}

TEST_F(CtasTest, CreateTableAsSelectTableSubquery) {
    std::string table_as_select6("table_as_select_subquery");
    std::string select_table1("select_subquery_table1");
    std::string select_table2("select_subquery_table2");
    auto result = conn->Query(
        fmt::format("create table {} (id integer, score integer, age integer, name varchar);", select_table1).c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto result1 = conn->Query(
        fmt::format("insert into {} values(1, 99, 25, 'ruo'),(2, 78, 23,'peo'),(3, 87, 24, 'erw'),(4, 99, 23, 'ads');",
                    select_table1)
            .c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto result2 = conn->Query(
        fmt::format("create table {} (id integer, gpa decimal, class_name varchar);", select_table2).c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto result3 = conn->Query(fmt::format("insert into {} values(1, 99.9, '语文'),(2, 78.0,'数学'),(3, 87.5, "
                                           "'英语'),(4, 99.5, '物理'),(5, 80.5, '历史'),(6, 72.5, '化学');",
                                           select_table2)
                                   .c_str());
    EXPECT_TRUE(result3->GetRetCode() == GS_SUCCESS);

    std::string create2(
        fmt::format("create table {} AS SELECT * from {} where id in (select id from {} where gpa > 80);",
                    table_as_select6, select_table1, select_table2));
    auto result4 = conn->Query(create2.c_str());
    EXPECT_TRUE(result4->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(table_as_select6);
    EXPECT_FALSE(table_info == nullptr);
    // test
    auto r = conn->Query(fmt::format("select * from \"{}\"", table_as_select6).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 3);

    r = conn->Query("create table aab as select s.id from select_subquery_table2 s");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("create table table_from_sub1 as select * from (select 'abc',1)");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("create table table_from_sub_values as select * from (values (1,2),('1',2.1))");
    EXPECT_EQ(r->GetRetCode(), 0);
    table_info = conn->GetTableInfo("table_from_sub_values");
    EXPECT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 2);
    EXPECT_EQ(table_info->GetIndexCount(), 0);
    auto col = table_info->GetColumnByName("col0");
    EXPECT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    // EXPECT_EQ(col->nullable, GS_FALSE);
    EXPECT_EQ(col->nullable, GS_TRUE);
    EXPECT_TRUE(col->size > 0);
    col = table_info->GetColumnByName("col1");
    EXPECT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_DECIMAL);
    // EXPECT_EQ(col->nullable, GS_FALSE);
    EXPECT_EQ(col->nullable, GS_TRUE);
    EXPECT_TRUE(col->size > 0);
    EXPECT_EQ(col->precision, 11);
    EXPECT_EQ(col->scale, 1);
}

TEST_F(CtasTest, CreateTableAsWithUnion) {
    auto r = conn->Query("create table u1 as select 1 union select 2");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("create table u2 as select null union all select null");
    EXPECT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select * from u2");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetType(), GStorDataType::GS_TYPE_INTEGER);

    r = conn->Query("create table u3 as select 'abc' union select 'abcd'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->GetEffectRow(), 2);

    r = conn->Query("create table u4 as select 1111.0 union select 2.11");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->GetEffectRow(), 2);
    r = conn->Query("select * from u4");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).precision, 6);
    EXPECT_EQ(r->Row(0).Field(0).scale, 2);

    // r = conn->Query("create table u5 as select 1111.0 union select 2");
    // ASSERT_EQ(r->GetRetCode(), 0);
    // EXPECT_EQ(r->RowCount(), 2);
    // r = conn->Query("select * from u5");
    // ASSERT_EQ(r->GetRetCode(), 0);
    // EXPECT_EQ(r->RowCount(), 2);
    // EXPECT_EQ(r->Row(0).Field(0).precision, 11);
    // EXPECT_EQ(r->Row(0).Field(0).scale, 1);
}

// 检查新旧表的约束是否一致
TEST_F(CtasTest, CreateTableWithDefCheck) {
    conn->Query("drop table if exists t");
    conn->Query("create table t(a int , b int)");
    conn->Query("insert into t values (null,1),(2,2)");
    conn->Query("drop table if exists u3");
    auto r = conn->Query("create table u3 as select a+1 , -a from t");
    EXPECT_EQ(r->GetRetCode(), 0);  // 成功创建，说明插入成功，说明列时可以插入null值,符合预期
}

TEST_F(CtasTest, CreateTableAsSelectTableBug709229042) {
    std::string table_as_select6("i2");
    std::string select_table1("integers");
    auto result = conn->Query(fmt::format("create table {} (i INTEGER);", select_table1).c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto result1 = conn->Query(fmt::format("insert into {} values (1), (2), (3), (4), (5);", select_table1).c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    std::string create2(
        fmt::format("create table {} AS SELECT 1 AS i FROM {} WHERE i % 2 <> 0;", table_as_select6, select_table1));
    auto result4 = conn->Query(create2.c_str());
    EXPECT_TRUE(result4->GetRetCode() == GS_SUCCESS);

    auto r = conn->Query(fmt::format("UPDATE {} SET i=NULL;", table_as_select6).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(table_as_select6);
    EXPECT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 1);
    auto i_name_col = table_info->GetColumnByName("i");
    EXPECT_FALSE(i_name_col == NULL);
    EXPECT_TRUE(i_name_col->nullable == GS_TRUE);
}
// CREATE TABLE IF NOT EXISTS t1 as select
TEST_F(CtasTest, CreateTableAsSelectIfNotExists) {
    std::string select_table("select_table_ifnotexists");
    std::string query(
        fmt::format("create table IF NOT EXISTS {} (id integer PRIMARY KEY, score integer, age integer DEFAULT 20, "
                    "name varchar NOT NULL);",
                    select_table));
    std::cout << query << std::endl;

    // create table select_table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(select_table);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    // insert data
    std::string insert_data1(
        fmt::format("insert into {} values(1, 99, 23, 'ruo'), (2, 98, 25, 'teng');", select_table));
    auto result1 = conn->Query(insert_data1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    // test
    auto r = conn->Query(fmt::format("select * from {}", select_table).c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 2);
    // create table with data no condition
    std::string table_as_select2("table_as_select_ifnotexists");
    std::string create2(
        fmt::format("create table IF NOT EXISTS {} AS SELECT id, name, age FROM {};", table_as_select2, select_table));
    // first
    auto result2 = conn->Query(create2.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(table_as_select2);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 3);
    EXPECT_EQ(table_info1->GetIndexCount(), 0);
    auto name_col = table_info1->GetColumnByName("name");
    EXPECT_FALSE(name_col == NULL);
    EXPECT_EQ(name_col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    auto age_col = table_info1->GetColumnByName("age");
    EXPECT_FALSE(age_col == NULL);
    EXPECT_EQ(age_col->col_type, GStorDataType::GS_TYPE_INTEGER);

    // test
    auto r1 = conn->Query(fmt::format("select * from {}", table_as_select2).c_str());
    EXPECT_TRUE(r1->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r1->RowCount(), 2);
    // second
    auto result3 = conn->Query(create2.c_str());
    EXPECT_TRUE(result3->GetRetCode() == GS_SUCCESS);
    //
    auto result4 = conn->Query(
        fmt::format("create table {} AS SELECT id, name, age FROM {};", table_as_select2, select_table).c_str());
    EXPECT_TRUE(result4->GetRetCode() == GS_ERROR);

    auto result5 = conn->Query(fmt::format("insert into {} values(3, 22, 53, 'ee');", select_table).c_str());
    EXPECT_TRUE(result5->GetRetCode() == GS_SUCCESS);

    // third
    auto result6 = conn->Query(create2.c_str());
    EXPECT_TRUE(result6->GetRetCode() == GS_SUCCESS);

    auto r2 = conn->Query(fmt::format("select * from {}", table_as_select2).c_str());
    EXPECT_TRUE(r2->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r2->RowCount(), 2);

    auto r3 = conn->Query(fmt::format("select * from {}", select_table).c_str());
    EXPECT_TRUE(r3->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r3->RowCount(), 3);
}

// CREATE OR REPLACE TABLE AS SELECT
TEST_F(CtasTest, CreateTableAsSelectTableBug709565416) {
    std::string tablename("table_as_replace");
    std::string query(fmt::format("CREATE OR REPLACE TABLE {} AS SELECT 9;", tablename));
    std::cout << query << std::endl;
    // first
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_ERROR);
    // EXPECT_STREQ(result->GetRetMsg().c_str(), "replace expr is not supported in create-table-as-select sql!");
}

// create as select from values
TEST_F(CtasTest, CreateTableAsSelectFromValues) {
    conn->Query("drop table if exists strings");
    auto r = conn->Query("create table strings as select * from ( values (' ') , ('abc') )");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    r = conn->Query("select * from strings");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), " ");
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<std::string>(), "abc");
}

TEST_F(CtasTest, CreateTableAsSelectWithSameNameColumn) {
    conn->Query("drop table if exists join1");
    conn->Query("drop table if exists join2");
    conn->Query("create table join1 (a int)");
    conn->Query("create table join2 (a int)");
    conn->Query("insert into join1 values (1),(2),(3)");
    conn->Query("insert into join2 values (0)");
    auto r = conn->Query("create table join3 as select * from join1 ,join2");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    r = conn->Query("select * from join3");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    ASSERT_EQ(r->RowCount(), 3);
    ASSERT_EQ(r->ColumnCount(), 2);
    auto schema = r->GetSchema();
    ASSERT_EQ(schema.GetColumn(0).NameWithoutPrefix(), "a");
    ASSERT_EQ(schema.GetColumn(1).NameWithoutPrefix(), "a_1");
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 0);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<int32_t>(), 0);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(2).Field(1).GetCastAs<int32_t>(), 0);

    // test with same alias
    r = conn->Query("create table same_alias as select a as b , a as b from join1");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    r = conn->Query("select * from same_alias");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    ASSERT_EQ(r->ColumnCount(), 2);
    schema = r->GetSchema();
    ASSERT_EQ(schema.GetColumn(0).NameWithoutPrefix(), "b");
    ASSERT_EQ(schema.GetColumn(1).NameWithoutPrefix(), "b_1");
}

TEST_F(CtasTest, CreateTableAsSelectWithNullConstant) {
    conn->Query("drop table if exists null_table");
    auto r = conn->Query("create table null_table as select null add_null");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    r = conn->Query("select * from null_table");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    r = conn->Query("show null_table");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    ASSERT_EQ(r->RowCount(), 1);
    ASSERT_EQ(r->RowRef(0).Field(0).GetCastAs<std::string>(), "add_null");
    ASSERT_EQ(r->RowRef(0).Field(1).GetCastAs<std::string>(), "INTEGER");
    ASSERT_EQ(r->RowRef(0).Field(2).GetCastAs<std::string>(), "4");
}

TEST_F(CtasTest,CreateTableAsSelctWitDecimal) {
    conn->Query("drop table tbl");
    conn->Query("drop table decimals");
    auto r = conn->Query("create table tbl(i int)");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);

    r = conn->Query("insert into tbl values(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);

    r = conn->Query("CREATE TABLE decimals AS SELECT i::DECIMAL(4,1) AS d1, (i * i)::DECIMAL(9,1) AS d2, (i * i * i)::DECIMAL(18,1) AS d3 FROM tbl");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    
    auto table_info = conn->GetTableInfo("decimals");
    ASSERT_FALSE(table_info == nullptr);
    ASSERT_EQ(table_info->GetColumnCount(), 3);
    const auto& col1 = table_info->GetColumnByName("d1");
    ASSERT_FALSE(col1 == nullptr);
    ASSERT_EQ(col1->col_type, GStorDataType::GS_TYPE_DECIMAL);
    EXPECT_EQ(col1->precision, 4);
    EXPECT_EQ(col1->scale, 1);

    const auto& col2 = table_info->GetColumnByName("d2");
    ASSERT_FALSE(col2 == nullptr);
    ASSERT_EQ(col2->col_type, GStorDataType::GS_TYPE_DECIMAL);
    EXPECT_EQ(col2->precision, 9);
    EXPECT_EQ(col2->scale, 1);

    const auto& col3 = table_info->GetColumnByName("d3");
    ASSERT_FALSE(col3 == nullptr);
    ASSERT_EQ(col3->col_type, GStorDataType::GS_TYPE_DECIMAL);
    EXPECT_EQ(col3->precision, 18);
    EXPECT_EQ(col3->scale, 1);

    r = conn->Query("create table decimals2 as select i::Decimal(4,1) + i::Decimal(4,1) as d from tbl");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    table_info = conn->GetTableInfo("decimals2");
    ASSERT_FALSE(table_info == nullptr);
    ASSERT_EQ(table_info->GetColumnCount(), 1);
    const auto& col = table_info->GetColumnByName("d");
    ASSERT_FALSE(col == nullptr);
    ASSERT_EQ(col->col_type, GStorDataType::GS_TYPE_DECIMAL);
    EXPECT_EQ(col->precision, 5);
    EXPECT_EQ(col->scale, 1);
}

TEST_F(CtasTest,CreateTableAsSelctWitCountStar) {
    conn->Query("drop table tbl");
    conn->Query("drop table count_star");
    auto r = conn->Query("create table tbl(i int)");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);

    r = conn->Query("insert into tbl values(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);

    r = conn->Query("CREATE TABLE count_star AS SELECT COUNT(*) AS c1, COUNT(1) AS c2, COUNT() AS c3 FROM tbl");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    
    auto table_info = conn->GetTableInfo("count_star");
    ASSERT_FALSE(table_info == nullptr);
    ASSERT_EQ(table_info->GetColumnCount(), 3);
    const auto& col1 = table_info->GetColumnByName("c1");
    ASSERT_FALSE(col1 == nullptr);
    ASSERT_EQ(col1->col_type, GStorDataType::GS_TYPE_BIGINT);

    const auto& col2 = table_info->GetColumnByName("c2");
    ASSERT_FALSE(col2 == nullptr);
    ASSERT_EQ(col2->col_type, GStorDataType::GS_TYPE_BIGINT);

    const auto& col3 = table_info->GetColumnByName("c3");
    ASSERT_FALSE(col3 == nullptr);
    ASSERT_EQ(col3->col_type, GStorDataType::GS_TYPE_BIGINT);
}

int main(int argc, char **argv) {
    ::testing::GTEST_FLAG(output) = "xml";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
