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
* show_test.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/test/show_test.cpp
*
* -------------------------------------------------------------------------
*/
// test for show table
#include <gtest/gtest.h>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "main/connection.h"
#include "main/database.h"

class ShowTest : public ::testing::Test {
   protected:
    ShowTest() {}
    ~ShowTest() {}
    static void SetUpTestSuite() {
        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance->Init();
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();

        // sleep for a while to wait for db to start
        sleep(1);
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

std::shared_ptr<IntarkDB> ShowTest::db_instance = nullptr;
std::unique_ptr<Connection> ShowTest::conn = nullptr;

TEST_F(ShowTest, ShowTablesNoUserTable) {
    std::string query(fmt::format("show tables;"));
    std::cout << query << std::endl;

    // show tables
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(result->RowCount(), 0);
}

TEST_F(ShowTest, ShowTablesHavingUserTable) {
    std::string query(fmt::format("show tables;"));
    std::cout << query << std::endl;

    std::string create_table(
        "CREATE TABLE show_normal_type_table(sid BIGINT, age INTEGER, name VARCHAR, sex BOOL, duty STRING, title "
        "VARCHAR, grade SMALLINT, class INT4, rank INT, score REAL, is_grad BOOLEAN, school_make_up TINYINT, mathscore "
        "FLOAT, englishscore FLOAT4, randnum1 INT8, randnum2 DOUBLE, course INT2, mastercnt MEDIUMINT, grade_s "
        "DECIMAL(5,2), in_time DATE, start_time TIMESTAMP);");
    auto result = conn->Query(create_table.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto result1 = conn->Query(query.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(result1->RowCount(), 1);
}

TEST_F(ShowTest, ShowSpecificTableNotCreated) {
    std::string tablename("not_created_table");
    std::string query(fmt::format("show {};", tablename));
    auto result = conn->Query(query.c_str());
    EXPECT_FALSE(result->GetRetCode() == GS_SUCCESS);
    EXPECT_STREQ(result->GetRetMsg().c_str(),
                 intarkdb::Exception(ExceptionType::CATALOG, fmt::format("table {} not found", tablename)).what());
}

TEST_F(ShowTest, ShowSpecificTable) {
    std::string tablename("show_normal_table");
    std::string query(fmt::format("show {};", tablename));
    std::cout << query << std::endl;

    std::string create_tabler(
        fmt::format("CREATE TABLE {}(sidn INTEGER, agen INTEGER default 20, namen VARCHAR, honor VARCHAR(20) default "
                    "'demo', class number(2), grade number(10,3), del decimal(5, 2) NOT NULL);",
                    tablename));
    auto result1 = conn->Query(create_tabler.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 7);
    auto& headers = result->GetSchema().GetColumnInfos();
    EXPECT_STREQ("sidn", result->Row(0).Field(headers[0].slot).ToString().c_str());     // col_name
    EXPECT_STREQ("INTEGER", result->Row(0).Field(headers[1].slot).ToString().c_str());  // datatype
    EXPECT_STREQ("4", result->Row(0).Field(headers[2].slot).ToString().c_str());        // col_size
    EXPECT_STREQ("", result->Row(0).Field(headers[3].slot).ToString().c_str());         // precision
    EXPECT_STREQ("", result->Row(0).Field(headers[4].slot).ToString().c_str());         // scale
    EXPECT_STREQ("true", result->Row(0).Field(headers[5].slot).ToString().c_str());     // nullable
    EXPECT_STREQ("null", result->Row(0).Field(headers[6].slot).ToString().c_str());     // default

    EXPECT_STREQ("agen", result->Row(1).Field(headers[0].slot).ToString().c_str());     // col_name
    EXPECT_STREQ("INTEGER", result->Row(1).Field(headers[1].slot).ToString().c_str());  // datatype
    EXPECT_STREQ("4", result->Row(1).Field(headers[2].slot).ToString().c_str());        // col_size
    EXPECT_STREQ("", result->Row(1).Field(headers[3].slot).ToString().c_str());         // precision
    EXPECT_STREQ("", result->Row(1).Field(headers[4].slot).ToString().c_str());         // scale
    EXPECT_STREQ("true", result->Row(1).Field(headers[5].slot).ToString().c_str());     // nullable
    EXPECT_STREQ("20", result->Row(1).Field(headers[6].slot).ToString().c_str());       // default

    EXPECT_STREQ("namen", result->Row(2).Field(headers[0].slot).ToString().c_str());    // col_name
    EXPECT_STREQ("VARCHAR", result->Row(2).Field(headers[1].slot).ToString().c_str());  // datatype
    EXPECT_STREQ("65535", result->Row(2).Field(headers[2].slot).ToString().c_str());    // col_size
    EXPECT_STREQ("", result->Row(2).Field(headers[3].slot).ToString().c_str());         // precision
    EXPECT_STREQ("", result->Row(2).Field(headers[4].slot).ToString().c_str());         // scale
    EXPECT_STREQ("true", result->Row(2).Field(headers[5].slot).ToString().c_str());     // nullable
    EXPECT_STREQ("null", result->Row(2).Field(headers[6].slot).ToString().c_str());     // default

    EXPECT_STREQ("honor", result->Row(3).Field(headers[0].slot).ToString().c_str());    // col_name
    EXPECT_STREQ("VARCHAR", result->Row(3).Field(headers[1].slot).ToString().c_str());  // datatype
    EXPECT_STREQ("20", result->Row(3).Field(headers[2].slot).ToString().c_str());       // col_size
    EXPECT_STREQ("", result->Row(3).Field(headers[3].slot).ToString().c_str());         // precision
    EXPECT_STREQ("", result->Row(3).Field(headers[4].slot).ToString().c_str());         // scale
    EXPECT_STREQ("true", result->Row(3).Field(headers[5].slot).ToString().c_str());     // nullable
    EXPECT_STREQ("demo", result->Row(3).Field(headers[6].slot).ToString().c_str());     // default

    EXPECT_STREQ("class", result->Row(4).Field(headers[0].slot).ToString().c_str());   // col_name
    EXPECT_STREQ("DECIMAL", result->Row(4).Field(headers[1].slot).ToString().c_str());  // datatype
    EXPECT_STREQ("8", result->Row(4).Field(headers[2].slot).ToString().c_str());       // col_size
    EXPECT_STREQ("2", result->Row(4).Field(headers[3].slot).ToString().c_str());       // precision
    EXPECT_STREQ("0", result->Row(4).Field(headers[4].slot).ToString().c_str());       // scale
    EXPECT_STREQ("true", result->Row(4).Field(headers[5].slot).ToString().c_str());    // nullable
    EXPECT_STREQ("null", result->Row(4).Field(headers[6].slot).ToString().c_str());    // default

    EXPECT_STREQ("grade", result->Row(5).Field(headers[0].slot).ToString().c_str());   // col_name
    EXPECT_STREQ("DECIMAL", result->Row(5).Field(headers[1].slot).ToString().c_str());  // datatype
    EXPECT_STREQ("8", result->Row(5).Field(headers[2].slot).ToString().c_str());       // col_size
    EXPECT_STREQ("10", result->Row(5).Field(headers[3].slot).ToString().c_str());      // precision
    EXPECT_STREQ("3", result->Row(5).Field(headers[4].slot).ToString().c_str());       // scale
    EXPECT_STREQ("true", result->Row(5).Field(headers[5].slot).ToString().c_str());    // nullable
    EXPECT_STREQ("null", result->Row(5).Field(headers[6].slot).ToString().c_str());    // default

    EXPECT_STREQ("del", result->Row(6).Field(headers[0].slot).ToString().c_str());      // col_name
    EXPECT_STREQ("DECIMAL", result->Row(6).Field(headers[1].slot).ToString().c_str());  // datatype
    EXPECT_STREQ("8", result->Row(6).Field(headers[2].slot).ToString().c_str());        // col_size
    EXPECT_STREQ("5", result->Row(6).Field(headers[3].slot).ToString().c_str());        // precision
    EXPECT_STREQ("2", result->Row(6).Field(headers[4].slot).ToString().c_str());        // scale
    EXPECT_STREQ("false", result->Row(6).Field(headers[5].slot).ToString().c_str());    // nullable
    EXPECT_STREQ("null", result->Row(6).Field(headers[6].slot).ToString().c_str());     // default
}

TEST_F(ShowTest, DescribeSpecificTable) {
    std::string tablename("des_normal_table");
    std::string query(fmt::format("describe {};", tablename));
    std::cout << query << std::endl;

    std::string create_tabler(fmt::format("CREATE TABLE {}(sidn INTEGER, agen INTEGER, namen VARCHAR, address varchar default null);", tablename));
    auto result1 = conn->Query(create_tabler.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 4);
    auto& headers = result->GetSchema().GetColumnInfos();
    EXPECT_STREQ("sidn", result->Row(0).Field(headers[0].slot).ToString().c_str());     // col_name
    EXPECT_STREQ("INTEGER", result->Row(0).Field(headers[1].slot).ToString().c_str());  // datatype
    EXPECT_STREQ("4", result->Row(0).Field(headers[2].slot).ToString().c_str());        // col_size
    EXPECT_STREQ("", result->Row(0).Field(headers[3].slot).ToString().c_str());         // precision
    EXPECT_STREQ("", result->Row(0).Field(headers[4].slot).ToString().c_str());         // scale
    EXPECT_STREQ("true", result->Row(0).Field(headers[5].slot).ToString().c_str());     // nullable
    EXPECT_STREQ("null", result->Row(0).Field(headers[6].slot).ToString().c_str());     // default

    EXPECT_STREQ("agen", result->Row(1).Field(headers[0].slot).ToString().c_str());     // col_name
    EXPECT_STREQ("INTEGER", result->Row(1).Field(headers[1].slot).ToString().c_str());  // datatype
    EXPECT_STREQ("4", result->Row(1).Field(headers[2].slot).ToString().c_str());        // col_size
    EXPECT_STREQ("", result->Row(1).Field(headers[3].slot).ToString().c_str());         // precision
    EXPECT_STREQ("", result->Row(1).Field(headers[4].slot).ToString().c_str());         // scale
    EXPECT_STREQ("true", result->Row(1).Field(headers[5].slot).ToString().c_str());     // nullable
    EXPECT_STREQ("null", result->Row(1).Field(headers[6].slot).ToString().c_str());     // default

    EXPECT_STREQ("namen", result->Row(2).Field(headers[0].slot).ToString().c_str());    // col_name
    EXPECT_STREQ("VARCHAR", result->Row(2).Field(headers[1].slot).ToString().c_str());  // datatype
    EXPECT_STREQ("65535", result->Row(2).Field(headers[2].slot).ToString().c_str());    // col_size
    EXPECT_STREQ("", result->Row(2).Field(headers[3].slot).ToString().c_str());         // precision
    EXPECT_STREQ("", result->Row(2).Field(headers[4].slot).ToString().c_str());         // scale
    EXPECT_STREQ("true", result->Row(2).Field(headers[5].slot).ToString().c_str());     // nullable
    EXPECT_STREQ("null", result->Row(2).Field(headers[6].slot).ToString().c_str());     // default
  
    EXPECT_STREQ("address", result->Row(3).Field(headers[0].slot).ToString().c_str());  // col_name
    EXPECT_STREQ("VARCHAR", result->Row(3).Field(headers[1].slot).ToString().c_str());  // datatype
    EXPECT_STREQ("65535", result->Row(3).Field(headers[2].slot).ToString().c_str());    // col_size
    EXPECT_STREQ("", result->Row(3).Field(headers[3].slot).ToString().c_str());         // precision
    EXPECT_STREQ("", result->Row(3).Field(headers[4].slot).ToString().c_str());         // scale
    EXPECT_STREQ("true", result->Row(3).Field(headers[5].slot).ToString().c_str());     // nullable
    EXPECT_STREQ("null", result->Row(3).Field(headers[6].slot).ToString().c_str());     // default
}

TEST_F(ShowTest, DescribeSpecificTableWithIndex) {
    conn->Query("drop table if exists des_normal_table");
    auto r = conn->Query("create table des_normal_table(sidn INTEGER primary key, v int , name varchar)");
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    conn->Query("insert into des_normal_table values(1, 1, 'a')");
    r = conn->Query("create index idx_des_normal_table on des_normal_table(name)");
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    r = conn->Query("show des_normal_table");
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->RowRef(0).Field(0).GetCastAs<std::string>(), "sidn");
    EXPECT_EQ(r->RowRef(0).Field(7).GetCastAs<std::string>(), "PRI");
    EXPECT_EQ(r->RowRef(1).Field(0).GetCastAs<std::string>(), "v");
    EXPECT_EQ(r->RowRef(1).Field(7).GetCastAs<std::string>(), "");
    EXPECT_EQ(r->RowRef(2).Field(0).GetCastAs<std::string>(), "name"); 
    EXPECT_EQ(r->RowRef(2).Field(7).GetCastAs<std::string>(), "MUL"); 
}

int main(int argc, char** argv) {
    system("rm -rf intarkdb/");
    ::testing::GTEST_FLAG(output) = "xml";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
