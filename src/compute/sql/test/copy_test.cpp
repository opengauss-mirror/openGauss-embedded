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
 * copy_test.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/test/copy_test.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <gtest/gtest.h>

#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "common/csv_util.h"
#include "main/connection.h"
#include "main/database.h"

class ConnectionForCopyTest : public ::testing::Test {
   protected:
    ConnectionForCopyTest() {}
    ~ConnectionForCopyTest() {}
    static void SetUpTestSuite() {
        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance->Init();
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();

        system("rm test.csv test2.csv test_out.csv test_out2.csv test_out3.csv");

        // create test.csv
        std::ofstream out("test.csv");
        out << "1,hello" << std::endl;
        out << "2,world" << std::endl;
        out << "3,null" << std::endl;
        out.close();

        // create test2.csv with | delimiter
        std::ofstream out2("test2.csv");
        out2 << "1|hello" << std::endl;
        out2 << "2|world" << std::endl;
        out2 << "3|null" << std::endl;
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

std::shared_ptr<IntarkDB> ConnectionForCopyTest::db_instance = nullptr;
std::unique_ptr<Connection> ConnectionForCopyTest::conn = nullptr;

TEST_F(ConnectionForCopyTest, TestCopyFrom) {
    // table not exists
    // print pwd
    auto r = conn->Query("COPY copy_test FROM 'test.csv'");
    ASSERT_NE(r->GetRetCode(), 0);

    r = conn->Query("CREATE TABLE copy_test (a INT, b varchar(20))");
    ASSERT_EQ(r->GetRetCode(), 0);

    r = conn->Query("COPY copy_test FROM 'test.csv'");
    ASSERT_EQ(r->GetRetCode(), 0);

    r = conn->Query("select * from copy_test");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "hello");
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<std::string>(), "world");
    EXPECT_EQ(r->Row(2).Field(1).IsNull(), true);

    // test with delimiter
    r = conn->Query("CREATE TABLE copy_test2 (a INT, b varchar(20))");
    r = conn->Query("COPY copy_test2 FROM 'test2.csv' (DELIMITER '|')");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select * from copy_test2");
    EXPECT_EQ(r->RowCount(), 3);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "hello");
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<std::string>(), "world");
    EXPECT_EQ(r->Row(2).Field(1).IsNull(), true);

    // test with header
    r = conn->Query("CREATE TABLE copy_test3 (a INT, b varchar(20))");
    r = conn->Query("COPY copy_test3 FROM 'test.csv' (HEADER)");
    // not suported yet
    ASSERT_NE(r->GetRetCode(), 0);

    // test with not csv format
    r = conn->Query("COPY copy_test2 FROM 'test2.csv' (DELIMITER '|', FORMAT 'parquet')");
    ASSERT_NE(r->GetRetCode(), 0);

    // test auto detect
    r = conn->Query("COPY copy_test2 FROM 'test2.csv' (DELIMITER '|', AUTO_DETECT true)");
    ASSERT_NE(r->GetRetCode(), 0);

    // not support ARRAY
    r = conn->Query("COPY copy_test2 FROM 'test2.csv' (DELIMITER '|', ARRAY true)");
    ASSERT_NE(r->GetRetCode(), 0);
}

TEST_F(ConnectionForCopyTest, TestCopyTo) {
    // clean
    auto r = conn->Query("copy copy_test to 'test_out.csv'");
    ASSERT_EQ(r->GetRetCode(), 0);

    auto table = conn->GetTableInfo("copy_test");
    intarkdb::CSVReader reader("test_out.csv", Schema(std::string(table->GetTableName()), table->columns));
    ASSERT_EQ(reader.Open(), true);
    int count = 0;
    while (true) {
        const auto& [row, eof] = reader.ReadRecord();
        if (eof) {
            break;
        }
        count++;
    }
    EXPECT_EQ(count, 3);

    // test with delimiter
    r = conn->Query("copy copy_test to 'test_out2.csv' (DELIMITER '|')");
    ASSERT_EQ(r->GetRetCode(), 0);

    intarkdb::CSVReader reader2("test_out2.csv", Schema(std::string(table->GetTableName()), table->columns), '|');
    ASSERT_EQ(reader2.Open(), true);
    count = 0;
    while (true) {
        const auto& [row, eof] = reader2.ReadRecord();
        if (eof) {
            break;
        }
        count++;
    }
    EXPECT_EQ(count, 3);

    // test with header
    r = conn->Query("copy copy_test to 'test_out2.csv' (DELIMITER '|',HEADER)");
    // not suported yet
    ASSERT_NE(r->GetRetCode(), 0);

    // test select statement in copy to
    r = conn->Query("copy (select 42 as a , 'hello' as b ) to 'test_out3.csv' (DELIMITER '|')");
    ASSERT_EQ(r->GetRetCode(), 0);

    std::vector<Column> cols;
    cols.push_back(Column("a", intarkdb::INT32_DEF()));
    cols.push_back(Column("b", intarkdb::VARCHAR_DEF(20)));
    intarkdb::CSVReader reader3("test_out3.csv", Schema("test", cols), '|');
    ASSERT_EQ(reader3.Open(), true);
    count = 0;
    while (true) {
        const auto& [row, eof] = reader3.ReadRecord();
        if (eof) {
            break;
        }
        EXPECT_EQ(row.Field(0).GetCastAs<int>(), 42);
        EXPECT_EQ(row.Field(1).GetCastAs<std::string>(), "hello");
        count++;
    }
    EXPECT_EQ(count, 1);
}
