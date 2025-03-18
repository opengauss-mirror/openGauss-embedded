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
 * c_api_test.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/test/c_api_test.cpp
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

#include "interface/c/intarkdb_sql.h"

class CApiTest : public ::testing::Test {
   protected:
    CApiTest() {}
    ~CApiTest() {}
    static void SetUpTestSuite() {
        intarkdb_result = intarkdb_init_result();
        char path[1024] = "./";
        intarkdb_open(path, &db);
        intarkdb_connect(db, &conn);

        // prepare data
        std::string sql = "CREATE TABLE IF NOT EXISTS CAPI_TEST_TABLE(LOGID BIGINT, ACCTID INTEGER, \
                        CHGAMT VARCHAR(20), BAK1 VARCHAR(20), CHGTIME TIMESTAMP, BIN_TMP BLOB)";
        intarkdb_query(conn, sql.c_str(), intarkdb_result);
    }

    // Per-test-suite tear-down.
    // Called after the last test in this test suite.
    // Can be omitted if not needed.
    static void TearDownTestSuite() {}

    void SetUp() override {}

    // void TearDown() override {}

    static intarkdb_database db;
    static intarkdb_connection conn;
    static intarkdb_result intarkdb_result;
};

intarkdb_database CApiTest::db = nullptr;
intarkdb_connection CApiTest::conn = nullptr;
intarkdb_result CApiTest::intarkdb_result = nullptr;

// insert
TEST_F(CApiTest, intarkdb_query_insert) {
    std::string sql = "INSERT INTO CAPI_TEST_TABLE(LOGID,ACCTID,CHGAMT,BAK1,CHGTIME) \
            VALUES (-1,11,'AAA','bak1','2023-8-1'), (-2,22,'BBB','bak1','2023-8-2')";
    auto r = intarkdb_query(conn, sql.c_str(), intarkdb_result);
    std::string errmsg = intarkdb_result_msg(intarkdb_result);
    ASSERT_EQ(r, SQL_SUCCESS);
    EXPECT_EQ(errmsg, "Sql success!");
}

// select
TEST_F(CApiTest, intarkdb_query_select) {
    std::string sql = "select * from CAPI_TEST_TABLE";
    auto r = intarkdb_query(conn, sql.c_str(), intarkdb_result);
    std::string errmsg = intarkdb_result_msg(intarkdb_result);
    ASSERT_EQ(r, SQL_SUCCESS);
    EXPECT_EQ(intarkdb_row_count(intarkdb_result), 2);
    EXPECT_EQ(errmsg, "Sql success!");
}

// 

