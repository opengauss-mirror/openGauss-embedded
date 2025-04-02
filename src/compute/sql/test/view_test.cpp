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
* view_test.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/test/view_test.cpp
*
* -------------------------------------------------------------------------
*/
#include <gtest/gtest.h>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "main/connection.h"
#include "main/database.h"

class ViewTest : public ::testing::Test {
   protected:
    ViewTest() {}
    ~ViewTest() {}
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

std::shared_ptr<IntarkDB> ViewTest::db_instance = nullptr;
std::unique_ptr<Connection> ViewTest::conn = nullptr;

TEST_F(ViewTest, CreateViewWithUnion) {
    auto r = conn->Query("create view v1 as select 1 union select 2");
    EXPECT_EQ(r->GetRetCode(), 0);
}

// CREATE OR REPLACE VIEW
TEST_F(ViewTest, CreateViewBug709565416) {
    std::string viewname("table_replace");
    std::string query(fmt::format("CREATE OR REPLACE VIEW {} as select 1;", viewname));
    std::cout << query << std::endl;
    // first
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_ERROR);
}

TEST_F(ViewTest, CreateViewWithParams) {
    auto result = conn->Query("CREATE VIEW view_with_params as select 1,?");
    ASSERT_NE(result->GetRetCode(), 0);
    // create table
    conn->Query("drop table if exists t1");
    conn->Query("create table t1 (a int)");
    result = conn->Query("create view view_with_alias(x) as select a from t1");
    ASSERT_NE(result->GetRetCode(), 0);  // not support alias
    result = conn->Query("CREATE VIEW view_with_params as select 1,?");
    ASSERT_NE(result->GetRetCode(), 0);  // not support params
    result = conn->Query("create view view_with_params as select * from t1 where a = ?");
    ASSERT_NE(result->GetRetCode(), 0);  // not support params
}
