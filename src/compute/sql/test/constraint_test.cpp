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
* constraint_test.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/test/constraint_test.cpp
*
* -------------------------------------------------------------------------
*/
// test for create table as select
#include <gtest/gtest.h>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "main/connection.h"
#include "main/database.h"

class ConstraintTest : public ::testing::Test {
 protected:
    ConstraintTest(){ 
        
    }
    ~ConstraintTest(){
        
    }
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
    static void TearDownTestSuite() {
        conn.reset();
    }
    
    void SetUp() override {
    }

    //void TearDown() override {}

    static std::shared_ptr<IntarkDB> db_instance;
    static std::unique_ptr<Connection> conn;
};

std::shared_ptr<IntarkDB> ConstraintTest::db_instance = nullptr;
std::unique_ptr<Connection> ConstraintTest::conn = nullptr;

TEST_F(ConstraintTest, TestConstraintNotNullInsert) {
    std::string select_table("table_not_null_cons_insert");
    std::string not_null_col("name");
    std::string create_stmt(fmt::format("create table {} (id integer, score integer, age integer DEFAULT 20, {} varchar NOT NULL);", select_table, not_null_col));
    std::cout << create_stmt << std::endl;

    auto result = conn->Query(create_stmt.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_SUCCESS);

    std::string insert_stmt(fmt::format("insert into {} values(1, 99, 23, NULL);", select_table));
    auto resultq = conn->Query(insert_stmt.c_str());
    EXPECT_FALSE(resultq->GetRetCode()==GS_SUCCESS);
    EXPECT_STREQ(resultq->GetRetMsg().c_str(), fmt::format("column ( {} ) not nullable!",not_null_col).c_str());

    auto result1 = conn->Query(fmt::format("select * from {};", select_table).c_str());
    EXPECT_TRUE(result1->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result1->RowCount(), 0);
}

TEST_F(ConstraintTest, TestConstraintNotNull) {
    std::string select_table("table_not_null_cons");
    std::string not_null_col("name");
    std::string create_stmt(fmt::format("create table if not exists {} (id integer, score integer, age integer DEFAULT 20, {} varchar NOT NULL);", select_table, not_null_col));
    std::cout << create_stmt << std::endl;

    auto result1 = conn->Query(create_stmt.c_str());
    EXPECT_TRUE(result1->GetRetCode()==GS_SUCCESS);

    std::string insert_stmt(fmt::format("insert into {} values(1, 99, 23, 'ruo');", select_table));
    auto result2 = conn->Query(insert_stmt.c_str());
    EXPECT_TRUE(result2->GetRetCode()==GS_SUCCESS);

    std::string update_stmt(fmt::format("UPDATE {} SET {} = NULL WHERE {} = 'ruo';", select_table, not_null_col, not_null_col));
    auto result3 = conn->Query(update_stmt.c_str());
    EXPECT_FALSE(result3->GetRetCode()==GS_SUCCESS);

    auto result = conn->Query(fmt::format("select * from {};", select_table).c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result->RowCount(), 1);
    EXPECT_STREQ(result->Row(0).Field(3).GetCastAs<std::string>().c_str(), "ruo");
}

TEST_F(ConstraintTest, TestConstraintNotNullCreate) {
    std::string select_table("table_notnull_cons_tb");
    std::string not_null_col("name");
    std::string create_stmt(fmt::format("create table if not exists {} (id integer, score integer, age integer DEFAULT 20, {} varchar NOT NULL);", select_table, not_null_col));
    std::cout << create_stmt << std::endl;
    auto result = conn->Query(create_stmt.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_SUCCESS);
}

TEST_F(ConstraintTest, TestConstraintNotNullAlter) {
    std::string select_table("table_notnull_alter_tb");
    std::string not_null_col("name");
    std::string create_stmt(fmt::format("create table if not exists {} (id integer, score integer, age integer DEFAULT 20, {} varchar NOT NULL);", select_table, not_null_col));
    auto result = conn->Query(create_stmt.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_SUCCESS);
    
    std::string alter_stmt(fmt::format("alter table {} add column_new INT NOT NULL;", select_table));
    auto result1 = conn->Query(alter_stmt.c_str());
    EXPECT_TRUE(result1->GetRetCode()==GS_SUCCESS);
}

int main(int argc, char **argv) {
    ::testing::GTEST_FLAG(output) = "xml";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}