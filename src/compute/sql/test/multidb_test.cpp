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
* multidb_test.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/test/multidb_test.cpp
*
* -------------------------------------------------------------------------
*/
#include <gtest/gtest.h>

#include <ctime>
#include <iomanip>
#include <iostream>
#include <thread>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "main/connection.h"
#include "main/database.h"

class ConnectionForTest : public ::testing::Test {
   protected:
    ConnectionForTest() {}
    ~ConnectionForTest() {}
    static void SetUpTestSuite() {}

    // Per-test-suite tear-down.
    // Called after the last test in this test suite.
    // Can be omitted if not needed.
    static void TearDownTestSuite() {}

    void SetUp() override {}

    // void TearDown() override {}
};

TEST_F(ConnectionForTest, MulitDataBase) {
    auto instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
    auto conn = std::make_unique<Connection>(instance);
    conn->Init();
    {
        // release the db instance
        auto instance2 = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
    }
    auto r = conn->Query("show tables");
    ASSERT_EQ(r->GetRetCode(), 0);
}

TEST_F(ConnectionForTest, ReOpenDB) {
    {
        auto instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        auto connection = std::make_unique<Connection>(instance);
        connection->Init();
        auto r = connection->Query("show tables");
        ASSERT_EQ(r->GetRetCode(), 0);
    }
    {
        auto instance2 = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        auto connection = std::make_unique<Connection>(instance2);
        connection->Init();
        auto r = connection->Query("show tables");
        ASSERT_EQ(r->GetRetCode(), 0);
    }

    auto instance = IntarkDB::GetInstance("./");
    auto connection = std::make_unique<Connection>(instance);
    connection->Init();
    {
        auto instance2 = IntarkDB::GetInstance("./");
        auto connection2 = std::make_unique<Connection>(instance2);
    }
    auto r = connection->Query("show tables");
    ASSERT_EQ(r->GetRetCode(),0);
    // reopen ? 
    auto intance3 = IntarkDB::GetInstance("./");
    auto connection3 = std::make_unique<Connection>(intance3);
    connection3->Init();
    auto r2 = connection3->Query("show tables");
    ASSERT_EQ(r2->GetRetCode(), 0);
}

void thread_func() {
    auto instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
    auto connection = std::make_unique<Connection>(instance);
    connection->Init();
    auto r = connection->Query("show tables");
    ASSERT_EQ(r->GetRetCode(), 0);
}

TEST_F(ConnectionForTest, MultiThreadOpenSameDB) {
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++) {
        threads.emplace_back(thread_func);
    }
    for (auto& t : threads) {
        t.join();
    }
}

TEST_F(ConnectionForTest , SelectDoNothing) {
    // 测试 parse 不调用任何sql时，是否能正确释放
    auto instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
    auto conn2 = std::make_unique<Connection>(instance);
    conn2->Init();
}
