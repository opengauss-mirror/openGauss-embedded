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
* tsl_slm_test.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/test/tsl_slm_test.cpp
*
* -------------------------------------------------------------------------
*/
#include <gtest/gtest.h>

#include <ctime>
#include <iomanip>
#include <iostream>
#include <string>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "main/connection.h"
#include "main/database.h"

class TslSlmTest : public ::testing::Test {
   protected:
    TslSlmTest() {}
    ~TslSlmTest() {}
    static void SetUpTestSuite() {
        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance->Init();
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();

        // sleep for a while to wait for db to start
        sleep(1);
        // // prepare data
        conn->Query("drop table temm_ts_table;");
        conn->Query("drop table temm_ts_table_hour;");

        conn->Query(
            "create TABLE temm_ts_table(date timestamp, id int, value int) PARTITION BY RANGE(date)  TIMESCALE "
            "INTERVAL '1d' RETENTION '30d' AUTOPART;");
        conn->Query("ALTER TABLE temm_ts_table ADD PARTITION temm_ts_table_20230905;");

        conn->Query(
            "create TABLE temm_ts_table_hour(date timestamp, id int, value int) PARTITION BY RANGE(date)  TIMESCALE "
            "INTERVAL '1h' RETENTION '5h' AUTOPART;");
        conn->Query("ALTER TABLE temm_ts_table_hour ADD PARTITION temm_ts_table_hour_2023090501;");
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

std::shared_ptr<IntarkDB> TslSlmTest::db_instance = nullptr;
std::unique_ptr<Connection> TslSlmTest::conn = nullptr;

TEST_F(TslSlmTest, CheckValidTablePart) {
    sleep(360);
    time_t now = time(NULL);
    struct tm * local_tm = localtime(&now);
    char date[16] = {0};
    sprintf_s(date, 16, "%d%02d%02d", local_tm->tm_year + 1900, local_tm->tm_mon + 1, local_tm->tm_mday);
    std::string s_date = date;
    std::string query_sql = "select * from \"SYS_TABLE_PARTS\" where \"NAME\" ='temm_ts_table_" + s_date + "'";
    printf("query_sql = %s\n", query_sql.c_str());
    auto r = conn->Query(query_sql.c_str());
    printf("retcode= %d, msg = %s,count=%d \n", r->GetRetCode(), r->GetRetMsg().c_str(), r->RowCount()); 
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);

    sprintf_s(date, 16, "%d-%02d-%02d", local_tm->tm_year + 1900, local_tm->tm_mon + 1, local_tm->tm_mday);
    std::string s_insert_date = date;
    std::string insert_sql =
        "INSERT INTO temm_ts_table (date,id,value) VALUES ('" + s_insert_date + " 10:00:01',1234,5678)";
    printf("insert_sql = %s\n", insert_sql.c_str());
    r = conn->Query(insert_sql.c_str());
    printf("retcode= %d, msg = %s,count=%d \n", r->GetRetCode(), r->GetRetMsg().c_str(), r->GetEffectRow());
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->GetEffectRow(), 1);

    query_sql = "select * from temm_ts_table";
    printf("query_sql = %s\n", query_sql.c_str());
    r = conn->Query(query_sql.c_str());
    printf("retcode= %d, msg = %s,count=%d \n", r->GetRetCode(), r->GetRetMsg().c_str(), r->RowCount());
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);

    sprintf_s(date, 16, "%d%02d%02d%02d", local_tm->tm_year + 1900, local_tm->tm_mon + 1, local_tm->tm_mday, local_tm->tm_hour);
    s_date = date;
    query_sql = "select * from \"SYS_TABLE_PARTS\" where \"NAME\" >='temm_ts_table_hour_" + s_date + "'";
    printf("query_sql = %s\n", query_sql.c_str());
    r = conn->Query(query_sql.c_str());
    printf("retcode= %d, msg = %s,count=%d \n", r->GetRetCode(), r->GetRetMsg().c_str(), r->RowCount());
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
}

TEST_F(TslSlmTest, CheckInValidTablePart) {
    std::string query_sql ="select * from \"SYS_TABLE_PARTS\" where \"NAME\" ='temm_ts_table_20230905' ;";
    auto r = conn->Query(query_sql.c_str());
    printf("retcode= %d, msg = %s,count=%d, sql = %s \n", r->GetRetCode(), r->GetRetMsg().c_str(), r->RowCount(), query_sql.c_str());
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);

    query_sql = "select * from \"SYS_TABLE_PARTS\" where \"NAME\" ='temm_ts_table_hour_2023090501' ;";
    r = conn->Query(query_sql.c_str());
    printf("retcode= %d, msg = %s,count=%d, sql = %s \n", r->GetRetCode(), r->GetRetMsg().c_str(), r->RowCount(),
           query_sql.c_str());
    EXPECT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 0);
}

int main(int argc, char** argv) {
    system("rm -rf intarkdb/");
    ::testing::GTEST_FLAG(output) = "xml";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
