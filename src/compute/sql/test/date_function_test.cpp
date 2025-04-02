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
 * select_test.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/test/select_test.cpp
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

#include "function/date/date_function.h"

class DateFunctionTest : public ::testing::Test {
   protected:
    DateFunctionTest() {}
    ~DateFunctionTest() {}
    static void SetUpTestSuite() {
        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance->Init();
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();

        // prepare data
        conn->Query("create table date_func_test(id int, d1 date, d2 timestamp)");
        conn->Query(
            "insert into date_func_test values(1,'2024-04-24','2024-04-28'), "
            "(2,'2024-04-24','2024-04-21'),(3,'2024-02-29','2024-02-28'),(4,'2024-05-31','2024-05-30');");
        conn->Query("update date_func_test set d2 = date_add(d2, interval 1 second)");
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

std::shared_ptr<IntarkDB> DateFunctionTest::db_instance = nullptr;
std::unique_ptr<Connection> DateFunctionTest::conn = nullptr;

// d1 - d2
TEST_F(DateFunctionTest, datediff) {
    auto r = conn->Query("select datediff('2024-5-22', '2024-5-20')");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d1 - d2
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 2);

    r = conn->Query("select datediff('2024-5-20', '2024-5-22')");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d1 - d2
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), -2);

    r = conn->Query("select datediff(d1, d2) from date_func_test");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d1 - d2
    EXPECT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), -4);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 1);

    r = conn->Query("select datediff(d2, d1) from date_func_test");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d1 - d2
    EXPECT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), -3);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), -1);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), -1);
}

// d2 - d1
TEST_F(DateFunctionTest, timestampdiff_year) {
    auto r = conn->Query("select timestampdiff('year', '2024-5-20', '2025-5-20')");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d2 - d1
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);

    r = conn->Query("select timestampdiff('year', '2024-5-20', '2025-5-20 00:00:00.000001')");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d2 - d1
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);

    r = conn->Query("select timestampdiff('year', '2024-5-20 00:00:00.000001', '2025-5-20')");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d2 - d1
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 0);

    // ----------------------------------------------------------------------------------- //

    r = conn->Query("select timestampdiff('year', '2025-5-20', '2024-5-20')");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d2 - d1
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), -1);

    r = conn->Query("select timestampdiff('year', '2025-5-20', '2024-5-20 00:00:00.000001')");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d2 - d1
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 0);

    r = conn->Query("select timestampdiff('year', '2025-5-20 00:00:00.000001', '2024-5-20')");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d2 - d1
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), -1);
}

TEST_F(DateFunctionTest, timestampdiff_month) {
    auto r = conn->Query("select timestampdiff('MONTH', '1991-03-28', '2000-02-29') as a;");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d2 - d1
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 107);
}

TEST_F(DateFunctionTest, date_add_year) {
    auto r = conn->Query("select date_add('2024-2-29', interval 1 year)");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d1 + interval
    EXPECT_EQ(r->RowCount(), 1);
    auto date = r->Row(0).Field(0).GetCastAs<timestamp_stor_t>();
    date_detail_t detail;
    date.ts += TIMEZONE_GET_MICROSECOND(cm_get_db_timezone());
    date.ts += CM_UNIX_EPOCH;
    cm_decode_date(date.ts, &detail);
    EXPECT_EQ(detail.year, 2025);
    EXPECT_EQ(detail.mon, 2);
    EXPECT_EQ(detail.day, 28);
}

TEST_F(DateFunctionTest, date_add_quarter) {
    auto r = conn->Query("select date_add('2024-1-31', interval 1 quarter)");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d1 + interval
    EXPECT_EQ(r->RowCount(), 1);
    auto date = r->Row(0).Field(0).GetCastAs<timestamp_stor_t>();
    date_detail_t detail;
    date.ts += TIMEZONE_GET_MICROSECOND(cm_get_db_timezone());
    date.ts += CM_UNIX_EPOCH;
    cm_decode_date(date.ts, &detail);
    EXPECT_EQ(detail.year, 2024);
    EXPECT_EQ(detail.mon, 4);
    EXPECT_EQ(detail.day, 30);
}

TEST_F(DateFunctionTest, date_add_month) {
    auto r = conn->Query("select date_add('2024-1-31', interval 1 month)");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d1 + interval
    EXPECT_EQ(r->RowCount(), 1);
    auto date = r->Row(0).Field(0).GetCastAs<timestamp_stor_t>();
    date_detail_t detail;
    date.ts += TIMEZONE_GET_MICROSECOND(cm_get_db_timezone());
    date.ts += CM_UNIX_EPOCH;
    cm_decode_date(date.ts, &detail);
    EXPECT_EQ(detail.year, 2024);
    EXPECT_EQ(detail.mon, 2);
    EXPECT_EQ(detail.day, 29);
}

TEST_F(DateFunctionTest, date_sub_month) {
    auto r = conn->Query("select date_sub('2024-3-31', interval 13 month)");
    ASSERT_EQ(r->GetRetCode(), 0);
    // d1 - interval
    EXPECT_EQ(r->RowCount(), 1);
    auto date = r->Row(0).Field(0).GetCastAs<timestamp_stor_t>();
    date_detail_t detail;
    date.ts += TIMEZONE_GET_MICROSECOND(cm_get_db_timezone());
    date.ts += CM_UNIX_EPOCH;
    cm_decode_date(date.ts, &detail);
    EXPECT_EQ(detail.year, 2023);
    EXPECT_EQ(detail.mon, 2);
    EXPECT_EQ(detail.day, 28);
}