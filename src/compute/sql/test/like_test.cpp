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
 * like_test.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/test/like_test.cpp
 *
 * -------------------------------------------------------------------------
 */
// test for like operator
#include <gtest/gtest.h>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "main/connection.h"
#include "main/database.h"

std::string tablename("like_test_table");
class LikeOpTest : public ::testing::Test {
   protected:
    LikeOpTest() {}
    ~LikeOpTest() {}
    static void SetUpTestSuite() {
        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance->Init();
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();

        conn->Query(
            fmt::format("create table {} (id integer, score integer, age integer, name varchar);", tablename).c_str());
        conn->Query(fmt::format("insert into {} values(1, 99, 25, 'ruo'),(2, 78, 23,'peo'),(3, 87, 24, 'erw'),(4, 99, "
                                "23, 'ads'),(5, 100, 28, 'pp%c');",
                                tablename)
                        .c_str());
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

std::shared_ptr<IntarkDB> LikeOpTest::db_instance = nullptr;
std::unique_ptr<Connection> LikeOpTest::conn = nullptr;
// like
TEST_F(LikeOpTest, SelectLikeStartPercentageColVarchar) {
    // like expression , op name = "~~"
    std::string query(fmt::format("select * from {} where name like '%o';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 2);

    // not like expression , op name = "!~~"
    result = conn->Query("select * from like_test_table where name not like '%o'");
    ASSERT_EQ(result->GetRetCode(), 0);
    EXPECT_EQ(result->RowCount(), 3);
}

TEST_F(LikeOpTest, SelectLikeStartPercentageColInteger) {
    // like 也可以作用在integer类型上
    std::string query(fmt::format("select * from {} where age like '%5';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 1);
}

TEST_F(LikeOpTest, SelectLikeOnlyPercentage1) {
    std::string query(fmt::format("select * from {} where age like '%';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 5);
}

TEST_F(LikeOpTest, SelectLikeOnlyPercentage2) {
    std::string query(fmt::format("select * from {} where age like '%%';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 5);
}

TEST_F(LikeOpTest, SelectLikeMiddlePercentageColVarchar) {
    std::string query(fmt::format("select * from {} where name like 'r%o';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 1);
}

TEST_F(LikeOpTest, SelectLikeEndPercentageColVarchar) {
    std::string query(fmt::format("select * from {} where name like 'p%';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 2);
}

TEST_F(LikeOpTest, SelectLikeStartAndEndPercentageColVarchar) {
    std::string query(fmt::format("select * from {} where name like '%p%';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 2);
}

TEST_F(LikeOpTest, SelectLikeNoPercentageColVarchar1) {
    std::string query(fmt::format("select * from {} where name like 'p';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 0);
}

TEST_F(LikeOpTest, SelectLikeNoPercentageColVarchar2) {
    std::string query(fmt::format("select * from {} where name like 'peo';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 1);
}

TEST_F(LikeOpTest, SelectLikeNoPercentageUpLowCase) {
    std::string query(fmt::format("select * from {} where name like '%O';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 0);
}

TEST_F(LikeOpTest, SelectLikeUnderscore1) {
    std::string query(fmt::format("select * from {} where name like '%_';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 5);
}

TEST_F(LikeOpTest, SelectLikeUnderscore2) {
    std::string query(fmt::format("select * from {} where name like '%e_';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 1);
}

TEST_F(LikeOpTest, SelectLikeUnderscore3) {
    std::string query(fmt::format("select * from {} where name like 'r__';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 1);
}
// not like
TEST_F(LikeOpTest, SelectNotLikeStartPercentageColVarchar) {
    std::string query(fmt::format("select * from {} where name not like '%o';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 3);
}

TEST_F(LikeOpTest, SelectNotLikeStartPercentageColInteger) {
    std::string query(fmt::format("select * from {} where age not like '%5';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 4);
}

TEST_F(LikeOpTest, SelectNotLikeOnlyPercentage1) {
    std::string query(fmt::format("select * from {} where age not like '%';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 0);
}

TEST_F(LikeOpTest, SelectNotLikeOnlyPercentage2) {
    std::string query(fmt::format("select * from {} where age not like '%%';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 0);
}

TEST_F(LikeOpTest, SelectNotLikeMiddlePercentageColVarchar) {
    std::string query(fmt::format("select * from {} where name not like 'r%o';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 4);
}

TEST_F(LikeOpTest, SelectNotLikeEndPercentageColVarchar) {
    std::string query(fmt::format("select * from {} where name not like 'p%';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 3);
}

TEST_F(LikeOpTest, SelectNotLikeStartAndEndPercentageColVarchar) {
    std::string query(fmt::format("select * from {} where name not like '%p%';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 3);
}

TEST_F(LikeOpTest, SelectNotLikeNoPercentageColVarchar1) {
    std::string query(fmt::format("select * from {} where name not like 'p';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 5);
}

TEST_F(LikeOpTest, SelectNotLikeNoPercentageColVarchar2) {
    std::string query(fmt::format("select * from {} where name not like 'peo';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 4);
}

TEST_F(LikeOpTest, SelectNotLikeUpLowCase) {
    std::string query(fmt::format("select * from {} where name not like '%O';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 5);
}

TEST_F(LikeOpTest, SelectNotLikeUnderscore1) {
    std::string query(fmt::format("select * from {} where name not like '%_';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 0);
}

TEST_F(LikeOpTest, SelectNotLikeUnderscore2) {
    std::string query(fmt::format("select * from {} where name not like '%e_';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 4);
}

TEST_F(LikeOpTest, SelectNotLikeUnderscore3) {
    std::string query(fmt::format("select * from {} where name not like 'r__';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 4);
}
// ilike
TEST_F(LikeOpTest, SelectILikeUpLowCase) {
    // ilike expression , op name = "~~*"
    // ilike 与 like 的区别在于，ilike不区分大小写
    std::string query(fmt::format("select * from {} where name ilike '%O';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 2);
}

TEST_F(LikeOpTest, SelectILikeUnderscore1) {
    std::string query(fmt::format("select * from {} where name ilike '%_';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 5);
}

TEST_F(LikeOpTest, SelectILikeUnderscore2) {
    std::string query(fmt::format("select * from {} where name ilike '%E_';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 1);
}

TEST_F(LikeOpTest, SelectILikeUnderscore3) {
    std::string query(fmt::format("select * from {} where name ilike 'R__';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 1);
}
// not ilike
TEST_F(LikeOpTest, SelectNotILikeUpLowCase) {
    std::string query(fmt::format("select * from {} where name not ilike '%O';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 3);
}

TEST_F(LikeOpTest, SelectNotILikeUnderscore1) {
    std::string query(fmt::format("select * from {} where name not ilike '%_';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 0);
}

TEST_F(LikeOpTest, SelectNotILikeUnderscore2) {
    std::string query(fmt::format("select * from {} where name not ilike '%E_';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 4);
}

TEST_F(LikeOpTest, SelectNotILikeUnderscore3) {
    std::string query(fmt::format("select * from {} where name not ilike 'R__';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 4);
}
// like escape
TEST_F(LikeOpTest, SelectLikeEscape1) {
    // 带 escape 时，不再是 like expression , 而是 like function ， 函数名为 like_escape
    std::string query(fmt::format("select * from {} where name like '%&%%' escape '&';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 1);
}

TEST_F(LikeOpTest, SelectLikeEscape2) {
    std::string query(fmt::format("select * from {} where name like 'pp&%c' escape '&';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 1);
}

TEST_F(LikeOpTest, SelectLikeEscape3) {
    std::string query(fmt::format("select * from {} where name like '&' escape '&';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_FALSE(result->GetRetCode() == GS_SUCCESS);
    ASSERT_STREQ(result->GetRetMsg().c_str(), fmt::format("Like pattern must not end with escape character!").c_str());
}

// not like escape
TEST_F(LikeOpTest, SelectNotLikeEscape1) {
    // like function not like escape
    std::string query(fmt::format("select * from {} where name not like '%&%%' escape '&';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 4);
}

TEST_F(LikeOpTest, SelectNotLikeEscape2) {
    std::string query(fmt::format("select * from {} where name not like 'pp&%c' escape '&';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 4);
}

TEST_F(LikeOpTest, SelectNotLikeEscape3) {
    std::string query(fmt::format("select * from {} where name not like '&' escape '&';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_FALSE(result->GetRetCode() == GS_SUCCESS);
    ASSERT_STREQ(result->GetRetMsg().c_str(), fmt::format("Like pattern must not end with escape character!").c_str());
}

// ilike escape
TEST_F(LikeOpTest, SelectILikeEscape1) {
    std::string query(fmt::format("select * from {} where name ilike '%&%%' escape '&';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 1);
}

TEST_F(LikeOpTest, SelectILikeEscape2) {
    std::string query(fmt::format("select * from {} where name ilike 'pp&%c' escape '&';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 1);
}

TEST_F(LikeOpTest, SelectILikeEscape3) {
    std::string query(fmt::format("select * from {} where name ilike '&' escape '&';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_FALSE(result->GetRetCode() == GS_SUCCESS);
    ASSERT_STREQ(result->GetRetMsg().c_str(), fmt::format("Like pattern must not end with escape character!").c_str());
}

// not ilike escape
TEST_F(LikeOpTest, SelectNotILikeEscape1) {
    std::string query(fmt::format("select * from {} where name not ilike '%&%%' escape '&';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 4);
}

TEST_F(LikeOpTest, SelectNotILikeEscape2) {
    std::string query(fmt::format("select * from {} where name not ilike 'pp&%c' escape '&';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    EXPECT_EQ(result->RowCount(), 4);
}

TEST_F(LikeOpTest, SelectNotILikeEscape3) {
    std::string query(fmt::format("select * from {} where name not ilike '&' escape '&';", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_FALSE(result->GetRetCode() == GS_SUCCESS);
    ASSERT_STREQ(result->GetRetMsg().c_str(), fmt::format("Like pattern must not end with escape character!").c_str());
}

TEST_F(LikeOpTest, SelectLikeEscape4) {
    auto r = conn->Query("select 'a%c' ilike 'a$%C' escape '$'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("select 'A%c' ilike 'a$%c' escape '$'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("select 'a%c' ilike 'a$%C' escape '/'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("select null ilike 'a$%C' escape '/'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("select 'a%c' ilike null escape '$'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("select 'a%c' ilike 'a$%c' escape null");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    // test like with utf-8
    r = conn->Query("select 'twoñthree₡four🦆end' ilike 'two$%three$%four$%end$%' escape '$'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("select 'twoñthree₡four🦆end' ilike '%🦆%' escape '/'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("select 'twoñthree₡four🦆end' ilike '%🦆%' escape '🦆'");
    ASSERT_NE(r->GetRetCode(), 0);

    // more utf-8 characters
    r = conn->Query("select '你好世界' like '你好%'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("select '你好世界' like '%世界'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    conn->Query("drop table if exists tbl");
    conn->Query("create table tbl (str varchar,pat varchar)");
    conn->Query("insert into tbl values ('a%c','a$%C')");
    r = conn->Query("select str ilike pat escape '$' from tbl");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("select str not ilike pat escape '$' from tbl");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("select null ilike pat escape '$' from tbl");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("select str ilike null escape '$' from tbl");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    r = conn->Query("select str ilike pat escape null from tbl");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);

    // mutil byte escape not supported
    r = conn->Query("select 'a%c' ilike 'a$%C' escape '///'");
    ASSERT_NE(r->GetRetCode(), 0);

    r = conn->Query("select str ilike pat escape str from tbl");
    ASSERT_NE(r->GetRetCode(), 0);

    r = conn->Query("select '%' like '%' escape '%'");  // like pattern must not end with escape character
    ASSERT_NE(r->GetRetCode(), 0);
}

TEST_F(LikeOpTest, BaseTest) {
    auto r = conn->Query("select 'abc' like 'abc'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);
    r = conn->Query("select 'abc' like 'a%'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);
    r = conn->Query("select 'abc' like '_b_'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);
    r = conn->Query("select 'abc' like 'c'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);
    r = conn->Query("select 'abc' like 'c%'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);
    r = conn->Query("select 'abc' like '%c'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);
    r = conn->Query("select 'abc' not like '%c'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("select 'a%c' like 'a$%c' escape '$'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);
    r = conn->Query("select 'azc' like 'a$%c' escape '$'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);
    r = conn->Query("select 'A%c' ilike 'a$%c' escape '$'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    // like 中含有一些特殊字符(对于正则表达式来说)
    r = conn->Query("select 'a.c' like 'a.c'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);
    r = conn->Query("select '[abc]' like '[%c]'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);
}

TEST_F(LikeOpTest, ILikeTestWithUnicodeCase) {
    auto r = conn->Query("select '你好世界' ilike '你好%'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("select '你好世界' ilike '%世界'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("select 'MÜHLEISEN' ILIKE 'mühleisen'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("select 'MÜHLEISEN' LIKE 'mühleisen'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);
}

TEST_F(LikeOpTest, LikeTestWithSpeificEscape) {
    auto r = conn->Query(R"(select '\' LIKE '\\' ESCAPE '\' )");  // 使用R"()"来避免转义
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query(R"(SELECT '\\' LIKE '\\' ESCAPE '\')");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);

    r = conn->Query("SELECT '%++' LIKE '*%++' ESCAPE '*'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    // SELECT '%++' NOT LIKE '*%++' ESCAPE '*';
    r = conn->Query("SELECT '%++' NOT LIKE '*%++' ESCAPE '*'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);

    // SELECT '%' LIKE '%' ESCAPE '%';
    r = conn->Query("SELECT '%' LIKE '%' ESCAPE '%'");
    ASSERT_NE(r->GetRetCode(), 0);

    // SELECT '%' LIKE '*' ESCAPE '*';
    r = conn->Query("SELECT '%' LIKE '*' ESCAPE '*'");
    ASSERT_NE(r->GetRetCode(), 0);

    // SELECT '%' LIKE '*%' ESCAPE '*';
    r = conn->Query("SELECT '%' LIKE '*%' ESCAPE '*'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("SELECT '_ ' LIKE '*_ ' ESCAPE '*'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    r = conn->Query("SELECT ' a ' LIKE '*_ ' ESCAPE '*'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), false);

    // SELECT '%_' LIKE '%_' ESCAPE '';
    r = conn->Query("SELECT '%_' LIKE '%_' ESCAPE ''");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    // SELECT '*%' NOT LIKE '*%' ESCAPE '*';
    r = conn->Query("SELECT '*%' NOT LIKE '*%' ESCAPE '*'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<bool>(), true);

    // escape 多于一个字符
    r = conn->Query(R"(SELECT '%_' LIKE '%_' ESCAPE '\\')");
    ASSERT_NE(r->GetRetCode(), 0);

    // SELECT '%_' LIKE '%_' ESCAPE '**';
    r = conn->Query("SELECT '%_' LIKE '%_' ESCAPE '**'");
    ASSERT_NE(r->GetRetCode(), 0);

    conn->Query("drop table if exists strings");
    conn->Query("CREATE TABLE strings(s STRING, pat STRING)");
    conn->Query("INSERT INTO strings VALUES ('abab', 'ab%'), ('aaa', 'a*_a'), ('aaa', '*%b'), ('bbb', 'a%')");
    r = conn->Query("SELECT s FROM strings");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "abab");
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<std::string>(), "aaa");
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<std::string>(), "aaa");
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<std::string>(), "bbb");

    r = conn->Query("SELECT pat FROM strings");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "ab%");
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<std::string>(), "a*_a");
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<std::string>(), "*%b");
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<std::string>(), "a%");

    // SELECT s FROM strings WHERE pat LIKE 'a*%' ESCAPE '*';
    r = conn->Query("SELECT s FROM strings WHERE pat LIKE 'a*%' ESCAPE '*'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "bbb");

    // SELECT s FROM strings WHERE 'aba' LIKE pat ESCAPE '*';
    r = conn->Query("SELECT s FROM strings WHERE 'aba' LIKE pat ESCAPE '*'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 2);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "abab");
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<std::string>(), "bbb");

    // SELECT s FROM strings WHERE s LIKE pat ESCAPE '*';
    r = conn->Query("SELECT s FROM strings WHERE s LIKE pat ESCAPE '*'");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<std::string>(), "abab");

    r = conn->Query("select 'a' like 'a' escape NULL");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->Row(0).Field(0).IsNull(), true);
}

TEST_F(LikeOpTest, LikeTestWithAlias) {
    auto r = conn->Query(
        R"(SELECT NULL AS "TABLE_CAT", NULL AS "TABLE_SCHEM", NAME AS "TABLE_NAME", TYPE AS "TABLE_TYPE" 
        FROM ( SELECT NAME, 'TABLE' AS TYPE FROM "SYS"."SYS_TABLES" WHERE "SPACE#"=3 
        UNION ALL 
        SELECT NAME, 'VIEW' AS TYPE FROM "SYS"."SYS_VIEWS" ) 
        WHERE "TABLE_NAME" LIKE '%' AND "TABLE_TYPE" IN ('TABLE','VIEW') ORDER BY "TABLE_TYPE", "TABLE_NAME")");
    ASSERT_EQ(r->GetRetCode(), 0);
}

int main(int argc, char **argv) {
    ::testing::GTEST_FLAG(output) = "xml";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
