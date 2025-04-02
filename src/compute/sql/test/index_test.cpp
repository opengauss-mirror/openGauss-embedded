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
* index_test.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/test/index_test.cpp
*
* -------------------------------------------------------------------------
*/
#include <gtest/gtest.h>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "main/connection.h"
#include "main/database.h"

std::string tablename("index_normal_type_table");

class IndexTest : public ::testing::Test {
   protected:
    IndexTest() {}
    ~IndexTest() {}
    static void SetUpTestSuite() {
        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance->Init();
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();

        // sleep for a while to wait for db to start
        sleep(1);

        std::string query(
            fmt::format("CREATE TABLE {}(sid BIGINT, age INTEGER, name VARCHAR, sex BOOL, duty STRING, title VARCHAR, "
                        "grade SMALLINT, class INT4, rank INT, score REAL, is_grad BOOLEAN, school_make_up TINYINT, "
                        "mathscore FLOAT, englishscore FLOAT4, randnum1 INT8, randnum2 DOUBLE, course INT2, mastercnt "
                        "MEDIUMINT, grade_s DECIMAL(5,2), in_time DATE, start_time TIMESTAMP);",
                        tablename));
        std::cout << query << std::endl;
        conn->Query(query.c_str());
        std::cout << tablename << "create table success." << std::endl;
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

std::shared_ptr<IntarkDB> IndexTest::db_instance = nullptr;
std::unique_ptr<Connection> IndexTest::conn = nullptr;

// A predicate-formatter for asserting that two integers are mutually prime.
testing::AssertionResult AssertColTextTCmp(const char* m_expr, const char* n_expr, int m, int n) {
    if (m != n) return testing::AssertionFailure();

    if (strncmp(m_expr, n_expr, m) == 0)
        return testing::AssertionSuccess();
    else
        return testing::AssertionFailure();
}

TEST_F(IndexTest, CreateIndexUniqueSuccess) {
    std::string crt_idx1(fmt::format("CREATE UNIQUE INDEX idx_age_name on {} (age, name);", tablename));
    auto result = conn->Query(crt_idx1.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_EQ(table_info->GetIndexBySlot(0).Name(), "idx_age_name");
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_FALSE);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);
}

TEST_F(IndexTest, CreateIndexSuccess) {
    std::string crt_idx2(fmt::format("CREATE INDEX idx_name on {} (name);", tablename));
    auto result = conn->Query(crt_idx2.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetIndexCount(), 2);
    EXPECT_EQ(table_info->GetIndexBySlot(1).Name(), "idx_name");
    EXPECT_TRUE(table_info->GetIndexBySlot(1).GetIndex().is_primary == GS_FALSE);
    EXPECT_TRUE(table_info->GetIndexBySlot(1).GetIndex().is_unique == GS_FALSE);

    // not support other index type
    conn->Query("drop index idx_name");
    auto r = conn->Query(fmt::format("CREATE INDEX idx_name on {} using btree(name)", tablename).c_str());
    ASSERT_NE(r->GetRetCode(), 0);

    // using art(a) 等价于 on (a)
    conn->Query("drop index idx_name");
    r = conn->Query(fmt::format("CREATE INDEX idx_name on {} using art(name)", tablename).c_str());
    ASSERT_EQ(r->GetRetCode(), 0);
}

TEST_F(IndexTest, CreateIndexDuplicateName) {
    std::string indexname("du_idx_name");
    std::string crt_idx1(fmt::format("CREATE INDEX {} on {} (sid, name);", indexname, tablename));

    auto result = conn->Query(crt_idx1.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto result1 = conn->Query(crt_idx1.c_str());
    EXPECT_FALSE(result1->GetRetCode() == GS_SUCCESS);
    EXPECT_STREQ(result1->GetRetMsg().c_str(), fmt::format("The object index {} already exists.", indexname).c_str());
}

TEST_F(IndexTest, CreateIndexTableNotExist) {
    std::string indexname("du_idx_name2");
    std::string not_exist_table("not_exist_table");
    std::string crt_idx1(fmt::format("CREATE INDEX {} on {} (name);", indexname, not_exist_table));

    auto result = conn->Query(crt_idx1.c_str());
    EXPECT_FALSE(result->GetRetCode() == GS_SUCCESS);
    // EXPECT_STREQ(result->GetRetMsg().c_str(),
    //              fmt::format("The table or view SYS.{} does not exist.", not_exist_table).c_str());
}

TEST_F(IndexTest, CreateIndexColDuplicateIndexed) {
    std::string indexname("du_idx_name3");
    std::string crt_idx1(fmt::format("CREATE INDEX {} on {} (sid, age, name);", indexname, tablename));

    auto result = conn->Query(crt_idx1.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    std::string indexname4("du_idx_name4");
    std::string crt_idx2(fmt::format("CREATE INDEX {} on {} (sid, age, name);", indexname4, tablename));
    auto result1 = conn->Query(crt_idx2.c_str());
    EXPECT_FALSE(result1->GetRetCode() == GS_SUCCESS);
    EXPECT_STREQ(result1->GetRetMsg().c_str(), fmt::format("The column has been indexed by {}.", indexname).c_str());
}

TEST_F(IndexTest, CreateIndexWithoutIndexName) {
    std::string crt_idx1(fmt::format("CREATE INDEX on {} (sid, age, name);", tablename));
    auto result = conn->Query(crt_idx1.c_str());
    EXPECT_FALSE(result->GetRetCode() == GS_SUCCESS);
}

TEST_F(IndexTest, CreateIndexColumnNotExist) {
    std::string indexname("idx_not_e");
    std::string not_exist_col("not_exist_col");
    std::string ct_index(fmt::format("create index {} on {} ({});", indexname, tablename, not_exist_col));
    auto result = conn->Query(ct_index.c_str());
    EXPECT_FALSE(result->GetRetCode() == GS_SUCCESS);
    // EXPECT_STREQ(result->GetRetMsg().c_str(), fmt::format("The column SYS.{} does not exist.", not_exist_col).c_str());
}

TEST_F(IndexTest, CreateIndexUniqueOnOneColHavingMaxMin) {
    std::string tablepname("table_boundary1");
    std::string createtable(fmt::format(
        "create table {} (id integer, score integer, age integer DEFAULT 20, name varchar NOT NULL);", tablepname));
    std::string indexname("max_value_idx");

    auto result = conn->Query(createtable.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto result1 =
        conn->Query(fmt::format("insert into {} values(1, 99, 23, 'ruo'), (2, 98, 25, 'teng'), (3, 53, 31, 'ilike'), "
                                "(4, 32, 22, 'like'), (5, 55, 26, 'notlike'), (6, 78, 30, 'gob'), (7, 99, 21, 'gad'), "
                                "(8, 88, 28, 'gas'), (9,  79, 29, 'handle'), (10, 100, 26, 'kingseq'), (2147483647, "
                                "22, 22, 'max_value'), (-2147483647, 11, 11, 'min_value');",
                                tablepname)
                        .c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    std::string crt_idx1(fmt::format("create unique index {} on {} (id);", indexname, tablepname));
    auto result2 = conn->Query(crt_idx1.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);
    auto table_info = conn->GetTableInfo(tablepname);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_EQ(table_info->GetIndexBySlot(0).Name(), indexname);
}

TEST_F(IndexTest, CreateIndexUniqueAndSelect) {
    std::string tablepname("table_boundary2");
    std::string createtable(fmt::format(
        "create table {} (id integer, score integer, age integer DEFAULT 20, name varchar NOT NULL);", tablepname));
    std::string indexname("id_unique_idx");

    auto result = conn->Query(createtable.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto result1 =
        conn->Query(fmt::format("insert into {} values(1, 99, 23, 'ruo'), (2, 98, 25, 'teng'), (3, 53, 31, 'ilike'), "
                                "(4, 32, 22, 'like'), (5, 55, 26, 'notlike'), (6, 78, 30, 'gob'), (7, 99, 21, 'gad'), "
                                "(8, 88, 28, 'gas'), (9,  79, 29, 'handle'), (10, 100, 26, 'kingseq'), (2147483647, "
                                "22, 22, 'max_value'), (-2147483647, 11, 11, 'min_value');",
                                tablepname)
                        .c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    std::string crt_idx1(fmt::format("create unique index {} on {} (id);", indexname, tablepname));
    auto result2 = conn->Query(crt_idx1.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablepname);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_EQ(table_info->GetIndexBySlot(0).Name(), indexname);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_FALSE);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);

    auto result3 = conn->Query(fmt::format("select * from {} where id = 3;", tablepname).c_str());
    EXPECT_TRUE(result3->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(result3->RowCount(), 1);
}

TEST_F(IndexTest, CreateIndexBug709238858) {
    std::string table1name("table1");
    std::string table2name("test2");
    std::string createtable(fmt::format("create table {} (f1 int, f2 int);", table1name));
    std::string indexname("id_unique_idx");

    auto result = conn->Query(createtable.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto result1 = conn->Query(fmt::format("insert into {} values(1, 2);", table1name).c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto result2 = conn->Query(fmt::format("select * from {} ORDER BY f1;", table1name).c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(result2->RowCount(), 1);

    auto result3 = conn->Query(fmt::format("BEGIN;").c_str());
    EXPECT_TRUE(result3->GetRetCode() == GS_SUCCESS);

    auto result4 = conn->Query(fmt::format("DELETE FROM {};", table1name).c_str());
    EXPECT_TRUE(result4->GetRetCode() == GS_SUCCESS);

    auto result5 = conn->Query(fmt::format("INSERT INTO {} VALUES(1,1);", table1name).c_str());
    EXPECT_TRUE(result5->GetRetCode() == GS_SUCCESS);

    auto result6 = conn->Query(fmt::format("INSERT INTO {} VALUES(2,4);", table1name).c_str());
    EXPECT_TRUE(result6->GetRetCode() == GS_SUCCESS);

    auto result7 = conn->Query(fmt::format("CREATE TABLE {} (f1 int default 111,f2 real default -4.32,f3 text default "
                                           "'hi',f4 text default 'abc-123',f5 varchar(10));",
                                           table2name)
                                   .c_str());
    EXPECT_TRUE(result7->GetRetCode() == GS_SUCCESS);

    auto result8 = conn->Query(fmt::format("INSERT INTO {}(f2,f4) VALUES(-2.22,'hi!');", table2name).c_str());
    EXPECT_TRUE(result8->GetRetCode() == GS_SUCCESS);

    auto result9 = conn->Query(fmt::format("INSERT INTO {}(f1,f5) VALUES(1,'xyzzy');", table2name).c_str());
    EXPECT_TRUE(result9->GetRetCode() == GS_SUCCESS);

    std::string crt_idx1(fmt::format("CREATE INDEX index9 ON {}(f1,f2);", table2name));
    auto result10 = conn->Query(crt_idx1.c_str());
    EXPECT_TRUE(result10->GetRetCode() == GS_SUCCESS);

    std::string crt_idx2(fmt::format("CREATE INDEX indext ON {}(f4,f5);", table2name));
    auto result11 = conn->Query(crt_idx2.c_str());
    EXPECT_TRUE(result11->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(table2name);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetIndexCount(), 2);
    EXPECT_EQ(table_info->GetIndexBySlot(0).Name(), "index9");
    EXPECT_EQ(table_info->GetIndexBySlot(1).Name(), "indext");

    auto result12 = conn->Query(fmt::format("select * from {};", table2name).c_str());
    EXPECT_TRUE(result12->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(result12->RowCount(), 2);
}

// CREATE INDEX IF NOT EXISTS
TEST_F(IndexTest, CreateIndexBug709239415) {
    std::string tablepname("table_idx_ifnotexist");
    std::string createtable(fmt::format("create table {} (i INTEGER);", tablepname));
    std::string indexname("i_index");

    auto result = conn->Query(createtable.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    std::string crt_idx1(fmt::format("create index IF NOT EXISTS {} on {} (i);", indexname, tablepname));
    // first
    auto result2 = conn->Query(crt_idx1.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);
    // second
    auto result3 = conn->Query(crt_idx1.c_str());
    EXPECT_TRUE(result3->GetRetCode() == GS_SUCCESS);
    //
    std::string crt_idx2(fmt::format("create index {} on {} (i);", indexname, tablepname));
    auto result4 = conn->Query(crt_idx2.c_str());
    EXPECT_TRUE(result4->GetRetCode() == GS_ERROR);

    // third
    auto result5 = conn->Query(crt_idx1.c_str());
    EXPECT_TRUE(result5->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablepname);
    EXPECT_TRUE(table_info != nullptr);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_EQ(table_info->GetIndexBySlot(0).Name(), indexname);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_FALSE);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_FALSE);
}

// support index on tinyint , smallint , unsigned tinyint , unsigned smallint , uint64 
TEST_F(IndexTest, CreateIndexOnTinyInt) {
    auto result = conn->Query("create table table_tinyint (a TINYINT PRIMARY KEY, b SMALLINT unique, c utinyint , d usmallint , f uint64);");
    ASSERT_EQ(result->GetRetCode(), GS_SUCCESS);

    result = conn->Query("insert into table_tinyint values(1, 2, 3, 4, 5);");
    ASSERT_EQ(result->GetRetCode(), GS_SUCCESS);

    // insert same value to test unique 
    result = conn->Query("insert into table_tinyint values(0, 2, 3, 4, 5)");
    ASSERT_NE(result->GetRetCode(), GS_SUCCESS);
    result = conn->Query("insert into table_tinyint values(1, 3, 3, 4, 5)");
    ASSERT_NE(result->GetRetCode(), GS_SUCCESS);

    // insert max value
    result = conn->Query("insert into table_tinyint values(127, 32767, 255, 65535, 18446744073709551615);");
    ASSERT_EQ(result->GetRetCode(), GS_SUCCESS);


    // build index 
    result = conn->Query("create index utinyint_idx_a on table_tinyint (c);");
    ASSERT_EQ(result->GetRetCode(), GS_SUCCESS);

    result = conn->Query("create index usmallint_idx_d on table_tinyint (d);");
    ASSERT_EQ(result->GetRetCode(), GS_SUCCESS);

    result = conn->Query("create index uint64_idx_f on table_tinyint (f);");
    ASSERT_EQ(result->GetRetCode(), GS_SUCCESS);

    // create multi index
    result = conn->Query("create index multi_idx on table_tinyint (b, c, d, f);");
    ASSERT_EQ(result->GetRetCode(), GS_SUCCESS);

    // select 
    result = conn->Query("select * from table_tinyint where b = 2;");
    ASSERT_EQ(result->GetRetCode(), GS_SUCCESS);
    ASSERT_EQ(result->RowCount(), 1);
      
    result = conn->Query("select * from table_tinyint where c = 255;");
    ASSERT_EQ(result->GetRetCode(), GS_SUCCESS);
    ASSERT_EQ(result->RowCount(), 1);
}

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(output) = "xml";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
