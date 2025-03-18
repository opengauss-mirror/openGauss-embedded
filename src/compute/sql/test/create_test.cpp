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
* create_test.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/test/create_test.cpp
*
* -------------------------------------------------------------------------
*/
#include <gtest/gtest.h>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "common/default_value.h"
#include "main/connection.h"
#include "main/database.h"

class ConnectionTest : public ::testing::Test {
   protected:
    ConnectionTest() {}
    ~ConnectionTest() {}
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

std::shared_ptr<IntarkDB> ConnectionTest::db_instance = nullptr;
std::unique_ptr<Connection> ConnectionTest::conn = nullptr;

// A predicate-formatter for asserting that two integers are mutually prime.
testing::AssertionResult AssertColTextTCmp(const char* m_expr, const char* n_expr, int m, int n) {
    if (m != n) return testing::AssertionFailure();

    if (strncmp(m_expr, n_expr, m) == 0)
        return testing::AssertionSuccess();
    else
        return testing::AssertionFailure();
}

TEST_F(ConnectionTest, TypeTest1CreateTable) {
    std::string tablename("crt_normal_type_table");
    std::string query(
        "CREATE TABLE crt_normal_type_table(sid BIGINT, age INTEGER, name VARCHAR, sex BOOL, duty STRING, title "
        "VARCHAR, grade SMALLINT, class INT4, rank INT, score REAL, is_grad BOOLEAN, school_make_up TINYINT, mathscore "
        "FLOAT, englishscore FLOAT4, randnum1 INT8, randnum2 DOUBLE, course INT2, mastercnt MEDIUMINT, grade_s "
        "DECIMAL(5,2), in_time DATE, start_time TIMESTAMP);");
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    std::cout << result->GetRetMsg() << std::endl;
    do {
        auto table_info = conn->GetTableInfo(tablename);
        EXPECT_TRUE(table_info != nullptr);
        EXPECT_EQ(table_info->GetColumnCount(), 21);
        EXPECT_EQ(table_info->GetIndexCount(), 0);
    } while (0);

    do {
        std::string crt_idx1("CREATE UNIQUE INDEX idx_crt_age_name on crt_normal_type_table (age, name);");
        auto result1 = conn->Query(crt_idx1.c_str());
        EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
        auto table_info = conn->GetTableInfo(tablename);
        EXPECT_TRUE(table_info != nullptr);
        EXPECT_EQ(table_info->GetIndexCount(), 1);
        EXPECT_EQ(table_info->GetIndexBySlot(0).Name(), "idx_crt_age_name");
    } while (0);

    do {
        std::string crt_idx2("CREATE INDEX idx_crt_name on crt_normal_type_table (name);");
        auto result2 = conn->Query(crt_idx2.c_str());
        EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);
        auto table_info = conn->GetTableInfo(tablename);
        EXPECT_TRUE(table_info != nullptr);
        EXPECT_EQ(table_info->GetIndexCount(), 2);
        EXPECT_EQ(table_info->GetIndexBySlot(1).Name(), "idx_crt_name");
    } while (0);
}

TEST_F(ConnectionTest, TypeTest2CreateTable) {
    std::string tablename("exception_type_table");
    std::string query(
        fmt::format("CREATE TABLE {}(sid BIGINT, age INTEGER, name VARCHAR, sex BOOL, duty STRING, title VARCHAR, "
                    "grade SMALLINT, class INT4, rank INT, score REAL, is_grad BOOLEAN, school_make_up TINYINT, "
                    "mathscore FLOAT, englishscore FLOAT4, randnum1 INT8, randnum2 DOUBLE, course INT2, mastercnt "
                    "MEDIUMINT, grade_s DECIMAL(5,2), in_time DATE, start_time UUID);",
                    tablename));
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_FALSE(result->GetRetCode() == GS_SUCCESS);
}

TEST_F(ConnectionTest, NameTooLongCreateTable) {
    std::string tablename("name_too_long_tableaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    std::string query(
        fmt::format("CREATE TABLE {}(sid BIGINT, age INTEGER, name VARCHAR, sex BOOL, duty STRING, title VARCHAR, "
                    "grade SMALLINT, class INT4, rank INT, score REAL, is_grad BOOLEAN, school_make_up TINYINT, "
                    "mathscore FLOAT, englishscore FLOAT4, randnum1 INT8, randnum2 DOUBLE, course INT2, mastercnt "
                    "MEDIUMINT, grade_s DECIMAL(5,2), in_time DATE, start_time UUID);",
                    tablename));
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_FALSE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info == nullptr);
}

TEST_F(ConnectionTest, IndexNameTooLongCreateTable) {
    std::string tablename("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    std::string query(
        fmt::format("CREATE TABLE {}(sid BIGINT, age INTEGER, name VARCHAR, sex BOOL, duty STRING, title VARCHAR, "
                    "grade SMALLINT, class INT4, rank INT, score REAL, is_grad BOOLEAN, school_make_up TINYINT, "
                    "mathscore FLOAT, englishscore FLOAT4, randnum1 INT8, randnum2 DOUBLE, course INT2, mastercnt "
                    "MEDIUMINT, grade_s DECIMAL(5,2), in_time DATE, start_time UUID);",
                    tablename));
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_FALSE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info == nullptr);
}

TEST_F(ConnectionTest, Duplicate1CreateTable) {
    std::string tablename("duplicate_1_table");
    std::string query(
        "CREATE TABLE duplicate_1_table(sid BIGINT, age INTEGER, name VARCHAR, sex BOOL, duty STRING, title VARCHAR, "
        "grade SMALLINT, class INT4, rank INT, score REAL, is_grad BOOLEAN, school_make_up TINYINT, mathscore FLOAT, "
        "englishscore FLOAT4, randnum1 INT8, randnum2 DOUBLE, course INT2, mastercnt MEDIUMINT, grade_s DECIMAL(5,2), "
        "in_time DATE, start_time TIMESTAMP);");
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto result1 = conn->Query(query.c_str());
    EXPECT_FALSE(result1->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(result1->GetRetMsg(), std::string("The object table ") + tablename + " already exists.");
}

TEST_F(ConnectionTest, NonConstraintCreateTable) {
    std::string tablename("crt_normal_table");
    std::string query("CREATE TABLE crt_normal_table(sidn INTEGER, agen INTEGER, namen VARCHAR);");
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    do {
        auto table_info = conn->GetTableInfo(tablename);
        EXPECT_TRUE(table_info != nullptr);
        EXPECT_EQ(table_info->GetColumnCount(), 3);
        EXPECT_EQ(table_info->GetIndexCount(), 0);
    } while (0);

    do {
        std::string crt_idx1("CREATE UNIQUE INDEX idx_agen_namen on crt_normal_table (agen, namen);");
        auto result1 = conn->Query(crt_idx1.c_str());
        EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
        auto table_info = conn->GetTableInfo(tablename);
        EXPECT_TRUE(table_info != nullptr);
        EXPECT_EQ(table_info->GetIndexCount(), 1);
        EXPECT_EQ(table_info->GetIndexBySlot(0).Name(), "idx_agen_namen");
    } while (0);

    do {
        std::string crt_idx2("CREATE INDEX idx_namen on crt_normal_table (namen);");
        auto result2 = conn->Query(crt_idx2.c_str());
        EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);
        auto table_info = conn->GetTableInfo(tablename);
        EXPECT_TRUE(table_info != nullptr);
        EXPECT_EQ(table_info->GetIndexCount(), 2);
        EXPECT_EQ(table_info->GetIndexBySlot(1).Name(), "idx_namen");
    } while (0);
}

TEST_F(ConnectionTest, PrimaryKeyCreateTable) {
    std::string tablename("primary_table");
    std::string query("CREATE TABLE primary_table(sid1 INTEGER PRIMARY KEY, age1 INTEGER, name1 VARCHAR);");
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    do {
        auto table_info = conn->GetTableInfo(tablename);
        EXPECT_TRUE(table_info != nullptr);
        EXPECT_EQ(table_info->GetColumnCount(), 3);
        EXPECT_EQ(table_info->GetIndexCount(), 1);
        EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_TRUE);
        EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);
    } while (0);

    do {
        std::string crt_idx1("CREATE UNIQUE INDEX idx_age1_name1 on primary_table (age1, name1);");
        auto result1 = conn->Query(crt_idx1.c_str());
        EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
        auto table_info = conn->GetTableInfo(tablename);
        EXPECT_TRUE(table_info != nullptr);
        EXPECT_EQ(table_info->GetIndexCount(), 2);
        EXPECT_EQ(table_info->GetIndexBySlot(1).Name(), "idx_age1_name1");
        EXPECT_TRUE(table_info->GetIndexBySlot(1).GetIndex().is_unique == GS_TRUE);
        EXPECT_TRUE(table_info->GetIndexBySlot(1).GetIndex().is_primary == GS_FALSE);
    } while (0);

    do {
        std::string crt_idx2("CREATE INDEX idx_name1 on primary_table (name1);");
        auto result2 = conn->Query(crt_idx2.c_str());
        EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);
        auto table_info = conn->GetTableInfo(tablename);
        EXPECT_TRUE(table_info != nullptr);
        EXPECT_EQ(table_info->GetIndexCount(), 3);
        EXPECT_EQ(table_info->GetIndexBySlot(2).Name(), "idx_name1");
        EXPECT_TRUE(table_info->GetIndexBySlot(2).GetIndex().is_unique == GS_FALSE);
        EXPECT_TRUE(table_info->GetIndexBySlot(2).GetIndex().is_primary == GS_FALSE);
    } while (0);
}

TEST_F(ConnectionTest, PrimaryKeyAndUniqueConsCreateTable) {
    std::string tablename("primary_unique_table");
    std::string query(
        "CREATE TABLE primary_unique_table(sid2 INTEGER PRIMARY KEY, age2 INTEGER, name2 VARCHAR, UNIQUE(age2, "
        "name2));");
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    do {
        auto table_info = conn->GetTableInfo(tablename);
        EXPECT_TRUE(table_info != nullptr);
        EXPECT_EQ(table_info->GetColumnCount(), 3);
        EXPECT_EQ(table_info->GetIndexCount(), 2);
        EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_TRUE);
        EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);
        EXPECT_TRUE(table_info->GetIndexBySlot(1).GetIndex().is_unique == GS_TRUE);
        EXPECT_TRUE(table_info->GetIndexBySlot(1).GetIndex().is_primary == GS_FALSE);
    } while (0);

    do {
        std::string crt_idx1("CREATE INDEX idx_name2 on primary_unique_table (name2);");
        auto result1 = conn->Query(crt_idx1.c_str());
        EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
        auto table_info = conn->GetTableInfo(tablename);
        EXPECT_TRUE(table_info != nullptr);
        EXPECT_EQ(table_info->GetIndexCount(), 3);
        EXPECT_EQ(table_info->GetIndexBySlot(2).Name(), "idx_name2");
        EXPECT_TRUE(table_info->GetIndexBySlot(2).GetIndex().is_unique == GS_FALSE);
        EXPECT_TRUE(table_info->GetIndexBySlot(2).GetIndex().is_primary == GS_FALSE);
    } while (0);
}

TEST_F(ConnectionTest, Duplicate1CreateIndex) {
    std::string indexname("du_idx_crt_name2");
    std::string query(
        "CREATE TABLE duplicate1_index_table(sid2 INTEGER PRIMARY KEY, age2 INTEGER, name2 VARCHAR, UNIQUE(age2, "
        "name2));");
    std::cout << query << std::endl;
    std::string crt_idx1("CREATE INDEX du_idx_crt_name2 on duplicate1_index_table (name2);");
    // 1.create table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    // 2.create index
    auto result1 = conn->Query(crt_idx1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto result2 = conn->Query(crt_idx1.c_str());
    EXPECT_FALSE(result2->GetRetCode() == GS_SUCCESS);
    EXPECT_EQ(result2->GetRetMsg(), std::string("The object index ") + indexname + " already exists.");
}

TEST_F(ConnectionTest, PrimaryConsCreateTable) {
    std::string tablename("primary_cons_table");
    std::string query("CREATE TABLE primary_cons_table(sid3 INTEGER, age3 INTEGER, name3 VARCHAR, PRIMARY KEY(sid3));");
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    do {
        auto table_info = conn->GetTableInfo(tablename);
        EXPECT_TRUE(table_info != nullptr);
        EXPECT_EQ(table_info->GetColumnCount(), 3);
        EXPECT_EQ(table_info->GetIndexCount(), 1);
        EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_TRUE);
        EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);
    } while (0);

    do {
        std::string crt_idx1("CREATE INDEX idx_age3_name3 on primary_cons_table (age3, name3);");
        auto result1 = conn->Query(crt_idx1.c_str());
        EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
        auto table_info = conn->GetTableInfo(tablename);
        EXPECT_TRUE(table_info != nullptr);
        EXPECT_EQ(table_info->GetIndexCount(), 2);
        EXPECT_EQ(table_info->GetIndexBySlot(1).Name(), "idx_age3_name3");
        EXPECT_TRUE(table_info->GetIndexBySlot(1).GetIndex().is_primary == GS_FALSE);
        EXPECT_TRUE(table_info->GetIndexBySlot(1).GetIndex().is_unique == GS_FALSE);
    } while (0);
}

TEST_F(ConnectionTest, NotNullConsCreateTable) {
    std::string tablename("notnull_cons_table");
    std::string query(
        "CREATE TABLE notnull_cons_table(sid4 INTEGER, age4 INTEGER, name4 VARCHAR NOT NULL, PRIMARY KEY(sid4));");
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 3);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_TRUE);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);
}

TEST_F(ConnectionTest, NullConsCreateTable) {
    std::string tablename("null_cons_table");
    std::string query(
        "CREATE TABLE null_cons_table(sid5 INTEGER, age5 INTEGER, name5 VARCHAR NULL, PRIMARY KEY(sid5));");
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 3);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_TRUE);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);
}

TEST_F(ConnectionTest, CreateIndexColumnNotExist) {
    std::string tablename("index_column_not_exist_table");
    std::string query(
        fmt::format("CREATE TABLE {}(sid5 INTEGER, age5 INTEGER, name5 VARCHAR NULL, PRIMARY KEY(sid5));", tablename));
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 3);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_TRUE);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);

    std::string indexname("idxage");
    std::string ct_index(fmt::format("create index {} on {} (age);", indexname, tablename));
    auto result1 = conn->Query(ct_index.c_str());
    EXPECT_FALSE(result1->GetRetCode() == GS_SUCCESS);
}

TEST_F(ConnectionTest, CreateTableAndInsertAndCreateIndex) {
    std::string tablename("student");
    std::string query(
        "create table student (sid integer primary key, name varchar(20), age integer, gpa real, score number(8), toff "
        "DECIMAL);");
    std::cout << query << std::endl;

    // create table
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 6);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_TRUE);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);
    // insert
    std::string insertq("insert into student values(1,'ruo', 25, 99.1,333, 1000);");
    auto result1 = conn->Query(insertq.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    // create index
    std::string createindexq("create index ageidx on student (age);");
    auto result2 = conn->Query(createindexq.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);
}

TEST_F(ConnectionTest, DefaultIntConsCreateTable) {
    std::string tablename("default_int_cons_table");
    int32 default_value(20);
    std::string query(
        fmt::format("CREATE TABLE default_int_cons_table(sid6 INTEGER, age6 INTEGER NOT NULL DEFAULT {}, name6 VARCHAR "
                    "NULL, PRIMARY KEY(sid6));",
                    default_value));
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 3);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_TRUE);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);
    auto col = table_info->GetColumnByName("age6");
    EXPECT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_INTEGER);
    EXPECT_EQ(col->nullable, GS_FALSE);
    EXPECT_EQ(col->is_default, GS_TRUE);
    DefaultValue* default_value_ptr = reinterpret_cast<DefaultValue*>(col->default_val.str);
    Value value(col->col_type, {default_value_ptr->data, default_value_ptr->size}, col->scale,col->precision);
    EXPECT_EQ(default_value, value.GetCastAs<int32_t>());
}

TEST_F(ConnectionTest, DefaultRealConsCreateTable) {
    std::string tablename("default_real_cons_table");
    double default_value(99.1);
    std::string query(
        fmt::format("CREATE TABLE default_real_cons_table(sid6 INTEGER, age6 INTEGER NOT NULL DEFAULT 20, name6 "
                    "VARCHAR NULL, title VARCHAR, score REAL DEFAULT {}, PRIMARY KEY(sid6));",
                    default_value));
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 5);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_TRUE);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);
    auto col = table_info->GetColumnByName("score");
    EXPECT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_REAL);
    EXPECT_EQ(col->is_default, GS_TRUE);
    DefaultValue* default_value_ptr = reinterpret_cast<DefaultValue*>(col->default_val.str);
    Value value(col->col_type, {default_value_ptr->data, default_value_ptr->size}, col->scale,col->precision);
    EXPECT_EQ(default_value, value.GetCastAs<double>());
}

TEST_F(ConnectionTest, DefaultNumberConsCreateTable) {
    std::string tablename("default_number_cons_table");
    double default_value(99.88);
    std::string query(
        fmt::format("CREATE TABLE default_number_cons_table(sid6 INTEGER, age6 INTEGER NOT NULL DEFAULT 20, name6 "
                    "VARCHAR NULL, title VARCHAR, score NUMBER(10,5) DEFAULT {}, PRIMARY KEY(sid6));",
                    default_value));
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 5);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_TRUE);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);
    auto col = table_info->GetColumnByName("score");
    EXPECT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_DECIMAL);
    EXPECT_EQ(col->precision, 10);
    EXPECT_EQ(col->scale, 5);
    EXPECT_EQ(col->is_default, GS_TRUE);
}

TEST_F(ConnectionTest, DefaultDecimalConsCreateTable) {
    std::string tablename("default_decimal_cons_table");
    double default_value(1001.1001);
    std::string query(
        fmt::format("CREATE TABLE default_decimal_cons_table(sid6 INTEGER, age6 INTEGER NOT NULL DEFAULT 20, name6 "
                    "VARCHAR NULL, title VARCHAR, score DECIMAL(9,5) DEFAULT {}, PRIMARY KEY(sid6));",
                    default_value));
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 5);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_TRUE);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);
    auto col = table_info->GetColumnByName("score");
    EXPECT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_DECIMAL);
    EXPECT_EQ(col->precision, 9);
    EXPECT_EQ(col->scale, 5);
    EXPECT_EQ(col->is_default, GS_TRUE);
}

TEST_F(ConnectionTest, DefaultStringConsCreateTable) {
    std::string tablename("default_string_cons_table");
    std::string default_value("stu");
    // std::string default_value_h("LoveCountryHonor");
    // std::string query(fmt::format("CREATE TABLE default_string_cons_table(sid6 INTEGER, age6 INTEGER NOT NULL DEFAULT
    // 20, name6 VARCHAR NULL, title VARCHAR DEFAULT '{}', score REAL DEFAULT 99.1, honor STRING DEFAULT '{}', PRIMARY
    // KEY(sid6));", default_value, default_value_h));
    std::string query(
        fmt::format("CREATE TABLE default_string_cons_table(sid6 INTEGER, age6 INTEGER NOT NULL DEFAULT 20, name6 "
                    "VARCHAR NULL, title VARCHAR DEFAULT '{}', score REAL DEFAULT 99.1, PRIMARY KEY(sid6));",
                    default_value));
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 5);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_TRUE);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);
    auto col = table_info->GetColumnByName("title");
    EXPECT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    EXPECT_EQ(col->is_default, GS_TRUE);
    DefaultValue* default_value_ptr = reinterpret_cast<DefaultValue*>(col->default_val.str);
    Value value(col->col_type, {default_value_ptr->data, default_value_ptr->size}, col->scale,col->precision);
    EXPECT_STREQ(default_value.c_str(), value.GetCastAs<std::string>().c_str());
}

TEST_F(ConnectionTest, DefaultNullConsCreateTable) {
    std::string tablename("default_null_cons_table");
    std::string query(
        "CREATE TABLE default_null_cons_table(sid6 INTEGER, age6 INTEGER NOT NULL DEFAULT 20, name6 VARCHAR DEFAULT "
        "NULL, title VARCHAR DEFAULT 'stu', score REAL DEFAULT 99.1, PRIMARY KEY(sid6));");
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 5);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_primary == GS_TRUE);
    EXPECT_TRUE(table_info->GetIndexBySlot(0).GetIndex().is_unique == GS_TRUE);
    auto col = table_info->GetColumnByName("name6");
    EXPECT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    EXPECT_EQ(col->nullable, GS_TRUE);
}

TEST_F(ConnectionTest, DefaultConsErrCreateTable) {
    std::string tablename("table_default_crt_err");
    std::string query(
        fmt::format("CREATE TABLE {} (column1 INT default 'abc', column2 VARCHAR(50) PRIMARY KEY);", tablename));
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_FALSE(result->GetRetCode() == GS_SUCCESS);
}

TEST_F(ConnectionTest, TimestampErrCreateTable) {
    std::string tablename("table_timestamp_err");
    std::string query(fmt::format(
        "CREATE TABLE {}( sec TIMESTAMP(0), msec TIMESTAMP(3), micros TIMESTAMP(6), nanos TIMESTAMP (9));", tablename));
    std::cout << query << std::endl;

    auto result = conn->Query(query.c_str());
    EXPECT_FALSE(result->GetRetCode() == GS_SUCCESS);
    // EXPECT_STREQ(result->GetRetMsg().c_str(), "precision or scale can not be seted for type Timestamp!");
}
// CREATE TABLE IF NOT EXISTS
TEST_F(ConnectionTest, CreateTableBug709239452) {
    std::string tablename("table_ifnotexists");
    std::string query(fmt::format("CREATE TABLE IF NOT EXISTS {}(i INTEGER, j INTEGER);", tablename));
    std::cout << query << std::endl;
    // first
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    // second
    auto result1 = conn->Query(query.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    //
    auto result2 = conn->Query(fmt::format("CREATE TABLE {}(i INTEGER, j INTEGER);", tablename).c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_ERROR);

    // third
    auto result3 = conn->Query(query.c_str());
    EXPECT_TRUE(result3->GetRetCode() == GS_SUCCESS);
}

// CREATE OR REPLACE TABLE
TEST_F(ConnectionTest, CreateTableBug709565416) {
    std::string tablename("table_replace");
    std::string query(fmt::format("CREATE OR REPLACE TABLE {} (i INT, j VARCHAR);", tablename));
    std::cout << query << std::endl;
    // first
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_ERROR);
    // EXPECT_STREQ(result->GetRetMsg().c_str(), "replace expr is not supported in create table sql!");
}

TEST_F(ConnectionTest, CreateTableWithUnSupportedFeature) {
    // create temp table
    auto r = conn->Query("CREATE TEMP TABLE table_temp (i INT, j VARCHAR)");
    ASSERT_NE(r->GetRetCode(), 0);
    // create virtual column
    r = conn->Query("CREATE TABLE table_virtual (i INT, j VARCHAR, k INT GENERATED ALWAYS AS (i + 1))");
    ASSERT_NE(r->GetRetCode(), 0);
}

// bugfix: #709982016 技术指标：单表最大列数默认配置与最大值冲突
TEST_F(ConnectionTest, CreateTableBug709982016) {
    std::string create_eq_max = "create table table_max_column(a int";
    for (int i = 1; i < 128; i++) {
        create_eq_max += ",a";
        create_eq_max += std::to_string(i);
        create_eq_max += " int";
    }
    create_eq_max += ");";
    auto r = conn->Query(create_eq_max.c_str());
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);

    std::string create_lt_max = "create table table_lt_max_column(a int";
    for (int i = 1; i < 129; i++) {
        create_lt_max += ",a";
        create_lt_max += std::to_string(i);
        create_lt_max += " int";
    }
    create_lt_max += ");";
    auto r1 = conn->Query(create_lt_max.c_str());
    ASSERT_NE(r1->GetRetCode(), GS_SUCCESS);
    EXPECT_STREQ(r1->GetRetMsg().c_str(), "Column count exceeded the limit 128");
}

TEST_F(ConnectionTest, CreateTableWithFuncDefault) {
    // create table with not exists sequence
    auto r = conn->Query("create table table_func_default (a int, b smallint default 2,c decimal(5,2) default 2.11, d int default nextval('default_val_seq1'))");
    ASSERT_NE(r->GetRetCode(), GS_SUCCESS);

    // create sequence 
    conn->Query("create sequence default_val_seq1;");
    r = conn->Query("create table table_func_default (a int, b smallint default 2,c decimal(5,2) default 2.11, d int default nextval('default_val_seq1'))");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    auto table_info = conn->GetTableInfo("table_func_default"); 
    EXPECT_EQ(table_info->GetColumnCount(), 4);
    EXPECT_EQ(table_info->GetColumnByName("a")->is_default, GS_FALSE);
    EXPECT_EQ(table_info->GetColumnByName("b")->is_default, GS_TRUE);
    EXPECT_EQ(table_info->GetColumnByName("c")->is_default, GS_TRUE);
    EXPECT_EQ(table_info->GetColumnByName("d")->is_default, GS_TRUE);
    auto col_def = table_info->GetColumnByName("d");
    DefaultValue* default_value_ptr = reinterpret_cast<DefaultValue*>(col_def->default_val.str);
    EXPECT_EQ(default_value_ptr->type, DefaultValueType::DEFAULT_VALUE_TYPE_FUNC);
    EXPECT_STREQ(default_value_ptr->data, "nextval(default_val_seq1)");
    r = conn->Query("insert into table_func_default(a) values(1)");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    r = conn->Query("select * from table_func_default");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(),1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int16_t>(),2);
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<double>(),2.11);
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<int32_t>(),1);
}

TEST_F(ConnectionTest , CreateTableWithAutoIncrement) {
    auto r = conn->Query("create table table_auto_increment (a int autoincrement, b int)");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    // test insert into auto_increment column with null 
    r = conn->Query("insert into table_auto_increment values(null,1)");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    r = conn->Query("select * from table_auto_increment");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(),1);

    // create table with auto_increment column by column type is not integer
    r = conn->Query("create table table_auto_increment1 (a varchar autoincrement, b int)");
    ASSERT_NE(r->GetRetCode(), GS_SUCCESS);

    // create table with muliti auto_increment column
    r = conn->Query("create table table_auto_increment2 (a int autoincrement, b int autoincrement)"); 
    ASSERT_NE(r->GetRetCode(), GS_SUCCESS);

    // create table with auto_increment column with default value 
    r = conn->Query("create table table_auto_increment3 (a int autoincrement default 1, b int)");
    ASSERT_NE(r->GetRetCode(), GS_SUCCESS);
}

int main(int argc, char** argv) {
    system("rm -rf intarkdb/");
    ::testing::GTEST_FLAG(output) = "xml";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
