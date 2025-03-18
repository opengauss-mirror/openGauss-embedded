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
 * alter_test.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/test/alter_test.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <gtest/gtest.h>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "common/default_value.h"
#include "main/connection.h"
#include "main/database.h"

std::string tablename("alter_table");

class AlterTest : public ::testing::Test {
   protected:
    AlterTest() {}
    ~AlterTest() {}
    static void SetUpTestSuite() {
        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance->Init();
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();

        std::string query(
            fmt::format("create table {} (id integer, score smallint, age tinyint, tiny tinyint, small smallint, intt "
                        "integer, name varchar);",
                        tablename));
        std::cout << query << std::endl;
        auto result = conn->Query(query.c_str());
        EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
        std::cout << tablename << " create table success." << std::endl;
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

std::shared_ptr<IntarkDB> AlterTest::db_instance = nullptr;
std::unique_ptr<Connection> AlterTest::conn = nullptr;

// A predicate-formatter for asserting that two integers are mutually prime.
testing::AssertionResult AssertColTextTCmp(const char* m_expr, const char* n_expr, int m, int n) {
    if (m != n) return testing::AssertionFailure();

    if (strncmp(m_expr, n_expr, m) == 0)
        return testing::AssertionSuccess();
    else
        return testing::AssertionFailure();
}

// not support add column with constraint
TEST_F(AlterTest, AlterTableAddColumnTypeIntAndPriKey) {
    // add column ,type integer, primary ke
    std::string column_name("primary_col");
    std::string add_column_q(fmt::format("alter table {} add column {} integer primary key;", tablename, column_name));
    std::cout << add_column_q << std::endl;
    auto result = conn->Query(add_column_q.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    // ASSERT_NE(result->GetRetCode(), GS_SUCCESS);

    // auto table_info = conn->GetTableInfo(tablename);
    // ASSERT_FALSE(table_info == nullptr);
    // EXPECT_EQ(table_info->GetColumnCount(), 8);
    // auto col = table_info->GetColumnByName(column_name);
    // ASSERT_FALSE(col == NULL);
    // EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_INTEGER);
    // EXPECT_EQ(col->is_default, GS_FALSE);
    // EXPECT_EQ(table_info->GetIndexCount(), 1);
    // EXPECT_EQ(table_info->GetIndexBySlot(0).GetIndex().col_ids[0], col->col_slot);
}

TEST_F(AlterTest, AlterTableAddColumnTypeVarcharAndNullCons) {
    conn->Query("drop table if exists alter_table");
    conn->Query(
        "create table alter_table (id integer, score smallint, age tinyint, tiny tinyint, small smallint, intt "
        "integer, name varchar,primary_col int primary key)");
    // insert data
    std::string insert_data1(fmt::format(
        "insert into {} values(1, 99, 23, 125, 342, 435, 'ruo', 1), (2, 98, 25, 112, 11, 435, 'teng', 2);", tablename));
    auto result = conn->Query(insert_data1.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 8);
    // add column ,type varchar, NULL
    std::string column_name("add_column");
    std::string add_column_q(fmt::format("alter table {} add column {} varchar(10) NULL;", tablename, column_name));
    std::cout << add_column_q << std::endl;
    auto result1 = conn->Query(add_column_q.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    ASSERT_EQ(result1->stmt_props.read_tables.size(),0);
    ASSERT_EQ(result1->stmt_props.modify_tables.size(),1);
    EXPECT_EQ(result1->stmt_props.modify_tables.begin()->first,tablename);
    EXPECT_EQ(result1->stmt_props.modify_tables.begin()->second.table_type,StatementProps::TableType::TABLE_TYPE_NORMAL);

    auto table_info1 = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info1 == nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 9);
    auto col = table_info1->GetColumnByName(column_name);
    ASSERT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    EXPECT_EQ(col->is_default, GS_FALSE);
    EXPECT_EQ(col->is_primary, GS_FALSE);
    EXPECT_EQ(col->nullable, GS_TRUE);
}

TEST_F(AlterTest, AlterTableAddColumnTypeVarcharAndNotNullCons) {
    // add column ,type varchar, NOT NULL
    std::string column_name("not_null_col");
    std::string add_column_q(fmt::format("alter table {} add column {} varchar(30) NULL;", tablename, column_name));
    std::cout << add_column_q << std::endl;
    auto result = conn->Query(add_column_q.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 10);
    auto col = table_info->GetColumnByName(column_name);
    ASSERT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    EXPECT_EQ(col->is_default, GS_FALSE);
    EXPECT_EQ(col->is_primary, GS_FALSE);
    EXPECT_TRUE(col->nullable == GS_TRUE);
}

// TEST_F(AlterTest, AlterTableAddColumnTypeBigintAndUniqueCons) {
//     // add column ,type bigint, UNIQUE
//     std::string column_name("unique_col");
//     std::string add_column_q(fmt::format("alter table {} add column {} bigint unique;", tablename, column_name));
//     std::cout << add_column_q << std::endl;
//     auto result = conn->Query(add_column_q.c_str());
//     EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

//     auto table_info = conn->GetTableInfo(tablename);
//     ASSERT_FALSE(table_info == nullptr);
//     EXPECT_EQ(table_info->GetColumnCount(), 11);
//     auto col = table_info->GetColumnByName(column_name);
//     ASSERT_FALSE(col == NULL);
//     EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_BIGINT);
//     EXPECT_EQ(col->is_default, GS_FALSE);
//     EXPECT_EQ(col->is_primary, GS_FALSE);
//     EXPECT_EQ(col->nullable, GS_TRUE);
// }

TEST_F(AlterTest, AlterTableAddColumnTypeBigintAndSetDefault) {
    // 兼容测试
    conn->Query("alter table alter_table add column unique_col bigint");
    // add column ,type bigint, DEFAULT
    std::string column_name("has_default_col");
    int64_t default_value = 99;
    std::string add_column_q(
        fmt::format("alter table {} add column {} bigint default {};", tablename, column_name, default_value));
    std::cout << add_column_q << std::endl;
    auto result = conn->Query(add_column_q.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 12);
    auto col = table_info->GetColumnByName(column_name);
    ASSERT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_BIGINT);
    EXPECT_EQ(col->is_default, GS_TRUE);
    EXPECT_EQ(col->is_primary, GS_FALSE);
    EXPECT_EQ(col->nullable, GS_TRUE);
    DefaultValue* default_value_ptr = (DefaultValue*)col->default_val.str;
    Value value(GStorDataType(col->col_type),col_text_t{default_value_ptr->data, default_value_ptr->size},col->scale,col->precision);
    EXPECT_EQ(default_value, value.GetCastAs<int64_t>());
}

TEST_F(AlterTest, AlterTableAddColumnTypeIntAndSetDefault) {
    // add column ,type integer and set default
    std::string column_name("int_def");
    int32 default_value = 100;
    std::string add_column_q(
        fmt::format("alter table {} add column {} integer default {};", tablename, column_name, default_value));
    std::cout << add_column_q << std::endl;
    auto result = conn->Query(add_column_q.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 13);
    auto col = table_info->GetColumnByName(column_name);
    ASSERT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_INTEGER);
    EXPECT_EQ(col->is_default, GS_TRUE);
    DefaultValue* default_value_ptr = (DefaultValue*)col->default_val.str;
    Value value(GStorDataType(col->col_type),col_text_t{default_value_ptr->data, default_value_ptr->size},col->scale,col->precision);
    EXPECT_EQ(default_value, value.GetCastAs<int32_t>());
}

TEST_F(AlterTest, AlterTableAlterColumnSetDefaultTypeVarchar) {
    // set default for adding column ,type varchar
    std::string column_name("add_column");
    std::string default_value("demo");
    std::string set_default_q(
        fmt::format("alter table {} alter column {} set default \'{}\';", tablename, column_name, default_value));
    std::cout << set_default_q << std::endl;
    auto result = conn->Query(set_default_q.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 13);
    auto col = table_info->GetColumnByName(column_name);
    ASSERT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_VARCHAR);
    EXPECT_EQ(col->is_default, GS_TRUE);
    DefaultValue* default_value_ptr = (DefaultValue*)col->default_val.str;
    Value value(GStorDataType(col->col_type),col_text_t{default_value_ptr->data, default_value_ptr->size},col->scale,col->precision);
    EXPECT_STREQ(default_value.c_str(), value.GetCastAs<std::string>().c_str());
}

TEST_F(AlterTest, AlterTableAlterColumnSetDefaultTypeInt) {
    // modify column - set default for adding column ,type integer
    std::string column_name("int_def");
    int32 default_value = 5;
    std::string add_column_q(
        fmt::format("alter table {} alter column {} set default {};", tablename, column_name, default_value));
    std::cout << add_column_q << std::endl;
    auto result = conn->Query(add_column_q.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 13);
    auto col = table_info->GetColumnByName(column_name);
    ASSERT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_INTEGER);
    EXPECT_EQ(col->is_default, GS_TRUE);
    DefaultValue* default_value_ptr = (DefaultValue*)col->default_val.str;
    Value value(GStorDataType(col->col_type),col_text_t{default_value_ptr->data, default_value_ptr->size},col->scale,col->precision);
    EXPECT_EQ(default_value, value.GetCastAs<int32_t>());
}

TEST_F(AlterTest, AlterTableAlterColumnSetTypeIntToBigint) {
    // modify column - set type to bigint for adding column ,type integer
    std::string column_name("al_datatype_col");
    std::string add_column_q(fmt::format("alter table {} add column {} integer;", tablename, column_name));
    std::cout << add_column_q << std::endl;
    auto result = conn->Query(add_column_q.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto int_table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(int_table_info == nullptr);
    auto col_int = int_table_info->GetColumnByName(column_name);
    ASSERT_FALSE(col_int == NULL);
    EXPECT_EQ(col_int->col_type, GStorDataType::GS_TYPE_INTEGER);
    EXPECT_EQ(col_int->is_default, GS_FALSE);

    std::string al_column_q(
        fmt::format("alter table {} alter column {} set data type bigint;", tablename, column_name));
    std::cout << al_column_q << std::endl;
    auto result1 = conn->Query(al_column_q.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info1 == nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 14);
    auto col = table_info1->GetColumnByName(column_name);
    ASSERT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_BIGINT);
    EXPECT_EQ(col->is_default, GS_FALSE);
}

TEST_F(AlterTest, AlterTableAlterColumnDropDefault) {
    // modify column - drop default to bigint for adding column ,type bigint
    std::string column_name("int_def");
    std::string add_column_q(fmt::format("alter table {} alter column {} drop default;", tablename, column_name));
    std::cout << add_column_q << std::endl;
    auto result = conn->Query(add_column_q.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 14);
    auto col = table_info->GetColumnByName(column_name);
    ASSERT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_INTEGER);
    EXPECT_EQ(col->is_default, GS_FALSE);
}

TEST_F(AlterTest, AlterTableAlterColumnRename) {
    // modify column - rename column to int_rename for adding column ,type bigint
    std::string column_name("int_def");
    std::string column_rename("int_rename");
    std::string add_column_q(
        fmt::format("alter table {} rename column {} to {};", tablename, column_name, column_rename));
    std::cout << add_column_q << std::endl;
    auto result = conn->Query(add_column_q.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 14);
    // old column name not exists
    auto col = table_info->GetColumnByName(column_name);
    EXPECT_TRUE(col == NULL);
    // new column name exists
    auto colre = table_info->GetColumnByName(column_rename);
    ASSERT_FALSE(colre == NULL);
    EXPECT_EQ(colre->col_type, GStorDataType::GS_TYPE_INTEGER);
    EXPECT_EQ(colre->is_default, GS_FALSE);
}

TEST_F(AlterTest, AlterTableDropColumn) {
    // add column for droping
    std::string column_name("drop_col");
    int32 default_value = 100;
    std::string add_column_q(
        fmt::format("alter table {} add column {} integer default {};", tablename, column_name, default_value));
    std::cout << add_column_q << std::endl;
    auto result = conn->Query(add_column_q.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info == nullptr);
    ASSERT_EQ(table_info->GetColumnCount(), 15);
    auto col = table_info->GetColumnByName(column_name);
    ASSERT_FALSE(col == NULL);
    EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_INTEGER);
    EXPECT_EQ(col->is_default, GS_TRUE);
    DefaultValue* default_value_ptr = (DefaultValue*)col->default_val.str;
    Value value(GStorDataType(col->col_type),col_text_t{default_value_ptr->data, default_value_ptr->size},col->scale,col->precision);
    EXPECT_EQ(default_value, value.GetCastAs<int32_t>());

    // drop column
    std::string drop_column_q(fmt::format("alter table {} drop column {};", tablename, column_name));
    std::cout << drop_column_q << std::endl;
    auto result1 = conn->Query(drop_column_q.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto drop_table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(drop_table_info == nullptr);
    EXPECT_EQ(drop_table_info->GetColumnCount(), 14);
    auto drop_col = drop_table_info->GetColumnByName(column_name);
    EXPECT_TRUE(drop_col == NULL);
}

TEST_F(AlterTest, TestConstraintDefaultAlterColNotExist) {
    std::string column_name("column_not_exist");
    std::string add_column_q(fmt::format("alter table {} alter column {} set default 'abc';", tablename, column_name));
    auto result = conn->Query(add_column_q.c_str());
    ASSERT_FALSE(result->GetRetCode() == GS_SUCCESS);
    EXPECT_STREQ(result->GetRetMsg().c_str(),
                 fmt::format("Binder Error: Alter column {} not exists!", column_name).c_str());
}

TEST_F(AlterTest, AlterTableAlterColumnDropDefaultForNotExistDefault) {
    std::string column_name("col_not_exist_default");
    auto result = conn->Query(fmt::format("alter table {} add column {} int;", tablename, column_name).c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    std::string add_column_q(fmt::format("alter table {} alter column {} drop default;", tablename, column_name));
    auto result1 = conn->Query(add_column_q.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
}

TEST_F(AlterTest, TestAlterTableDataTypeHavingDataFailed) {
    // modify column - set type to bigint for adding column ,type integer
    std::string column_name("al_datatype_intcol");
    std::string add_column_q(fmt::format("alter table {} add column {} integer;", tablename, column_name));
    auto result = conn->Query(add_column_q.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);

    auto result1 =
        conn->Query(fmt::format("insert into {} (primary_col, {}) values (5,5);", tablename, column_name).c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    // std::string al_column_q(
    //     fmt::format("alter table {} alter column {} set data type bigint;", tablename, column_name));
    // auto result2 = conn->Query(al_column_q.c_str());
    // EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    // auto table_info = conn->GetTableInfo(tablename);
    // ASSERT_FALSE(table_info == nullptr);
    // EXPECT_EQ(table_info->GetColumnCount(), 16);
    // auto col = table_info->GetColumnByName(column_name);
    // ASSERT_FALSE(col == NULL);
    // EXPECT_EQ(col->col_type, GStorDataType::GS_TYPE_BIGINT);
    // EXPECT_EQ(col->is_default, GS_FALSE);
}

TEST_F(AlterTest, AlterTableRenameTable) {
    // rename table
    std::string table_new_name("alter_table_new");
    std::string rename_table_q(fmt::format("alter table {} rename to {};", tablename, table_new_name));
    std::cout << rename_table_q << std::endl;
    auto result = conn->Query(rename_table_q.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    // old table name not exists
    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info == nullptr);
    // new table name exists
    auto table_info_new = conn->GetTableInfo(table_new_name);
    ASSERT_FALSE(table_info_new == nullptr);
    // new column name exists
    EXPECT_EQ(table_info_new->GetColumnCount(), 16);
}

// bugfix: #709950961 使用alter修改表的数据类型后插入数据，数据发生变化
TEST_F(AlterTest, AlterTableDataTypeBug709950961) {
    // rename table
    std::string tablename("table_alter_datatype_multicol");
    std::string create_tb(fmt::format("CREATE TABLE {} (id INT,col2 INT);", tablename));
    std::cout << create_tb << std::endl;
    auto result = conn->Query(create_tb.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    auto table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 2);
    EXPECT_EQ(table_info->GetColumnByName("id")->col_type, GStorDataType::GS_TYPE_INTEGER);
    EXPECT_EQ(table_info->GetColumnByName("id")->size, 4);
    EXPECT_EQ(table_info->GetColumnByName("col2")->col_type, GStorDataType::GS_TYPE_INTEGER);
    EXPECT_EQ(table_info->GetColumnByName("col2")->size, 4);

    std::string alter_tb1(fmt::format("alter table {} alter id set data type varchar;", tablename));
    auto result1 = conn->Query(alter_tb1.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    std::string alter_tb2(fmt::format("alter table {} alter col2 set data type number;", tablename));
    auto result2 = conn->Query(alter_tb2.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info1 == nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 2);
    EXPECT_EQ(table_info1->GetColumnByName("id")->col_type, GStorDataType::GS_TYPE_VARCHAR);
    EXPECT_EQ(table_info1->GetColumnByName("id")->size, 65535);
    EXPECT_EQ(table_info1->GetColumnByName("col2")->col_type, GStorDataType::GS_TYPE_DECIMAL);
    EXPECT_EQ(table_info1->GetColumnByName("col2")->size, 8);

    std::string insert_tb(fmt::format("insert into {} values('d1',33),('d2',44);", tablename));
    auto result3 = conn->Query(insert_tb.c_str());
    EXPECT_TRUE(result3->GetRetCode() == GS_SUCCESS);

    std::string select_tb(fmt::format("select * from {};", tablename));
    auto r = conn->Query(select_tb.c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "d1");
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<double>(), 33);
    EXPECT_STREQ(r->Row(1).Field(0).GetCastAs<std::string>().c_str(), "d2");
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<double>(), 44);
}

// bugfix: #709950800 使用alter将数据类型varchar修改为bigint后show表的col_size结果错误
TEST_F(AlterTest, AlterTableDataTypeBug709950800) {
    // rename table
    std::string tablename("table_709950800");
    std::string create_tb(fmt::format("CREATE TABLE {} (id INT,name varchar);", tablename));
    std::cout << create_tb << std::endl;
    auto result = conn->Query(create_tb.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    auto table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 2);
    EXPECT_EQ(table_info->GetColumnByName("id")->col_type, GStorDataType::GS_TYPE_INTEGER);
    EXPECT_EQ(table_info->GetColumnByName("id")->size, 4);
    EXPECT_EQ(table_info->GetColumnByName("name")->col_type, GStorDataType::GS_TYPE_VARCHAR);
    EXPECT_EQ(table_info->GetColumnByName("name")->size, 65535);

    std::string alter_tb(fmt::format("alter table {} alter name set data type bigint;", tablename));
    auto result1 = conn->Query(alter_tb.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);

    auto table_info1 = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info1 == nullptr);
    EXPECT_EQ(table_info1->GetColumnCount(), 2);
    EXPECT_EQ(table_info1->GetColumnByName("name")->col_type, GStorDataType::GS_TYPE_BIGINT);
    EXPECT_EQ(table_info1->GetColumnByName("name")->size, 8);

    std::string show_tb(fmt::format("show {};", tablename));
    auto r = conn->Query(show_tb.c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    ASSERT_EQ(r->RowCount(), 2);
    EXPECT_STREQ(r->Row(0).Field(1).GetCastAs<std::string>().c_str(), "INTEGER");
    EXPECT_STREQ(r->Row(0).Field(2).GetCastAs<std::string>().c_str(), "4");
    EXPECT_STREQ(r->Row(1).Field(1).GetCastAs<std::string>().c_str(), "BIGINT");
    EXPECT_STREQ(r->Row(1).Field(2).GetCastAs<std::string>().c_str(), "8");
}

// bugfix: #710003944 删除表的列后show表，删除的列显示还在
TEST_F(AlterTest, AlterTableDropColumnBug710003944) {
    // rename table
    std::string tablename("table_dropcolumnbug710003944");
    std::string create_tb(fmt::format("CREATE TABLE {} (x INT,y varchar, z bigint);", tablename));
    std::cout << create_tb << std::endl;
    auto result = conn->Query(create_tb.c_str());
    EXPECT_TRUE(result->GetRetCode() == GS_SUCCESS);
    auto table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetColumnCount(), 3);
    EXPECT_EQ(table_info->GetColumnByName("x")->col_type, GStorDataType::GS_TYPE_INTEGER);
    EXPECT_EQ(table_info->GetColumnByName("x")->size, 4);
    EXPECT_EQ(table_info->GetColumnByName("y")->col_type, GStorDataType::GS_TYPE_VARCHAR);
    EXPECT_EQ(table_info->GetColumnByName("y")->size, 65535);
    EXPECT_EQ(table_info->GetColumnByName("z")->col_type, GStorDataType::GS_TYPE_BIGINT);
    EXPECT_EQ(table_info->GetColumnByName("z")->size, 8);

    std::string create_index(fmt::format("create index idx_y on {} (y);", tablename));
    auto result1 = conn->Query(create_index.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    auto table_info1 = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info1 == nullptr);
    EXPECT_EQ(table_info1->GetIndexCount(), 1);
    EXPECT_EQ(table_info1->GetIndexBySlot(0).Name(), "idx_y");
    EXPECT_EQ(table_info1->GetIndexBySlot(0).GetIndex().col_count, 1);
    // drop z , has no index on z
    std::string alter_tb(fmt::format("alter table {} drop y;", tablename));
    auto result2 = conn->Query(alter_tb.c_str());
    // EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);
    ASSERT_NE(result2->GetRetCode(), GS_SUCCESS);
    auto table_info2 = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info2 == nullptr);
    // EXPECT_EQ(table_info2->GetColumnCount(), 2);
    EXPECT_EQ(table_info2->GetColumnCount(), 3);
    EXPECT_EQ(table_info2->GetColumnByName("x")->col_type, GStorDataType::GS_TYPE_INTEGER);
    EXPECT_EQ(table_info2->GetColumnByName("z")->col_type, GStorDataType::GS_TYPE_BIGINT);
    // EXPECT_EQ(table_info2->GetIndexCount(), 0);
    EXPECT_EQ(table_info2->GetIndexCount(), 1);

    std::string show_tb(fmt::format("show {};", tablename));
    auto r = conn->Query(show_tb.c_str());
    EXPECT_TRUE(r->GetRetCode() == GS_SUCCESS);
    ASSERT_EQ(r->RowCount(), 3);
    EXPECT_STREQ(r->Row(0).Field(0).GetCastAs<std::string>().c_str(), "x");
    EXPECT_STREQ(r->Row(0).Field(1).GetCastAs<std::string>().c_str(), "INTEGER");
    EXPECT_STREQ(r->Row(0).Field(2).GetCastAs<std::string>().c_str(), "4");
    EXPECT_STREQ(r->Row(1).Field(0).GetCastAs<std::string>().c_str(), "y");
    EXPECT_STREQ(r->Row(1).Field(1).GetCastAs<std::string>().c_str(), "VARCHAR");
    EXPECT_STREQ(r->Row(1).Field(2).GetCastAs<std::string>().c_str(), "65535");
    EXPECT_STREQ(r->Row(2).Field(0).GetCastAs<std::string>().c_str(), "z");
    EXPECT_STREQ(r->Row(2).Field(1).GetCastAs<std::string>().c_str(), "BIGINT");
    EXPECT_STREQ(r->Row(2).Field(2).GetCastAs<std::string>().c_str(), "8");
    // drop y , has a index on y
    std::string alter_tb1(fmt::format("alter table {} drop z;", tablename));
    auto result3 = conn->Query(alter_tb1.c_str());
    EXPECT_TRUE(result3->GetRetCode() == GS_SUCCESS);
    auto table_info3 = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info3 == nullptr);
    EXPECT_EQ(table_info3->GetColumnCount(), 2);
    EXPECT_EQ(table_info3->GetColumnByName("x")->col_type, GStorDataType::GS_TYPE_INTEGER);
    EXPECT_EQ(table_info3->GetIndexCount(), 1);

    std::string show_tb1(fmt::format("show {};", tablename));
    auto r1 = conn->Query(show_tb1.c_str());
    EXPECT_TRUE(r1->GetRetCode() == GS_SUCCESS);
    ASSERT_EQ(r1->RowCount(), 2);
    EXPECT_STREQ(r1->Row(0).Field(0).GetCastAs<std::string>().c_str(), "x");
    EXPECT_STREQ(r1->Row(0).Field(1).GetCastAs<std::string>().c_str(), "INTEGER");
    EXPECT_STREQ(r1->Row(0).Field(2).GetCastAs<std::string>().c_str(), "4");
    EXPECT_STREQ(r1->Row(1).Field(0).GetCastAs<std::string>().c_str(), "y");
    EXPECT_STREQ(r1->Row(1).Field(1).GetCastAs<std::string>().c_str(), "VARCHAR");
    EXPECT_STREQ(r1->Row(1).Field(2).GetCastAs<std::string>().c_str(), "65535");

    // drop x , the last column in table
    std::string alter_tb2(fmt::format("alter table {} drop x;", tablename));
    auto result4 = conn->Query(alter_tb2.c_str());
    ASSERT_EQ(result4->GetRetCode(), GS_SUCCESS);
    // EXPECT_TRUE(result4->GetRetCode() == GS_ERROR);
    // EXPECT_STREQ(result4->GetRetMsg().c_str(), "Invalid operation,cannot drop all columns in a table");
}

TEST_F(AlterTest, AlterTableRenameTableBugfix) {
    std::string tablename("table_rename_src");
    std::string create_tb(
        fmt::format("CREATE TABLE {} (f1 int default 111,f2 real primary key default -4.32,f3 text default 'hi',f4 "
                    "text default 'abc-123',f5 varchar(10), unique (f1));",
                    tablename));
    std::cout << create_tb << std::endl;
    auto result1 = conn->Query(create_tb.c_str());
    EXPECT_TRUE(result1->GetRetCode() == GS_SUCCESS);
    // rename table
    std::string table_new_name("table_rename_dst");
    std::string rename_table_q(fmt::format("alter table {} rename to {};", tablename, table_new_name));
    std::cout << rename_table_q << std::endl;
    auto result2 = conn->Query(rename_table_q.c_str());
    EXPECT_TRUE(result2->GetRetCode() == GS_SUCCESS);

    auto result3 = conn->Query(create_tb.c_str());
    EXPECT_TRUE(result3->GetRetCode() == GS_SUCCESS);
}

TEST_F(AlterTest, AddConstraint) {
    conn->Query("drop table if exists alter_table");
    conn->Query("create table alter_table (id int , age int , name varchar)");
    conn->Query("insert into alter_table values(1, 23, 'ruo'), (2, 25, 'teng')");
    // test unspported constraint
    auto r = conn->Query("alter table alter_table add constraint alter_table_check check (id > 0)");
    ASSERT_NE(r->GetRetCode(), GS_SUCCESS);

    r = conn->Query("alter table alter_table add constraint alter_table_pri primary key (id)");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);

    // 不可重复添加约束
    r = conn->Query("alter table alter_table add constraint alter_table_pri2 primary key (id)");
    ASSERT_NE(r->GetRetCode(), GS_SUCCESS);

    r = conn->Query("alter table alter_table add constraint alter_table_uni unique (age,name)");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    
    // 获取索引信息
    auto table_info = conn->GetTableInfo("alter_table");
    ASSERT_FALSE(table_info == nullptr);
    EXPECT_EQ(table_info->GetIndexCount(), 2);
    EXPECT_EQ(table_info->GetIndexBySlot(0).Name(), "alter_table_pri");
    EXPECT_EQ(table_info->GetIndexBySlot(0).GetIndex().col_count, 1);
    EXPECT_EQ(table_info->GetIndexBySlot(0).GetIndex().col_ids[0], 0);

    EXPECT_EQ(table_info->GetIndexBySlot(1).Name(), "alter_table_uni");
    EXPECT_EQ(table_info->GetIndexBySlot(1).GetIndex().col_count, 2);
    EXPECT_EQ(table_info->GetIndexBySlot(1).GetIndex().col_ids[0], 1);
    EXPECT_EQ(table_info->GetIndexBySlot(1).GetIndex().col_ids[1], 2);
}

// test alter varchar size 
TEST_F(AlterTest, AlterTableAlterColumnVarcharSize) {
    conn->Query("drop table if exists alter_table");
    conn->Query("create table alter_table (id int , age int , name varchar(10) unique)"); // with unique index
    conn->Query("insert into alter_table values(1, 23, 'ruo'), (2, 25, 'teng'), (3, 25, null)");

    // alter to longger size 
    auto r = conn->Query("alter table alter_table alter name set data type varchar(20)");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);

    // alter to shorter size
    r = conn->Query("alter table alter_table alter name set data type varchar(5)");
    ASSERT_EQ(r->GetRetCode(), GS_SUCCESS);
    
    // alter shorter size that can't hold the data
    r = conn->Query("alter table alter_table alter name set data type varchar(2)");
    ASSERT_NE(r->GetRetCode(), GS_SUCCESS);
}

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(output) = "xml";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
