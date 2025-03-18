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
* pg_type_test.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/test/pg_type_test.cpp
*
* -------------------------------------------------------------------------
*/

#include <gtest/gtest.h>
#include "postgres_parser.hpp"
#include "nodes/parsenodes.hpp"
#include "binder/transform_typename.h"
#include <string_view>
#include <iomanip>
#include <algorithm>

constexpr std::string_view sql_col_type_list[] = {
    "TINYINT","UTINYINT","SMALLINT","USMALLINT","INT2","SHORT","INTEGER",
    "INT4","INT","MEDIUMINT","SIGNED","BIGINT","INT8","LONG",
    "DOUBLE","NUMERIC","DECIMAL","DECIMAL(5, 2)","DATE","TIMESTAMP","DATETIME",
    "CHAR","BPCHAR","VARCHAR","TEXT","STRING","BOOLEAN","BOOL",
    "LOGICAL","REAL","FLOAT4","FLOAT","BINARY","BLOB","BYTEA",
    "VARBINARY","NUMBER"};

std::string TestLower(const std::string &str) {
	std::string copy(str);
	std::transform(copy.begin(), copy.end(), copy.begin(), [](unsigned char c) { return std::tolower(c); });
	return (copy);
}

void show_print(const std::string& sql_keyword, const std::string& after_pg_parse, 
                const std::string& gs_type_id, std::string_view gs_type_name){
    std::cout << "--------------------------------------------------------------------------------" << std::endl;
    std::cout << std::setiosflags(std::ios::left) << std::setw(20) << "sql keyword"
        << std::setw(20) << "| after pg parse" 
        << std::setw(20) << "| gs type id" 
        << std::setw(20) << "| gs type name"
        << std::resetiosflags(std::ios::left) << std::endl;
    std::cout << "--------------------------------------------------------------------------------" << std::endl;
    std::cout << std::setiosflags(std::ios::left) << std::setw(20) << sql_keyword
        << std::setw(20) << "| " << after_pg_parse 
        << std::setw(20) << "| " << gs_type_id
        << std::setw(20) << "| " << gs_type_name
        << std::resetiosflags(std::ios::left) << std::endl; 
    std::cout << "--------------------------------------------------------------------------------" << std::endl;
}

class PgTypeTest : public testing::TestWithParam<std::string_view> {
public:
    PgTypeTest():typeindex(0){
    }
    ~PgTypeTest(){}

    static void SetUpTestSuite() {
        
    }
    static void TearDownTestSuite() {}
    
    void SetUp() override {}
    void TearDown() override {
        typeindex++;
    }

    uint32 typeindex;
};

TEST_P(PgTypeTest, PgParseSupportType) {
    auto sql_type_name = GetParam();
    std::string sql = fmt::format("create table type_test_table (t {});", sql_type_name);
    std::cout << sql << std::endl;
    duckdb::PostgresParser parser;
    try{
        parser.Parse(sql);
    } catch (const std::exception& ex) {
        std::cout << ex.what() << std::endl;
        ASSERT_FALSE(true) << sql_type_name << " pg do not supported!" << std::endl;
    }
    int size = 0;
    auto tree = parser.parse_tree;
    if (tree == nullptr){
        std::cout << sql_type_name << " sql-engine do not supported!" << std::endl;
        ASSERT_FALSE(true);
    }
    for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
        size++;
    }
    EXPECT_EQ(size, 1);

    auto stmt = reinterpret_cast<duckdb_libpgquery::PGNode *>(tree->head->data.ptr_value);
    EXPECT_EQ(stmt->type, duckdb_libpgquery::T_PGRawStmt);

    auto rawstmt = reinterpret_cast<duckdb_libpgquery::PGRawStmt *>(stmt)->stmt;
    EXPECT_EQ(rawstmt->type, duckdb_libpgquery::T_PGCreateStmt);

    auto createtmt = reinterpret_cast<duckdb_libpgquery::PGCreateStmt *>(rawstmt);
    auto c = createtmt->tableElts->head;
    auto node = reinterpret_cast<duckdb_libpgquery::PGNode *>(c->data.ptr_value);
    ASSERT_EQ(node->type, duckdb_libpgquery::T_PGColumnDef);

    auto cdef = reinterpret_cast<duckdb_libpgquery::PGColumnDef *>(c->data.ptr_value);
    auto type_name_str = std::string(reinterpret_cast<duckdb_libpgquery::PGValue *>(cdef->typeName->names->tail->data.ptr_value)->val.str);
    auto col_name = std::string(cdef->colname);
    auto type_name = TestLower(type_name_str);
    uint32_t index = 0;
    for (; internal_types[index].name != nullptr; index++) {
        if (internal_types[index].name == type_name) {
            break;
        }
    }
    if (internal_types[index].name == nullptr){
        std::cout << "pg parse suc, but sql-engine not support." << std::endl;
        show_print(sql_type_name.data(), type_name, "-", "-");
        EXPECT_TRUE(false) << sql_type_name << "-" << type_name << ", pg parse suc, but sql-engine not support." << std::endl;
    }else{
        std::cout << "the type keyword support!" << std::endl;
        show_print(sql_type_name.data(), type_name, std::to_string((int)internal_types[index].type), internal_types[index].gs_type_name);
        EXPECT_TRUE(true) << sql_type_name << "-" << type_name << internal_types[index].type << "-" << internal_types[index].gs_type_name << std::endl;
    }
}

INSTANTIATE_TEST_SUITE_P(PgqueryTypeTest,
                         PgTypeTest,
                         testing::ValuesIn(sql_col_type_list));

void singleTypeTest(const std::string& sql_type_name){
    std::string sql = fmt::format("create table type_test_table (t {});", sql_type_name);
    std::cout << sql << std::endl;
    duckdb::PostgresParser parser;
    try{
        parser.Parse(sql);
    } catch (const std::exception& ex) {
        std::cout << ex.what() << std::endl;
        return;
    }
    int size = 0;
    auto tree = parser.parse_tree;
    if (tree == nullptr){
        std::cout << sql_type_name << " after parse do not supported!" << std::endl;
        show_print(sql_type_name, "-", "-", "-");
        return;
    }
    for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
        size++;
    }
    auto stmt = reinterpret_cast<duckdb_libpgquery::PGNode *>(tree->head->data.ptr_value);
    assert(stmt->type==duckdb_libpgquery::T_PGRawStmt);

    auto rawstmt = reinterpret_cast<duckdb_libpgquery::PGRawStmt *>(stmt)->stmt;
    assert(rawstmt->type==duckdb_libpgquery::T_PGCreateStmt);

    auto createtmt = reinterpret_cast<duckdb_libpgquery::PGCreateStmt *>(rawstmt);
    auto c = createtmt->tableElts->head;
    auto node = reinterpret_cast<duckdb_libpgquery::PGNode *>(c->data.ptr_value);
    assert(node->type==duckdb_libpgquery::T_PGColumnDef);

    auto cdef = reinterpret_cast<duckdb_libpgquery::PGColumnDef *>(c->data.ptr_value);
    auto type_name_str = std::string(reinterpret_cast<duckdb_libpgquery::PGValue *>(cdef->typeName->names->tail->data.ptr_value)->val.str);
    auto col_name = std::string(cdef->colname);
    auto type_name = TestLower(type_name_str);
    uint32_t index = 0;
    for (; internal_types[index].name != nullptr; index++) {
        if (internal_types[index].name == type_name) {
            break;
        }
    }
    if (internal_types[index].name == nullptr){
        std::cout << "pg parse suc, but sql-engine not support." << std::endl;
        show_print(sql_type_name, type_name, "-", "-");
    }else{
        std::cout << "the type keyword support!" << std::endl;
        show_print(sql_type_name, type_name, std::to_string((int)internal_types[index].type), internal_types[index].gs_type_name);
    }
}

int main(int argc, char **argv) {
    if (argc <= 1){
        ::testing::GTEST_FLAG(output) = "json";
        ::testing::InitGoogleTest(&argc, argv);
        return RUN_ALL_TESTS();
    }else if (argc == 2){
        std::cout << "the type keyword: " << argv[1] << std::endl;
        singleTypeTest(argv[1]);
        return 0;
    }else{
        std::cout << "please input 0/1 type keyword!" << std::endl;
        return 0;
    }
}
