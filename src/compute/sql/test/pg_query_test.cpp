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
* pg_query_test.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/test/pg_query_test.cpp
*
* -------------------------------------------------------------------------
*/
#include <gtest/gtest.h>

#include "postgres_parser.hpp"

TEST(PGQueryTest, ParseSimpleSelect) {
    duckdb::PostgresParser parser;
    parser.Parse("select * from student");
    int size = 0;
    auto tree = parser.parse_tree;
    for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
        size++;
    }
    EXPECT_EQ(size, 1);
}

TEST(PGQueryTest, ParseSubQuerySelect) {
    duckdb::PostgresParser parser;
    parser.Parse("select * from student where sid in (select sid from abc)");
    int size = 0;
    auto tree = parser.parse_tree;
    for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
        size++;
    }
    EXPECT_EQ(size, 1);
}

TEST(PGQueryTest, ParseComplexSelect) {
    std::string sql = R"(WITH cte1 AS (
                            SELECT id, name, age FROM table1 WHERE age > 21), cte2 AS (
                                SELECT id, name, city FROM table2 WHERE city = 'New York')
                            SELECT cte1.id, cte2.name
                            FROM cte1 INNER JOIN cte2 ON cte1.id = cte2.id
                        WHERE cte1.age > 25;)";
    duckdb::PostgresParser parser;
    parser.Parse(sql);
    int size = 0;
    auto tree = parser.parse_tree;
    for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
        size++;
    }
    EXPECT_EQ(size, 1);
}