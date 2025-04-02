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
* example_cc.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/examples/example-cpp/example_cc.cpp
*
* -------------------------------------------------------------------------
*/
#include "intarkdb.hpp"

void PrintResult(std::unique_ptr<RecordBatch> result);

int main() {
    try
    {
        std::string path = "./";
        auto db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance(path.c_str()));
        db_instance->Init();

        Connection conn(db_instance);
        conn.Init();

        conn.Query("CREATE TABLE if not exists example_table_cc(id int, name varchar(20), money bigint)");
        conn.Query("INSERT INTO example_table_cc VALUES (1, '小贺', 100000000)");
        conn.Query("INSERT INTO example_table_cc VALUES (2, '小帅', 200000000)");
        auto result = conn.Query("SELECT * FROM example_table_cc");
        // print method 1
        // result->Print();     // can print

        // print method 2
        PrintResult(std::move(result));
    }
    catch (std::exception &e) {
        std::cout<<"exception:"<<e.what()<<std::endl;
        return -1;
    }
    return 0;
}

void PrintResult(std::unique_ptr<RecordBatch> result) {
    // print cloumn name
    const auto& schema_columns = result->GetSchema().GetColumnInfos();
    for (const auto& column_def : schema_columns) {
        const auto& column_name = column_def.GetColNameWithoutTableName();
        std::cout<<column_name<<"        ";
    }
    std::cout<<std::endl;

    // print resultset
    size_t row_count = result->RowCount();
    size_t column_count = result->ColumnCount();
    for (size_t i = 0; i < row_count; i++) {
        auto row = result->Row(i);
        for (size_t j = 0; j < column_count; j++) {
            auto col = row.Field(j);
            std::cout<<col.ToString()<<"        ";
        }
        std::cout<<std::endl;
    }
}
