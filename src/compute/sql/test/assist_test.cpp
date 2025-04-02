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
* assist_test.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/test/assist_test.cpp
*
* -------------------------------------------------------------------------
*/

#include <gtest/gtest.h>
#include <string_view>
#include <iomanip>
#include <algorithm>
#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "main/connection.h"
#include "main/database.h"
#include <fmt/core.h>
#include <fmt/format.h>
#include "type/type_system.h"
#include "type/type_str.h"

class AssistTest {
 public:
    AssistTest(){ 
        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance->Init();
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();
        
    }
    ~AssistTest(){
        conn.reset();
    }

    void GetTableInfo(const std::string& tablename);

private:
    std::shared_ptr<IntarkDB> db_instance;
    std::unique_ptr<Connection> conn;
};

void AssistTest::GetTableInfo(const std::string& tablename) {
    if (tablename.size() == 0){
        std::cout << "Please input the table name!" << std::endl;
        return;
    }
    std::unique_ptr<TableInfo> table_info = nullptr;
    try{
        table_info = conn->GetTableInfo(tablename);
    } catch (const std::exception& ex) {
        std::cout << ex.what() << std::endl;
        return;
    }
    if (table_info == nullptr){
        std::cout << tablename << " get failed! please check." << std::endl;
        return;
    }
    TableInfo& info = *table_info.get();
    std::cout << "table name: " << info.table_name << std::endl;
    std::cout << "column count: " << info.GetColumnCount() << ", index count: " << info.GetIndexCount() << std::endl;
    std::cout << "-----------------------------------------------------------------------------------------------------------------" << std::endl;
    std::cout << std::setiosflags(std::ios::left) << std::setw(5) << " id"
        << std::setw(15) << "| column name" 
        << std::setw(14) << "| column type" 
        << std::setw(7) << "| size"
        << std::setw(11) << "| nullable"
        << std::setw(13) << "| is primary"
        << std::setw(13) << "| is default"
        << std::setw(16) << "| default value"
        << std::setw(12) << "| precision"
        << std::setw(8) << "| scale"
        << std::resetiosflags(std::ios::left) << std::endl;
    std::cout << "-----------------------------------------------------------------------------------------------------------------" << std::endl;
    const exp_table_meta& meta = info.GetTableMetaInfo();
    for (uint32 i=0; i< meta.column_count; i++){
        const exp_column_def_t& col = meta.columns[i];
        Value default_value(col.col_type,col.default_val,col.scale,col.precision);
        std::cout << std::setiosflags(std::ios::left) << std::setw(5) << std::to_string(col.col_slot)
            << std::setw(15) << "| " + (std::string)col.name.str 
            << std::setw(14) << "| " + (std::string)fmt::format("{}", col.col_type)
            << std::setw(7) << "| " + std::to_string(col.size)
            << std::setw(11) << "| " + (std::string)(col.nullable == true ? "true" : "false")
            << std::setw(13) << "| " + (std::string)(col.is_primary == true ? "true" : "false")
            << std::setw(13) << "| " + (std::string)(col.is_default == true ? "true" : "false")
            << std::setw(16) << "| " + (col.is_default ? default_value.ToString() : "")
            << std::setw(12) << "| " + std::to_string(col.precision)
            << std::setw(8) << "| " + std::to_string(col.scale)
            << std::resetiosflags(std::ios::left) << std::endl; 
        std::cout << "-----------------------------------------------------------------------------------------------------------------" << std::endl;
    }
}


int main(int argc, char **argv) {
    if (argc == 2){
        std::cout << "table name: " << argv[1] << std::endl;
        AssistTest assist_test;
        assist_test.GetTableInfo(argv[1]);
        return 0;
    }else{
        std::cout << "please input a table name!" << std::endl;
        return 0;
    }
}
