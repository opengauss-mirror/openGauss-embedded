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
* example_prepared_cc.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/examples/example-cpp/example_prepared_cc.cpp
*
* -------------------------------------------------------------------------
*/
#include "intarkdb.hpp"

int main() {
    try
    {
        std::string path = "./";
        auto db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance(path.c_str()));
        db_instance->Init();

        Connection conn(db_instance);
        conn.Init();

        conn.Query("CREATE TABLE if not exists example_table_pp(id int, name varchar(20), money bigint, time timestamp)");
        // prepare insert
        auto stmt_insert = conn.Prepare("INSERT INTO example_table_pp(id,name,money,time) VALUES (1,?,?,now()), (2,?,?,now())");
        if (stmt_insert->HasError()) {
            std::cout<<"prepare insert error!! errmsg:"<<stmt_insert->ErrorMsg()<<std::endl;
            return -1;
        }
        // bind
        std::vector<Value> bind_values;
        bind_values.push_back(ValueFactory::ValueVarchar("小乖"));
        bind_values.push_back(ValueFactory::ValueBigInt(168000000));
        bind_values.push_back(ValueFactory::ValueVarchar("小宝"));
        bind_values.push_back(ValueFactory::ValueBigInt(88888888));
        // execute
        stmt_insert->Execute(bind_values);

        // prepare select
        auto stmt_select = conn.Prepare("select * from example_table_pp where money>? and time>?");
        if (stmt_select->HasError()) {
            std::cout<<"prepare select error!! errmsg:"<<stmt_select->ErrorMsg()<<std::endl;
            return -1;
        }
        // bind
        bind_values.clear();
        bind_values.push_back(ValueFactory::ValueBigInt(1000000));
        bind_values.push_back(ValueFactory::ValueTimeStamp("2023-12-01 01:01:01"));
        // execute
        auto result = stmt_select->Execute(bind_values);
        // print
        result->Print();
    }
    catch (std::exception &e) {
        std::cout<<"exception:"<<e.what()<<std::endl;
        return -1;
    }
    return 0;
}
