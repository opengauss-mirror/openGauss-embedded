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
* partition_test.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/test/partition_test.cpp
*
* -------------------------------------------------------------------------
*/
// test for partition
#include <gtest/gtest.h>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "main/connection.h"
#include "main/database.h"


typedef enum TimeOutputType {
    TIME_OUTPUT_TYPE_HUMAN = 1,
    TIME_OUTPUT_TYPE_PARTKEY_HOUR = 2,
    TIME_OUTPUT_TYPE_PARTKEY_DAY = 3
}TimeOutputType_e;

// output: like"2024-10-15 02:12:17"
std::string GetNowTime(TimeOutputType_e type){
    // 获取当前时间点
    auto currentTime = std::chrono::system_clock::now();
    // 将时间点转换为 time_t 类型
    std::time_t tt = std::chrono::system_clock::to_time_t(currentTime);
    auto time_tm = localtime(&tt);
	char strTime[25] = { 0 };
    if (type == TIME_OUTPUT_TYPE_HUMAN){
        sprintf(strTime, "%d-%02d-%02d %02d:%02d:%02d", time_tm->tm_year + 1900,
            time_tm->tm_mon + 1, time_tm->tm_mday, time_tm->tm_hour,
            time_tm->tm_min, time_tm->tm_sec);
    }else if (type == TIME_OUTPUT_TYPE_PARTKEY_HOUR){
        sprintf(strTime, "%d%02d%02d%02d", time_tm->tm_year + 1900,
            time_tm->tm_mon + 1, time_tm->tm_mday, time_tm->tm_hour);
    }else {
        sprintf(strTime, "%d%02d%02d", time_tm->tm_year + 1900,
            time_tm->tm_mon + 1, time_tm->tm_mday);
    }
	
    return std::string(strTime);
}

class PartitionTest : public ::testing::Test {
 protected:
    PartitionTest(){ 
        
    }
    ~PartitionTest(){
        
    }
    static void SetUpTestSuite() {
        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance->Init();
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();

        // sleep for a while to wait for db to start
        sleep(1);  
    }

    // Per-test-suite tear-down.
    // Called after the last test in this test suite.
    // Can be omitted if not needed.
    static void TearDownTestSuite() {
        conn.reset();
    }
    
    void SetUp() override {
    }

    //void TearDown() override {}

    static std::shared_ptr<IntarkDB> db_instance;
    static std::unique_ptr<Connection> conn;
};

std::shared_ptr<IntarkDB> PartitionTest::db_instance = nullptr;
std::unique_ptr<Connection> PartitionTest::conn = nullptr;

TEST(PartitionParseTest, TimescaleParse) {
    duckdb::PostgresParser parser;
    parser.Parse("CREATE TABLE tbp_timescale (id integer, date timestamp) TIMESCALE;");
    int size = 0;
    auto tree = parser.parse_tree;
    for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
        size++;
    }
    EXPECT_EQ(size, 1);
}

TEST(PartitionParseTest, AllParamParse) {
    duckdb::PostgresParser parser;
    parser.Parse("CREATE TABLE tbp_allparam (id int,date timestamp,value int) PARTITION BY RANGE(date) timescale interval '1d' retention '30d' autopart crosspart;");
    int size = 0;
    auto tree = parser.parse_tree;
    for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
        size++;
    }
    EXPECT_EQ(size, 1);
}

TEST(PartitionParseTest,AlterTableAddPartParse) {
    duckdb::PostgresParser parser;
    parser.Parse("alter table mytable3 add partition mytable3_2023082218;");
    int size = 0;
    auto tree = parser.parse_tree;
    for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
        size++;
    }
    EXPECT_EQ(size, 1);
}

TEST(PartitionParseTest,AlterTableDropPartParse) {
    duckdb::PostgresParser parser;
    parser.Parse("alter table mytable3 drop partition mytable3_2023082218;");
    int size = 0;
    auto tree = parser.parse_tree;
    for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
        size++;
    }
    EXPECT_EQ(size, 1);
}

TEST_F(PartitionTest, AllParamCreateTable) {
    std::string tablename("tbp_all_params");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp,value int) PARTITION BY RANGE(date) timescale interval '1d' retention '30d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_SUCCESS);
    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info != nullptr);
    //EXPECT_EQ(table_info->GetTableMetaInfo().appendonly, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().is_timescale, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().has_retention, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().retention.day, 30);
    EXPECT_EQ(table_info->GetTableMetaInfo().parted, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.auto_addpart, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.is_crosspart, true);
    EXPECT_STREQ(table_info->GetTableMetaInfo().part_table.desc.interval.str, "1d");
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partkeys, 1);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.parttype, PART_TYPE_RANGE);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partcnt, 0);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_STREQ(table_info->GetIndexBySlot(0).Name().c_str(), (tablename+"date").c_str());
}

TEST_F(PartitionTest, AddPartitionIntervalWithDay) {
    std::string tablename("tbp_add_part_day");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp,value int) PARTITION BY RANGE(date) timescale interval '1d' retention '30d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_SUCCESS);
    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info != nullptr);
    //EXPECT_EQ(table_info->GetTableMetaInfo().appendonly, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().is_timescale, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().has_retention, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().retention.day, 30);
    EXPECT_EQ(table_info->GetTableMetaInfo().parted, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.auto_addpart, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.is_crosspart, true);
    EXPECT_STREQ(table_info->GetTableMetaInfo().part_table.desc.interval.str, "1d");
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partkeys, 1);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.parttype, PART_TYPE_RANGE);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partcnt, 0);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_STREQ(table_info->GetIndexBySlot(0).Name().c_str(), (tablename+"date").c_str());

    std::string part1(tablename+"_20230822");
    std::string addp1(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part1));
    auto result1 = conn->Query(addp1.c_str());
    EXPECT_TRUE(result1->GetRetCode()==GS_SUCCESS);
    auto table_info1 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetTableMetaInfo().part_table.desc.partcnt, 1);

    std::string part2(tablename+"_20230823");
    std::string addp2(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part2));
    auto result2 = conn->Query(addp2.c_str());
    EXPECT_TRUE(result2->GetRetCode()==GS_SUCCESS);
    auto table_info2 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info2 != nullptr);
    EXPECT_EQ(table_info2->GetTableMetaInfo().part_table.desc.partcnt, 2);

    std::string part3(tablename+"_20230824");
    std::string addp3(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part3));
    auto result3 = conn->Query(addp3.c_str());
    EXPECT_TRUE(result3->GetRetCode()==GS_SUCCESS);
    auto table_info3 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info3 != nullptr);
    EXPECT_EQ(table_info3->GetTableMetaInfo().part_table.desc.partcnt, 3);

    std::string part4(tablename+"_20230820");
    std::string addp4(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part4));
    auto result4 = conn->Query(addp4.c_str());
    EXPECT_TRUE(result4->GetRetCode()==GS_SUCCESS);
    auto table_info4 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info4 != nullptr);
    EXPECT_EQ(table_info4->GetTableMetaInfo().part_table.desc.partcnt, 4);

    std::string part5(tablename+"_2023082510");  // hour
    std::string addp5(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part5));
    auto result5 = conn->Query(addp5.c_str());
    EXPECT_TRUE(result5->GetRetCode()==GS_ERROR);
    auto table_info5 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info5 != nullptr);
    EXPECT_EQ(table_info5->GetTableMetaInfo().part_table.desc.partcnt, 4);
    // drop
    std::string part6(tablename+"_20230820");
    std::string addp6(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part6));
    auto result6 = conn->Query(addp6.c_str());
    EXPECT_TRUE(result6->GetRetCode()==GS_SUCCESS);
    auto table_info6 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info6 != nullptr);
    EXPECT_EQ(table_info6->GetTableMetaInfo().part_table.desc.partcnt, 3);
    // drop
    std::string part7(tablename+"_20230820");
    std::string addp7(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part7));
    auto result7 = conn->Query(addp7.c_str());
    EXPECT_TRUE(result7->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result7->GetRetMsg().c_str(), 
                   fmt::format("Binder Error: part {} not exists!", part7).c_str());
    // drop
    std::string part8(tablename+"_2023082510");
    std::string addp8(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part8));
    auto result8 = conn->Query(addp8.c_str());
    EXPECT_TRUE(result8->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result8->GetRetMsg().c_str(), 
                   fmt::format("Binder Error: part {} not exists!", part8).c_str());

    std::string part9(tablename+"_20230822");
    std::string addp9(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part9));
    auto result9 = conn->Query(addp9.c_str());
    EXPECT_TRUE(result9->GetRetCode()==GS_SUCCESS);
    auto table_info9 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info9 != nullptr);
    EXPECT_EQ(table_info9->GetTableMetaInfo().part_table.desc.partcnt, 2);

    std::string part10(tablename+"_20230823");
    std::string addp10(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part10));
    auto result10 = conn->Query(addp10.c_str());
    EXPECT_TRUE(result10->GetRetCode()==GS_SUCCESS);
    auto table_info10 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info10 != nullptr);
    EXPECT_EQ(table_info10->GetTableMetaInfo().part_table.desc.partcnt, 1);

    std::string part11(tablename+"_20230824");
    std::string addp11(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part11));
    auto result11 = conn->Query(addp11.c_str());
    EXPECT_TRUE(result11->GetRetCode()==GS_SUCCESS);
    auto table_info11 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info11 != nullptr);
    EXPECT_EQ(table_info11->GetTableMetaInfo().part_table.desc.partcnt, 0);
}

TEST_F(PartitionTest, AddPartitionIntervalWithHour) {
    std::string tablename("tbp_add_part_hour");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp,value int) PARTITION BY RANGE(date) timescale interval '1h' retention '30d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_SUCCESS);
    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info != nullptr);
    //EXPECT_EQ(table_info->GetTableMetaInfo().appendonly, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().is_timescale, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().has_retention, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().retention.day, 30);
    EXPECT_EQ(table_info->GetTableMetaInfo().parted, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.auto_addpart, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.is_crosspart, true);
    EXPECT_STREQ(table_info->GetTableMetaInfo().part_table.desc.interval.str, "1h");
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partkeys, 1);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.parttype, PART_TYPE_RANGE);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partcnt, 0);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_STREQ(table_info->GetIndexBySlot(0).Name().c_str(), (tablename+"date").c_str());

    std::string part1(tablename+"_2023082210");
    std::string addp1(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part1));
    auto result1 = conn->Query(addp1.c_str());
    EXPECT_TRUE(result1->GetRetCode()==GS_SUCCESS);
    auto table_info1 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetTableMetaInfo().part_table.desc.partcnt, 1);

    std::string part2(tablename+"_2023082211");
    std::string addp2(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part2));
    auto result2 = conn->Query(addp2.c_str());
    EXPECT_TRUE(result2->GetRetCode()==GS_SUCCESS);
    auto table_info2 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info2 != nullptr);
    EXPECT_EQ(table_info2->GetTableMetaInfo().part_table.desc.partcnt, 2);

    std::string part3(tablename+"_2023082212");
    std::string addp3(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part3));
    auto result3 = conn->Query(addp3.c_str());
    EXPECT_TRUE(result3->GetRetCode()==GS_SUCCESS);
    auto table_info3 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info3 != nullptr);
    EXPECT_EQ(table_info3->GetTableMetaInfo().part_table.desc.partcnt, 3);

    std::string part4(tablename+"_2023082209");
    std::string addp4(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part4));
    auto result4 = conn->Query(addp4.c_str());
    EXPECT_TRUE(result4->GetRetCode()==GS_SUCCESS);
    auto table_info4 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info4 != nullptr);
    EXPECT_EQ(table_info4->GetTableMetaInfo().part_table.desc.partcnt, 4);

    std::string part5(tablename+"_20230823");  // day
    std::string addp5(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part5));
    auto result5 = conn->Query(addp5.c_str());
    EXPECT_TRUE(result5->GetRetCode()==GS_ERROR);
    auto table_info5 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info5 != nullptr);
    EXPECT_EQ(table_info5->GetTableMetaInfo().part_table.desc.partcnt, 4);
    // drop
    std::string part6(tablename+"_2023082209");
    std::string addp6(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part6));
    auto result6 = conn->Query(addp6.c_str());
    EXPECT_TRUE(result6->GetRetCode()==GS_SUCCESS);
    auto table_info6 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info6 != nullptr);
    EXPECT_EQ(table_info6->GetTableMetaInfo().part_table.desc.partcnt, 3);
    // drop
    std::string part7(tablename+"_2023082209");
    std::string addp7(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part7));
    auto result7 = conn->Query(addp7.c_str());
    EXPECT_TRUE(result7->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result7->GetRetMsg().c_str(), 
                   fmt::format("Binder Error: part {} not exists!", part7).c_str());
    // drop
    std::string part8(tablename+"_20230822");
    std::string addp8(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part8));
    auto result8 = conn->Query(addp8.c_str());
    EXPECT_TRUE(result8->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result8->GetRetMsg().c_str(), 
                   fmt::format("Binder Error: part {} not exists!", part8).c_str());

    std::string part9(tablename+"_2023082210");
    std::string addp9(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part9));
    auto result9 = conn->Query(addp9.c_str());
    EXPECT_TRUE(result9->GetRetCode()==GS_SUCCESS);
    auto table_info9 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info9 != nullptr);
    EXPECT_EQ(table_info9->GetTableMetaInfo().part_table.desc.partcnt, 2);

    std::string part10(tablename+"_2023082211");
    std::string addp10(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part10));
    auto result10 = conn->Query(addp10.c_str());
    EXPECT_TRUE(result10->GetRetCode()==GS_SUCCESS);
    auto table_info10 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info10 != nullptr);
    EXPECT_EQ(table_info10->GetTableMetaInfo().part_table.desc.partcnt, 1);

    std::string part11(tablename+"_2023082212");
    std::string addp11(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part11));
    auto result11 = conn->Query(addp11.c_str());
    EXPECT_TRUE(result11->GetRetCode()==GS_SUCCESS);
    auto table_info11 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info11 != nullptr);
    EXPECT_EQ(table_info11->GetTableMetaInfo().part_table.desc.partcnt, 0);
}

TEST_F(PartitionTest, PartitionIndexTest) {
    std::string tablename("tbp_index");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp,value int) PARTITION BY RANGE(date) timescale interval '1h' retention '30d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_SUCCESS);
    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info != nullptr);
    //EXPECT_EQ(table_info->GetTableMetaInfo().appendonly, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().is_timescale, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().has_retention, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().retention.day, 30);
    EXPECT_EQ(table_info->GetTableMetaInfo().parted, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.auto_addpart, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.is_crosspart, true);
    EXPECT_STREQ(table_info->GetTableMetaInfo().part_table.desc.interval.str, "1h");
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partkeys, 1);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.parttype, PART_TYPE_RANGE);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partcnt, 0);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_STREQ(table_info->GetIndexBySlot(0).Name().c_str(), (tablename+"date").c_str());

    std::string part1(tablename+"_2023082210");
    std::string addp1(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part1));
    auto result1 = conn->Query(addp1.c_str());
    EXPECT_TRUE(result1->GetRetCode()==GS_SUCCESS);
    auto table_info1 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetTableMetaInfo().part_table.desc.partcnt, 1);

    std::string addp2(fmt::format("INSERT INTO {} VALUES (1, '2023-08-22 10:11:12', 44), (2, '2023-08-22 10:21:22', 55), (3, '2023-08-22 10:31:32', 66), (4, '2023-08-22 10:41:32', 77);", tablename));
    auto result2 = conn->Query(addp2.c_str());
    EXPECT_TRUE(result2->GetRetCode()==GS_SUCCESS);

    std::string addp3(fmt::format("SELECT * FROM {} WHERE date > '2023-08-22 10:11:22';", tablename));
    auto result3 = conn->Query(addp3.c_str());
    EXPECT_TRUE(result3->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result3->RowCount(), 3);
    ASSERT_EQ(result3->stmt_props.read_tables.size(),1);
    auto read_table = result3->stmt_props.read_tables.begin();
    EXPECT_STREQ(read_table->first.c_str(), tablename.c_str());
    EXPECT_EQ(read_table->second.table_type, StatementProps::TableType::TABLE_TYPE_TIMESCALE);

    std::string part4(tablename+"_2023082218");
    std::string addp4(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part4));
    auto result4 = conn->Query(addp4.c_str());
    EXPECT_TRUE(result4->GetRetCode()==GS_SUCCESS);
    auto table_info4 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info4 != nullptr);
    EXPECT_EQ(table_info4->GetTableMetaInfo().part_table.desc.partcnt, 2);

    std::string addp5(fmt::format("insert into {} values(1,'2023-08-22 18:01:00',55);", tablename));
    auto result5 = conn->Query(addp5.c_str());
    EXPECT_TRUE(result5->GetRetCode()==GS_SUCCESS);

    std::string addp6(fmt::format("SELECT * FROM {} WHERE date > '2023-08-22 10:11:22';", tablename));
    auto result6 = conn->Query(addp6.c_str());
    EXPECT_TRUE(result6->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result6->RowCount(), 4);

    std::string addp7(fmt::format("SELECT * FROM {};", tablename));
    auto result7 = conn->Query(addp7.c_str());
    EXPECT_TRUE(result7->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result7->RowCount(), 5);

    std::string part8(tablename+"_2023082210");
    std::string addp8(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part8));
    auto result8 = conn->Query(addp8.c_str());
    EXPECT_TRUE(result8->GetRetCode()==GS_SUCCESS);
    auto table_info8 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info8 != nullptr);
    EXPECT_EQ(table_info8->GetTableMetaInfo().part_table.desc.partcnt, 1);

    std::string addp9(fmt::format("SELECT * FROM {};", tablename));
    auto result9 = conn->Query(addp9.c_str());
    EXPECT_TRUE(result9->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result9->RowCount(), 1);
}

TEST_F(PartitionTest, CreateTableWithTimestampHasPrimaryKey) {
    std::string tablename("tbp_primary");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp primary key,value int) PARTITION BY RANGE(date) timescale interval '1d' retention '30d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result->GetRetMsg().c_str(), "Binder Error: timescale table can not have the primary key or unique key!");
}

TEST_F(PartitionTest, CreateTableWithTimestampHasUnique) {
    std::string tablename("tbp_unique");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp unique,value int) PARTITION BY RANGE(date) timescale interval '1d' retention '30d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result->GetRetMsg().c_str(), "Binder Error: timescale table can not have the primary key or unique key!");
}

TEST_F(PartitionTest, PartitionIndexTestOtherIndex) {
    std::string tablename("tbp_index_other");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp,value int) PARTITION BY RANGE(date) timescale interval '1h' retention '30d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_SUCCESS);
    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info != nullptr);
    //EXPECT_EQ(table_info->GetTableMetaInfo().appendonly, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().is_timescale, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().has_retention, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().retention.day, 30);
    EXPECT_EQ(table_info->GetTableMetaInfo().parted, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.auto_addpart, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.is_crosspart, true);
    EXPECT_STREQ(table_info->GetTableMetaInfo().part_table.desc.interval.str, "1h");
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partkeys, 1);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.parttype, PART_TYPE_RANGE);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partcnt, 0);
    EXPECT_EQ(table_info->GetIndexCount(), 1);
    EXPECT_STREQ(table_info->GetIndexBySlot(0).Name().c_str(), (tablename+"date").c_str());

    std::string index_name("idx_name_tbp_index_other");
    std::string crt_idx2(fmt::format("CREATE INDEX {} on {} (id);", index_name, tablename));
    auto resultidx = conn->Query(crt_idx2.c_str());
    EXPECT_TRUE(resultidx->GetRetCode()==GS_SUCCESS);
    auto idxtable_info = conn->GetTableInfo(tablename);
    EXPECT_TRUE(idxtable_info != nullptr);
    EXPECT_EQ(idxtable_info->GetIndexCount(), 2);
    EXPECT_STREQ(idxtable_info->GetIndexBySlot(1).Name().c_str(), index_name.c_str());

    std::string part1(tablename+"_2023082210");
    std::string addp1(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part1));
    auto result1 = conn->Query(addp1.c_str());
    EXPECT_TRUE(result1->GetRetCode()==GS_SUCCESS);
    auto table_info1 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info1 != nullptr);
    EXPECT_EQ(table_info1->GetTableMetaInfo().part_table.desc.partcnt, 1);

    std::string addp2(fmt::format("INSERT INTO {} VALUES (1, '2023-08-22 10:11:12', 44), (2, '2023-08-22 10:21:22', 55), (3, '2023-08-22 10:31:32', 66), (4, '2023-08-22 10:41:32', 77);", tablename));
    auto result2 = conn->Query(addp2.c_str());
    EXPECT_TRUE(result2->GetRetCode()==GS_SUCCESS);

    std::string addp3(fmt::format("SELECT * FROM {} WHERE date > '2023-08-22 10:11:22';", tablename));
    auto result3 = conn->Query(addp3.c_str());
    EXPECT_TRUE(result3->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result3->RowCount(), 3);

    std::string part4(tablename+"_2023082218");
    std::string addp4(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part4));
    auto result4 = conn->Query(addp4.c_str());
    EXPECT_TRUE(result4->GetRetCode()==GS_SUCCESS);
    auto table_info4 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info4 != nullptr);
    EXPECT_EQ(table_info4->GetTableMetaInfo().part_table.desc.partcnt, 2);

    std::string addp5(fmt::format("insert into {} values(1,'2023-08-22 18:01:00',55);", tablename));
    auto result5 = conn->Query(addp5.c_str());
    EXPECT_TRUE(result5->GetRetCode()==GS_SUCCESS);

    std::string addp6(fmt::format("SELECT * FROM {} WHERE date > '2023-08-22 10:11:22';", tablename));
    auto result6 = conn->Query(addp6.c_str());
    EXPECT_TRUE(result6->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result6->RowCount(), 4);

    std::string addp7(fmt::format("SELECT * FROM {};", tablename));
    auto result7 = conn->Query(addp7.c_str());
    EXPECT_TRUE(result7->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result7->RowCount(), 5);

    std::string selectp(fmt::format("SELECT * FROM {} where id = 1;", tablename));
    auto resultsp = conn->Query(selectp.c_str());
    EXPECT_TRUE(resultsp->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(resultsp->RowCount(), 2);

    std::string part8(tablename+"_2023082210");
    std::string addp8(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part8));
    auto result8 = conn->Query(addp8.c_str());
    EXPECT_TRUE(result8->GetRetCode()==GS_SUCCESS);
    auto table_info8 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info8 != nullptr);
    EXPECT_EQ(table_info8->GetTableMetaInfo().part_table.desc.partcnt, 1);

    std::string addp9(fmt::format("SELECT * FROM {};", tablename));
    auto result9 = conn->Query(addp9.c_str());
    EXPECT_TRUE(result9->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result9->RowCount(), 1);
}

// INTERVAL must be larger than 0
TEST_F(PartitionTest, CreateTableWithTimestampBug709853174_1) {
    std::string tablename("tbp_intervalpositive1");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp unique,value int) PARTITION BY RANGE(date) timescale interval '-1d' retention '30d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result->GetRetMsg().c_str(), "the interval must be  1h/1d, but now is -1d!");
}

TEST_F(PartitionTest, CreateTableWithTimestampBug709853174_2) {
    std::string tablename("tbp_intervalpositive2");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp unique,value int) PARTITION BY RANGE(date) timescale interval '0d' retention '30d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result->GetRetMsg().c_str(), "the interval must be  1h/1d, but now is 0d!");
}

TEST_F(PartitionTest, CreateTableWithTimestampBug709853174_3) {
    std::string tablename("tbp_intervalpositive3");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp unique,value int) PARTITION BY RANGE(date) timescale interval '1' retention '30d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result->GetRetMsg().c_str(), "the interval must be  1h/1d, but now is 1!");
}

TEST_F(PartitionTest, CreateTableWithTimestampMissingInterval) {
    std::string tablename("tbp_missinginterval");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp unique,value int) PARTITION BY RANGE(date) timescale retention '30d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result->GetRetMsg().c_str(), "timescale table missing interval!");
}

TEST_F(PartitionTest, CreateTableWithTimestampBug709846727_1) {
    std::string tablename("tbp_retentionpositive1");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp,value int) PARTITION BY RANGE(date) timescale interval '1d' retention '-30d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result->GetRetMsg().c_str(), "the number prefix of retention -30d must be a positive integer, and suffix must be h/d!");
}

TEST_F(PartitionTest, CreateTableWithTimestampBug709846727_2) {
    std::string tablename("tbp_retentionpositive2");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp,value int) PARTITION BY RANGE(date) timescale interval '1d' retention '0d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result->GetRetMsg().c_str(), "the number prefix of retention 0d must be a positive integer, and suffix must be h/d!");
}

TEST_F(PartitionTest, CreateTableWithTimestampBug709846727_3) {
    std::string tablename("tbp_retentionpositive3");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp,value int) PARTITION BY RANGE(date) timescale interval '1d' retention '30' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result->GetRetMsg().c_str(), "the number prefix of retention 30 must be a positive integer, and suffix must be h/d!");
}

// retention must be biger than interval
TEST_F(PartitionTest, CreateTableWithTimestampBug709846727_4) {
    std::string tablename("tbp_retentionpositive4");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp,value int) PARTITION BY RANGE(date) timescale interval '1d' retention '1h' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_ERROR);
}

// retention default is 7 days
TEST_F(PartitionTest, CreateTableWithTimestampDefaultRetention) {
    std::string tablename("tbp_retentiondefault");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp,value int) PARTITION BY RANGE(date) timescale interval '1d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_SUCCESS);
    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info != nullptr);
    //EXPECT_EQ(table_info->GetTableMetaInfo().appendonly, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().is_timescale, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().has_retention, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().retention.day, 7);
    EXPECT_EQ(table_info->GetTableMetaInfo().parted, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.auto_addpart, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.is_crosspart, true);
    EXPECT_STREQ(table_info->GetTableMetaInfo().part_table.desc.interval.str, "1d");
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partkeys, 1);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.parttype, PART_TYPE_RANGE);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partcnt, 0);
}

// NON-TIMESCALE part table do not support 
TEST_F(PartitionTest, CreatePartTableNonTimescale) {
    std::string tablename("tbp_nontimescale");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp,value int) PARTITION BY RANGE(date) interval '1d' retention '1h' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result->GetRetMsg().c_str(), "we do not support NON-TIMESCALE part table yet!");
}

TEST_F(PartitionTest, AlterTSTableAddColWithUnique) {
    std::string tablename("tsp_addcol_unique");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp,value int) PARTITION BY RANGE(date) timescale interval '1d' retention '30d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_SUCCESS);
    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info != nullptr);
    //EXPECT_EQ(table_info->GetTableMetaInfo().appendonly, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().is_timescale, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().has_retention, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().retention.day, 30);
    EXPECT_EQ(table_info->GetTableMetaInfo().parted, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.auto_addpart, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.is_crosspart, true);
    EXPECT_STREQ(table_info->GetTableMetaInfo().part_table.desc.interval.str, "1d");
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partkeys, 1);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.parttype, PART_TYPE_RANGE);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partcnt, 0);
    EXPECT_EQ(table_info->GetTableMetaInfo().column_count, 3);

    std::string alter_addcol_unique(fmt::format("ALTER TABLE {} ADD COLUMN age int unique;", tablename));
    auto result1 = conn->Query(alter_addcol_unique.c_str());
    EXPECT_TRUE(result1->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result1->GetRetMsg().c_str(), "Binder Error: Primary key or unique key is not supported for time scale table");

    std::string alter_addcol_primary_key(fmt::format("ALTER TABLE {} ADD COLUMN age int primary key;", tablename));
    auto result2 = conn->Query(alter_addcol_primary_key.c_str());
    EXPECT_TRUE(result2->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result2->GetRetMsg().c_str(), "Binder Error: Primary key or unique key is not supported for time scale table");

    std::string alter_addcol(fmt::format("ALTER TABLE {} ADD COLUMN age int;", tablename));
    auto result3 = conn->Query(alter_addcol.c_str());
    EXPECT_TRUE(result3->GetRetCode()==GS_SUCCESS);
    auto table_info1 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info1 != nullptr);
    //EXPECT_EQ(table_info->GetTableMetaInfo().appendonly, true);
    EXPECT_EQ(table_info1->GetTableMetaInfo().is_timescale, true);
    EXPECT_EQ(table_info1->GetTableMetaInfo().has_retention, true);
    EXPECT_EQ(table_info1->GetTableMetaInfo().retention.day, 30);
    EXPECT_EQ(table_info1->GetTableMetaInfo().parted, true);
    EXPECT_EQ(table_info1->GetTableMetaInfo().part_table.desc.auto_addpart, true);
    EXPECT_EQ(table_info1->GetTableMetaInfo().part_table.desc.is_crosspart, true);
    EXPECT_STREQ(table_info1->GetTableMetaInfo().part_table.desc.interval.str, "1d");
    EXPECT_EQ(table_info1->GetTableMetaInfo().part_table.desc.partkeys, 1);
    EXPECT_EQ(table_info1->GetTableMetaInfo().part_table.desc.parttype, PART_TYPE_RANGE);
    EXPECT_EQ(table_info1->GetTableMetaInfo().part_table.desc.partcnt, 0);
    EXPECT_EQ(table_info1->GetTableMetaInfo().column_count, 4);
}
// bugfix#710924403 时序数据库建表成功，建分区失败 -> 刚好达到了表名长度限制附近，分区创建时分区名被截断
TEST_F(PartitionTest, AlterTSTablePartNameTooLong) {
    std::string tablename("aarch64");
    while (tablename.length() < GS_MAX_NAME_LEN - strlen("aarch64")){
        tablename += "aarch64";
    }
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp,value int) PARTITION BY RANGE(date) timescale interval '1h' retention '30d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_SUCCESS);
    auto table_info = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info != nullptr);
    //EXPECT_EQ(table_info->GetTableMetaInfo().appendonly, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().is_timescale, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().has_retention, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().retention.day, 30);
    EXPECT_EQ(table_info->GetTableMetaInfo().parted, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.auto_addpart, true);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.is_crosspart, true);
    EXPECT_STREQ(table_info->GetTableMetaInfo().part_table.desc.interval.str, "1h");
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partkeys, 1);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.parttype, PART_TYPE_RANGE);
    EXPECT_EQ(table_info->GetTableMetaInfo().part_table.desc.partcnt, 0);
    EXPECT_EQ(table_info->GetTableMetaInfo().column_count, 3);


    std::string part1(tablename+"_32023082210"+GetNowTime(TIME_OUTPUT_TYPE_PARTKEY_HOUR));
    std::string addp1(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part1));
    auto result1 = conn->Query(addp1.c_str());
    EXPECT_TRUE(result1->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result1->GetRetMsg().c_str(), "Binder Error: add Partition name is too long");

    std::string addp2(fmt::format("INSERT INTO {} VALUES (1, '2023-08-22 10:11:12', 44), (2, '2023-08-22 10:21:22', 55), (3, '2023-08-22 10:31:32', 66), (4, '2023-08-22 10:41:32', 77);", tablename));
    auto result2 = conn->Query(addp2.c_str());
    EXPECT_TRUE(result2->GetRetCode()==GS_SUCCESS);

    std::string addp3(fmt::format("SELECT * FROM {} WHERE date > '2023-08-22 10:11:22';", tablename));
    auto result3 = conn->Query(addp3.c_str());
    EXPECT_TRUE(result3->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result3->RowCount(), 3);

    std::string part4(tablename+"_"+GetNowTime(TIME_OUTPUT_TYPE_PARTKEY_HOUR));
    std::string addp4(fmt::format("ALTER TABLE {} ADD PARTITION {};", tablename, part4));
    std::cout << "addp4=" << addp4 << std::endl;
    auto result4 = conn->Query(addp4.c_str());
    EXPECT_TRUE(result4->GetRetCode()==GS_SUCCESS);
    if(result4->GetRetCode() != GS_SUCCESS) {
        std::cout << result4->GetRetMsg() << std::endl;
    }
    auto table_info4 = conn->GetTableInfo(tablename);
    EXPECT_TRUE(table_info4 != nullptr);
    EXPECT_EQ(table_info4->GetTableMetaInfo().part_table.desc.partcnt, 2);

    std::string drop1(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part1));
    auto result5 = conn->Query(drop1.c_str());
    EXPECT_TRUE(result5->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result5->GetRetMsg().c_str(), "Binder Error: drop Partition name is too long");

    std::string drop2(fmt::format("ALTER TABLE {} DROP PARTITION {};", tablename, part4));
    auto result6 = conn->Query(drop2.c_str());
    EXPECT_TRUE(result6->GetRetCode()==GS_SUCCESS);
}

// bugfix: #710953428 时序表分区和过期逻辑
TEST_F(PartitionTest, CreateTsTableIntervalInvalid) {
    std::string tablename("tbp_ts_interval_invalid");
    std::string query(fmt::format("CREATE TABLE {} (id int,date timestamp,value int) PARTITION BY RANGE(date) timescale interval '5h' retention '1d' autopart crosspart;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_ERROR);
    EXPECT_STREQ(result->GetRetMsg().c_str(), "the interval must be  1h/1d, but now is 5h!");
}

// 时序表支持lob类型(clob/blob)
TEST_F(PartitionTest, TsTableLobSupport) {
    std::string tablename("max_col_table");
    std::string query(fmt::format("CREATE TABLE {} (\
    a1 INTEGER,\
    a2 INT,\
    a3 INT4,\
    a4 INT32,\
    a5 SIGNED,\
    a6 BIGINT,\
    a7 LONG,\
    a8 INT8,\
    a9 INT64,\
    a10 SMALLINT,\
    a11 SHORT,\
    a12 INT2,\
    a13 INT16,\
    a14 TINYINT,\
    a15 BOOL,\
    a16 BOOLEAN,\
    a17 LOGICAL,\
    a18 REAL,\
    a19 DOUBLE,\
    a20 FLOAT8,\
    a21 NUMBER,\
    a22 NUMERIC,\
    a23 DECIMAL,\
    a24 NUMERIC(38, 4),\
    a25 DATE,\
    a26 DATETIME,\
    a27 TIMESTAMP,\
    a28 CHAR,\
    a29 VARCHAR,\
    a30 BPCHAR,\
    a31 TEXT,\
    a32 CLOB,\
    a33 VARCHAR(4),\
    a34 char(4),\
    a35 VARCHAR(128),\
    a36 INTEGER,\
    a37 INT,\
    a38 INT4,\
    a39 INT32,\
    a40 SIGNED,\
    a41 BIGINT,\
    a42 LONG,\
    a43 INT8,\
    a44 INT64,\
    a45 SMALLINT,\
    a46 SHORT,\
    a47 INT2,\
    a48 INT16,\
    a49 TINYINT,\
    a50 BOOL,\
    a51 BOOLEAN,\
    a52 LOGICAL,\
    a53 REAL,\
    a54 DOUBLE,\
    a55 FLOAT8,\
    a56 NUMBER,\
    a57 NUMERIC,\
    a58 DECIMAL,\
    a59 NUMERIC(38, 4),\
    a60 DATE,\
    a61 DATETIME,\
    a62 TIMESTAMP,\
    a63 CHAR,\
    a64 VARCHAR,\
    a65 BPCHAR,\
    a66 TEXT,\
    a67 CLOB,\
    a68 VARCHAR(4),\
    a69 char(4),\
    a70 VARCHAR(128),\
    a71 INTEGER,\
    a72 INT,\
    a73 INT4,\
    a74 INT32,\
    a75 SIGNED,\
    a76 BIGINT,\
    a77 LONG,\
    a78 INT8,\
    a79 INT64,\
    a80 SMALLINT,\
    a81 SHORT,\
    a82 INT2,\
    a83 INT16,\
    a84 TINYINT,\
    a85 BOOL,\
    a86 BOOLEAN,\
    a87 LOGICAL,\
    a88 REAL,\
    a89 DOUBLE,\
    a90 FLOAT8,\
    a91 NUMBER,\
    a92 NUMERIC,\
    a93 DECIMAL,\
    a94 NUMERIC(38, 4),\
    a95 DATE,\
    a96 DATETIME,\
    a97 TIMESTAMP,\
    a98 CHAR,\
    a99 VARCHAR,\
    a100 BPCHAR,\
    a101 TEXT,\
    a102 CLOB,\
    a103 VARCHAR(4),\
    a104 char(4),\
    a105 VARCHAR(128),\
    a106 INTEGER,\
    a107 INT,\
    a108 INT4,\
    a109 INT32,\
    a110 SIGNED,\
    a111 BIGINT,\
    a112 LONG,\
    a113 INT8,\
    a114 INT64,\
    a115 SMALLINT,\
    a116 SHORT,\
    a117 INT2,\
    a118 INT16,\
    a119 TINYINT,\
    a120 BOOL,\
    a121 BOOLEAN,\
    a122 LOGICAL,\
    a123 REAL,\
    a124 DOUBLE,\
    a125 FLOAT8,\
    a126 NUMBER,\
    a127 NUMERIC,\
    a128 DECIMAL\
) PARTITION BY RANGE(a27) TIMESCALE INTERVAL '1d' RETENTION '30d' AUTOPART CROSSPART;", tablename));
    std::cout << query << std::endl;
    auto result = conn->Query(query.c_str());
    EXPECT_TRUE(result->GetRetCode()==GS_SUCCESS);

    std::string query1("INSERT INTO max_col_table VALUES(2147483647, 2147483647,2147483647, -2147483648, 2147483647, 9223372036854775807, 9223372036854775807, -9223372036854775807, -9223372036854775807, 32767,  32767, 32767, 32767, 127, true, true, false, 2.2250738585072014E-308,2.2250738585072014E-308,1.7976931348623157E+308, 1234567890, 1234567890, 1234567890, 1234567890, now(), now(), now(),'C', '#$%$%^^%&*&)(*QWE', '{}' , 'Sample Text', '{\"name\": \"John Doe\",\"age\": 30,\"is_student\": false,\"grades\": [95, 87, 92, 88], \"address\": {\
        \"street\": \"123 Main Street\",\
        \"city\": \"Anytown\",\
        \"country\": \"USA\"\
    },\
    \"contacts\": {\
        \"email\": \"johndoe@example.com\",\
        \"phone\": \"+1-202-555-0123\"\
    },\
    \"is_active\": true,\
    \"tags\": [\"json\", \"data\", \"example\"],\
    \"preferences\": {\
        \"theme\": \"dark\",\
        \"language\": \"English\",\
        \"notifications\": true\
    }}','Test', 'abcd','csd',2147483647, 2147483647, -2147483648, -2147483648, 2147483647, 9223372036854775807, 9223372036854775807, -9223372036854775807, -9223372036854775807, 32767,  32767, 32767, 32767, 127, true, true, false, 2.2250738585072014E-308,2.2250738585072014E-308,1.7976931348623157E+308, 1234567890, 1234567890, 1234567890, 1234567890, now(), now(), now(),'C', '#$%$%^^%&*&)(*QWE', '{}' , 'Sample Text', '{\"name\": \"John Doe\",\"age\": 30,\"is_student\": false,\"grades\": [95, 87, 92, 88], \"address\": {\
        \"street\": \"123 Main Street\",\
        \"city\": \"Anytown\",\
        \"country\": \"USA\"},\
    \"contacts\": {\
        \"email\": \"johndoe@example.com\",\
        \"phone\": \"+1-202-555-0123\"},\
    \"is_active\": true,\
    \"tags\": [\"json\", \"data\", \"example\"],\
    \"preferences\": {\
        \"theme\": \"dark\",\
        \"language\": \"English\",\
        \"notifications\": true\
    }}','Test', 'abcd','csd',2147483647, 2147483647, -2147483648, -2147483648, 2147483647, 9223372036854775807, 9223372036854775807, -9223372036854775807, -9223372036854775807, 32767,  32767, 32767, 32767, 127, true, true, false, 2.2250738585072014E-308,2.2250738585072014E-308,1.7976931348623157E+308, 1234567890, 1234567890, 1234567890, 1234567890, now(), now(), now(),'C', '#$%$%^^%&*&)(*QWE', '{}' , 'Sample Text', '{\"name\": \"John Doe\",\"age\": 30,\"is_student\": false,\"grades\": [95, 87, 92, 88], \"address\": {\
        \"street\": \"123 Main Street\",\
        \"city\": \"Anytown\",\
        \"country\": \"USA\"\
    },\
    \"contacts\": {\
        \"email\": \"johndoe@example.com\",\
        \"phone\": \"+1-202-555-0123\"\
    },\
    \"is_active\": true,\
    \"tags\": [\"json\", \"data\", \"example\"],\
    \"preferences\": {\
        \"theme\": \"dark\",\
        \"language\": \"English\",\
        \"notifications\": true\
    }}','Test', 'abcd','csd',2147483647, 2147483647, -2147483648, -2147483648, 2147483647, 9223372036854775807, 9223372036854775807, -9223372036854775807, -9223372036854775807, 32767,  32767, 32767, 32767, 127, true, true, false, 2.2250738585072014E-308,2.2250738585072014E-308,1.7976931348623157E+308, 1234567890, 1234567890, 1234567890);");
    std::cout << query1 << std::endl;
    auto result1 = conn->Query(query1.c_str());
    EXPECT_TRUE(result1->GetRetCode()==GS_SUCCESS);

    std::string query2(fmt::format("SELECT count(*) FROM {};", tablename));
    auto result2 = conn->Query(query2.c_str());
    EXPECT_EQ(result2->RowCount(), 1);
    EXPECT_EQ(result2->Row(0).Field(0).GetCastAs<int32_t>(), 1);

    std::string query3(fmt::format("SELECT count(*) FROM \"SYS_TABLE_PARTS\" where \"NAME\" like '{}%';", tablename));
    auto result3 = conn->Query(query3.c_str());
    EXPECT_TRUE(result3->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result3->RowCount(), 1);
    EXPECT_EQ(result3->Row(0).Field(0).GetCastAs<int32_t>(), 1);

    auto table_info = conn->GetTableInfo(tablename);
    ASSERT_FALSE(table_info == nullptr);
    std::string query4(fmt::format("SELECT count(*) FROM \"SYS_LOBPART\" where \"TABLE#\" = {};", table_info->GetTableId()));
    std::cout << query4 << std::endl;
    auto result4 = conn->Query(query4.c_str());
    EXPECT_TRUE(result4->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result4->RowCount(), 1);
    EXPECT_EQ(result4->Row(0).Field(0).GetCastAs<int32_t>(), 3); // 有3个lob字段,每个分区中每个lob字段一个lobpart

    std::string query5(fmt::format("drop table {};", tablename));
    auto result5 = conn->Query(query5.c_str());
    EXPECT_TRUE(result5->GetRetCode()==GS_SUCCESS);

    auto result6 = conn->Query(query3.c_str());
    EXPECT_TRUE(result6->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result6->RowCount(), 1);
    EXPECT_EQ(result6->Row(0).Field(0).GetCastAs<int32_t>(), 0);

    auto result7 = conn->Query(query4.c_str());
    EXPECT_TRUE(result7->GetRetCode()==GS_SUCCESS);
    EXPECT_EQ(result7->RowCount(), 1);
    EXPECT_EQ(result7->Row(0).Field(0).GetCastAs<int32_t>(), 0);
}

int main(int argc, char **argv) {
    ::testing::GTEST_FLAG(output) = "xml";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
