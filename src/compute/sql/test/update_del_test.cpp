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
 * update_del_test.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/test/update_del_test.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <gtest/gtest.h>

#include <ctime>
#include <iomanip>
#include <iostream>
#include <fstream>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "main/connection.h"
#include "main/database.h"

class ConnectionForUpdateDeleteTest : public ::testing::Test
{
protected:
    ConnectionForUpdateDeleteTest() {}
    ~ConnectionForUpdateDeleteTest() {}
    static void SetUpTestSuite() {
        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance->Init();
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();
    }

    static void TearDownTestSuite() {}

    void SetUp() override {}

    static std::shared_ptr<IntarkDB> db_instance;
    static std::unique_ptr<Connection> conn;
};

std::shared_ptr<IntarkDB> ConnectionForUpdateDeleteTest::db_instance = nullptr;
std::unique_ptr<Connection> ConnectionForUpdateDeleteTest::conn = nullptr;

TEST_F(ConnectionForUpdateDeleteTest, UpdateAndDeleteWithSubquery) {
    conn->Query("drop table if exists t1");
    conn->Query("drop table if exists a");
    conn->Query("CREATE TABLE t1(a integer primary key,b int, c int, d int,e int, f int,UNIQUE(c,d))");
    conn->Query("create table a (a int)");
    conn->Query("insert into t1 values 1,2,3,4,5,6),(2,3,4,4,6,7)");
    conn->Query("insert into a values (1),(2),(3)");

    // update
    // 关联子查询
    auto r = conn->Query("update t1 set a = a + 1 where b in ( select a from a where a = t1.a)");
    EXPECT_EQ(r->GetRetCode(), 0);

    // 非关联子查询
    r = conn->Query("update t1 set a = a + 1 where b in (select a from a)");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("delete from t1 where b not in (select a from a)");
    EXPECT_EQ(r->GetRetCode(), 0);

    r = conn->Query("delete from t1 where b not in (select a from a where a = t1.a)");
    EXPECT_EQ(r->GetRetCode(), 0);
}

TEST_F(ConnectionForUpdateDeleteTest, UpdateInPageCase) {
    conn->Query("drop table if exists t1");
    conn->Query("create table t1 (id int ,name varchar(20), pi varchar(20),c int)");
    conn->Query("insert into t1 (id) values (1)");
    auto r = conn->Query("update t1 set c = 4 , name = 'hello', pi = 'world' where id = 1");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select * from t1");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 1);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<std::string>(), "hello");
    EXPECT_EQ(r->Row(0).Field(2).GetCastAs<std::string>(), "world");
    EXPECT_EQ(r->Row(0).Field(3).GetCastAs<int32_t>(), 4);
}

TEST_F(ConnectionForUpdateDeleteTest, UpdateWithSubquery) {
    conn->Query("drop table if exists integers");
    conn->Query("CREATE TABLE integers(id INTEGER, i INTEGER)");
    conn->Query("INSERT INTO integers VALUES (1, 1), (2, 2), (3, 3), (4, NULL)");
    auto r = conn->Query("UPDATE integers i1 SET i=(SELECT MAX(i) FROM integers WHERE i1.i<>i)");
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query("select * from integers");
    ASSERT_EQ(r->GetRetCode(), 0);
    EXPECT_EQ(r->RowCount(), 4);
    EXPECT_EQ(r->Row(0).Field(0).GetCastAs<int32_t>(), 1);
    EXPECT_EQ(r->Row(0).Field(1).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(1).Field(0).GetCastAs<int32_t>(), 2);
    EXPECT_EQ(r->Row(1).Field(1).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(2).Field(0).GetCastAs<int32_t>(), 3);
    EXPECT_EQ(r->Row(2).Field(1).IsNull(), true);
    EXPECT_EQ(r->Row(3).Field(0).GetCastAs<int32_t>(), 4);
    EXPECT_EQ(r->Row(3).Field(1).IsNull(), true);
}

TEST_F(ConnectionForUpdateDeleteTest, TestDeleteSysTable) {
    auto r = conn->Query(R"(delete from "SYS"."SYS_TABLES")");
    ASSERT_NE(r->GetRetCode(), 0);
    // 明确是因为系统表，不能删除的错误
    EXPECT_EQ(r->GetRetMsg(),
              "Binder Error: Cannot delete from system table : SYS_TABLES");
}

TEST_F(ConnectionForUpdateDeleteTest, TestTsTableUpdateFail)
{
    conn->Query("drop table if exists product_task_stat");
    auto r = conn->Query(R"(create table product_task_stat(
                        planned_num int,
                        effective_num int,
                        collect_status int,
                        create_time timestamp default now()
                        ) partition by range(create_time)
                        timescale interval '1h' retention '30d' autopart crosspart;)");
    EXPECT_EQ(r->GetRetCode(), 0);
    r = conn->Query(R"(insert into product_task_stat (planned_num, effective_num, collect_status)
                    values(999, 1000, 1000), (485, 500, 1000), (9999, 10000, 20000);)");
    EXPECT_EQ(r->GetRetCode(), 0);
    r = conn->Query(R"(update product_task_stat set planned_num = 10 where planned_num = 999;)");
    ASSERT_NE(r->GetRetCode(), 0);
    EXPECT_EQ(r->GetRetMsg(),
              "Binder Error: time series table cannot update! please set TS_UPDATE_SUPPORT to TRUE");
}

TEST_F(ConnectionForUpdateDeleteTest, UpdateTableOverOnePage) {
    conn->Query("drop table if exists his_diagnosis_task_info");
    conn->Query("create sequence his_diagnosis_task_seq ");
    auto r = conn->Query(R"(CREATE TABLE his_diagnosis_task_info (
    id INTEGER NOT NULL PRIMARY KEY DEFAULT nextval('his_diagnosis_task_seq'),
    cluster_id TEXT,
    node_id TEXT,
    db_name TEXT,
    task_name TEXT,
    topology_map TEXT,
    sql_id TEXT,
    sql TEXT,
    pid INTEGER,
    debug_query_id BIGINT,
    session_id BIGINT,
    his_data_start_time DATETIME,
    his_data_end_time DATETIME,
    task_start_time DATETIME,
    task_end_time DATETIME,
    state TEXT,
    span TEXT,
    remarks TEXT,
    conf TEXT,
    threshold TEXT,
    node_vo_sub TEXT,
    task_type TEXT,
    diagnosis_type TEXT,
    is_deleted INTEGER,
    create_time DATETIME,
    update_time DATETIME
))");
    fmt::print("{}\n", r->GetRetMsg());
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query(
        "insert into his_diagnosis_task_info values "
        "(1,'1','1','1','1','1','1','1',1,1,1,now(),now(),now(),now(),'1','1','1','1','1','1','1','1',1,now(),now())");
    fmt::print("{}\n", r->GetRetMsg());
    ASSERT_EQ(r->GetRetCode(), 0);
    r = conn->Query(R"(UPDATE his_diagnosis_task_info SET cluster_id='集群1',
 node_id='62d9b979-aeed-4823-b0a6-32a3a7371099',
 topology_map=' -HisTaskInfo ROOT
--DatabaseHostPressure CENTER
---AvgCpuUsageRate CENTER
----DbProcessAvgCpuUsage DIAGNOSIS
-----TopCpuTimeSql DISPLAY
-----CpuTimeTopSql DISPLAY
----WdrAnalysis CENTER
----AspAnalysis DIAGNOSIS
-----BusinessConnCount DIAGNOSIS
----OtherProcessAvgCpuUsage DIAGNOSIS
---CurrentCpuUsage CENTER
----DbProcessCurrentCpuUsage DIAGNOSIS
-----TopDbProcess DIAGNOSIS
----OtherProcessCurrentCpuUsage DIAGNOSIS
---PoorSql CENTER
----CurrentSlowSql DIAGNOSIS
---WaitEventAnalysis CENTER
----CurrentWaitEvent DIAGNOSIS
----HistoryWaitEvent DIAGNOSIS
---LockAnalysis CENTER
----DeadlockAnalysis DIAGNOSIS
----LockTimeout DIAGNOSIS
---MemoryAnalysis CENTER
----SwapAnalysis DIAGNOSIS
--DatabaseHostLowPressure CENTER',
 pid=0,
 debug_query_id=0,
 session_id=0,
 his_data_start_time='2023-12-13 03:06:41.0',
 his_data_end_time='2023-12-13 04:06:41.0',
 task_start_time='2023-12-13 04:06:54.479',
 task_end_time='2023-12-13 04:07:02.763',
 state='FINISH',
 span='00:00:08.284',
 remarks=' ***Ready to start diagnosis***<br/>04:06:54.479307 start running diagnosis<br/>04:07:01.122115 start analysis ...<br/>04:07:01.305523 start check collection SwapInItem<br/>04:07:01.305631 start check collection TopCpuTimeSqlItem<br/>04:07:01.305877 start check collection AvgCpuItem<br/>04:07:01.306023 start check collection ActivityNumLowItem<br/>04:07:01.306921 start check collection CpuTimeTopSqlItem<br/>04:07:01.307078 start check collection SwapOutItem<br/>04:07:01.308740 start check collection ThreadPoolUsageHighItem<br/>04:07:01.308841 start check collection BusinessConnCountItem<br/>04:07:01.320869 start check collection ActivityNumHighItem<br/>04:07:01.321886 start check collection WaitEventItem<br/>04:07:01.323040 start check collection DbAvgCpuItem<br/>04:07:01.325114 start check collection ThreadPoolUsageLowItem<br/>04:07:01.659708 stop check collection ThreadPoolUsageHighItem<br/>04:07:01.661509 stop check collection SwapInItem<br/>04:07:01.664950 stop check collection ActivityNumLowItem<br/>04:07:01.762707 start collection ActivityNumLowItem<br/>04:07:01.778533 stop check collection CpuTimeTopSqlItem<br/>04:07:01.791702 stop check collection TopCpuTimeSqlItem<br/>04:07:01.804253 stop collection ActivityNumLowItem<br/>04:07:01.918224 stop check collection WaitEventItem<br/>04:07:01.918323 stop check collection SwapOutItem<br/>04:07:01.918455 stop check collection BusinessConnCountItem<br/>04:07:01.918935 start collection ThreadPoolUsageHighItem<br/>04:07:01.919127 stop check collection ThreadPoolUsageLowItem<br/>04:07:01.920116 start collection SwapInItem<br/>04:07:01.920281 stop check collection ActivityNumHighItem<br/>04:07:01.961920 stop collection SwapInItem<br/>04:07:01.977112 stop collection ThreadPoolUsageHighItem<br/>04:07:01.987152 start collection TopCpuTimeSqlItem<br/>04:07:01.989717 start collection CpuTimeTopSqlItem<br/>04:07:02.053739 start collection BusinessConnCountItem<br/>04:07:02.054335 start collection SwapOutItem<br/>04:07:02.054466 stop check collection AvgCpuItem<br/>04:07:02.055801 start collection ThreadPoolUsageLowItem<br/>04:07:02.056004 start collection WaitEventItem<br/>04:07:02.070141 start collection ActivityNumHighItem<br/>04:07:02.089667 stop collection ThreadPoolUsageLowItem<br/>04:07:02.094560 stop collection BusinessConnCountItem<br/>04:07:02.098543 stop collection ActivityNumHighItem<br/>04:07:02.099236 stop collection WaitEventItem<br/>04:07:02.112268 stop collection SwapOutItem<br/>04:07:02.173400 stop collection TopCpuTimeSqlItem<br/>04:07:02.173552 stop collection CpuTimeTopSqlItem<br/>04:07:02.179052 start analysis HisTaskInfo<br/>04:07:02.179663 stop analysis HisTaskInfo<br/>04:07:02.246921 start collection AvgCpuItem<br/>04:07:02.247224 stop check collection DbAvgCpuItem<br/>04:07:02.248734 start analysis LockAnalysis<br/>04:07:02.248804 stop analysis LockAnalysis<br/>04:07:02.286483 stop collection AvgCpuItem<br/>04:07:02.396266 start analysis MemoryAnalysis<br/>04:07:02.396329 start analysis PoorSql<br/>04:07:02.396423 stop analysis PoorSql<br/>04:07:02.396455 start analysis WaitEventAnalysis<br/>04:07:02.396551 stop analysis WaitEventAnalysis<br/>04:07:02.396898 stop analysis MemoryAnalysis<br/>04:07:02.409448 start collection DbAvgCpuItem<br/>04:07:02.441047 stop collection DbAvgCpuItem<br/>04:07:02.512367 start analysis CpuTimeTopSql<br/>04:07:02.513632 stop analysis CpuTimeTopSql<br/>04:07:02.584933 start analysis TopCpuTimeSql<br/>04:07:02.585725 stop analysis TopCpuTimeSql<br/>04:07:02.763657 analysis finish<br/>04:07:02.763851 finish diagnosis',
 conf='[{"option":"IS_LOCK","name":"{{i18n,history.option.isLock}}","isCheck":false,"sortNo":6}]',
 threshold='[{"id":1,"clusterId":null,"nodeId":null,"thresholdType":"CPU","threshold":"cpuUsageRate","thresholdName":"{{i18n,history.threshold.cpuUsageRate.title}}","thresholdValue":"50","thresholdUnit":"%","thresholdDetail":"{{i18n,history.threshold.cpuUsageRate.detail}}","sortNo":null,"diagnosisType":null,"isDeleted":0,"createTime":"2023-12-13T09:06:58Z","updateTime":"2023-12-13T09:06:58Z"},{"id":2,"clusterId":null,"nodeId":null,"thresholdType":"CPU","threshold":"dbCpuUsageRate","thresholdName":"{{i18n,history.threshold.dbCpuUsageRate.title}}","thresholdValue":"50","thresholdUnit":"%","thresholdDetail":"{{i18n,history.threshold.dbCpuUsageRate.detail}}","sortNo":null,"diagnosisType":null,"isDeleted":0,"createTime":"2023-12-13T09:06:58Z","updateTime":"2023-12-13T09:06:58Z"},{"id":3,"clusterId":null,"nodeId":null,"thresholdType":"CPU","threshold":"proCpuUsageRate","thresholdName":"{{i18n,history.threshold.proCpuUsageRate.title}}","thresholdValue":"50","thresholdUnit":"%","thresholdDetail":"{{i18n,history.threshold.proCpuUsageRate.detail}}","sortNo":null,"diagnosisType":null,"isDeleted":0,"createTime":"2023-12-13T09:06:58Z","updateTime":"2023-12-13T09:06:58Z"},{"id":4,"clusterId":null,"nodeId":null,"thresholdType":"CPU","threshold":"activityNum","thresholdName":"{{i18n,history.threshold.activityNum.title}}","thresholdValue":"10","thresholdUnit":"pcs","thresholdDetail":"{{i18n,history.threshold.activityNum.detail}}","sortNo":null,"diagnosisType":null,"isDeleted":0,"createTime":"2023-12-13T09:06:58Z","updateTime":"2023-12-13T09:06:58Z"},{"id":5,"clusterId":null,"nodeId":null,"thresholdType":"CPU","threshold":"threadPoolUsageRate","thresholdName":"{{i18n,history.threshold.threadPoolUsageRate.title}}","thresholdValue":"30","thresholdUnit":"%","thresholdDetail":"{{i18n,history.threshold.threadPoolUsageRate.detail}}","sortNo":null,"diagnosisType":null,"isDeleted":0,"createTime":"2023-12-13T09:06:58Z","updateTime":"2023-12-13T09:06:58Z"},{"id":6,"clusterId":null,"nodeId":null,"thresholdType":"CPU","threshold":"connectionNum","thresholdName":"{{i18n,history.threshold.connectionNum.title}}","thresholdValue":"10","thresholdUnit":"pcs","thresholdDetail":"{{i18n,history.threshold.connectionNum.detail}}","sortNo":null,"diagnosisType":null,"isDeleted":0,"createTime":"2023-12-13T09:06:58Z","updateTime":"2023-12-13T09:06:58Z"},{"id":7,"clusterId":null,"nodeId":null,"thresholdType":"CPU","threshold":"duration","thresholdName":"{{i18n,history.threshold.duration.title}}","thresholdValue":"60","thresholdUnit":"s","thresholdDetail":"{{i18n,history.threshold.duration.detail}}","sortNo":null,"diagnosisType":null,"isDeleted":0,"createTime":"2023-12-13T09:06:58Z","updateTime":"2023-12-13T09:06:58Z"},{"id":8,"clusterId":null,"nodeId":null,"thresholdType":"CPU","threshold":"sqlNum","thresholdName":"{{i18n,history.threshold.sqlNum.title}}","thresholdValue":"1","thresholdUnit":"pcs","thresholdDetail":"{{i18n,history.threshold.sqlNum.detail}}","sortNo":null,"diagnosisType":null,"isDeleted":0,"createTime":"2023-12-13T09:06:58Z","updateTime":"2023-12-13T09:06:58Z"},{"id":9,"clusterId":null,"nodeId":null,"thresholdType":"CPU","threshold":"waitEventNum","thresholdName":"{{i18n,history.threshold.waitEventNum.title}}","thresholdValue":"10","thresholdUnit":"pcs","thresholdDetail":"{{i18n,history.threshold.waitEventNum.detail}}","sortNo":null,"diagnosisType":null,"isDeleted":0,"createTime":"2023-12-13T09:06:58Z","updateTime":"2023-12-13T09:06:58Z"},{"id":10,"clusterId":null,"nodeId":null,"thresholdType":"IO","threshold":"diskUtilization","thresholdName":"{{i18n,history.threshold.diskUtilization.title}}","thresholdValue":"50","thresholdUnit":"%","thresholdDetail":"{{i18n,history.threshold.diskUtilization.detail}}","sortNo":null,"diagnosisType":null,"isDeleted":0,"createTime":"2023-12-13T09:06:58Z","updateTime":"2023-12-13T09:06:58Z"},{"id":12,"clusterId":null,"nodeId":null,"thresholdType":"CPU","threshold":"swapIn","thresholdName":"{{i18n,sql.threshold.swapIn.title}}","thresholdValue":"0","thresholdUnit":"page","thresholdDetail":"{{i18n,sql.threshold.swapIn.detail}}","sortNo":null,"diagnosisType":null,"isDeleted":0,"createTime":"2023-12-13T09:06:58Z","updateTime":"2023-12-13T09:06:58Z"},{"id":13,"clusterId":null,"nodeId":null,"thresholdType":"CPU","threshold":"swapOut","thresholdName":"{{i18n,sql.threshold.swapOut.title}}","thresholdValue":"0","thresholdUnit":"page","thresholdDetail":"{{i18n,sql.threshold.swapOut.detail}}","sortNo":null,"diagnosisType":null,"isDeleted":0,"createTime":"2023-12-13T09:06:58Z","updateTime":"2023-12-13T09:06:58Z"}]',
 task_type='MANUAL',
 diagnosis_type='history',
 is_deleted=0,
 create_time='2023-12-13 04:06:53.674',
 update_time='2023-12-13 04:06:53.674' WHERE id=1)");
    ASSERT_NE(r->GetRetCode(), 0);
    // r = conn->Query("select * from his_diagnosis_task_info");
    // ASSERT_EQ(r->GetRetCode(), 0);
}

void StartDBAndModifyCfgFile()
{
    {
        std::shared_ptr<IntarkDB> db = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
    }
    std::string filename = "./intarkdb/cfg/intarkdb.ini";
    std::string tempFilename = "temp.ini";

    std::string keyToModify = "TS_UPDATE_SUPPORT";
    std::string newValue = "TRUE";

    std::ifstream inputFile(filename);
    if (!inputFile.is_open()) {
        std::cerr << "Error opening file." << std::endl;
    }
    std::ofstream tempFile(tempFilename);
    if (!tempFile.is_open()) {
        std::cerr << "Error creating temp file." << std::endl;
    }

    std::string line;
    while (std::getline(inputFile, line)) {
        if (line.find(keyToModify) != std::string::npos) {
            line = keyToModify + " = " + newValue;
        }
        tempFile << line << '\n';
    }
    inputFile.close();
    tempFile.close();

    if (std::rename(tempFilename.c_str(), filename.c_str()) != 0) {
        std::cerr << "Error renaming file." << std::endl;
    }
}

class ConnectionTsUpdateTest : public ::testing::Test {
   protected:
    ConnectionTsUpdateTest() {}
    ~ConnectionTsUpdateTest() {}
    static void SetUpTestSuite() {
        system("rm -rf intarkdb/");
        StartDBAndModifyCfgFile();
        db_instance1 = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        // 启动db
        db_instance1->Init();
        conn1 = std::make_unique<Connection>(db_instance1);
        conn1->Init();
    }

    static void TearDownTestSuite() {}

    void SetUp() override {}

    static std::shared_ptr<IntarkDB> db_instance1;
    static std::unique_ptr<Connection> conn1;
};

std::shared_ptr<IntarkDB> ConnectionTsUpdateTest::db_instance1 = nullptr;
std::unique_ptr<Connection> ConnectionTsUpdateTest::conn1 = nullptr;

TEST_F(ConnectionTsUpdateTest, TestTsTableUpdateSuc)
{
    conn1->Query("drop table if exists product_task_stat");
    auto r = conn1->Query(R"(create table product_task_stat(
                        planned_num int,
                        effective_num int,
                        collect_status int,
                        create_time timestamp default now()
                        ) partition by range(create_time)
                         timescale interval '1h' retention '30d' autopart crosspart;)");
    EXPECT_EQ(r->GetRetCode(), 0);
    r = conn1->Query(R"(insert into product_task_stat (planned_num, effective_num, collect_status)
                    values(999, 1000, 1000), (485, 500, 1000), (9999, 10000, 20000);)");
    EXPECT_EQ(r->GetRetCode(), 0);
    r = conn1->Query(R"(update product_task_stat set planned_num = 10 where planned_num = 999;)");
    EXPECT_EQ(r->GetRetCode(), 0);
}