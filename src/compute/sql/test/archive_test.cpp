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
 * archive_test.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/test/archive_test.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <gtest/gtest.h>

#include <fstream>
#include <filesystem>
#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "common/default_value.h"
#include "main/connection.h"
#include "main/database.h"

std::string tablename("arch_table");
void StartDBAndModifyCfgFile();

class ArchiveTest : public ::testing::Test {
   protected:
    ArchiveTest() {}
    ~ArchiveTest() {}
    static void SetUpTestSuite() {
        {
            std::shared_ptr<IntarkDB> l_db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
            // 启动db
            l_db_instance->Init();
            auto main_conn = std::make_unique<Connection>(l_db_instance);
            main_conn->Init();

            auto r = main_conn->Query("CREATE TABLE IF NOT EXISTS z1 (chgtime timestamp, x1 INT, x2 INT, txt VARCHAR(100)) PARTITION BY RANGE(chgtime) TIMESCALE INTERVAL '1h' AUTOPART CROSSPART;");
            if(r->GetRetCode() != GS_SUCCESS) {
                std::cout << "Failed to create table" << std::endl;
            }
        }
        StartDBAndModifyCfgFile();

        db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
        conn = std::make_unique<Connection>(db_instance);
        conn->Init();
    }

    void insert_proc();
    // Per-test-suite tear-down.
    // Called after the last test in this test suite.
    // Can be omitted if not needed.
    static void TearDownTestSuite() { conn.reset(); }

    void SetUp() override {}

    // void TearDown() override {}

    static std::shared_ptr<IntarkDB> db_instance;
    static std::unique_ptr<Connection> conn;
};
std::shared_ptr<IntarkDB> ArchiveTest::db_instance = nullptr;
std::unique_ptr<Connection> ArchiveTest::conn = nullptr;

void StartDBAndModifyCfgFile(){
    {
        std::shared_ptr<IntarkDB> db = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance("./"));
    }
    std::string filename = "./intarkdb/cfg/intarkdb.ini";
    std::string tempFilename = "temp.ini";

    std::string keyToModify = "MAX_ARCH_FILES_SIZE";
    std::string newValue = "16M";

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
        if (line.find("REDO_SPACE_SIZE") != std::string::npos) {
            std::cout << line << std::endl;
        }
        if (line.find("ARCHIVE_MODE") != std::string::npos) {
            line = std::string("ARCHIVE_MODE") + " = " + std::to_string(1);
        }
        tempFile << line << '\n';
    }
    inputFile.close();
    tempFile.close();

    if (std::rename(tempFilename.c_str(), filename.c_str()) != 0) {
        std::cerr << "Error renaming file." << std::endl;
    }
}

unsigned int swizzle(unsigned int in, unsigned int limit) {
  unsigned int out = 0;
  while (limit) {
    out = (out << 1) | (in & 1);
    in >>= 1;
    limit >>= 1;
  }
  return out;
}

std::string numbername(unsigned int n) {
  static const std::vector<std::string> ones = {
      "zero",    "one",     "two",       "three",    "four",
      "five",    "six",     "seven",     "eight",    "nine",
      "ten",     "eleven",  "twelve",    "thirteen", "fourteen",
      "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"};

  static const std::vector<std::string> tens = {
      "",      "ten",   "twenty",  "thirty", "forty",
      "fifty", "sixty", "seventy", "eighty", "ninety"};

  std::ostringstream oss;

  if (n >= 1000000000) {
    oss << numbername(n / 1000000000) << " billion";
    n %= 1000000000;
  }

  if (n >= 1000000) {
    if (!oss.str().empty())
      oss << ' ';
    oss << numbername(n / 1000000) << " million";
    n %= 1000000;
  }

  if (n >= 1000) {
    if (!oss.str().empty())
      oss << ' ';
    oss << numbername(n / 1000) << " thousand";
    n %= 1000;
  }

  if (n >= 100) {
    if (!oss.str().empty())
      oss << ' ';
    oss << ones[n / 100] << " hundred";
    n %= 100;
  }

  if (n >= 20) {
    if (!oss.str().empty())
      oss << ' ';
    oss << tens[n / 10];
    n %= 10;
  }

  if (n > 0) {
    if (!oss.str().empty())
      oss << ' ';
    oss << ones[n];
  }

  if (oss.str().empty()) {
    oss << "zero";
  }

  return oss.str();
}

void ArchiveTest::insert_proc() {
  conn->Query("begin");
  std::string insert_sql = "INSERT INTO z1 VALUES ";
  for (int i = 1; i < 330; i++) {
    insert_sql += "(now(), ?, ?, ?),";
  }
  insert_sql += "(now(), ?, ?, ?);";

  int i = 0;
  uint32_t maxb = 100 * 1000;

  auto prepare = conn->Prepare(insert_sql.c_str());
  if (prepare->HasError()) {
    std::cout << "Failed to prepare" << std::endl;
    return;
  }

  std::vector<Value> values;
  values.reserve(990);
  uint64_t time_cost = 0;
  for(i = 1 ; i <= 1100220; ++i) {
    auto x1 = swizzle(i, maxb);
    auto txt = numbername(x1);
    // auto txt = "1234234";
    values.emplace_back(ValueFactory::ValueInt(x1));
    values.emplace_back(ValueFactory::ValueInt(i));
    values.emplace_back(ValueFactory::ValueVarchar(txt));
    if (i % 330 ==0) {
      auto start = std::chrono::high_resolution_clock::now();
      prepare->Execute(values);
      conn->Query("commit");
      conn->Query("begin");
      time_cost += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start).count();
      values.clear();
    }
  }
  conn->Query("commit");
  std::cout << "execture time cost(us): " << time_cost << std::endl;
}

TEST_F(ArchiveTest, ArchiveTestOutOfMax) {
    // add column ,type integer, primary ke
    try {
        insert_proc();
        EXPECT_TRUE(true);
    } catch (const std::exception& e) {
        std::cout << "Exception: " << e.what() << std::endl;
        EXPECT_FALSE(true);
    }

    std::string path = "./intarkdb/archive_log/";

    for (const auto &entry : std::filesystem::directory_iterator(path)) {
        if (std::filesystem::is_regular_file(entry.path())) {
            std::cout << entry.path().filename() << std::endl;
        }
        EXPECT_STRNE(entry.path().filename().c_str(), "arch_0_0.arc");
    }
}

int main(int argc, char** argv) {
    system("rm -rf intarkdb/");
    ::testing::GTEST_FLAG(output) = "xml";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
