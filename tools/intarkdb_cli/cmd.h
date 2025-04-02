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
 * cmd.h
 *
 * IDENTIFICATION
 * openGauss-embedded/tools/intarkdb_cli/cmd.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once
#include <time.h>

#include <chrono>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "common/record_batch.h"
#include "kv_operator.h"
#include "main/connection.h"

const std::string TOKEN_SQL_SPLIT = ";";
constexpr uint32_t MIN_SQL_LENGTH = 3;
constexpr int32_t PRINT_LOCATION_MAX_LEN = 40;
#define CLOSE_TIMES 2

enum EPrintType { invalid = 0, box, table, csv, json, insert };
const std::map<std::string, EPrintType> m_print_type = {
    {"box", box}, {"table", table}, {"csv", csv}, {"json", json}, {"insert", insert}};

enum class EImportType { invalid = 0, csv, sqlite };

class ClassCmd {
   public:
    ClassCmd(const std::string& path, Connection* conn, KvOperator* kv_oper);
    ~ClassCmd(){};
    void main();
    bool execute(std::string& query);
    // 进一步判断是否SQL
    bool IsSqlCmd(std::string query, std::vector<std::string>& strList);
    // begin commit rollback
    bool IsDCL(std::string query);

    typedef std::string (ClassCmd::*func)(const std::vector<std::string>& strList);
    std::string Help(const std::vector<std::string>& strList);
    std::string Info(const std::vector<std::string>& strList);
    std::string Version(const std::vector<std::string>& strList);
    std::string Dump(const std::vector<std::string>& strList);
    std::string Import(const std::vector<std::string>& strList);
    std::string Read(const std::vector<std::string>& strList);
    std::string ShowTable(const std::vector<std::string>& strList);
    std::string ShowIndex(const std::vector<std::string>& strList);
    std::string SHowKeys(const std::vector<std::string>& strList);
    std::string Schema(const std::vector<std::string>& strList);
    std::string FullSchema(const std::vector<std::string>& strList);

    std::string Mode(const std::vector<std::string>& strList);
    std::string Explain(const std::vector<std::string>& strList);
    std::string SetNullStr(const std::vector<std::string>& strList);
    std::string SetWidth(const std::vector<std::string>& strList);
    std::string SetEcho(const std::vector<std::string>& strList);
    std::string SetChange(const std::vector<std::string>& strList);
    std::string SetTimer(const std::vector<std::string>& strList);
    std::string SetOutFile(const std::vector<std::string>& strList);
    std::string SetOutputOnce(const std::vector<std::string>& strList);
    std::string SetPrompt(const std::vector<std::string>& strList);
    std::string ShowOption(const std::vector<std::string>& strList);
    std::string DBConfig(const std::vector<std::string>& strList);

    std::map<std::string, func> ex_func_map;  // <cmd, func>

    class PrintDelegate {
       public:
        PrintDelegate() : fp(nullptr), filename_("stdout"){};
        PrintDelegate(const char* file) : fp(fopen(file, "a")), filename_(file){};
        ~PrintDelegate() {
            if (fp) {
                fflush(fp);
                fclose(fp);
            }
        };

        template <typename T>
        typename std::enable_if<std::is_convertible<T, std::string_view>::value, void>::type Print(const T& str) const {
            if (fp == nullptr) {
                fmt::print(stdout, "{}", str);
            } else {
                fmt::print(fp, "{}", str);
            }
        }

        const char* GetFileName() const { return filename_.c_str(); }

       private:
        FILE* fp;
        std::string filename_;
    };

    void PrintResult(const std::string& str);

    const char* GetBeginPrompt() { return begin_prompt.c_str(); };
    const char* GetContinuePrompt() { return continue_prompt.c_str(); };
    uint8 close_num;

   private:
    std::string GetModeType(const std::string& separator);
    void DrowTableLine(const std::vector<size_t>& vlength, std::string_view symbols, size_t symbols_size,
                       std::string_view line);
    void PrintTable(RecordIterator& record_batch, const std::string& symbols, size_t symbols_size);
    std::function<bool(std::string&)> PrintTable(const RecordBatch& record_batch, const std::string& symbols,
                                                 size_t symbols_size);
    auto PrintInsert(RecordIterator& record_iterator, const std::string& table_name) -> void;
    auto PrintSelectRecords(RecordIterator& record_batch) -> void;
    std::string SetSwitch(const std::string& cmd, bool& bswitch, const std::vector<std::string>& strList);
    double timeDiff(const timeval& pStart, const timeval& pEnd);
    std::string ImportCSV(const std::string& file, const std::string& table, uint16 skip);

    void StringSplit(const std::string& str, const char split, std::vector<std::string>& res);
    void GetCmd(std::string str, const char split, std::vector<std::string>& res);
    std::string GetTableName(const std::string& str);

    auto IsQuitCmd(const std::string& cmd) -> bool { return cmd == ".quit" || cmd == ".exit"; }
    // 是否内置命令
    auto IsBuildInCmd(const std::string& cmd, func& build_func) -> bool;
    auto IsKVCmd(const std::string& cmd, KvOperator::kv_func& kv_func) -> bool;
    auto GetPrintDelegate(const std::string& output_file) -> std::shared_ptr<PrintDelegate>;
    auto UpdatePrintDelegate(uint16_t& output_times, std::string& tmp_out_file) -> void;
    auto PrintCmd(const std::string& cmd) -> void;
    auto UpdateTotalChanges(uint64_t current_changes) -> void;
    auto PrintEffectRow(uint64_t current_changes) -> void;

    auto PrintBuildInCmdResult(const std::string& query, const func& build_func, const std::vector<std::string>& args)
        -> void;
    auto PrintKVCmdResult(const std::string& query, const KvOperator::kv_func& kv_func,
                          const std::vector<std::string>& args) -> void;
    auto PrintSQLCmdResult(const std::string& query) -> void;

    auto PrintError(RecordIterator &record_iterator) -> void;
    auto PrintRecords(RecordIterator& record_batch) -> void;
    auto PrintRows(const std::vector<std::string>& headers, const std::vector<std::vector<std::string>>& rows,
                   std::vector<size_t>& widths, const std::string& symbols, int symbols_size, bool first_page) -> void;

    auto GetTimer() const { return std::chrono::steady_clock::now(); }

    auto PrintTimer(const std::chrono::steady_clock::time_point& start,
                    const std::chrono::steady_clock::time_point& end) -> void;

    auto PrintCSVFormat(RecordIterator& record_iterator) -> void;
    auto PrintJSONFormat(RecordIterator& record_iterator) -> void;

    auto PrintHeader(const std::vector<std::string>& headers, const std::vector<size_t>& widths,
                     const std::string& symbols, int symbols_size) -> void;
    auto PrintHeaderAndBody(RecordIterator& iterator, const std::vector<std::string>& headers,
                            std::vector<size_t>& widths, const std::string& symbols, int symbols_size) -> void;
    auto PrintFooter(const std::vector<size_t>& widths, const std::string& symbols, int symbols_size) -> void;

    auto RecordToString(const Record& record, const std::string& null_format) -> std::vector<std::string>;

    auto IsFinishQuery(const std::string&) -> bool;

    Connection* sql_conn;
    KvOperator* kv_operator;

    std::string db_path;
    std::shared_ptr<PrintDelegate> print_delegate_;
    uint16_t output_times{0};
    std::string tmp_output_file{""};
    EPrintType print_type;
    std::string null_str{""};
    size_t min_col_len{8};
    uint64_t total_changes{0};
    std::string begin_prompt{"intarkdb> "};
    std::string continue_prompt{"     ...> "};
    std::string help_msg;

    // switch
    bool b_echo{0};
    bool b_show_change{0};
    bool b_timer{0};
    bool b_explain{0};

    // sql
    std::string query_sql;
};
