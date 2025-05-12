/*
 * Copyright (c) GBA-NCTI-ISDC. 2022-2024.
 *
 * openGauss embedded is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2. You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FITFOR A PARTICULAR PURPOSE. See the
 * Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * cmd.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/tools/intarkdb_cli/cmd.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cmd.h"

#include <fmt/color.h>
#include <fmt/core.h>
#include <fmt/format.h>
#ifdef _MSC_VER
#include <windows.h>
#else
#include <sys/time.h>
#endif

#include <fstream>
#include <functional>
#include <iostream>
#include <list>
#include <ratio>
#include <set>
#include <sstream>
#include <string>

#include "cJSON.h"
#include "cm_signal.h"
#include "include/intarkdb.h"
#include "compute/kv/intarkdb_kv.h"
#include "storage/gstor/zekernel/common/cm_thread.h"
#include "linenoise.h"
#include "cli_util.h"


#define PLATFORM_INFO_SIZE 1024

ClassCmd::ClassCmd(const std::string &path, Connection *conn, KvOperator *kv_oper)
    : sql_conn(conn), kv_operator(kv_oper), db_path(path + "intarkdb"), print_type(table) {
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".help", &ClassCmd::Help));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".dbinfo", &ClassCmd::Info));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".version", &ClassCmd::Version));

    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".dump", &ClassCmd::Dump));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".import", &ClassCmd::Import));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".read", &ClassCmd::Read));

    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".table", &ClassCmd::ShowTable));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".index", &ClassCmd::ShowIndex));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".keys", &ClassCmd::SHowKeys));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".schema", &ClassCmd::Schema));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".fullschema", &ClassCmd::FullSchema));

    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".mode", &ClassCmd::Mode));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".explain", &ClassCmd::Explain));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".nullvalue", &ClassCmd::SetNullStr));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".width", &ClassCmd::SetWidth));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".echo", &ClassCmd::SetEcho));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".change", &ClassCmd::SetChange));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".timer", &ClassCmd::SetTimer));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".output", &ClassCmd::SetOutFile));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".once", &ClassCmd::SetOutputOnce));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".prompt", &ClassCmd::SetPrompt));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".show", &ClassCmd::ShowOption));
    ex_func_map.insert(std::make_pair<std::string, ClassCmd::func>(".dbconfig", &ClassCmd::DBConfig));

    help_msg = "Help: \n";
    std::map<std::string, std::string> help_map;
    int length = 20;
    help_map.insert(std::make_pair(".echo on|off", "Turn command echo on or off"));
    help_map.insert(std::make_pair(".quit", "Exit this program"));
    help_map.insert(std::make_pair(".exit", "Exit this program"));
    help_map.insert(std::make_pair(".help", "Show help text"));
    help_map.insert(std::make_pair(".help kv", "Show help text for kv"));
    help_map.insert(std::make_pair(".dbinfo", "Show status information about the database"));
    help_map.insert(std::make_pair(".version", "Show  version infomation"));
    help_map.insert(std::make_pair(".dump", "Render database content as SQL"));
    help_map.insert(std::make_pair(".import $FILE $TABLE", "Import data from FILE into TABLE"));
    help_map.insert(std::make_pair(".read $FILE", "Read input from FILE"));

    help_map.insert(std::make_pair(".table $TABLE", "List names of tables matching LIKE pattern TABLE"));
    help_map.insert(std::make_pair(".index $TABLE", "Show names of indexes"));
    help_map.insert(std::make_pair(".schema $PATTERN", "Show the CREATE statements matching PATTERN"));
    help_map.insert(std::make_pair(".fullschema", "Show schema and the content of sqlite_stat tables"));
    help_map.insert(std::make_pair(".keys", "show all keys"));

    help_map.insert(std::make_pair(".mode", "Set output mode, support: " + GetModeType(",")));
    help_map.insert(std::make_pair(".explain on|off", "Turn command echo on or off"));
    help_map.insert(std::make_pair(".nullvalue", "Use STRING in place of NULL values"));
    help_map.insert(std::make_pair(".width $NUM", "Set minimum column widths"));
    help_map.insert(std::make_pair(".echo on|off", "Turn command echo on or off"));
    help_map.insert(std::make_pair(".change on|off", "Show number of rows changed by SQL"));
    help_map.insert(std::make_pair(".timer on|off", "Turn SQL timer on or off"));
    help_map.insert(std::make_pair(".output $FILE", "Send output to FILE or stdout if FILE is omitted"));
    help_map.insert(std::make_pair(".once $FILE", "Output for the next SQL command only to FILE"));
    help_map.insert(std::make_pair(".prompt", "Replace the standard prompts"));
    help_map.insert(std::make_pair(".show", "Show the current values for various settings"));
    help_map.insert(std::make_pair(".dbconfig $OP $VAL", " List or change onfig optionss"));
    for (auto item : help_map) {
        fmt::format_to(std::back_inserter(help_msg), "{:<{}}: {}\n", item.first, length, item.second);
    }
}

void ClassCmd::main() {
    int HistorySetMaxLen = 30;
    close_num = 0;
    auto prompt = GetBeginPrompt();
    print_delegate_ = GetPrintDelegate("stdout");
    std::cout << "WelCome intarkdb shell." << std::endl;
    while (true) {
        std::string query;
        while (true) {
            char *input = linenoise(prompt);
            if (input == nullptr) {
                close_num++;
                if (close_num < CLOSE_TIMES) {
                    std::cout << "If you want to close, press ctrl+c again" << std::endl;
                    continue;
                } else {
                    std::cout << "Closing..." << std::endl;
                    return;
                }
            } else {
                close_num = 0;
            }
            linenoiseEnableRawMode();
            
            query += std::string(input);
            if (IsFinishQuery(query)) {
                prompt = GetBeginPrompt();
                break;
            }
            prompt = GetContinuePrompt();
            query += " \n ";

        }
        linenoiseDisableRawMode();
        linenoiseHistoryAdd(query.c_str());
        if (execute(query) == false) {
            std::cout << "Closing..." << std::endl;
            break;
        }
    }
    return;
}

const size_t DOUBLE_QUOTE_SIZE = 2;

std::string ClassCmd::GetTableName(const std::string &str) {
    static const char double_quote = '\"';
    if (str[0] == double_quote && str.back() == double_quote) {
        return str.substr(1, str.size() - DOUBLE_QUOTE_SIZE);
    }
    std::string tmp = str;
    transform(tmp.begin(), tmp.end(), tmp.begin(), ::tolower);
    return tmp;
}

void ClassCmd::GetCmd(std::string str, const char split, std::vector<std::string> &res) {
    std::istringstream iss(str);
    std::string buf;
    while (getline(iss, buf, split)) {
        if (buf.length() > 0 && buf != "\n") res.push_back(buf);
    }
    // 去掉";"
    if (res.size() > 0) {
        int last = res.size() - 1;
        if (res[last][res[last].size() - 1] == ';') res[last] = res[last].substr(0, res[last].size() - 1);
        if (res[last].size() == 0) res.pop_back();
    }
}

bool ClassCmd::execute(std::string &query) {
    std::vector<std::string> strList;
    GetCmd(query, ' ', strList);
    std::string result;
    try {
        if (IsQuitCmd(strList[0])) {
            return false;
        }

        func build_func = nullptr;
        if (IsBuildInCmd(strList[0], build_func)) {
            PrintBuildInCmdResult(query, build_func, strList);
            return true;
        }

        KvOperator::kv_func kv_func = nullptr;
        if (IsKVCmd(strList[0], kv_func) && !IsSqlCmd(query, strList)) {
            PrintKVCmdResult(query, kv_func, strList);
            if (!IsDCL(strList[0])) {
                return true;
            }
        }

        PrintSQLCmdResult(query);
    } catch (const std::exception &ex) {
        std::cout << "Error:" << std::string(ex.what()) << std::endl;
    }
    return true;
}

bool ClassCmd::IsSqlCmd(std::string query, std::vector<std::string> &strList) {
    std::string cmd = strList[0];
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::tolower);

    std::string sql = query;
    std::transform(sql.begin(), sql.end(), sql.begin(), ::tolower);
    if (cmd == "set") {
        if (sql.find("auto_commit") != std::string::npos && sql.find("=") != std::string::npos) {
            return true;
        }
        if (sql.find("max_connections") != std::string::npos && sql.find("=") != std::string::npos) {
            return true;
        }
        if (strList.size() != 3) {
            return true;
        }
    }

    return false;
}

bool ClassCmd::IsDCL(std::string query) {
    std::string result = query;
    result.erase(std::remove_if(result.begin(), result.end(), [](unsigned char ch) {
        return std::isspace(ch);
    }), result.end());

    if (result == "begin" || result == "commit" || result == "rollback") {
        return true;
    }
    return false;
}

const size_t SPECIFIC_HELP_ARGS_NUM = 2;

std::string ClassCmd::Help(const std::vector<std::string> &strList) {
    if (strList.size() == 1)
        return help_msg;
    else if (strList.size() == SPECIFIC_HELP_ARGS_NUM) {
        if (strList[1] == "kv") return KvOperator::show_kv_help();
    }

    return help_msg;
}

std::string ClassCmd::GetModeType(const std::string &separator) {
    std::string s_mode_type_msg;
    for (const auto &item : m_print_type) s_mode_type_msg += item.first + separator;
    s_mode_type_msg = s_mode_type_msg.substr(0, s_mode_type_msg.size() - 1);
    return s_mode_type_msg;
}

const size_t MODE_ARGS_NUM = 2;

std::string ClassCmd::Mode(const std::vector<std::string> &strList) {
    std::string prompt_msg = "Useage: .mode " + GetModeType("|") + "\n";
    if (strList.size() != MODE_ARGS_NUM) {
        return prompt_msg;
    }
    auto item = m_print_type.find(strList[1]);
    if (item == m_print_type.end()) return prompt_msg;
    print_type = item->second;
    return "change mode = " + strList[1] + "\n";
}

std::string ClassCmd::Info(const std::vector<std::string> &strList) {
    std::stringstream msg;
    msg << "Info: " << std::endl;
    msg << "version: " << get_version() << std::endl;
    msg << "git commit id: " << get_commit_id() << std::endl;
    msg << "DB_PATH: " << db_path << std::endl;
    return msg.str();
}

std::string ClassCmd::Version(const std::vector<std::string> &strList) {
    std::stringstream msg;
    msg << "Info: " << std::endl;
    msg << "version: " << get_version() << std::endl;
    msg << "git commit id: " << get_commit_id() << std::endl;
    return msg.str();
}

const size_t SET_NULL_STR_ARGS_NUM = 2;

std::string ClassCmd::SetNullStr(const std::vector<std::string> &strList) {
    std::string prompt_msg = "Useage: .nullvalue $null\n";
    if (strList.size() != SET_NULL_STR_ARGS_NUM) {
        return prompt_msg;
    }
    null_str = strList[1];
    return "change nullvalue = " + strList[1] + "\n";
}

const size_t SET_WIDTH_ARGS_NUM = 2;

std::string ClassCmd::SetWidth(const std::vector<std::string> &strList) {
    std::string prompt_msg = "Useage: .width $num\n";
    if (strList.size() != SET_WIDTH_ARGS_NUM) {
        return prompt_msg;
    }
    min_col_len = atoi(strList[1].c_str());
    return "change min width = " + strList[1];
}

void ClassCmd::DrowTableLine(const std::vector<size_t> &vlength, std::string_view symbols, size_t symbols_size,
                             std::string_view line) {
    const uint32_t SYMBOLS_AND_INTERVAL_SIZE = 3;
    std::stringstream s_format;
    s_format << "{:" << line << ">{}}";
    print_delegate_->Print(symbols.substr(0, symbols_size));
    for (std::size_t i = 0; i < vlength.size() - 1; i++) {
        print_delegate_->Print(fmt::format(s_format.str(), symbols.substr(symbols_size, symbols_size),
                                           vlength[i] + SYMBOLS_AND_INTERVAL_SIZE));
    }
    s_format << "\n";
    print_delegate_->Print(fmt::format(s_format.str(), symbols.substr(symbols_size * 2, symbols_size),
                                       vlength[vlength.size() - 1] + SYMBOLS_AND_INTERVAL_SIZE));
}

static auto UpdateWidth(const std::vector<std::string> &record, std::vector<size_t> &vlength) -> void {
    for (std::size_t i = 0; i < record.size(); i++) {
        if (record[i].size() > vlength[i]) {
            vlength[i] = record[i].size();
        }
    }
}

static auto UpdateWidth(const std::vector<std::vector<std::string>> &record_batch, std::vector<size_t> &vlength)
    -> void {
    for (const auto &row : record_batch) {
        UpdateWidth(row, vlength);
    }
}

auto ClassCmd::PrintHeader(const std::vector<std::string> &headers, const std::vector<size_t> &widths,
                           const std::string &symbols, int symbols_size) -> void {
    std::string_view symbols_view = symbols;
    std::string_view first_line_symbol(symbols_view.substr(symbols_size * 2, symbols_size * 3));
    std::string_view line_symbol(symbols_view.substr(symbols_size, symbols_size));
    std::string_view separator(symbols_view.substr(0, symbols_size));
    std::string_view second_line_symbol(symbols_view.substr(symbols_size * 5, symbols_size * 3));

    DrowTableLine(widths, first_line_symbol, symbols_size, line_symbol);
    for (std::size_t i = 0; i < headers.size(); i++) {
        print_delegate_->Print(fmt::format(fmt::emphasis::bold, "{} {:<{}} ", separator, headers[i], widths[i]));
    }
    print_delegate_->Print(separator);
    print_delegate_->Print("\n");
    DrowTableLine(widths, second_line_symbol, symbols_size, line_symbol);
}

auto ClassCmd::PrintFooter(const std::vector<size_t> &widths, const std::string &symbols, int symbols_size) -> void {
    std::string_view symbols_view = symbols;
    std::string_view end_line_symbol(symbols_view.substr(symbols_size * 8, symbols_size * 3));
    std::string_view line_symbol(symbols_view.substr(symbols_size, symbols_size));
    DrowTableLine(widths, end_line_symbol, symbols_size, line_symbol);
}

auto ClassCmd::PrintRows(const std::vector<std::string> &headers, const std::vector<std::vector<std::string>> &rows,
                         std::vector<size_t> &widths, const std::string &symbols, int symbols_size, bool first_page)
    -> void {
    std::string_view symbols_view = symbols;
    std::string_view separator(symbols_view.substr(0, symbols_size));
    if (first_page) {
        PrintHeader(headers, widths, symbols, symbols_size);
    }
    for (size_t i = 0; i < rows.size(); ++i) {
        for (size_t j = 0; j < rows[i].size(); ++j) {
            print_delegate_->Print(fmt::format("{} {:<{}} ", separator, rows[i][j], widths[j]));
        }
        print_delegate_->Print(separator);
        print_delegate_->Print("\n");
    }
}

auto ClassCmd::PrintHeaderAndBody(RecordIterator &records, const std::vector<std::string> &headers,
                                  std::vector<size_t> &widths, const std::string &symbols, int symbols_size) -> void {
    constexpr int PAGE_SIZE = 100;  // 每次打印的行数
    int row_count = 0;
    std::vector<std::vector<std::string>> record_batch;
    record_batch.reserve(PAGE_SIZE);
    bool first_page = true;
    UpdateWidth(headers, widths);  // 根据header更新列宽
    while (true) {
        const auto &[record, eof] = records.Next();
        if (eof) {
            break;
        }
        row_count++;
        record_batch.push_back(RecordToString(record, null_str));
        if (row_count == PAGE_SIZE) {
            UpdateWidth(record_batch, widths);  // 根据新批次的数据更新列宽
            PrintRows(headers, record_batch, widths, symbols, symbols_size, first_page);
            first_page = false;
            record_batch.clear();
            row_count = 0;
        }
    }

    if (row_count > 0) {
        UpdateWidth(record_batch, widths);  // 根据新批次的数据更新列宽
        PrintRows(headers, record_batch, widths, symbols, symbols_size, first_page);
        first_page = false;
    }
    if (first_page) {
        // empty table , 上面都没有打印
        PrintRows(headers, record_batch, widths, symbols, symbols_size, first_page);
    }
}

// 打印整个结果集
auto ClassCmd::PrintTable(RecordIterator &records, const std::string &symbols, size_t symbols_size) -> void {
    auto headers = records.GetHeader();
    std::vector<size_t> widths(headers.size(), min_col_len);
    PrintHeaderAndBody(records, headers, widths, symbols, symbols_size);
    PrintFooter(widths, symbols, symbols_size);
}

auto ClassCmd::PrintSelectRecords(RecordIterator &record_iterator) -> void {
    switch (b_explain ? table : print_type) {
        case box: {
            PrintTable(record_iterator, "│─┌┬┐├┼┤└┴┘", 3);  // 3 bytes
            break;
        }
        case table: {
            PrintTable(record_iterator, "|-+++++++++", 1);
            break;
        }
        case csv: {
            PrintCSVFormat(record_iterator);
            break;
        }
        case json: {
            PrintJSONFormat(record_iterator);
            break;
        }
        case insert: {
            PrintInsert(record_iterator, "\"table\"");
            break;
        }
        default:
            break;
    }
}

static auto PrintInsertValue(std::shared_ptr<ClassCmd::PrintDelegate> &print_delegate, const Value &v) {
    if (v.IsNull()) {
        print_delegate->Print("NULL");
    } else if (v.IsNumeric()) {
        print_delegate->Print(fmt::format("{}", v.GetCastAs<double>()));
    } else if (v.GetLogicalType().TypeId() == GS_TYPE_BOOLEAN) {
        print_delegate->Print(fmt::format("{}", v.GetCastAs<bool>()));
    } else {
        print_delegate->Print(fmt::format("\"{}\"", v.ToString()));
    }
}

auto ClassCmd::PrintInsert(RecordIterator &record_iterator, const std::string &table_name) -> void {
    while (true) {
        const auto &[record, eof] = record_iterator.Next();
        if (eof) {
            break;
        }
        print_delegate_->Print("INSERT INTO " + table_name + " VALUES");
        auto col_num = record.ColumnCount();
        for (uint32_t i = 0; i < col_num; ++i) {
            const auto &v = record.Field(i);
            if (i == 0) {
                print_delegate_->Print("(");
            } else {
                print_delegate_->Print(",");
            }
            PrintInsertValue(print_delegate_, v);
        }
        print_delegate_->Print(");\n");
    }
}

auto ClassCmd::PrintError(RecordIterator &record_iterator) -> void {
    if (record_iterator.GetRetCode() != 0) {
        print_delegate_->Print(
            fmt::format("Error Code:{} Msg:{}\n", record_iterator.GetRetCode(), record_iterator.GetRetMsg()));

        auto location = record_iterator.GetRetLocation();
        if (location < 0) {
            return;
        }

        // Print one line only
        int32_t query_sql_len = query_sql.length();
        auto back_i = location;
        auto forward_i = location;
        for (; back_i > 0; back_i--) {
            auto c = query_sql.at(back_i);
            if (c == '\r' || c == '\n') {
                back_i++;
                break;
            }
        }
        for (; forward_i < query_sql_len; forward_i++) {
            auto c = query_sql.at(forward_i);
            if (c == '\r' || c == '\n') {
                break;
            }
        }

        // Can't print too long
        std::string back_prefix_1;
        std::string back_prefix_2;
        if (location - back_i > PRINT_LOCATION_MAX_LEN) {
            back_i = location - PRINT_LOCATION_MAX_LEN;
            back_prefix_1 = "...";
            back_prefix_2 = "   ";
        }
        std::string forward_prefix;
        if (forward_i - location > PRINT_LOCATION_MAX_LEN) {
            forward_i = location + PRINT_LOCATION_MAX_LEN;
            forward_prefix = "...";
        }
        auto err_sql = query_sql.substr(back_i, forward_i - back_i);
        auto err_pos = location - back_i;
        print_delegate_->Print(
            fmt::format("  {}{}{}\n", back_prefix_1, err_sql, forward_prefix));

        std::string err_location;
        if (err_pos > 0) {
            err_location = std::string(err_pos, ' ');
        }
        print_delegate_->Print(
            fmt::format("  {}{}^\n", err_location, back_prefix_2));
    }
}

auto ClassCmd::PrintRecords(RecordIterator &record_iterator) -> void {
    if (record_iterator.GetRetCode() != 0) {
        return PrintError(record_iterator);
    }

    switch (record_iterator.GetStmtType()) {
        case StatementType::INVALID_STATEMENT: {
            return;
        }
        case StatementType::SHOW_STATEMENT:
        case StatementType::SELECT_STATEMENT: {
            PrintSelectRecords(record_iterator);
            break;
        }        
        default: {
            print_delegate_->Print("Query OK\n");
            break;
        }
    }
    UpdateTotalChanges(record_iterator.GetEffectRow());
    PrintEffectRow(record_iterator.GetEffectRow());
}

void ClassCmd::PrintTimer(const std::chrono::steady_clock::time_point &start,
                          const std::chrono::steady_clock::time_point &end) {
    if (b_timer) {
        print_delegate_->Print(
            fmt::format("Run Time: {:.4f} ms \n", std::chrono::duration<double, std::milli>(end - start).count()));
    }
}

void ClassCmd::UpdatePrintDelegate(uint16_t &output_times, std::string &tmp_out_file) {
    if (output_times > 0) {
        if (tmp_out_file.size() > 0) {
            print_delegate_ = GetPrintDelegate(tmp_out_file);
            tmp_out_file.clear();
        } else {
            output_times--;
            if (output_times == 0) {
                print_delegate_ = GetPrintDelegate("stdout");
            }
        }
    }
}

void ClassCmd::PrintResult(const std::string &str) {
    print_delegate_->Print(str);
    // 更新print_delegate_
    UpdatePrintDelegate(output_times, tmp_output_file);
}

std::string ClassCmd::SetEcho(const std::vector<std::string> &strList) { return SetSwitch(".echo", b_echo, strList); }

std::string ClassCmd::SetChange(const std::vector<std::string> &strList) {
    return SetSwitch(".change", b_show_change, strList);
}

std::string ClassCmd::SetTimer(const std::vector<std::string> &strList) {
    return SetSwitch(".timer", b_timer, strList);
}

std::string ClassCmd::Explain(const std::vector<std::string> &strList) {
    return SetSwitch(".explain", b_explain, strList);
}

const size_t SWITCH_ARGS_NUM = 2;

std::string ClassCmd::SetSwitch(const std::string &cmd, bool &bswitch, const std::vector<std::string> &strList) {
    static std::string prompt_msg = "Useage: " + cmd + " on|off \n";  // 只创建一次
    if (strList.size() != SWITCH_ARGS_NUM) {
        return prompt_msg;
    }

    if (strList[1] == "on") {
        bswitch = true;
    } else if (strList[1] == "off") {
        bswitch = false;
    } else {
        return prompt_msg;
    }
    return "";
}

const size_t OUT_FILE_ARGS_NUM = 2;

std::string ClassCmd::SetOutFile(const std::vector<std::string> &strList) {
    std::string prompt_msg = "Useage: .output FILE|off\n";
    if (strList.size() != OUT_FILE_ARGS_NUM) {
        return prompt_msg;
    }
    if (strList[1] == "off") {
        print_delegate_ = GetPrintDelegate("stdout");
    } else {
        print_delegate_ = GetPrintDelegate(strList[1]);
    }
    output_times = 0;
    return "";
}

const size_t OUTPUT_ONCE_ARGS_NUM = 2;

std::string ClassCmd::SetOutputOnce(const std::vector<std::string> &strList) {
    std::string prompt_msg = "Useage: .once FILE\n";
    if (strList.size() != OUTPUT_ONCE_ARGS_NUM) {
        return prompt_msg;
    }
    tmp_output_file = strList[1];
    output_times = 1;
    return "";
}

const size_t PROMPT_ARGS_NUM = 2;

std::string ClassCmd::SetPrompt(const std::vector<std::string> &strList) {
    std::string prompt_msg = "Useage: .prompt your_prompt\n";
    if (strList.size() != PROMPT_ARGS_NUM) {
        return prompt_msg;
    }
    begin_prompt.clear();
    continue_prompt.clear();
    fmt::format_to(std::back_inserter(begin_prompt), "{:<{}}> ", strList[1], strList[1].size());
    fmt::format_to(std::back_inserter(continue_prompt), "{:>{}} ", ">>>", strList[1].size() + 1);
    return "";
}

std::string ClassCmd::ShowOption(const std::vector<std::string> &strList) {
    std::string msg;
    int length = 10;
    fmt::format_to(std::back_inserter(msg), "{:>{}}: {}\n", "path", length, db_path);
    fmt::format_to(std::back_inserter(msg), "{:>{}}: {}\n", "output", length, print_delegate_->GetFileName());

    for (auto item : m_print_type)
        if (item.second == print_type) {
            fmt::format_to(std::back_inserter(msg), "{:>{}}: {}\n", "mode", length, item.first);
            break;
        }
    fmt::format_to(std::back_inserter(msg), "{:>{}}: {}\n", "nullvalue", length, null_str);
    fmt::format_to(std::back_inserter(msg), "{:>{}}: {}\n", "echo", length, b_echo ? "on" : "off");
    fmt::format_to(std::back_inserter(msg), "{:>{}}: {}\n", "change", length, b_show_change ? "on" : "off");
    fmt::format_to(std::back_inserter(msg), "{:>{}}: {}\n", "timer", length, b_timer ? "on" : "off");
    return msg;
}

std::string ClassCmd::ShowTable(const std::vector<std::string> &strList) {
    std::string result;
    std::stringstream sql_query;
    sql_query << "select NAME from \"SYS_TABLES\" where \"SPACE#\"=" << SQL_SPACE_TYPE_USERS;
    if (strList.size() > 1) {
        std::string sName = strList[1];
        transform(sName.begin(), sName.end(), sName.begin(), ::tolower);
        sql_query << " and (NAME like '" << sName << "' or NAME like '" << strList[1] << "')";
    }
    sql_query << ";";
    auto records = sql_conn->Query(sql_query.str().c_str())->GetRecords();
    for (std::size_t i = 1; i < records.size(); i++) {
        for (auto item : records[i]) {
            result += item + "\n";
        }
    }
    return result;
}

std::string ClassCmd::ShowIndex(const std::vector<std::string> &strList) {
    std::string result;
    std::stringstream sql_query;
    sql_query << "select ti.NAME from \"SYS_INDEXES\" ti join (select \"ID\", "
                 "\"NAME\" from \"SYS_TABLES\" "
              << "where \"SPACE#\"=" << SQL_SPACE_TYPE_USERS;
    if (strList.size() > 1) {
        std::string sName = strList[1];
        transform(sName.begin(), sName.end(), sName.begin(), ::tolower);
        sql_query << " and (NAME like '" << sName << "' or NAME like '" << strList[1] << "')";
    }
    sql_query << ") tt on ti.\"TABLE#\" = tt.\"ID\";";
    auto records = sql_conn->Query(sql_query.str().c_str())->GetRecords();
    for (std::size_t i = 1; i < records.size(); i++) {
        for (auto item : records[i]) {
            result += item + "\n";
        }
    }
    return result;
}

std::string ClassCmd::SHowKeys(const std::vector<std::string> &strList) {
    std::string result;
    std::stringstream sql_query;
    sql_query << "SELECT KEY FROM \"" << kv_operator->getKVTable() << "\";";
    auto records = sql_conn->Query(sql_query.str().c_str())->GetRecords();
    for (std::size_t i = 1; i < records.size(); i++) {
        for (auto item : records[i]) {
            result += item + "\n";
        }
    }
    return result;
}

const size_t SCHEMA_ARGS_NUM = 2;

std::string ClassCmd::Schema(const std::vector<std::string> &strList) {
    std::string prompt_msg = "Useage: .schema $TABLE\n";
    if (strList.size() != SCHEMA_ARGS_NUM) {
        return prompt_msg;
    }
    std::vector<std::string> v_index_sqls;
    std::string table = GetTableName(strList[1]);
    std::string result = sql_conn->ShowCreateTable(table, v_index_sqls) + "\n";
    for (auto item : v_index_sqls) {
        result += item + "\n";
    }
    return result;
}

std::string ClassCmd::FullSchema(const std::vector<std::string> &strList) {
    std::string result;
    std::stringstream sql_query;
    sql_query << "show tables;";
    auto records = sql_conn->Query(sql_query.str().c_str())->GetRecords();
    for (std::size_t i = 1; i < records.size(); i++) {
        for (auto item : records[i]) {
            std::vector<std::string> v_index_sqls;
            result += sql_conn->ShowCreateTable(item, v_index_sqls) + "\n";
            for (auto item : v_index_sqls) {
                result += item + "\n";
            }
            result += "\n";
        }
    }
    return result;
}

const size_t DUMP_ARGS_NUM = 2;
// TODO: 内置函数的输出也不一定是短小的，需要考虑机制让内置函数也可以选择
// 构造字符串，或直接使用 print_delegate 输出
std::string ClassCmd::Dump(const std::vector<std::string> &strList) {
    // NOTE： 先特殊处理让结果直接输出，并返回一个空字符串
    std::string prompt_msg = "Useage: .dump $TABLE\n";
    if (strList.size() != DUMP_ARGS_NUM) {
        return prompt_msg;
    }
    print_delegate_->Print("BEGIN;\n");
    std::vector<std::string> v_index_sqls;
    auto create_table_sql = sql_conn->ShowCreateTable(strList[1], v_index_sqls);
    print_delegate_->Print(create_table_sql);
    print_delegate_->Print("\n");
    std::string sql_query = "SELECT * FROM " + strList[1];
    auto record_iterator = sql_conn->QueryIterator(sql_query.c_str());
    PrintInsert(*record_iterator, strList[1]);
    print_delegate_->Print("COMMIT;\n");
    return "";
}

void ClassCmd::StringSplit(const std::string &str, const char split, std::vector<std::string> &res) {
    std::istringstream iss(str + split);
    std::string buf;
    while (getline(iss, buf, split)) {
        res.push_back(buf);
    }
}

std::string ClassCmd::ImportCSV(const std::string &file, const std::string &table, uint16 skip) {
    std::ifstream fin;
    fin.open(file.c_str(), std::ios::in);
    if (!fin.is_open()) {
        return "open file: " + file + " error\n";
    }

    std::string str_line;
    // skip n
    for (uint16 i = 0; i < skip; i++) getline(fin, str_line);

    sql_conn->Query("BEGIN");

    // 判断数据表是否存在，不存在则创建
    if (sql_conn->GetTableInfo(table) == nullptr) {
        getline(fin, str_line);
        std::vector<std::string> col_name;
        StringSplit(str_line, ',', col_name);
        std::stringstream sql;
        sql << "CREATE TABLE " << table << " (";
        for (size_t i = 0; i < col_name.size(); i++) {
            if (i != 0) sql << ", ";
            sql << "\"" << col_name[i] << "\" VARCHAR";
        }
        sql << ");";
        auto r = sql_conn->Query(sql.str().c_str());
        if (r->GetRetCode() != 0) {
            std::stringstream sErrMsg;
            sErrMsg << "Error Code:" << r->GetRetCode() << " Msg:" << r->GetRetMsg() << std::endl;
            return sErrMsg.str();
        }
    }
    std::vector<std::vector<std::string>> rows;
    bool bResult = true;
    std::string result = "Import OK\n";
    while (getline(fin, str_line) && str_line.size() > 0) {
        std::vector<std::string> row;
        StringSplit(str_line, ',', row);
        std::stringstream insert_sql;
        insert_sql << "INSERT INTO " << table << " VALUES(";
        for (size_t i = 0; i < row.size(); i++) {
            if (i != 0) insert_sql << ", ";
            if (row[i].size() > 0)
                insert_sql << "'" << row[i] << "'";
            else
                insert_sql << "NULL";
        }
        insert_sql << ");";
        auto r = sql_conn->Query(insert_sql.str().c_str());
        if (r->GetRetCode() != 0) {
            std::stringstream sErrMsg;
            sErrMsg << "Error Code:" << r->GetRetCode() << " Msg:" << r->GetRetMsg() << std::endl;
            result = sErrMsg.str();
            sql_conn->Query("ROLLBACK");
            bResult = false;
            break;
        }
    }
    if (bResult == true) sql_conn->Query("COMMIT");
    return result;
}

const size_t IMPORT_ARGS_NUM = 4;

std::string ClassCmd::Import(const std::vector<std::string> &strList) {
    std::string prompt_msg =
        "Useage: .import $FILE $TABLE [--OPTION]"
        "   Options: \n"
        "     --csv                 Use , and \\n as column and row separators\n" 
        "     --skip N              Skip the first N rows of input\n";
    if (strList.size() < IMPORT_ARGS_NUM) {
        return prompt_msg;
    }

    EImportType format_type = EImportType::invalid;
    uint16 skip = 0;
    std::vector<std::string> strcmd;
    for (size_t i = 1; i < strList.size(); i++) {
        if (strList[i].substr(0, 2) == "--") {
            std::string option = strList[i].substr(2);
            if (option == "csv")
                format_type = EImportType::csv;
            else if (option == "skip" && i < strList.size() - 1)
                skip = atoi(strList[++i].c_str());
        } else {
            strcmd.push_back(strList[i]);
        }
    }

    switch (format_type) {
        case EImportType::csv: {
            return ImportCSV(strcmd[0], GetTableName(strcmd[1]), skip);
        }
        default: {
            return "FORMAT OPTION ERROR! \n" + prompt_msg;
        }
    }
}

const size_t READ_ARGS_NUM = 2;

std::string ClassCmd::Read(const std::vector<std::string> &strList) {
    std::string prompt_msg = "Useage: .read $FILE\n";
    if (strList.size() < READ_ARGS_NUM) {
        return prompt_msg;
    }
    std::ifstream fin;
    fin.open(strList[1].c_str(), std::ios::in);
    if (!fin.is_open()) {
        return "open file: " + strList[1] + " error\n";
    }
    std::string str_line;
    std::string cmd;
    while (getline(fin, str_line)) {
        cmd += str_line;
#ifdef WIN32
            cmd += "\r\n";
#else
            cmd += "\n";
#endif
    }
    execute(cmd);
    return "";
}

std::string ClassCmd::DBConfig(const std::vector<std::string> &strList) {
    std::string result;
    int length = 10;
    if (strList.size() == 1) {  // show all config
        fmt::format_to(std::back_inserter(result), "{:>{}}: {}\n", "log_level", length,
                       cm_log_param_instance()->log_level);
    }
    if (strList.size() >= 3) {
        if (strList[1] == "log_level") {
            cm_log_param_instance()->log_level = atoi(strList[2].c_str());
            fmt::format_to(std::back_inserter(result), "{:>{}}: {}\n", "log_level", length,
                           cm_log_param_instance()->log_level);
        }
    }
    return result;
}

std::shared_ptr<ClassCmd::PrintDelegate> ClassCmd::GetPrintDelegate(const std::string &filename) {
    if (filename == "stdout") {
        return std::make_shared<PrintDelegate>();
    } else {
        return std::make_shared<PrintDelegate>(filename.c_str());
    }
}

auto ClassCmd::PrintCmd(const std::string &cmd) -> void {
    if (b_echo) {
        print_delegate_->Print(cmd);
        print_delegate_->Print("\n");
    }
}

auto ClassCmd::UpdateTotalChanges(uint64_t current_changes) -> void { total_changes += current_changes; }

auto ClassCmd::PrintEffectRow(uint64_t current_changes) -> void {
    if (b_show_change) {
        print_delegate_->Print(fmt::format("changes: {}\ttotal_changes: {}\n", current_changes, total_changes));
    }
}

auto ClassCmd::IsBuildInCmd(const std::string &cmd, func &build_func) -> bool {
    auto iter = ex_func_map.find(cmd);
    if (iter != ex_func_map.end()) {
        build_func = iter->second;
    }
    return iter != ex_func_map.end();
}

auto ClassCmd::IsKVCmd(const std::string &cmd, KvOperator::kv_func &kv_func) -> bool {
    auto iter = kv_operator->kv_func_map.find(cmd);
    if (iter != kv_operator->kv_func_map.end()) {
        kv_func = iter->second;
    }
    return iter != kv_operator->kv_func_map.end();
}

auto ClassCmd::PrintBuildInCmdResult(const std::string &query, const func &build_func,
                                     const std::vector<std::string> &args) -> void {
    auto result = (this->*build_func)(args);
    // 短结果直接打印
    PrintCmd(query);
    PrintResult(result);
}

auto ClassCmd::PrintKVCmdResult(const std::string &query, const KvOperator::kv_func &kv_func,
                                const std::vector<std::string> &args) -> void {
    auto start = GetTimer();
    auto result = (kv_operator->*kv_func)(args);
    // 短结果直接打印
    PrintCmd(query);
    PrintResult(result);
    auto end = GetTimer();
    PrintTimer(start, end);
}

auto ClassCmd::PrintSQLCmdResult(const std::string &query) -> void {
    // split sqls if necessary
    auto sql_list = SplitSQLStrings(query);

    for (const auto& sql : sql_list) {
        // handle sql
        query_sql = sql;
        auto start = GetTimer();
        auto r = sql_conn->QueryIterator(sql.c_str());
        PrintCmd(sql);
        PrintRecords(*r);
        auto end = GetTimer();  // RecordBatch改为RecordIterator后，需要Print之后，才执行完成。
        PrintTimer(start, end);
    }
}

static auto PrintCSVRow(std::shared_ptr<ClassCmd::PrintDelegate> &print_delegate,
                        const std::vector<std::string> &row_content) {
    for (size_t i = 0; i < row_content.size(); i++) {
        if (i > 0) {
            print_delegate->Print(",");
        }
        print_delegate->Print(row_content[i]);
    }
    print_delegate->Print("\n");
}

auto ClassCmd::PrintCSVFormat(RecordIterator &record_iterator) -> void {
    auto headers = record_iterator.GetHeader();
    PrintCSVRow(print_delegate_, headers);

    while (true) {
        const auto &[record, eof] = record_iterator.Next();
        if (eof) {
            break;
        }
        auto row_content = RecordToString(record, null_str);
        PrintCSVRow(print_delegate_, row_content);
    }
}

auto ClassCmd::RecordToString(const Record &record, const std::string &null_format) -> std::vector<std::string> {
    std::vector<std::string> row_content;
    for (size_t i = 0; i < record.ColumnCount(); i++) {
        const auto &val = record.Field(i);
        row_content.emplace_back(val.IsNull() ? null_format : val.ToString());
    }
    return row_content;
}

// TODO: 和 PrintInsertValue 逻辑重复了，考虑抽象合并
static auto AddJsonValue(cJSON *jrow, const char *field_name, const Value &val) -> void {
    if (val.IsNull()) {
        cJSON_AddItemToObject(jrow, field_name, cJSON_CreateNull());
    } else if (val.IsNumeric()) {
        cJSON_AddItemToObject(jrow, field_name, cJSON_CreateNumber(val.GetCastAs<double>()));
    } else if (val.GetLogicalType().TypeId() == GS_TYPE_BOOLEAN) {
        cJSON_AddItemToObject(jrow, field_name, cJSON_CreateBool(val.GetCastAs<bool>()));
    } else {
        cJSON_AddItemToObject(jrow, field_name, cJSON_CreateString(val.ToString().c_str()));
    }
}

static auto RecordToJSONRow(const std::vector<std::string> &headers, const Record &record) -> std::string {
    cJSON *jrow = cJSON_CreateObject();
    for (size_t j = 0; j < headers.size(); ++j) {
        const auto &header = headers[j];
        const auto &v = record.Field(j);
        AddJsonValue(jrow, header.c_str(), v);
    }
    char *str = cJSON_PrintUnformatted(jrow);
    std::string json_row = str;
    // 注意释放内存，使用方法参考cJSON测试用例,
    // 需要保证前面不会抛出异常，否则会导致free语句执行不到
    // 最好使用RAII的方法保证内存能够被正确释放
    free(str);
    cJSON_Delete(jrow);
    return json_row;
}

auto ClassCmd::PrintJSONFormat(RecordIterator &record_iterator) -> void {
    auto headers = record_iterator.GetHeader();
    print_delegate_->Print("[\n");
    bool first = true;
    while (true) {
        const auto &[record, eof] = record_iterator.Next();
        if (!eof && !first) {
            print_delegate_->Print(",\n");
        }
        if (eof) {
            print_delegate_->Print("\n");
            break;
        }
        first = false;
        print_delegate_->Print(RecordToJSONRow(headers, record));
    }
    print_delegate_->Print("]\n");
}

auto ClassCmd::IsFinishQuery(const std::string &query) -> bool {
    if (query[0] == '.') {
        return true;
    }
    
    bool inQuote = false;
    bool inDoublequotes = false;
    if (query.length() > 0) {
        for (int i = 0; i < query.length(); i++) {
            char ch = query[i];
            if (ch == '\'') {
                inQuote = !inQuote;
            } else if (ch == '"') {
                inDoublequotes = !inDoublequotes;
            } else if (ch == ';' && !inDoublequotes && !inQuote && i == query.length() - 1) {
                return true;
            }
        }
    }

    return false;
}
