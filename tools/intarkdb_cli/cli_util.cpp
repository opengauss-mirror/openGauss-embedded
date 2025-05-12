/*
 * 版权所有 (c) GBA-NCTI-ISDC 2022-2024
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
 * cli_util.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/tools/intarkdb_cli/cli_util.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <iostream>
#include <sstream>
#include <vector>
#include <string>

std::string Trim(const std::string& str)
{
    size_t first = str.find_first_not_of(" \t\n\r");
    size_t last = str.find_last_not_of(" \t\n\r");
    return (first == std::string::npos || last == std::string::npos) ? "" : str.substr(first, (last - first + 1));
}

std::vector<std::string> SplitSQLStrings(const std::string& sql) {
    std::vector<std::string> statements;
    std::string statement;
    std::string::size_type pos = 0;
    bool inQuote = false;
    bool inDoublequotes = false;

    for(int i = 0; i < sql.size(); i++ ){
        char ch = sql[i];
        if (ch == '\'') {
            inQuote = !inQuote;
            statement += ch;
        } else if (ch == '"') {
            inDoublequotes = !inDoublequotes;
            statement += ch;
        } else if (ch == ';' && !inDoublequotes && !inQuote) {
            statement += ch;
            statement =  Trim(statement);
            if (!statement.empty() ) {
                statements.push_back(statement);
            }
            statement.clear();
        } else {
            statement += ch;
        }

    }
    statement =  Trim(statement);
    if (!statement.empty() ) {
        statements.push_back(statement);
    }
    return statements;
}

