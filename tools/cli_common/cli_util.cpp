#include <iostream>
#include <sstream>
#include <vector>
#include <string>

std::string trim(const std::string& str) {
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
            statement =  trim(statement);
            if (!statement.empty() ) {
                statements.push_back(statement);
            }
            statement.clear();
        } else {
            statement += ch;
        }

    }
    statement =  trim(statement);
    if (!statement.empty() ) {
        statements.push_back(statement);
    }
    return statements;
}

