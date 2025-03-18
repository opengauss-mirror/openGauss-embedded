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
 * csv_util.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/common/csv_util.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "common/csv_util.h"

#include "common/string_util.h"
#include "type/type_system.h"

namespace intarkdb {

auto CSVWriter::Open() -> bool {
    file_.open(filename_, std::ios::app);
    return file_.is_open();
}

auto CSVWriter::IsOpen() -> bool { return file_.is_open(); }

auto CSVWriter::Close() -> bool {
    if (file_.is_open()) {
        file_.close();
        return true;
    }
    return false;
}

auto CSVWriter::WriteRecord(const Record& record) -> void {
    if (!file_.is_open()) {
        throw std::runtime_error("file is not open");
    }
    auto count = record.ColumnCount();
    for (size_t i = 0; i < count; i++) {
        file_ << record.Field(i).ToString();  // null will return "null"
        if (i + 1 != count) {
            file_ << delimiter_;
        }
    }
    file_ << std::endl;
}

auto CSVReader::Open() -> bool {
    file_.open(filename_);
    return file_.is_open();
}

auto CSVReader::IsOpen() -> bool { return file_.is_open(); }

auto CSVReader::Close() -> bool {
    if (file_.is_open()) {
        file_.close();
        return true;
    }
    return false;
}

auto CSVReader::ReadRecord() -> std::tuple<Record, bool> {
    if (!file_.is_open()) {
        throw std::runtime_error("file is not open");
    }

    std::string line;
    if (!std::getline(file_, line)) {
        return std::make_tuple(Record(), true);
    }
    auto fields = StringUtil::SplitAllString(line, delimiter_);
    const auto& columns = schema_.GetColumnInfos();
    if (fields.size() != columns.size()) {
        throw intarkdb::Exception(
            ExceptionType::EXECUTOR,
            fmt::format("not match columns size: field size={} columns size={}", fields.size(), columns.size()));
    }

    std::vector<Value> field_vec;
    field_vec.reserve(columns.size());
    for (size_t i = 0; i < fields.size(); ++i) {
        const auto& field = fields[i];
        if (field == "null") {  // null specifier
            field_vec.push_back(ValueFactory::ValueNull());
        } else {
            auto str_value = ValueFactory::ValueVarchar(field);
            field_vec.push_back(DataType::GetTypeInstance(columns[i].col_type.TypeId())->CastValue(str_value));
        }
    }
    return {Record(std::move(field_vec)), false};
}

}  // namespace intarkdb
