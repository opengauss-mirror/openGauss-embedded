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
 * csv_util.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/csv_util.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fstream>
#include <optional>
#include <string>

#include "common/record_batch.h"

namespace intarkdb {

constexpr char kDefaultCSVDelimiter = ',';

class CSVWriter {
   public:
    EXPORT_API CSVWriter(const std::string& filename, char delimiter = kDefaultCSVDelimiter)
        : filename_(filename), delimiter_(delimiter) {}

    EXPORT_API ~CSVWriter() { Close(); }

    EXPORT_API auto Open() -> bool;
    EXPORT_API auto IsOpen() -> bool;
    EXPORT_API auto Close() -> bool;
    EXPORT_API auto WriteRecord(const Record& record) -> void;

   private:
    std::string filename_;
    char delimiter_;
    std::ofstream file_;
};

class CSVReader {
   public:
    EXPORT_API CSVReader(const std::string& filename, const Schema& schema, char delimiter = kDefaultCSVDelimiter)
        : filename_(filename), schema_(schema), delimiter_(delimiter) {}

    EXPORT_API ~CSVReader() { Close(); }

    EXPORT_API auto Open() -> bool;
    EXPORT_API auto IsOpen() -> bool;
    EXPORT_API auto Close() -> bool;
    EXPORT_API auto ReadRecord() -> std::tuple<Record, bool>;

   private:
    std::string filename_;
    Schema schema_;
    char delimiter_;
    std::ifstream file_;
};

}  // namespace intarkdb
