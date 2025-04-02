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
 * exception.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/exception.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#include <stdexcept>
#include <string>

#include "gstor_exception.h"
#include "winapi.h"

namespace intarkdb {

class EXPORT_API Exception : public std::exception {
   public:
#ifdef _MSC_VER
    explicit Exception(const std::string &msg);
    Exception(ExceptionType exception_type, const std::string &message, int location = -1);
    ~Exception() {}
#else
    EXPORT_API explicit Exception(const std::string &msg);
    EXPORT_API Exception(ExceptionType exception_type, const std::string &message, int location = -1);
    EXPORT_API ~Exception() {}
#endif
    ExceptionType type;

   public:
#ifdef _MSC_VER
    const char *what() const noexcept override;
    const std::string &RawMessage() const;

    static std::string ExceptionTypeToString(ExceptionType type);
    [[noreturn]] static void ThrowAsTypeWithMessage(ExceptionType type, const std::string &message);
#else
    EXPORT_API const char *what() const noexcept override;
    EXPORT_API const std::string &RawMessage() const;

    EXPORT_API static std::string ExceptionTypeToString(ExceptionType type);
    [[noreturn]] EXPORT_API static void ThrowAsTypeWithMessage(ExceptionType type, const std::string &message);
#endif
    int GetLocation() const { return location_; }

   private:
    std::string exception_message_;
    std::string raw_message_;
    int location_;
};
}  // namespace intarkdb
