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
 * record_iterator.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/record_iterator.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "binder/statement_type.h"
#include "catalog/schema.h"
#include "common/record.h"
#include "common/winapi.h"

enum class RecordIteratorType : uint8_t {
    Batch,
    Streaming,
};

enum class RecordBatchType : uint8_t {
    Invalid,
    Select,
    Delete,
    Update,
    Insert,
};

class RecordIterator {
   public:
    EXPORT_API explicit RecordIterator(RecordIteratorType iterator_type);

    EXPORT_API virtual ~RecordIterator();

    EXPORT_API virtual std::tuple<Record, bool> Next() = 0;

    EXPORT_API virtual int32_t GetRetCode() { return ret_code_; }
    EXPORT_API virtual void SetRetCode(int32_t ret_code) { ret_code_ = ret_code; }

    EXPORT_API virtual const std::string& GetRetMsg() { return ret_msg_; }
    EXPORT_API virtual void SetRetMsg(const std::string& msg) { ret_msg_ = msg; }

    EXPORT_API virtual void SetRetLocation(int32_t ret_location) { ret_location_ = ret_location; }
    EXPORT_API virtual int32_t GetRetLocation() { return ret_location_; }

    EXPORT_API virtual void SetRecordBatchType(RecordBatchType type) { rb_type_ = type; }
    EXPORT_API virtual RecordBatchType GetRecordBatchType() const { return rb_type_; }

    EXPORT_API virtual void SetPragmaAttr(bool is_pragma) { is_pragma_ = is_pragma; }
    EXPORT_API virtual bool GetPragmaAttr() const { return is_pragma_; }

    EXPORT_API virtual uint64_t GetEffectRow() const { return effect_row_; }
    EXPORT_API virtual void SetEffectRow(uint64_t effect_row) { effect_row_ = effect_row; }

    EXPORT_API virtual StatementType GetStmtType() const { return stmt_type; }

    EXPORT_API virtual RecordIteratorType GetIteratorType() const { return type_; }

    EXPORT_API virtual const Schema& GetSchema() const = 0;

    EXPORT_API virtual uint32_t ColumnCount() const = 0;

    EXPORT_API virtual auto GetHeader() -> std::vector<std::string> const;

   public:
    // 子类共享的属性
    StatementType stmt_type;
    StatementProperty stmt_props;

   protected:
    RecordIteratorType type_;
    RecordBatchType rb_type_ = RecordBatchType::Invalid;
    int32_t ret_code_ = 0;
    std::string ret_msg_{"Sql success!"};
    int32_t ret_location_ = -1;
    bool is_pragma_{false};
    int64_t effect_row_{0};
};
