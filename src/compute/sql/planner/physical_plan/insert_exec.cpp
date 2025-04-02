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
 * insert_exec.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/physical_plan/insert_exec.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "planner/physical_plan/insert_exec.h"

#include <unordered_set>

#include "common/default_value.h"
#include "common/null_check_ptr.h"
#include "function/sql_function.h"
#include "storage/db_handle.h"
#include "storage/gstor/zekernel/common/cm_date.h"
#include "storage/gstor/zekernel/common/cm_dec4.h"
#include "storage/gstor/zekernel/common/cm_decimal.h"
#include "common/stat.h"

#define PAGE_SIZE 8192
#define MAX_ROW_SIZE (PAGE_SIZE - 256)
#define MIN_ROW_SIZE 16
#define MAX_BATCH_ROW_COUNT 255
#define MAX_LOOP_BATCH_SIZE (MAX_BATCH_ROW_COUNT * 10)

const std::string PART_INTERVAL_DAY = "1d";
const std::string PART_INTERVAL_HOUR = "1h";

Schema InsertExec::GetSchema() const { return source_->GetSchema(); }

// not use
auto InsertExec::Execute() const -> RecordBatch { return RecordBatch(Schema()); }

// batch insert
void InsertExec::Execute(RecordBatch &rb_out) {
    TIMESTATISTICS;
    std::vector<Value> autoincrement_list;
    std::vector<Value> default_value_list;
    if (unbound_defaults_.size() > 0) {
        default_value_list.reserve(MAX_LOOP_BATCH_SIZE * unbound_defaults_.size());
    }
    std::map<std::string, std::unique_ptr<std::vector<std::vector<Column>>>> insert_rows_map;

    // table define
    auto &table_info = source_->GetTableRef().GetTableInfo();
    auto &meta_info = table_info.GetTableMetaInfo();
    // 初始化表定义
    GetTableDef(table_info, meta_info);

    // autoincrement, only one column can be set
    if (m_auto_increment) {
        autoincrement_list.reserve(MAX_LOOP_BATCH_SIZE);
    }

    MakeSchema(rb_out);

    uint64_t row_count = 0;
    uint64_t loop_batch_size = 0;
    bool is_first = true;
    std::vector<std::vector<Value>> final_values;
    std::vector<Value> tmp_values;
    while (true) {
        auto&& [r, _, eof] = child_->Next();
        if (eof) {
            break;
        }
        tmp_values.clear();
        for(uint16_t i = 0; i < r.ColumnCount(); i++) {
            tmp_values.push_back(r.Field(i));
        }
        final_values.push_back(std::move(tmp_values));
        ++loop_batch_size;
        ++row_count;
        // record 的列长度 与 绑定列长度不一致
        if (r.ColumnCount() != bound_columns_.size()) {
            throw std::runtime_error("Number of columns does not match");
        }

        // open table one time
        if (is_first) {
            source_->OpenStorageTable(m_table_name);
            is_first = false;
        }

        // each row
        std::vector<Column> column_list;
        if (m_auto_increment) {
            column_list.reserve(bound_columns_.size() + unbound_defaults_.size() + 1);
        } else {
            column_list.reserve(bound_columns_.size() + unbound_defaults_.size());
        }
        // auto_increment cloumn value
        std::vector<Value> values;
        if (m_auto_increment && !m_autoincrement_col_is_bound) {
            Value null_val;
            GetAutoIncrement(column_list, autoincrement_list, values, null_val);
        }

        // bound_columns value
        GetBoundValue(final_values.back(), column_list, autoincrement_list, values);

        // unbound_defaults value
        if (unbound_defaults_.size() > 0) {
            GetDefaultValue(column_list, default_value_list, values);
        }

        // if need return insert resultset
        if (NeedResultSetEx()) {
            rb_out.AddRecord(Record(values));
        }

        // partition key
        if (m_is_parted) {
            if (!m_is_crosspart || (m_is_crosspart && m_part_key_key_ == "-1")) {
                GetPartitionKey();
            }
        }

        // !grouping rows by part key and insert, if not part table, key is -1
        GroupRowsByKey(std::move(column_list), insert_rows_map);

        if (loop_batch_size >= MAX_LOOP_BATCH_SIZE || is_insert_or_ignore) {
            source_->OpenStorageTable(m_table_name);
            Insert(insert_rows_map);
            loop_batch_size = 0;
            insert_rows_map.clear();
            autoincrement_list.clear();
            default_value_list.clear();
            if (is_auto_commit_) {
                gstor_commit(((db_handle_t*)source_->GetStorageHandle())->handle);
            }
        }
    }
    // insert the rest rows
    if (insert_rows_map.size() > 0) {
        source_->OpenStorageTable(m_table_name);
        Insert(insert_rows_map);
    }

    rb_out.SetEffectRow(row_count);
    rb_out.SetLastInsertRowid(last_insert_rowid);
}

void InsertExec::GroupRowsByKey(
    std::vector<Column> &&column_list,
    std::map<std::string, std::unique_ptr<std::vector<std::vector<Column>>>> &insert_rows_map) {
    std::string new_part_name = m_table_name + "_" + m_part_key_key_;
    auto it = insert_rows_map.find(new_part_name);
    if (it != insert_rows_map.end()) {
        it->second->emplace_back(std::move(column_list));
    } else {
        auto insert_rows = std::make_unique<std::vector<std::vector<Column>>>();
        insert_rows->emplace_back(std::move(column_list));
        insert_rows_map.insert(std::make_pair(new_part_name, std::move(insert_rows)));

        // !find part & auto create
        if (m_is_parted) {
            if (m_part_name_map_.find(new_part_name) == m_part_name_map_.end()) {
                // !auto create part
                if (m_auto_addpart) {
                    uint32_t part_no = source_->AutoAddPartition(m_table_name, m_part_key_key_, m_part_type);
                    m_part_name_map_.insert(std::make_pair(new_part_name, part_no));
                } else {
                    std::string ex = "Can't find table partition to insert! part_name = " + new_part_name;
                    throw std::runtime_error(ex.c_str());
                }
            }
        }
    }
}

void InsertExec::Insert(
    const std::map<std::string, std::unique_ptr<std::vector<std::vector<Column>>>> &insert_rows_map) {
    std::vector<std::vector<Column>> insert_rows;
    std::map<std::string, std::unique_ptr<std::vector<std::vector<Column>>>>::const_iterator it;
    TIMESTATISTICS;
    bool has_lob_column = false;
    for (it = insert_rows_map.begin(); it != insert_rows_map.end(); ++it) {
        insert_rows.clear();
        int32_t batch_insert_size = 0;
        for (auto &row : *(it->second)) {
            int32_t curr_row_size = 8;  // FIXME:头部大小,不要使用magic number
            for (auto &col : row) {
                if (col.GetRaw().col_type == GS_TYPE_CLOB || col.GetRaw().col_type == GS_TYPE_BLOB) {
                    // more than sizeof(lob_locator_t)
                    curr_row_size += 64;
                    has_lob_column = true;
                } else if (intarkdb::IsString(col.GetRaw().col_type)) {
                    curr_row_size += CM_ALIGN4(col.GetRaw().crud_value.len + sizeof(uint16));
                } else if (intarkdb::IsDecimal(col.GetRaw().col_type)) {
                    uint32_t dec4_size = 4;
                    if (col.GetRaw().crud_value.str) {
                        dec4_size = cm_dec4_stor_sz((dec4_t *)col.GetRaw().crud_value.str);
                    }
                    if (dec4_size <= 8) {
                        if (dec4_size <= 4) {
                            curr_row_size += sizeof(int32);
                        } else {
                            curr_row_size += sizeof(int64);
                        }
                    } else {
                        curr_row_size += CM_ALIGN4(dec4_size + sizeof(uint16));
                    }
                } else {
                    curr_row_size += CM_ALIGN4(col.GetSize());
                }
            }
            if (curr_row_size < MIN_ROW_SIZE) {
                curr_row_size = MIN_ROW_SIZE;
            }
            batch_insert_size += curr_row_size;
            if (has_lob_column) {
                if (batch_insert_size >= MAX_ROW_SIZE) {
                    throw std::runtime_error("out of one row size!");
                }
                insert_rows.emplace_back(std::move(row));
                source_->BatchInsert(insert_rows, it->first, m_part_name_map_[it->first], is_insert_or_ignore);
                insert_rows.clear();
                batch_insert_size = 0;
            } else if (batch_insert_size >= MAX_ROW_SIZE || insert_rows.size() >= MAX_BATCH_ROW_COUNT) {
                if (insert_rows.size() == 0) {
                    throw std::runtime_error("out of one row size!");
                }
                source_->BatchInsert(insert_rows, it->first, m_part_name_map_[it->first], is_insert_or_ignore);
                insert_rows.clear();
                insert_rows.emplace_back(std::move(row));
                batch_insert_size = curr_row_size;
            } else {
                insert_rows.emplace_back(std::move(row));
            }
        }
        if (insert_rows.size() > 0) {
            source_->BatchInsert(insert_rows, it->first, m_part_name_map_[it->first], is_insert_or_ignore);
        }
    }
}

void InsertExec::GetTableDef(const TableInfo &table_info, const exp_table_meta &meta_info) {
    m_table_name = source_->GetTableName();

    // for part table
    m_is_parted = meta_info.parted;
    if (m_is_parted) {
        m_auto_addpart = meta_info.part_table.desc.auto_addpart;
        m_is_crosspart = meta_info.part_table.desc.is_crosspart;
        if (meta_info.part_table.keycols) {
            m_part_key_col_slot = meta_info.part_table.keycols->column_id;
        }
        m_part_interval = std::string(meta_info.part_table.desc.interval.str);
        m_part_type = meta_info.part_table.desc.parttype;

        for (uint32_t i = 0; i < meta_info.part_table.desc.partcnt; i++) {
            m_part_name_map_.insert(std::make_pair(std::string(meta_info.part_table.entitys[i].desc.name),
                                                   meta_info.part_table.entitys[i].part_no));
        }
    }

    // auto_increment
    m_auto_increment = meta_info.has_autoincrement;
    if (m_auto_increment) {
        bool found_autoincrement_col = false;
        for (uint32_t i = 0; i < meta_info.column_count; i++) {
            if (meta_info.columns[i].is_autoincrement) {
                m_autoincrement_col_ = meta_info.columns[i];
                found_autoincrement_col = true;
                break;
            }
        }
        if (!found_autoincrement_col) {
            throw std::runtime_error("not found autoincrement column def!");
        }

        for (auto &s : bound_columns_) {
            if (s.IsAutoIncrement()) {
                m_autoincrement_col_is_bound = true;
                break;
            }
        }
    }

    // opt_or_action
    if (opt_or_action_ == ONCONFLICT_ALIAS_IGNORE) {
        is_insert_or_ignore = GS_TRUE;
    }
}

void InsertExec::MakeSchema(RecordBatch &rb_out) {
    std::vector<SchemaColumnInfo> schema_cols;
    if (m_auto_increment) {
        if (!m_autoincrement_col_is_bound) {
            schema_cols.push_back(ColumnToSchemaColumnInfo("", Column(m_autoincrement_col_)));
        }
    }
    for (auto &s : bound_columns_) {
        schema_cols.push_back({{s.NameWithoutPrefix()}, "", s.GetLogicalType(), s.Slot()});
    }
    for (auto &s : unbound_defaults_) {
        schema_cols.push_back({{s.NameWithoutPrefix()}, "", s.GetLogicalType(), s.Slot()});
    }
    rb_out = RecordBatch(Schema(std::move(schema_cols)));
}

void InsertExec::GetPartitionKey() {
    if (m_is_parted) {
        if (m_is_crosspart && m_part_key_key_ != "-1") {
            // !if is_crosspart, partition base on the first row
            return;
        }
        if (m_part_key_value_.GetType() == GS_TYPE_TIMESTAMP || m_part_key_value_.GetType() == GS_TYPE_DATE) {
            // YYYY-MM-DD HH
            std::string str = m_part_key_value_.ToString();
            m_part_key_key_ = str.substr(0, 4) + str.substr(5, 2) + str.substr(8, 2);
            if (m_part_interval == PART_INTERVAL_HOUR) {
                m_part_key_key_ += str.substr(11, 2);
            }
        } else {
            m_part_key_key_ = m_part_key_value_.ToString();
        }
    }
}

void InsertExec::GetBoundValue(std::vector<Value>& row_in, std::vector<Column> &column_list, std::vector<Value> &autoincrement_list,
                               std::vector<Value>& values) {
    for (uint32_t j = 0; j < row_in.size(); j++) {
        auto& val = row_in[j];
        exp_column_def_t col_def = bound_columns_[j].GetRaw();
        // if autoincrement col is set
        if (m_auto_increment && col_def.col_slot == m_autoincrement_col_.col_slot) {
            GetAutoIncrement(column_list, autoincrement_list, values, val);
        }
        if (!val.IsNull() && col_def.col_type != val.GetType()) {
            try {
                val = DataType::GetTypeInstance(col_def.col_type)->CastValue(val);
            } catch (intarkdb::Exception& e) {
                if (e.type == ExceptionType::OUT_OF_RANGE) {
                    throw intarkdb::Exception(ExceptionType::OUT_OF_RANGE,
                        fmt::format("column ({}), value ({}) out of range!", col_def.name.str, val.ToString()));
                }
            }
        }
        // 检查转换后的值是否符合列定义
        CheckValueWithDef(val, col_def);

        if (m_is_parted && col_def.col_slot == m_part_key_col_slot) {
            m_part_key_value_ = val;
        }
        col_def.crud_value.str = const_cast<char *>(val.GetRawBuff());
        col_def.crud_value.len = val.Size();
        column_list.push_back(std::move(Column(col_def)));
        if (NeedResultSetEx()) {
            values.push_back(val);
        }
    }
}

void InsertExec::GetDefaultValue(std::vector<Column> &column_list, std::vector<Value> &default_value_list,
                                 std::vector<Value>& values) {
    for (size_t j = 0; j < unbound_defaults_.size(); j++) {
        auto &col = unbound_defaults_[j];
        if (!col.HasDefault()) {  // 没有默认值
            throw intarkdb::Exception(ExceptionType::EXECUTOR,
                                      fmt::format("unbound column get default value error! column:{}", col.Name()));
        }
        const exp_column_def_t &col_def = col.GetRaw();
        DefaultValue *default_value = NullCheckPtrCast<DefaultValue>(col_def.default_val.str).get();
        auto val = ToValue(*default_value, intarkdb::NewLogicalType(col_def), *source_);
        default_value_list.push_back(val);
        if (m_is_parted && col_def.col_slot == m_part_key_col_slot) {
            m_part_key_value_ = val;
        }
        col.SetCrud(const_cast<char *>(default_value_list[j].GetRawBuff()), default_value_list[j].Size(),
                    ASSIGN_TYPE_EQUAL);
        column_list.emplace_back(col.GetRaw());
        if (NeedResultSetEx()) {
            values.push_back(default_value_list.back());
        }
    }
}

void InsertExec::GetAutoIncrement(std::vector<Column> &column_list, std::vector<Value> &autoincrement_list,
                                  std::vector<Value>& values, Value &bound_value) {
    if (!m_auto_increment) {
        return;
    }

    int64_t auto_val_64 = 0;
    if (!m_autoincrement_col_is_bound) {
        if (source_->AutoIncrementNextValue(m_autoincrement_col_.col_slot, &auto_val_64) != GS_SUCCESS) {
            // suppose the error is caused by bigint over flow
            throw intarkdb::Exception(ExceptionType::OUT_OF_RANGE, "autoincrement nextval out of range!");
        }
        Value val_auto = Value(GS_TYPE_BIGINT, auto_val_64);
        val_auto = DataType::GetTypeInstance(m_autoincrement_col_.col_type)->CastValue(val_auto);
        autoincrement_list.push_back(val_auto);
        auto &val = autoincrement_list.back();
        if (m_is_parted && m_autoincrement_col_.col_slot == m_part_key_col_slot) {
            m_part_key_value_ = val;
        }
        m_autoincrement_col_.crud_value.str = const_cast<char *>(val.GetRawBuff());
        m_autoincrement_col_.crud_value.len = val.Size();
        column_list.push_back(Column(m_autoincrement_col_));
        if (NeedResultSetEx()) {
            values.push_back(val);
        }
    } else {
        if (bound_value.IsNull()) {
            if (source_->AutoIncrementNextValue(m_autoincrement_col_.col_slot, &auto_val_64) != GS_SUCCESS) {
                throw std::runtime_error("can't get autoincrement nextval!");
            }
            bound_value = Value(GS_TYPE_BIGINT, auto_val_64);
        } else {
            Value tmp_val = DataType::GetTypeInstance(GS_TYPE_BIGINT)->CastValue(bound_value);
            int64_t tmp_val_64 = *((int64_t *)tmp_val.GetRawBuff());
            if (auto_val_64 <= tmp_val_64) {
                source_->AlterIncrementValue(m_autoincrement_col_.col_slot, tmp_val_64);
                auto_val_64 = tmp_val_64;
            }
        }
    }
    last_insert_rowid = auto_val_64;
}

auto InsertExec::Children() const -> std::vector<PhysicalPlanPtr> { return {child_}; }

auto InsertExec::ToString() const -> std::string {
    // TODO
    return "InsertExec";
}
