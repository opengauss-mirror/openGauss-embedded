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
 * table_datasource.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/datasource/table_datasource.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "datasource/table_datasource.h"

#include <iostream>
#include <unordered_map>

#include "catalog/catalog.h"
#include "common/default_value.h"
#include "common/null_check_ptr.h"
#include "common/row_container.h"
#include "common/string_util.h"
#include "storage/db_handle.h"
#include "storage/gstor/gstor_executor.h"
#include "storage/gstor/zekernel/common/cm_log.h"
#include "type/type_id.h"
#include "type/type_system.h"
#include "common/stat.h"

const Schema& TableDataSource::GetSchema() const { return schema_; }

auto TableDataSource::Rows() const -> int64_t {
    const auto& table_name = table_->GetBoundTableName();
    int64_t rows = 0;
    if (!NeedParitionScan()) {
        auto ret = gstor_open_user_table_with_user(((db_handle_t*)handle_)->handle, user_.c_str(), table_name.c_str());
        if (ret != GS_SUCCESS) {
            throw intarkdb::Exception(ExceptionType::EXECUTOR, "open table fail");
        }
        ret = gstor_fast_count_table_row(((db_handle_t*)handle_)->handle, table_name.c_str(), idx_, &rows);
        if (ret != GS_SUCCESS) {  // 可能失败的原因是，表的存储类型不是 pcrh heap
            throw intarkdb::Exception(ExceptionType::EXECUTOR, "fast scan table fail");
        }
    } else {  // 时序表
        const auto& meta = table_->GetTableInfo();
        for (size_t i = 0; i < meta.GetTablePartCount(); ++i) {
            auto part_table_info = meta.GetTablePartByIdx(i);
            if (part_table_info == NULL) {
                break;
            }
            gstor_modified_partno(((db_handle_t*)handle_)->handle, idx_, part_table_info->part_no);
            auto ret =
                gstor_open_user_table_with_user(((db_handle_t*)handle_)->handle, user_.c_str(), table_name.c_str());
            int64_t part_rows = 0;
            ret = gstor_fast_count_table_row(((db_handle_t*)handle_)->handle, table_name.c_str(), idx_, &part_rows);
            if (ret != GS_SUCCESS) {
                throw intarkdb::Exception(ExceptionType::EXECUTOR, "fast scan table fail");
            }
            rows += part_rows;
        }
    }
    return rows;
}

const int CACHE_SIZE_FACTOR = 2;

static auto InitCondition(const IndexMatchInfo& info, std::vector<Value>& cache_values,
                          std::vector<condition_def_t>& conditions) -> bool {
    conditions.clear();
    conditions.resize(info.index_columns.size());
    // 用于保存条件值
    cache_values.clear();
    cache_values.reserve(info.index_columns.size() * CACHE_SIZE_FACTOR);  // 大小很关键
    for (size_t i = 0; i < info.index_columns.size(); ++i) {
        auto& item = info.index_columns[i];
        if (item.upper == nullptr && item.lower == nullptr) {
            continue;
        }
        conditions[i].col_type = item.data_type;
        conditions[i].scan_edge = SCAN_EDGE_EQ;
        if (item.lower != nullptr) {
            auto val = item.lower->Evaluate(Record{});
            if (val.GetLogicalType().TypeId() != item.data_type) {
                if (!Value::TryCast(val, item.data_type)) {
                    return false;
                }
            }
            cache_values.push_back(val);
            conditions[i].left_buff = cache_values.back().GetRawBuff();
            conditions[i].left_size = cache_values.back().Size();
            conditions[i].scan_edge = SCAN_EDGE_GE;
        }
        if (item.upper != nullptr) {
            Value val = item.upper->Evaluate(Record{});
            if (val.GetLogicalType().TypeId() != item.data_type) {
                if (!Value::TryCast(val, item.data_type)) {
                    return false;
                }
            }
            cache_values.push_back(val);
            conditions[i].right_buff = cache_values.back().GetRawBuff();
            conditions[i].right_size = cache_values.back().Size();
            conditions[i].scan_edge = SCAN_EDGE_LE;
        }
        if (item.upper != nullptr && item.lower != nullptr) {
            conditions[i].scan_edge = SCAN_EDGE_EQ;
        }
    }
    return true;
}

auto TableDataSource::Init() -> void {
    // preapre index match info
    if (!index_bind_data_.use_index) {
        return;
    }
    // init index info
    if (!InitCondition(index_bind_data_, cache_values_, conditions_)) {
        idx_slot_ = -1;  // use index fail
    } else {
        idx_slot_ = index_bind_data_.index_slot;  // use index success
        index_column_count_ = index_bind_data_.total_index_column;
        condition_count_ = index_bind_data_.index_columns.size();
        GS_LOG_RUN_INF("use idx slot = %d\n", idx_slot_);
    }
}

static auto Columnlist2RowContainer(const res_row_def_t& res_row_list) -> intarkdb::RowContainerPtr {
    size_t total_size = 0;
    for (int i = 0; i < res_row_list.column_count; ++i) {
        if (!intarkdb::IsNull(res_row_list.row_column_list[i].crud_value)) {
            total_size += res_row_list.row_column_list[i].crud_value.len;
        }
    }
    intarkdb::RowBuffer buffer(total_size, res_row_list.column_count);
    for (int i = 0; i < res_row_list.column_count; i++) {
        const auto& item = res_row_list.row_column_list[i];
        LogicalType type = intarkdb::NewLogicalType(item);
        if (!buffer.AddItem(item.crud_value.str, item.crud_value.len, type, intarkdb::IsNull(item.crud_value))) {
            // 不应出现的错误
            throw intarkdb::Exception(ExceptionType::EXECUTOR, "AddItem fail");
        }
    }
    return std::make_unique<intarkdb::RowContainer>(std::move(buffer));
}

auto TableDataSource::CommonTableNext() -> std::tuple<intarkdb::RowContainerPtr, knl_cursor_t*, bool> {
    const auto& table_name = table_->GetBoundTableName();
    const auto& col_defs = Column::TransformColumnVecToDefs(table_->GetTableInfo().columns);
    uint32_t col_size = col_defs.size();

    bool32 eof = GS_FALSE;
    int res_row_count = 0;
    res_row_def_t res_row_list;
    res_row_list.column_count = col_defs.size();
    res_row_list.row_column_list = row_column_list_.get();

    if (first_) {
        auto ret = gstor_open_user_table_with_user(((db_handle_t*)handle_)->handle, user_.c_str(), table_name.c_str());
        if (ret != GS_SUCCESS) {
            throw std::runtime_error("open table fail");
        }
        if (idx_slot_ != GS_INVALID_ID32) {
            ret = gstor_open_cursor_ex(((db_handle_t*)handle_)->handle, table_name.c_str(), index_column_count_,
                                       condition_count_, conditions_.data(), &eof, idx_slot_, action_, idx_,
                                       lock_clause_);
        } else {
            ret = gstor_open_cursor_ex(((db_handle_t*)handle_)->handle, table_name.c_str(), 0, 0, nullptr, &eof, -1,
                                       action_, idx_, lock_clause_);
        }
        if (ret != GS_SUCCESS) {
            throw std::runtime_error("fail to open cursor");
        }
        first_ = false;
    }

    auto ret = gstor_cursor_next(((db_handle_t*)handle_)->handle, &eof, idx_);
    if (ret != GS_SUCCESS) {
        int32_t err_code;
        const char* message = nullptr;
        cm_get_error(&err_code, &message, NULL);
        GS_LOG_RUN_INF("cursor next fail!! errno = %d, message = %s\n", err_code, message);
        std::string msg = message;
        cm_reset_error();
        throw std::runtime_error(msg);
    }
    if (eof == GS_TRUE) {
        first_ = true;
    } else {
        scan_count_++;
        ret = gstor_cursor_fetch(((db_handle_t*)handle_)->handle, col_size,
                                 const_cast<exp_column_def_t*>(col_defs.data()), &res_row_count, &res_row_list, idx_);
        if (ret != GS_SUCCESS) {
            throw std::runtime_error("fail to get table data");
        }
        return std::make_tuple(Columnlist2RowContainer(res_row_list), nullptr, eof);
    }
    return {nullptr, nullptr, true};
}

auto TableDataSource::PartitionTableNext() -> std::tuple<intarkdb::RowContainerPtr, knl_cursor_t*, bool> {
    const auto& table_name = table_->GetBoundTableName();
    const auto& col_defs = Column::TransformColumnVecToDefs(table_->GetTableInfo().columns);
    uint32_t col_size = col_defs.size();

    bool32 eof = GS_FALSE;
    int res_row_count = 0;
    res_row_def_t res_row_list;
    res_row_list.column_count = col_defs.size();
    res_row_list.row_column_list = row_column_list_.get();

    const auto& meta = table_->GetTableInfo();
    if (first_) {
        if (scan_partition_no_ >= meta.GetTablePartCount()) {
            return {nullptr, nullptr, true};
        }
        auto part_table_info = meta.GetTablePartByIdx(scan_partition_no_);
        if (part_table_info == NULL) {
            return {nullptr, nullptr, true};
        }
        gstor_modified_partno(((db_handle_t*)handle_)->handle, idx_, part_table_info->part_no);
        auto ret = gstor_open_user_table_with_user(((db_handle_t*)handle_)->handle, user_.c_str(), table_name.c_str());
        if (ret != GS_SUCCESS) {
            throw std::runtime_error("open table fail");
        }
        if (idx_slot_ != GS_INVALID_ID32) {
            ret = gstor_open_cursor_ex(((db_handle_t*)handle_)->handle, table_name.c_str(), index_column_count_,
                                       condition_count_, conditions_.data(), &eof, idx_slot_, action_, idx_,
                                       lock_clause_);
        } else {
            ret = gstor_open_cursor_ex(((db_handle_t*)handle_)->handle, table_name.c_str(), 0, 0, nullptr, &eof, -1,
                                       action_, idx_, lock_clause_);
        }
        if (ret != GS_SUCCESS) {
            throw std::runtime_error("fail to open cursor");
        }
        first_ = false;
    }

    auto ret = gstor_cursor_next(((db_handle_t*)handle_)->handle, &eof, idx_);
    if (eof == GS_TRUE) {
        scan_partition_no_++;
        if (scan_partition_no_ < meta.GetTablePartCount()) {
            first_ = true;
            return Next();
        }
        gstor_modified_partno(((db_handle_t*)handle_)->handle, idx_, 0);
        return {nullptr, nullptr, true};
    }
    scan_count_++;

    ret = gstor_cursor_fetch(((db_handle_t*)handle_)->handle, col_size, const_cast<exp_column_def_t*>(col_defs.data()),
                             &res_row_count, &res_row_list, idx_);
    if (ret != GS_SUCCESS) {
        throw std::runtime_error("fail to get table data");
    }

    return std::make_tuple(Columnlist2RowContainer(res_row_list), gstor_get_cursor(handle_, idx_), eof);
}

auto TableDataSource::Next() -> std::tuple<intarkdb::RowContainerPtr, knl_cursor_t*, bool> {
    if (!NeedParitionScan()) {
        return CommonTableNext();
    } else {
        return PartitionTableNext();
    }
}

status_t TableDataSource::OpenStorageTable(std::string table_name) {
    if (gstor_open_user_table_with_user(((db_handle_t*)handle_)->handle, user_.c_str(), table_name.c_str()) !=
        GS_SUCCESS) {
        std::string msg = "open table fail, table:" + table_name;
        throw std::runtime_error(msg.c_str());
    }
    return GS_SUCCESS;
}

void TableDataSource::Insert(const std::vector<Column>& columns) {
    const auto& table_name = table_->GetBoundTableName();
    uint32_t column_count = columns.size();

    auto ret = gstor_open_user_table_with_user(((db_handle_t*)handle_)->handle, user_.c_str(), table_name.c_str());
    if (ret != GS_SUCCESS) {
        throw std::runtime_error("open table fail");
    }

    auto row_column_list = std::make_unique<exp_column_def_t[]>(column_count);
    for (uint32_t i = 0; i < column_count; i++) {
        row_column_list_[i] = columns[i].GetRaw();
    }

    //
    ret = gstor_executor_insert_row(((db_handle_t*)handle_)->handle, table_name.c_str(), column_count,
                                    row_column_list_.get());
    if (ret != GS_SUCCESS) {
        int32_t err_code;
        const char* message = nullptr;
        cm_get_error(&err_code, &message, nullptr);
        GS_LOG_RUN_INF("insert error, errno = %d, message = %s\n", err_code, message);
        std::string msg = message;
        cm_reset_error();
        throw std::runtime_error(msg);
    }
}

void TableDataSource::BatchInsert(std::vector<std::vector<Column>>& insert_rows,
                                const std::string &part_name, uint32_t part_no,
                                bool32 is_ignore) {
    TIMESTATISTICS;
    const auto& table_name = table_->GetBoundTableName();
    uint32_t row_count = insert_rows.size();
    uint32_t column_count = insert_rows[0].size();
    auto row_list = std::make_unique<res_row_def_t[]>(row_count);
    std::vector<std::vector<exp_column_def_t>> row_column_lists(row_count, std::vector<exp_column_def_t>(column_count));
    for (uint32_t row_i = 0; row_i < row_count; row_i++) {
        auto& row_column_list = row_column_lists[row_i];
        for (uint32_t i = 0; i < column_count; i++) {
            row_column_list[i] = insert_rows[row_i][i].GetRaw();
        }
        row_list[row_i].column_count = column_count;
        row_list[row_i].row_column_list = row_column_list.data();
    }

    auto ret = gstor_batch_insert_row(((db_handle_t*)handle_)->handle, table_name.c_str(), row_count, row_list.get(),
                                        part_no, is_ignore);
    if (ret != GS_SUCCESS) {
        int32_t err_code;
        const char* message = nullptr;
        cm_get_error(&err_code, &message, nullptr);
        if (err_code == ERR_DC_INVALIDATED || err_code == ERR_PARTITION_NOT_READY) {
            GS_LOG_RUN_WAR("insert error, errno = %d, message = %s, try again...\n", err_code, message);
            cm_reset_error();
            // lock for dc
            std::lock_guard<std::mutex> lock(Catalog::dc_mutex_);
            auto ret =
                gstor_open_user_table_with_user(((db_handle_t*)handle_)->handle, user_.c_str(), table_name.c_str());
            if (ret != GS_SUCCESS) {
                throw std::runtime_error("open table fail");
            }
            if (!ReflashPartition(table_name, part_name, part_no)) {
                throw std::runtime_error("refresh part_no fail");
            }

            ret = gstor_batch_insert_row(((db_handle_t*)handle_)->handle, table_name.c_str(), row_count, row_list.get(),
                                         part_no, is_ignore);
            if (ret != GS_SUCCESS) {
                cm_get_error(&err_code, &message, nullptr);
                std::string msg = message;
                GS_LOG_RUN_WAR("try to insert again failed, errno = %d, message = %s\n", err_code, message);
                cm_reset_error();
                throw std::runtime_error(msg);
            }
            GS_LOG_RUN_WAR("try to insert again success!\n");
        } else {
            GS_LOG_RUN_INF("insert error, errno = %d, message = %s\n", err_code, message);
            std::string msg = message;
            cm_reset_error();
            throw std::runtime_error(msg);
        }
    }
}

void TableDataSource::Delete() {
    auto ret = gstor_executor_delete(((db_handle_t*)handle_)->handle, idx_);
    if (ret != GS_SUCCESS) {
        int32_t err_code;
        const char* message = nullptr;
        cm_get_error(&err_code, &message, nullptr);
        GS_LOG_RUN_INF("delete error, errno = %d, message = %s\n", err_code, message);
        std::string msg = message;
        cm_reset_error();
        throw std::runtime_error(msg);
    }
}

void TableDataSource::Update(int column_count, exp_column_def_t* column_list) {
    auto ret = gstor_executor_update(((db_handle_t*)handle_)->handle, column_count, column_list, idx_);
    if (ret != GS_SUCCESS) {
        int32_t err_code;
        const char* message = nullptr;
        cm_get_error(&err_code, &message, nullptr);
        GS_LOG_RUN_INF("update error, errno = %d, message = %s\n", err_code, message);
        std::string msg = message;
        cm_reset_error();
        throw std::runtime_error(msg);
    }
}

auto TableDataSource::IsParitionTable() const -> bool { return table_->GetTableInfo().GetTableMetaInfo().parted; }

auto TableDataSource::IsParitionIndex(uint32_t idx_slot) const -> bool {
    return idx_slot != GS_INVALID_ID32 && table_->GetTableInfo().GetTableMetaInfo().index_count > 0 &&
           table_->GetTableInfo().GetTableMetaInfo().indexes != NULL &&
           table_->GetTableInfo().GetTableMetaInfo().indexes[idx_slot].parted;
}

auto TableDataSource::NeedParitionScan() const -> bool {
    return (IsParitionTable() && idx_slot_ != GS_INVALID_ID32 && IsParitionIndex(idx_slot_)) ||
           (IsParitionTable() && idx_slot_ == GS_INVALID_ID32);
}

int32_t TableDataSource::AutoAddPartition(std::string table_name, std::string part_key, part_type_t part_type) {
    std::string part_name = table_name + "_" + part_key;
    exp_altable_def_t altable_def;
    error_info_t err_info;
    altable_def.action = ALTABLE_ADD_PARTITION;
    altable_def.part_opt.part_name.str = (char*)part_name.c_str();
    altable_def.part_opt.part_name.len = part_name.length();
    altable_def.part_opt.part_type = part_type;

    auto hpartbound_time = intarkdb::StringToTime((char*)part_key.c_str());
    if (part_key.length() == PART_NAME_SUFFIX_HOUR_TIME_LEN) {
        hpartbound_time += SECONDS_PER_HOUR;
    } else {
        hpartbound_time += SECONDS_PER_DAY;
    }
    auto hpartbound_str = std::to_string(hpartbound_time * MICROSECS_PER_SECOND_LL);
    altable_def.part_opt.hiboundval.str = (char*)hpartbound_str.c_str();
    altable_def.part_opt.hiboundval.len = hpartbound_str.length();

    // lock for DC
    std::lock_guard<std::mutex> lock(Catalog::dc_mutex_);
    auto ret = sqlapi_gstor_alter_table(((db_handle_t*)handle_)->handle, user_.c_str(), table_name.c_str(),
                                        &altable_def, &err_info);
    if (ret != GS_SUCCESS) {
        // errmsg
        if (err_info.code == ERR_DUPLICATE_PART_NAME) {
            GS_LOG_RUN_WAR("Partition(%s) has been added by others", part_name.c_str());
        } else {
            throw std::runtime_error(err_info.message);
        }
    }

    // get part_no
    uint32_t wait_count = 0;
    uint32_t part_no = GS_INVALID_ID32;
    while (!ReflashPartition(table_name, part_name, part_no)) {
        cm_sleep(1);
        wait_count++;
        if (wait_count > GS_WAIT_REFLASH_DC) {
            break;
        }
    }

    if (part_no == GS_INVALID_ID32) {
        std::string err_msg = "Can't get the part_no for partition :" + part_name;
        GS_LOG_RUN_WAR("%s", err_msg.c_str());
        throw std::runtime_error(err_msg);
    }

    return part_no;
}

bool TableDataSource::ReflashPartition(std::string table_name, std::string part_name, uint32_t& part_no) {
    bool is_found_partition = false;

    // !reflash table_info
    exp_table_meta meta_info;
    meta_info.columns = nullptr;
    meta_info.indexes = nullptr;
    meta_info.part_table.entitys = nullptr;
    meta_info.part_table.keycols = nullptr;
    meta_info.part_table.pbuckets = nullptr;
    error_info_t err_info;
    auto ret = gstor_get_table_info(((db_handle_t*)handle_)->handle, user_.c_str(), table_name.c_str(), &meta_info, &err_info);
    if (ret != GS_SUCCESS) {
        throw std::runtime_error(std::to_string(err_info.code) + "," + err_info.message);
    }

    bool32 is_parted = meta_info.parted;
    if (!is_parted) {
        free_table_info(&meta_info);
        return is_found_partition;
    }

    uint32_t part_cnt = meta_info.part_table.desc.partcnt;
    for (uint32 part_i = 0; part_i < part_cnt; part_i++) {
        std::string full_part_name = std::string(meta_info.part_table.entitys[part_i].desc.name);
        if (full_part_name.find(part_name) != std::string::npos) {
            is_found_partition = true;
            part_no = meta_info.part_table.entitys[part_i].part_no;
            break;
        }
    }
    free_table_info(&meta_info);

    return is_found_partition;
}

status_t TableDataSource::AutoIncrementNextValue(uint32_t slot, int64_t* nextval) {
    return gstor_autoincrement_nextval(((db_handle_t*)handle_)->handle, slot, nextval);
}

status_t TableDataSource::AlterIncrementValue(uint32_t slot, int64_t nextval) {
    return gstor_autoincrement_updateval(((db_handle_t*)handle_)->handle, slot, nextval);
}

int64_t TableDataSource::SeqNextValue(std::string seq_name) {
    int64_t value;
    auto ret = gstor_seq_nextval(((db_handle_t*)handle_)->handle, user_.c_str(), seq_name.c_str(), &value);
    if (ret != GS_SUCCESS) {
        std::string errmsg = "SeqNextValue seq_nextval fail! sequence name :" + seq_name;
        GS_LOG_RUN_WAR("%s", errmsg.c_str());
        throw std::runtime_error(errmsg.c_str());
    }
    return value;
}

int64_t TableDataSource::SeqCurrValue(std::string seq_name) {
    int64_t value;
    auto ret = gstor_seq_currval(((db_handle_t*)handle_)->handle, user_.c_str(), seq_name.c_str(), &value);
    if (ret != GS_SUCCESS) {
        return SeqNextValue(seq_name);
    }
    return value;
}
