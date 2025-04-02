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
 * table_info.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/catalog/table_info.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "catalog/table_info.h"

#include <algorithm>
#include <string_view>
#include <vector>

#include "catalog/column.h"
#include "common/string_util.h"

bool IsValidTableName(std::string_view name) {
    bool have_char = false;
    for (auto c : name) {
        if (std::isalpha(c)) {
            have_char = true;
            continue;
        }
        if (std::isdigit(c) || '_' == c) {
            continue;
        }
        return false;
    }
    return have_char;
}

void OrderdPartitionList::InsertBatch(size_t cnt, int64_t interval, const exp_table_part_t* partitions) {
    for (size_t i = 0; i < cnt; ++i) {
        ParitionTableItem item;
        item.info = partitions + i;
        item.hibound = ::atoll(partitions[i].desc.hiboundval.str);
        item.lobound = item.hibound - interval;
        partitions_.emplace_back(item);
        name2partition_.insert(std::make_pair(std::string(partitions[i].desc.name), partitions + i));
    }
    std::sort(partitions_.begin(), partitions_.end(),
              [](const ParitionTableItem& a, const ParitionTableItem& b) { return a.hibound < b.hibound; });
}

const exp_table_part_t* OrderdPartitionList::Query(uint64_t ts_us) const {
    // 二分查找partitions
    auto it = std::lower_bound(partitions_.begin(), partitions_.end(), ts_us,
                               [](const ParitionTableItem& item, uint64_t ts_us) { return item.hibound < ts_us; });
    if (it == partitions_.end()) {
        return NULL;
    }
    if (ts_us >= it->lobound && ts_us < it->hibound) {
        return it->info;
    }
    return NULL;
}

const exp_table_part_t* OrderdPartitionList::Get(int no) const {
    if (no >= (int)partitions_.size() || no < 0) return NULL;
    return partitions_[no].info;
}

const exp_table_part_t* OrderdPartitionList::Get(const std::string partition_name) const {
    auto iter = name2partition_.find(partition_name);
    if (iter == name2partition_.end()) {
        return NULL;
    }
    return iter->second;
}

static void exp_table_meta_deleter(exp_table_meta* m) {
    free_table_info(m);
    delete m;
}

TableInfo::TableInfo(std::unique_ptr<exp_table_meta> meta) {
    meta_.reset(meta.release(), exp_table_meta_deleter);  // 设置deleter
    table_name = meta_->name;
    user_id = meta_->uid;

    columns.reserve(meta_->column_count);
    for (size_t i = 0; i < meta_->column_count; ++i) {
        auto col = meta_->columns + i;
        columns.emplace_back(*col);
    }

    if (meta_->parted) {
        int64_t interval = ::atol(meta_->part_table.desc.interval.str);
        partitions_.InsertBatch(meta_->part_table.desc.partcnt, interval, meta_->part_table.entitys);
    }
}

const exp_column_def_t* TableInfo::GetColumnByName(const std::string& col_name) const {
    for (uint32 i = 0; i < meta_->column_count; i++) {
        if (intarkdb::StringUtil::IsEqualIgnoreCase(meta_->columns[i].name.str, (size_t)meta_->columns[i].name.len,
                                                    col_name.c_str(), col_name.size())) {
            return &meta_->columns[i];
        }
    }
    return NULL;
}

const exp_table_part_t* TableInfo::GetTablePartByTimestamp(uint64_t ts_us) const {
    const exp_table_part_t* rs = partitions_.Query(ts_us);
    return rs;
}

const exp_table_part_t* TableInfo::GetTablePartByName(const std::string& part_name) const {
    if (!meta_->parted) {
        GS_LOG_RUN_WAR("table is not a part table!\n");
        return NULL;
    }
    return partitions_.Get(part_name);
}
