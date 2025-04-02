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
 * table_info.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/catalog/table_info.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>

#include <map>
#include <string>
#include <vector>

#include "catalog/index.h"
#include "catalog/schema.h"
#include "storage/gstor/gstor_executor.h"

bool IsValidTableName(std::string_view name);

// 把区间有序化，方便查找
class OrderdPartitionList {
    struct ParitionTableItem {
        uint64_t hibound;
        uint64_t lobound;
        const exp_table_part_t* info;
    };

   public:
    OrderdPartitionList() {}

    void InsertBatch(size_t cnt, int64_t interval, const exp_table_part_t* partition_info);
    // ts 单位为微秒
    const exp_table_part_t* Query(uint64_t ts_us) const;

    const exp_table_part_t* Get(int no) const;

    const exp_table_part_t* Get(const std::string partition_name) const;

    size_t PartitionNum() const { return partitions_.size(); }

   private:
    std::vector<ParitionTableItem> partitions_;
    std::map<std::string, const exp_table_part_t*> name2partition_;
};

// 表的元数据
class TableInfo {
   public:
    TableInfo(std::unique_ptr<exp_table_meta> meta);
    ~TableInfo() {}

    // delete copy construct
    TableInfo(const TableInfo&) = delete;
    TableInfo& operator=(const TableInfo&) = delete;

    std::string_view table_name;
    std::vector<Column> columns;

    uint32_t user_id;

   public:
    auto GetIndexBySlot(uint32_t slot) const -> const Index {
        if (slot >= meta_->index_count) {
            throw std::invalid_argument(fmt::format("invalid index slot {}", slot));
        }

        return Index(meta_->indexes[slot].name.str, std::string(table_name), meta_->indexes[slot]);
    }

    const uint32_t GetIndexCount() const { return meta_->index_count; }

    const uint32_t GetColumnCount() const { return meta_->column_count; }

    const exp_table_meta& GetTableMetaInfo() const { return *meta_; }

    // get object type
    const exp_dict_type_t GetObjectType() const { return meta_->dict_type; }
    uint32_t GetSpaceId() const { return meta_->space_id; }
    uint32_t GetTableId() const { return meta_->id; }
    bool IsTimeScale() const { return meta_->is_timescale == GS_TRUE; }
    std::string_view GetTableName() const { return table_name; }

    EXPORT_API const exp_column_def_t* GetColumnByName(const std::string& col_name) const;

    const exp_table_part_t* GetTablePartByTimestamp(uint64_t timestamp) const;
    const exp_table_part_t* GetTablePartByIdx(size_t idx) const { return partitions_.Get(idx); }
    size_t GetTablePartCount() const { return partitions_.PartitionNum(); }

    const exp_table_part_t* GetTablePartByName(const std::string& part_name) const;

   private:
    std::shared_ptr<exp_table_meta> meta_;
    OrderdPartitionList partitions_;
};
