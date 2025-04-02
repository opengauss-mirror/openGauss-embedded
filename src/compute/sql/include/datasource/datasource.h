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
 * datasource.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/datasource/datasource.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <string>
#include <tuple>
#include <vector>

#include "catalog/schema.h"
#include "common/row_container.h"

class DataSource {
   public:
    /** Return the schema for the underlying data source */
    virtual const Schema& GetSchema() const = 0;

    virtual std::tuple<intarkdb::RowContainerPtr, knl_cursor_t*, bool> Next() = 0;

    virtual ~DataSource() {}
};
