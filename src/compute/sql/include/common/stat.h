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
 * stat.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/stat.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#ifdef TIMESTAT
#include <vector>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <unordered_map>
#include <mutex>

#include "common/winapi.h"

extern std::unordered_map<std::string, int64_t> g_time_statistics;
class TimeStatistics {
public:
    INTARKDB_EXTENSION_API TimeStatistics();
    INTARKDB_EXTENSION_API TimeStatistics(std::string func_name);

    INTARKDB_EXTENSION_API void Record(std::string name);

    INTARKDB_EXTENSION_API ~TimeStatistics();

    void Print();

    int64_t CalCostTime(struct timeval *begin_time, struct timeval *end_time);

private:
    struct timeval start_; 
    std::string func_name_;
    std::vector<std::pair<std::string, struct timeval>> times_;
    static std::mutex time_stat_mutex_;
};

INTARKDB_EXTENSION_API void TimeStatisticsPrint();

#define TIMESTATISTICS TimeStatistics cost_stat(__func__)
#define TIMESTATISTICSWITHNAME(name) TimeStatistics cost_stat(name)
#define TIMESTATISTICSPRINT TimeStatisticsPrint()
#else
#define TIMESTATISTICS
#define TIMESTATISTICSWITHNAME(name)

#define TIMESTATISTICSPRINT
#endif
