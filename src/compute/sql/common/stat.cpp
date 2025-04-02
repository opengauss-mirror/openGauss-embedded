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
* stat.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/common/stat.cpp
*
* -------------------------------------------------------------------------
*/

#ifdef TIMESTAT
#include <iostream>
#include "common/stat.h"

std::mutex TimeStatistics::time_stat_mutex_;

std::unordered_map<std::string, int64_t> g_time_statistics;
std::vector<std::string> g_tstat_keyorder;
TimeStatistics::TimeStatistics() {
    gettimeofday(&start_, NULL);
}

TimeStatistics::TimeStatistics(std::string func_name) {
    gettimeofday(&start_, NULL);
    func_name_ = func_name;
}

void TimeStatistics::Record(std::string name) {
    struct timeval record_point;
    gettimeofday(&record_point, NULL);
    times_.push_back({name, record_point});
}

TimeStatistics::~TimeStatistics() {
    struct timeval end_point;
    gettimeofday(&end_point, NULL);
    int64_t cost = CalCostTime(&start_, &end_point);
    std::lock_guard<std::mutex> lock(TimeStatistics::time_stat_mutex_);
    if (g_time_statistics.find(func_name_) == g_time_statistics.end()){
        g_time_statistics[func_name_] = cost;
        g_tstat_keyorder.push_back(func_name_);
    } else {
        g_time_statistics[func_name_] += cost;
    }
}

void TimeStatistics::Print() {
    std::cout << times_[0].first << " cost: " << CalCostTime(&start_, &times_[0].second) << " us" << std::endl;
    for (size_t i=1;i<times_.size();i++) {
        std::cout << times_[i].first << " cost: " << CalCostTime(&times_[i-1].second, &times_[i].second) << " us" << std::endl;
    }
}

int64_t TimeStatistics::CalCostTime(struct timeval *begin_time, struct timeval *end_time) {
    return (end_time->tv_sec*1000000+end_time->tv_usec) - (begin_time->tv_sec*1000000+begin_time->tv_usec);
}

void TimeStatisticsPrint() {
    for (auto& key : g_tstat_keyorder) {
        std::cout << key << " cost: " << g_time_statistics[key] << " us" << std::endl;
    }
}

#endif