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
* status.
*
* IDENTIFICATION
* openGauss-embedded/src/monitor/status.h
*
* -------------------------------------------------------------------------
*/

#ifndef __UTIL_STATUS_
#define __UTIL_STATUS_

#include "compute/sql/include/common/winapi.h"
#include "cm_base.h"
#include "cm_defs.h"
#ifdef WIN32
#include <sys/types.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif
EXPORT_API unsigned long get_cpu_total_occupy();EXPORT_API double get_self_cpu();
#ifdef _MSC_VER
EXPORT_API uint64_t get_memory_by_pid(DWORD pid);
#else
EXPORT_API uint64_t get_memory_by_pid(pid_t pid);
#endif
EXPORT_API uint64_t get_memory();
EXPORT_API uint64_t get_dir_size(const char* filename);
EXPORT_API void get_os_info(char* osInfo, const uint32_t size);
EXPORT_API uint64 get_start_time();
#ifdef __cplusplus
}
#endif
#endif