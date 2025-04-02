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
* status.c
*
* IDENTIFICATION
* openGauss-embedded/src/monitor/status.c
*
* -------------------------------------------------------------------------
*/

#include <stdio.h>
#include <string.h>
#include <time.h>
#include "cm_types.h"
#include "cm_defs.h"
#include "cm_log.h"
#include "status.h"

#ifndef WIN32
#include <sys/resource.h>
#include <sys/utsname.h>
#include <dirent.h>
#include <sys/stat.h>
#else
#include <windows.h>
#include <psapi.h>
#endif

const uint32 MAX_RETRY_TIMES = 5;

/* unsigned long get_cpu_total_occupy() {
	FILE *fd;
	long int temp[10];
	char buff[1024]={0};
    uint32_t times = 0;
    const uint32 time_internal = 5;
    do {
	    fd = fopen("/proc/stat","r");
        times++;
        if (times > 1) {
            auto errNum = errno;
            GS_LOG_RUN_ERR("open /proc/stat failed times:%u errno = %d\n", times, errNum);
            cm_sleep(time_internal);
            if (times >= MAX_RETRY_TIMES && fd == NULL) {
                return 0;
            }
        }
    } while (fd == NULL);
    
	fgets(buff,sizeof(buff),fd);
	char name[64] = {0};
	sscanf(buff, "%16s %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld", name, &temp[0], &temp[1], &temp[2], &temp[3], &temp[4], 
           &temp[5], &temp[6], &temp[7], &temp[8], &temp[9]);
	fclose(fd);
	return temp[0] + temp[1] + temp[2] + temp[3];
} */

#ifdef WIN32 
double get_self_cpu_win(unsigned long long time_internal) {
    return 0; 
/*     HANDLE process;
    process = GetCurrentProcess();


    FILETIME ftStart, ftEnd;
    FILETIME kernel_time_last, user_time_last, kernel_time, user_time, tmp;
    ULONGLONG last_tick_count = 0, now_ticks = 0;
    ULONGLONG elapsed_ticks, idle_ticks, kernel_ticks, user_ticks;
    if(!GetProcessTimes(process, &ftStart, &tmp, &kernel_time_last, &user_time_last)) {
        printf("GetProcessTimes failed (Error code: %u)\n", GetLastError());
        return 0; 
    }
    last_tick_count = GetTickCount64();

    cm_sleep(time_internal * 50);
    if(!GetProcessTimes(process, &ftEnd, &tmp, &kernel_time, &user_time)) {
        printf("GetProcessTimes failed (Error code: %u)\n", GetLastError());
        return 0;
    }
    now_ticks = GetTickCount64();

    kernel_ticks = ((ULONGLONG)kernel_time.dwHighDateTime << 32) + kernel_time.dwLowDateTime - 
                   ((ULONGLONG)kernel_time_last.dwHighDateTime << 32) - kernel_time_last.dwLowDateTime;
    user_ticks = ((ULONGLONG)user_time.dwHighDateTime << 32) + user_time.dwLowDateTime - 
                 ((ULONGLONG)user_time_last.dwHighDateTime << 32) - user_time_last.dwLowDateTime;
    //todo del printf
    printf("time_internal:%llu \n", time_internal);
    printf("now  kernel_time: %lu-%lu user_ticks %lu-%lu\n", kernel_time.dwHighDateTime, kernel_time.dwLowDateTime, user_time.dwHighDateTime, user_time.dwLowDateTime);
    printf("last kernel_time: %lu-%lu user_ticks %lu-%lu\n", kernel_time_last.dwHighDateTime, kernel_time_last.dwLowDateTime, user_time_last.dwHighDateTime, user_time_last.dwLowDateTime);
    printf("now_ticks:%llu  last_tick_count: %llu \n",now_ticks, last_tick_count);
    elapsed_ticks = now_ticks - last_tick_count;
    if(elapsed_ticks == 0) {
        //todo log
        return 0;
    }[n]
    
    return (double)(kernel_ticks + user_ticks) / elapsed_ticks; */
}
#endif

double get_self_cpu() {
uint32 time_internal = 100;
#ifdef WIN32
  return get_self_cpu_win(time_internal);
#else
    struct rusage self_ru, self_ru2;
    struct timespec start_time, end_time;
    getrusage(RUSAGE_SELF, &self_ru);
    if (clock_gettime(CLOCK_MONOTONIC, &start_time) != 0) {
        GS_LOG_RUN_ERR("clock_gettime error");
        return 0;
    }

    cm_sleep(time_internal);
    getrusage(RUSAGE_SELF, &self_ru2);
    if (clock_gettime(CLOCK_MONOTONIC, &end_time) != 0) {
        GS_LOG_RUN_ERR("clock_gettime error");
        return 0;
    }
    uint64_t cores = sysconf(_SC_NPROCESSORS_ONLN);
    if (cores == -1) {
        GS_LOG_RUN_ERR("sysconf get number of cpu cores error");
        return 0;
    }
    GS_LOG_RUN_INF("Number of cores: %lld\n", cores);
    double elapsed_time = end_time.tv_sec - start_time.tv_sec
                         + (end_time.tv_nsec - start_time.tv_nsec) / 1e9;

    double proc_cpu_time = (double)(self_ru2.ru_stime.tv_sec + self_ru2.ru_utime.tv_sec - 
                            self_ru.ru_stime.tv_sec - self_ru.ru_utime.tv_sec) + 
                            (double)(self_ru2.ru_stime.tv_usec + self_ru2.ru_utime.tv_usec - 
                            self_ru.ru_stime.tv_usec - self_ru.ru_utime.tv_usec)/1e6;
    if (elapsed_time != 0 && cores != 0){
        double rate = proc_cpu_time/ elapsed_time / cores;
        GS_LOG_RUN_INF("CPU utilization rate:%f Number of cores: %lld", rate, cores);
        return rate; 
    }
    GS_LOG_RUN_INF("Get CPU utilization rate error! Number of cores: %lld", cores);
    return 0;

#endif
}


#ifdef WIN32
#ifdef _MSC_VER
uint64_t get_memory_by_pid(DWORD pid)
#else
uint64_t get_memory_by_pid(pid_t pid)
#endif
 {
    HANDLE process;
    PROCESS_MEMORY_COUNTERS pmc;
    process = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, pid);
    if (!GetProcessMemoryInfo(process, &pmc, sizeof(pmc))) {
        CloseHandle(process);
        return 0;
    }
    CloseHandle(process);
    return pmc.WorkingSetSize;
}
#else
uint64_t get_memory_by_pid(pid_t pid) {
    FILE *fd;
    char line[1024] = {0};
    char virtual_filename[32] = {0};
    char vmrss_name[32] = {0};
    int vmrss_num = 0;
    sprintf(virtual_filename, "/proc/%d/status", pid);
    fd = fopen(virtual_filename, "r");
    if (fd == NULL) {
        auto errNum = errno;
        GS_LOG_RUN_ERR("open %s failed errno:%d", virtual_filename, errNum);
        return 0;
    }

    for (int i = 0; i < GS_SEC_PER_MIN; i++) {
        fgets(line, sizeof(line), fd);
        GS_LOG_DEBUG_INF("get line:%s", line);
        if (strstr(line, "VmRSS:") != NULL) {
            sscanf(line, "%16s %d", vmrss_name, &vmrss_num);
            break;
        }
    }
    fclose(fd);
    return vmrss_num;
}
#endif

uint64_t get_memory() {
#ifdef WIN32
    #ifdef _MSC_VER
        DWORD pid = GetCurrentProcessId();
    #else
        pid_t pid = GetCurrentProcessId();
    #endif
#else 
  pid_t pid = getpid();
#endif
  return get_memory_by_pid(pid);
}

uint64_t get_dir_size(const char* filename) {
#ifdef WIN32
    uint64_t size = 0;
    WIN32_FIND_DATA fd;
    memset(&fd, 0, sizeof(fd));
    SetCurrentDirectory(filename);
    HANDLE hd = FindFirstFile("*.*", &fd);
    if (hd != INVALID_HANDLE_VALUE) { 
        do {
            if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
                 if (fd.cFileName[0] != L'.' || (fd.cFileName[1] != L'\0' && (fd.cFileName[1] != L'.' || fd.cFileName[2] != L'\0'))) {
                    size += get_dir_size(fd.cFileName);
                    SetCurrentDirectory("..");
                }
            }
            else {
                size += fd.nFileSizeLow + ((fd.nFileSizeHigh) << (sizeof(fd.nFileSizeLow) * 8));
            }
        } while (FindNextFile(hd, &fd));
        FindClose(hd);
    }
    return size;
#else
    DIR *dir;
    struct dirent *entry;
    struct stat statbuf;
    long long int totalSize=0;

    lstat(filename, &statbuf);
    totalSize+=statbuf.st_size;
    if ((dir = opendir(filename)) == NULL) {
        fprintf(stderr, "Cannot open dir: %s", filename);
        return totalSize;
    }
    while ((entry = readdir(dir)) != NULL) {
        char subdir[256];
        sprintf(subdir, "%s/%s", filename, entry->d_name);
        lstat(subdir, &statbuf);
        if (S_ISDIR(statbuf.st_mode)) {
            if (strcmp(".", entry->d_name) == 0 || strcmp("..", entry->d_name) == 0)
                continue;
            uint64 subDirSize = get_dir_size(subdir);
            totalSize+=subDirSize;
        }
        else {
            totalSize+=statbuf.st_size;
        }
    }
    closedir(dir);    
    return totalSize;
#endif
}

void get_os_info(char* osInfo, const uint32_t size) {
#ifdef WIN32
    OSVERSIONINFOEX osVersionInfo;
    
    ZeroMemory(&osVersionInfo, sizeof(OSVERSIONINFOEX));
    osVersionInfo.dwOSVersionInfoSize = sizeof(OSVERSIONINFOEX);

    if (!GetVersionEx((LPOSVERSIONINFO)&osVersionInfo)) {
        sprintf_s(osInfo, size, "Windows");
        return;
    }
    static char architecture[32], os_name[32];
    SYSTEM_INFO systemInfo;
    GetSystemInfo(&systemInfo);

    switch (systemInfo.wProcessorArchitecture) {
        case PROCESSOR_ARCHITECTURE_INTEL:
            sprintf_s(architecture, sizeof(architecture), "x86");
            break;
        case PROCESSOR_ARCHITECTURE_MIPS:
            sprintf_s(architecture, sizeof(architecture), "MIPS");
            break;
        case PROCESSOR_ARCHITECTURE_ARM:
            sprintf_s(architecture, sizeof(architecture), "ARM");
            break;
        case PROCESSOR_ARCHITECTURE_IA64:
            sprintf_s(architecture, sizeof(architecture), "IA-64");
            break;
        case PROCESSOR_ARCHITECTURE_ALPHA64:
            sprintf_s(architecture, sizeof(architecture), "Alpha64");
            break;
        case PROCESSOR_ARCHITECTURE_PPC:
            sprintf_s(architecture, sizeof(architecture), "PowerPC");
            break;
        case PROCESSOR_ARCHITECTURE_SHX:
            sprintf_s(architecture, sizeof(architecture), "SHx");
            break;
        case PROCESSOR_ARCHITECTURE_AMD64:
            sprintf_s(architecture, sizeof(architecture), "x64");
            break;
        case PROCESSOR_ARCHITECTURE_ARM64:
            sprintf_s(architecture, sizeof(architecture), "ARM64");
            break;
        default:
            sprintf_s(architecture, sizeof(architecture), "");
            break;
    }

    switch (osVersionInfo.dwPlatformId) {
        case VER_PLATFORM_WIN32_NT:
            if (osVersionInfo.dwMajorVersion == 6 && osVersionInfo.dwMinorVersion == 0) {
                sprintf_s(os_name, sizeof(os_name), "Windows Vista");
            } else if (osVersionInfo.dwMajorVersion == 6 && osVersionInfo.dwMinorVersion == 1) {
                sprintf_s(os_name, sizeof(os_name), "Windows 7");
            } else if (osVersionInfo.dwMajorVersion == 6 && osVersionInfo.dwMinorVersion == 2) {
                sprintf_s(os_name, sizeof(os_name), "Windows 8");
            } else if (osVersionInfo.dwMajorVersion == 6 && osVersionInfo.dwMinorVersion == 3) {
                sprintf_s(os_name, sizeof(os_name), "Windows 8.1");
            } else if (osVersionInfo.dwMajorVersion == 10) {
                if (osVersionInfo.dwBuildNumber >= 22000) {
                    sprintf_s(os_name, sizeof(os_name), "Windows 11");
                } else {
                    sprintf_s(os_name, sizeof(os_name), "Windows 10");
                }
            }
            break;
        default:
            sprintf_s(os_name, sizeof(os_name), "Windows");
    }

    sprintf_s(osInfo, size, "%s Version:%d.%d.%d %s\n", 
              os_name,   
              osVersionInfo.dwMajorVersion,
              osVersionInfo.dwMinorVersion,
              osVersionInfo.dwBuildNumber,
              architecture);
    return;
#else
    static struct utsname name;
    uname(&name);
    snprintf(osInfo, size, "%s %s %s", name.sysname, name.release, name.machine);
    return;
#endif 
}

uint64 get_start_time() {
#ifdef WIN32
    return 0;
    /* HANDLE process;
    FILETIME start_time, exit_time, kernel_time, user_time;
    ULONGLONG time = 0;
    process = GetCurrentProcess();
    if (!(process, &start_time, &exit_time, &kernel_time, &user_time)) {
        fprintf(stderr, "GetProcessTimes failed (Error code: %u)\n", GetLastError());
        CloseHandle(process);
        return 0;
    }
    CloseHandle(process);
    time = ((ULONGLONG)start_time.dwHighDateTime << 32) + start_time.dwLowDateTime;
    ULONGLONG result = (time / 10000000ULL) - 11644473600;
    //printf("dwHighDateTime:%u dwLowDateTime:%u time:%llu result:%llu \n", start_time.dwHighDateTime, start_time.dwLowDateTime, time, result);
    return result; */
#else
    pid_t pid = getpid();
    FILE *fd;
    char *p;
    unsigned long btime, run_time;
    char line[1024] = {0};
    char btime_name[8] = {0};
    fd = fopen("/proc/stat", "r");
    if (fd == NULL) {
        auto errNum = errno;
        GS_LOG_RUN_ERR("open /proc/stat failed errno:%d", errNum);
        return 0;
    }
    while (fgets(line, sizeof(line), fd) != NULL) {
        if (strstr(line, "btime") != NULL) {
            sscanf(line, "%5s %lu", btime_name, &btime);
            break;
        }
    }
    fclose(fd);

    GS_LOG_DEBUG_INF("btime:%ld", btime);
    char pid_stat[255];
    sprintf(pid_stat, "/proc/%d/stat", pid);
    fd = fopen(pid_stat, "r");
    if (fd == NULL) {
        auto errNum = errno;
        GS_LOG_RUN_ERR("open %s failed errno:%d", pid_stat, errNum);
        return 0;
    }
    char char_stat[1024] = {0};
    for (int i = 0; i < 22; i++) {
        fscanf(fd, "%s", char_stat);
    }
    run_time = strtoul(char_stat, &p, 10);
    fclose(fd);
    return run_time / sysconf(_SC_CLK_TCK) + btime;
#endif
}