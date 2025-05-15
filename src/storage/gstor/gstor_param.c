/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * gstor_param.c
 *    gstor param
 *
 * IDENTIFICATION
 *    src/storage/gstor/gstor_param.c
 *
 * -------------------------------------------------------------------------
 */

#include "cm_defs.h"
#include "gstor_param.h"

config_item_t g_parameters[] = {
    // name (30B)             isdefault  attr      defaultvalue value     runtime desc range datatype           write_ini
    // -------------          ---------  ----      ------------ -----     ------- ---- ----- --------           -------- 
#if defined(INTARK_LITE)
    {"DATA_BUFFER_SIZE",        GS_TRUE, ATTR_NONE, "4M",       "4M",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"USE_LARGE_PAGES",         GS_TRUE, ATTR_NONE, "FALSE",    "FALSE",    NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"SHARED_AREA_SIZE",        GS_TRUE, ATTR_NONE, "3M",       "8M",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"MAX_RMS",                 GS_TRUE, ATTR_NONE, "1024",     "1024",     NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"INIT_LOCKPOOL_PAGES",     GS_TRUE, ATTR_NONE, "32",       "32",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"DEFAULT_EXTENTS",         GS_TRUE, ATTR_NONE, "8",        "8",        NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
#else           
    {"DATA_BUFFER_SIZE",        GS_TRUE, ATTR_NONE, "256M",     "256M",     NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"USE_LARGE_PAGES",         GS_TRUE, ATTR_NONE, "TRUE",     "TRUE",     NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"SHARED_AREA_SIZE",        GS_TRUE, ATTR_NONE, "65M",      "65M",      NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"MAX_RMS",                 GS_TRUE, ATTR_NONE, "16320",    "16320",    NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"INIT_LOCKPOOL_PAGES",     GS_TRUE, ATTR_NONE, "1024",     "1024",     NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"DEFAULT_EXTENTS",         GS_TRUE, ATTR_NONE, "32",       "32",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
#endif
    {"REDO_SPACE_SIZE",         GS_TRUE, ATTR_NONE, "16M",      "16M",      NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"USER_SPACE_SIZE",         GS_TRUE, ATTR_NONE, "2M",       "2M",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"UNDO_SPACE_SIZE",         GS_TRUE, ATTR_NONE, "2M",       "2M",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"SWAP_SPACE_SIZE",         GS_TRUE, ATTR_NONE, "2M",       "1M",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"SYSTEM_SPACE_SIZE",       GS_TRUE, ATTR_NONE, "2M",       "2M",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
#if defined(DCC_LITE) || defined(INTARK_LITE)       
    {"LOG_BUFFER_SIZE",         GS_TRUE, ATTR_NONE, "1M",       "1M",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"LOG_BUFFER_COUNT",        GS_TRUE, ATTR_NONE, "1",        "1",        NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"UNDO_RESERVE_SIZE",       GS_TRUE, ATTR_NONE, "64",       "64",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE }, // DEFAULT_UNDO_RESERVER_SIZE
    {"UNDO_SEGMENTS",           GS_TRUE, ATTR_NONE, "2",        "2",        NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE }, // DEFAULT_UNDO_SEGMENTS
    {"INDEX_BUF_SIZE",          GS_TRUE, ATTR_NONE, "16K",      "16K",      NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE }, // DEFAULT_INDEX_BUF_SIZE
#else           
    {"LOG_BUFFER_SIZE",         GS_TRUE, ATTR_NONE, "16M",      "16M",      NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"LOG_BUFFER_COUNT",        GS_TRUE, ATTR_NONE, "4",        "4",        NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"UNDO_RESERVE_SIZE",       GS_TRUE, ATTR_NONE, "1024",     "1024",     NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"UNDO_SEGMENTS",           GS_TRUE, ATTR_NONE, "32",       "32",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"INDEX_BUF_SIZE",          GS_TRUE, ATTR_NONE, "32M",      "32M",      NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
#endif          
#if defined(INTARK_LITE)            
    {"VMA_SIZE",                GS_TRUE, ATTR_NONE, "132K",     "132K",     NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"LARGE_VMA_SIZE",          GS_TRUE, ATTR_NONE, "2100K",    "2100K",    NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"LARGE_POOL_SIZE",         GS_TRUE, ATTR_NONE, "320K",     "320K",     NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"TEMP_BUF_SIZE",           GS_TRUE, ATTR_NONE, "1M",       "1M",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"CR_POOL_SIZE",            GS_TRUE, ATTR_NONE, "16K",      "16K",      NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
#elif defined(DCC_LITE)         
    {"VMA_SIZE",                GS_TRUE, ATTR_NONE, "4M",       "1M",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE }, // DEFAULT_VMA_SIZE
    {"LARGE_VMA_SIZE",          GS_TRUE, ATTR_NONE, "1M",       "2100K",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE }, // DEFAULT_LARGE_VMA_SIZE
    {"LARGE_POOL_SIZE",         GS_TRUE, ATTR_NONE, "4M",       "2M",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE }, // DEFAULT_LARGE_POOL_SIZE
    {"TEMP_BUF_SIZE",           GS_TRUE, ATTR_NONE, "4M",       "1M",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE }, // DEFAULT_TEMP_BUF_SIZE
    {"CR_POOL_SIZE",            GS_TRUE, ATTR_NONE, "1M",       "1M",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE }, // DEFAULT_CR_POOL_SIZE
#else           
    {"VMA_SIZE",                GS_TRUE, ATTR_NONE, "16M",      "16M",      NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"LARGE_VMA_SIZE",          GS_TRUE, ATTR_NONE, "16M",      "16M",      NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"LARGE_POOL_SIZE",         GS_TRUE, ATTR_NONE, "16M",      "16M",      NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"TEMP_BUF_SIZE",           GS_TRUE, ATTR_NONE, "128M",     "128M",     NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"CR_POOL_SIZE",            GS_TRUE, ATTR_NONE, "64M",      "64M",      NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
#endif          
    {"BUF_POOL_NUM",            GS_TRUE, ATTR_NONE, "1",        "1",        NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"PAGE_SIZE",               GS_TRUE, ATTR_NONE, "8K",       "8K",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"SPACE_SIZE",              GS_TRUE, ATTR_NONE, "16M",     "16M",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"UNDO_TABLESPACE",         GS_TRUE, ATTR_NONE, "UNDO",     "UNDO",     NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"ARCHIVE_MODE",            GS_TRUE, ATTR_NONE, "1",        "1",        NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"MAX_ARCH_FILES_SIZE",     GS_TRUE, ATTR_NONE, "512M",     "512M",     NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"REDO_SAVE_TIME",          GS_TRUE, ATTR_NONE, "1800",     "1800",     NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"ARCHIVE_DEST_1",          GS_TRUE, ATTR_NONE, "",         NULL,       NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_2",          GS_TRUE, ATTR_NONE, "",         NULL,       NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_3",          GS_TRUE, ATTR_NONE, "",         NULL,       NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_4",          GS_TRUE, ATTR_NONE, "",         NULL,       NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_5",          GS_TRUE, ATTR_NONE, "",         NULL,       NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_6",          GS_TRUE, ATTR_NONE, "",         NULL,       NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_7",          GS_TRUE, ATTR_NONE, "",         NULL,       NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_8",          GS_TRUE, ATTR_NONE, "",         NULL,       NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_9",          GS_TRUE, ATTR_NONE, "",         NULL,       NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_10",         GS_TRUE, ATTR_NONE, "",         NULL,       NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_STATE_1",    GS_TRUE, ATTR_NONE, "ENABLE",   "ENABLE",   NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_STATE_2",    GS_TRUE, ATTR_NONE, "ENABLE",   "ENABLE",   NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_STATE_3",    GS_TRUE, ATTR_NONE, "ENABLE",   "ENABLE",   NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_STATE_4",    GS_TRUE, ATTR_NONE, "ENABLE",   "ENABLE",   NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_STATE_5",    GS_TRUE, ATTR_NONE, "ENABLE",   "ENABLE",   NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_STATE_6",    GS_TRUE, ATTR_NONE, "ENABLE",   "ENABLE",   NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_STATE_7",    GS_TRUE, ATTR_NONE, "ENABLE",   "ENABLE",   NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_STATE_8",    GS_TRUE, ATTR_NONE, "ENABLE",   "ENABLE",   NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_STATE_9",    GS_TRUE, ATTR_NONE, "ENABLE",   "ENABLE",   NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"ARCHIVE_DEST_STATE_10",   GS_TRUE, ATTR_NONE, "ENABLE",   "ENABLE",   NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"_SYS_PASSWORD",           GS_TRUE, ATTR_NONE, "",         NULL,       NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_FALSE },
    {"LOG_LEVEL",               GS_TRUE, ATTR_NONE, "3",        "3",        NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"CTRL_LOG_BACK_LEVEL",     GS_TRUE, ATTR_NONE, "0",        "0",        NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"TEMP_POOL_NUM",           GS_TRUE, ATTR_NONE, "1",        "1",        NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"CR_POOL_COUNT",           GS_TRUE, ATTR_NONE, "1",        "1",        NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"CKPT_INTERVAL",           GS_TRUE, ATTR_NONE, "300",      "300",      NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"CKPT_IO_CAPACITY",        GS_TRUE, ATTR_NONE, "4096",     "4096",     NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"LOG_REPLAY_PROCESSES",    GS_TRUE, ATTR_NONE, "1",        "1",        NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"RCY_SLEEP_INTERVAL",      GS_TRUE, ATTR_NONE, "32",       "32",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"DBWR_PROCESSES",          GS_TRUE, ATTR_NONE, "4",        "1",        NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"UNDO_RETENTION_TIME",     GS_TRUE, ATTR_NONE, "20",       "20",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"TX_ROLLBACK_PROC_NUM",    GS_TRUE, ATTR_NONE, "2",        "2",        NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"MAX_COLUMN_COUNT",        GS_TRUE, ATTR_NONE, "128",      "128",      NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"MAX_TEMP_TABLES",         GS_TRUE, ATTR_NONE, "128",      "64",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"STACK_SIZE",              GS_TRUE, ATTR_NONE, "512K",     "512K",     NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"JOB_QUEUE_CAPACITY",      GS_TRUE, ATTR_NONE, "60",       "60",       NULL, "-", "-", "GS_TYPE_INTEGER",  GS_FALSE },
    {"CONTROL_FILES",           GS_TRUE, ATTR_NONE, "(%s/data/ctrl1,%s/data/ctrl2,%s/data/ctrl3)",  "(%s/data/ctrl1,%s/data/ctrl2,%s/data/ctrl3)",  NULL, "-", "-", "GS_TYPE_VARCHAR", GS_FALSE },
    {"ARCHIVE_FORMAT",          GS_TRUE, ATTR_NONE, "arch_%r_%s.arc",   "arch_%r_%s.arc",   NULL, "-", "-", "GS_TYPE_VARCHAR", GS_FALSE },
    {"SYS_USER_PASSWORD",       GS_TRUE, ATTR_NONE, "",   "9d062088d7779405895f9b4304f555cb921c2a72af5388d318da8834c6ff00ac",   NULL, "-", "-", "GS_TYPE_VARCHAR", GS_FALSE},
    {"LOCK_WAIT_TIMEOUT",       GS_TRUE, ATTR_NONE, "60000",    "60000",    NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_TRUE  },
    {"SQL_ENGINE_MEMORY_SIZE",  GS_TRUE, ATTR_NONE,"2G",        "2G",       NULL, "-", "-", "GS_TYPE_BIGINT",   GS_TRUE  },
    {"MAX_CONN_NUM",            GS_TRUE, ATTR_NONE, "100",      "100",      NULL, "-", "-", "GS_TYPE_INTEGER",  GS_TRUE  },
    {"SYNCHRONOUS_COMMIT",      GS_TRUE, ATTR_NONE, "on",       "on",       NULL, "-", "-", "GS_TYPE_VARCHAR",  GS_TRUE  },
    {"ENFORCED_IGNORE_ALL_REDO_LOGS",   GS_TRUE, ATTR_NONE, "FALSE",    "FALSE",    NULL, "-", "-", "GS_TYPE_INTEGER", GS_TRUE  },
        // 202411210   吴锦锋    人为设置直接忽略所有损坏redo文件的恢复，直接打开数据库，本设置位TRUE时排斥IGNORE_CORRUPTED_LOGS配置。
    {"IGNORE_CORRUPTED_LOGS",   GS_TRUE, ATTR_NONE, "TRUE",    "TRUE",    NULL, "-", "-", "GS_TYPE_INTEGER", GS_TRUE  },
        // 20241022   吴锦锋    增加是否在启动遇到日志损坏时先尽量修复，遇到损坏部分开始才忽略损坏文件修复的配置，本配置只有在ENFORCED_IGNORE_ALL_REDO_LOG为FALSE时才起作用
    {"SMON_LOOP_IN_IGNORE_MODE",   GS_TRUE, ATTR_NONE, "TRUE",    "TRUE",    NULL, "-", "-", "GS_TYPE_INTEGER", GS_TRUE  },
        // 20241101   吴锦锋    如果启动时遇到undo文件及其控制文件和段损坏，让系统监督smon对其进行收缩时,尽最大可能跳过损坏
    {"MAX_LOG_FILE_SIZE",       GS_TRUE, ATTR_NONE, "128M",      "128M",      NULL, "-", "-",
        "GS_TYPE_INTEGER",  GS_TRUE, "## The maximum size of the log file(default:128M)"},
    {"LOG_FILE_COUNT",          GS_TRUE, ATTR_NONE, "10",       "10",       NULL, "-", "-",
        "GS_TYPE_INTEGER",  GS_TRUE, "## The number of log files(default:10)"},
    {"ISOLATION_LEVEL",         GS_TRUE, ATTR_NONE, "1",        "1",        NULL, "-", "-",
        "GS_TYPE_INTEGER",  GS_TRUE, "## ISOLATION_LEVEL(1:Read Committed, 2:Repeatable Read)"  },
    {"TS_UPDATE_SUPPORT",       GS_TRUE, ATTR_NONE, "FALSE",    "FALSE",    NULL, "-", "-",
        "GS_TYPE_INTEGER",  GS_TRUE, "## if support update or not for timeseries table"  },
};

// copy from g_parameters
/*
    ITEM: *name;
    ITEM: is_default;
    ITEM: attr;
    ITEM: *default_value;
    ITEM: *value;
    ITEM: *runtime_value;
    ITEM: *description;
    ITEM: *range;
    ITEM: *datatype;
    ITEM: is_write_ini;
    ITEM: comment;
*/
int knl_param_get_config_info(config_item_t **params, uint32 *count)
{
    // *params = g_parameters;
    *count = sizeof(g_parameters) / sizeof(config_item_t);

    *params = (config_item_t *)malloc(sizeof(g_parameters));
    if (*params == NULL) {
        GS_LOG_RUN_ERR("alloc params object failed");
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, "copy from g_parameters");
        return GS_ERROR;
    }
    memset_s((*params), sizeof(g_parameters), 0, sizeof(g_parameters));

    for (int32 i = 0; i < *count; i++) {
        config_item_t * item = g_parameters + i;
        config_item_t * param_item = (*params) + i;
        // name
        param_item->name = item->name;
        // is_default
        param_item->is_default = item->is_default;
        // attr
        param_item->attr = item->attr;
        // default_value
        param_item->default_value = item->default_value;
        // value
        param_item->value = item->value;
        // runtime_value
        param_item->runtime_value = item->runtime_value;
        // description
        param_item->description = item->description;
        // range
        param_item->range = item->range;
        // datatype
        param_item->datatype = item->datatype;
        // is_write_ini
        param_item->is_write_ini = item->is_write_ini;
        // is_write_ini
        param_item->comment = item->comment;
    }
}

status_t knl_param_get_size_uint64(config_t *config, char *param_name, uint64 *param_value)
{
    char *value = cm_get_config_value(config, param_name);
    if (value == NULL || strlen(value) == 0) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, param_name);
        return GS_ERROR;
    }
    int64 val_int64 = 0;
    if (cm_str2size(value, &val_int64) != GS_SUCCESS || val_int64 < 0) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, param_name);
        return GS_ERROR;
    }
    *param_value = (uint64)val_int64;
    return GS_SUCCESS;
}

status_t knl_param_get_uint32(config_t *config, char *param_name, uint32 *param_value)
{
    char *value = cm_get_config_value(config, param_name);
    if (value == NULL || strlen(value) == 0) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, param_name);
        return GS_ERROR;
    }

    if (cm_str2uint32(value, param_value) != GS_SUCCESS) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, param_name);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

status_t knl_param_get_size_uint32(config_t *config, char *param_name, uint32 *param_value)
{
    char *value = cm_get_config_value(config, param_name);
    int64 val_int64 = 0;

    if (value == NULL || strlen(value) == 0) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, param_name);
        return GS_ERROR;
    }

    if (cm_str2size(value, &val_int64) != GS_SUCCESS || val_int64 < 0 || val_int64 > UINT_MAX) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, param_name);
        return GS_ERROR;
    }

    *param_value = (uint32)val_int64;
    return GS_SUCCESS;
}
