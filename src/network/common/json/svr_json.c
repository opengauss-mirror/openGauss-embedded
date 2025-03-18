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
* svr_json.c
*
* IDENTIFICATION
* openGauss-embedded/src/network/common/json/svr_json.h
*
* -------------------------------------------------------------------------
*/

#include "svr_json.h"
#include "storage/gstor/zekernel/common/cm_log.h"

status_t json_get_number(const cJSON *object, const char *key, double *value)
{
    GS_RETURN_IF_FALSE(cJSON_HasObjectItem(object, key));
    cJSON* node = cJSON_GetObjectItem(object, key);
    GS_RETURN_IF_FALSE(cJSON_IsNumber(node));
    *value = cJSON_GetNumberValue(node);
    return GS_SUCCESS;
}

status_t json_get_string(const cJSON *object, const char *key, char * value)
{
    GS_RETURN_IF_FALSE(cJSON_HasObjectItem(object, key));
    cJSON* node = cJSON_GetObjectItem(object, key);
    GS_RETURN_IF_FALSE(cJSON_IsString(node));
    strcpy(value,cJSON_GetStringValue(node));
    //GS_LOG_DEBUG_INF("key:%s value:%s", key, value);
    return GS_SUCCESS;
}

status_t json_get_bool(const cJSON *object, const char *key, cJSON_bool * value)
{
    GS_RETURN_IF_FALSE(cJSON_HasObjectItem(object, key));
    cJSON* node = cJSON_GetObjectItem(object, key);
    GS_RETURN_IF_FALSE(cJSON_IsBool(node));
    *value = cJSON_IsTrue(node);
    //GS_LOG_DEBUG_INF("key:%s value:%d", key, value);
    return GS_SUCCESS;
}

status_t json_get_array(const cJSON *object, const char *key, cJSON **array, int *size)
{
    GS_RETURN_IF_FALSE(cJSON_HasObjectItem(object, key));
    *array = cJSON_GetObjectItem(object, key);
    *size = cJSON_GetArraySize(*array);
    //printf("key:%s array size:%d %s\n", key, *size, cJSON_PrintUnformatted(*array));
    return GS_SUCCESS;
}

status_t json_get_object(const cJSON *object, const char *key, cJSON **son_object)
{
    GS_RETURN_IF_FALSE(cJSON_HasObjectItem(object, key));
    *son_object = cJSON_GetObjectItem(object, key);
    //GS_LOG_DEBUG_INF("key:%s object:%s", key, cJSON_PrintUnformatted(*son_object));
    return GS_SUCCESS;
}
