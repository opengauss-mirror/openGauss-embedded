#include "cJSON.h"
#include "storage/gstor/zekernel/common/cm_defs.h"
#include "cm_base.h"

EXPORT_API status_t json_get_number(const cJSON *object, const char *key, double *value);
EXPORT_API status_t json_get_string(const cJSON *object, const char *key, char *value);
EXPORT_API status_t json_get_bool(const cJSON *object, const char *key, cJSON_bool * value);
EXPORT_API status_t json_get_array(const cJSON *object, const char *key, cJSON **array, int *size);
EXPORT_API status_t json_get_object(const cJSON *object, const char *key, cJSON **son_object);