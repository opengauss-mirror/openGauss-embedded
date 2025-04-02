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
* default_views.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/catalog/default_views.cpp
*
* -------------------------------------------------------------------------
*/
#include "catalog/default_view.h"

#include <vector>

#include "main/connection.h"

static bool g_is_create_default_views = false;

static std::vector<std::string> default_sqls = {
    "create view if not exists sqlite_master as \
        select 'table' as type, \"NAME\" as name, \"NAME\" as tbl_name, 0 as rootpage, '' as sql from \"SYS_TABLES\" \
        where \"SPACE#\" = 3 \
        union all \
        select 'view' as type, \"NAME\" as name, \"NAME\" as tbl_name, 0 as rootpage, '' as sql from \"SYS_VIEWS\" \
        where \"NAME\" != 'sqlite_master' \
        union all \
        select 'index' as type, \"NAME\" as name, \"NAME\" as tbl_name, 0 as rootpage, '' as sql from \"SYS_INDEXES\" \
        where \"SPACE#\" = 3",
};

void DefaultViewGenerator::CreateDefaultEntry(Connection *conn) {
    if (g_is_create_default_views) {
        return;
    }

    for (auto &sql : default_sqls) {
        if (!g_is_create_default_views) {
            g_is_create_default_views = true;
        }

        try {
            auto rb = conn->Query((char *)sql.c_str());
            if (rb->GetRetCode() != GS_SUCCESS) {
                GS_LOG_RUN_INF("Failed to create default views, ignoring...(msg=%s)", rb->GetRetMsg().c_str());
                GS_LOG_RUN_INF("Failed to create default views, ignoring...(sql=%s)", sql.c_str());
                cm_reset_error();
            }
        } catch (intarkdb::Exception& instar_ex) {
            GS_LOG_RUN_INF("Failed to create default views, ignoring...(sql=%s)", sql.c_str());
            cm_reset_error();
        } catch (const std::exception& e) {
            GS_LOG_RUN_INF("Failed to create default views, ignoring...(sql=%s)", sql.c_str());
            cm_reset_error();
        } catch (...) {
            GS_LOG_RUN_INF("Failed to create default views, ignoring...(sql=%s)", sql.c_str());
            cm_reset_error();
        }
    }
    g_is_create_default_views = true;
}
