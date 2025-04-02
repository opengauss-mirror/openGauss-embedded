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
* main.cpp
*
* IDENTIFICATION
* openGauss-embedded/tools/intarkdb_cli/main.cpp
*
* -------------------------------------------------------------------------
*/
#include <fmt/core.h>
#include <fmt/format.h>

#include <iostream>
#include <memory>
#include <set>


#include "linenoise.h"
#include "binder/binder.h"
#include "catalog/catalog.h"
#include "main/connection.h"
#include "main/database.h"
#include "kv_operator.h"
#include "cmd.h"
#include "assist.h"
#include "function/version/pragma_version.h"
#include "compute/kv/kv_connection.h"
#include "cm_signal.h"

#define HISTORY_MAX_LINE 2000

std::function<void(int)> shutdown_handler;

static void signal_handler(int sig_no) { shutdown_handler(sig_no); }

auto main(int argc, char** argv) -> int {
    std::string path = "./";
    if (argc >= 2) {
        if (strcmp(argv[1], "--version")==0){
            std::cout << intarkdb::LibraryVersion() << " " << intarkdb::SourceID() << std::endl;
            return 0;
        } else {
            path = argv[1];
            path += "/";
        }
    }
    try {
        auto db_instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance(path.c_str()));
        // 启动db
        db_instance->Init();

        Connection conn(db_instance);
        conn.Init();

        auto kv_conn = std::make_unique<KvConnection>(db_instance);
        kv_conn->Init();
        KvOperator kv_operator(std::move(kv_conn));
        ClassCmd cmd(path, &conn, &kv_operator);

        char *zHome;
        int nHistory;
        char *zHistory = getenv("INTARKDB_HISTORY");
        
        
        if( zHistory ) {
            zHistory = strdup(zHistory);
        }else if( (zHome = find_home_dir(0))!=nullptr ) {
            nHistory = strlen30(zHome) + 20;
            if( (zHistory = (char*)malloc(nHistory))!=nullptr ) {
                snprintf(zHistory, nHistory, "%s/.intarkdb_history", zHome);
            }
        }
        if( zHistory ) {
            linenoiseSetMultiLine(1);
            linenoiseHistorySetMaxLen(HISTORY_MAX_LINE);
            linenoiseHistoryLoad(zHistory);
        }
        shutdown_handler = [&cmd ,&zHistory, &db_instance](int sig_no) {
            cmd.close_num++;
            if (cmd.close_num < CLOSE_TIMES) {
                std::cout << "\nIf you want to close, press ctrl+c again" << std::endl;
            } else {
                std::cout << "\nClosing..." << std::endl;
                db_instance.reset();
                if (zHistory) {
                    linenoiseHistorySave(zHistory);
                    linenoiseHistoryFree();
                    free(zHistory);
                }
                exit(0);
            }
        };
        (void)cm_regist_signal(SIGINT, signal_handler);
        cmd.main();
        if( zHistory ) {
            linenoiseHistorySave(zHistory);
            linenoiseHistoryFree();
            free(zHistory);
        }
    } catch (const std::exception& e) {
        std::cout<<" ERROR MSG:"<<e.what()<<std::endl;
    }
    
    return 0;
}
