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
* srv_handle.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/network/server/srv_handle.cpp
*
* -------------------------------------------------------------------------
*/

#include "srv_handle.h"
#include "srv_session_base.h"
#include "cm_defs.h"
#include "../protocol/protocol.h"
#include "srv_def.h"
#include "login/login.h"
#include "compute/sql/include/binder/statement_type.h"
#include "main/connection.h"
#include "srv_instance.h"
#include "srv_interface.h"
#include <unordered_map>
#include <sstream>
#include <string>

#ifndef _WIN32
#include <sys/resource.h>
#include <dirent.h>
#endif


#ifdef __cplusplus
extern "C" {
#endif

struct StatementWrapper {
    std::map<uint32_t, std::unique_ptr<PreparedStatement>> statement_map;
};

const std::unordered_map<uint32_t, std::string> m_rescode_msg = {
    {RES_SUCCESS, "success"},
    {RES_FIELD_ERR, "unpacket error"},
    {RES_CMD_ERR, "operation error"},
    {RES_SERVER_ERR, "server error"},
    {RES_NOT_EXIST, "key not exist"},
    {RES_COMMIT_ERR, "commit error"},
    {RES_DB_ERROR, "db error"},
    {RES_USER_NOT_EXIST, "user not exist"}, 
    {RES_PASSWORLD_ERROR, "incorrect user or password"},
    {RES_NOT_LOGGED, "user not logged"},
    {RES_STMT_EXIST, "prepare statment is exist, please change stmt_id"},
    {RES_STMT_NOT_EXIST, "prepare statment not exist"},
};

std::string get_error_msg(uint32_t rescode) {
    auto item = m_rescode_msg.find(rescode);
    if (item == m_rescode_msg.end())
        return "error";
    else
        return item->second;
}


status_t session_close(session_t *session);
status_t result_close(session_t *session);
status_t prepare_close(session_t *session);

status_t login_handle(session_t *session);
status_t check_tranding_heandle(session_t *session);
status_t prepare_handle(session_t *session);
status_t prepare_update_handle(session_t *session);
status_t prepare_query_handle(session_t *session);
status_t execute_handle(session_t *session);

status_t srv_process_command(session_t *session) {
    session->is_used = GS_TRUE;
    status_t res = GS_SUCCESS;
    uint32_t operation = 0;
    if (ProtoFunc::ReadInt(session->pipe, operation) == GS_ERROR) {
        GS_LOG_RUN_ERR("[SESS] get operation error ");
        kill_sess(session);
        session->is_used = GS_FALSE;
        return GS_ERROR;
    }
    GS_LOG_DEBUG_INF("handle operation:%d", operation);
    switch (operation) {//todo change to map
    case SESSION_CLOSE:
        res = session_close(session);
        break;
    case RESULT_CLOSE:
        res = result_close(session);
        break;
    case SESSION_SET_ID:
        res = login_handle(session);
        break;
    case SESSION_HAS_PENDING_TRANSACTION:
        res = check_tranding_heandle(session);
        break;
    case SESSION_PREPARE_READ_PARAMS:
        res = prepare_handle(session);
        break;
    case COMMAND_EXECUTE_UPDATE:
        res = prepare_update_handle(session);
        break;
    case COMMAND_EXECUTE_QUERY:
        res = prepare_query_handle(session);
        break;
    case COMMAND_CLOSE:
        res = prepare_close(session);
        break;
    case COMMAND_EXECUTE:
        res = execute_handle(session);
        break;
    default:
        break;
    }
    session->is_used = GS_FALSE;
    return res;
}

status_t session_close(session_t *session) {
    SessionCloseReq req;
    SessionCloseRes res;
    res.rescode = RES_SUCCESS;
    res.sendProto(session->pipe);
    GS_LOG_RUN_INF("rescode:%u resmsg:%s", res.rescode, res.res_msg.c_str());
    res.sendProto(session->pipe);
    kill_sess(session);
    return GS_SUCCESS;
}

status_t result_close(session_t *session) {
    ResultCloseReq req;
    ResultCloseRes res;
    res.rescode = RES_SUCCESS;
    do {
        if(req.unpacket(session->pipe) == GS_ERROR) {
            res.rescode = RES_FIELD_ERR;
            break;
        }
    } while (0);
    //todo delete result
    res.sendProto(session->pipe);
    GS_LOG_RUN_INF("rescode:%u resmsg:%s", res.rescode, res.res_msg.c_str());
    return GS_SUCCESS;
}

status_t login_handle(session_t *session) {
    LoginProtoReq req;
    LoginProtoRes res;
    res.rescode = RES_SUCCESS;
    do {
        if(req.unpacket(session->pipe) == GS_ERROR) {
            res.rescode = RES_FIELD_ERR;
            break;
        }
       
        char ipstr_remote[CM_MAX_IP_LEN];
        std::string ip = cm_inet_ntop((struct sockaddr *)&session->pipe->link.tcp.remote.addr, ipstr_remote, CM_MAX_IP_LEN);
        GS_LOG_RUN_INF("session_id:%s max_version:%u max_version:%u dbname:%s user:%s user_passworld:%s ip:%s",
                       req.seq_id.c_str(), req.min_proto_version, req.max_proto_version, req.database_name.c_str(), 
                       req.user_name.c_str(), req.user_passworld.c_str(), ip.c_str());
        
        bool find_db = false;
        for (int i = 0; i < MAX_DB_NUM; i++) {
            if (req.database_name == srv_get_instance()->dbs[i].name) {
                find_db = true;
                char err_msg[GS_BUFLEN_1K];
                uint32_t connection_error_code;
                if (intarkdb_connect_with_user(srv_get_instance()->dbs[i].db, &session->db_conn, req.user_name.c_str(),
                    req.user_passworld.c_str(), ipstr_remote, session->id, &connection_error_code, err_msg) != GS_SUCCESS || session->db_conn == NULL) {
                    if (connection_error_code == ERR_ACCOUNT_AUTH_FAILED) {
                        session->is_logged = GS_FALSE; 
                        GS_LOG_RUN_ERR("db login fail msg:%s", err_msg);
                        res.rescode = RES_PASSWORLD_ERROR;
                    } else {
                        GS_LOG_RUN_ERR("db connect fail");
                        res.rescode = RES_DB_ERROR;
                        res.res_msg = "db connect fail";
                    }
                } else {
                    session->is_logged = GS_TRUE;
                }
                break;
            }
        } 
        if (!find_db) {
            res.rescode = RES_DB_ERROR;
            res.res_msg = "db not find";
            break;
        }

    } while (0);
    if (res.rescode != RES_SUCCESS && res.res_msg.size() == 0) {
        res.res_msg = get_error_msg(res.rescode);
    }
    res.proto_version = PROTO_VERSION;
    res.sendProto(session->pipe);
    GS_LOG_RUN_INF("rescode:%u resmsg:%s", res.rescode, res.res_msg.c_str());
    return GS_SUCCESS;
}

status_t check_tranding_heandle(session_t *session) {
    CheckTransactionReq req;
    CheckTransactionRes res;
    res.hasPendingTransaction = 0;
    res.rescode = RES_SUCCESS;
    do {
        if(req.unpacket(session->pipe) == GS_ERROR) {
            res.rescode = RES_FIELD_ERR;
            break;
        }
    } while (0);
    res.sendProto(session->pipe);
    GS_LOG_RUN_INF("rescode:%u resmsg:%s", res.rescode, res.res_msg.c_str());
    return GS_SUCCESS;

}

status_t prepare_handle(session_t *session) {
    PrepareReq req;
    PrepareRes res;
    res.rescode = RES_SUCCESS;
    do {
        if (!session->is_logged) {
            res.rescode = RES_NOT_LOGGED;
            break;
        }
        if(req.unpacket(session->pipe) == GS_ERROR) {
            res.rescode = RES_FIELD_ERR;
            break;
        }
        GS_LOG_RUN_INF("seq_id:%s sql:%s stmt_id:%u", req.seq_id.c_str(), req.sql.c_str(), req.stmt_id);
        Connection *conn = (Connection *)session->db_conn;
        auto wrapper = (StatementWrapper *)session->stmt;
        if (!wrapper)
        {
            res.rescode = RES_DB_ERROR;
            res.res_msg = "Don't have prepared statement";
        }
        auto item = wrapper->statement_map.find(req.stmt_id);
        if (item != wrapper->statement_map.end()) {
            res.rescode = RES_STMT_EXIST;
            break;
        }
        wrapper->statement_map[req.stmt_id] = conn->Prepare(req.sql);
        if (wrapper->statement_map[req.stmt_id]->HasError()) {
            res.rescode = RES_DB_ERROR;
            res.res_msg = wrapper->statement_map[req.stmt_id]->ErrorMsg();
        }

        res.param_num = wrapper->statement_map[req.stmt_id]->ParamCount();
        res.is_read_only = 0;
        res.is_select = wrapper->statement_map[req.stmt_id]->IsRecordBatchSelect();

    } while (0);
    if (res.rescode != RES_SUCCESS && res.res_msg.size() == 0) {
        res.res_msg = get_error_msg(res.rescode);
    }
    res.sendProto(session->pipe);
    GS_LOG_RUN_INF("rescode:%u resmsg:%s", res.rescode, res.res_msg.c_str());
    return GS_SUCCESS;
}

void write_result(const std::unique_ptr<RecordBatch> &r, ResultStruct& result) {
    result.row_count = r->RowCount();
    const auto &col_header = r->GetSchema().GetColumnInfos();
    result.col_count = col_header.size();
    GS_LOG_DEBUG_INF("cols:%u rows:%lu", result.col_count, result.row_count);
    for (const auto &item : col_header) {
        ColInfoStruct col{};
        col.columnName = item.GetColNameWithoutTableName();
        col.type = item.col_type.TypeId();
        result.col_info.push_back(col);
    }

    for (size_t i = 0; i < r->RowCount(); i++) {
        RowStruct row;
        for (size_t j = 0; j < col_header.size(); ++j) {
            const auto &v = r->Row(i).Field(j);
            row.values.push_back(v);
        }
        result.rows.push_back(row);
    }
}

status_t prepare_update_handle(session_t *session) {
    PrepareUpdateReq req;
    PrepareUpdateRes res;
    res.rescode = RES_SUCCESS;
    do {
        if (!session->is_logged) {
            res.rescode = RES_NOT_LOGGED;
            break;
        }

        if(req.unpacket(session->pipe) == GS_ERROR) {
            res.rescode = RES_FIELD_ERR;
            break;
        }
        GS_LOG_RUN_INF("seq_id:%s stmt_id:%u", req.seq_id.c_str(), req.stmt_id);

        try {

            auto wrapper = (StatementWrapper *)session->stmt;
            if (!wrapper) {
                res.rescode = RES_DB_ERROR;
                res.res_msg = "Don't have prepared statement";
            }
            auto item = wrapper->statement_map.find(req.stmt_id);
            if (item == wrapper->statement_map.end()) {
                res.rescode = RES_STMT_NOT_EXIST;
                break;
            }
            if (!item->second || item->second->HasError()) {
                res.rescode = RES_DB_ERROR;
                res.res_msg = "Don't have prepared statement";
            }
            if (req.key_mode) {
                item->second->SetNeedResultSetEx(true);
            }
            auto r = item->second->Execute(req.parameters);
            
            GS_LOG_DEBUG_INF("ret:%d StmtType:%u", r->GetRetCode(), r->GetStmtType());
            if (r->GetRetCode() != 0) {
                res.rescode = RES_SERVER_ERR;
                res.res_msg = r->GetRetMsg();
                break;
            }
            res.effect_rows = r->GetEffectRow();
           
            if (r->GetRecordBatchType() == RecordBatchType::Select || req.key_mode == 1) {
                res.has_result = true;
                write_result(r, res.result);
            } else {
                res.has_result = false;
            }

        } catch (const std::exception& ex) {
            res.rescode = RES_SERVER_ERR;
            res.res_msg = std::string(ex.what());
        }

    } while (0);
    if (res.rescode != RES_SUCCESS && res.res_msg.size() == 0) {
        res.res_msg = get_error_msg(res.rescode);
    }
    res.sendProto(session->pipe);
    GS_LOG_RUN_INF("rescode:%u resmsg:%s", res.rescode, res.res_msg.c_str());
    return GS_SUCCESS;
}

status_t prepare_query_handle(session_t *session) {
    PrepareQueryReq req;
    PrepareQueryRes res;
    res.rescode = RES_SUCCESS;
    do {
        if (!session->is_logged) {
            res.rescode = RES_NOT_LOGGED;
            break;
        }

        if(req.unpacket(session->pipe) == GS_ERROR) {
            res.rescode = RES_FIELD_ERR;
            break;
        }
        GS_LOG_RUN_INF("seq_id:%s, stmt_id:%u", req.seq_id.c_str(), req.stmt_id);

        try {
            auto wrapper = (StatementWrapper *)session->stmt;
            if (!wrapper) {
                res.rescode = RES_DB_ERROR;
                res.res_msg = "Don't have prepared statement";
            }
            auto item = wrapper->statement_map.find(req.stmt_id);
            if (item == wrapper->statement_map.end()) {
                res.rescode = RES_STMT_NOT_EXIST;
                break;
            }
            if (!item->second || item->second->HasError()) {
                res.rescode = RES_DB_ERROR;
                res.res_msg = "Don't have prepared statement";
            }
            item->second->SetLimitRowsEx(req.limit_rows);
            auto r = item->second->Execute(req.parameters);
            
            GS_LOG_DEBUG_INF("ret:%d StmtType:%u", r->GetRetCode(), r->GetStmtType());
            if (r->GetRetCode() != 0) {
                res.rescode = RES_SERVER_ERR;
                res.res_msg = r->GetRetMsg();
                break;
            }

            res.has_result = true;
            write_result(r, res.result);
        } catch (const std::exception& ex) {
            res.rescode = RES_SERVER_ERR;
            res.res_msg = std::string(ex.what());
        }

    } while (0);
    if (res.rescode != RES_SUCCESS && res.res_msg.size() == 0) {
        res.res_msg = get_error_msg(res.rescode);
    }
    res.sendProto(session->pipe);
    GS_LOG_RUN_INF("rescode:%u resmsg:%s", res.rescode, res.res_msg.c_str());
    return GS_SUCCESS;
}

status_t prepare_close(session_t *session) {
    CommandCloseReq req;
    CommandCloseRes res;
    res.rescode = RES_SUCCESS;
    do
    {
        if (req.unpacket(session->pipe) == GS_ERROR) {
            res.rescode = RES_FIELD_ERR;
            break;
        }
        GS_LOG_RUN_INF("seq_id:%s, stmt_id:%u", req.seq_id.c_str(), req.stmt_id);
        auto wrapper = (StatementWrapper *)session->stmt;
        if (!wrapper) {
            res.rescode = RES_DB_ERROR;
            res.res_msg = "Don't have prepared statement";
        }
        auto item = wrapper->statement_map.find(req.stmt_id);
        if (item == wrapper->statement_map.end()) {
            res.rescode = RES_STMT_NOT_EXIST;
            break;
        }
        wrapper->statement_map.erase(item);
    } while (0);

    res.sendProto(session->pipe);
    GS_LOG_RUN_INF("rescode:%u resmsg:%s", res.rescode, res.res_msg.c_str());
    return GS_SUCCESS;
}

status_t execute_handle(session_t *session) {
    ExecuteProtoReq req;
    ExecuteProtoRes res;
    res.rescode = RES_SUCCESS;
    do {
        if (!session->is_logged) {
            res.rescode = RES_NOT_LOGGED;
            break;
        }
        if(req.unpacket(session->pipe) == GS_ERROR) {
            res.rescode = RES_FIELD_ERR;
            break;
        }
        GS_LOG_RUN_INF("seq_id:%s, sql:%s, key_mode:%u", req.seq_id.c_str(), req.sql.c_str(), req.key_mode);
        try {
            Connection *conn = (Connection *)session->db_conn;
            
            if (req.key_mode) {
                conn->SetNeedResultSetEx(true);
            }
            conn->SetLimitRowsEx(req.limit_rows);
            auto r = conn->Query(req.sql.c_str());
            res.is_read_only = false;
            GS_LOG_DEBUG_INF("ret:%d StmtType:%u", r->GetRetCode(), r->GetStmtType());
            if (r->GetRetCode() != 0) {
                res.rescode = RES_SERVER_ERR;
                res.res_msg = r->GetRetMsg();
                break;
            }
            res.effect_rows = r->GetEffectRow();
            res.is_select = true;
            if (r->GetRecordBatchType() != RecordBatchType::Select) {
                res.is_select = false;
            }
            if (r->GetRecordBatchType() == RecordBatchType::Select || req.key_mode == 1) {
                res.has_result = true;
                write_result(r, res.result);
            } else {
                res.has_result = false;
            }

        } catch (const std::exception& ex) {
            res.rescode = RES_SERVER_ERR;
            res.res_msg = std::string(ex.what());
        }

    } while (0);
    if (res.rescode != RES_SUCCESS && res.res_msg.size() == 0) {
        res.res_msg = get_error_msg(res.rescode);
    }
    res.sendProto(session->pipe);
    GS_LOG_RUN_INF("rescode:%u resmsg:%s write_result:%d", res.rescode, res.res_msg.c_str(), res.has_result);
    return GS_SUCCESS;
}

void set_prepare_stmt(session_t *session) {
    auto wrapper = new StatementWrapper();
    session->stmt = (intarkdb_prepared_statement)wrapper;
}

void delete_prepare_stmt(session_t *session) {
    auto wrapper = (StatementWrapper *)session->stmt;
    if (wrapper)
        delete wrapper;
}


#ifdef __cplusplus
}
#endif
