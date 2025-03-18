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
 * catalog.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/catalog/catalog.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "catalog/catalog.h"

#include "storage/gstor/zekernel/kernel/common/knl_context.h"
#include "storage/gstor/zekernel/kernel/common/knl_session.h"

std::mutex Catalog::dc_mutex_;

std::unique_ptr<TableDataSource> Catalog::CreateTableDataSource(std::unique_ptr<BoundBaseTable> table_ref,
                                                                scan_action_t action, size_t table_idx) const {
    auto user = table_ref->GetSchema();
    auto source = std::make_unique<TableDataSource>(handle_, std::move(table_ref), action, table_idx);
    source->SetUser(user);
    return source;
}

uint32_t Catalog::GetUserID() const {
    return GetUserID(user_);
}

uint32_t Catalog::GetUserID(const std::string& user_name) const {
    uint32_t user_id = 0;
    if (gstor_get_user_id(((db_handle_t *)handle_)->handle, (char *)user_name.c_str(), &user_id) != GS_TRUE) {
        throw std::runtime_error(fmt::format("user {} not found", user_name));
    }
    return user_id;
}

std::string Catalog::GetUserName(uint32_t uid) const {
    knl_session_t *session = EC_SESSION(GetStorageHandle()->handle);

    text_t user_name;
    if (knl_get_user_name(session, uid, &user_name) != GS_SUCCESS) {
        throw std::runtime_error(fmt::format("uid {} not found", uid));
    }

    if (user_name.str) {
        return std::string(user_name.str);
    } else {
        throw std::runtime_error(fmt::format("uid {} can't get user name", uid));
    }
}

bool32 Catalog::IsRole(const std::string& role_name) const {
    uint32_t role_id = 0;
    return gstor_get_role_id(((db_handle_t *)handle_)->handle, (char *)role_name.c_str(), &role_id);
}

uint32_t Catalog::GetRoleID(const std::string& role_name) const {
    uint32_t role_id = 0;
    if (gstor_get_user_id(((db_handle_t *)handle_)->handle, (char *)role_name.c_str(), &role_id) != GS_TRUE) {
        throw std::runtime_error(fmt::format("role {} not found", role_name));
    }
    return role_id;
}

bool32 Catalog::CheckSysPrivilege(uint32 priv_id) const {
    knl_session_t *session = EC_SESSION(GetStorageHandle()->handle);

    text_t curr_user = { (char *)user_.c_str(), static_cast<uint32_t>(user_.length()) };
    if (knl_check_sys_priv_by_name(session, &curr_user, priv_id)) {
        return GS_TRUE;
    }

    return GS_FALSE;
}

bool32 Catalog::CheckPrivilege(const std::string& objuser, const std::string& objname, object_type_t objtype, uint32 priv_id) const {
    if (objuser == user_) {
        return GS_TRUE;
    }

    knl_session_t *session = EC_SESSION(GetStorageHandle()->handle);

    text_t curr_user = { (char *)user_.c_str(), static_cast<uint32_t>(user_.length()) };
    text_t obj_user = { (char *)objuser.c_str(), static_cast<uint32_t>(objuser.length()) };
    text_t obj_name = { (char *)objname.c_str(), static_cast<uint32_t>(objname.length()) };

    if (knl_check_user_priv_by_name(session, &curr_user, &obj_user, priv_id)) {
        return GS_TRUE;
    }

    if (knl_check_obj_priv_by_name(session, &curr_user, &obj_user, &obj_name, objtype, priv_id)) {
        return GS_TRUE;
    }

    return GS_FALSE;
}

int Catalog::CreateTable(const std::string &table_name, const std::vector<Column>& columns, const CreateStatement &stmt) {
    error_info_t err_info = {0};
    exp_attr_def_t attr = {0};
    attr.is_timescale = stmt.Timescale();
    attr.parted = stmt.Partition();
    attr.retention = stmt.Retention().length() == 0 ? NULL : (char *)stmt.Retention().c_str();
    attr.part_def = const_cast<exp_part_obj_def_t *>(&stmt.GetPartObjDef());
    attr.comment = (char *)stmt.Comment().c_str();

    auto column_defs = Column::TransformColumnVecToDefs(columns);

    if (GS_SUCCESS != sqlapi_gstor_create_user_table(
                          ((db_handle_t *)handle_)->handle, user_.c_str(), table_name.c_str(), column_defs.size(),
                          column_defs.data(), stmt.GetIndexs().size(), stmt.GetIndexDefs().data(),
                          stmt.GetConstraints().size(), stmt.GetConstraintDefs().data(), attr, &err_info)) {
#ifdef ENABLE_PG_QUERY
        if (ERR_OBJECT_EXISTS == err_info.code &&
            stmt.GetConflictOpt() == duckdb_libpgquery::PGOnCreateConflict::PG_IGNORE_ON_CONFLICT) {
            return GS_IGNORE_OBJECT_EXISTS;
        }
#endif
        GS_LOG_RUN_ERR("sqlapi_gstor_create_user_table failed, code:%d, msg:%s", err_info.code, err_info.message);
        throw std::runtime_error(fmt::format(err_info.message));
    }
    return GS_SUCCESS;
}

/**
 * Query table metadata by name.
 * @param table_name The name of the table
 */
auto Catalog::GetTable(const std::string &schema_name, const std::string &table_name) const -> std::unique_ptr<TableInfo> {
    // 获取表信息
    // lock for DC
    if (need_lock_dc_) {
        std::lock_guard<std::mutex> lock(Catalog::dc_mutex_);
        auto table_meta = std::make_unique<exp_table_meta>();
        error_info_t err_info;
        auto ret =
            gstor_get_table_info(((db_handle_t *)handle_)->handle, schema_name.c_str(), table_name.c_str(),
                                table_meta.get(), &err_info);
        if (ret != GS_SUCCESS) {
            return nullptr;
        }
        return std::make_unique<TableInfo>(std::move(table_meta));
    } else {
        auto table_meta = std::make_unique<exp_table_meta>();
        error_info_t err_info;
        auto ret =
            gstor_get_table_info(((db_handle_t *)handle_)->handle, schema_name.c_str(), table_name.c_str(),
                                table_meta.get(), &err_info);
        if (ret != GS_SUCCESS) {
            return nullptr;
        }
        return std::make_unique<TableInfo>(std::move(table_meta));
    }
}

int Catalog::CreateIndex(const std::string &table_name, const CreateIndexStatement &stmt) {
    error_info_t err_info = {0};
    if (GS_SUCCESS != sqlapi_gstor_create_index(((db_handle_t *)handle_)->handle, user_.c_str(), table_name.c_str(),
                                                &stmt.GetIndexRef().GetIndex(), &err_info)) {
#ifdef ENABLE_PG_QUERY
        if (ERR_OBJECT_EXISTS == err_info.code &&
            stmt.GetConflictOpt() == duckdb_libpgquery::PGOnCreateConflict::PG_IGNORE_ON_CONFLICT) {
            return GS_SUCCESS;
        }
#endif
        GS_LOG_RUN_ERR("sqlapi_gstor_create_index failed, code:%d, msg:%s", err_info.code, err_info.message);
        throw std::runtime_error(fmt::format(err_info.message));
    }
    return GS_SUCCESS;
}

int Catalog::AlterTable(const std::string &table_name, const AlterStatement &stmt) {
    error_info_t err_info = {0};
    if (GS_SUCCESS != sqlapi_gstor_alter_table(((db_handle_t *)handle_)->handle, stmt.schema_name_.c_str(),
                        table_name.c_str(), &stmt.GetAlterTableInfoDefs(), &err_info)) {
        GS_LOG_RUN_ERR("sqlapi_gstor_alter_table failed, code:%d, msg:%s", err_info.code, err_info.message);
        throw std::runtime_error(fmt::format(err_info.message));
    }
    return GS_SUCCESS;
}

int Catalog::CreateSequence(const CreateSequenceStatement &stmt) {
    sequence_def_t sequence_info = {0};
    sequence_info.name = (char *)stmt.name.c_str();
    sequence_info.increment = stmt.increment;
    sequence_info.min_value = stmt.min_value;
    sequence_info.max_value = stmt.max_value;
    sequence_info.start_value = stmt.start_value;
    sequence_info.is_cycle = stmt.cycle;

    if (GS_SUCCESS != gstor_create_sequence(((db_handle_t *)handle_)->handle,
                                            stmt.schema_name.c_str(), &sequence_info)) {
        int32_t err_code;
        const char *message = nullptr;
        cm_get_error(&err_code, &message, NULL);
        std::string err_msg = fmt::format("create sequence fail!! errcode = {}, message = {}", err_code, message);
        GS_LOG_RUN_WAR("%s", err_msg.c_str());

        if (stmt.ignore_conflict && err_code == ERR_DUPLICATE_TABLE) {
            cm_reset_error();
            return GS_SUCCESS;
        }
        cm_reset_error();
        throw std::runtime_error(err_msg);
    }
    return GS_SUCCESS;
}

int64_t Catalog::GetSequenceCurrVal(const std::string &seq_name) const {
    int64_t value;
    if (GS_SUCCESS != gstor_seq_currval(((db_handle_t *)handle_)->handle, user_.c_str(), seq_name.c_str(), &value)) {
        GS_LOG_RUN_ERR("gstor_seq_currval failed.");
        throw std::runtime_error(fmt::format("gstor_seq_currval {} failed.", seq_name));
    }
    return value;
}

int64_t Catalog::GetSequenceNextVal(const std::string &seq_name) const {
    int64_t value;
    if (GS_SUCCESS != gstor_seq_nextval(((db_handle_t *)handle_)->handle, user_.c_str(), seq_name.c_str(), &value)) {
        GS_LOG_RUN_ERR("gstor_seq_nextval failed.");
        throw std::runtime_error(fmt::format("gstor_seq_nextval {} failed.", seq_name));
    }
    return value;
}

auto Catalog::IsSequenceExists(const std::string &seq_name) const -> bool {
    bool32 is_exist = 0;
    if (GS_SUCCESS != gstor_is_sequence_exist(((db_handle_t *)handle_)->handle, user_.c_str(), seq_name.c_str(), &is_exist)) {
        GS_LOG_RUN_ERR("gstor_is_sequence_exists failed.");
        throw std::runtime_error(fmt::format("gstor_is_sequence_exists {} failed.", seq_name));
    }
    return is_exist == 1;
}

int Catalog::CreateView(const std::string &viewName, const std::vector<Column>& columns, const std::string &query,
                        bool ignoreConflict) {
    auto metaInfo = GetTable(user_, viewName);
    if (metaInfo != nullptr) {
        if (ignoreConflict) {
            return GS_SUCCESS;
        } else {
            return GS_ERROR;
        }
    }

    exp_view_def_t view_def;
    view_def.uid = GS_INVALID_ID32;
    view_def.name = {(char *)viewName.data(), (uint32)viewName.length()};
    view_def.user = {(char *)user_.c_str(), (uint32)user_.length()};
    view_def.columns = {};
    view_def.sub_sql = {(char *)query.data(), (uint32)query.length()};
    view_def.sql_tpye = SQL_STYLE_GS;
    view_def.is_replace = false;
    view_def.ref_objects = nullptr;
    view_def.select = nullptr;

    std::vector<exp_column_def_t> all_col_def = Column::TransformColumnVecToDefs(columns);
    auto ret =
        gstor_create_user_view(((db_handle_t *)handle_)->handle, &view_def, all_col_def.size(), all_col_def.data());
    return ret;
}

int Catalog::forceCheckpoint() { return gstor_force_checkpoint(((db_handle_t *)handle_)->handle); }

status_t Catalog::CommentOn(exp_comment_def_t *def) {
    auto ret = gstor_comment_on(((db_handle_t *)handle_)->handle, def);
    if (ret != GS_SUCCESS) {
        throw std::runtime_error("fail to comment_on!");
    }
    return ret;
}
