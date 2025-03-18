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
* protocol.h
*
* IDENTIFICATION
* openGauss-embedded/src/network/protocol/protocol.h
*
* -------------------------------------------------------------------------
*/

#ifndef __NETWORK_PROTOCOL_PROTOCOL_H__
#define __NETWORK_PROTOCOL_PROTOCOL_H__

#include "cs_pipe.h"
#include "srv_def.h"
#include <string>
#include <vector>
#include <sstream>
#include  "compute/sql/include/type/value.h"

#ifdef CLIENT_LOG 
#   include "logger.h"
#else
#   define INFO(format, ...) GS_LOG_RUN_INF(format, ##__VA_ARGS__)
#   define DEBUG(format, ...) GS_LOG_DEBUG_INF(format, ##__VA_ARGS__)
#   define ERROR(format, ...) GS_LOG_RUN_ERR(format, ##__VA_ARGS__)
#   define WARNING(format, ...) GS_LOG_RUN_WAR(format, ##__VA_ARGS__)
#endif




typedef std::variant<uint32_t, uint64_t, int32_t, int64_t, double, float, std::string, dec4_t, hugeint_t, 
        timestamp_stor_t, date_stor_t>  multiple_type;

enum PROTO_OPTION_ID {
    SESSION_PREPARE = 0,
    SESSION_CLOSE = 1,
    COMMAND_EXECUTE_QUERY = 2,
    COMMAND_EXECUTE_UPDATE = 3,
    COMMAND_CLOSE = 4,
    RESULT_FETCH_ROWS = 5,
    RESULT_RESET = 6,
    RESULT_CLOSE = 7,
    COMMAND_COMMIT = 8,
    CHANGE_ID = 9,
    COMMAND_GET_META_DATA = 10,
    SESSION_SET_ID = 12,
    SESSION_CANCEL_STATEMENT = 13,
    SESSION_CHECK_KEY = 14,
    SESSION_SET_AUTOCOMMIT = 15,
    SESSION_HAS_PENDING_TRANSACTION = 16,
    LOB_READ = 17,
    SESSION_PREPARE_READ_PARAMS = 18,
    GET_JDBC_META = 19,
    COMMAND_EXECUTE = 20
};



class ProtoFunc{
public:

    static status_t ReadInt(cs_pipe_t *pipe, uint32_t &value);
    static status_t ReadLong(cs_pipe_t *pipe, uint64_t &value);
    static status_t ReadDouble(cs_pipe_t *pipe, double &value);
    static status_t ReadBool(cs_pipe_t *pipe, bool &value);
    static status_t ReadByte(cs_pipe_t *pipe, unsigned char &value);
    static status_t ReadString(cs_pipe_t *pipe, std::string &value);
    static status_t ReadBytes(cs_pipe_t *pipe, std::string &value);
    static status_t ReadBlob(cs_pipe_t *pipe, Value& value);
    static status_t ReadValueVector(cs_pipe_t *pipe, std::vector<Value> &values, uint32_t num);

    template <typename T>
    static status_t ReadVector(cs_pipe_t *pipe, std::vector<T>& vector_value)
    {
        int size = 0;
        uint32_t vector_size;
        if (ProtoFunc::ReadInt(pipe, vector_size) == GS_ERROR)
        {
            DEBUG("[proto] get vector size err");
            return GS_ERROR;
        }
        GS_RETSUC_IFTRUE(vector_size == 0);
        std::vector<T> tmp_vector;
        using mtype = typename decltype(tmp_vector)::value_type; // todo 完善类型

        for (int i = 0; i < vector_size; i++)
        {
            if (typeid(mtype) == typeid(int32_t) || typeid(mtype) == typeid(uint32_t) ) {
                T value;
                if (ProtoFunc::ReadInt(pipe, (uint32_t&)value) == GS_ERROR)
                {
                    DEBUG("[proto] get vector value err");
                    return GS_ERROR;
                }
                vector_value.push_back(value);
            } else if (typeid(mtype) == typeid(int64_t) || typeid(mtype) == typeid(uint64_t)) {
                T value;
                if (ProtoFunc::ReadLong(pipe, (uint64_t&)value) == GS_ERROR)
                {
                    DEBUG("[proto] get vector value err");
                    return GS_ERROR;
                }
                vector_value.push_back((T)value);
            }

            else if (typeid(mtype) == typeid(std::string))
            {
                T value;
                if (ProtoFunc::ReadString(pipe, (std::string&)value) == GS_ERROR)
                {
                    DEBUG("[proto] get vector value err");
                    return GS_ERROR;
                }
                vector_value.push_back(value);
            }
            else
            {
                DEBUG("[proto] not support type");
            }
        }
    } 

    static void WriteInt(const uint32_t value, std::ostringstream &buf, uint32_t &proto_length);
    static void WriteLong(const uint64_t value, std::ostringstream &buf, uint32_t &proto_length);
    static void WriteDouble(const double value, std::ostringstream &buf, uint32_t &proto_length);
    static void WriteBool(const bool value, std::ostringstream &buf, uint32_t &proto_length);
    static void WriteByte(const unsigned char value, std::ostringstream &buf, uint32_t &proto_length);
    static void WriteString(const std::string &value, std::ostringstream &buf, uint32_t &proto_length);
    static void WriteValueVector(const std::vector<Value> &values, std::ostringstream &buf, uint32_t &length);
    //todo writebytes
};

class PackProto {
public:
    uint32_t length;
public:
    PackProto();
    virtual void packet(std::ostringstream &buf) = 0;
    virtual status_t unpacket(cs_pipe_t *pipe) = 0;
    virtual status_t sendProto(cs_pipe_t *pipe);        
};


class LoginProtoReq: public PackProto {
public:
    uint32_t operation = SESSION_SET_ID;
    std::string seq_id;
    uint32_t min_proto_version;
    uint32_t max_proto_version;
    std::string database_name;
    std::string original_url;
    std::string user_name;
    std::string user_passworld;
    std::string file_passworld;
    std::vector<std::string> keys;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class LoginProtoRes: public PackProto {
public:
    LoginProtoRes();
    uint32_t rescode = RES_SUCCESS;
    std::string res_msg;
    uint32_t proto_version = 1;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class ColInfoStruct:public PackProto {
public:
    std::string alias;
    std::string schemaName;
    std::string tableName;
    std::string columnName;
    uint32_t type = 0;
    bool is_idenity = false;
    bool is_nullable = true;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class RowStruct: public PackProto {
private:
    uint32_t count;
public:
    bool isStart = 1; // 强制为1，标识行开始
    std::vector<Value> values;

    RowStruct();
    void setCount(uint32_t col_count);
    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class ResultStruct: public PackProto {
public:
    uint64_t row_count = 0;
    uint32_t col_count = 0;
    std::vector<ColInfoStruct> col_info;
    std::vector<RowStruct> rows;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class ExecuteProtoReq: public PackProto { 
public:
    uint32_t operation = COMMAND_EXECUTE;
    std::string seq_id;
    uint32_t result_id = 0;
    std::string sql;
    uint32_t key_mode = 0;
    uint32_t limit_rows = 0;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class ExecuteProtoRes: public PackProto {
public:
    uint32_t rescode = RES_SUCCESS;
    std::string res_msg;
    bool is_select = true;
    bool is_read_only = false;
    uint64_t effect_rows = 0;
    bool has_result = false;
    ResultStruct result;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class PrepareUpdateReq: public PackProto { 
public:
    uint32_t operation = COMMAND_EXECUTE_UPDATE;
    std::string seq_id;
    uint32_t stmt_id = 0;
    uint32_t result_id = 0;
    uint32_t parameters_size = 0;
    std::vector<Value> parameters;
    uint32_t key_mode = 0;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class PrepareUpdateRes: public PackProto {
public:
    uint32_t rescode = RES_SUCCESS;
    std::string res_msg;
    uint64_t effect_rows = 0;
    bool has_result = false;
    ResultStruct result;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class PrepareQueryReq: public PackProto { 
public:
    uint32_t operation = COMMAND_EXECUTE_QUERY;
    std::string seq_id;
    uint32_t stmt_id = 0;
    uint32_t result_id = 0;
    uint32_t limit_rows = 0;
    uint32_t parameters_size = 0;
    std::vector<Value> parameters;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class PrepareQueryRes: public PackProto {
public:
    uint32_t rescode = RES_SUCCESS;
    std::string res_msg;
    bool has_result =false;
    ResultStruct result;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class SessionCloseReq: public PackProto { 
public:
    uint32_t operation = SESSION_CLOSE;
    std::string seq_id;
    uint32_t stmt_id = 0;   

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class SessionCloseRes: public PackProto {
public:
    uint32_t rescode = RES_SUCCESS;
    std::string res_msg;
    
    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class ResultCloseReq: public PackProto { 
public:
    uint32_t operation = RESULT_CLOSE;
    std::string seq_id;
    uint32_t result_id = 0;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class ResultCloseRes: public PackProto {
public:
    uint32_t rescode = RES_SUCCESS;
    std::string res_msg;
    
    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class CommandCloseReq: public PackProto { 
public:
    uint32_t operation = COMMAND_CLOSE;
    std::string seq_id;
    uint32_t stmt_id;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class CommandCloseRes: public PackProto {
public:
    uint32_t rescode = RES_SUCCESS;
    std::string res_msg;
    
    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class CheckTransactionReq: public PackProto { 
public:
    uint32_t operation = SESSION_HAS_PENDING_TRANSACTION;
    std::string seq_id;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class CheckTransactionRes: public PackProto {
public:
    uint32_t rescode = RES_SUCCESS;
    std::string res_msg;
    uint32_t hasPendingTransaction = 0;
    
    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class PrepareReq: public PackProto { 
public:
    uint32_t operation = SESSION_PREPARE_READ_PARAMS;
    std::string seq_id;
    uint32_t stmt_id = 0;
    std::string sql;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class PrepareParamInfo: public PackProto {
public:
    uint32_t param_type = 0;
    bool is_nullable = false;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

class PrepareRes: public PackProto {
public:
    uint32_t rescode = RES_SUCCESS;
    std::string res_msg;
    bool is_select = false;
    bool is_read_only = false;
    uint32_t param_num = 0;

    void packet(std::ostringstream &buf);
    status_t unpacket(cs_pipe_t *pipe);
};

#endif // __NETWORK_PROTOCOL_PROTOCOL_H__
