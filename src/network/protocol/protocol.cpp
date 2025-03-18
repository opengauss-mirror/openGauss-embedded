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
* protocol.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/network/protocol/protocol.cpp
*
* -------------------------------------------------------------------------
*/

#include "protocol.h"
#include <set>
#include <typeinfo>
#include <sstream>
#include <iostream>

const static uint32_t PROTO_MAX_STR_LEN = 64 * 1024 * 1024;


#ifndef WIN32
uint64_t htonll(uint64_t val) {
    return (((uint64_t)htonl(val)) << 32) + htonl(val >> 32);
}

uint64_t ntohll(uint64_t val) {
    return (((uint64_t)ntohl(val)) << 32) + ntohl(val >> 32);
}
#endif // WIN32

union DoubleToLong {
    double d;
    uint64_t l;
};

uint64_t doubleToLong(double d) {
    DoubleToLong converter;
    converter.d = d;
    return converter.l;
}

double longTodouble(uint64_t l) {
    DoubleToLong converter;
    converter.l = l;
    return converter.d;
}

status_t ProtoFunc::ReadInt(cs_pipe_t *pipe, uint32_t &value) {
    uint32_t length = sizeof(uint32_t);
    char *buf = new char[length];
    int size = 0;
    if (cs_read_stream(pipe, buf, SVR_TIME_OUT, length, &size) == GS_ERROR || size != length) {
        ERROR("[proto] socket read err");
        delete[] buf;
        return GS_ERROR;
    }
    memcpy(&value, buf, length);
    value = ntohl(value);
    DEBUG("[proto] value:%u", value); // debug
    delete[] buf;
    return GS_SUCCESS;
}

status_t ProtoFunc::ReadLong(cs_pipe_t *pipe, uint64_t &value) {
    uint32_t length = sizeof(uint64_t);
    char *buf = new char[length];
    int size = 0; 
    if (cs_read_stream(pipe, buf, SVR_TIME_OUT, length, &size) == GS_ERROR || size != length) {
        ERROR("[proto] socket read err");
        delete[] buf;
        return GS_ERROR;
    }
    memcpy(&value, buf, length);
    value = ntohll(value);
    DEBUG("[proto] value:%llu", value); // debug
    delete[] buf;
    return GS_SUCCESS;
}

status_t ProtoFunc::ReadDouble(cs_pipe_t *pipe, double &value) {
    uint64_t tmp;
    status_t result = ReadLong(pipe, tmp);
    value = longTodouble(tmp);
    DEBUG("[proto] value:%lf", value); // debug
    return result;
}

status_t ProtoFunc::ReadBool(cs_pipe_t *pipe, bool &value) {
    uint32_t length = 1;
    char buf[1] = {0};
    int size = 0;
    if (cs_read_stream(pipe, buf, SVR_TIME_OUT, length, &size) == GS_ERROR || size != length) {
        ERROR("[proto] socket read err");
        return GS_ERROR;
    }
    uint8_t tmp;
    memcpy(&tmp, buf, length);
    value = tmp != 0 ? true: false;
    DEBUG("[proto] value:%u", value); // debug
    return GS_SUCCESS;
}

status_t ProtoFunc::ReadByte(cs_pipe_t *pipe, unsigned char &value) {
    uint32_t length = 1;
    char buf[1] = {0};
    int size = 0;
    if (cs_read_stream(pipe, buf, SVR_TIME_OUT, length, &size) == GS_ERROR || size != length) {
        ERROR("[proto] socket read err");
        return GS_ERROR;
    }
    memcpy(&value, buf, length);
    DEBUG("[proto] value:%u", value); // debug
    return GS_SUCCESS;
}

status_t ProtoFunc::ReadBlob(cs_pipe_t *pipe, Value& value) {
    status_t resCode = GS_SUCCESS;
    int32_t size = 0;
    uint32_t value_size;
    do {
        if (ProtoFunc::ReadInt(pipe, value_size) == GS_ERROR) {
            DEBUG("[proto] get string size err");
            resCode = GS_ERROR;
            break;
        }
        if ((int32_t)value_size == -1 || value_size == 0) {
            break;
        } /* else if (value_size > PROTO_MAX_STR_LEN) {
            DEBUG("[proto] string over size");
            resCode = GS_ERROR;
            break;
        }//todo  等有整个协议长度后再限制 */
        
        char* buf = new char[value_size];
        buf[0] = '\0';
        if (cs_read_stream(pipe, buf, SVR_TIME_OUT, value_size, &size) == GS_ERROR || size != value_size) {
            ERROR("[proto] socket read err");
            resCode = GS_ERROR;
            delete[] buf;
            break;
        }
        DEBUG("[proto] size:%d - %d", value_size, size); //debug
        char *hex_str = new char[value_size * 2 +1]{};
        for (int i = 0; i < value_size; i++) {
            sprintf(&hex_str[i * 2], "%02x",buf[i] & 0xFF);
        }
        hex_str[value_size * 2] = '\0';
        value = ValueFactory::ValueBlob((uint8_t *)buf, value_size);
        delete[] buf;
        DEBUG("[proto] blob hex:%s", hex_str); //debug
        delete[] hex_str;
    } while (0);
    return resCode;
}

status_t ProtoFunc::ReadString(cs_pipe_t *pipe, std::string &value) {
    status_t resCode = GS_SUCCESS;
    int32_t size = 0;
    uint32_t value_size;
    do {
        if (ProtoFunc::ReadInt(pipe, value_size) == GS_ERROR) {
            ERROR("[proto] get string size err");
            resCode = GS_ERROR;
            break;
        }
        if ((int32_t)value_size == -1 || value_size == 0) {
            value = "";
            break;
        } /* else if (value_size > PROTO_MAX_STR_LEN) {
            DEBUG("[proto] string over size");
            resCode = GS_ERROR;
            break;
        }//todo  等有整个协议长度后再限制 */
        
        char* buf = new char[value_size];
        buf[0] = '\0';
        DEBUG("[proto] size:%d", value_size); //debug
        if (cs_read_stream(pipe, buf, SVR_TIME_OUT, value_size, &size) == GS_ERROR || size != value_size) {
            ERROR("[proto] socket read err");
            resCode = GS_ERROR;
            delete[] buf;
            break;
        }
        value.assign(buf, 0, value_size);
        delete[] buf;
        DEBUG("[proto] get string:%s len:%d suc", value.c_str(), value.size()); // debug
    } while (0);
    return resCode;
}

status_t ProtoFunc::ReadValueVector(cs_pipe_t *pipe, std::vector<Value> &values, uint32_t num) {
    status_t resCode = GS_SUCCESS;
    uint32_t value_type = GS_TYPE_NULL;
    for (uint32_t i = 0; i< num; i++) {
        if (ProtoFunc::ReadInt(pipe, value_type) == GS_ERROR) {
            DEBUG("[proto] get value type err");
            resCode = GS_ERROR;
            break;
        }
        switch (value_type) {
            case GS_TYPE_NULL:
                values.push_back(ValueFactory::ValueNull());
                break;
            case GStorDataType::GS_TYPE_UTINYINT: {
                uint8_t tmp = 0;
                resCode = ProtoFunc::ReadByte(pipe, (unsigned char &)tmp);
                values.push_back(ValueFactory::ValueUnsignTinyInt(tmp));
                break;
            }
            case GStorDataType::GS_TYPE_TINYINT: {
                int8_t tmp = 0;
                resCode = ProtoFunc::ReadByte(pipe, (unsigned char &)tmp);
                values.push_back(ValueFactory::ValueTinyInt(tmp));
                break;
            }
            case GStorDataType::GS_TYPE_UINT32:
            case GStorDataType::GS_TYPE_USMALLINT:{
                uint32_t tmp = 0;
                resCode = ProtoFunc::ReadInt(pipe, tmp);
                values.push_back(ValueFactory::ValueUnsignInt(tmp));
                break;
            }
            case GStorDataType::GS_TYPE_INTEGER:
            case GStorDataType::GS_TYPE_SMALLINT: {
                int32_t tmp = 0;
                resCode = ProtoFunc::ReadInt(pipe, (uint32_t &)tmp);
                values.push_back(ValueFactory::ValueInt(tmp));
                break;
            }
            case GStorDataType::GS_TYPE_UINT64: {
                uint64_t tmp = 0;
                resCode = ProtoFunc::ReadLong(pipe, tmp);
                values.push_back(ValueFactory::ValueUnsignBigInt(tmp));
                break;

            }
            case GStorDataType::GS_TYPE_BIGINT: {
                int64_t tmp = 0;
                resCode = ProtoFunc::ReadLong(pipe, (uint64_t &)tmp);
                values.push_back(ValueFactory::ValueBigInt(tmp));
                break;

            }
            case GStorDataType::GS_TYPE_REAL:
            case GStorDataType::GS_TYPE_FLOAT: {
                double tmp = 0;
                resCode = ProtoFunc::ReadDouble(pipe, tmp);
                values.push_back(ValueFactory::ValueDouble(tmp));
                break;
            }
            case GStorDataType::GS_TYPE_BOOLEAN: {
                bool tmp = false;
                resCode = ProtoFunc::ReadBool(pipe, tmp);
                values.push_back(ValueFactory::ValueBool(tmp));
                break;
            }
            case GStorDataType::GS_TYPE_BLOB:{
                Value value;
                resCode = ProtoFunc::ReadBlob(pipe, value);
                values.push_back(value);
                break;
            }
            case GStorDataType::GS_TYPE_RAW:
            case GStorDataType::GS_TYPE_CLOB:
            case GStorDataType::GS_TYPE_STRING:
            case GStorDataType::GS_TYPE_CHAR:
            case GStorDataType::GS_TYPE_VARCHAR: {
                std::string tmp;
                resCode = ProtoFunc::ReadString(pipe, tmp);
                values.push_back(ValueFactory::ValueVarchar(tmp));
                break;
            }
            case GStorDataType::GS_TYPE_DATE: {
                std::string tmp;
                resCode = ProtoFunc::ReadString(pipe, tmp);
                values.push_back(ValueFactory::ValueDate(tmp.c_str()));
                break;
            }
            case GStorDataType::GS_TYPE_TIMESTAMP: {
                std::string tmp;
                resCode = ProtoFunc::ReadString(pipe, tmp);
                values.push_back(ValueFactory::ValueTimeStamp(tmp.c_str()));
                
                break;
            }
            case GStorDataType::GS_TYPE_DECIMAL:
            case GStorDataType::GS_TYPE_NUMBER:{
                std::string tmp;
                dec4_t num;
                resCode = ProtoFunc::ReadString(pipe, tmp);
                if (TryCast::Operation<std::string, dec4_t>(tmp, num)) {
                    values.push_back(ValueFactory::ValueDecimal(num));
                } else {
                    resCode = GS_ERROR;
                }
                break;
            }
            default:{
                resCode = GS_ERROR;
                ERROR("unknow type:%u", value_type);
            }
        }
        if (resCode != GS_SUCCESS) {
            ERROR("[proto] %s i:%u get value error", __func__, i); // debug
            break;
        }

        DEBUG("[proto] %s i:%u value:%s suc", __func__, i, values[i].ToString().c_str()); // debug
            
    }
    return resCode;
}

status_t ProtoFunc::ReadBytes(cs_pipe_t *pipe, std::string &value) {
    status_t resCode = GS_SUCCESS;
    int32_t size = 0;
    uint32_t value_size;
    do {
        if (ProtoFunc::ReadInt(pipe, value_size) == GS_ERROR) {
            ERROR("[proto] get string size err");
            resCode = GS_ERROR;
            break;
        }
        if ((int32_t)value_size == -1 || value_size == 0) {
            value = "";
            break;
        } /* else if (value_size > PROTO_MAX_STR_LEN) {
            ERROR("[proto] string over size");
            resCode = GS_ERROR;
            break;
        }  //todo  等有整个协议长度后再限制*/
        DEBUG("[proto] size:%d", value_size); //debug
        char* buf = new char[value_size];
        buf[0] = '\0';
        if (cs_read_stream(pipe, buf, SVR_TIME_OUT, value_size, &size) == GS_ERROR || size != value_size) {
            ERROR("[proto] socket read err");
            resCode = GS_ERROR;
            delete[] buf;
            break;
        }
        char* str_dgst = new char[value_size * 2 + 1];
        for (int i = 0; i < value_size; i++) {
            sprintf(&str_dgst[i * 2], "%02x", buf[i] & 0xFF);
        }
        value.assign(str_dgst, 0, value_size * 2);
        delete[] str_dgst;
        delete[] buf;
        DEBUG("[proto] get string:%s len:%d suc", value.c_str(), value.size()); // debug
    } while (0);
    return resCode;
}

void ProtoFunc::WriteInt(const uint32_t value, std::ostringstream &buf, uint32_t &proto_length) {
    uint32_t length = sizeof(uint32_t);
    proto_length += length;
    uint32_t tmp = htonl(value);
    DEBUG("[proto] WriteInt %u", value); 
    buf.write((char *)&tmp, length);
}

void ProtoFunc::WriteLong(const uint64_t value, std::ostringstream &buf, uint32_t &proto_length) {
    uint32_t length = sizeof(uint64_t);
    proto_length += length;
    uint64_t tmp = htonll(value);
    DEBUG("[proto] WriteLong %llu", value);
    buf.write((char *)&tmp, length);
}

void ProtoFunc::WriteDouble(const double value, std::ostringstream &buf, uint32_t &proto_length) {
    uint64_t v = doubleToLong(value);
    WriteLong(v, buf, proto_length);
}

void ProtoFunc::WriteBool(const bool value, std::ostringstream &buf, uint32_t &proto_length) {
    uint32_t length = 1;
    proto_length += length;
    unsigned char tmp = value;
    buf.write((char *)&tmp, length);
}

void ProtoFunc::WriteByte(const unsigned char value, std::ostringstream &buf, uint32_t &proto_length) {
    uint32_t length = 1;
    proto_length += length;
    unsigned char tmp = value;
    buf.write((char *)&tmp, length);
}


void ProtoFunc::WriteString(const std::string &value, std::ostringstream &buf, uint32_t &proto_length) {
    uint32_t value_length = value.size();
    WriteInt(value_length, buf, proto_length);
    proto_length += value_length;
    char *hex_str = new char[value_length * 2 + 1]();
    for (int i = 0; i < value_length; i++) {
        sprintf(&hex_str[i * 2], "%02x", value.c_str()[i] & 0xFF);
    }
    hex_str[value_length * 2] = '\0';
    DEBUG("[proto] WriteString:%s hex:%s  len:%u ", value.c_str(), hex_str, value_length);
    delete[] hex_str;
    buf.write(value.c_str(), value_length);
}

void ProtoFunc::WriteValueVector(const std::vector<Value> &values, std::ostringstream &buf, uint32_t &length) {
    for(const auto &item : values) {
        if (item.IsNull()) {
            ProtoFunc::WriteInt(GS_TYPE_NULL, buf, length);
            DEBUG("value: Null");
        } else {
            ProtoFunc::WriteInt(item.GetType(), buf, length);
            switch (item.GetType())
            {
                case GStorDataType::GS_TYPE_UTINYINT:
                {
                    ProtoFunc::WriteByte(item.GetCastAs<uint8_t>(), buf, length);
                    DEBUG("value: %u", item.GetCastAs<uint32_t>());
                    break;
                }
                case GStorDataType::GS_TYPE_TINYINT:
                {
                    ProtoFunc::WriteByte(item.GetCastAs<int8_t>(), buf, length);
                    DEBUG("value: %d", item.GetCastAs<int32_t>());
                    break;
                }
                
                case GStorDataType::GS_TYPE_INTEGER:
                case GStorDataType::GS_TYPE_SMALLINT:
                {
                    ProtoFunc::WriteInt(item.GetCastAs<int32_t>(), buf, length);
                    DEBUG("value: %d", item.GetCastAs<int32_t>());
                    break;
                }
                case GStorDataType::GS_TYPE_UINT32:
                case GStorDataType::GS_TYPE_USMALLINT: {
                    ProtoFunc::WriteInt(item.GetCastAs<uint32_t>(), buf, length);
                    DEBUG("value: %u", item.GetCastAs<uint32_t>());
                    break;
                }

                case GStorDataType::GS_TYPE_UINT64:
                {
                    ProtoFunc::WriteLong(item.GetCastAs<uint64_t>(), buf, length);
                    DEBUG("value: %llu", item.GetCastAs<uint64_t>());
                    break;
                }
                case GStorDataType::GS_TYPE_BIGINT:
                {
                    ProtoFunc::WriteLong(item.GetCastAs<int64_t>(), buf, length);
                    DEBUG("value: %lld", item.GetCastAs<int64_t>());
                    break;
                }
                case GStorDataType::GS_TYPE_REAL:
                case GStorDataType::GS_TYPE_FLOAT: {
                    ProtoFunc::WriteDouble(item.GetCastAs<double>(), buf, length);
                    DEBUG("value: %lf", item.GetCastAs<double>());
                    break;
                }
                case GStorDataType::GS_TYPE_BOOLEAN: {
                    ProtoFunc::WriteBool(item.GetCastAs<bool>(), buf, length);
                    DEBUG("value: %d", item.GetCastAs<bool>());
                    break;
                }
                default:
                    // 转string
                    std::string tmp= item.ToString();
                    DEBUG("value: %s", tmp.c_str());
                    ProtoFunc::WriteString(tmp, buf, length);
                    break;
            } 
            
        }
    }
}

PackProto::PackProto():length(0){}

status_t PackProto::sendProto(cs_pipe_t *pipe) {
    std::ostringstream buf;
    packet(buf);
    return cs_write_stream(pipe, buf.str().c_str(), length, MAX_SEND_SIZE);
}

void  LoginProtoReq::packet(std::ostringstream &buf) {
    ProtoFunc::WriteInt(operation, buf, length);
    ProtoFunc::WriteString(seq_id, buf, length);
    ProtoFunc::WriteInt(min_proto_version, buf, length);
    ProtoFunc::WriteInt(max_proto_version, buf, length);
    ProtoFunc::WriteString(database_name, buf, length);
    ProtoFunc::WriteString(original_url, buf, length);
    ProtoFunc::WriteString(user_name, buf, length);
    ProtoFunc::WriteString(user_passworld, buf, length);
    ProtoFunc::WriteString(file_passworld, buf, length);
    ProtoFunc::WriteInt(keys.size(), buf, length);
    for (const auto &item: keys) {
        ProtoFunc::WriteString(item, buf, length);
    }
}

status_t LoginProtoReq::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, seq_id));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, min_proto_version));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, max_proto_version));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, database_name));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, original_url));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, user_name));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, user_passworld));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, file_passworld));
    GS_RETURN_IFERR(ProtoFunc::ReadVector(pipe, keys));
    return GS_SUCCESS;
}

LoginProtoRes::LoginProtoRes():proto_version(PROTO_VERSION) {}

status_t LoginProtoRes::unpacket(cs_pipe_t *pipe){
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, rescode));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, res_msg));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, proto_version));
    return GS_SUCCESS;
}

void  LoginProtoRes::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(rescode, buf, length);
    ProtoFunc::WriteString(res_msg, buf, length);
    ProtoFunc::WriteInt(proto_version, buf, length);
}

void ColInfoStruct::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteString(alias, buf, length);
    ProtoFunc::WriteString(schemaName, buf, length);
    ProtoFunc::WriteString(tableName, buf, length);
    ProtoFunc::WriteString(columnName, buf, length);
    ProtoFunc::WriteInt(type, buf, length);
    ProtoFunc::WriteBool(is_idenity, buf, length);
    ProtoFunc::WriteBool(is_nullable, buf, length);
}

status_t ColInfoStruct::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, alias));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, schemaName));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, tableName));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, columnName));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, type));
    GS_RETURN_IFERR(ProtoFunc::ReadBool(pipe, is_idenity));
    GS_RETURN_IFERR(ProtoFunc::ReadBool(pipe, is_nullable));
    DEBUG("ColInfo alias:%s schemaName:%s tableName:%s columnName:%s type:%u", alias.c_str(), 
                      schemaName.c_str(), tableName.c_str(), columnName.c_str(), type);
    return GS_SUCCESS;
}

RowStruct::RowStruct(): isStart(1) {};

void RowStruct::setCount(uint32_t col_count) {
    count = col_count;
};

status_t RowStruct::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadBool(pipe, isStart));
    GS_RETURN_IFERR(ProtoFunc::ReadValueVector(pipe, values, count));
    return GS_SUCCESS;
}

void RowStruct::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteBool(isStart, buf, length);
    ProtoFunc::WriteValueVector(values, buf, length);
}

status_t ResultStruct::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadLong(pipe, row_count));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, col_count));
    for (int i = 0; i < col_count; ++i) {
        ColInfoStruct info;
        GS_RETURN_IFERR(info.unpacket(pipe));
        col_info.push_back(info);
    }
    for (int i = 0; i < row_count; ++i) {
        RowStruct row;
        row.setCount(col_count);
        GS_RETURN_IFERR(row.unpacket(pipe));
        rows.push_back(row);
    }
    return GS_SUCCESS;
}

void ResultStruct::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteLong(row_count, buf, length);
    ProtoFunc::WriteInt(col_count, buf, length);
    for (auto& item : col_info) {
        item.packet(buf);
        length += item.length;
    }
    for (auto& item : rows) {
        item.packet(buf);
        length += item.length;
    }

}

status_t PrepareQueryReq::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, seq_id));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, stmt_id));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, result_id));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, limit_rows));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, parameters_size));
    GS_RETURN_IFERR(ProtoFunc::ReadValueVector(pipe, parameters, parameters_size));
    return GS_SUCCESS;
}

void PrepareQueryReq::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(operation, buf, length);
    ProtoFunc::WriteString(seq_id, buf, length);
    ProtoFunc::WriteInt(stmt_id, buf, length);
    ProtoFunc::WriteInt(result_id, buf, length);
    ProtoFunc::WriteInt(limit_rows, buf, length);
    ProtoFunc::WriteInt(parameters_size, buf, length);
    ProtoFunc::WriteValueVector(parameters, buf, length);
}

status_t PrepareQueryRes::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, rescode));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, res_msg));
    GS_RETURN_IFERR(ProtoFunc::ReadBool(pipe, has_result));
    if (has_result == true) {
        result.unpacket(pipe);
    } 
    return GS_SUCCESS;
}

void PrepareQueryRes::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(rescode, buf, length);
    ProtoFunc::WriteString(res_msg, buf, length);
    ProtoFunc::WriteBool(has_result, buf, length);
    if (has_result == true) {
        result.packet(buf);
        length += result.length;
    }
}

status_t PrepareUpdateReq::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, seq_id));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, stmt_id));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, result_id));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, parameters_size));
    GS_RETURN_IFERR(ProtoFunc::ReadValueVector(pipe, parameters, parameters_size));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, key_mode));
    return GS_SUCCESS;
}

void PrepareUpdateReq::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(operation, buf, length);
    ProtoFunc::WriteString(seq_id, buf, length);
    ProtoFunc::WriteInt(stmt_id, buf, length);
    ProtoFunc::WriteInt(result_id, buf, length);
    ProtoFunc::WriteInt(parameters_size, buf, length);
    ProtoFunc::WriteValueVector(parameters, buf, length);
    ProtoFunc::WriteInt(key_mode, buf, length);
}

status_t PrepareUpdateRes::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, rescode));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, res_msg));
    GS_RETURN_IFERR(ProtoFunc::ReadLong(pipe, effect_rows));
    GS_RETURN_IFERR(ProtoFunc::ReadBool(pipe, has_result));
    if(has_result) {
        GS_RETURN_IFERR(result.unpacket(pipe));
    }
    return GS_SUCCESS;
}

void PrepareUpdateRes::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(rescode, buf, length);
    ProtoFunc::WriteString(res_msg, buf, length);
    ProtoFunc::WriteLong(effect_rows, buf, length);
    ProtoFunc::WriteBool(has_result, buf, length);
    if (has_result == true) {
        result.packet(buf);
        length += result.length;
    }
}

status_t ExecuteProtoReq::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, seq_id));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, result_id));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, sql));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, key_mode));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, limit_rows));
    return GS_SUCCESS;
}

void ExecuteProtoReq::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(operation, buf, length);
    ProtoFunc::WriteString(seq_id, buf, length);
    ProtoFunc::WriteInt(result_id, buf, length);
    ProtoFunc::WriteString(sql, buf, length);
    ProtoFunc::WriteInt(key_mode, buf, length);
    ProtoFunc::WriteInt(limit_rows, buf, length);
}

status_t ExecuteProtoRes::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, rescode));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, res_msg));
    GS_RETURN_IFERR(ProtoFunc::ReadBool(pipe, is_select));
    GS_RETURN_IFERR(ProtoFunc::ReadBool(pipe, is_read_only));
    GS_RETURN_IFERR(ProtoFunc::ReadLong(pipe, effect_rows));
    GS_RETURN_IFERR(ProtoFunc::ReadBool(pipe, has_result));
    if (has_result) {
        GS_RETURN_IFERR(result.unpacket(pipe));
    }
    
    return GS_SUCCESS;
}

void ExecuteProtoRes::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(rescode, buf, length);
    ProtoFunc::WriteString(res_msg, buf, length);
    ProtoFunc::WriteBool(is_select, buf, length);
    ProtoFunc::WriteBool(is_read_only, buf, length);
    ProtoFunc::WriteLong(effect_rows, buf, length);
    ProtoFunc::WriteBool(has_result, buf, length);
    if (has_result == true) {
        result.packet(buf);
        length += result.length;
    }
}

status_t SessionCloseReq::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, seq_id));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, stmt_id));
    return GS_SUCCESS;
}

void SessionCloseReq::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(operation, buf, length);
    ProtoFunc::WriteString(seq_id, buf, length);
    ProtoFunc::WriteInt(stmt_id, buf, length);
    return;
}

status_t SessionCloseRes::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, rescode));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, res_msg));
    return GS_SUCCESS;
}

void SessionCloseRes::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(rescode, buf, length);
    ProtoFunc::WriteString(res_msg, buf, length);
}

status_t ResultCloseReq::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, seq_id));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, result_id));
    return GS_SUCCESS;
}

void ResultCloseReq::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(operation, buf, length);
    ProtoFunc::WriteString(seq_id, buf, length);
    ProtoFunc::WriteInt(result_id, buf, length);
    return;
}

status_t ResultCloseRes::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, rescode));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, res_msg));
    return GS_SUCCESS;
}

void ResultCloseRes::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(rescode, buf, length);
    ProtoFunc::WriteString(res_msg, buf, length);
}

status_t CommandCloseReq::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, seq_id));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, stmt_id));
    return GS_SUCCESS;
}

void CommandCloseReq::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(operation, buf, length);
    ProtoFunc::WriteString(seq_id, buf, length);
    ProtoFunc::WriteInt(stmt_id, buf, length);
    return;
}

status_t CommandCloseRes::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, rescode));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, res_msg));
    return GS_SUCCESS;
}

void CommandCloseRes::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(rescode, buf, length);
    ProtoFunc::WriteString(res_msg, buf, length);
}

status_t CheckTransactionReq::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, seq_id));
    return GS_SUCCESS;
}

void CheckTransactionReq::packet(std::ostringstream &buf) {
    ProtoFunc::WriteInt(operation, buf, length);
    ProtoFunc::WriteString(seq_id, buf, length);
    return;
}

status_t CheckTransactionRes::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, rescode));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, res_msg));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, hasPendingTransaction));
    return GS_SUCCESS;
}

void CheckTransactionRes::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(rescode, buf, length);
    ProtoFunc::WriteString(res_msg, buf, length);
    ProtoFunc::WriteInt(hasPendingTransaction, buf, length);

}

status_t PrepareReq::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, seq_id));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, stmt_id));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, sql));
    return GS_SUCCESS;
}

void PrepareReq::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(operation, buf, length);
    ProtoFunc::WriteString(seq_id, buf, length);
    ProtoFunc::WriteInt(stmt_id, buf, length);
    ProtoFunc::WriteString(sql, buf, length);
}

status_t PrepareParamInfo::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, param_type));
    GS_RETURN_IFERR(ProtoFunc::ReadBool(pipe, is_nullable));
    return GS_SUCCESS;
}

void PrepareParamInfo::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(param_type, buf, length);
    ProtoFunc::WriteBool(is_nullable, buf, length);
}

status_t PrepareRes::unpacket(cs_pipe_t *pipe) {
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, rescode));
    GS_RETURN_IFERR(ProtoFunc::ReadString(pipe, res_msg));
    GS_RETURN_IFERR(ProtoFunc::ReadBool(pipe, is_select));
    GS_RETURN_IFERR(ProtoFunc::ReadBool(pipe, is_read_only));
    GS_RETURN_IFERR(ProtoFunc::ReadInt(pipe, param_num));
    return GS_SUCCESS;
}

void PrepareRes::packet(std::ostringstream &buf) {
    length = 0;
    ProtoFunc::WriteInt(rescode, buf, length);
    ProtoFunc::WriteString(res_msg, buf, length);
    ProtoFunc::WriteBool(is_select, buf, length);
    ProtoFunc::WriteBool(is_read_only, buf, length);
    ProtoFunc::WriteInt(param_num, buf, length);
}
