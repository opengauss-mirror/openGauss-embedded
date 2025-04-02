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
* type_test.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/test/type_test.cpp
*
* -------------------------------------------------------------------------
*/
#include <gtest/gtest.h>

#include <iostream>

#include "cm_defs.h"
#include "storage/gstor/zekernel/common/cm_date.h"
#include "type/hugeint.h"
#include "type/operator/cast_operators.h"
#include "type/type_id.h"
#include "type/type_system.h"

TEST(TypeSystemTest, TinyIntCompare) {
    auto type = GS_TYPE_TINYINT;
    using stortype = int32_t;
    Value v1(type, static_cast<stortype>(1));
    Value v2(type, static_cast<stortype>(1));
    EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v1, v2), Trivalent::TRI_TRUE);

    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThanOrEqual(v1, v2), Trivalent::TRI_TRUE);

    Value v3(type, static_cast<stortype>(3));
    Value v4(type, static_cast<stortype>(4));
    EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v3, v4), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThan(v3, v4), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThanOrEqual(v3, v4), Trivalent::TRI_TRUE);

    // overflow test
    Value v5(GS_TYPE_BIGINT, int64_t(INT_MAX) + 1);
    EXPECT_THROW(v5.GetCastAs<int32_t>(), intarkdb::Exception);

    // int to string
    EXPECT_EQ(v3.GetCastAs<VarcharType::StorType>() == "3", true);

    // int to double
    EXPECT_EQ(v3.GetCastAs<RealType::StorType>(), 3.0);
}

TEST(TypeSystemTest, SmallIntCompare) {
    auto type = GS_TYPE_SMALLINT;
    using stortype = int16_t;
    Value v1(type, static_cast<stortype>(1));
    Value v2(type, static_cast<stortype>(1));
    EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v1, v2), Trivalent::TRI_TRUE);

    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThanOrEqual(v1, v2), Trivalent::TRI_TRUE);

    Value v3(type, static_cast<stortype>(3));
    Value v4(type, static_cast<stortype>(4));
    EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v3, v4), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThan(v3, v4), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThanOrEqual(v3, v4), Trivalent::TRI_TRUE);
}

TEST(TypeSystemTest, IntegerCompare) {
    auto type = GS_TYPE_INTEGER;
    using stortype = int32_t;
    Value v1(type, static_cast<stortype>(1));
    Value v2(type, static_cast<stortype>(1));
    EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v1, v2), Trivalent::TRI_TRUE);

    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThanOrEqual(v1, v2), Trivalent::TRI_TRUE);

    Value v3(type, static_cast<stortype>(3));
    Value v4(type, static_cast<stortype>(4));
    EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v3, v4), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThan(v3, v4), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThanOrEqual(v3, v4), Trivalent::TRI_TRUE);
}

TEST(TypeSystemTest, BigIntCompare) {
    auto type = GS_TYPE_BIGINT;
    using stortype = int64_t;
    Value v1(type, static_cast<stortype>(1));
    Value v2(type, static_cast<stortype>(1));
    EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v1, v2), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThanOrEqual(v1, v2), Trivalent::TRI_TRUE);

    Value v3(type, static_cast<stortype>(3));
    Value v4(type, static_cast<stortype>(4));
    EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v3, v4), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThan(v3, v4), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThanOrEqual(v3, v4), Trivalent::TRI_TRUE);

    // test overflow
    Value v5(GS_TYPE_UINT64, uint64_t(std::numeric_limits<int64_t>::max()) + 1);
    EXPECT_THROW(v5.GetCastAs<BigIntType::StorType>(), intarkdb::Exception);
}

TEST(TypeSystemTest, VarcharCompare) {
    auto type = GS_TYPE_VARCHAR;
    Value v1(type, "123");
    Value v2(type, std::string_view("123"));
    // EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v1, v2), true);
    EXPECT_EQ(v1.Equal(v2), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThanOrEqual(v1, v2), Trivalent::TRI_TRUE);

    Value v3(type, "abc");
    Value v4(type, "efgh");
    EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v3, v4), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThan(v3, v4), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThanOrEqual(v3, v4), Trivalent::TRI_TRUE);
}

TEST(TypeSystemTest, BoolCompare) {
    auto type = GS_TYPE_BOOLEAN;
    Value v1(type, false);
    Value v2(type, false);
    EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v1, v2), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThanOrEqual(v1, v2), Trivalent::TRI_TRUE);

    Value v3(type, false);
    Value v4(type, true);
    EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v3, v4), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThan(v3, v4), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThanOrEqual(v3, v4), Trivalent::TRI_TRUE);
}

TEST(TypeSystemTest, DiffIntCompare) {
    Value v1(GStorDataType::GS_TYPE_TINYINT, static_cast<int8_t>(-1));
    Value v2(GStorDataType::GS_TYPE_SMALLINT, static_cast<int16_t>(0));
    Value v3(GStorDataType::GS_TYPE_INTEGER, static_cast<int32_t>(2));
    Value v4(GStorDataType::GS_TYPE_BIGINT, static_cast<int64_t>(2));

    EXPECT_EQ(DataType::GetTypeInstance(v1.GetType())->Equal(v1, v2), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(v3.GetType())->Equal(v3, v4), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(v2.GetType())->Equal(v2, v3), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(v1.GetType())->LessThan(v1, v3), Trivalent::TRI_TRUE);
}

TEST(TypeSystemTest, RealCompare) {
    auto type = GS_TYPE_REAL;
    Value v1(type, 1.3333);
    Value v2(type, 1.3333);
    EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v1, v2), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThanOrEqual(v1, v2), Trivalent::TRI_TRUE);

    Value v3(type, 1e-16);
    Value v4(type, 1e-15);
    EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v3, v4), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThan(v3, v4), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThanOrEqual(v3, v4), Trivalent::TRI_TRUE);

    Value v5(type, 1.333333);
    EXPECT_EQ(DataType::GetTypeInstance(type)->Equal(v1, v5), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(type)->LessThan(v1, v5), Trivalent::TRI_TRUE);
}

TEST(TypeSystemTest, RealWithIntCompare) {
    Value v_real(GS_TYPE_REAL, 1.3333);
    Value v1(GStorDataType::GS_TYPE_TINYINT, static_cast<int32_t>(-1));
    Value v2(GStorDataType::GS_TYPE_SMALLINT, static_cast<int32_t>(123));
    Value v3(GStorDataType::GS_TYPE_INTEGER, static_cast<int32_t>(1000));
    Value v4(GStorDataType::GS_TYPE_BIGINT, static_cast<int64_t>(2000000));

    EXPECT_EQ(DataType::GetTypeInstance(v1.GetType())->Equal(v1, v_real), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(v2.GetType())->Equal(v2, v_real), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(v3.GetType())->Equal(v3, v_real), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(v4.GetType())->Equal(v4, v_real), Trivalent::TRI_FALSE);

    // EXPECT_EQ(v_real.LessThan(v4), true);
}

TEST(TypeSystemTest, VarCharWithIntCompare) {
    Value v1(GStorDataType::GS_TYPE_TINYINT, static_cast<int32_t>(-1));
    Value v2(GStorDataType::GS_TYPE_SMALLINT, static_cast<int32_t>(123));
    Value v3(GStorDataType::GS_TYPE_INTEGER, static_cast<int32_t>(1000));
    Value v4(GStorDataType::GS_TYPE_BIGINT, static_cast<int64_t>(2000000));

    Value str(GStorDataType::GS_TYPE_VARCHAR, "123");
    Value str2(GStorDataType::GS_TYPE_VARCHAR, "abc");

    EXPECT_EQ(DataType::GetTypeInstance(v1.GetType())->Equal(v1, str), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(v2.GetType())->Equal(v2, str), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(str.GetType())->Equal(str, v2), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(v3.GetType())->LessThan(v3, str), Trivalent::TRI_FALSE);
    EXPECT_EQ(DataType::GetTypeInstance(v1.GetType())->LessThan(v1, str2), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(v4.GetType())->LessThan(v4, str2), Trivalent::TRI_TRUE);
    EXPECT_EQ(DataType::GetTypeInstance(str2.GetType())->LessThan(str2, v4), Trivalent::TRI_FALSE);
}

TEST(TypeSystemTest, VarcharWithRealCompare) {}

TEST(TypeSystemTest, TimeStampTest) {
    // test parse date string
    time_t timestamp_type;
    const char* date_str = "2022-03-14 12:12:12";
    auto r = cm_str2time(const_cast<char*>(date_str), nullptr, &timestamp_type);
    EXPECT_EQ(r, GS_SUCCESS);
    EXPECT_EQ(timestamp_type, 1647231132);

    Value v(GStorDataType::GS_TYPE_VARCHAR, "2023-05-11 08:23:23");
    Value str_v(GStorDataType::GS_TYPE_VARCHAR, "2020-01-01 00:00:00");
    Value str_v2(GStorDataType::GS_TYPE_VARCHAR, "2024-01-01 00:00:00");
    EXPECT_EQ(v.LessThan(str_v), Trivalent::TRI_FALSE);
    EXPECT_EQ(v.LessThan(str_v2), Trivalent::TRI_TRUE);
}

TEST(TypeSystemTest, DateTest) {
    const char* date_str = "2022-03-14";
    const char* timestamp_str = "2022-03-14 23:59:59";
    Value v = ValueFactory::ValueDate(date_str);
    Value v2 = ValueFactory::ValueDate(timestamp_str);
    EXPECT_EQ(v.Equal(v2), Trivalent::TRI_TRUE);
    Value v3 = ValueFactory::ValueTimeStamp(timestamp_str);
    EXPECT_EQ(v.Equal(DataType::GetTypeInstance(v.GetType())->CastValue(v3)), Trivalent::TRI_TRUE);

    Value v4 = ValueFactory::ValueDate("1993-08-14");
    Value v5 = ValueFactory::ValueTimeStamp("1993-08-14 00:00:01");
    EXPECT_EQ(v4.Equal(DataType::GetTypeInstance(v4.GetType())->CastValue(v5)), Trivalent::TRI_TRUE);
}

TEST(TypeSystemTest, HugeIntTest) {
    hugeint_t x(100);
    EXPECT_EQ(x.ToString() == "100", true);

    std::string str_val = "123456789123456789";

    hugeint_t hugeint_value;
    // if that is not successful; try to cast as hugeint
    if (TryCast::Operation<std::string, hugeint_t>(str_val, hugeint_value)) {
        // successfully cast to bigint: bigint value
        EXPECT_EQ(hugeint_value.ToString() == "123456789123456789", true);
    }
}

TEST(TypeSystemTest, BigIntCastTest) {
    std::string str_val = "123456789123456789";

    int64_t bigint_value;
    // try to cast as bigint first
    if (TryCast::Operation<std::string, int64_t>(str_val, bigint_value)) {
        // successfully cast to bigint: bigint value
        EXPECT_EQ(bigint_value, 123456789123456789);
    } else {
        ASSERT_TRUE(false);
    }
}

TEST(TypeSystemTest, DecimalTest) {
    dec4_t x;
    cm_real_to_dec4(3.0, &x);
    dec4_t y;
    cm_real_to_dec4(128.213, &y);
    dec4_t z;
    cm_real_to_dec4(3.00, &z);
    Value v1(GS_TYPE_DECIMAL, x);
    Value v2(GS_TYPE_DECIMAL, y);
    Value v3(GS_TYPE_DECIMAL, z);
    v3.SetScaleAndPrecision(0, 18);
    EXPECT_EQ(v1.Equal(v3), Trivalent::TRI_TRUE);
    EXPECT_EQ(v1.LessThan(v2), Trivalent::TRI_TRUE);
    EXPECT_EQ(v3.GetCastAs<VarcharType::StorType>() == "3", true);

    // test decimal tostring
    dec4_t x1;
    cm_real_to_dec4(100.0, &x1);
    Value v4 = ValueFactory::ValueDecimal(x1);
    v4.SetScaleAndPrecision(0, 10);
    EXPECT_STREQ(v4.ToString().c_str(), "100");

    dec4_t x2;
    cm_real_to_dec4(123.123456678, &x2);
    Value v5 = ValueFactory::ValueDecimal(x2);
    v5.SetScaleAndPrecision(2, 10);
    EXPECT_STREQ(v5.ToString().c_str(), "123.12");

    // 有效数字 & 保留小数判断
    EXPECT_EQ((int)DecimalType::IsDecimalFormat("12.345", 5, 3), (int)DecimalType::DecimalCheckType::SUCCESS);
    EXPECT_EQ((int)DecimalType::IsDecimalFormat("0.00123", 6, 5), (int)DecimalType::DecimalCheckType::SUCCESS);
    EXPECT_EQ((int)DecimalType::IsDecimalFormat("-12.34", 5, 2), (int)DecimalType::DecimalCheckType::SUCCESS);
    EXPECT_EQ((int)DecimalType::IsDecimalFormat("0", 1, 0), (int)DecimalType::DecimalCheckType::SUCCESS);

    EXPECT_EQ((int)DecimalType::IsDecimalFormat("1.345", 4, 2), (int)DecimalType::DecimalCheckType::PRECISION_OVERFLOW);
    EXPECT_EQ((int)DecimalType::IsDecimalFormat("0.00123", 3, 4), (int)DecimalType::DecimalCheckType::SCALE_OVERFLOW);
    EXPECT_EQ((int)DecimalType::IsDecimalFormat("-12.34", 2, 1), (int)DecimalType::DecimalCheckType::SCALE_OVERFLOW);
    EXPECT_EQ((int)DecimalType::IsDecimalFormat("0.0", 0, 0), (int)DecimalType::DecimalCheckType::SCALE_OVERFLOW);
    EXPECT_EQ((int)DecimalType::IsDecimalFormat("123.23", 4, 2), (int)DecimalType::DecimalCheckType::SCALE_OVERFLOW);

    EXPECT_STREQ(DecimalType::TrimDecimal("123.456789", 2).c_str(), "123.45");
    EXPECT_STREQ(DecimalType::TrimDecimal("0.123456789", 4).c_str(), "0.1234");
    EXPECT_STREQ(DecimalType::TrimDecimal("12.345", 3).c_str(), "12.345");
    EXPECT_STREQ(DecimalType::TrimDecimal("123.45", 5).c_str(), "123.45");
    EXPECT_STREQ(DecimalType::TrimDecimal("0.0", 0).c_str(), "0");
}

TEST(TypeSystemTest, RealTypePrint) {
    Value v = ValueFactory::ValueDouble(100);
    EXPECT_STREQ(v.ToString().c_str(), "100");
    Value v2 = ValueFactory::ValueDouble(100.123);
    EXPECT_STREQ(v2.ToString().c_str(), "100.123");
}

using namespace intarkdb;
TEST(TypeSystemTest, TypeCast) {
    auto v = GetCompatibleType(GS_TYPE_BOOLEAN, GS_TYPE_INTEGER);
    EXPECT_EQ(v.TypeId(), GS_TYPE_INTEGER);

    v = GetCompatibleType(GS_TYPE_INTEGER, GS_TYPE_INTEGER);
    EXPECT_EQ(v.TypeId(), GS_TYPE_INTEGER);

    v = GetCompatibleType(GS_TYPE_INTEGER, GS_TYPE_VARCHAR);
    EXPECT_EQ(v.TypeId(), GS_TYPE_VARCHAR);

    v = GetCompatibleType(GS_TYPE_BIGINT, GS_TYPE_REAL);
    EXPECT_EQ(v.TypeId(), GS_TYPE_REAL);

    v = GetCompatibleType(GS_TYPE_INTEGER, GS_TYPE_DATE);
    EXPECT_EQ(v.TypeId(), GS_TYPE_DATE);
}
