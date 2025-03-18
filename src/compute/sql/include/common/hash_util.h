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
* hash_util.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/common/hash_util.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <stddef.h>

using hash_t = size_t;

const hash_t PRIME_FACTOR = 10000019;

class HashUtil {
   public:
    static inline auto HashBytes(const char* bytes, size_t length) -> hash_t {
        hash_t hash = length;
        for (size_t i = 0; i < length; ++i) {
            hash = (hash << 5) ^ (hash >> 27) ^ bytes[i];
        }
        return hash;
    }

    static inline auto CombineHash(hash_t hash1, hash_t hash2) -> hash_t {
        hash_t both[2] = {};
        both[0] = hash1;
        both[1] = hash2;
        return HashBytes(reinterpret_cast<const char*>(both), sizeof(hash_t) * 2);
    }
};
