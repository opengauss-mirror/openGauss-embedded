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
* hashmap.h
*
* IDENTIFICATION
* openGauss-embedded/src/network/common/hashmap/hashmap.h
*
* -------------------------------------------------------------------------
*/

#ifndef HASH_MAP_FUNC_INCLUDE
#define HASH_MAP_FUNC_INCLUDE
#include <stddef.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include "../../../storage/gstor/zekernel/common/cm_types.h"

#ifdef _WIN32
#define EXPORT_API __declspec(dllexport)
#else
#define EXPORT_API __attribute__ ((visibility ("default")))
#endif

#define DEFAULT_LIST_SIZE 16

typedef struct st_HashNode
 {
    void * key;             // 键
    void * value;           // 值
    struct st_HashNode * next;     // 冲突链表
    bool8 in_use;
}*HashNode;

typedef enum 
{
    TYPE_STR = 0,
    TYPE_NUM_32 = 1,
    TYPE_NUM_64 = 2,
} hash_key_type;

typedef enum
{
    H_FALSE = 0,
    H_TRUE = 1,
} HBOOL;

typedef struct hash_map {
    HashNode list;      // 存储区域
    int size;           // 当前大小
    int list_size;       // 有效空间大小
    hash_key_type key_type; // key的类型
}*HashMap;

typedef struct hashMapIterator {
    HashNode node;    // 迭代器当前指向
    int count;      // 迭代次数
    int hashCode;   // 键值对的哈希值
    HashMap hash_map;
}*HashMapIterator;

#define newHashNode() (HashNode)malloc(sizeof(struct st_HashNode))
#define newHashMap() (HashMap)malloc(sizeof(struct hash_map))
#define newHashNodeList(length) (HashNode)malloc(length * sizeof(struct st_HashNode))
#define newHashMapIterator() (HashMapIterator)malloc(sizeof(struct st_HashNode))


void hashnodeSet(HashNode node, const void *key, const void *value, HashNode next, bool8 in_use, hash_key_type key_type);

HashMap createtHashMap(hash_key_type key_type);

void resetHashMap(HashMap hash_map, uint32 list_size);

unsigned int HashMapHashFunc(HashMap hash_map, const void * key);

static bool8 HashMapEqual(void * key1, void * key2, hash_key_type type);

void HashMapPut(HashMap hash_map, const void * key, const void * value);

void * HashMapGet(HashMap hash_map, const void * key);

bool8 HashMapRemove(HashMap hash_map, const void * key);

void HashMapClear(HashMap hash_map);

unsigned int getkeySize(hash_key_type key_type, const void * key);


// 创建哈希结构迭代器
HashMapIterator createHashMapIterator(HashMap hash_map);

// 迭代器是否有下一个,有的话iterator指向下一个
bool8 nextHashMapIterator(HashMapIterator iterator);

// 释放迭代器内存
void freeHashMapIterator(HashMapIterator * iterator);

#endif