#include "hashmap.h"


void testint()
{

    HashMap strHashMap = createtHashMap(TYPE_NUM_32);
    for(int i = 0; i < 18; i++)
    {
        char *value = (char*)malloc(30);
        sprintf(value, "val-%d", i);
        HashMapPut(strHashMap, &i, value);
        printf("put k %d v %s \n", i,  value);
    }
    //char* strvalue = HashMapGet(strHashMap, "key1");
    //printf("k %s v %s \n", key,  strvalue);

    printf("\n");
    HashMapIterator iterator = createHashMapIterator(strHashMap);
    for (int index = 0; nextHashMapIterator(iterator); index++)
    {
        printf("k %d v %s in_use:%d\n", *(int32*)iterator->node->key, iterator->node->value, iterator->node->in_use);
    }
    printf("\n");
    for(int i = 0; i < 18; i++)
    {
        //char key[20];
        //sprintf(key, "key-%d", i);
        printf("get k %d v %s \n", i, (char * )HashMapGet(strHashMap, &i));
        if (i < 10)
        {
            HashMapRemove(strHashMap, &i);
        }
    }
    iterator = createHashMapIterator(strHashMap);
    for (int index = 0; nextHashMapIterator(iterator); index++)
    {
        printf("k %d v %s in_use:%d\n", *(int32*)iterator->node->key, iterator->node->value, iterator->node->in_use);
        //sleep(1);
    }
}

void testint64()
{

    HashMap strHashMap = createtHashMap(TYPE_NUM_64);
    for(uint64 i = 120; i < 138; i++)
    {
        char *value = (char*)malloc(30);
        sprintf(value, "val-%llu", i);
        HashMapPut(strHashMap, &i, value);
        printf("put k %llu v %s \n", i,  value);
    }
    //char* strvalue = HashMapGet(strHashMap, "key1");
    //printf("k %s v %s \n", key,  strvalue);

    printf("\n");
    HashMapIterator iterator = createHashMapIterator(strHashMap);
    for (int index = 0; nextHashMapIterator(iterator); index++)
    {
        printf("k %ld v %s in_use:%d\n", *(uint64*)iterator->node->key, iterator->node->value, iterator->node->in_use);
    }
    printf("\n");
    for(uint64 i = 120; i < 138; i++)
    {
        //char key[20];
        //sprintf(key, "key-%d", i);
        printf("get k %llu v %s \n", i, (char * )HashMapGet(strHashMap, &i));
        if (i < 10)
        {
            HashMapRemove(strHashMap, &i);
        }
    }
    iterator = createHashMapIterator(strHashMap);
    for (int index = 0; nextHashMapIterator(iterator); index++)
    {
        printf("k %llu v %s in_use:%d\n", *(uint64*)iterator->node->key, iterator->node->value, iterator->node->in_use);
        //sleep(1);
    }
}

void teststr()
{

    HashMap strHashMap = createtHashMap(TYPE_STR);
    for(int i = 0; i < 18; i++)
    {
        char key[20];
        sprintf(key, "key-%d", i);
        char *value = (char*)malloc(30);
        sprintf(value, "val-%d", i);
        HashMapPut(strHashMap, key, value);
        printf("put k %s v %s \n", key,  value);
    }
    //char* strvalue = HashMapGet(strHashMap, "key1");
    //printf("k %s v %s \n", key,  strvalue);

    printf("\n");
    HashMapIterator iterator = createHashMapIterator(strHashMap);
    for (int index = 0; nextHashMapIterator(iterator); index++)
    {
        printf("k %s v %s in_use:%d\n", iterator->node->key, iterator->node->value, iterator->node->in_use);
    }
    printf("\n");
    for(int i = 0; i < 18; i++)
    {
        char key[20];
        sprintf(key, "key-%d", i);
        printf("get k %s v %s \n", key, (char * )HashMapGet(strHashMap, key));
        if (i < 10)
        {
            HashMapRemove(strHashMap, key);
        }
    }
    iterator = createHashMapIterator(strHashMap);
    for (int index = 0; nextHashMapIterator(iterator); index++)
    {
        printf("k %s v %s in_use:%d\n", iterator->node->key, iterator->node->value, iterator->node->in_use);
        //sleep(1);
    }
}

int main()
{
    printf("\ntest int64\n");
    testint64();
    /* printf("\ntest int\n");
    testint();
    printf("\ntest str\n");
    teststr(); */
}