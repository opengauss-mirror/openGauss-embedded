/*
    test_miniz_use.c 测试miniz库的setUnzipMemoryFunc和setZipMemoryFunc接口
*/

#include <stdlib.h>
#include <string.h>
#include "zip.h"
#include "unzip.h"



#define TEST_STRING "hello hello minizip."
#define TEST_STRING_SIZE 20
#define TEST_FILENAME "memory_func.txt"
#define TEST_ZIP_FILENAME "test.zip"

#define CHECK_ERROR(ret) { if (ret != ZIP_OK) { return ret; } }
#define CHECK_RESULT(ret, v) { if (ret != v) { return ret; } }

voidp custom_malloc(size_t size)
{
    return malloc(size);
}

void custom_free(voidp address)
{
    free(address);
}

int test()
{
    int ret = ZIP_OK;

    // zip
    zip_fileinfo zi;
    memset(&zi, 0, sizeof(zip_fileinfo));
    setZipMemoryFunc(custom_malloc, custom_free);
    zipFile zfd = zipOpen(TEST_ZIP_FILENAME, 0);
    ret = zipOpenNewFileInZip(zfd, TEST_FILENAME, &zi, NULL, 0, NULL, 0, NULL, Z_DEFLATED, 9);
    CHECK_ERROR(ret);
    ret = zipWriteInFileInZip(zfd, TEST_STRING, TEST_STRING_SIZE);
    CHECK_ERROR(ret);
    ret = zipCloseFileInZip(zfd);
    CHECK_ERROR(ret);
    ret = zipClose(zfd, NULL);
    CHECK_ERROR(ret);

    // unzip
    char buffer[TEST_STRING_SIZE + 1];
    buffer[TEST_STRING_SIZE] = '\0';
    setUnzipMemoryFunc(custom_malloc, custom_free);
    unzFile unzfd = unzOpen(TEST_ZIP_FILENAME);
    ret = unzLocateFile(unzfd, TEST_FILENAME, 1);
    CHECK_ERROR(ret);
    ret = unzOpenCurrentFile(unzfd);
    CHECK_ERROR(ret);
    ret = unzReadCurrentFile(unzfd, (char *)buffer, TEST_STRING_SIZE);
    CHECK_RESULT(ret, TEST_STRING_SIZE);
    ret = unzCloseCurrentFile(unzfd);
    CHECK_ERROR(ret);
    ret = unzClose(unzfd);
    CHECK_ERROR(ret);

    // check result
    if (strcmp(TEST_STRING, buffer) != 0) {
        return -1;
    }
    return 0;
}

int main(int argc, char* argv[])
{
    int ret = test();
    if (ret == 0) {
        printf("success - test_memory_func.\n");
    } else {
        printf("fail - test_memory_func.\n");
    }
    return ret;
}