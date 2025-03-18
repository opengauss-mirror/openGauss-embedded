/*
    test_miniz_use.c 测试miniz库一般使用
*/

#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include "zip.h"
#include "unzip.h"



#define TEST_STRING "hello hello minizip."
#define TEST_STRING_SIZE 20
#define TEST_FILENAME "miniz_use.txt"

#define TEST_STRING2 "hello hello minizip append mode."
#define TEST_STRING_SIZE2 32
#define TEST_FILENAME2 "miniz_use2.txt"

#define TEST_ZIP_FILENAME "test.zip"

#define CHECK_ERROR(ret) { if (ret != ZIP_OK) { return ret; } }
#define CHECK_RESULT(ret, v) { if (ret != v) { return ret; } }

int test()
{
    int ret = ZIP_OK;

    // zip create new file
    zip_fileinfo zi;
    memset(&zi, 0, sizeof(zip_fileinfo));
    zipFile zfd = zipOpen(TEST_ZIP_FILENAME, APPEND_STATUS_CREATE);
    ret = zipOpenNewFileInZip(zfd, TEST_FILENAME, &zi, NULL, 0, NULL, 0, NULL, Z_DEFLATED, 9);
    CHECK_ERROR(ret);
    ret = zipWriteInFileInZip(zfd, TEST_STRING, TEST_STRING_SIZE);
    CHECK_ERROR(ret);
    ret = zipCloseFileInZip(zfd);
    CHECK_ERROR(ret);
    ret = zipClose(zfd, NULL);
    CHECK_ERROR(ret);

    // zip append to exist file
    memset(&zi, 0, sizeof(zip_fileinfo));
    zfd = zipOpen(TEST_ZIP_FILENAME, APPEND_STATUS_ADDINZIP);
    ret = zipOpenNewFileInZip(zfd, TEST_FILENAME2, &zi, NULL, 0, NULL, 0, NULL, Z_DEFLATED, 9);
    CHECK_ERROR(ret);
    ret = zipWriteInFileInZip(zfd, TEST_STRING2, TEST_STRING_SIZE2);
    CHECK_ERROR(ret);
    ret = zipCloseFileInZip(zfd);
    CHECK_ERROR(ret);
    ret = zipClose(zfd, NULL);
    CHECK_ERROR(ret);

    // unzip
    unzFile unzfd = unzOpen(TEST_ZIP_FILENAME);
    char buffer[TEST_STRING_SIZE + 1];
    buffer[TEST_STRING_SIZE] = '\0';
    ret = unzLocateFile(unzfd, TEST_FILENAME, 1);
    CHECK_ERROR(ret);
    ret = unzOpenCurrentFile(unzfd);
    CHECK_ERROR(ret);
    ret = unzReadCurrentFile(unzfd, (char *)buffer, TEST_STRING_SIZE);
    CHECK_RESULT(ret, TEST_STRING_SIZE);
    ret = unzCloseCurrentFile(unzfd);
    CHECK_ERROR(ret);

    char buffer2[TEST_STRING_SIZE2 + 1];
    buffer2[TEST_STRING_SIZE2] = '\0';
    ret = unzLocateFile(unzfd, TEST_FILENAME2, 1);
    CHECK_ERROR(ret);
    ret = unzOpenCurrentFile(unzfd);
    CHECK_ERROR(ret);
    ret = unzReadCurrentFile(unzfd, (char *)buffer2, TEST_STRING_SIZE2);
    CHECK_RESULT(ret, TEST_STRING_SIZE2);
    ret = unzCloseCurrentFile(unzfd);
    CHECK_ERROR(ret);
    ret = unzClose(unzfd);
    CHECK_ERROR(ret);

    // check result
    if (strcmp(TEST_STRING, buffer) != 0) {
        return -1;
    }
    if (strcmp(TEST_STRING2, buffer2) != 0) {
        return -1;
    }
    return 0;
}

int main(int argc, char* argv[])
{
    int ret = test();
    if (ret == 0) {
        printf("success - test_miniz_use.\n");
    } else {
        printf("fail - test_miniz_use.\n");
    }
    return ret;
}