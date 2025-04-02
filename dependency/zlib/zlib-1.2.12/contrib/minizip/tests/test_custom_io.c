/*
    test_miniz_use.c 测试miniz库的定制io功能
*/
#include <stdio.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "zip.h"
#include "unzip.h"



#define TEST_STRING "hello hello minizip."
#define TEST_STRING_SIZE 20
#define TEST_FILENAME "custom_io.txt"
#define TEST_ZIP_FILENAME "test.zip"

#define CHECK_ERROR(ret) { if (ret != ZIP_OK) { return ret; } }
#define CHECK_RESULT(ret, v) { if (ret != v) { return ret; } }

// custom io function
voidpf custom_fopen(voidpf opaque, const char* filename, int mode)
{
    int file = -1;
    int mode_fopen = -1;
    if ((mode & ZLIB_FILEFUNC_MODE_READWRITEFILTER) == ZLIB_FILEFUNC_MODE_READ) {
        mode_fopen = O_RDONLY;
    }
    else if (mode & ZLIB_FILEFUNC_MODE_EXISTING) {
        mode_fopen = O_RDWR;
    }
    else if (mode & ZLIB_FILEFUNC_MODE_CREATE) {
        mode_fopen = O_WRONLY | O_CREAT;
    }
    if ((filename != NULL) && (mode_fopen != -1)) {
        file = open(filename, mode_fopen, 0);
    }
    return (voidpf)(size_t)file;
}

uLong custom_fread(voidpf opaque, voidpf stream, void* buf, uLong size)
{
    return read((int)(size_t)stream, buf, size);
}

uLong custom_fwrite(voidpf opaque, voidpf stream, const void* buf, uLong size)
{
    return write((int)(size_t)stream, buf, size);
}

int custom_fclose(voidpf opaque, voidpf stream)
{
    return close((int)(size_t)stream);
}

int custom_ferror(voidpf opaque, voidpf stream)
{
    return 0;
}

long custom_fseek(voidpf opaque, voidpf stream, uLong offset, int origin)
{
    int fseek_origin = 0;
    switch (origin)
    {
    case ZLIB_FILEFUNC_SEEK_CUR :
        fseek_origin = SEEK_CUR;
        break;
    case ZLIB_FILEFUNC_SEEK_END :
        fseek_origin = SEEK_END;
        break;
    case ZLIB_FILEFUNC_SEEK_SET :
        fseek_origin = SEEK_SET;
        break;
    default: return -1;
    }

    long ret = lseek((int)(size_t)stream, (off_t)offset, fseek_origin);
    if (ret != -1) {
        ret = 0;
    }
    return ret;
}

long custom_ftell(voidpf opaque, voidpf stream)
{
    return (long)lseek((int)(size_t)stream, (off_t)0, SEEK_CUR);
}

int test()
{
    int ret = ZIP_OK;

    // init custom io function
    zlib_filefunc_def filefunc;
    filefunc.zopen_file = custom_fopen;
    filefunc.zread_file = custom_fread;
    filefunc.zwrite_file = custom_fwrite;
    filefunc.ztell_file = custom_ftell;
    filefunc.zseek_file = custom_fseek;
    filefunc.zclose_file = custom_fclose;
    filefunc.zerror_file = custom_ferror;
    filefunc.opaque = NULL;


    // zip
    zip_fileinfo zi;
    memset(&zi, 0, sizeof(zip_fileinfo));
    zipFile zfd = zipOpen2(TEST_ZIP_FILENAME, 0, NULL, &filefunc);
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
    unzFile unzfd = unzOpen2(TEST_ZIP_FILENAME, &filefunc);
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
        printf("success - test_custom_io\n");
    } else {
        printf("fail - test_custom_io\n");
    }
    return ret;
}