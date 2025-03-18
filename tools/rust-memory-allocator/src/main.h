#include <stdio.h>

typedef struct User
{
    unsigned int age;
    char name[64];
} User;

int rust_alloc_init(int block_initial_size, int page_cnt_per_block, int page_size);
User *rust_get_page(int index);
