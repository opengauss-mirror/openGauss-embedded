#include "main.h"
#include "string.h"

#define P(...) printf("\033[1;32m[C]\033[0m " __VA_ARGS__)
#define E(...) printf("\033[1;32m[C]\033[1;31m ERROR.\033[0m " __VA_ARGS__)

void PrintUser(const User *u)
{
    if (u == NULL)
    {
        E("user return null\n");
        return;
    }
    P("user[%p] .name = %s, .age = %d\n", u, u->name, u->age);
    // P("user[%p]\n", u);
}

int main()
{
    int result;
    result = rust_alloc_init(10, 100, 8 * 1024);
    P("result = %d\n", result);
    User *user = NULL;

    user = (User *)rust_get_page(100);
    PrintUser(user);

    strncpy(user->name, "ssss", 64);
    user->age = 10;
    P("User -----------\n");
    PrintUser(user);

    P("User2 -----------\n");
    User *user2 = (User *)rust_get_page(100);
    PrintUser(user2);

    user = (User *)rust_get_page(100000);
    PrintUser(user);
    if (NULL == user)
    {
        return 1;
    }

    return 0;
}
