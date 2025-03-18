add_subdirectory(linenoise-ng)

# assist
file(GLOB CLI_ASSIST_SRC assist.cpp assist.h)
add_library(cli_assist OBJECT ${CLI_ASSIST_SRC})

#util
file(GLOB CLI_UTIL_SRC cli_util.cpp cli_util.h)
add_library(cli_util OBJECT ${CLI_UTIL_SRC})