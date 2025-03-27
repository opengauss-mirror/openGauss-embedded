add_subdirectory(tools_dependency)
IF (ENABLE_PG_QUERY)
    add_subdirectory(intarkdb_cli)
ENDIF()