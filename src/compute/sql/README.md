## 编译步骤 & 执行步骤

1. 在3rd/gstor目录直接 make 即可编译（会自动编译存储引擎和SQL引擎的代码）
    - make/make debug: 编译生成debug版本
    - make test: 编译生成debug版本，同时会编译test目录
    - make release: 编译生成release版本
2. 生成的SQL引擎相关的库文件和测试程序在gstor/output/<span style="color: red;"><debug|release></span>目录下
```
openGauss-embedded/3rd/gstor/output目录结构如下：
├── debug   # 使用make/make debug/make test编译时debug版本存储引擎和SQL引擎的可执行文件和库文件保存路径
│   ├── bin # debug版本可执行文件保存路径
│   └── lib # debug版本库文件保存路径
├── inc     # 第三方库文件的头文件保存路径，同以前
│   ├── cJSON
│   ├── huawei_security
│   ├── libpg_query
│   ├── libutf8proc
│   └── zlib
├── lib     # 第三方库文件保存路径，同以前
│   ├── libcjson.so -> libcjson.so.1
│   ├── libcjson.so.1 -> libcjson.so.1.7.15
│   ├── libcjson.so.1.7.15
│   ├── libduckdb_pg_query.a
│   ├── libfmt.a
│   ├── libfmtd.a
│   ├── libsecurec.a
│   ├── libsecurec.so
│   ├── libutf8proc.a
│   ├── libz.a
│   ├── libz.so -> libz.so.1
│   ├── libz.so.1 -> libz.so.1.2.12
│   └── libz.so.1.2.12
└── release  # 使用make release 编译时release版本存储引擎和SQL引擎的可执行文件和库文件保存路径
    ├── bin  # release版本可执行文件保存路径
    └── lib  # release版本库文件保存路径
```
3. 生成的SQL引擎的动态库是gstor/output/<span style="color: red;"><debug|release></span>/lib目录下的libintarkdb.so
4. 执行 output/<span style="color: red;"><debug|release></span>/bin/intarkdb_cli 可以启动客户端测试工具,后面可以跟参数指定数据文件位置 eg:./intarkdb_cli test
5. 目前支持：
    - create table, 支持类型 integer , smallint , varchar, real
        e.g: 
        `CREATE TABLE [IF NOT EXISTS] <table name> (<column name> <type name> [NULL|NOT NULL|UNIQUE|PRIMARY KEY] [DEFAULT <default value>], …, [<UNIQUE|PRIMARY KEY> (<column name>, …)]);`
    - create table as select
        e.g: 
        `CREATE TABLE [IF NOT EXISTS] <table name> AS SELECT <*|column name [AS <alias>], …> FROM <table name> [WHERE <condition stmt>];`
    - 时序表ddl
        e.g:
      - create table with partition
        `CREATE TABLE [IF NOT EXISTS] <table name> (<column name> <type name> [NULL|NOT NULL|UNIQUE|PRIMARY KEY] [DEFAULT <default value>], …, [<UNIQUE|PRIMARY KEY> (<column name>, …)]) PARTITION BY RANGE(date) [TIMESCALE] [INTERVAL <'1d'/'1h'>] [RETENTION '30d'] [AUTOPART] [CROSSPART];`
      - add partition
            `ALTER TABLE <table name> ADD PARTITION <partition name>;`
      - drop partition
            `ALTER TABLE <table name> DROP PARTITION <partition name>;`
    - create index, primary key必须在建表时或者增加列时创建
        e.g: 
        `CREATE [UNIQUE] INDEX <index name> ON <table name> (<column name>, …);`
    - alter table, 注意，设置默认值语句执行成功后不会改变已插入行的列值，新插入行才会看到新的默认值
        e.g: 
      - add column
            `ALTER TABLE <table name> ADD [COLUMN] <column name> <data type> [NULL/NOT NULL/UNIQUE/PRIMARY KEY] [DEFAULT <default value>];`
      - drop column
            `ALTER TABLE <table name> DROP [COLUMN] [IF EXISTS] <column name>;`
      - alter column
            `ALTER TABLE <table name> ALTER [COLUMN] <column name> [SET DATA] TYPE <data type> [NULL/NOT NULL/UNIQUE/PRIMARY KEY] [DEFAULT <default value>];`
            `ALTER TABLE <table name> ALTER [COLUMN] <column name> SET DEFAULT <default value>;`
            `ALTER TABLE <table name> ALTER [COLUMN] <column name> DROP DEFAULT;`
      - rename column
            `ALTER TABLE <table name> RENAME [COLUMN] <column name> TO <new column name>;`
      - rename table
            `ALTER TABLE <table name> RENAME TO <new table name>;`
    - show tables
        e.g: `SHOW tables;`
    - show table
        e.g: `<SHOW|DESCRIBE> <table name>;`
    - select 语句，支持简单where语句
        e.g: `select * from student ;`
             `select name , age from student ;`
             `select * from student where age > 10;`
             `select * from student where age between 10 and 20;`
             `select * from student where age in (10,15,20);`
      - like操作符, 目前仅支持where子句中的条件表达式中; 包含like, not like, ilike, not ilike及escape;支持的通配符 `%`-匹配任意个字符序列、`_`-匹配任意单个字符; escape 只支持单个字符
        e.g: `SELECT * FROM <table name> WHERE <column name> [NOT] <LIKE|ILIKE> <pattern> [ESCAPE <escape character>];`
    - insert 语句
    - 命令行附加功能：以".”开头，输入".help"查看全部命令行命令


    除触发器以外的多数的DDL、DQL、DML语句

6. 运行单例测试：
    先在3rd/gstor目录下执行`make test`进行编译测试
    运行测试: `bash testshell.sh`
7. 测试sql语句支持的类型字符串
   进入build/debug/sql-engine/test目录
   执行`./pg_type_test` 时，会测试固定的34中sql语句类型关键字
   执行`./pg_type_test [type keyword]`时，会测试[type keyword]是否是sql-engine支持的类型关键字
8. 打印指定表元信息:
   将build/debug/sql-engine/test目录下的assist_test拷贝到数据库所在目录
   执行`./assist_test <table name>`
9. SQL保留关键字说明：
   保留关键字含义：如果将保留关键字作为标识符，则会报错，解析不通过
   保留关键字详细列表：在dependency/libpg_query/grammar/keywords/reserved_keywords.list中

10. 性能相关-运行时间统计类使用方法：
    - 引入头文件：`#include "common/stat.h"`
    - 打开编译选项：根目录下的CMakeLists.txt中添加`add_compile_options(-DTIMESTAT)`
    - 使用 TIMESTATISTICS 或 TIMESTATISTICSWITHNAME(name) 宏在需要统计耗时的地方添加：`TIMESTATISTICS;` 或 `TIMESTATISTICSWITHNAME(name);`
        - 统计函数耗时，直接使用 `TIMESTATISTICS;`，它会自动获取函数名
        - 统计自定义代码块耗时，使用 `TIMESTATISTICSWITHNAME(name);`，其中name为自定义名称，用“{}”将代码块括起来，如 
            ```
            {
                TIMESTATISTICSWITHNAME("my_func");
                // 自定义代码块
            }
            ```
    - 统计结果输出： 在需要输出的位置使用 `TIMESTATISTICSPRINT;`，它会自动输出所有统计结果