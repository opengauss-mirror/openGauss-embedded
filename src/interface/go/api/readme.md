#####
##go语言简易SQL客户端测试程序
#####

##
##依赖说明
依赖动态库
gstor_sql.so
gstor_dev_c.so 
gstor.so 
storage.so

依赖头文件
gstor_interface/include/gstor.h

##
##运行
在当前目录下go run main.go  即可，也可以带目录运行: go run main.go -path database_dir （database_dir为gstor数据库目录）
不带目录运行会在当前目录下创建gstor数据库目录

##
##当前功能支持
1、创建表
2、增删改查
3、提交事务

##
##当前功能限制说明（当前SQL功能不完善、不稳定，不按下列约束操作可能会引起客户端程序崩溃）
1、建表必须建主键
2、插入需要指定所有字段插入
3、查询只能按主键查、或者不带条件查，查询需要查询所有字段
4、修改必须按主键修改，且需要按顺序修改所有字段（需要修改的字段前面的所有字段，后面的可以不带）
5、删除必须按主键删除
6、需要显式提交事务，暂不支持默认提交
7、当前不支持复杂SQL语句，只支持单表操作
8、当前只支持等值条件和AND条件

##
##测试案例
##建表-建主键
CREATE TABLE AAAA_TEST_TABLE(LOGID INTEGER, ACCTID INTEGER, CHGAMT VARCHAR(20), BAK1 VARCHAR(20), BAK2 VARCHAR(20), PRIMARY KEY (LOGID,ACCTID));
##插入-指定所有字段
INSERT INTO AAAA_TEST_TABLE(LOGID,ACCTID,CHGAMT,BAK1,BAK2) VALUES (1,11,'AAA','bak1','bak2'), (2,22,'BBB','bak1','bak2');
##删除-按主键
DELETE FROM AAAA_TEST_TABLE WHERE LOGID=1 and ACCTID=11;
##修改-按主键，且按顺序指定修改字段
UPDATE AAAA_TEST_TABLE set LOGID=2,ACCTID=22,CHGAMT='CCC' WHERE LOGID=2 and ACCTID=22;
##查询1-按主键
SELECT LOGID,ACCTID,CHGAMT,BAK1,BAK2 FROM AAAA_TEST_TABLE where LOGID=1 and ACCTID=11;
##查询2-全表查询
SELECT LOGID,ACCTID,CHGAMT,BAK1,BAK2 FROM AAAA_TEST_TABLE;

######
客户端输入的SQL语句以英文分号结束
输入quit退出程序
######