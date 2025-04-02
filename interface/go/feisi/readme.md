飞思捷跃场景压测

1.	验证1500个表，每个表1个点位，写入能否在1S内完成？
建表语句:CREATE TABLE if not exists sql_test_1 (id int, date timestamp,value int) PARTITION BY RANGE(date) TIMESCALE INTERVAL '1h' RETENTION '30d';
插入语句:INSERT INTO sql_test_1 VALUES (0,'2023-10-10 10:11:15', 44)

2.	验证1个表，一个表1500个点位，写入耗时
建表语句:CREATE TABLE if not exists sql_test_1 (id int, date timestamp,value int) PARTITION BY RANGE(date) TIMESCALE INTERVAL '1h' RETENTION '30d';
插入语句:INSERT INTO sql_test_1 VALUES (0,'2023-10-10 10:11:15', 0),(1,'2023-10-10 10:11:15', 1)....;

3.	验证1个表，一个表1500个点位，持续插入2小时，平均耗时，系统资源占用情况
例如：
insert count: 7116 total time: 7200.050749809 avg time: 1.0118115162744519

CPU 使用率
平均45.36717613
最大
最小
12.2

内存使用率
平均
18.1301391
最大
19.4009
最小
12.5747
