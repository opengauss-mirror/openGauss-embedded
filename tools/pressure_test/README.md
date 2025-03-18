# 压测

## 功能

测多线程读、写，以及准备读的数据

## 命令

./pressure_test ${path} ${thread_num} ${cmd:R\W\P} 
R: 测读
W: 测写
P: 准备测读的数据

## 相关脚本
embedded_test_prepare.sh embedded_test_start.sh embedded_test_stop.sh

## 编译

在tools下[CMakeLists.txt](../CMakeLists.txt)增加add_subdirectory(pressure_test),
重新cmake、make之后会生成sqlite_test、pressure_test



