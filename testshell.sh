#!/bin/bash
# 运行测试用例
if [ $# == 1 ] && [ $1 == "all" ]; then
    cd build/debug/src/compute/sql/test && ctest 
elif [ $# == 1 ] && [ $1 == "sql" ]; then
    cd build/debug/src/compute/sql/test && ctest
else
    echo "请输入正确的参数! 语法："
    echo "./testshell.sh <option>"
    echo "option取值："
    echo "all：执行所有单元测试用例"
    echo "sql：仅执行sql引擎单元测试用例"
fi
