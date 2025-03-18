#####
##go语言简易SQL客户端测试程序
#####

##
##依赖说明
go接口依赖intarkdb动态库
需要在intarkdb_interface/intarkdb_interface.go中指定动态库路径
#cgo LDFLAGS: -L${SRCDIR}/../../../../output/release/lib -lintarkdb -Wl,-rpath=${SRCDIR}/../../../../output/release/lib

依赖头文件
intarkdb_interface/include/intarkdb.h

##
##运行
1）搭建go语言环境，要求go版本1.18
2）添加项目动态库路径到系统路径
export LD_LIBRARY_PATH=项目路径/intarkdb_interface/lib:$LD_LIBRARY_PATH
3）go mod tidy // go第三方包依赖
4）go run main.go 也可以带目录运行: go run main.go -path database_dir （database_dir为intarkdb数据库目录）
不带目录运行会在当前目录下创建intarkdb数据库目录

##
##交叉编译
export LD_LIBRARY_PATH=项目路径/intarkdb_interface/lib:$LD_LIBRARY_PATH
export GOOS=linux
export CC="/usr/local/gcc-linaro-7.5.0-2019.12-x86_64_arm-linux-gnueabihf/bin/arm-linux-gnueabihf-gcc" //交叉编译工具，和交叉编译intarkdb时相同
export GOARCH=arm
export GOARM=7
export CGO_ENABLED=1
go build main.go

##
##辅助工具
用户可以通过二进制程序intarkdb_cli与instardb进行交互，实现增删改查功能

运行方式
./intarkdb_cli  绝对路径
运行后.help 查看帮助
.quit 退出

例如：
当前绝对路径为/home/go/go-api，该路径下有二进制程序intarkdb_cli和数据库文件intarkdb
则可以通过如下命令运行
./intarkdb_cli /home/go/go-api
注：绝对路径如果以instardb结尾则在instardb内部创建打开instardb，而不是打开当前路径下的instardb