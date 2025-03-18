# Ncti Orm

# Ncti Orm

#### 一、工程说明
##### 1、编程语言：Rust
##### 2、编译工程：Cargo
##### 3、目录说明：
-   src: 源代码目录，按子目录划分模块解耦；
-   依赖工程代码
    rbs  //rbatis 工具
    macro_driver  //创建表宏，创建索引宏，crud宏
    codegen  //工具类 ，model
    interface  //rust - C接口类

#### 二、编译指导
##### 1、概述
编译国创Rust 需要依赖gstor ,需要先编译运行gstor生成依赖包 。
##### 2、操作系统和软件依赖要求
支持以下操作系统：
-   CentOS 7.6（x86）
-   Cargo
##### 3、编译工具
1.VS code   remote ssh
2.Idea  remote ssh
需要安装rust插件
安装rust环境问题解决：
1.rust环境搭建
https://www.runoob.com/rust/rust-setup.html
2.不做 https 下载验证  ，curl ssl 验证失败
https://www.codenong.com/35770372/
	$ curl -s https://install.meteor.com | grep curl
	curl --progress-bar --fail"$TARBALL_URL" | tar -xzf - -C"$INSTALL_TMPDIR" -o
要将curl按下，可以在curlrc中添加--insecure：
	$ echo insecure >> ~/.curlrc
或者只使用http：
	$ curl http://install.meteor.com
3.Vscode Rust  linux环境报错
"/root/.vscode-server/extensions/rust-lang.rust-analyzer-0.3.1394/server/rust-analyzer: /lib64/libc.so.6: version `GLIBC_2.25' not found (required by /root/.vscode-server/extensions/rust-lang.rust-analyzer-0.3.1394/server/rust-analyzer)\n"
升级 libc
https://blog.csdn.net/LJL_1003/article/details/127968993

##### 4、代码编译

#####DEMO示例
运行src 下的main
