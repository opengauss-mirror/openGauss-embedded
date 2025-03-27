# Opengauss-embedded

此仓库包含Opengauss-embedded嵌入式数据库的源代码 ，还包括一些测试工具。

#### 一、工程说明

##### 1、编程语言：C/C++

##### 2、编译工程：cmake/make

##### 3、目录说明：

-   src: 源代码目录，按子目录划分模块解耦；
-   build：编译构建目录
-	dependency: 第三方依赖
-   example: c/c++/golang/python接口使用样例
-   tools: 工具项

#### 二、编译指导

##### 1、操作系统和软件依赖要求

支持以下操作系统：
-   CentOS 7.6
-   OpenEuler 20.03-LTS
-   Ubuntu 20.04.6-LTS
-   Windows
-   适配其他系统，可参照编译


编译工具：
-   GCC >= 7.3
-   CMAKE >= 3.15

##### 2、在类UNIX系统编译

- make debug: 编译生成debug版本
- make/make release: 编译生成release版本
- make test: 编译生成debug版本，同时会编译src/compute/sql/test目录
- make debug_lite: 编译生成<span style="color: red;">轻量化</span>debug版本
- make release_lite: 编译生成<span style="color: red;">轻量化</span>release版本
- make test_lite: 编译生成<span style="color: red;">轻量化</span>debug版本，同时会编译src/compute/sql/test目录
- make install: 对编译生成的release版本进行安装
- make install_debug: 对编译生成的debug版本进行安装
- make uninstall: 卸载通过make install安装的release版本和通过make install_debug安装的debug版本

##### 3、在windwos上编译

当前在windows上编译暂时只支持通过minGW工具进行编译，需要安装minGW/cmake/make，前提环境准备好后，在工程目录，执行
mingw32-make.exe 进行编译。

##### 4、交叉编译ARM环境
###### 交叉编译arm linux环境
准备好交叉编译工具链，可以选择安装或者下载。
修改CMakelists.txt中arm32的交叉编译器 如：
set(CMAKE_C_COMPILER arm-linux-gnueabihf-gcc)
set(CMAKE_CXX_COMPILER arm-linux-gnueabihf-g++)
-make OS_ARCH=arm32
###### 交叉编译ARM openharmony环境
交叉编译需要配置交叉编译环境，需要将鸿蒙交叉工具链和头文件准备好，并通过命令传入到编译脚本中
例如：编译openharmony arm32命令：
- make OS_ARCH=arm32 TOOLCHAIN_PATH="/opt/native/llvm" OHOS_SYSROOT_PATH="/opt/native/sysroot"
其中OS_ARCH为编译目标平台，TOOLCHAIN_PATH为编译工具链地址，OHOS_SYSROOT_PATH为头文件路径
编译openharmony aarch64命令：
- make OS_ARCH=aarch64 TOOLCHAIN_PATH="/opt/native/llvm" OHOS_SYSROOT_PATH="/opt/native/sysroot"

##### 5、在安卓ndk环境编译
使用安卓官方ndk工具，将intarkdb项目放进安卓的cpp文件夹下
修改安卓项目的src目录下的build.gradle文件，在android - defaultConfig属性下添加externalNativeBuild和ndk属性，及使用externalNativeBuild属性指定CmakeLists.txt的路径，如
```gradle
android {
    defaultConfig {
        externalNativeBuild {
            cmake {
                cppFlags "-landroid -std=c++17 -Wno-error=format-security -DANDROID=On  -DUT=OFF -DENABLE_GCOV=OFF -DENABLE_MEMCHECK=OFF -DENABLE_EXPORT_API=ON -DSTATISTICS_O=OFF -DENABLE_BACKUP=OFF -DENABLE_LIBAIO=OFF -DSSL=OFF -DCRYPTO=OFF -DBUILD_TESTS=OFF"
                arguments "-DANDROID_STL=c++_static",  "-DOS_ARCH=aarch64",  "-DCMAKE_BUILD_TYPE=Debug", "-DENABLE_DCC_LITE=ON"
            }
        }
        ndk {
            abiFilters 'arm64-v8a'
        }
    }
    externalNativeBuild {
        cmake {
            path file('src/main/cpp/openGauss-embedded/CMakeLists.txt')
            version '3.22.1'
        }
    }
    // 其余属性如namespace、compileSdk、compileOptions、buildFeatures等按需配置，在此省略
}
```
然后编译项目即可

#### 三、运行指导

编译完成后，会在工程目录下生成output目录，当前生成两个测试demo提供对嵌入式数据库的测试。

```
output目录结构如下：
├── debug   # 使用make/make debug/make test编译时debug版本存储引擎和SQL引擎的可执行文件和库文件保存路径
│   ├── bin # debug版本可执行文件保存路径
│   └── lib # debug版本库文件保存路径
├── inc     # 第三方库文件的头文件保存路径，同以前
│   ├── cJSON
│   ├── huawei_security
│   ├── libpg_query
│   ├── libutf8proc
│   ├── zlib
│   └── fmt 
└── release  # 使用make release 编译时release版本存储引擎和SQL引擎的可执行文件和库文件保存路径
    ├── bin  # release版本可执行文件保存路径
    └── lib  # release版本库文件保存路径
```

进入 output/<span style="color: red;"><debug|release></span>/bin/目录， 可以启动客户端测试工具intarkdb_cli,后面可以跟参数指定数据文件位置 eg: ./intarkdb_cli test

1. 运行单例测试：
    先执行`make test`进行编译测试
    运行测试: `bash testshell.sh sql`
    说明: 
    -   sql: 运行sql引擎单元测试用例

2. 测试sql语句支持的类型字符串
   进入build/debug/src/compute/sql/test目录   
   执行`./pg_type_test` 时，会测试固定的34中sql语句类型关键字
   执行`./pg_type_test [type keyword]`时，会测试[type keyword]是否是sql-engine支持的类型关键字

3. 打印指定表元信息:
   将build/debug/src/compute/sql/test目录下的assist_test拷贝到数据库所在的目录
   执行`./assist_test <table name>`

#### 四、开发指南
##### 1、增加配置落盘,使参数可配置
  1. 第一步，在src/storage/gstor/gstor_param.c 文件中 g_parameters 数组中增加对应配置项, 如 ：`{"STACK_SIZE",GS_TRUE,ATTR_NONE,"512K","512K",NULL, "-", "-","GS_TYPE_INTEGER",GS_FALSE },`
  2. 第二步，在 static status_t gstor_load_param_config(void)函数中增加读取对应配置项的代码：`GS_RETURN_IFERR(knl_param_get_size_uint32(g_config, "STACK_SIZE", &g_instance->attr.stack_size));`
