# openGauss-embedded

This repository contains the source code for the openGauss-embedded database, along with various testing tools.

#### 1. Project Description

##### 1. Programming language: C/C++

##### 2. Compilation project: CMake or Make

##### 3. Directories:

-   `src`: the source code directory, where modules are decoupled by subdirectory
-   `build`: compilation and build directory
-	`dependency`: third-party dependencies
-   `example`: usage examples for C/C++/Go/Python APIs
-   `tools`: tools

#### 2. Compilation Guide

##### 1. OS and Software Dependencies

The following OSs are supported:
-   CentOS 7.6
-   openEuler 20.03 LTS
-   Ubuntu 20.04.6 LTS
-   Windows
-   For other systems, see the compilation guide.


Required tools:
-   GCC 7.3 or later
-   CMake 3.15 or later

##### 2. Compiling on UNIX-like Systems

- `make debug`: compiles the debug version.
- `make/make release`: compiles the release version.
- `make test`: compiles the debug version and the `src/compute/sql/test` directory.
- `make debug_lite`: compiles the <span style="color: red;">lightweight</span> debug version.
- `make release_lite`: compiles the <span style="color: red;">lightweight</span> release version.
- `make test_lite`: compiles the <span style="color: red;">lightweight</span> debug version and the `src/compute/sql/test` directory.
- `make install`: installs the compiled release version.
- `make install_debug`: installs the compiled debug version
- `make uninstall`: uninstalls both release and debug versions.

##### 3. Compiling on Windows

Currently, Windows compilation is supported only with MinGW. Ensure MinGW, CMake, and Make are installed and configured in your environment. Run `mingw32-make.exe` in the project directory to compile.

##### 4. Cross-Compilation for Arm
###### Arm Linux Environment
Install or download your cross-compilation toolchain.
Modify the Arm32 cross-compiler settings in `CMakeLists.txt`. For example:
set(CMAKE_C_COMPILER arm-linux-gnueabihf-gcc)
set(CMAKE_CXX_COMPILER arm-linux-gnueabihf-g++)
-make OS_ARCH=arm32
###### Arm OpenHarmony Environment
Prepare the cross-toolchain and header files. Pass these paths to the build script via command line arguments.
For OpenHarmony Arm32:
- make OS_ARCH=arm32 TOOLCHAIN_PATH="/opt/native/llvm" OHOS_SYSROOT_PATH="/opt/native/sysroot"

   Note: `OS_ARCH` is the target platform, `TOOLCHAIN_PATH` is the toolchain location, and `OHOS_SYSROOT_PATH` is the header file path.
For OpenHarmony AArch64:
- make OS_ARCH=aarch64 TOOLCHAIN_PATH="/opt/native/llvm" OHOS_SYSROOT_PATH="/opt/native/sysroot"

##### 5. Compiling in the Android NDK Environment
Using the official Android NDK, place the `intarkdb` project into your Android project's `cpp` folder.
Modify the `build.gradle` file in your Android project's `src` directory. Add the `externalNativeBuild` and `ndk` attributes under `android -> defaultConfig`, and specify the `CMakeLists.txt` path. For example:
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
    // Other attributes, such as namespace, compileSdk, compileOptions, and buildFeatures, are configured as required.
}
```
Then, compile the project.

#### 3. Running Guide

After compilation, an `output` directory is generated in the project directory. Two test demos are provided to verify the embedded database.

```
The structure of the output directory is as follows:
├── debug # Executables and libraries for debug builds (make/make debug/make test)
│ ├── bin # Debug executables
│ └── lib # Debug libraries
├── inc # Header files for third-party dependencies
│   ├── cJSON
│   ├── huawei_security
│   ├── libpg_query
│   ├── libutf8proc
│   ├── zlib
│   └── fmt 
└── release # Executables and libraries for release builds (make release)
    ├── bin # Release executables
    └── lib # Release libraries
```

Navigate to the `output/`<span style="color: red;"><debug|release></span>`/bin/` directory to launch the client test tool `intarkdb_cli`. You can specify the data file path as an argument. For example, `./intarkdb_cli test`.

1. Run unit tests.
    First, run `make test` to compile the test suite.
    Then execute the tests: `bash testshell.sh sql`.
    Note:
    -   `sql`: Runs the SQL engine unit test cases.

2. Test SQL type keywords.
   Go to the `build/debug/src/compute/sql/test` directory.  
   Run `./pg_type_test` to test the 34 standard SQL statement type keywords.
   Run `./pg_type_test [type keyword]` to verify if a specific keyword is supported by the SQL engine.

3. Print table metadata.
   Copy the `assist_test` file from `build/debug/src/compute/sql/test` to your database directory.
   Run `./assist_test <table name>`.

#### 4. Development Guide
##### 1. Add the configuration flushing to make parameters configurable.
  1. Step 1: Add the configuration item to the `g_parameters` array in `src/storage/gstor/gstor_param.c` file, for example, `{"STACK_SIZE",GS_TRUE,ATTR_NONE,"512K","512K",NULL , "-", "-"," GS_TYPE_INTEGER",GS_FALSE },`
  2. Step 2: Add the logic to read the configuration item in the `static status_t gstor_load_param_config(void)` function: `GS_RETURN_IFERR(knl_param_get_size_uint32(g_config, "STACK_SIZE", &g_instance->attr.stack_size));`
