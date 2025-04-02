## 生成JDBC依赖 so包
## 编译步骤 & 执行步骤
## 注意：需要把 /interface/CMakeLists.txt 下的# add_subdirectory(java) 修改成 add_subdirectory(java)才可以编译运行java的jni so包
1. 在目录直接 make 即可编译（会自动编译存储引擎和SQL引擎的代码）
   - make/make debug: 编译生成debug版本
   - make test: 编译生成debug版本，同时会编译test目录
   - make release: 编译生成release版本
2. 生成的SQL引擎相关的库文件和测试程序在output/<span style="color: red;"><debug|release></span>/lib目录下
3. 生成的SQL引擎的动态库是output/<span style="color: red;"><debug|release></span>/lib目录下的libintarkdb.so， libsecurec.so，libgstor_jni.so
   
   java jni的动态库是libgstor_jni.so
4. 将数据库相关so : libsecurec.so,libintarkdb.so,libintarkdb_jni.so
   文件引入到JDBC项目 interface/java/jdbc/src/main/resources/org/intarkdb/native/Linux 对应的系统文件中，
   JDBC就可以引用动态库，进行数据库相关操作。

例如：运行系统是X86_64位，则将libsecurec.so,libintarkdb.so,libintarkdb_jni.so
   放在interface/java/jdbc/src/main/resources/org/intarkdb/native/Linux/x86_64 文件夹中。

   aarch 64位，则放在对应的aarch64文件下。

5. 在JDBC项目中执行mvn相关命令可以打包jdbc jar

6.jdbc版本支持JDK1.8及以上版本。

