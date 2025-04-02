open source target name £ºopenssl
source code repository : product warehouse
compile dependency: NULL
upgrade open source package method£º
----|pull command : python $(pwd)../../build/pull_open_source.py "path" "name" "id"
      |----path : the parent directory name
      |----name£ºthe package name in product warehouse
      |----id£ºpdm version id
the compile command : python build.py -m all -f openssl-1.1.1g.tar.gz -t "comm|llt"
To meet the requirements of compiler security options, both comm and llt are required.
comm is used to build lib and include files, and llt is used to build bin/openssl.
Patch Info: None