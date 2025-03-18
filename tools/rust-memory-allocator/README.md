# 使用说明

## 编译

在本项目顶级目录下直接运行

```bash
make
```

即可。

## 生成文件说明

- `./target/debug/libc_call_rust.so`：Rust 编译出来的供 C 调用的动态库；
- `./src/build/main`：C 编译出的可执行程序。
