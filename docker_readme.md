# Docker Build Instructions

This document describes how to build the Docker images for the community
version of `intarkdb`.

## Files

- `Dockerfile.arm64-ubuntu20.04`
  - Target environment: `arm64/aarch64 + Ubuntu 20.04`
  - Default verification path: enable `GCOV`, then run parallel `make debug`

- `Dockerfile.x86_64-ubuntu20.04`
  - Target environment: `x86_64/amd64 + Ubuntu 20.04`

## Basic Commands

### 1. Build ARM64 Ubuntu 20.04 image

```bash
docker build -f Dockerfile.arm64-ubuntu20.04 -t intarkdb-build-arm64 .
```

### 2. Build x86_64 Ubuntu 20.04 image

```bash
docker build -f Dockerfile.x86_64-ubuntu20.04 -t intarkdb-build-x86_64 .
```

## Common Build Args

### 1. Specify parallel build jobs

If not specified, the Dockerfile uses `nproc` inside the container.

Example: use 8 parallel jobs.

```bash
docker build -f Dockerfile.arm64-ubuntu20.04 \
  --build-arg MAKE_JOBS=8 \
  -t intarkdb-build-arm64 .
```

### 2. Specify build target

The Dockerfiles default to building the `test` target.
You can specify a different target (e.g., `release`, `debug`, `test_lite`) using `BUILD_TARGET`.

If the `BUILD_TARGET` contains `debug` or `test`, the Dockerfile will automatically enable the GCOV switch before building. Otherwise, it will build the target without GCOV.

```bash
docker build -f Dockerfile.arm64-ubuntu20.04 \
  --build-arg BUILD_TARGET=release \
  -t intarkdb-build-arm64 .
```

You can combine both options:

```bash
docker build -f Dockerfile.arm64-ubuntu20.04 \
  --build-arg MAKE_JOBS=8 \
  --build-arg BUILD_TARGET=test \
  -t intarkdb-build-arm64 .
```

## Notes

- Run `docker build` in the repository root directory.
- The final `.` in the command is required. It means the current repository is
  used as the Docker build context.
- `.dockerignore` controls which files are sent into the Docker build context.
- The ARM Dockerfile has been verified with:
  - `GCOV` enabled
  - parallel `make debug`

## Docker 环境下构建与 UT 覆盖率收集
本项目提供了基于 Ubuntu 20.04 的 Dockerfile，可一键完成编译、测试及 GCOV 代码覆盖率的收集。

### 1、构建 Docker 镜像
在项目根目录下，执行以下命令构建 ARM64 架构的镜像（可根据宿主机替换为 `Dockerfile.x86_64-ubuntu20.04`）：
```bash
docker build -t intarkdb:arm64 -f Dockerfile.arm64-ubuntu20.04 --build-arg MAKE_JOBS=8 .
```

### 2、运行单元测试与收集覆盖率
镜像构建完成后，启动容器并依次执行 UT 测试用例 (`testshell.sh`) 与代码覆盖率收集 (`lcov`)。运行完毕后会自动清理容器 (`--rm`)。
```bash
docker run --rm intarkdb:arm64 bash -c "bash testshell.sh all && lcov --capture --directory . --output-file run.info && lcov --list run.info"
```
*注：上述流程已经内置了修复 C++ 模板在较新 GCC 版本下的 `mismatched exception tag` 错误配置，并默认开启了分支覆盖率统计。*


