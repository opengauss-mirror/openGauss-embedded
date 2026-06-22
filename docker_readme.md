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
docker build -t intarkdb:arm64 -f Dockerfile.arm64-ubuntu20.04 --build-arg MAKE_JOBS=8 --build-arg BUILD_TARGET=test .
```

### 2、运行单元测试与收集全量覆盖率
镜像构建完成后，启动容器并依次执行 UT 测试用例 (`testshell.sh`) 与代码覆盖率收集 (`lcov`)。运行完毕后会自动清理容器 (`--rm`)。
```bash
docker run --rm intarkdb:arm64 bash -c "bash testshell.sh all && lcov --capture --directory . --output-file run.info && lcov --list run.info"
```
*注：上述流程已经内置了修复 C++ 模板在较新 GCC 版本下的 `mismatched exception tag` 错误配置，并默认开启了分支覆盖率统计。*

### 3、计算增量代码覆盖率并导出 HTML 报告
在持续集成或代码审查阶段，我们通常需要计算当前分支相对于主干分支（如 `master`）的**增量代码覆盖率**，并将可视化的 HTML 报告保存到宿主机。
此时可以利用 Dockerfile 中已经打包好的 `diff-cover` 工具，结合 Docker 的 `-v` 挂载功能：

```bash
# 1. 在宿主机新建用于接收报告的目录
mkdir -p $(pwd)/coverage_report

# 2. 运行容器，通过 -v 挂载导出目录和只读的 .git 历史
docker run --rm \
  -v $(pwd)/coverage_report:/out \
  -v $(pwd)/.git:/workspace/openGauss-embedded/.git:ro \
  intarkdb:arm64 \
  bash -c "bash testshell.sh all && \
           lcov --capture --directory . --output-file run.info && \
           lcov_cobertura run.info -o coverage.xml && \
           diff-cover coverage.xml --compare-branch=master --html-report /out/diff_coverage.html"
```
*注：`--compare-branch` 可以根据需要替换为 `origin/main`、`origin/master` 等。执行完成后，在宿主机的 `coverage_report` 目录下直接双击打开 `diff_coverage.html` 即可直观查看增量覆盖情况。*

### 4、如何阅读增量覆盖率报告 (HTML)
打开 `diff_coverage.html` 后，你可以直观地看到代码的覆盖情况：

- **红色背景 (Missing)**：表示该行是**可执行代码**，但在运行测试用例时**未被执行**（即未覆盖）。
- **无背景色 (Normal)**：表示该行**已被覆盖**，或者是**非可执行代码**（如大括号、注释等）。

> **说明**：`diff-cover` 工具的设计理念是**“只暴露问题”**，因此它只会把**没有被覆盖到的增量代码标红**，以提醒开发者补充用例。已经覆盖的代码会保持和普通文本一样的无背景色，以减少视觉干扰。

**常见疑问：为什么 `} else {` 也没有被标红？**
在报告中，你可能会发现像 `if (flag > 0) {` 这样的逻辑判断被标红（未覆盖），但紧接着的 `} else {` 却像普通文本一样没有背景色。
这是因为代码覆盖率是基于**编译后的汇编机器指令**进行统计的。`} else {` 仅仅是高级语言的语法结构，编译器在处理它时，不会为 `} else {` 这几个字符生成独立的机器指令（真正的分支跳转指令绑定在 `if` 所在的那一行）。
因此，覆盖率工具将其视为**“非可执行代码”**（类似于注释、空行、单独的大括号 `}` 等），直接跳过统计。开发者只需要重点关注那些带有红色背景的行即可。


