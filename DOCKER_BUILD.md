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

```bash
docker build -f Dockerfile.x86_64-ubuntu20.04 \
  --build-arg MAKE_JOBS=8 \
  -t intarkdb-build-x86_64 .
```

### 2. Enable optional release verification

The ARM Dockerfile defaults to verifying `GCOV + make debug` only.
If you also want it to run `make release`, pass `VERIFY_RELEASE=1`.

```bash
docker build -f Dockerfile.arm64-ubuntu20.04 \
  --build-arg VERIFY_RELEASE=1 \
  -t intarkdb-build-arm64 .
```

You can combine both options:

```bash
docker build -f Dockerfile.arm64-ubuntu20.04 \
  --build-arg MAKE_JOBS=8 \
  --build-arg VERIFY_RELEASE=1 \
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

