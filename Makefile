.PHONY:  debug release test clean release_lite debug_lite test_lite install uninstall

ARCH := $(shell getconf LONG_BIT)
SHELL_NAME := $(shell uname)

ifneq ($(findstring $(OS_ARCH), arm32 aarch64),)
	ifeq ($(and $(TOOLCHAIN_PATH),$(OHOS_SYSROOT_PATH)),)
		# $(warning "Please give the TOOLCHAIN_PATH or OHOS_SYSROOT_PATH")
		# exit -1
	endif
else
OS_ARCH=""
TOOLCHAIN_PATH=""
OHOS_SYSROOT_PATH=""
ifeq ($(OS),Windows_NT)
@echo "Windows System: ARCH=$(ARCH)"
ifneq (, $(findstring MINGW, $(SHELL_NAME)))
CMAKE_PARAMS := -G "MinGW Makefiles" 
endif
endif
endif

all: release

release:
	mkdir -p build/release
	cd build/release && \
	cmake $(CMAKE_PARAMS) -DCMAKE_BUILD_TYPE=Release  -DUT=OFF -DENABLE_GCOV=OFF -DENABLE_DCC_LITE=ON \
	-DENABLE_PG_QUERY=ON \
	-DENABLE_MEMCHECK=OFF -DENABLE_EXPORT_API=ON -DSTATISTICS=OFF -DENABLE_BACKUP=OFF \
	-DENABLE_LIBAIO=OFF -DSSL=OFF -DCRYPTO=OFF -DBUILD_TESTS=OFF -DOS_ARCH=$(OS_ARCH) -DTOOLCHAIN_PATH=$(TOOLCHAIN_PATH) -DOHOS_SYSROOT_PATH=$(OHOS_SYSROOT_PATH) ../.. && \
	$(MAKE)


debug:
	mkdir -p build/debug
	cd build/debug && \
	cmake $(CMAKE_PARAMS) -DCMAKE_BUILD_TYPE=Debug -DENABLE_PRECOMPILED_HEADERS=OFF -DUT=OFF -DENABLE_GCOV=OFF -DENABLE_DCC_LITE=ON \
	-DENABLE_PG_QUERY=ON \
	-DENABLE_MEMCHECK=OFF -DENABLE_EXPORT_API=ON -DSTATISTICS=OFF -DENABLE_BACKUP=OFF \
	-DENABLE_LIBAIO=OFF -DSSL=OFF -DCRYPTO=OFF -DBUILD_TESTS=OFF -DOS_ARCH=$(OS_ARCH) -DTOOLCHAIN_PATH=$(TOOLCHAIN_PATH) -DOHOS_SYSROOT_PATH=$(OHOS_SYSROOT_PATH) ../.. && \
	$(MAKE)

test:
	mkdir -p build/debug
	cd build/debug && \
	cmake $(CMAKE_PARAMS) -DCMAKE_BUILD_TYPE=Debug  -DUT=OFF -DENABLE_GCOV=OFF -DENABLE_DCC_LITE=ON \
	-DENABLE_PG_QUERY=ON \
	-DENABLE_MEMCHECK=OFF -DENABLE_EXPORT_API=ON -DSTATISTICS=OFF -DENABLE_BACKUP=OFF \
	-DENABLE_LIBAIO=OFF -DSSL=OFF -DCRYPTO=OFF -DBUILD_TESTS=ON -DOS_ARCH=$(OS_ARCH) -DTOOLCHAIN_PATH=$(TOOLCHAIN_PATH) -DOHOS_SYSROOT_PATH=$(OHOS_SYSROOT_PATH) ../.. && \
	$(MAKE)

clean:
	rm -rf build/debug
	rm -rf build/debug_lite
	rm -rf build/release
	rm -rf build/release_lite
	rm -rf output/*

release_lite:
	mkdir -p build/release_lite
	cd build/release_lite && \
	cmake $(CMAKE_PARAMS) -DCMAKE_BUILD_TYPE=Release  -DUT=OFF -DENABLE_GCOV=OFF -DENABLE_DCC_LITE=ON \
	-DENABLE_PG_QUERY=ON \
	-DENABLE_INTARK_LITE=ON -DENABLE_MEMCHECK=OFF -DENABLE_EXPORT_API=ON -DSTATISTICS=OFF \
	-DENABLE_BACKUP=OFF -DENABLE_LIBAIO=OFF -DSSL=OFF -DCRYPTO=OFF -DBUILD_TESTS=OFF -DOS_ARCH=$(OS_ARCH) -DTOOLCHAIN_PATH=$(TOOLCHAIN_PATH) -DOHOS_SYSROOT_PATH=$(OHOS_SYSROOT_PATH) ../.. && \
	$(MAKE)

debug_lite:
	mkdir -p build/debug_lite
	cd build/debug_lite && \
	cmake $(CMAKE_PARAMS) -DCMAKE_BUILD_TYPE=Debug  -DUT=OFF -DENABLE_GCOV=OFF -DENABLE_DCC_LITE=ON \
	-DENABLE_PG_QUERY=ON \
	-DENABLE_INTARK_LITE=ON -DENABLE_MEMCHECK=OFF -DENABLE_EXPORT_API=ON -DSTATISTICS=OFF \
	-DENABLE_BACKUP=OFF -DENABLE_LIBAIO=OFF -DSSL=OFF -DCRYPTO=OFF -DBUILD_TESTS=OFF -DOS_ARCH=$(OS_ARCH) -DTOOLCHAIN_PATH=$(TOOLCHAIN_PATH) -DOHOS_SYSROOT_PATH=$(OHOS_SYSROOT_PATH) ../.. && \
	$(MAKE)

test_lite:
	mkdir -p build/debug_lite
	cd build/debug_lite && \
	cmake $(CMAKE_PARAMS) -DCMAKE_BUILD_TYPE=Debug  -DUT=OFF -DENABLE_GCOV=OFF -DENABLE_DCC_LITE=ON \
	-DENABLE_PG_QUERY=ON \
	-DENABLE_INTARK_LITE=ON -DENABLE_MEMCHECK=OFF -DENABLE_EXPORT_API=ON -DSTATISTICS=OFF \
	-DENABLE_BACKUP=OFF -DENABLE_LIBAIO=OFF -DSSL=OFF -DCRYPTO=OFF -DBUILD_TESTS=ON -DOS_ARCH=$(OS_ARCH) -DTOOLCHAIN_PATH=$(TOOLCHAIN_PATH) -DOHOS_SYSROOT_PATH=$(OHOS_SYSROOT_PATH) ../.. && \
	$(MAKE)

install:
	cd build/release && \
	$(MAKE) install

install_debug:
	cd build/debug && \
	$(MAKE) install

uninstall:
	@echo "Current directory: $(shell pwd)"; \
	if [ -d build/release ]; then \
		echo "Found build/release, proceeding to uninstall..."; \
		pushd build/release > /dev/null; \
		$(MAKE) uninstall; \
		popd > /dev/null; \
	else \
		echo "build/release does not exist."; \
	fi; \
	if [ -d build/debug ]; then \
		echo "Found build/debug, proceeding to uninstall..."; \
		pushd build/debug > /dev/null; \
		$(MAKE) uninstall; \
		popd > /dev/null; \
	else \
		echo "build/debug does not exist."; \
	fi