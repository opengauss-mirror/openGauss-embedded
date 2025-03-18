cmake_minimum_required(VERSION 3.14.1)
project(GmSSL)

SET(CMAKE_PROJECT_HOMEPAGE_URL "http://gmssl.org")

SET(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include_directories(include)

set(src
	src/version.c
	src/debug.c
	src/sm4_common.c
	src/sm4_enc.c
	src/sm4_modes.c
	src/sm4_setkey.c
	src/sm3.c
	src/sm3_hmac.c
	src/sm3_kdf.c
	src/sm2_alg.c
	src/sm2_key.c
	src/sm2_lib.c
	src/sm9_alg.c
	src/sm9_key.c
	src/sm9_lib.c
	src/zuc.c
	src/zuc_modes.c
	src/aes.c
	src/aes_modes.c
	src/sha256.c
	src/sha512.c
	src/chacha20.c
	src/hash_drbg.c
	src/block_cipher.c
	src/digest.c
	src/hmac.c
	src/hkdf.c
	src/pbkdf2.c
	src/gf128.c
	src/gcm.c
	src/aead.c
	src/pkcs8.c
	src/ec.c
	src/rsa.c
	src/asn1.c
	src/hex.c
	src/base64.c
	src/pem.c
	src/x509_alg.c
	src/x509_cer.c
	src/x509_ext.c
	src/x509_req.c
	src/x509_crl.c
	src/x509_new.c
	src/cms.c
	src/sdf/sdf.c
	src/sdf/sdf_lib.c
	src/sdf/sdf_meth.c
	src/sdf/sdf_ext.c
	src/sdf/sdf_sansec.c
	src/skf/skf.c
	src/skf/skf_lib.c
	src/skf/skf_meth.c
	src/skf/skf_ext.c
	src/skf/skf_prn.c
	src/skf/skf_wisec.c
	src/socket.c
	src/tls.c
	src/tls_ext.c
	src/tls_trace.c
	src/tlcp.c
	src/tls12.c
	src/tls13.c
	src/file.c
)

# when an option has been enabled, `cmake ..` will not refresh the value
# use `cmake .. -DENABLE_XXX=OFF` to disable the option

option(ENABLE_SM2_ALGOR_ID_ENCODE_NULL "Enable AlgorithmIdenifier with algorithm sm2sign_with_sm3 encode a NULL object as parameters" OFF)
if (ENABLE_SM2_ALGOR_ID_ENCODE_NULL)
	message(STATUS "ENABLE_SM2_ALGOR_ID_ENCODE_NULL")
	add_definitions(-DSM2_ALGOR_ID_ENCODE_NULL)
endif()


option(ENABLE_SM2_PRIVATE_KEY_EXPORT "Enable export un-encrypted SM2 private key" OFF)
if (ENABLE_SM2_PRIVATE_KEY_EXPORT)
	message(STATUS "ENABLE_SM2_PRIVATE_KEY_EXPORT")
	add_definitions(-DSM2_PRIVATE_KEY_EXPORT)
	list(APPEND demos demo_sm2_key_export)
endif()


option(ENABLE_TLS_DEBUG "Enable TLS and TLCP print debug message" OFF)
if (ENABLE_TLS_DEBUG)
	message(STATUS "ENABLE_TLS_DEBUG")
	add_definitions(-DTLS_DEBUG)
endif()

option(ENABLE_SM3_AVX_BMI2 "Enable SM3 AVX+BMI2 assembly implementation" OFF)
if (ENABLE_SM3_AVX_BMI2)
	message(STATUS "ENABLE_SM3_AVX_BMI2")
	add_definitions(-DSM3_AVX_BMI2)
	enable_language(ASM)
	list(APPEND src src/sm3_avx_bmi2.s)
endif()


option(ENABLE_SM4_AESNI_AVX "Enable SM4 AESNI+AVX assembly implementation" OFF)
if (ENABLE_SM4_AESNI_AVX)
	message(STATUS "ENABLE_SM4_AESNI_AVX")
	add_definitions(-DSM4_AESNI_AVX)
	list(APPEND src src/sm4_aesni_avx.c)
	list(APPEND tests sm4_aesni_avx)
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")
endif()


option(ENABLE_SM2_EXTS "Enable SM2 Extensions" OFF)
if (ENABLE_SM2_EXTS)
	message(STATUS "ENABLE_SM4_AESNI_AVX")
	list(APPEND src
		src/sm2_key_share.c
		src/sm2_recover.c
		src/sm2_blind.c
		src/sm2_ring.c
		src/sm2_elgamal.c
		src/sm2_commit.c)
	list(APPEND tests sm2_key_share sm2_blind sm2_ring sm2_elgamal sm2_commit)
endif()


option(ENABLE_BROKEN_CRYPTO "Enable broken crypto algorithms" OFF)
if (ENABLE_BROKEN_CRYPTO)
	message(STATUS "ENABLE_BROKEN_CRYPTO")
	list(APPEND src src/des.c src/sha1.c src/md5.c src/rc4.c)
	list(APPEND tests des sha1 md5 rc4)
endif()


option(ENABLE_RDRND "Enable Intel RDRND instructions" OFF)
if (${CMAKE_SYSTEM_PROCESSOR} MATCHES x86_64)
	set(ENABLE_RDRND ON)
endif()
# OS_ARCH is arm32 or aarch64 
if (OS_ARCH)
	if (${OS_ARCH} STREQUAL "arm32" OR ${OS_ARCH} STREQUAL "aarch64")
		set(ENABLE_RDRND OFF)
	endif()
endif()


if (ENABLE_RDRND)
	message(STATUS "ENABLE_RDRND")
	list(APPEND src src/rdrand.c)
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mrdrnd -mrdseed")
endif()

option(ENABLE_GMT_0105_RNG "Enable GM/T 0105 Software RNG" OFF)
if (ENABLE_GMT_0105_RNG)
	message(STATUS "ENABLE_GMT_0105_RNG")
	list(APPEND src src/sm3_rng.c src/sm4_cbc_mac.c src/sm4_rng.c)
	list(APPEND tests sm3_rng sm4_cbc_mac sm4_rng)
endif()

if (WIN32)
	list(APPEND src src/rand_win.c src/http_win.c)
elseif (APPLE)
	list(APPEND src src/rand_apple.c src/http.c)
elseif (ANDROID)
	list(APPEND src src/rand.c src/http.c)
else()
	list(APPEND src src/rand.c src/http.c)
endif()

option(ENABLE_HTTP_TESTS "Enable HTTP GET/POST related tests" OFF)
if (ENABLE_HTTP_TESTS)
	message(STATUS "ENABLE_HTTP_TESTS")
	list(APPEND tests http http_crl)
endif()


option(BUILD_SHARED_LIBS "Build using shared libraries" ON)

if (CMAKE_C_COMPILER_ID MATCHES "MSVC")
	message(STATUS "CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS")
	set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON) # set before add_library
endif()

add_library(gmssl ${src})


if (WIN32)
	target_link_libraries(gmssl -lws2_32)
elseif (APPLE)
	target_link_libraries(gmssl dl)
	target_link_libraries(gmssl "-framework Security")
	#target_link_libraries(gmssl "-framework CoreFoundation") # rand_apple.c CFRelease()
elseif (MINGW)
	target_link_libraries(gmssl PRIVATE wsock32)
else()
	target_link_libraries(gmssl dl)
endif()


SET_TARGET_PROPERTIES(gmssl PROPERTIES VERSION 3.0 SOVERSION 3)


install(TARGETS gmssl ARCHIVE DESTINATION lib LIBRARY DESTINATION lib RUNTIME DESTINATION bin)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/gmssl DESTINATION include)


if (CMAKE_C_COMPILER_ID MATCHES "MSVC")
	set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY VS_STARTUP_PROJECT gmssl-bin)
	set(CMAKE_INSTALL_PREFIX "C:/Program Files/GmSSL") # change by `cmake -DCMAKE_INSTALL_PREFIX=C:\path\to\install`
	# run `set path=%path%;C:\Program Files\GmSSL\bin`
	add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
#	target_compile_options(gmssl PRIVATE /wd4996)
#	target_compile_options(gmssl-bin PRIVATE /wd4996)
endif()

if (UNIX)
	# packaging
	include(LinuxPacking)
	include(DebPacking)
	include(RpmPacking)
	include(CPack)
endif (UNIX)
