cmake_minimum_required(VERSION 3.1)
project(Realm)
include(GNUInstallDirs)
include(CheckIncludeFiles)
set(CMAKE_CXX_STANDARD 11)
enable_testing()
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/tools/cmake" ${CMAKE_MODULE_PATH})

set(REALM_VERSION_MAJOR 0)
set(REALM_VERSION_MINOR 96)
set(REALM_VERSION_PATCH 2)
set(REALM_VERSION "${REALM_VERSION_MAJOR}.${REALM_VERSION_MINOR}.${REALM_VERSION_PATCH}")

set(REALM_ENABLE_ASSERTIONS "0" CACHE STRING "Enable assertions in release mode.")
set(REALM_MAX_BPNODE_SIZE "1000" CACHE STRING "Max B+ tree node side.")
set(REALM_ENABLE_ALLOC_SET_ZERO "0" CACHE STRING "Zero all allocations.")
set(REALM_ENABLE_ENCRYPTION "0" CACHE STRING "Enable encryption.")

check_include_files(malloc.h HAVE_MALLOC_H)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

if(CMAKE_BUILD_TYPE STREQUAL Debug)
    set(REALM_DEBUG 1)
endif()

configure_file(
    ${CMAKE_SOURCE_DIR}/src/realm/util/config.h.in
    ${CMAKE_SOURCE_DIR}/src/realm/util/config.h
)

include_directories(src)

add_subdirectory(src/realm)
add_subdirectory(test)

set(CMAKE_CXX_FLAGS "-Wextra -Wpedantic -DREALM_HAVE_CONFIG" CACHE INTERNAL "")


if(APPLE)
    set(OPENSSL_ROOT_DIR "/usr/local/opt/openssl")

    set(IOS_SDK_VERSION "9.2")
    set(IOS_SDK_DEV_ROOT "/Applications/Xcode.app/Contents/Developer/Platforms/iPhoneOS.platform/Developer")
    set(IOS_SDK_ROOT "${IOS_SDK_DEV_ROOT}/SDKs/iPhoneOS${IOS_SDK_VERSION}.sdk")

    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -framework CoreFoundation" CACHE INTERNAL "")
    set(CMAKE_OSX_ARCHITECTURES "$(ARCHS_STANDARD_64_BIT) $(ARCHS_STANDARD_32_BIT)")
    set(CMAKE_XCODE_EFFECTIVE_PLATFORMS "-iphoneos;-iphonesimulator")
    set(CMAKE_OSX_SYSROOT "${IOS_SDK_ROOT}")
endif(APPLE)

include(FindOpenSSL)

