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

option(REALM_ENABLE_ASSERTIONS "Enable assertions in release mode.")
option(REALM_ENABLE_ALLOC_SET_ZERO "Zero all allocations.")
option(REALM_ENABLE_ENCRYPTION "Enable encryption.")
set(REALM_MAX_BPNODE_SIZE "1000" CACHE STRING "Max B+ tree node side.")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra -Wpedantic -DREALM_HAVE_CONFIG $ENV{EXTRA_CFLAGS}")
if(REALM_COVERAGE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage -fno-inline -fno-elide-constructors")
endif()
set(CMAKE_LINKER_FLAGS $ENV{EXTRA_LDFLAGS})

if(APPLE)
    set(OPENSSL_ROOT_DIR "/usr/local/opt/openssl")
    set(CMAKE_EXE_LINKER_FLAGS "-framework Foundation")
    if(CMAKE_GENERATOR STREQUAL Xcode)
        set(CMAKE_XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS
            "macosx iphoneos iphonesimulator appletvos appletvsimulator watchos watchsimulator"
        )
        set(CMAKE_XCODE_EFFECTIVE_PLATFORMS
            "-macosx;-iphoneos;-iphonesimulator;-appletvos;-appletvsimulator;-watchos;-watchsimulator"
        )
        set(CMAKE_XCODE_ATTRIBUTE_ENABLE_BITCODE "NO")
        set(CMAKE_OSX_SYSROOT $(PLATFORM_NAME))
        set(CMAKE_XCODE_ATTRIBUTE_IPHONEOS_DEPLOYMENT_TARGET "7.0")
    endif()
endif()

check_include_files(malloc.h HAVE_MALLOC_H)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

if(CMAKE_BUILD_TYPE STREQUAL Debug)
    set(REALM_DEBUG 1)
endif()

configure_file(
    ${CMAKE_SOURCE_DIR}/src/realm/util/config.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/src/realm/util/config.h
)

include_directories(src)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/src) # For generated files (like config.h)

add_subdirectory(src/realm)
add_subdirectory(test)

include(FindOpenSSL)
