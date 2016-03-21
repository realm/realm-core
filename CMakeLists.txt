cmake_minimum_required(VERSION 3.1)
# Custom CMake modules
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/tools/cmake" ${CMAKE_MODULE_PATH})

cmake_policy(SET CMP0048 NEW)

# Project version
set(REALM_VERSION_MAJOR 0)
set(REALM_VERSION_MINOR 96)
set(REALM_VERSION_PATCH 2)
set(REALM_SOVERSION "9.0.0")

project(
    realm
    VERSION ${REALM_VERSION_MAJOR}.${REALM_VERSION_MINOR}.${REALM_VERSION_PATCH}
)

# Include general CMake modules
include(GNUInstallDirs)
include(CheckIncludeFiles)
include(FindOpenSSL)

# Project-wide build flags
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra -Wpedantic $ENV{EXTRA_CFLAGS}")
set(CMAKE_LINKER_FLAGS $ENV{EXTRA_LDFLAGS})

# Target-specific build flags
if(REALM_COVERAGE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage -fno-inline -fno-elide-constructors")
endif()

# Platform-specific build configuration
if(APPLE)
    set(OPENSSL_ROOT_DIR "/usr/local/opt/openssl")
    set(CMAKE_EXE_LINKER_FLAGS "-framework Foundation")

    # Enable Apple-specific platforms on Xcode
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

# Find dependencies
# Note: OpenSSL was already found through the `FindOpenSSL` CMake module
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Options (passed to CMake)
option(REALM_ENABLE_ASSERTIONS "Enable assertions in release mode.")
option(REALM_ENABLE_ALLOC_SET_ZERO "Zero all allocations.")
option(REALM_ENABLE_ENCRYPTION "Enable encryption.")
set(REALM_MAX_BPNODE_SIZE "1000" CACHE STRING "Max B+ tree node side.")

check_include_files(malloc.h HAVE_MALLOC_H)

if(CMAKE_BUILD_TYPE STREQUAL Debug)
    set(REALM_DEBUG 1)
endif()

# Store configuration in header file
configure_file(src/realm/util/config.h.in src/realm/util/config.h)

# Tell the build system where to find the sources (and generated sources)
include_directories(src)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/src) # For generated files (like config.h)

# Include additional CMakeLists
add_subdirectory(src/realm)

# Enable CTest and include unit tests
enable_testing()
add_subdirectory(test)
