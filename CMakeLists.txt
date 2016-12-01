cmake_minimum_required(VERSION 3.1)

project(Realm)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/tools/cmake" ${CMAKE_MODULE_PATH})

cmake_policy(SET CMP0048 NEW)

# Project version
set(REALM_VERSION_MAJOR 1)
set(REALM_VERSION_MINOR 5)
set(REALM_VERSION_PATCH 1)
set(REALM_VERSION_EXTRA "")
set(REALM_SOVERSION "9.0.0")

if(ANDROID)
    set(ANDROID_OPENSSL_VERSION "1.0.1t")
    set(REALM_LIBTYPE "STATIC")
    list(APPEND PLATFORM_LIBRARIES log)
    list(APPEND PLATFORM_LIBRARIES android)
    if (ANDROID_ABI STREQUAL mips)
        list(APPEND PLATFORM_LIBRARIES atomic)
    endif()
endif()

project(
    realm
    VERSION ${REALM_VERSION_MAJOR}.${REALM_VERSION_MINOR}.${REALM_VERSION_PATCH}
)

# Include general CMake modules
include(GNUInstallDirs)
include(CheckIncludeFiles)
include(GetGitRevisionDescription)

# Get accurate git-describe version
git_describe(REALM_VERSION)

if(NOT DEFINED REALM_LIBTYPE)
  set(REALM_LIBTYPE "SHARED")
endif()

# Project-wide build flags
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED on)
set(CMAKE_CXX_EXTENSIONS off)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra -Wpedantic $ENV{EXTRA_CFLAGS}")
set(CMAKE_LINKER_FLAGS $ENV{EXTRA_LDFLAGS})

if(CMAKE_CONFIGURATION_TYPES)
    # Add the MinSizeDebug configuration, used for packaging releases for Cocoa.
    list(APPEND CMAKE_CONFIGURATION_TYPES MinSizeDebug)
    list(REMOVE_DUPLICATES CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_CONFIGURATION_TYPES "${CMAKE_CONFIGURATION_TYPES}" CACHE STRING "Add MinSizeDebug configuration" FORCE)
endif()

# Add REALM_DEBUG=1 to compile definitions for Debug / MinSizeDebug targets
set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS
    $<$<CONFIG:Debug>:REALM_DEBUG=1>
    $<$<CONFIG:MinSizeDebug>:REALM_DEBUG=1>
)

# Coverage-specific build flags
if(REALM_COVERAGE)
  if(MSVC)
    message( FATAL_ERROR "Code coverage is not yet supported on Visual Studio builds" )
  else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -fprofile-arcs -ftest-coverage -fno-inline -fno-elide-constructors")
  endif()
endif()

# Platform-specific build configuration
if(APPLE)
    find_library(Foundation Foundation)

    # Enable Apple-specific platforms on Xcode
    if(CMAKE_GENERATOR STREQUAL Xcode)
        set(CMAKE_XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS
            "macosx iphoneos iphonesimulator appletvos appletvsimulator watchos watchsimulator"
        )
        set(CMAKE_XCODE_EFFECTIVE_PLATFORMS
            "-macosx;-iphoneos;-iphonesimulator;-appletvos;-appletvsimulator;-watchos;-watchsimulator"
        )
        set(CMAKE_XCODE_ATTRIBUTE_CLANG_CXX_LIBRARY "libc++")

        # Deployment targets
        set(CMAKE_XCODE_ATTRIBUTE_MACOSX_DEPLOYMENT_TARGET "10.8")
        set(CMAKE_XCODE_ATTRIBUTE_IPHONEOS_DEPLOYMENT_TARGET "7.0")
        set(CMAKE_XCODE_ATTRIBUTE_WATCHOS_DEPLOYMENT_TARGET "2.0")
        set(CMAKE_XCODE_ATTRIBUTE_TVOS_DEPLOYMENT_TARGET "9.0")

        # Architectures. Everything but iOS uses the default architectures.
        set(CMAKE_XCODE_ATTRIBUTE_ARCHS[sdk=iphoneos*] "\$(ARCHS_iphoneos_\$(CONFIGURATION))")
        set(CMAKE_XCODE_ATTRIBUTE_ARCHS_iphoneos_Debug "armv7 arm64")
        set(CMAKE_XCODE_ATTRIBUTE_ARCHS_iphoneos_MinSizeDebug "armv7 arm64")
        # iOS release configurations need to build for armv7s for sake of CocoaPods.
        set(CMAKE_XCODE_ATTRIBUTE_ARCHS_iphoneos_Release "armv7 armv7s arm64")
        set(CMAKE_XCODE_ATTRIBUTE_ARCHS_iphoneos_RelWithDebInfo "armv7 armv7s arm64")
        set(CMAKE_XCODE_ATTRIBUTE_ARCHS_iphoneos_MinSizeRel "armv7 armv7s arm64")

        # Bitcode. May be overridden by Rakefile.
        set(CMAKE_XCODE_ATTRIBUTE_ENABLE_BITCODE[sdk=iphone*] "YES")
        set(CMAKE_XCODE_ATTRIBUTE_ENABLE_BITCODE[sdk=watch*] "YES")
        set(CMAKE_XCODE_ATTRIBUTE_ENABLE_BITCODE[sdk=appletv*] "YES")
        set(CMAKE_XCODE_ATTRIBUTE_BITCODE_GENERATION_MODE "\$(BITCODE_GENERATION_MODE_\$(CONFIGURATION))")
        set(CMAKE_XCODE_ATTRIBUTE_BITCODE_GENERATION_MODE_Debug "marker")
        set(CMAKE_XCODE_ATTRIBUTE_BITCODE_GENERATION_MODE_MinSizeDebug "marker")
        set(CMAKE_XCODE_ATTRIBUTE_BITCODE_GENERATION_MODE_Release "bitcode")
        set(CMAKE_XCODE_ATTRIBUTE_BITCODE_GENERATION_MODE_RelWithDebInfo "bitcode")
        set(CMAKE_XCODE_ATTRIBUTE_BITCODE_GENERATION_MODE_MinSizeRel "bitcode")
    endif()
endif(APPLE)

# Find dependencies
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
if(NOT APPLE AND NOT ANDROID)
    include(FindOpenSSL)
endif()

# Options (passed to CMake)
option(REALM_ENABLE_ASSERTIONS "Enable assertions in release mode.")
option(REALM_ENABLE_ALLOC_SET_ZERO "Zero all allocations.")
option(REALM_ENABLE_ENCRYPTION "Enable encryption.")
set(REALM_MAX_BPNODE_SIZE "1000" CACHE STRING "Max B+ tree node size.")

check_include_files(malloc.h HAVE_MALLOC_H)

# Store configuration in header file
configure_file(src/realm/util/config.h.in src/realm/util/config.h)
# Configure source code to use right version number
configure_file(src/realm/version.hpp.in src/realm/version.hpp)

if(EXISTS "${CMAKE_SOURCE_DIR}/src/realm/util/config.h")
    message(FATAL_ERROR "config.h exists in the source directory, and will take precedence over the generated configuration in the build directory. Please remove it before continuing.")
endif()

# Tell the build system where to find the sources (and generated sources)
include_directories(src)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/src) # For generated files (like config.h)

# Include additional CMakeLists
add_subdirectory(src)

# Enable CTest and include unit tests
if(NOT REALM_BUILD_LIB_ONLY)
    enable_testing()
    add_subdirectory(test)
endif()

# Enable CPack
set(CPACK_GENERATOR "TGZ")
string(TOLOWER "realm-core-${REALM_LIBTYPE}-${CMAKE_BUILD_TYPE}" REALM_PACKAGE_NAME)
set(CPACK_PACKAGE_NAME ${REALM_PACKAGE_NAME})
set(CPACK_PACKAGE_VERSION_MAJOR ${REALM_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${REALM_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${REALM_VERSION_PATCH})
set(CPACK_PACKAGE_VENDOR "Realm Inc.")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
include(CPack)
