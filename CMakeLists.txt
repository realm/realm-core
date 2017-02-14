cmake_minimum_required(VERSION 3.4)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/tools/cmake" ${CMAKE_MODULE_PATH})

project(realm VERSION 2.3.0)

# Include general CMake modules
include(GNUInstallDirs)
include(CheckIncludeFiles)
include(GetGitRevisionDescription)

# Get accurate git-describe version
git_describe(REALM_VERSION)

# Project-wide build flags
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED on)
set(CMAKE_CXX_EXTENSIONS off)

# Set global warnings settings
if(MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W3")
else()
  set(CMAKE_CXX_FLAGS, "${CMAKE_CXX_FLAGS} -Wextra -Wpedantic")
endif()

# Import some flags from the environment variables
# (for compatibility with build.sh)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} $ENV{EXTRA_CFLAGS}")
set(CMAKE_LINKER_FLAGS $ENV{EXTRA_LDFLAGS})

# Add the MinSizeDebug configuration, used for packaging releases for Cocoa.
if(APPLE AND CMAKE_CONFIGURATION_TYPES)
    list(APPEND CMAKE_CONFIGURATION_TYPES MinSizeDebug)
    list(REMOVE_DUPLICATES CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_CONFIGURATION_TYPES "${CMAKE_CONFIGURATION_TYPES}"
        CACHE STRING "Add MinSizeDebug configuration" FORCE)
endif()

# Add REALM_DEBUG=1 to compile definitions for Debug / MinSizeDebug targets
set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS
    $<$<CONFIG:Debug>:REALM_DEBUG=1>
    $<$<CONFIG:MinSizeDebug>:REALM_DEBUG=1>
)

# Add REALM_UWP=1 to compile with Windows UWP support
if(WINDOWS_STORE AND CMAKE_SYSTEM_VERSION STREQUAL 10.0)
    add_definitions(-DREALM_UWP=1 -D_CRT_SECURE_NO_WARNINGS)
endif()

# Coverage-specific build flags
option(REALM_COVERAGE "Add coverage support." OFF)
if(REALM_COVERAGE)
  if(MSVC)
    message(FATAL_ERROR
            "Code coverage is not yet supported on Visual Studio builds")
  else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -fprofile-arcs -ftest-coverage -fno-inline -fno-elide-constructors")
  endif()
endif()

# Platform-specific build configuration
if(APPLE)
    find_library(Foundation Foundation)

    # Enable Apple-specific platforms on Xcode
    if(CMAKE_GENERATOR STREQUAL Xcode)
        unset(CMAKE_XCODE_ATTRIBUTE_INSTALL_PATH)

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
elseif(ANDROID)
  set(ANDROID_OPENSSL_VERSION "1.0.1t")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Os")
  list(APPEND PLATFORM_LIBRARIES log)
  list(APPEND PLATFORM_LIBRARIES android)
endif()

# Find dependencies
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

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
if(NOT REALM_BUILD_LIB_ONLY AND NOT REALM_NO_TESTS)
    enable_testing()
    add_subdirectory(test)
endif()

# Enable CPack
if(APPLE OR ANDROID)
    set(CPACK_GENERATOR "TGZ")
else()
    set(CPACK_GENERATOR "TGZ;RPM;DEB")
endif()
string(TOLOWER "realm-core-${CMAKE_BUILD_TYPE}" REALM_PACKAGE_NAME)
set(CPACK_PACKAGE_NAME ${REALM_PACKAGE_NAME})
set(CPACK_PACKAGE_VERSION ${REALM_VERSION})
set(CPACK_PACKAGE_VENDOR "Realm Inc.")
set(CPACK_PACKAGE_CONTACT "Realm <help@realm.io>")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Realm is a mobile database: a replacement for Core Data & SQLite")
set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE "${CMAKE_SOURCE_DIR}/tools/cmake/run_ldconfig.sh")
set(CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE "${CMAKE_SOURCE_DIR}/tools/cmake/run_ldconfig.sh")
set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_DEBIAN_POST_INSTALL_SCRIPT_FILE "${CMAKE_SOURCE_DIR}/tools/cmake/run_ldconfig.sh")
set(CPACK_DEBIAN_POST_UNINSTALL_SCRIPT_FILE "${CMAKE_SOURCE_DIR}/tools/cmake/run_ldconfig.sh")
set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)
include(CPack)
cpack_add_component(runtime DEPENDS runtime)
cpack_add_component(devel DEPENDS devel)

