cmake_minimum_required(VERSION 3.4)

set(CMAKE_BUILD_TYPE Debug CACHE STRING "")
project(RealmCore)

list(APPEND CMAKE_MODULE_PATH "${RealmCore_SOURCE_DIR}/tools/cmake")

# Include general CMake modules
include(GNUInstallDirs)
include(CheckIncludeFiles)
include(GetGitRevisionDescription)
include(Utilities)
include(SpecialtyBuilds)
include(GetVersion)

# Get accurate git-describe version
git_describe(REALM_VERSION)

# Project-wide build flags
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(ANDROID)
    # TODO: Core APIs should always built for internal usage. But there seems to be issues with cocoa. Enable it only for Android for now.
    set(CMAKE_CXX_VISIBILITY_PRESET hidden)
elseif(APPLE)
    set(CMAKE_VISIBILITY_INLINES_HIDDEN 1)
endif()

# Set global warnings settings
if(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W3 /D_CRT_SECURE_NO_WARNINGS /D_SCL_SECURE_NO_WARNINGS")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic -Wundef -Wshadow")
    # TODO: Remove this when fixed
    if(ANDROID)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-uninitialized")
    elseif(${CMAKE_CXX_COMPILER_ID} MATCHES ".*[Cc]lang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wunreachable-code -Wshorten-64-to-32 -Wold-style-cast -Wconditional-uninitialized -Wextra-semi -Wno-nested-anon-types")
    endif()
endif()

if(ANDROID)
    # On some architectures char is unsigned by default. Make it signed
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsigned-char")
endif()

# Add REALM_DEBUG=1 to compile definitions for Debug / MinSizeDebug targets
set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS
    $<$<CONFIG:Debug>:REALM_DEBUG=1>
    $<$<CONFIG:MinSizeDebug>:REALM_DEBUG=1>
)


set(CMAKE_DEBUG_POSTFIX "-dbg")
set(CMAKE_MINSIZEDEBUG_POSTFIX "-dbg")

set(CMAKE_POSITION_INDEPENDENT_CODE true)

if(CMAKE_SYSTEM_NAME MATCHES "^Windows")
    add_definitions(-D_SCL_SECURE_NO_WARNINGS -DWIN32_LEAN_AND_MEAN)
endif()

if(MSVC)
    add_compile_options(
        /MP # Enable multi-processor compilation
    )
    if(NOT WINDOWS_STORE)
        # Statically link the run-time library
        # https://docs.microsoft.com/bg-bg/cpp/build/reference/md-mt-ld-use-run-time-library
        # https://cmake.org/Wiki/CMake_FAQ#How_can_I_build_my_MSVC_application_with_a_static_runtime.3F
        foreach(flag_var
            CMAKE_CXX_FLAGS CMAKE_CXX_FLAGS_DEBUG CMAKE_CXX_FLAGS_RELEASE
            CMAKE_CXX_FLAGS_MINSIZEREL CMAKE_CXX_FLAGS_RELWITHDEBINFO)
            if(${flag_var} MATCHES "/MD")
                string(REGEX REPLACE "/MD" "/MT" ${flag_var} "${${flag_var}}")
            endif()

            # Disable runtime stack bound checks, because these slow down unit tests
            # disproportionately.
            if(${flag_var} MATCHES "/RTC")
                string(REGEX REPLACE "/RTC(su|[1su])" "" ${flag_var} "${${flag_var}}")
            endif()
        endforeach()

        # Let the compiler optimize debug builds ever so slightly.
        # /Ob1: This allows the compiler to inline functions marked __inline.
        # /Oi:  This enables (faster) intrinsic functions.
        string(REPLACE "/Ob0" "/Ob1" CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
        string(CONCAT CMAKE_CXX_FLAGS_DEBUG "/Oi " "${CMAKE_CXX_FLAGS_DEBUG}")
    endif()
endif()

# Platform-specific build configuration
if(APPLE)
    find_library(Foundation Foundation)
elseif(ANDROID)
    set(ANDROID_OPENSSL_VERSION "1.1.1a")
    set(ANDROID_OPENSSL_BUILD_NUMBER "1")
    list(APPEND PLATFORM_LIBRARIES log android)
endif()

# Find dependencies
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
list(APPEND PLATFORM_LIBRARIES Threads::Threads)

# Options (passed to CMake)
option(REALM_ENABLE_ASSERTIONS "Enable assertions in release mode." OFF)
option(REALM_ENABLE_ALLOC_SET_ZERO "Zero all allocations." OFF)
option(REALM_ENABLE_ENCRYPTION "Enable encryption." ON)
option(REALM_ENABLE_MEMDEBUG "Add additional memory checks" OFF)
option(REALM_VALGRIND "Tell the test suite we are running with valgrind" OFF)
option(REALM_METRICS "Enable various metric tracking" ON)
set(REALM_MAX_BPNODE_SIZE "1000" CACHE STRING "Max B+ tree node size.")

check_include_files(malloc.h HAVE_MALLOC_H)

# Store configuration in header file
configure_file(src/realm/util/config.h.in src/realm/util/config.h)

# Configure source code to use right version number
configure_file(src/realm/version.hpp.in src/realm/version.hpp)

set(DEPRECATED_CONFIG_FILE "${RealmCore_SOURCE_DIR}/src/realm/util/config.h")
if(EXISTS "${DEPRECATED_CONFIG_FILE}")
    message(FATAL_ERROR "${DEPRECATED_CONFIG_FILE} exists in the source directory, and will take precedence over the generated configuration in the build directory. Please remove this file before continuing. Alternatively, you can also clean your realm-core to remove this and other stale files: git clean -xfd")
endif()

set(PEGTL_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/external/pegtl/include/tao/)

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

# Install the licence and changelog files
install(FILES LICENSE CHANGELOG.md 
        DESTINATION "doc/realm" 
        COMPONENT devel
)

# Make the project importable from the build directory
export(TARGETS Core QueryParser NAMESPACE RealmCore:: FILE RealmCoreTargets.cmake)
configure_file(${CMAKE_CURRENT_LIST_DIR}/tools/cmake/RealmCoreConfig.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/RealmCoreConfig.cmake @ONLY)

# Make the project importable from the install directory
install(EXPORT realm
        NAMESPACE RealmCore::
        FILE RealmCoreTargets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/RealmCore
        COMPONENT devel)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/RealmCoreConfig.cmake 
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/RealmCore 
        COMPONENT devel
)

# CPack
set(CPACK_GENERATOR "TGZ")
set(CPACK_PACKAGE_NAME "realm-core-${CMAKE_BUILD_TYPE}")
set(CPACK_PACKAGE_VERSION ${REALM_VERSION})
set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)
include(CPack)
cpack_add_component(runtime DEPENDS runtime)
cpack_add_component(devel DEPENDS devel)

