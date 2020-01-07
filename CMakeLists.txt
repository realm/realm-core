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
include(CheckCXXCompilerFlag)
include(CheckCXXSourceRuns)

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

function(add_cxx_flag_if_supported flag)
    if(flag MATCHES "^-Wno-")
        # Compilers ignore unknown -Wno-foo flags, so look for -Wfoo instead.
        string(REPLACE "-Wno-" "-W" check_flag ${flag})
    else()
        set(check_flag ${flag})
    endif()

    check_cxx_compiler_flag(${check_flag} HAVE${check_flag})
    if(HAVE${check_flag})
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${flag}" PARENT_SCOPE)
    endif()
endfunction()

function(use_faster_linker)
    # If a linker has already been set, don't override.
    if ("${CMAKE_EXE_LINKER_FLAGS}" MATCHES "-fuse-ld=")
        return()
    endif()

    foreach(LINKER lld gold) # lld > gold > default
        # there is no link equivalent to check_cxx_compiler_flag()
        set(CMAKE_REQUIRED_LINK_OPTIONS "-fuse-ld=${LINKER}")
        check_cxx_source_compiles("int main() {}" HAVE_LINKER_${LINKER})
        if(HAVE_LINKER_${LINKER})
            foreach(KIND EXE SHARED MODULE)
                set(CMAKE_${KIND}_LINKER_FLAGS "${CMAKE_${KIND}_LINKER_FLAGS} -fuse-ld=${LINKER}" PARENT_SCOPE)
            endforeach()
            return()
        endif()
    endforeach()
endfunction()

# Set global warnings settings
if(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W3 /D_CRT_SECURE_NO_WARNINGS /D_SCL_SECURE_NO_WARNINGS")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic -Wundef -Wshadow")
    # TODO: Remove this when fixed
    if(ANDROID)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-maybe-uninitialized -Wno-uninitialized")
    elseif(${CMAKE_CXX_COMPILER_ID} MATCHES ".*[Cc]lang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wunreachable-code -Wshorten-64-to-32 -Wold-style-cast -Wconditional-uninitialized -Wextra-semi -Wno-nested-anon-types")
    endif()

    # This warning is too agressive. It warns about moves that are not redundant on older
    # compilers that we still support. It is also harmless, unlike pessimizing moves.
    add_cxx_flag_if_supported(-Wno-redundant-move)

    # Ninja buffers output so we need to tell the compiler to use colors even though stdout isn't a tty.
    if("${CMAKE_GENERATOR}" STREQUAL "Ninja")
        add_cxx_flag_if_supported(-fdiagnostics-color)
    endif()

    use_faster_linker()
endif()

# Add REALM_DEBUG=1 to compile definitions for Debug / MinSizeDebug targets
set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS
    $<$<CONFIG:Debug>:REALM_DEBUG=1>
    $<$<CONFIG:MinSizeDebug>:REALM_DEBUG=1>
)
set_property(DIRECTORY APPEND PROPERTY COMPILE_OPTIONS
    $<$<CONFIG:MinSizeDebug>:-g>
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
    add_definitions(
        /D_DISABLE_EXTENDED_ALIGNED_STORAGE #https://developercommunity.visualstudio.com/comments/279328/view.html
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
    set(ANDROID_OPENSSL_VERSION "1.0.2k")
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
configure_file(src/realm/version_numbers.hpp.in src/realm/version_numbers.hpp)

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
install(FILES LICENSE CHANGELOG.md DESTINATION "doc/realm" COMPONENT devel)

# Make the project importable from the build directory
export(TARGETS Core QueryParser NAMESPACE Realm:: FILE realm-config.cmake)

# Make the project importable from the install directory
install(EXPORT realm
        NAMESPACE Realm::
        FILE realm-config.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/realm
        COMPONENT devel)

# CPack
set(CPACK_GENERATOR "TGZ")
set(CPACK_PACKAGE_NAME "realm-core-${CMAKE_BUILD_TYPE}")
set(CPACK_PACKAGE_VERSION ${REALM_VERSION})
set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)
include(CPack)
cpack_add_component(runtime DEPENDS runtime)
cpack_add_component(devel DEPENDS devel)

