cmake_minimum_required(VERSION 3.2.0)

if(REALM_PLATFORM STREQUAL "Android")
    # This must be before project()
    set(CMAKE_TOOLCHAIN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/CMake/android.toolchain.cmake")
    set(ANDROID_ABI "x86_64" CACHE STRING "")
endif()

set(CMAKE_BUILD_TYPE Debug CACHE STRING "")
project(realm-object-store)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake")

include(CodeCoverage)
include(CompilerFlags)
include(Sanitizers)

include(RealmCore)
set(REALM_CORE_VERSION "1.5.0" CACHE STRING "")
use_realm_core(${REALM_CORE_VERSION})

set(PEGTL_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/external/pegtl)

add_subdirectory(src)
add_subdirectory(tests)
