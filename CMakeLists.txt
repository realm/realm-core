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
set(REALM_CORE_VERSION "2.1.0" CACHE STRING "")
use_realm_core(${REALM_CORE_VERSION})

if(REALM_SYNC_PREFIX)
  set(REALM_ENABLE_SYNC ON)
  build_realm_sync(${REALM_SYNC_PREFIX})
endif()

if(REALM_ENABLE_SYNC)
  add_definitions(-DREALM_ENABLE_SYNC)
endif()

set(PEGTL_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/external/pegtl)

add_subdirectory(src)
add_subdirectory(tests)
