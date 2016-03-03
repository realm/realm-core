cmake_minimum_required(VERSION 3.1)
include(GNUInstallDirs)
include(CheckIncludeFiles)
project(Realm)
set(CMAKE_CXX_STANDARD 11)
enable_testing()

set(REALM_VERSION_MAJOR 0)
set(REALM_VERSION_MINOR 96)
set(REALM_VERSION_PATCH 2)
set(REALM_VERSION "${REALM_VERSION_MAJOR}.${REALM_VERSION_MINOR}.${REALM_VERSION_PATCH}")

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
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -framework CoreFoundation" CACHE INTERNAL "")
endif(APPLE)

