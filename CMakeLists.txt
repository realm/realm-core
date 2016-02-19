cmake_minimum_required(VERSION 3.1)
project(Realm)
set(CMAKE_CXX_STANDARD 11)
enable_testing()

include_directories(src)

add_subdirectory(src/realm)
add_subdirectory(test)

set(CMAKE_CXX_FLAGS_DEBUG "-DDEBUG" CACHE INTERNAL "")

if(APPLE)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -framework CoreFoundation" CACHE INTERNAL "")
endif(APPLE)

