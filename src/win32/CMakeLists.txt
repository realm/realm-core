add_library(getopt_win32 STATIC getopt.cpp)

target_include_directories(getopt_win32 INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(pthread)
