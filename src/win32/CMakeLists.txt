add_library(getopt_win32 INTERFACE)
target_include_directories(getopt_win32 INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

install(FILES getopt.h
        DESTINATION include/win32
        COMPONENT devel)

add_subdirectory(kalven-sha2)
