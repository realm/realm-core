add_library(pthread_win32 STATIC pthread.c)

target_include_directories(pthread_win32 INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})