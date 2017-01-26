add_library(pthread_win32 OBJECT pthread.c)

target_include_directories(pthread_win32 INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})