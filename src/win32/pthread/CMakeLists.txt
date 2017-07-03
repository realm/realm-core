add_library(pthread_win32 STATIC pthread.c)

set_property(TARGET pthread_win32 APPEND PROPERTY COMPILE_DEFINITIONS PTW32_STATIC_LIB)

# Disable a warning we have no control over
target_compile_options(pthread_win32 PUBLIC "/wd4013" "/wd4047")

target_include_directories(pthread_win32 INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

install(TARGETS pthread_win32 ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT devel)
install(DIRECTORY ./ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/win32/pthread COMPONENT devel FILES_MATCHING PATTERN "*.h")
