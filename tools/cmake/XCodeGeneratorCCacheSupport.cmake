# Work-around for the XCode Generator not working correctly with CCache.
# While not needed for Ninja and Makefiles, use the same helper scripts 
# to ensure that all build types are routed through the same code path.
#
# See # See https://crascit.com/2016/04/09/using-ccache-with-cmake/#h-improved-functionality-from-cmake-3-4
#
# The work-around mentioned in the above link is modified to also work when Realm Core is used through
# `add_subdirectory()`.

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    # Set up wrapper scripts
    # Note: this will override any other user-defined CMAKE_C_COMPILER_LAUNCHER or CMAKE_CXX_COMPILER_LAUNCHER settings.

    message(STATUS "Found ccache. Copying launcher scripts from ${PROJECT_SOURCE_DIR} to ${PROJECT_BINARY_DIR}")
    set(C_LAUNCHER   "${CCACHE_PROGRAM}")
    set(CXX_LAUNCHER "${CCACHE_PROGRAM}")
    configure_file("${PROJECT_SOURCE_DIR}/tools/cmake/launch-c.in" "${PROJECT_BINARY_DIR}/launch-c")
    configure_file("${PROJECT_SOURCE_DIR}/tools/cmake/launch-cxx.in" "${PROJECT_BINARY_DIR}/launch-cxx")
    execute_process(COMMAND chmod a+rx
                     "${PROJECT_BINARY_DIR}/launch-c"
                     "${PROJECT_BINARY_DIR}/launch-cxx"
    )

    if(CMAKE_GENERATOR STREQUAL "Xcode")
        # Set Xcode project attributes to route compilation and linking
        # through our scripts
        set(CMAKE_XCODE_ATTRIBUTE_CC         "${PROJECT_BINARY_DIR}/launch-c")
        set(CMAKE_XCODE_ATTRIBUTE_CXX        "${PROJECT_BINARY_DIR}/launch-cxx")
        set(CMAKE_XCODE_ATTRIBUTE_LD         "${PROJECT_BINARY_DIR}/launch-c")
        set(CMAKE_XCODE_ATTRIBUTE_LDPLUSPLUS "${PROJECT_BINARY_DIR}/launch-cxx")
    else()
        # Support Unix Makefiles and Ninja
        set(CMAKE_C_COMPILER_LAUNCHER   "${PROJECT_BINARY_DIR}/launch-c")
        set(CMAKE_CXX_COMPILER_LAUNCHER "${PROJECT_BINARY_DIR}/launch-cxx")
    endif()
 endif()
