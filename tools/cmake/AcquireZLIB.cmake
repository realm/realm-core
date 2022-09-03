# Use Zlib, but allow integrators to override it
if(NOT TARGET ZLIB::ZLIB)
    if(APPLE)
        # Don't use find_library(ZLIB) on Apple platforms - it hardcodes the path per platform,
        # so for an iOS build it'll use the path from the Device plaform, which is an error on Simulator.
        # Just use -lz and let Xcode figure it out
        return()
    endif()
    if(ANDROID)
        # On Android FindZLIB chooses the static libz over the dynamic one, but this leads to issues
        # (see https://github.com/android/ndk/issues/1179)
        # We want to link against the stub library instead of statically linking anyway,
        # so we hack find_library to only consider shared object libraries when looking for libz
        set(_CMAKE_FIND_LIBRARY_SUFFIXES_orig ${CMAKE_FIND_LIBRARY_SUFFIXES})
        set(CMAKE_FIND_LIBRARY_SUFFIXES .so)
    endif()
    find_package(ZLIB)
    if(ANDROID)
        set(CMAKE_FIND_LIBRARY_SUFFIXES ${_CMAKE_FIND_LIBRARY_SUFFIXES_orig})
    endif()
    if(NOT ZLIB_FOUND)
        message(STATUS "Zlib not found, building from source with FetchContent")
        include(FetchContent)
        FetchContent_Declare(
            zlib
            GIT_REPOSITORY https://github.com/madler/zlib.git
            GIT_TAG v1.2.12
        )
        FetchContent_Populate(zlib)
        add_subdirectory(${zlib_SOURCE_DIR} ${zlib_BINARY_DIR} EXCLUDE_FROM_ALL)
        set(REALM_EMBEDDABLE_ZLIB zlibstatic)
    endif()
endif()