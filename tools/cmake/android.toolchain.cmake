if(CMAKE_GENERATOR STREQUAL Xcode)
    message(FATAL_ERROR "Building for Android cannot use the Xcode generator.")
endif()

# Callers can pick their own sysroot for packaging purposes, currently only needed for plain macosx builds
if(NOT DEFINED CMAKE_SYSTEM_NAME)
    set(CMAKE_SYSTEM_NAME Android)
endif()

# For some reason, APPLE is set when building for Android on MacOS
# This leads to the incorrect ar program being seleccted: "/usr/bin/ar" vs "llvm-ar"
# Unset APPLE now so the correct ar program is selected
# Remove once https://gitlab.kitware.com/cmake/cmake/-/issues/23333 is resolved
if(APPLE)
    unset(APPLE)
endif()
