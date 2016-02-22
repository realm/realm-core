cmake_minimum_required(VERSION 3.1)
project(Realm)
set(CMAKE_CXX_STANDARD 11)
enable_testing()

include_directories(src)

add_subdirectory(src/realm)
add_subdirectory(test)

set(CMAKE_CXX_FLAGS "-Wextra -Wpedantic -DREALM_HAVE_CONFIG" CACHE INTERNAL "")
set(CMAKE_CXX_FLAGS_DEBUG "-DDEBUG -DREALM_DEBUG" CACHE INTERNAL "")

if(APPLE)
    set(IOS_SDK_VERSION "9.2")
    set(IOS_SDK_DEV_ROOT "/Applications/Xcode.app/Contents/Developer/Platforms/iPhoneOS.platform/Developer")
    set(IOS_SDK_ROOT "${IOS_SDK_DEV_ROOT}/SDKs/iPhoneOS${IOS_SDK_VERSION}.sdk")

    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -framework CoreFoundation" CACHE INTERNAL "")
    set(CMAKE_OSX_ARCHITECTURES "$(ARCHS_STANDARD_64_BIT) $(ARCHS_STANDARD_32_BIT)")
    set(CMAKE_XCODE_EFFECTIVE_PLATFORMS "-iphoneos;-iphonesimulator")
    set(CMAKE_OSX_SYSROOT "${IOS_SDK_ROOT}")
endif(APPLE)

