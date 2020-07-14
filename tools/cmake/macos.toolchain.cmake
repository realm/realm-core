# include("${CMAKE_CURRENT_LIST_DIR}/Utilities.cmake")
# set_common_xcode_attributes()

# set(CMAKE_OSX_SDKROOT $(xcrun --show-sdk-path))
# set(CMAKE_OSX_SDKROOT macosx)
# set(CMAKE_OSX_SYSROOT "macosx")
set(CMAKE_INSTALL_LIBDIR "lib")
# set(CMAKE_XCODE_ARCHS "x86_64")

# set(CMAKE_OSX_ARCHITECTURES "x86_64 arm64") # This should work
#  error: /Users/py/Downloads/Xcode-beta_macs.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/libtool: can't open file: /Users/py/Documents/GitHub/armmac/realm-core/build-macos-Release/src/realm/RealmCore.build/Release/CoreObjects.build/Objects-normal/x86_64 arm64/interprocess_mutex.o (No such file or directory)
#
set(CMAKE_OSX_ARCHITECTURES "x86_64; arm64") # This should work

set(CMAKE_XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS "macosx")
set(CMAKE_XCODE_EFFECTIVE_PLATFORMS "-macosx")
set(CMAKE_XCODE_ATTRIBUTE_ARCHS "x86_64 arm64")
set(CMAKE_XCODE_ATTRIBUTE_VALID_ARCHS "x86_64 arm64")
# set(CMAKE_XCODE_ATTRIBUTE_ARCHS "arm64")
# set(CMAKE_XCODE_ATTRIBUTE_VALID_ARCHS "arm64")
# set(CMAKE_XCODE_ATTRIBUTE_ARCHS $(ARCHS_STANDARD))
# set(CMAKE_XCODE_ATTRIBUTE_VALID_ARCHS $(ARCHS_STANDARD))
set(CMAKE_XCODE_ATTRIBUTE_MACOSX_DEPLOYMENT_TARGET "11")
#set(CMAKE_CXX_FLAGS "-target x86_64-apple-ios13.0-macabi -Wno-overriding-t-option")
#set(CMAKE_CXX_FLAGS "-target arm64e-apple-macosx10.15.0")
#set(CPACK_SYSTEM_NAME "macos")

# set(triple x86_64)
# set(CMAKE_C_COMPILER "clang ${triple}")
# set(CMAKE_CXX_COMPILER "clang++ ${triple}")

set(CMAKE_C_COMPILER clang)
set(CMAKE_CXX_COMPILER clang++)

include("${CMAKE_CURRENT_LIST_DIR}/Utilities.cmake")
set_common_xcode_attributes()
# set(CMAKE_XCODE_ARCHS "x86_64")
