include("${CMAKE_CURRENT_LIST_DIR}/Utilities.cmake")

set_common_xcode_attributes()

set(CMAKE_XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS "macosx")
set(CMAKE_XCODE_EFFECTIVE_PLATFORMS "-macosx")
set(CMAKE_XCODE_ATTRIBUTE_ARCHS $(ARCHS_STANDARD))
set(CMAKE_XCODE_ATTRIBUTE_VALID_ARCHS $(ARCHS_STANDARD))
set(CMAKE_XCODE_ATTRIBUTE_MACOSX_DEPLOYMENT_TARGET "10.15")
#set(CMAKE_CXX_FLAGS "-target x86_64-apple-ios13.0-macabi -Wno-overriding-t-option")
set(CMAKE_CXX_FLAGS "-target arm64e-apple-macosx10.15.0")
set(CPACK_SYSTEM_NAME "macos")
set(CMAKE_C_COMPILER "clang")
set(CMAKE_CXX_COMPILER "clang++")
