include("${CMAKE_CURRENT_LIST_DIR}/Utilities.cmake")

set_common_xcode_attributes()

set(CMAKE_XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS "macosx")
set(CMAKE_XCODE_EFFECTIVE_PLATFORMS "-maccatalyst")
set(CMAKE_XCODE_ATTRIBUTE_MACOSX_DEPLOYMENT_TARGET "10.15")
set(CMAKE_CXX_FLAGS "-target x86_64-apple-ios13.0-macabi -Wno-overriding-t-option")
set(CPACK_SYSTEM_NAME "maccatalyst")
