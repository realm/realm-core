include("${CMAKE_CURRENT_LIST_DIR}/utilities.cmake")

check_generator("Xcode")

set_common_xcode_attributes()

set(CMAKE_XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS "macosx")
set(CMAKE_XCODE_EFFECTIVE_PLATFORMS "-macosx")
set(CMAKE_XCODE_ATTRIBUTE_MACOSX_DEPLOYMENT_TARGET "10.8")
