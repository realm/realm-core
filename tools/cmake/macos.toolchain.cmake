include("${CMAKE_CURRENT_LIST_DIR}/Utilities.cmake")

check_generator("Xcode")

set_common_xcode_attributes()

set(CMAKE_DEBUG_POSTFIX "-macos-dbg")
set(CMAKE_MINSIZEDEBUG_POSTFIX "-macos-dbg")
set(CMAKE_RELEASE_POSTFIX "-macos")
set(CMAKE_XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS "macosx")
set(CMAKE_XCODE_EFFECTIVE_PLATFORMS "-macosx")
set(CMAKE_XCODE_ATTRIBUTE_MACOSX_DEPLOYMENT_TARGET "10.8")
