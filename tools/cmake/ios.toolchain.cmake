include("${CMAKE_CURRENT_LIST_DIR}/Utilities.cmake")

check_generator("Xcode")

fix_xcode_try_compile()

set_common_xcode_attributes()

set(REALM_SKIP_SHARED_LIB ON)
set(CPACK_SYSTEM_NAME "ios")

set(CMAKE_OSX_SYSROOT "iphoneos")

set(CMAKE_XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS "iphoneos iphonesimulator")
set(CMAKE_XCODE_EFFECTIVE_PLATFORMS "-iphoneos;-iphonesimulator")
set(CMAKE_XCODE_ATTRIBUTE_IPHONEOS_DEPLOYMENT_TARGET "8.0")
set(CMAKE_XCODE_ATTRIBUTE_CODE_SIGN_IDENTITY "")
set(CMAKE_XCODE_ATTRIBUTE_CODE_SIGNING_REQUIRED "NO")

set(CMAKE_XCODE_ATTRIBUTE_ARCHS[sdk=iphoneos*] "\$(ARCHS_iphoneos_\$(CONFIGURATION))")
set(CMAKE_XCODE_ATTRIBUTE_ARCHS_iphoneos_Debug "armv7 arm64")
set(CMAKE_XCODE_ATTRIBUTE_ARCHS_iphoneos_MinSizeDebug "armv7 arm64")
set(CMAKE_XCODE_ATTRIBUTE_ARCHS_iphoneos_Release "armv7 arm64")
set(CMAKE_XCODE_ATTRIBUTE_ARCHS_iphoneos_RelWithDebInfo "armv7 arm64")
set(CMAKE_XCODE_ATTRIBUTE_ARCHS_iphoneos_MinSizeRel "armv7 arm64")

set(CMAKE_XCODE_ATTRIBUTE_ENABLE_BITCODE[sdk=iphone*] "YES")

set_bitcode_attributes()
