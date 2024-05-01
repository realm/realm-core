if(NOT CMAKE_GENERATOR STREQUAL Xcode)
    message(FATAL_ERROR "Building for Apple platforms is only supported with the Xcode generator.")
endif()

# Callers can pick their own sysroot for packaging purposes, currently only needed for plain macosx builds
if(NOT DEFINED CMAKE_SYSTEM_NAME)
    set(CMAKE_SYSTEM_NAME iOS)
    set(CPACK_SYSTEM_NAME "\$ENV{PLATFORM_NAME}")
endif()

set(CMAKE_XCODE_ATTRIBUTE_ARCHS "$(ARCHS_STANDARD)")
set(CMAKE_XCODE_ATTRIBUTE_ENABLE_BITCODE "NO")
set(CMAKE_XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS "iphoneos iphonesimulator appletvos appletvsimulator watchos watchsimulator macosx xros xrsimulator")
set(CMAKE_XCODE_EFFECTIVE_PLATFORMS "-iphoneos;-iphonesimulator;-appletvos;-appletvsimulator;-watchos;-watchsimulator;-maccatalyst")
set(CMAKE_XCODE_ATTRIBUTE_SUPPORTS_MACCATALYST "YES")
set(CMAKE_XCODE_ATTRIBUTE_TARGETED_DEVICE_FAMILY "1,2,3,4,7")

# With Xcode 14+ the base SDK *mostly* doesn't matter any more, as it
# officially supports multi-platform builds from a single target.
# However, as of Xcode 15 beta 8 xcodebuild (but not Xcode itself) requires the
# visionOS SDK to build for visionOS. 15.0 final doesn't include the visionOS
# SDK, so the SDKROOT is explicitly set by the invoker when building with the
# beta Xcode rather than here.
# Xcode 13 requires using the correct SDK. We no longer support Xcode 13, but
# still use it on evergreen to build the macOS tests (and nothing else).
if(NOT DEFINED CMAKE_XCODE_ATTRIBUTE_SDKROOT)
    set(CMAKE_XCODE_ATTRIBUTE_SDKROOT_1500 "iphoneos")
    set(CMAKE_XCODE_ATTRIBUTE_SDKROOT_1400 "iphoneos")
    set(CMAKE_XCODE_ATTRIBUTE_SDKROOT_1300 "macosx")
    set(CMAKE_XCODE_ATTRIBUTE_SDKROOT "$(SDKROOT_$(XCODE_VERSION_MAJOR))")
endif()

set(CMAKE_XCODE_ATTRIBUTE_IPHONEOS_DEPLOYMENT_TARGET "12.0")
set(CMAKE_XCODE_ATTRIBUTE_MACOSX_DEPLOYMENT_TARGET_CATALYST_NO "10.13")
set(CMAKE_XCODE_ATTRIBUTE_MACOSX_DEPLOYMENT_TARGET_CATALYST_YES "10.15")
set(CMAKE_XCODE_ATTRIBUTE_MACOSX_DEPLOYMENT_TARGET "$(MACOSX_DEPLOYMENT_TARGET_CATALYST_$(IS_MACCATALYST))")
set(CMAKE_XCODE_ATTRIBUTE_WATCHOS_DEPLOYMENT_TARGET "4.0")
set(CMAKE_XCODE_ATTRIBUTE_TVOS_DEPLOYMENT_TARGET "12.0")

set(REALM_ENABLE_ASSERTIONS ON CACHE BOOL "Enable release assertions")
set(REALM_XCODE_TOOLCHAIN TRUE)
