include(utilities.cmake)

check_generator("Xcode")

fix_xcode_try_compile()

set_common_xcode_attributes()

set(CMAKE_XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS "watchos watchsimulator")
set(CMAKE_XCODE_EFFECTIVE_PLATFORMS "-watchos;-watchsimulator")
set(CMAKE_XCODE_ATTRIBUTE_WATCHOS_DEPLOYMENT_TARGET "2.0")

set(CMAKE_XCODE_ATTRIBUTE_ENABLE_BITCODE[sdk=watch*] "YES")

set_bitcode_attributes()