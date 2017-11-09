if(DEFINED REALM_UTILITIES)
    return()
else()
    set(REALM_UTILITIES 1)
endif()

if(UNIX AND NOT APPLE AND NOT ANDROID)
    set(LINUX ON)
endif()

macro(check_generator _generator)
    string(COMPARE EQUAL "${CMAKE_GENERATOR}" "${_generator}" _is_correct_generator)
    if(NOT _is_correct_generator)
        message(FATAL_ERROR "Wrong generator. '${_generator}' was expected but '${CMAKE_GENERATOR}' was found.")
    endif()
endmacro()

macro(set_common_xcode_attributes)
    set(REALM_ENABLE_ASSERTIONS ON)

    list(APPEND CMAKE_CONFIGURATION_TYPES MinSizeDebug)
    list(REMOVE_DUPLICATES CMAKE_CONFIGURATION_TYPES)

    unset(CMAKE_XCODE_ATTRIBUTE_INSTALL_PATH)
    set(CMAKE_XCODE_ATTRIBUTE_CLANG_CXX_LIBRARY "libc++")
endmacro()

macro(set_bitcode_attributes)
    set(CMAKE_XCODE_ATTRIBUTE_BITCODE_GENERATION_MODE "\$(BITCODE_GENERATION_MODE_\$(CONFIGURATION))")
    set(CMAKE_XCODE_ATTRIBUTE_BITCODE_GENERATION_MODE_Debug "marker")
    set(CMAKE_XCODE_ATTRIBUTE_BITCODE_GENERATION_MODE_MinSizeDebug "marker")
    set(CMAKE_XCODE_ATTRIBUTE_BITCODE_GENERATION_MODE_Release "bitcode")
    set(CMAKE_XCODE_ATTRIBUTE_BITCODE_GENERATION_MODE_RelWithDebInfo "bitcode")
    set(CMAKE_XCODE_ATTRIBUTE_BITCODE_GENERATION_MODE_MinSizeRel "bitcode")
endmacro()

macro(fix_xcode_try_compile)
    set(MACOSX_BUNDLE_GUI_IDENTIFIER io.realm)
    set(CMAKE_MACOSX_BUNDLE YES)
    set(CMAKE_XCODE_ATTRIBUTE_CODE_SIGNING_REQUIRED NO)
endmacro()

macro(set_target_xcode_attributes _target)
    set_target_properties(
            "${_target}" PROPERTIES
            XCODE_ATTRIBUTE_SKIP_INSTALL "NO"
            XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL "\$(GCC_OPTIMIZATION_LEVEL_\$(CONFIGURATION))"
            XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL_Debug "0"
            XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL_MinSizeDebug "z"
            XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL_Release "3"
            XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL_RelWithDebInfo "3"
            XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL_RelMinSize "3"
    )
endmacro()

macro(get_package_file_name _PACKAGE_NAME _PACKAGE_VERSION)
    if(NOT DEFINED CMAKE_BUILD_TYPE)
        message(WARNING "In order to generate the proper package name CMAKE_BUILD_TYPE must be set also on multi-configuration generators")
    endif()

    if(ANDROID)
        set(REALM_OS "Android-${ANDROID_ABI}")
    elseif(LINUX)
        set(REALM_OS "Linux-${CMAKE_SYSTEM_PROCESSOR}")
    elseif(WINDOWS_STORE)
        set(REALM_OS "UWP-${CMAKE_GENERATOR_PLATFORM}")
    elseif(WIN32)
        set(REALM_OS "Windows-${CMAKE_GENERATOR_PLATFORM}")
    elseif(APPLE)
        if(CMAKE_XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS MATCHES "macosx")
            set(REALM_OS Darwin)
        elseif(CMAKE_XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS MATCHES "iphoneos")
            set(REALM_OS iphoneos)
        elseif(CMAKE_XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS MATCHES "watchos")
            set(REALM_OS watchos)
        elseif(CMAKE_XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS MATCHES "tvos")
            set(REALM_OS tvos)
        else()
            set(REALM_OS Darwin)
        endif()
    endif()

    set("${_PACKAGE_NAME}_FILE_NAME"
        "${_PACKAGE_NAME}-${CMAKE_BUILD_TYPE}-${_PACKAGE_VERSION}-${REALM_OS}"
    )
endmacro()
