if(DEFINED REALM_UTILITIES)
    return()
else()
    set(REALM_UTILITIES 1)
endif()

macro(check_generator _generator)
    string(COMPARE EQUAL "${CMAKE_GENERATOR}" "${_generator}" _is_correct_generator)
    if(NOT _is_correct_generator)
        message(FATAL_ERROR "Wrong generator. '${_generator}' was expected but '${CMAKE_GENERATOR}' was found.")
    endif()
endmacro()

macro(set_common_xcode_attributes)
    set(REALM_ENABLE_ASSERTIONS ON CACHE BOOL "Enable release assertions")

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

include(GNUInstallDirs)
macro(install_arch_slices_for_platform _platform)
    if(NOT "${CMAKE_BUILD_TYPE}" STREQUAL "Release")
        set(BUILD_SUFFIX "-dbg")
    endif()
    # Device builds will contain '-device' so it does not collide with the fat libs
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/src/realm/${CMAKE_BUILD_TYPE}-${_platform}os/librealm${BUILD_SUFFIX}.a
            DESTINATION ${CMAKE_INSTALL_LIBDIR}/ RENAME librealm-${_platform}-device${BUILD_SUFFIX}.a
            COMPONENT devel
    )

    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/src/realm/${CMAKE_BUILD_TYPE}-${_platform}simulator/librealm${BUILD_SUFFIX}.a
            DESTINATION ${CMAKE_INSTALL_LIBDIR}/ RENAME librealm-${_platform}-simulator${BUILD_SUFFIX}.a
            COMPONENT devel
    )

    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/src/realm/parser/${CMAKE_BUILD_TYPE}-${_platform}os/librealm-parser${BUILD_SUFFIX}.a
            DESTINATION ${CMAKE_INSTALL_LIBDIR}/ RENAME librealm-parser-${_platform}-device${BUILD_SUFFIX}.a
            COMPONENT devel
    )

    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/src/realm/parser/${CMAKE_BUILD_TYPE}-${_platform}simulator/librealm-parser${BUILD_SUFFIX}.a
            DESTINATION ${CMAKE_INSTALL_LIBDIR}/ RENAME librealm-parser-${_platform}-simulator${BUILD_SUFFIX}.a
            COMPONENT devel
    )
endmacro()
