if(DEFINED REALM_UTILITIES)
    return()
else()
    set(REALM_UTILITIES 1)
endif()

macro(set_target_xcode_attributes _target)
    set_target_properties(
            "${_target}" PROPERTIES
            XCODE_ATTRIBUTE_SKIP_INSTALL "NO"
            XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL "\$(GCC_OPTIMIZATION_LEVEL_\$(CONFIGURATION))"
            XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL_Debug "0"
            XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL_Release "3"
            XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL_RelWithDebInfo "3"
            XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL_RelMinSize "3"
    )
endmacro()

macro(set_target_resources _target _resources)
    source_group("Resources" FILES ${_resources})
    set_target_properties("${_target}" PROPERTIES
        MACOSX_BUNDLE TRUE
        MACOSX_BUNDLE_GUI_IDENTIFIER "io.realm.${_target}"
        MACOSX_BUNDLE_SHORT_VERSION_STRING "${PROJECT_VERSION}"
        MACOSX_BUNDLE_LONG_VERSION_STRING "${REALM_VERSION}"
        MACOSX_BUNDLE_BUNDLE_VERSION "1"
        RESOURCE "${_resources}"
    )
    set_property(SOURCE "${_resources}" PROPERTY VS_DEPLOYMENT_LOCATION "TestAssets")

    if(NOT WINDOWS_STORE AND NOT APPLE)
        add_custom_command(TARGET "${_target}" POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E make_directory $<TARGET_FILE_DIR:${_target}>/resources
            COMMAND ${CMAKE_COMMAND} -E copy_if_different ${_resources} $<TARGET_FILE_DIR:${_target}>/resources
        )
    endif()
endmacro()

macro(enable_stdfilesystem _target)
    if(APPLE)
        # CMake doesn't reasonably support setting a per-target deployment
        # check, so just disable libc++ availability checks (which will result
        # in crashes at runtime if running on too old of a macOS version).
        target_compile_definitions("${_target}" PRIVATE _LIBCPP_DISABLE_AVAILABILITY)
    elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
            target_link_libraries("${_target}" stdc++fs)
        endif()
    endif()
endmacro()

macro(add_bundled_test _target)
    if(NOT APPLE)
        add_test(NAME ${_target} COMMAND ${_target})
    else()
        # When given a target name, add_test() is supposed to automatically
        # determine the path to the executable. However, this is very broken on
        # Apple platforms. ctest doesn't perform macro expansion, so the path
        # is left with a `$(EFFECTIVE_PLATFORM_NAME)` in it. The generator
        # expression `$<TARGET_FILE:target>` is also just wrong on macOS, as it
        # uses a path suitable for an iOS bundle rather than a macOS bundle. As
        # a result, we have to construct the path manually.
        add_test(NAME ${_target} COMMAND $<TARGET_FILE_NAME:${_target}>.app/Contents/MacOS/$<TARGET_FILE_NAME:${_target}>)
    endif()
endmacro()
