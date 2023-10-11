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

# Additional arguments will be passed to the _target command
function(add_labeled_test _name _label _target)
    if(NOT APPLE)
        add_test(NAME ${_name} COMMAND ${_target} ${ARGN})
    else()
        # When given a target name, add_test() is supposed to automatically
        # determine the path to the executable. However, this is very broken on
        # Apple platforms. ctest doesn't perform macro expansion, so the path
        # is left with a `$(EFFECTIVE_PLATFORM_NAME)` in it. The generator
        # expression `$<TARGET_FILE:target>` is also just wrong on macOS, as it
        # uses a path suitable for an iOS bundle rather than a macOS bundle. As
        # a result, we have to construct the path manually.
        add_test(NAME ${_name} COMMAND $<TARGET_FILE_NAME:${_target}>.app/Contents/MacOS/$<TARGET_FILE_NAME:${_target}> ${ARGN})
    endif()
    set_tests_properties(${_name} PROPERTIES LABELS "${_label}")
endfunction()

# Parse the lines in a text file, removing comments that begin with '#' and
# removing leading/trailing whitespace.
function(parse_list_file _path _outlist)
    if(EXISTS "${_path}")
        file(STRINGS "${_path}" _test_list)
    endif()
    if(_test_list)
        foreach(_item ${_test_list})
            # Remove comments
            if(_item)
                string(REGEX REPLACE "#.*" "" _item_clean "${_item}")
            endif()
            # Trim leading/trailing space
            string(STRIP "${_item_clean}" _item_trimmed)
            if(_item_trimmed)
                list(APPEND _found_tests "${_item_trimmed}")
            endif()
        endforeach()
        if (_found_tests)
            set(${_outlist} ${_found_tests} PARENT_SCOPE)
        endif()
    endif()
endfunction()

macro(set_macos_only _dir)
    get_property(_targets DIRECTORY "${_dir}" PROPERTY BUILDSYSTEM_TARGETS)
    foreach(_target IN LISTS _targets)
        set_target_properties(
                "${_target}" PROPERTIES
                XCODE_ATTRIBUTE_SDKROOT "macosx"
                XCODE_ATTRIBUTE_SUPPORTED_PLATFORMS "macosx"
        )
    endforeach()
endmacro()
