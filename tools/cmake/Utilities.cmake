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
