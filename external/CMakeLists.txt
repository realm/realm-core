cmake_minimum_required(VERSION 3.15)

add_cxx_flag_if_supported(-Wno-unused-but-set-variable)

if (NOT REALM_BUILD_LIB_ONLY AND NOT REALM_NO_TESTS)
    add_subdirectory(catch)
endif()
