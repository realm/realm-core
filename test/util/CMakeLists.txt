set(TEST_UTIL_SOURCES
    demangle.cpp
    mem.cpp
    timer.cpp
    random.cpp
    quote.cpp
    wildcard.cpp
    unit_test.cpp
    test_path.cpp
    test_only.cpp
    crypt_key.cpp
    misc.cpp
    verified_integer.cpp
    verified_string.cpp
    resource_limits.cpp
    jsmn.cpp
) # TEST_UTIL_SOURCES

if (NOT MSVC)
    list(APPEND TEST_UTIL_SOURCES benchmark_results.cpp)
endif()

add_library(test-util ${TEST_UTIL_SOURCES})

target_link_libraries(test-util realm)

