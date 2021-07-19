set(TEST_UTIL_SOURCES
    benchmark_results.cpp
    demangle.cpp
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
) # TEST_UTIL_SOURCES

add_library(TestUtil ${TEST_UTIL_SOURCES})

target_link_libraries(TestUtil Storage)

if(UNIX AND NOT APPLE)
    find_library(LIBRT rt)
    if(LIBRT)
        target_link_libraries(TestUtil ${LIBRT})
    else()
        message(WARNING "librt was not found. This means that the benchmarks will not be able to link properly.")
    endif()
endif()



