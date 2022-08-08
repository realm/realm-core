set(TEST_UTIL_SOURCES
    benchmark_results.cpp
    crypt_key.cpp
    demangle.cpp
    misc.cpp
    quote.cpp
    random.cpp
    resource_limits.cpp
    test_only.cpp
    test_path.cpp
    timer.cpp
    unit_test.cpp
    verified_integer.cpp
    verified_string.cpp
    wildcard.cpp
) # TEST_UTIL_SOURCES

set(TEST_UTIL_HEADERS
    benchmark_results.hpp
    check_logic_error.hpp
    check_system_error.hpp
    compare_groups.hpp
    crypt_key.hpp
    demangle.hpp
    dump_changesets.hpp
    misc.hpp
    number_names.hpp
    quote.hpp
    random.hpp
    resource_limits.hpp
    semaphore.hpp
    super_int.hpp
    test_logger.hpp
    test_only.hpp
    test_path.hpp
    test_types.hpp
    thread_wrapper.hpp
    timer.hpp
    unit_test.hpp
    verified_integer.hpp
    verified_string.hpp
    wildcard.hpp
) # TEST_UTIL_HEADERS

if(REALM_ENABLE_SYNC)
    list(APPEND TEST_UTIL_SOURCES
        compare_groups.cpp
        dump_changesets.cpp
    )
endif()

add_library(TestUtil STATIC ${TEST_UTIL_SOURCES} ${TEST_UTIL_HEADERS})

target_link_libraries(TestUtil Storage)

if(UNIX AND NOT APPLE)
    find_library(LIBRT rt)
    if(LIBRT)
        target_link_libraries(TestUtil ${LIBRT})
    # Android has librt included in libc
    elseif(NOT ANDROID)
        message(WARNING "librt was not found. This means that the benchmarks will not be able to link properly.")
    endif()
endif()
