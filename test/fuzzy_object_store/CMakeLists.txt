# the aim is to try to reproduce the corruption bug we have been seeing.
set(TEST_AFL_SOURCES
    afl_runner.cpp
    fuzz_engine.cpp
    fuzz_object.cpp
    fuzz_configurator.cpp
) # TEST_AFL_SOURCES_OBJECT_STORE

set(TEST_LIBFUZZER_SOURCES
    libfuzzer_runner.cpp
    fuzz_engine.cpp
    fuzz_object.cpp
    fuzz_configurator.cpp
) # TEST_LIBFUZZER_SOURCES_OBJECT_STORE

file(GLOB AFL_RUN_SCRIPTS
    "scripts/start_fuzz_afl.sh"
    "scripts/start_lib_fuzzer.sh")

file(COPY ${AFL_RUN_SCRIPTS}
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(GLOB AFL_SEEDS "testcases/*")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testcases)
file(COPY ${AFL_SEEDS}
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/testcases)

if(NOT APPLE AND NOT WINDOWS_STORE)
    find_package(LibUV REQUIRED)
    set(libuv_target LibUV::LibUV)
endif()

add_executable(realm-afl++ ${TEST_AFL_SOURCES})
target_link_libraries(realm-afl++ TestUtil ObjectStore ${libuv_target}) 

if(REALM_LIBFUZZER)
    if(${CMAKE_CXX_COMPILER_ID} MATCHES "Clang")
        add_executable(realm-libfuzz ${TEST_LIBFUZZER_SOURCES})
        target_link_libraries(realm-libfuzz TestUtil ObjectStore ${libuv_target})
    endif()
endif()
