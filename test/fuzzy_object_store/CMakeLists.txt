# for now support just apple platforms
# the aim is to try to reproduce the corruption bug we have been seeing.
if(APPLE)
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
        "scripts/stop_fuzz_afl.sh"
        "scripts/start_lib_fuzzer.sh"
        "scripts/stop_lib_fuzzer.sh")

        file(COPY ${AFL_RUN_SCRIPTS}
            DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
        file(GLOB AFL_SEEDS "testcases/*")
        file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testcases)
        file(COPY ${AFL_SEEDS}
            DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/testcases)
        add_executable(afl-realm-fuzz ${TEST_AFL_SOURCES})
        target_link_libraries(afl-realm-fuzz TestUtil ObjectStore)

    if(REALM_LIBFUZZER)
        if(${CMAKE_CXX_COMPILER_ID} MATCHES "Clang")
            add_executable(libfuzz-realm ${TEST_LIBFUZZER_SOURCES})
            add_compile_options("-fsanitize=fuzzer")
            target_link_libraries(libfuzz-realm TestUtil ObjectStore)
        endif()
    endif()
endif()
