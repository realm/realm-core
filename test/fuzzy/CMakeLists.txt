set(TEST_AFL_SOURCES
    afl.cpp
    ../fuzz_group.cpp
) # TEST_AFL_SOURCES

set(TEST_AFL_TRANSACT_LOG
    fuzz_transact_log.cpp
    ../fuzz_group.cpp
) # TEST_AFL_TRANSACT_LOG

set(TEST_LIBFUZZER_SOURCES
    libfuzzer_entry.cpp
    ../fuzz_group.cpp
) # TEST_LIBFUZZER_SOURCES

file(GLOB AFL_RUN_SCRIPTS
     "simple_start.sh"
     "simple_stop.sh"
     "start_parallel_fuzzer.sh"
     "stop_parallel_fuzzer.sh")

file(COPY ${AFL_RUN_SCRIPTS}
     DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

file(GLOB AFL_SEEDS "testcases/*")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testcases)
file(COPY ${AFL_SEEDS}
     DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/testcases)


add_executable(fuzz-transact-log ${TEST_AFL_TRANSACT_LOG})
target_link_libraries(fuzz-transact-log ${PLATFORM_LIBRARIES} test-util realm)

add_executable(fuzz-group ${TEST_AFL_SOURCES})
target_link_libraries(fuzz-group ${PLATFORM_LIBRARIES} test-util realm)

add_executable(realm-libfuzzer ${TEST_LIBFUZZER_SOURCES})
target_link_libraries(realm-libfuzzer ${PLATFORM_LIBRARIES} test-util realm)

