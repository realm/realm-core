set(TEST_AFL_SOURCES
    object_store_afl.cpp
    fuzz_object_store.cpp
) # TEST_AFL_SOURCES_OBJECT_STORE

set(TEST_LIBFUZZER_SOURCES
    libfuzzer_object_store_entry.cpp
    fuzz_object_store.cpp
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

add_executable(fuzz-object-store ${TEST_AFL_SOURCES})
target_link_libraries(fuzz-object-store TestUtil Storage ObjectStore)

if(REALM_LIBFUZZER)
    add_executable(realm-libfuzzer-object-store ${TEST_LIBFUZZER_SOURCES})
    target_link_libraries(rrealm-libfuzzer-object-store TestUtil Storage ObjectStore)
endif()
