add_executable(FuzzGroup EXCLUDE_FROM_ALL afl.cpp ../fuzz_group.cpp)
add_executable(realm-fuzz-group ALIAS FuzzGroup) # TODO: Remove when the scripts are updated
set_target_properties(FuzzGroup PROPERTIES OUTPUT_NAME realm-fuzz-group)
target_link_libraries(FuzzGroup ${PLATFORM_LIBRARIES} TestUtil)

add_executable(FuzzTransactLog EXCLUDE_FROM_ALL fuzz_transact_log.cpp ../fuzz_group.cpp)
add_executable(realm-fuzz-transact-log ALIAS FuzzTransactLog) # TODO: Remove when the scripts are updated
set_target_properties(FuzzTransactLog PROPERTIES OUTPUT_NAME realm-fuzz-transact-log)
target_link_libraries(FuzzTransactLog TestUtil)

file(
    COPY
        "simple_start.sh"
        "simple_stop.sh"
        "start_parallel_fuzzer.sh"
        "stop_parallel_fuzzer.sh"
    DESTINATION ${RealmCore_BINARY_DIR}/test/fuzzy
)

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testcases)
file(
    COPY testcases/test0
    DESTINATION ${RealmCore_BINARY_DIR}/test/fuzzy/testcases
)
