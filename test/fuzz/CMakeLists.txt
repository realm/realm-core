
# if AFL build is enabled, add the AFL-specific stuff to the output portfolio

if(REALM_AFL OR REALM_BUILD_FUZZING_TOOLS)
    set(FUZZ_TRANSFORM_SOURCES
        fuzz_transform.cpp
    )

    add_executable(FuzzTransform ${FUZZ_TRANSFORM_SOURCES})
    set_target_properties(FuzzTransform PROPERTIES OUTPUT_NAME "fuzz-transform")
    target_link_libraries(FuzzTransform TestUtils Sync)

    add_custom_command(TARGET FuzzTransform POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_SOURCE_DIR}/test/fuzz/testcases $<TARGET_FILE_DIR:FuzzTransform>/testcases)

    add_custom_command(TARGET FuzzTransform POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_SOURCE_DIR}/test/fuzz/start_parallel_fuzzer.sh $<TARGET_FILE_DIR:FuzzTransform>)

    add_custom_command(TARGET FuzzTransform POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_SOURCE_DIR}/test/fuzz/stop_parallel_fuzzer.sh $<TARGET_FILE_DIR:FuzzTransform>)
endif()
