include_directories(../external/catch/single_include ../external/json .)

set(HEADERS
    util/event_loop.hpp
    util/index_helpers.hpp
    util/test_file.hpp
    util/test_utils.hpp
)

set(SOURCES
    collection_change_indices.cpp
    frozen_objects.cpp
    index_set.cpp
    list.cpp
    main.cpp
    migrations.cpp
    object.cpp
    object_store.cpp
    realm.cpp
    results.cpp
    schema.cpp
    thread_safe_reference.cpp
    transaction_log_parsing.cpp
    uuid.cpp

    util/event_loop.cpp
    util/test_file.cpp
    util/test_utils.cpp
)


if(REALM_ENABLE_SYNC)
    list(APPEND HEADERS
        sync/sync_test_utils.hpp
        sync/session/session_util.hpp
    )
    list(APPEND SOURCES
        bson.cpp
        sync/file.cpp
        sync/metadata.cpp
        sync/remote_mongo_tests.cpp
        sync/session/connection_change_notifications.cpp
        sync/session/progress_notifications.cpp
        sync/session/session.cpp
        sync/session/wait_for_completion.cpp
        sync/sync_manager.cpp
        sync/sync_test_utils.cpp
        sync/user.cpp
        sync/app.cpp
    )

endif()

add_executable(tests ${SOURCES} ${HEADERS})

set(REALM_ENABLE_AUTH_TESTS OFF CACHE BOOL "")

if(REALM_ENABLE_AUTH_TESTS AND NOT REALM_MONGODB_ENDPOINT)
    message(FATAL_ERROR "REALM_MONGODB_ENDPOINT must be set when specifying REALM_ENABLE_AUTH_TESTS.")
endif()

if(REALM_ENABLE_AUTH_TESTS AND NOT REALM_STITCH_CONFIG)
    message(FATAL_ERROR "REALM_STITCH_CONFIG must be set when specifying REALM_ENABLE_AUTH_TESTS.")
endif()

set(EXTRA_LIBRARIES "")

if (REALM_ENABLE_AUTH_TESTS)
    add_definitions(-DREALM_ENABLE_AUTH_TESTS=1)
    add_definitions(-DREALM_MONGODB_ENDPOINT="${REALM_MONGODB_ENDPOINT}")
    add_definitions(-DREALM_STITCH_CONFIG="${REALM_STITCH_CONFIG}")
    SET(EXTRA_LIBRARIES curl)
endif()

target_compile_definitions(tests PRIVATE ${PLATFORM_DEFINES})

if(VSCODE_TEST_RUNNER)
    # Increase the Catch2 virtual console width so that the Visual Studio Code
    # Test Explorer extension can parse long test names
    target_compile_definitions(tests PRIVATE -DCATCH_CONFIG_CONSOLE_WIDTH=300)
endif()

if(REALM_ENABLE_SYNC)
    # It's necessary to explicitly link to realm-sync here to control the order in which libraries are
    # linked to avoid link errors when using GNU ld.
    target_link_libraries(tests realm-sync realm-sync-server)
endif()

target_link_libraries(tests realm-object-store ${EXTRA_LIBRARIES} ${PLATFORM_LIBRARIES} realm-parser)

add_custom_command(TARGET tests POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy_if_different ${CMAKE_CURRENT_SOURCE_DIR}/sync-1.x.realm $<TARGET_FILE_DIR:tests>)

create_coverage_target(generate-coverage tests)

add_custom_target(run-tests USES_TERMINAL DEPENDS tests COMMAND ./tests)

add_subdirectory(notifications-fuzzer)
add_subdirectory(benchmarks)
