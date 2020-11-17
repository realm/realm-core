set(HEADERS
    util/event_loop.hpp
    util/index_helpers.hpp
    util/test_file.hpp
    util/test_utils.hpp
    collection_fixtures.hpp
)

set(SOURCES
    collection_change_indices.cpp
    frozen_objects.cpp
    index_set.cpp
    dictionary.cpp
    list.cpp
    main.cpp
    migrations.cpp
    object.cpp
    object_store.cpp
    primitive_list.cpp
    realm.cpp
    results.cpp
    set.cpp
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

add_executable(ObjectStoreTests ${SOURCES} ${HEADERS})
set_target_properties(ObjectStoreTests PROPERTIES
    OUTPUT_NAME realm-object-store-tests
)

target_link_libraries(ObjectStoreTests ObjectStore)

create_coverage_target(generate-coverage ObjectStoreTests)

if (NOT REALM_TSAN)
    add_test(NAME ObjectStoreTests COMMAND realm-object-store-tests)
endif()

if(REALM_ENABLE_SYNC)
    target_link_libraries(ObjectStoreTests SyncServer)

    option(REALM_ENABLE_AUTH_TESTS "" OFF)
    if(REALM_ENABLE_AUTH_TESTS)
        if(NOT REALM_MONGODB_ENDPOINT)
            message(FATAL_ERROR "REALM_MONGODB_ENDPOINT must be set when specifying REALM_ENABLE_AUTH_TESTS.")
        endif()

        if(NOT REALM_STITCH_CONFIG)
            message(FATAL_ERROR "REALM_STITCH_CONFIG must be set when specifying REALM_ENABLE_AUTH_TESTS.")
        endif()

        target_compile_definitions(ObjectStoreTests PRIVATE
            REALM_ENABLE_AUTH_TESTS=1
            REALM_MONGODB_ENDPOINT="${REALM_MONGODB_ENDPOINT}"
            REALM_STITCH_CONFIG="${REALM_STITCH_CONFIG}"
        )

        find_package(CURL REQUIRED)
        target_link_libraries(ObjectStoreTests CURL::libcurl)
    endif()
endif()

target_include_directories(ObjectStoreTests PRIVATE ${CATCH_INCLUDE_DIR} ${JSON_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR})

if(VSCODE_TEST_RUNNER)
    # Increase the Catch2 virtual console width so that the Visual Studio Code
    # Test Explorer extension can parse long test names
    target_compile_definitions(ObjectStoreTests PRIVATE -DCATCH_CONFIG_CONSOLE_WIDTH=300)
endif()

add_custom_command(TARGET ObjectStoreTests POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy_if_different ${CMAKE_CURRENT_SOURCE_DIR}/sync-1.x.realm $<TARGET_FILE_DIR:ObjectStoreTests>)

if(NOT APPLE)
    find_package(LibUV REQUIRED)
    # FIXME: ObjectStore itself shouldn't care about this, but we need to refactor scheduler.cpp to make it happen
    target_link_libraries(ObjectStore PUBLIC LibUV::LibUV)
    target_compile_definitions(ObjectStore PUBLIC REALM_HAVE_UV=1)
endif()

add_subdirectory(notifications-fuzzer)
add_subdirectory(benchmarks)
