add_subdirectory(util)
add_custom_target(benchmarks)
add_subdirectory(object-store)

# AFL not yet supported by Windows
if(NOT CMAKE_SYSTEM_NAME MATCHES "^Windows")
    add_subdirectory(fuzzy)
endif()

add_subdirectory(benchmark-common-tasks)
add_subdirectory(benchmark-crud)
add_subdirectory(benchmark-larger)
add_subdirectory(benchmark-sync)
# FIXME: Add other benchmarks

set(CORE_TEST_SOURCES
    # slowest to compile first
    test_query.cpp
    test_query2.cpp
    test_query_big.cpp
    test_table.cpp
    test_lang_bind_helper.cpp
    test_parser.cpp

    test_all.cpp
    test_alloc.cpp
    test_array.cpp
    test_array_blob.cpp
    test_array_blobs_big.cpp
    test_array_blobs_small.cpp
    test_array_float.cpp
    test_array_integer.cpp
    test_array_mixed.cpp
    test_array_string_short.cpp
    test_binary_data.cpp
    test_bplus_tree.cpp
    test_column.cpp
    test_column_float.cpp
    test_column_string.cpp
    test_column_timestamp.cpp
    test_compaction.cpp
    test_db.cpp
    test_decimal128.cpp
    test_destroy_guard.cpp
    test_dictionary.cpp
    test_file.cpp
    test_file_locks.cpp
    test_global_key.cpp
    test_group.cpp
    test_impl_simulated_failure.cpp
    test_index_string.cpp
    test_json.cpp
    test_link_query_view.cpp
    test_links.cpp
    test_metrics.cpp
    test_mixed_null_assertions.cpp
    test_object_id.cpp
    test_optional.cpp
    test_priority_queue.cpp
    test_replication.cpp
    test_safe_int_ops.cpp
    test_self.cpp
    test_set.cpp
    test_shared.cpp
    test_status.cpp
    test_string_data.cpp
    test_table_view.cpp
    test_thread.cpp
    test_transactions.cpp
    test_transactions_lasse.cpp
    test_typed_links.cpp
    test_unresolved_links.cpp
    test_upgrade_database.cpp
    test_utf8.cpp
    test_util_any.cpp
    test_util_backtrace.cpp
    test_util_base64.cpp
    test_util_chunked_binary.cpp
    test_util_cli_args.cpp
    test_util_compression.cpp
    test_util_error.cpp
    test_util_file.cpp
    test_util_fixed_size_buffer.cpp
    test_util_flat_map.cpp
    test_util_from_chars.cpp
    test_util_functional.cpp
    test_util_future.cpp
    test_util_inspect.cpp
    test_util_logger.cpp
    test_util_memory_stream.cpp
    test_util_overload.cpp
    test_util_scope_exit.cpp
    test_util_to_string.cpp
    test_util_type_list.cpp
    test_uuid.cpp
    )

if (REALM_ENABLE_ENCRYPTION)
    list(APPEND CORE_TEST_SOURCES test_encrypted_file_mapping.cpp)
endif()

set(LARGE_TEST_SOURCES
    large_tests/test_column_large.cpp
    large_tests/test_strings.cpp)

set(REALM_TEST_HEADERS
    fuzz_group.hpp
    pthread_test.hpp
    test.hpp
    test_all.hpp
    test_string_types.hpp
    test_table_helper.hpp
    test_types_helper.hpp
    testsettings.hpp
) # REALM_TEST_HEADERS

set(FUZZY_TEST_SOURCES fuzz_group.cpp)

set(CORE_TESTS ${CORE_TEST_SOURCES} ${LARGE_TEST_SOURCES} ${FUZZY_TEST_SOURCES})

# FIXME: Benchmarks

if (MSVC)
    set_source_files_properties(test_query.cpp test_query_big.cpp PROPERTIES COMPILE_FLAGS /bigobj)
endif()

# Resources required for running the tests
file(GLOB REQUIRED_TEST_FILES RELATIVE ${CMAKE_CURRENT_BINARY_DIR}
     "*.json"
     "*.realm"
     "expect_string.txt")

add_executable(CoreTests main.cpp ${CORE_TESTS} ${REQUIRED_TEST_FILES} ${REALM_TEST_HEADERS})
set_target_resources(CoreTests "${REQUIRED_TEST_FILES}")
set_target_properties(CoreTests PROPERTIES
    OUTPUT_NAME "realm-tests"
    VS_WINDOWS_TARGET_PLATFORM_MIN_VERSION 10.0.17134.0
)

if (REALM_TEST_DURATION)
    add_compile_definitions(TEST_DURATION=${REALM_TEST_DURATION})
endif()

if(WINDOWS_STORE)
    file(GLOB UWP_ASSETS "${CMAKE_ROOT}/Templates/Windows/*")

    set(UWP_SOURCES
        Package.appxmanifest
        ${UWP_ASSETS}
        ${REQUIRED_TEST_FILES}
    )

    target_sources(CoreTests PRIVATE ${UWP_SOURCES})
    set_property(SOURCE ${UWP_SOURCES} PROPERTY VS_DEPLOYMENT_CONTENT 1)

    set_property(SOURCE ${UWP_ASSETS} PROPERTY VS_DEPLOYMENT_LOCATION "Assets")
endif()

target_link_libraries(CoreTests TestUtil QueryParser)
enable_stdfilesystem(CoreTests)

if(UNIX AND NOT APPLE)
    # This enables symbols in backtraces
    target_link_libraries(CoreTests "-rdynamic")
endif()

target_include_directories(CoreTests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

add_bundled_test(CoreTests)

if(REALM_ENABLE_SYNC)
    set(SYNC_TESTS
        test_all.cpp
        test_array_sync.cpp
        test_changeset_encoding.cpp
        test_client_reset.cpp
        test_compact_changesets.cpp
        test_crypto.cpp
        test_embedded_objects.cpp
        test_instruction_replication.cpp
        test_lang_bind_helper_sync.cpp
        test_noinst_server_dir.cpp
        test_server_history.cpp
        test_stable_ids.cpp
        test_sync.cpp
        test_sync_auth.cpp
        test_sync_history_migration.cpp
        test_sync_subscriptions.cpp
        test_sync_pending_bootstraps.cpp
        test_transform.cpp
        test_util_buffer_stream.cpp
        test_util_circular_buffer.cpp
        test_util_http.cpp
        test_util_json_parser.cpp
        test_util_network.cpp
        test_util_network_ssl.cpp
        test_util_uri.cpp
        test_util_websocket.cpp
    )

    set(TEST_HEADERS
        test.hpp
        test_all.hpp
        fuzz_tester.hpp
        peer.hpp
        sync_fixtures.hpp
    )

    set(TEST_CLIENT_SOURCES
        client/main.cpp
        client/peer.cpp
    )

    set(TEST_CLIENT_HEADERS
        client/metrics.hpp
        client/peer.hpp
        client/object_observer.hpp
        client/statistics.hpp
    )

    file(GLOB TEST_RESOURCES RELATIVE ${CMAKE_CURRENT_BINARY_DIR}
        *.json *.pem
        ../certificate-authority/certs/*
        ../certificate-authority/root-ca/*.pem
        resources/*.realm
        resources/history_migration/*.realm
        test_flx_metadata_tables_v1.realm)

    if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
        # Embed the manifest on Windows. The manifest allows very long paths (>255).
        list(APPEND SYNC_TESTS test.manifest)
    endif()

    add_executable(SyncTests main.cpp ${SYNC_TESTS} ${TEST_HEADERS} ${TEST_RESOURCES})
    set_target_properties(SyncTests PROPERTIES OUTPUT_NAME "realm-sync-tests")
    set_target_resources(SyncTests "${TEST_RESOURCES}")
    enable_stdfilesystem(SyncTests)
    target_link_libraries(SyncTests Sync SyncServer TestUtil)
    add_bundled_test(SyncTests)

    if(UNIX AND NOT APPLE)
        # This enables symbols in backtraces
        target_link_libraries(SyncTests "-rdynamic")
    endif()
endif()
