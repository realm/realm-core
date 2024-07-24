add_subdirectory(util)
add_custom_target(benchmarks)
add_subdirectory(object-store)

if(CMAKE_SYSTEM_NAME MATCHES "^Windows")
    # Increase the Catch2 virtual console width because our test names can be very long and they break test reports
    set(CATCH_CONFIG_CONSOLE_WIDTH 300)
elseif(APPLE AND NOT CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(CATCH_CONFIG_NO_POSIX_SIGNALS ON CACHE BOOL "Disable Catch support for POSIX signals on Apple platforms other than macOS")
endif()
set(CATCH_CONFIG_ENABLE_ALL_STRINGMAKERS ON CACHE BOOL "Enable Catch2's optional standard library stringmakers")
add_subdirectory(external/catch)

# Cmake does not let us easily set the deployment target per-target, but Catch2
# can use modern features because we only run tests on recent OS versions.
target_compile_definitions(Catch2 PRIVATE _LIBCPP_DISABLE_AVAILABILITY)

# AFL and LIBFUZZER not yet supported by Windows
if(NOT CMAKE_SYSTEM_NAME MATCHES "^Windows" AND NOT EMSCRIPTEN AND NOT ANDROID)
    add_subdirectory(fuzzy)
    add_subdirectory(realm-fuzzer)
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
    test_list.cpp
    test_mixed_null_assertions.cpp
    test_object_id.cpp
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
    test_util_logger.cpp
    test_util_memory_stream.cpp
    test_util_overload.cpp
    test_util_scope_exit.cpp
    test_util_to_string.cpp
    test_uuid.cpp
)

if (REALM_ENABLE_ENCRYPTION)
    list(APPEND CORE_TEST_SOURCES test_encrypted_file_mapping.cpp)
endif()

if (REALM_ENABLE_GEOSPATIAL)
    list(APPEND CORE_TEST_SOURCES test_query_geo.cpp)
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

set_source_files_properties(test_query_geo.cpp test_json.cpp PROPERTIES
    INCLUDE_DIRECTORIES "${RealmCore_SOURCE_DIR}/src/external"
    # the only flag not supported with pragma diagnostic disable in src file by gcc until 13.
    COMPILE_FLAGS "$<$<CXX_COMPILER_ID:GNU>: -Wno-unknown-pragmas>"
)

if (MSVC)
    append_source_file_compile_options(
        FILES test_parser.cpp test_table.cpp test_query.cpp test_query_big.cpp
        FLAGS /bigobj
    )
endif()

# Resources required for running the tests
file(GLOB REQUIRED_TEST_FILES
     "*.json"
     "*.realm"
     "expect_string.txt")

add_library(CoreTestLib OBJECT ${CORE_TESTS} ${REQUIRED_TEST_FILES} ${REALM_TEST_HEADERS})
enable_stdfilesystem(CoreTestLib)
target_link_libraries(CoreTestLib QueryParser Bson)

add_executable(CoreTests main.cpp test_all.cpp ${REQUIRED_TEST_FILES})
target_link_libraries(CoreTests CoreTestLib TestUtil)
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

enable_stdfilesystem(CoreTests)

if(UNIX AND NOT APPLE)
    # This enables symbols in backtraces
    target_link_libraries(CoreTests "-rdynamic")
endif()

target_include_directories(CoreTests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

add_bundled_test(CoreTests)

# SyncTests
if(REALM_ENABLE_SYNC)
    set(SYNC_TESTS
        test_array_sync.cpp
        test_changeset_encoding.cpp
        test_client_reset.cpp
        test_compact_changesets.cpp
        test_crypto.cpp
        test_embedded_objects.cpp
        test_instruction_replication.cpp
        test_lang_bind_helper_sync.cpp
        test_noinst_server_dir.cpp
        test_stable_ids.cpp
        test_sync.cpp
        test_sync_auth.cpp
        test_sync_history_migration.cpp
        test_sync_protocol_codec.cpp
        test_sync_subscriptions.cpp
        test_sync_pending_bootstraps.cpp
        test_sync_error_backoff.cpp
        test_transform_collections_mixed.cpp
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

    set(SYNC_TEST_HEADERS
        test.hpp
        test_all.hpp
        fuzz_tester.hpp
        peer.hpp
        sync_fixtures.hpp
    )

    file(GLOB SYNC_TEST_RESOURCES
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

    add_library(SyncTestLib OBJECT ${SYNC_TESTS} ${SYNC_TEST_HEADERS} ${SYNC_TEST_RESOURCES})
    enable_stdfilesystem(SyncTestLib)
    target_link_libraries(SyncTestLib Sync SyncServer Storage)

    add_executable(SyncTests main.cpp test_all.cpp ${SYNC_TEST_RESOURCES})
    set_target_properties(SyncTests PROPERTIES OUTPUT_NAME "realm-sync-tests")
    set_target_resources(SyncTests "${SYNC_TEST_RESOURCES}")
    enable_stdfilesystem(SyncTests)
    target_link_libraries(SyncTests SyncTestLib TestUtil)
    add_bundled_test(SyncTests)

    if(UNIX AND NOT APPLE)
        # This enables symbols in backtraces
        target_link_libraries(SyncTests "-rdynamic")
    endif()
endif()

# CombinedTests
get_directory_property(OS_TEST_RESOURCES DIRECTORY object-store TEST_RESOURCES)
set(COMBINED_RESOURCES "${REQUIRED_TEST_FILES};${SYNC_TEST_RESOURCES};${OS_TEST_RESOURCES}")

add_executable(CombinedTests combined_tests.cpp test_all.cpp ${COMBINED_RESOURCES})
set_target_resources(CombinedTests "${COMBINED_RESOURCES}")
set_target_properties(CombinedTests PROPERTIES
    OUTPUT_NAME "realm-combined-tests"
    VS_WINDOWS_TARGET_PLATFORM_MIN_VERSION 10.0.17134.0
)
target_include_directories(CombinedTests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}" "object-store")

if(WINDOWS_STORE)
    target_sources(CombinedTests PRIVATE ${UWP_SOURCES})
endif()

if(REALM_ENABLE_SYNC)
    target_link_libraries(CombinedTests ObjectStoreTestLib CoreTestLib SyncTestLib TestUtil)
else()
    target_link_libraries(CombinedTests ObjectStoreTestLib CoreTestLib TestUtil)
endif()

enable_stdfilesystem(CombinedTests)

if(UNIX AND NOT APPLE)
    # This enables symbols in backtraces
    target_link_libraries(CombinedTests "-rdynamic")
endif()

