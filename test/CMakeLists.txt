add_subdirectory(util)
add_subdirectory(object-store)

# AFL not yet supported by Windows
if(NOT CMAKE_SYSTEM_NAME MATCHES "^Windows")
    add_subdirectory(fuzzy)
endif()

add_custom_target(benchmarks)

add_subdirectory(benchmark-common-tasks)
add_subdirectory(benchmark-crud)
add_subdirectory(benchmark-larger)
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
    test_array_blobs_small.cpp
    test_array_blob.cpp
    test_array_blobs_big.cpp
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
    test_db.cpp
    test_decimal128.cpp
    test_destroy_guard.cpp
    test_dictionary.cpp
    test_file.cpp
    test_file_locks.cpp
    test_group.cpp
    test_impl_simulated_failure.cpp
    test_index_string.cpp
    test_json.cpp
    test_link_query_view.cpp
    test_links.cpp
    test_typed_links.cpp
    test_set.cpp
    test_unresolved_links.cpp
    test_metrics.cpp
    test_global_key.cpp
    test_object_id.cpp
    test_optional.cpp
    test_priority_queue.cpp
    test_replication.cpp
    test_safe_int_ops.cpp
    test_self.cpp
    test_shared.cpp
    test_status.cpp
    test_string_data.cpp
    test_table_view.cpp
    test_thread.cpp
    test_transactions.cpp
    test_transactions_lasse.cpp
    test_upgrade_database.cpp
    test_utf8.cpp
    test_util_any.cpp
    test_util_backtrace.cpp
    test_util_base64.cpp
    test_util_cli_args.cpp
    test_util_chunked_binary.cpp
    test_util_error.cpp
    test_util_file.cpp
    test_util_functional.cpp
    test_util_future.cpp
    test_util_from_chars.cpp
    test_util_inspect.cpp
    test_util_logger.cpp
    test_util_memory_stream.cpp
    test_util_overload.cpp
    test_util_scope_exit.cpp
    test_util_stringbuffer.cpp
    test_util_to_string.cpp
    test_util_type_list.cpp
    test_util_fixed_size_buffer.cpp
	test_uuid.cpp
    test_mixed_null_assertions.cpp)

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

if(CMAKE_GENERATOR STREQUAL Xcode)
    set(MAIN_FILE main.mm)
else()
    set(MAIN_FILE main.cpp)
endif()

if (MSVC)
    set_source_files_properties(test_query.cpp test_query_big.cpp PROPERTIES COMPILE_FLAGS /bigobj)
endif()

# Resources required for running the tests
file(GLOB REQUIRED_TEST_FILES
     "*.json"
     "*.realm"
     "expect_string.txt")

add_executable(CoreTests ${CORE_TESTS} ${MAIN_FILE} ${REQUIRED_TEST_FILES} ${REALM_TEST_HEADERS})
set_target_properties(CoreTests PROPERTIES OUTPUT_NAME "realm-tests")

if (REALM_TEST_DURATION)
    add_compile_definitions(TEST_DURATION=${REALM_TEST_DURATION})
endif()

# Resources required for running the tests copied to the target directory
if(CMAKE_GENERATOR STREQUAL Xcode)
    # a simple copy doesn't work for an Xcode build because Xcode also needs to sign them
    set_target_properties(CoreTests PROPERTIES
                          MACOSX_BUNDLE YES
                          RESOURCE "${REQUIRED_TEST_FILES}")
else()
	add_custom_command(
		TARGET CoreTests POST_BUILD
		COMMAND ${CMAKE_COMMAND} -E copy_if_different
		${REQUIRED_TEST_FILES}
		$<TARGET_FILE_DIR:CoreTests>)
endif()

target_link_libraries(CoreTests
                      TestUtil QueryParser
)

if(WINDOWS_STORE)
    set_target_properties(CoreTests PROPERTIES
        VS_WINDOWS_TARGET_PLATFORM_MIN_VERSION 10.0.17134.0
        CXX_STANDARD 17 # the test runner needs to call Windows APIs with C++/WinRT which needs C++ 17
    )

    file(GLOB UWP_ASSETS "${CMAKE_ROOT}/Templates/Windows/*")

    set(UWP_SOURCES
        Package.appxmanifest
        ${UWP_ASSETS}
        ${REQUIRED_TEST_FILES}
    )

    target_sources(CoreTests PRIVATE ${UWP_SOURCES})
    set_property(SOURCE ${UWP_SOURCES} PROPERTY VS_DEPLOYMENT_CONTENT 1)

    set_property(SOURCE ${UWP_ASSETS} PROPERTY VS_DEPLOYMENT_LOCATION "Assets")
    set_property(SOURCE ${REQUIRED_TEST_FILES} PROPERTY VS_DEPLOYMENT_LOCATION "TestAssets")
endif()

if(UNIX AND NOT APPLE)
    # This enables symbols in backtraces
    target_link_libraries(CoreTests "-rdynamic")
endif()

target_include_directories(CoreTests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

add_test(NAME StorageTests COMMAND realm-tests)

if(REALM_ENABLE_SYNC)

	set(TEST_UTIL_SOURCES
	    test_all.cpp
	    util/benchmark_results.cpp
	    util/compare_groups.cpp
	    util/crypt_key.cpp
	    util/demangle.cpp
	    util/dump_changesets.cpp
	    util/misc.cpp
	    util/mock_metrics.cpp
	    util/quote.cpp
	    util/random.cpp
	    util/resource_limits.cpp
	    util/test_only.cpp
	    util/test_path.cpp
	    util/timer.cpp
	    util/unit_test.cpp
	    util/wildcard.cpp
	)

	set(TEST_UTIL_HEADERS
	    test.hpp
	    test_all.hpp
	    util/benchmark_results.hpp
	    util/check_logic_error.hpp
	    util/check_system_error.hpp
	    util/crypt_key.hpp
	    util/demangle.hpp
	    util/misc.hpp
	    util/mock_metrics.hpp
	    util/number_names.hpp
	    util/quote.hpp
	    util/random.hpp
	    util/resource_limits.hpp
	    util/semaphore.hpp
	    util/super_int.hpp
	    util/test_logger.hpp
	    util/test_only.hpp
	    util/test_path.hpp
	    util/test_types.hpp
	    util/thread_wrapper.hpp
	    util/timer.hpp
	    util/unit_test.hpp
	    util/verified_integer.hpp
	    util/verified_string.hpp
	    util/wildcard.hpp
	)

	set(TEST_MAIN_SOURCES
	    main.cpp
	)

	set(SYNC_TESTS
	    test_allocation_metrics.cpp
	    test_array_sync.cpp
	    test_async_open.cpp
	    test_client_reset.cpp
	    test_changeset_encoding.cpp
	    test_crypto.cpp
	    test_compact_changesets.cpp
	    test_embedded_objects.cpp
	    test_encrypt_fingerprint.cpp
	    test_encrypt_transform.cpp
	    test_instruction_replication.cpp
	    test_lang_bind_helper_sync.cpp
	    test_server_history.cpp
	    test_server_legacy_migration.cpp
	    test_scratch_allocator.cpp
	    test_stable_ids.cpp
	    test_sync.cpp
	    test_sync_auth.cpp
	    test_sync_history_migration.cpp
	    test_sync_subscriptions.cpp
	    test_transform.cpp
	    test_util_circular_buffer.cpp
	    test_util_buffer_stream.cpp
	    test_util_http.cpp
	    test_util_json_parser.cpp
	    test_util_network.cpp
	    test_util_network_ssl.cpp
	    test_util_uri.cpp
	    test_util_websocket.cpp
	    test_noinst_compression.cpp
	    test_noinst_server_dir.cpp
	    test_noinst_vacuum.cpp
	)

	set(TEST_HEADERS
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

	set(BENCH_TRANSFORM_SOURCES
	    bench-sync/bench_transform.cpp
	    test_all.cpp
	)

	add_library(TestUtils STATIC ${TEST_UTIL_SOURCES} ${TEST_UTIL_HEADERS})
	target_link_libraries(TestUtils PUBLIC Storage)

	if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
	    # Embed the manifest on Windows. The manifest allows very long paths (>255).
	    list(APPEND SYNC_TESTS test.manifest)
	endif()

	add_executable(SyncTests ${SYNC_TESTS} ${TEST_MAIN_SOURCES} ${TEST_HEADERS})
	set_target_properties(SyncTests PROPERTIES OUTPUT_NAME "realm-sync-tests")
	target_link_libraries(SyncTests Sync SyncServer TestUtils)
	add_test(NAME SyncTests COMMAND realm-sync-tests)

	if(UNIX AND NOT APPLE)
	    # This enables symbols in backtraces
	    target_link_libraries(SyncTests "-rdynamic")
	endif()

	add_executable(BenchTransform EXCLUDE_FROM_ALL ${BENCH_TRANSFORM_SOURCES})
	set_target_properties(BenchTransform PROPERTIES OUTPUT_NAME "bench-transform")
	target_link_libraries(BenchTransform TestUtils Sync)

	if(REALM_BUILD_DOGLESS)
	    add_executable(SyncTestClient ${TEST_CLIENT_SOURCES} ${TEST_CLIENT_HEADERS})
	    set_target_properties(SyncTestClient PROPERTIES OUTPUT_NAME "test-client")
	    target_link_libraries(SyncTestClient Sync Dogless)
	endif()

	file(GLOB test_resources *.json *.pem)

	add_custom_command(TARGET SyncTests POST_BUILD
	                   COMMAND ${CMAKE_COMMAND} -E copy_if_different
	                           ${test_resources}
	                           $<TARGET_FILE_DIR:SyncTests>)

	add_custom_command(TARGET SyncTests POST_BUILD
	                   COMMAND ${CMAKE_COMMAND} -E copy_directory
	                           ${PROJECT_SOURCE_DIR}/certificate-authority
	                           $<TARGET_FILE_DIR:SyncTests>/../certificate-authority)

	add_custom_command(TARGET SyncTests POST_BUILD
	                   COMMAND ${CMAKE_COMMAND} -E copy_directory
	                           ${PROJECT_SOURCE_DIR}/test/resources
	                           $<TARGET_FILE_DIR:SyncTests>/resources)
endif()
