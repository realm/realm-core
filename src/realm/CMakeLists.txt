set(REALM_SOURCES
    # slowest to compile first
    query.cpp
    array.cpp

    alloc.cpp
    alloc_slab.cpp
    array_backlink.cpp
    array_binary.cpp
    array_blob.cpp
    array_blobs_small.cpp
    array_blobs_big.cpp
    array_decimal128.cpp
    array_fixed_bytes.cpp
    array_integer.cpp
    array_key.cpp
    array_mixed.cpp
    array_unsigned.cpp
    array_string.cpp
    array_string_short.cpp
    array_timestamp.cpp
    bplustree.cpp
    chunked_binary.cpp
    cluster.cpp
    collection.cpp
    cluster_tree.cpp
    table_cluster_tree.cpp
    column_binary.cpp
    decimal128.cpp
    dictionary.cpp
    disable_sync_to_disk.cpp
    exceptions.cpp
    group.cpp
    db.cpp
    group_writer.cpp
    history.cpp
    impl/output_stream.cpp
    impl/simulated_failure.cpp
    impl/transact_log.cpp
    index_string.cpp
    list.cpp
    node.cpp
    mixed.cpp
    obj.cpp
    global_key.cpp
    query_engine.cpp
    query_expression.cpp
    replication.cpp
    spec.cpp
    string_data.cpp
    table.cpp
    table_ref.cpp
    obj_list.cpp
    object_id.cpp
    table_view.cpp
    sort_descriptor.cpp
    unicode.cpp
    utilities.cpp
    uuid.cpp
    version.cpp
) # REALM_SOURCES

set(UTIL_SOURCES
    util/allocator.cpp
    util/backtrace.cpp
    util/base64.cpp
    util/basic_system_errors.cpp
    util/encrypted_file_mapping.cpp
    util/fifo_helper.cpp
    util/file.cpp
    util/file_mapper.cpp
    util/interprocess_condvar.cpp
    util/logger.cpp
    util/memory_stream.cpp
    util/platform_info.cpp
    util/misc_errors.cpp
    util/serializer.cpp
    util/sha_crypto.cpp
    util/terminate.cpp
    util/time.cpp
    util/timestamp_formatter.cpp
    util/timestamp_logger.cpp
    util/thread.cpp
    util/to_string.cpp
    util/allocation_metrics.cpp
    util/copy_dir_recursive.cpp
    util/demangle.cpp
    util/duplicating_logger.cpp
    util/enum.cpp
    util/file_is_regular.cpp
    util/http.cpp
    util/json_parser.cpp
    util/load_file.cpp
    util/misc_ext_errors.cpp
    util/parent_dir.cpp
    util/random.cpp
    util/resource_limits.cpp
    util/substitute.cpp
    util/uri.cpp
    util/websocket.cpp
)

set(REALM_INSTALL_GENERAL_HEADERS
    alloc.hpp
    alloc_slab.hpp
    array.hpp
    array_backlink.hpp
    array_basic.hpp
    array_basic_tpl.hpp
    array_binary.hpp
    array_blob.hpp
    array_blobs_big.hpp
    array_blobs_small.hpp
    array_bool.hpp
    array_decimal128.hpp
    array_direct.hpp
    array_fixed_bytes.hpp
    array_integer.hpp
    array_mixed.hpp
    array_key.hpp
    array_list.hpp
    array_ref.hpp
    array_string.hpp
    array_string_short.hpp
    array_timestamp.hpp
    array_unsigned.hpp
    binary_data.hpp
    bplustree.hpp
    chunked_binary.hpp
    cluster.hpp
    cluster_tree.hpp
    table_cluster_tree.hpp
    collection.hpp
    column_binary.hpp
    column_integer.hpp
    column_fwd.hpp
    column_type.hpp
    column_type_traits.hpp
    data_type.hpp
    decimal128.hpp
    dictionary.hpp
    disable_sync_to_disk.hpp
    exceptions.hpp
    group.hpp
    db.hpp
    db_options.hpp
    group_writer.hpp
    handover_defs.hpp
    history.hpp
    index_string.hpp
    keys.hpp
    mixed.hpp
    null.hpp
    list.hpp
    node.hpp
    node_header.hpp
    obj.hpp
    global_key.hpp
    owned_data.hpp
    query.hpp
    query_conditions.hpp
    query_engine.hpp
    query_expression.hpp
    realm_nmmintrin.h
    replication.hpp
    spec.hpp
    string_data.hpp
    table.hpp
    table_ref.hpp
    sort_descriptor.hpp
    obj_list.hpp
    object_id.hpp
    table_view.hpp
    timestamp.hpp
    unicode.hpp
    utilities.hpp
    uuid.hpp
    version.hpp
    version_id.hpp
) # REALM_INSTALL_GENERAL_HEADERS

set(REALM_INSTALL_IMPL_HEADERS
    impl/array_writer.hpp
    impl/cont_transact_hist.hpp
    impl/destroy_guard.hpp
    impl/input_stream.hpp
    impl/output_stream.hpp
    impl/simulated_failure.hpp
    impl/transact_log.hpp
)

set(REALM_INSTALL_UTIL_HEADERS
    util/allocator.hpp
    util/any.hpp
    util/assert.hpp
    util/backtrace.hpp
    util/base64.hpp
    util/basic_system_errors.hpp
    util/bind_ptr.hpp
    util/buffer.hpp
    util/buffer_stream.hpp
    util/call_with_tuple.hpp
    util/fixed_size_buffer.hpp
    util/cf_ptr.hpp
    util/encrypted_file_mapping.hpp
    util/features.h
    util/fifo_helper.hpp
    util/file.hpp
    util/file_mapper.hpp
    util/function_ref.hpp
    util/hex_dump.hpp
    util/inspect.hpp
    util/interprocess_condvar.hpp
    util/interprocess_mutex.hpp
    util/logger.hpp
    util/memory_stream.hpp
    util/misc_errors.hpp
    util/miscellaneous.hpp
    util/optional.hpp
    util/overload.hpp
    util/priority_queue.hpp
    util/safe_int_ops.hpp
    util/serializer.hpp
    util/scope_exit.hpp
    util/shared_ptr.hpp
    util/string_buffer.hpp
    util/terminate.hpp
    util/thread.hpp
    util/to_string.hpp
    util/type_list.hpp
    util/type_traits.hpp
    util/utf8.hpp
) # REALM_INSTALL_UTIL_HEADERS

set(REALM_METRICS_HEADERS
    metrics/metrics.hpp
    metrics/metric_timer.hpp
    metrics/query_info.hpp
    metrics/transaction_info.hpp
) # REALM_METRICS_HEADERS

set(REALM_OTHER_UTIL_HEADERS
    util/sha_crypto.hpp
    util/aes_cryptor.hpp
    util/errno.hpp
) # REALM_OTHER_UTIL_HEADERS

set(REALM_INSTALL_ALL_HEADERS
    ${REALM_INSTALL_GENERAL_HEADERS}
    ${REALM_INSTALL_UTIL_HEADERS}
    ${REALM_INSTALL_IMPL_HEADERS}
    ${REALM_OTHER_UTIL_HEADERS}
    ${REALM_METRICS_HEADERS}
)

list(APPEND REALM_SOURCES
    metrics/metrics.cpp
    metrics/metric_timer.cpp
    metrics/query_info.cpp
    metrics/transaction_info.cpp)

if(NOT MSVC)
    list(APPEND REALM_SOURCES util/interprocess_mutex.cpp)
endif()

add_library(Storage STATIC
    ${REALM_SOURCES}
    ${UTIL_SOURCES}
    ${REALM_INSTALL_ALL_HEADERS}
    ${REALM_OBJECT_FILES}
    $<TARGET_OBJECTS:Bid>
)
add_library(Realm::Storage ALIAS Storage)

set_target_properties(Storage PROPERTIES 
    OUTPUT_NAME "realm"
)

target_compile_options(Storage PUBLIC ${REALM_SANITIZER_FLAGS})

target_compile_definitions(Storage PUBLIC
  $<$<CONFIG:Debug>:REALM_DEBUG=1>
  $<$<CONFIG:MinSizeDebug>:REALM_DEBUG=1>
)

if(NOT MSVC)
    target_compile_options(Storage PUBLIC ${REALM_SANITIZER_FLAGS})
    target_link_options(Storage PUBLIC ${REALM_SANITIZER_FLAGS})
endif()

target_include_directories(Storage INTERFACE
    $<BUILD_INTERFACE:${RealmCore_SOURCE_DIR}/src>
    $<BUILD_INTERFACE:${RealmCore_BINARY_DIR}/src>
    $<INSTALL_INTERFACE:include>
)

if(CMAKE_SYSTEM_NAME MATCHES "^Windows")
    target_sources(Storage PRIVATE $<TARGET_OBJECTS:sha_win32>)
    if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
        target_link_libraries(Storage INTERFACE Version.lib psapi.lib)
    endif()
elseif(ANDROID)
    target_link_libraries(Storage INTERFACE atomic)
elseif(APPLE)
    target_link_options(Storage INTERFACE "SHELL:-framework Foundation")
endif()

target_link_libraries(Storage INTERFACE Threads::Threads)

if(UNIX AND NOT APPLE)
    target_link_libraries(Storage PUBLIC OpenSSL::Crypto)
endif()

install(TARGETS Storage EXPORT realm
        ARCHIVE DESTINATION lib
        COMPONENT devel)

install(FILES ${REALM_INSTALL_GENERAL_HEADERS}
        DESTINATION include/realm
        COMPONENT devel)
install(FILES ${REALM_INSTALL_IMPL_HEADERS}
        DESTINATION include/realm/impl
        COMPONENT devel)
install(FILES ${REALM_INSTALL_UTIL_HEADERS}
        DESTINATION include/realm/util
        COMPONENT devel)
install(FILES ${REALM_OTHER_UTIL_HEADERS}
        DESTINATION include/realm/util
        COMPONENT devel)
install(FILES ${REALM_METRICS_HEADERS}
        DESTINATION include/realm/metrics
        COMPONENT devel)
install(FILES ${PROJECT_BINARY_DIR}/src/realm/version_numbers.hpp
        DESTINATION include/realm
        COMPONENT devel)
install(FILES ${PROJECT_BINARY_DIR}/src/realm/util/config.h
        DESTINATION include/realm/util
        COMPONENT devel)

add_subdirectory(parser)
add_subdirectory(exec)
add_subdirectory(object-store)
if (REALM_ENABLE_SYNC)
    add_subdirectory(sync)
endif()
