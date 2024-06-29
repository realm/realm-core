set(REALM_SOURCES
    # slowest to compile first
    query.cpp
    array.cpp
    array_with_find.cpp

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
    collection_parent.cpp
    cluster_tree.cpp
    error_codes.cpp
    column_binary.cpp
    decimal128.cpp
    dictionary.cpp
    disable_sync_to_disk.cpp
    exceptions.cpp
    group.cpp
    db.cpp
    group_writer.cpp
    history.cpp
    impl/copy_replication.cpp
    impl/output_stream.cpp
    impl/simulated_failure.cpp
    impl/transact_log.cpp
    index_string.cpp
    link_translator.cpp
    list.cpp
    node.cpp
    mixed.cpp
    obj.cpp
    object_converter.cpp
    global_key.cpp
    query_engine.cpp
    query_expression.cpp
    query_value.cpp
    replication.cpp
    set.cpp
    spec.cpp
    string_data.cpp
    table.cpp
    table_ref.cpp
    obj_list.cpp
    object_id.cpp
    table_view.cpp
    tokenizer.cpp
    to_json.cpp
    transaction.cpp
    sort_descriptor.cpp
    status.cpp
    unicode.cpp
    utilities.cpp
    uuid.cpp
    version.cpp
    backup_restore.cpp
) # REALM_SOURCES

set(UTIL_SOURCES
    util/backtrace.cpp
    util/base64.cpp
    util/basic_system_errors.cpp
    util/cli_args.cpp
    util/compression.cpp
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
    util/demangle.cpp
    util/enum.cpp
    util/json_parser.cpp
    util/load_file.cpp
    util/misc_ext_errors.cpp
    util/random.cpp
    util/resource_limits.cpp
    util/uri.cpp
    util/bson/bson.cpp
    util/bson/regular_expression.cpp
)

set(REALM_INSTALL_HEADERS
    aggregate_ops.hpp
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
    array_integer_tpl.hpp
    array_key.hpp
    array_list.hpp
    array_mixed.hpp
    array_ref.hpp
    array_string.hpp
    array_string_short.hpp
    array_timestamp.hpp
    array_typed_link.hpp
    array_unsigned.hpp
    array_with_find.hpp
    binary_data.hpp
    bplustree.hpp
    chunked_binary.hpp
    cluster.hpp
    cluster_tree.hpp
    collection.hpp
    collection_parent.hpp
    column_binary.hpp
    column_fwd.hpp
    column_integer.hpp
    column_mixed.hpp
    column_type.hpp
    column_type_traits.hpp
    data_type.hpp
    db.hpp
    db_options.hpp
    decimal128.hpp
    dictionary.hpp
    disable_sync_to_disk.hpp
    error_codes.h
    error_codes.hpp
    exceptions.hpp
    global_key.hpp
    group.hpp
    group_writer.hpp
    handover_defs.hpp
    history.hpp
    index_string.hpp
    keys.hpp
    list.hpp
    mixed.hpp
    node.hpp
    node_header.hpp
    null.hpp
    obj.hpp
    obj_list.hpp
    object_id.hpp
    path.hpp
    owned_data.hpp
    query.hpp
    query_conditions.hpp
    query_engine.hpp
    query_expression.hpp
    query_state.hpp
    query_value.hpp
    realm_nmmintrin.h
    replication.hpp
    search_index.hpp
    set.hpp
    sort_descriptor.hpp
    spec.hpp
    status.hpp
    status_with.hpp
    string_data.hpp
    table.hpp
    table_ref.hpp
    table_view.hpp
    transaction.hpp
    timestamp.hpp
    unicode.hpp
    utilities.hpp
    uuid.hpp
    version.hpp
    version_id.hpp
    backup_restore.hpp

    impl/array_writer.hpp
    impl/changeset_input_stream.hpp
    impl/cont_transact_hist.hpp
    impl/destroy_guard.hpp
    impl/output_stream.hpp
    impl/simulated_failure.hpp
    impl/transact_log.hpp

    util/aes_cryptor.hpp
    util/any.hpp
    util/async_future_adapter.hpp
    util/assert.hpp
    util/backtrace.hpp
    util/base64.hpp
    util/basic_system_errors.hpp
    util/bind_ptr.hpp
    util/bson/bson.hpp
    util/bson/indexed_map.hpp
    util/bson/max_key.hpp
    util/bson/min_key.hpp
    util/bson/mongo_timestamp.hpp
    util/bson/regular_expression.hpp
    util/buffer.hpp
    util/buffer_stream.hpp
    util/cf_ptr.hpp
    util/checked_mutex.hpp
    util/compression.hpp
    util/encrypted_file_mapping.hpp
    util/errno.hpp
    util/features.h
    util/flat_map.hpp
    util/fifo_helper.hpp
    util/file.hpp
    util/file_mapper.hpp
    util/fixed_size_buffer.hpp
    util/function_ref.hpp
    util/functional.hpp
    util/future.hpp
    util/hex_dump.hpp
    util/input_stream.hpp
    util/interprocess_condvar.hpp
    util/interprocess_mutex.hpp
    util/logger.hpp
    util/memory_stream.hpp
    util/misc_errors.hpp
    util/misc_ext_errors.hpp
    util/optional.hpp
    util/overload.hpp
    util/platform_info.hpp
    util/priority_queue.hpp
    util/random.hpp
    util/safe_int_ops.hpp
    util/scope_exit.hpp
    util/serializer.hpp
    util/sha_crypto.hpp
    util/span.hpp
    util/tagged_bool.hpp
    util/terminate.hpp
    util/thread.hpp
    util/to_string.hpp
    util/type_traits.hpp
    util/uri.hpp
) # REALM_INSTALL_HEADERS

set(REALM_NOINST_HEADERS
    link_translator.hpp
    object_converter.hpp
    query_conditions_tpl.hpp
    util/cf_str.hpp
    util/circular_buffer.hpp
    util/cli_args.hpp
    util/demangle.hpp
    util/enum.hpp
    util/from_chars.hpp
    util/json_parser.hpp
    util/load_file.hpp
    util/quote.hpp
    util/resource_limits.hpp
    util/scratch_allocator.hpp
    util/signal_blocker.hpp
    util/thread_exec_guard.hpp
    util/time.hpp
    util/timestamp_formatter.hpp
    util/timestamp_logger.hpp
    util/value_reset_guard.hpp
) # REALM_NOINST_HEADERS

if(NOT MSVC)
    list(APPEND REALM_SOURCES util/interprocess_mutex.cpp)
endif()

if (REALM_ENABLE_GEOSPATIAL)
    list(APPEND REALM_SOURCES geospatial.cpp)
    list(APPEND REALM_INSTALL_HEADERS geospatial.hpp)

    set_source_files_properties(geospatial.cpp PROPERTIES
        INCLUDE_DIRECTORIES "${RealmCore_SOURCE_DIR}/src/external"
        # the only flag not supported with pragma diagnostic disable in src file by gcc until 13.
        COMPILE_FLAGS "$<$<CXX_COMPILER_ID:GNU>: -Wno-unknown-pragmas>"
    )

    list(APPEND REALM_OBJECT_FILES $<TARGET_OBJECTS:s2geometry>)
endif()

add_library(Storage STATIC
    ${REALM_SOURCES}
    ${UTIL_SOURCES}
    ${REALM_INSTALL_HEADERS}
    ${REALM_NOINST_HEADERS}
    ${REALM_OBJECT_FILES}
    $<TARGET_OBJECTS:Bid>
)

add_library(Realm::Storage ALIAS Storage)

set_target_properties(Storage PROPERTIES
    OUTPUT_NAME "realm"
)

if(REALM_USE_FAST_LINKER)
    target_link_options(Storage PUBLIC ${REALM_USE_FAST_LINKER})
endif()

target_compile_definitions(Storage PUBLIC
  $<$<CONFIG:Debug>:REALM_DEBUG=1>
)

if (REALM_SANITIZER_FLAGS)
    target_compile_options(Storage PUBLIC ${REALM_SANITIZER_FLAGS})

    if (REALM_SANITIZER_LINK_FLAGS)
        target_link_options(Storage PUBLIC ${REALM_SANITIZER_LINK_FLAGS})
    endif()
endif()

target_include_directories(Storage INTERFACE
    $<BUILD_INTERFACE:${RealmCore_SOURCE_DIR}/src>
    $<BUILD_INTERFACE:${RealmCore_BINARY_DIR}/src>
    $<INSTALL_INTERFACE:include>
)

# On systems without a built-in SHA-1 implementation (or one provided by a dependency)
# we need to bundle the public domain implementation.
if(NOT APPLE AND NOT CMAKE_SYSTEM_NAME MATCHES "^Windows" AND NOT REALM_HAVE_OPENSSL)
    add_library(sha1 OBJECT ../external/sha-1/sha1.c)
    target_include_directories(Storage PRIVATE ../external/sha-1)
    target_sources(Storage PRIVATE $<TARGET_OBJECTS:sha1>)
endif()

# On systems without a built-in SHA-2 implementation (or one provided by a dependency)
# we need to bundle the public domain implementation.
# Note: This is also used on Windows because Windows lacks a native SHA224 hash needed for realm encryption
if(NOT APPLE AND NOT REALM_HAVE_OPENSSL)
    add_library(sha2 OBJECT ../external/sha-2/sha224.cpp ../external/sha-2/sha256.cpp)
    target_include_directories(Storage PRIVATE ../external/sha-2)
    target_sources(Storage PRIVATE $<TARGET_OBJECTS:sha2>)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "^Windows")
    if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
        target_link_libraries(Storage INTERFACE Version.lib psapi.lib)
    endif()
elseif(ANDROID)
    target_link_libraries(Storage INTERFACE android log atomic)
    # Because on Android we compile with `-ffunction-sections` and `-fdata-sections` (see the root CMakeLists.txt)
    # it makes sense to export this linker flag as well
    target_link_options(Storage INTERFACE LINKER:-gc-sections)
elseif(APPLE)
    target_link_options(Storage INTERFACE "SHELL:-framework Foundation")
elseif(LINUX)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "^armv7")
        target_link_libraries(Storage INTERFACE atomic)
    endif()
endif()

target_link_libraries(Storage INTERFACE Threads::Threads)

if(REALM_HAVE_BACKTRACE AND NOT CMAKE_GENERATOR STREQUAL Xcode)
    # Apple platforms always have backtrace. We disregard the `Backtrace_*` variables
    # because their paths are hardcoded to one SDK within Xcode (e.g. macosx),
    # whereas we build for several different SDKs and thus we can't use the include path from one in the other.
    # Otherwise if CMake found that the backtrace facility is provided by an external library and not built-in
    # we need to configure the interface target with the library include and link path.
    target_include_directories(Storage PRIVATE ${Backtrace_INCLUDE_DIRS})
    target_link_libraries(Storage PUBLIC ${Backtrace_LIBRARIES})
endif()

if(REALM_HAVE_OPENSSL AND (UNIX OR WIN32))
    target_link_libraries(Storage PUBLIC OpenSSL::Crypto)
endif()

# Use Zlib if the imported target is defined, otherise use -lz on Apple platforms
if(TARGET ZLIB::ZLIB)
    target_link_libraries(Storage PUBLIC ZLIB::ZLIB)
elseif(APPLE)
    target_link_options(Storage PUBLIC "-lz")
elseif(EMSCRIPTEN)
    target_compile_options(Storage PUBLIC "-sUSE_ZLIB=1")
    target_link_options(Storage PUBLIC "-sUSE_ZLIB=1")
else()
    message(FATAL_ERROR "No zlib dependency defined")
endif()

if(APPLE)
    target_link_libraries(Storage PUBLIC "-lcompression")
endif()

install(TARGETS Storage EXPORT realm
        ARCHIVE DESTINATION lib
        COMPONENT devel)

foreach(FILE ${REALM_INSTALL_HEADERS})
    get_filename_component(DIR ${FILE} DIRECTORY)
    INSTALL(FILES ${FILE}
            DESTINATION include/realm/${DIR}
            COMPONENT devel)
endforeach()
install(FILES ${PROJECT_BINARY_DIR}/src/realm/version_numbers.hpp
        DESTINATION include/realm
        COMPONENT devel)
install(FILES ${PROJECT_BINARY_DIR}/src/realm/util/config.h
        DESTINATION include/realm/util
        COMPONENT devel)

add_subdirectory(parser)
add_subdirectory(object-store)
if (REALM_ENABLE_SYNC)
    add_subdirectory(sync)
endif()
if(NOT REALM_BUILD_LIB_ONLY AND NOT WINDOWS_STORE)
    add_subdirectory(exec)
    set_macos_only(exec)
endif()
