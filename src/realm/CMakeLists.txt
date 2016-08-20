set(REALM_SOURCES
    alloc.cpp
    alloc_slab.cpp
    array.cpp
    array_binary.cpp
    array_blob.cpp
    array_blobs_big.cpp
    array_integer.cpp
    array_string.cpp
    array_string_long.cpp
    bptree.cpp
    column.cpp
    column_backlink.cpp
    column_binary.cpp
    column_link.cpp
    column_link_base.cpp
    column_linklist.cpp
    column_mixed.cpp
    column_string.cpp
    column_string_enum.cpp
    column_table.cpp
    column_timestamp.cpp
    commit_log.cpp
    descriptor.cpp
    disable_sync_to_disk.cpp
    exceptions.cpp
    group.cpp
    group_shared.cpp
    group_writer.cpp
    history.cpp
    impl/continuous_transactions_history.cpp
    impl/output_stream.cpp
    impl/simulated_failure.cpp
    impl/transact_log.cpp
    importer.cpp
    index_string.cpp
    lang_bind_helper.cpp
    link_view.cpp
    query.cpp
    query_engine.cpp
    replication.cpp
    row.cpp
    spec.cpp
    table.cpp
    table_view.cpp
    unicode.cpp
    util/basic_system_errors.cpp
    util/encrypted_file_mapping.cpp
    util/file.cpp
    util/file_mapper.cpp
    util/interprocess_condvar.cpp
    util/interprocess_mutex.cpp
    util/memory_stream.cpp
    util/misc_errors.cpp
    util/string_buffer.cpp
    util/terminate.cpp
    util/thread.cpp
    util/to_string.cpp
    utilities.cpp
    version.cpp
    views.cpp
) # REALM_SOURCES

set(REALM_INSTALL_GENERAL_HEADERS
    ${CMAKE_CURRENT_BINARY_DIR}/version.hpp
    alloc.hpp
    alloc_slab.hpp
    array.hpp
    array_basic.hpp
    array_basic_tpl.hpp
    array_binary.hpp
    array_blob.hpp
    array_blobs_big.hpp
    array_integer.hpp
    array_string.hpp
    array_string_long.hpp
    binary_data.hpp
    bptree.hpp
    column.hpp
    column_backlink.hpp
    column_binary.hpp
    column_fwd.hpp
    column_link.hpp
    column_linkbase.hpp
    column_linklist.hpp
    column_mixed.hpp
    column_mixed_tpl.hpp
    column_string.hpp
    column_string_enum.hpp
    column_table.hpp
    column_timestamp.hpp
    column_tpl.hpp
    column_type.hpp
    column_type_traits.hpp
    commit_log.hpp
    data_type.hpp
    descriptor.hpp
    descriptor_fwd.hpp
    disable_sync_to_disk.hpp
    exceptions.hpp
    group.hpp
    group_shared.hpp
    group_writer.hpp
    handover_defs.hpp
    history.hpp
    history.hpp
    impl/continuous_transactions_history.hpp
    impl/input_stream.hpp
    impl/transact_log.hpp
    importer.hpp
    index_string.hpp
    lang_bind_helper.hpp
    link_view.hpp
    link_view_fwd.hpp
    mixed.hpp
    null.hpp
    olddatetime.hpp
    owned_data.hpp
    query.hpp
    query_conditions.hpp
    query_engine.hpp
    query_expression.hpp
    realm_nmmintrin.h
    replication.hpp
    row.hpp
    spec.hpp
    string_data.hpp
    table.hpp
    table_accessors.hpp
    table_basic.hpp
    table_basic_fwd.hpp
    table_macros.hpp
    table_ref.hpp
    table_view.hpp
    table_view_basic.hpp
    timestamp.hpp
    unicode.hpp
    utilities.hpp
    views.hpp
) # REALM_INSTALL_GENERAL_HEADERS

set(REALM_INSTALL_UTIL_HEADERS
    util/assert.hpp
    util/basic_system_errors.hpp
    util/bind_ptr.hpp
    util/buffer.hpp
    util/call_with_tuple.hpp
    util/cf_ptr.hpp
    util/encrypted_file_mapping.hpp
    util/features.h
    util/file.hpp
    util/file_mapper.hpp
    util/hex_dump.hpp
    util/inspect.hpp
    util/interprocess_condvar.hpp
    util/interprocess_mutex.hpp
    util/logger.hpp
    util/memory_stream.hpp
    util/meta.hpp
    util/misc_errors.hpp
    util/miscellaneous.hpp
    util/optional.hpp
    util/priority_queue.hpp
    util/safe_int_ops.hpp
    util/scope_exit.hpp
    util/shared_ptr.hpp
    util/string_buffer.hpp
    util/terminate.hpp
    util/thread.hpp
    util/to_string.hpp
    util/tuple.hpp
    util/type_list.hpp
    util/type_traits.hpp
    util/utf8.hpp
) # REALM_INSTALL_UTIL_HEADERS

set(REALM_OTHER_UTIL_HEADERS
    util/aes_cryptor.hpp
    util/errno.hpp
) # REALM_OTHER_UTIL_HEADERS

# An object library to group together the compilation of the source files.
add_library(realm-objects OBJECT
    ${REALM_SOURCES} ${REALM_INSTALL_GENERAL_HEADERS}
    ${REALM_INSTALL_UTIL_HEADERS} ${REALM_OTHER_UTIL_HEADERS}
)

set_target_properties(realm-objects PROPERTIES
    POSITION_INDEPENDENT_CODE true
)

add_library(realm SHARED $<TARGET_OBJECTS:realm-objects> placeholder.cpp)
add_library(realm-static STATIC $<TARGET_OBJECTS:realm-objects> placeholder.cpp)

set_target_properties(
    realm PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${REALM_SOVERSION}
)

set_target_properties(
    realm-static PROPERTIES
    VERSION ${PROJECT_VERSION}
    OUTPUT_NAME realm
    # The static library may get included in a dynamic library
    # (the bindings), so build as position independent code.
    POSITION_INDEPENDENT_CODE true
)

target_link_libraries(
    realm
    ${REALM_EXTRA_LIBS}
    Threads::Threads
)

if(REALM_ENABLE_ENCRYPTION AND NOT APPLE)
    target_link_libraries(realm ${OPENSSL_LIBRARIES})
endif()

if(APPLE)
    target_link_libraries(realm ${Foundation})
endif(APPLE)

install(TARGETS realm
        DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(TARGETS realm-static
        DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES ${REALM_INSTALL_GENERAL_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm)
install(FILES ${REALM_INSTALL_UTIL_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/util)

add_executable(realm-config config_tool.cpp)
add_executable(realm-importer importer_tool.cpp)
target_link_libraries(realm-importer realm)

install(TARGETS realm-config realm-importer
        DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(realmd realmd.cpp)
target_link_libraries(realmd realm)
install(TARGETS realmd
        DESTINATION ${CMAKE_INSTALL_LIBEXECDIR})
