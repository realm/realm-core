set(REALM_SOURCES
    util/encrypted_file_mapping.cpp
    util/misc_errors.cpp
    util/basic_system_errors.cpp
    util/file.cpp
    util/file_mapper.cpp
    util/memory_stream.cpp
    util/string_buffer.cpp
    util/terminate.cpp
    util/thread.cpp
    util/network.cpp
    util/event_loop.cpp
    util/uri.cpp
    util/interprocess_condvar.cpp
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
    descriptor.cpp
    exceptions.cpp
    group.cpp
    group_shared.cpp
    group_writer.cpp
    impl/output_stream.cpp
    impl/transact_log.cpp
    impl/simulated_failure.cpp
    impl/continuous_transactions_history.cpp
    importer.cpp
    index_string.cpp
    lang_bind_helper.cpp
    link_view.cpp
    query.cpp
    query_engine.cpp
    row.cpp
    spec.cpp
    table.cpp
    table_view.cpp
    unicode.cpp
    utilities.cpp
    version.cpp
    views.cpp
    replication.cpp
    history.cpp
    commit_log.cpp
    disable_sync_to_disk.cpp
) # REALM_SOURCES

set(REALM_INSTALL_GENERAL_HEADERS
    exceptions.hpp
    utilities.hpp
    alloc.hpp
    alloc_slab.hpp
    array.hpp
    array_integer.hpp
    array_string.hpp
    bptree.hpp
    data_type.hpp
    column_type.hpp
    column_fwd.hpp
    spec.hpp
    datetime.hpp
    string_data.hpp
    impl/input_stream.hpp
    impl/transact_log.hpp
    impl/continuous_transactions_history.hpp
    null.hpp
    binary_data.hpp
    mixed.hpp
    owned_data.hpp
    table.hpp
    table_ref.hpp
    table_basic_fwd.hpp
    table_accessors.hpp
    table_basic.hpp
    table_view.hpp
    table_view_basic.hpp
    table_macros.hpp
    row.hpp
    descriptor_fwd.hpp
    descriptor.hpp
    group.hpp
    group_shared.hpp
    history.hpp
    handover_defs.hpp
    replication.hpp
    history.hpp
    query.hpp
    query_conditions.hpp
    lang_bind_helper.hpp
    realm_nmmintrin.h
    importer.hpp
    ${CMAKE_CURRENT_BINARY_DIR}/version.hpp
    unicode.hpp
    commit_log.hpp
    link_view_fwd.hpp
    link_view.hpp
    views.hpp
    disable_sync_to_disk.hpp
    array_basic.hpp
    array_basic_tpl.hpp
    array_binary.hpp
    array_blob.hpp
    array_blobs_big.hpp
    array_string_long.hpp
    column.hpp
    column_tpl.hpp
    column_binary.hpp
    column_string_enum.hpp
    column_string.hpp
    column_table.hpp
    column_link.hpp
    column_linklist.hpp
    column_linkbase.hpp
    column_backlink.hpp
    column_mixed.hpp
    column_mixed_tpl.hpp
    column_type_traits.hpp
    group_writer.hpp
    index_string.hpp
    query_engine.hpp
    query_expression.hpp
) # REALM_INSTALL_GENERAL_HEADERS

set(REALM_INSTALL_UTIL_HEADERS
    util/features.h
    util/meta.hpp
    util/inspect.hpp
    util/assert.hpp
    util/terminate.hpp
    util/type_list.hpp
    util/tuple.hpp
    util/type_traits.hpp
    util/safe_int_ops.hpp
    util/bind_ptr.hpp
    util/buffer.hpp
    util/string_buffer.hpp
    util/shared_ptr.hpp
    util/memory_stream.hpp
    util/logger.hpp
    util/misc_errors.hpp
    util/basic_system_errors.hpp
    util/thread.hpp
    util/file.hpp
    util/optional.hpp
    util/network.hpp
    util/event_loop.hpp
    util/uri.hpp
    util/utf8.hpp
    util/priority_queue.hpp
    util/interprocess_condvar.hpp
    util/interprocess_mutex.hpp
    util/file_mapper.hpp
    util/encrypted_file_mapping.hpp
    util/miscellaneous.hpp
    util/to_string.hpp
) # REALM_INSTALL_UTIL_HEADERS

add_library(realm ${REALM_SOURCES})

set_target_properties(
    realm PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${REALM_SOVERSION}
)

target_link_libraries(
    realm
    ${REALM_EXTRA_LIBS}
    Threads::Threads
)

if(REALM_ENABLE_ENCRYPTION)
    target_link_libraries(realm ${OPENSSL_LIBRARIES})
endif(REALM_ENABLE_ENCRYPTION)

install(TARGETS realm
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
