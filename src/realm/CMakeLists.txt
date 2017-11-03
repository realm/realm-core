add_library(Core
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
    descriptor.cpp
    disable_sync_to_disk.cpp
    exceptions.cpp
    group.cpp
    group_shared.cpp
    group_writer.cpp
    history.cpp
    impl/output_stream.cpp
    impl/simulated_failure.cpp
    impl/transact_log.cpp
    index_string.cpp
    $<$<BOOL:${REALM_METRICS}>:metrics/metrics.cpp>
    $<$<BOOL:${REALM_METRICS}>:metrics/metric_timer.cpp>
    $<$<BOOL:${REALM_METRICS}>:metrics/query_info.cpp>
    $<$<BOOL:${REALM_METRICS}>:metrics/transaction_info.cpp>
    lang_bind_helper.cpp
    link_view.cpp
    query.cpp
    query_engine.cpp
    query_expression.cpp
    replication.cpp
    row.cpp
    spec.cpp
    string_data.cpp
    table.cpp
    table_view.cpp
    unicode.cpp
    util/basic_system_errors.cpp
    util/encrypted_file_mapping.cpp
    util/file.cpp
    util/file_mapper.cpp
    util/interprocess_condvar.cpp
    $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:util/interprocess_mutex.cpp>
    util/logger.cpp
    util/memory_stream.cpp
    util/misc_errors.cpp
    util/string_buffer.cpp
    util/terminate.cpp
    util/thread.cpp
    util/to_string.cpp
    utilities.cpp
    version.cpp
    views.cpp
) # add_library(realm)
add_library(Realm::Core ALIAS Core)

# TODO: Remove when utility scripts are migrated
add_library(realm ALIAS Core)

set_target_properties(Core
    PROPERTIES
        OUTPUT_NAME realm
        POSITION_INDEPENDENT_CODE ON
)

if(COMMAND set_target_xcode_attributes)
    set_target_xcode_attributes(Core)
endif()

target_include_directories(Core
    PUBLIC
        $<BUILD_INTERFACE:${RealmCore_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${RealmCore_BINARY_DIR}/src>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# -------
# OpenSSL
# -------

if(REALM_ENABLE_ENCRYPTION AND UNIX AND NOT APPLE)
    if(ANDROID)
        string(TOLOWER "${CMAKE_BUILD_TYPE}" BUILD_TYPE)
        set(OPENSSL_FILENAME "openssl-${BUILD_TYPE}-${ANDROID_OPENSSL_VERSION}-${ANDROID_OPENSSL_BUILD_NUMBER}-Android-${ANDROID_ABI}")
        set(OPENSSL_URL "http://static.realm.io/downloads/openssl/${ANDROID_OPENSSL_VERSION}/Android/${ANDROID_ABI}/${OPENSSL_FILENAME}.tar.gz")

        message(STATUS "Downloading OpenSSL...")
        file(DOWNLOAD "${OPENSSL_URL}" "${RealmCore_BINARY_DIR}/${OPENSSL_FILENAME}.tar.gz")

        message(STATUS "Uncompressing OpenSSL...")
        execute_process(COMMAND ${CMAKE_COMMAND} -E tar xfz "${OPENSSL_FILENAME}.tar.gz")

        set(CMAKE_FIND_ROOT_PATH "${RealmCore_BINARY_DIR}/${OPENSSL_FILENAME}")
    endif()
    find_package(OpenSSL REQUIRED)
endif()

# --------
# PThreads
# --------

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# ----------
# Foundation
# ----------

if(APPLE)
    find_library(Foundation Foundation)
endif()

target_link_libraries(Core
    PUBLIC
        Threads::Threads
        $<$<CXX_COMPILER_ID:MSVC>:ShaWin32>
        $<$<CXX_COMPILER_ID:MSVC>:GetOptWin32>
        $<$<BOOL:${ANDROID}>:atomic>
        $<$<BOOL:${ANDROID}>:log>
        $<$<BOOL:${ANDROID}>:android>
        "$<$<BOOL:${APPLE}>:-framework Foundation>"
        $<$<BOOL:${OPENSSL_FOUND}>:OpenSSL::Crypto>
)

install(TARGETS Core EXPORT realm
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(
    FILES
        alloc.hpp
        alloc_slab.hpp
        array.hpp
        array_basic.hpp
        array_basic_tpl.hpp
        array_binary.hpp
        array_blob.hpp
        array_blobs_big.hpp
        array_direct.hpp
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
        data_type.hpp
        descriptor.hpp
        descriptor_fwd.hpp
        disable_sync_to_disk.hpp
        exceptions.hpp
        group.hpp
        group_shared.hpp
        group_shared_options.hpp
        group_writer.hpp
        handover_defs.hpp
        history.hpp
        impl/array_writer.hpp
        impl/cont_transact_hist.hpp
        impl/destroy_guard.hpp
        impl/input_stream.hpp
        impl/output_stream.hpp
        impl/sequential_getter.hpp
        impl/simulated_failure.hpp
        impl/transact_log.hpp
        index_string.hpp
        lang_bind_helper.hpp
        link_view.hpp
        link_view_fwd.hpp
        metrics/metric_timer.hpp
        metrics/metrics.hpp
        metrics/query_info.hpp
        metrics/transaction_info.hpp
        mixed.hpp
        null.hpp
        olddatetime.hpp
        owned_data.hpp
        query.hpp
        query_conditions.hpp
        query_engine.hpp
        query_expression.hpp
        query_operators.hpp
        realm_nmmintrin.h
        replication.hpp
        row.hpp
        spec.hpp
        string_data.hpp
        table.hpp
        table_ref.hpp
        table_view.hpp
        timestamp.hpp
        unicode.hpp
        util/aes_cryptor.hpp
        util/assert.hpp
        util/basic_system_errors.hpp
        util/bind_ptr.hpp
        util/buffer.hpp
        util/call_with_tuple.hpp
        util/cf_ptr.hpp
        util/encrypted_file_mapping.hpp
        util/errno.hpp
        util/features.h
        util/file.hpp
        util/file_mapper.hpp
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
        util/scope_exit.hpp
        util/shared_ptr.hpp
        util/string_buffer.hpp
        util/terminate.hpp
        util/thread.hpp
        util/to_string.hpp
        util/type_list.hpp
        util/type_traits.hpp
        util/utf8.hpp
        utilities.hpp
        version_id.hpp
        views.hpp
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm
)
install(
    FILES
        impl/array_writer.hpp
        impl/cont_transact_hist.hpp
        impl/destroy_guard.hpp
        impl/input_stream.hpp
        impl/output_stream.hpp
        impl/sequential_getter.hpp
        impl/simulated_failure.hpp
        impl/transact_log.hpp
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/impl
)
install(
    FILES
        metrics/metric_timer.hpp
        metrics/metrics.hpp
        metrics/query_info.hpp
        metrics/transaction_info.hpp
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/metrics
)
install(
    FILES
        util/aes_cryptor.hpp
        util/assert.hpp
        util/basic_system_errors.hpp
        util/bind_ptr.hpp
        util/buffer.hpp
        util/call_with_tuple.hpp
        util/cf_ptr.hpp
        util/encrypted_file_mapping.hpp
        util/errno.hpp
        util/features.h
        util/file.hpp
        util/file_mapper.hpp
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
        util/scope_exit.hpp
        util/shared_ptr.hpp
        util/string_buffer.hpp
        util/terminate.hpp
        util/thread.hpp
        util/to_string.hpp
        util/type_list.hpp
        util/type_traits.hpp
        util/utf8.hpp
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/util
)
install(FILES ${RealmCore_BINARY_DIR}/src/realm/version.hpp DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm)
install(FILES ${RealmCore_BINARY_DIR}/src/realm/util/config.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/util)

if(UNIX AND NOT APPLE AND NOT ANDROID)
    add_executable(RealmConfig config_tool.cpp)
    # TODO: Remove when utility scripts are migrated
    add_executable(realm-config ALIAS RealmConfig)

    set_target_properties(RealmConfig
        PROPERTIES
            OUTPUT_NAME realm-config
            DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
    )
    target_link_libraries(RealmConfig Core)

    add_executable(RealmImporter importer_tool.cpp importer.cpp)
    # TODO: Remove when utility scripts are migrated
    add_executable(realm-importer ALIAS RealmImporter)
    set_target_properties(RealmImporter
        PROPERTIES
            OUTPUT_NAME realm-importer
            DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
    )
    target_link_libraries(RealmImporter Core)

    install(TARGETS RealmConfig RealmImporter DESTINATION ${CMAKE_INSTALL_BINDIR})

    add_executable(RealmTrawler realm_trawler.cpp)
    # TODO: Remove when utility scripts are migrated
    add_executable(realm-trawler ALIAS RealmTrawler)
    set_target_properties(RealmTrawler
        PROPERTIES
            OUTPUT_NAME realm-trawler
            DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
    )
    target_link_libraries(RealmTrawler Core)

    add_executable(Realmd realmd.cpp)
    # TODO: Remove when utility scripts are migrated
    add_executable(realmd ALIAS Realmd)
    set_target_properties(Realmd
        PROPERTIES
            OUTPUT_NAME realmd
            DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
    )
    target_link_libraries(Realmd Core)
    install(TARGETS Realmd DESTINATION ${CMAKE_INSTALL_LIBEXECDIR})
endif()
