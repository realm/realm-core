add_library(Core
    alloc.cpp
    alloc.hpp
    alloc_slab.cpp
    alloc_slab.hpp
    array.cpp
    array.hpp
    array_basic.hpp
    array_basic_tpl.hpp
    array_binary.cpp
    array_binary.hpp
    array_blob.cpp
    array_blob.hpp
    array_blobs_big.cpp
    array_blobs_big.hpp
    array_direct.hpp
    array_integer.cpp
    array_integer.hpp
    array_string.cpp
    array_string.hpp
    array_string_long.cpp
    array_string_long.hpp
    binary_data.hpp
    bptree.cpp
    bptree.hpp
    column.cpp
    column.hpp
    column_backlink.cpp
    column_backlink.hpp
    column_binary.cpp
    column_binary.hpp
    column_fwd.hpp
    column_link.cpp
    column_link.hpp
    column_link_base.cpp
    column_linkbase.hpp
    column_linklist.cpp
    column_linklist.hpp
    column_mixed.cpp
    column_mixed.hpp
    column_mixed_tpl.hpp
    column_string.cpp
    column_string.hpp
    column_string_enum.cpp
    column_string_enum.hpp
    column_table.cpp
    column_table.hpp
    column_timestamp.cpp
    column_timestamp.hpp
    column_tpl.hpp
    column_type.hpp
    column_type_traits.hpp
    data_type.hpp
    descriptor.cpp
    descriptor.hpp
    descriptor_fwd.hpp
    disable_sync_to_disk.cpp
    disable_sync_to_disk.hpp
    exceptions.cpp
    exceptions.hpp
    group.cpp
    group.hpp
    group_shared.cpp
    group_shared.hpp
    group_shared_options.hpp
    group_writer.cpp
    group_writer.hpp
    handover_defs.hpp
    history.cpp
    history.hpp
    impl/array_writer.hpp
    impl/cont_transact_hist.hpp
    impl/destroy_guard.hpp
    impl/input_stream.hpp
    impl/output_stream.cpp
    impl/output_stream.hpp
    impl/sequential_getter.hpp
    impl/simulated_failure.cpp
    impl/simulated_failure.hpp
    impl/transact_log.cpp
    impl/transact_log.hpp
    index_string.cpp
    index_string.hpp
    lang_bind_helper.cpp
    lang_bind_helper.hpp
    link_view.cpp
    link_view.hpp
    link_view_fwd.hpp
    $<$<BOOL:${REALM_METRICS}>:metrics/metric_timer.cpp>
    $<$<BOOL:${REALM_METRICS}>:metrics/metric_timer.hpp>
    $<$<BOOL:${REALM_METRICS}>:metrics/metrics.cpp>
    $<$<BOOL:${REALM_METRICS}>:metrics/metrics.hpp>
    $<$<BOOL:${REALM_METRICS}>:metrics/query_info.cpp>
    $<$<BOOL:${REALM_METRICS}>:metrics/query_info.hpp>
    $<$<BOOL:${REALM_METRICS}>:metrics/transaction_info.cpp>
    $<$<BOOL:${REALM_METRICS}>:metrics/transaction_info.hpp>
    mixed.hpp
    null.hpp
    olddatetime.hpp
    owned_data.hpp
    query.cpp
    query.hpp
    query_conditions.hpp
    query_engine.cpp
    query_engine.hpp
    query_expression.cpp
    query_expression.hpp
    query_operators.hpp
    realm_nmmintrin.h
    replication.cpp
    replication.hpp
    row.cpp
    row.hpp
    spec.cpp
    spec.hpp
    string_data.cpp
    string_data.hpp
    table.cpp
    table.hpp
    table_ref.hpp
    table_view.cpp
    table_view.hpp
    timestamp.hpp
    unicode.cpp
    unicode.hpp
    util/aes_cryptor.hpp
    util/assert.hpp
    util/basic_system_errors.cpp
    util/basic_system_errors.hpp
    util/bind_ptr.hpp
    util/buffer.hpp
    util/call_with_tuple.hpp
    util/cf_ptr.hpp
    util/encrypted_file_mapping.cpp
    util/encrypted_file_mapping.hpp
    util/errno.hpp
    util/features.h
    util/file.cpp
    util/file.hpp
    util/file_mapper.cpp
    util/file_mapper.hpp
    util/hex_dump.hpp
    util/inspect.hpp
    util/interprocess_condvar.cpp
    util/interprocess_condvar.hpp
    $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:util/interprocess_mutex.cpp>
    util/interprocess_mutex.hpp
    util/logger.cpp
    util/logger.hpp
    util/memory_stream.cpp
    util/memory_stream.hpp
    util/misc_errors.cpp
    util/misc_errors.hpp
    util/miscellaneous.hpp
    util/optional.hpp
    util/overload.hpp
    util/priority_queue.hpp
    util/safe_int_ops.hpp
    util/scope_exit.hpp
    util/shared_ptr.hpp
    util/string_buffer.cpp
    util/string_buffer.hpp
    util/terminate.cpp
    util/terminate.hpp
    util/thread.cpp
    util/thread.hpp
    util/to_string.cpp
    util/to_string.hpp
    util/type_list.hpp
    util/type_traits.hpp
    util/utf8.hpp
    utilities.cpp
    utilities.hpp
    version.cpp
    version_id.hpp
    views.cpp
    views.hpp
) # add_library(realm)
add_library(Realm::Core ALIAS Core)

# TODO: Remove when utility scripts are migrated
add_library(realm ALIAS Core)

set_target_properties(Core
    PROPERTIES
        OUTPUT_NAME realm
        POSITION_INDEPENDENT_CODE ON
)

if(ANDROID)
    # TODO: Core APIs should always built for internal usage, but there seem to be issues with cocoa. Enable it only for Android for now.
    set_target_properties(Core PROPERTIES CXX_VISIBILITY_PRESET hidden)
    target_compile_options(Core
        PUBLIC
            -fsigned-char # char are unsigned by default on ARM
            -fdata-sections # give the linker the chance for further space optimization
            -ffunction-sections # give the linker the chance for further space optimization
    )
endif()

if(COMMAND set_target_xcode_attributes)
    set_target_xcode_attributes(Core)
endif()

target_include_directories(Core
    PUBLIC
        $<BUILD_INTERFACE:${RealmCore_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${RealmCore_BINARY_DIR}/src>
        $<INSTALL_INTERFACE:include>
)

target_compile_features(Core PUBLIC cxx_std_14)

# -------
# OpenSSL
# -------

if(REALM_ENABLE_ENCRYPTION AND UNIX AND NOT APPLE)
    if(ANDROID)
        string(TOLOWER "${CMAKE_BUILD_TYPE}" BUILD_TYPE)
        set(OPENSSL_FILENAME "openssl-${BUILD_TYPE}-${ANDROID_OPENSSL_VERSION}-${ANDROID_OPENSSL_BUILD_NUMBER}-Android-${CMAKE_ANDROID_ARCH_ABI}")
        set(OPENSSL_URL "http://static.realm.io/downloads/openssl/${ANDROID_OPENSSL_VERSION}/Android/${CMAKE_ANDROID_ARCH_ABI}/${OPENSSL_FILENAME}.tar.gz")

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
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    INCLUDES DESTINATION include
)

install(
    DIRECTORY ./
    DESTINATION include/realm
    FILES_MATCHING
    PATTERN "*.h"
    PATTERN "*.hpp"
)

install(FILES ${RealmCore_BINARY_DIR}/src/realm/version.hpp DESTINATION include/realm)
install(FILES ${RealmCore_BINARY_DIR}/src/realm/util/config.h DESTINATION include/realm/util)

if(UNIX AND NOT APPLE AND NOT ANDROID)
    add_executable(RealmConfig config_tool.cpp)
    set_target_properties(RealmConfig
        PROPERTIES
            OUTPUT_NAME realm-config
            DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
    )
    target_link_libraries(RealmConfig Core)

    add_executable(RealmImporter importer_tool.cpp importer.cpp importer.hpp)
    set_target_properties(RealmImporter
        PROPERTIES
            OUTPUT_NAME realm-importer
            DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
    )
    target_link_libraries(RealmImporter Core)

    install(TARGETS RealmConfig RealmImporter DESTINATION bin)

    add_executable(RealmTrawler realm_trawler.cpp)
    set_target_properties(RealmTrawler
        PROPERTIES
            OUTPUT_NAME realm-trawler
            DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
    )
    target_link_libraries(RealmTrawler Core)

    add_executable(Realmd realmd.cpp)
    set_target_properties(Realmd
        PROPERTIES
            OUTPUT_NAME realmd
            DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
    )
    target_link_libraries(Realmd Core)
    install(TARGETS Realmd DESTINATION libexec)
endif()
