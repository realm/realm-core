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
    index_string.cpp
    lang_bind_helper.cpp
    link_view.cpp
    query.cpp
    query_engine.cpp
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
    util/interprocess_mutex.cpp
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
) # REALM_SOURCES

set(REALM_INSTALL_GENERAL_HEADERS
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
    group_writer.hpp
    handover_defs.hpp
    history.hpp
    impl/continuous_transactions_history.hpp
    impl/input_stream.hpp
    impl/transact_log.hpp
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
    table_ref.hpp
    table_view.hpp
    timestamp.hpp
    unicode.hpp
    utilities.hpp
    version_id.hpp
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
    util/type_list.hpp
    util/type_traits.hpp
    util/utf8.hpp
) # REALM_INSTALL_UTIL_HEADERS

set(REALM_OTHER_UTIL_HEADERS
    util/aes_cryptor.hpp
    util/errno.hpp
) # REALM_OTHER_UTIL_HEADERS

add_library(realm-objects OBJECT ${REALM_SOURCES})
set_target_properties(realm-objects PROPERTIES POSITION_INDEPENDENT_CODE true)
target_compile_definitions(realm-objects PUBLIC PIC "$<$<CXX_COMPILER_ID:MSVC>:_CRT_SECURE_NO_WARNINGS>")

if(COMMAND set_target_xcode_attributes)
    set_target_xcode_attributes(realm-objects)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "^Windows")
    add_dependencies(realm-objects pthread_win32 getopt_win32)
    set_property(TARGET realm-objects APPEND PROPERTY COMPILE_DEFINITIONS PTW32_STATIC_LIB)
    include_directories(realm-objects INTERFACE
                        ${CMAKE_SOURCE_DIR}/src/win32
                        ${CMAKE_SOURCE_DIR}/src/win32/pthread)
    list(APPEND REALM_EXTRA_LIBS pthread_win32 getopt_win32)
elseif(ANDROID)
    list(APPEND REALM_EXTRA_LIBS atomic)
endif()

# The placeholder file is needed in order to make builds work on Xcode
# The issue is described in the last sentence here:
# https://cmake.org/cmake/help/v3.7/command/add_library.html#object-libraries
add_library(realm STATIC $<TARGET_OBJECTS:realm-objects> placeholder.cpp)

target_link_libraries(
        realm
        ${REALM_EXTRA_LIBS}
        ${CMAKE_THREAD_LIBS_INIT}
)

if(ANDROID OR CMAKE_SYSTEM_NAME MATCHES "^Windows")
    set(REALM_SKIP_SHARED_LIB ON)
endif()

if(NOT REALM_SKIP_SHARED_LIB)
    add_library(realm-shared SHARED $<TARGET_OBJECTS:realm-objects> placeholder.cpp)

    set_target_properties(
            realm-shared PROPERTIES
            LIBRARY_OUTPUT_NAME realm
    )

    target_link_libraries(
            realm-shared
            ${REALM_EXTRA_LIBS}
            ${CMAKE_THREAD_LIBS_INIT}
    )
endif()

if(REALM_ENABLE_ENCRYPTION AND NOT APPLE)
    if(ANDROID)
        string(TOLOWER "${CMAKE_BUILD_TYPE}" BUILD_TYPE)
        set(OPENSSL_FILENAME "openssl-${BUILD_TYPE}-${ANDROID_OPENSSL_VERSION}-Android-${ANDROID_ABI}")
        set(OPENSSL_URL "http://static.realm.io/downloads/openssl/${ANDROID_OPENSSL_VERSION}/Android/${ANDROID_ABI}/${OPENSSL_FILENAME}.tar.gz")

        message(STATUS "Downloading OpenSSL...")
        file(DOWNLOAD "${OPENSSL_URL}" "${CMAKE_BINARY_DIR}/${OPENSSL_FILENAME}.tar.gz")

        message(STATUS "Uncompressing OpenSSL...")
        execute_process(COMMAND ${CMAKE_COMMAND} -E tar xfz "${OPENSSL_FILENAME}.tar.gz")

        message(STATUS "Importing OpenSSL...")
        include(${CMAKE_BINARY_DIR}/${OPENSSL_FILENAME}/openssl.cmake)
        get_target_property(OPENSSL_INCLUDE_DIR crypto INTERFACE_INCLUDE_DIRECTORIES)
        get_target_property(CRYPTO_LIB crypto IMPORTED_LOCATION)
        get_target_property(SSL_LIB ssl IMPORTED_LOCATION)

        target_include_directories(realm-objects PUBLIC ${OPENSSL_INCLUDE_DIR})
        target_link_libraries(realm crypto)
    else()
        include(FindOpenSSL)
        target_link_libraries(realm ${OPENSSL_LIBRARIES})
        if(NOT REALM_SKIP_SHARED_LIB)
            target_link_libraries(realm-shared ${OPENSSL_LIBRARIES})
        endif()
    endif()
endif()

if(APPLE)
    target_link_libraries(realm ${Foundation})
    if(NOT REALM_SKIP_SHARED_LIB)
        target_link_libraries(realm-shared ${Foundation})
    endif()
endif(APPLE)

install(TARGETS realm
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT devel)

install(FILES ${REALM_INSTALL_GENERAL_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm
        COMPONENT devel)
install(FILES ${REALM_INSTALL_UTIL_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/util
        COMPONENT devel)
install(FILES ${PROJECT_BINARY_DIR}/src/realm/version.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm
        COMPONENT devel)
install(FILES ${PROJECT_BINARY_DIR}/src/realm/util/config.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/util
        COMPONENT devel)

if(NOT REALM_SKIP_SHARED_LIB)
    install(TARGETS realm-shared
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT runtime
            RUNTIME DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT runtime)
endif()

if(ANDROID)
    install(FILES ${CRYPTO_LIB} ${SSL_LIB}
            DESTINATION ${CMAKE_INSTALL_LIBDIR}
            COMPONENT devel)
    install(DIRECTORY ${OPENSSL_INCLUDE_DIR}/openssl
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
            COMPONENT devel)
endif()

if(NOT APPLE AND NOT ANDROID AND NOT REALM_BUILD_LIB_ONLY)
    add_executable(realm-config config_tool.cpp)
    add_executable(realm-importer importer_tool.cpp importer.cpp importer.hpp)
    target_link_libraries(realm-importer realm)

    install(TARGETS realm-config realm-importer
            COMPONENT runtime
            DESTINATION ${CMAKE_INSTALL_BINDIR})

    add_executable(realmd realmd.cpp)
    target_link_libraries(realmd realm)
    install(TARGETS realmd
            COMPONENT runtime
            DESTINATION ${CMAKE_INSTALL_LIBEXECDIR})
endif()
