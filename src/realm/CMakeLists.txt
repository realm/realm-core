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
    impl/output_stream.cpp
    impl/simulated_failure.cpp
    impl/transact_log.cpp
    index_string.cpp
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
    util/allocator.cpp
    util/backtrace.cpp
    util/base64.cpp
    util/basic_system_errors.cpp
    util/encrypted_file_mapping.cpp
    util/file.cpp
    util/file_mapper.cpp
    util/interprocess_condvar.cpp
    util/logger.cpp
    util/memory_stream.cpp
    util/misc_errors.cpp
    util/serializer.cpp
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
    group_shared_options.hpp
    group_writer.hpp
    handover_defs.hpp
    history.hpp
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
    utilities.hpp
    version_id.hpp
    views.hpp
) # REALM_INSTALL_GENERAL_HEADERS

set(REALM_PARSER_SOURCES
    parser/expression_container.cpp
    parser/keypath_mapping.cpp
    parser/parser.cpp
    parser/parser_utils.cpp
    parser/property_expression.cpp
    parser/query_builder.cpp
    parser/subquery_expression.cpp
    parser/value_expression.cpp
) # REALM_PARSER_SOURCES

set(REALM_PARSER_HEADERS
    parser/collection_operator_expression.hpp
    parser/expression_container.hpp
    parser/keypath_mapping.hpp
    parser/parser.hpp
    parser/parser_utils.hpp
    parser/property_expression.hpp
    parser/query_builder.hpp
    parser/subquery_expression.hpp
    parser/value_expression.hpp
) # REALM_PARSER_HEADERS

set(REALM_INSTALL_IMPL_HEADERS
    impl/array_writer.hpp
    impl/cont_transact_hist.hpp
    impl/destroy_guard.hpp
    impl/input_stream.hpp
    impl/output_stream.hpp
    impl/sequential_getter.hpp
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

if(REALM_METRICS)
    list(APPEND REALM_SOURCES
        metrics/metrics.cpp
        metrics/metric_timer.cpp
        metrics/query_info.cpp
        metrics/transaction_info.cpp)
endif()

if(NOT MSVC)
    list(APPEND REALM_SOURCES util/interprocess_mutex.cpp)
endif()

# We add the headers to the library only so they show up in dev
# environments. It won't actually affect compilation process.
add_library(CoreObjects OBJECT ${REALM_SOURCES} ${REALM_INSTALL_ALL_HEADERS})
set_target_properties(CoreObjects PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)
target_compile_definitions(CoreObjects PRIVATE
  "$<$<CXX_COMPILER_ID:MSVC>:_CRT_SECURE_NO_WARNINGS>"
  "$<$<CXX_COMPILER_ID:MSVC>:_SCL_SECURE_NO_WARNINGS>"
)

set(REALM_OBJECT_FILES $<TARGET_OBJECTS:CoreObjects>)

if(COMMAND set_target_xcode_attributes)
    set_target_xcode_attributes(CoreObjects)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "^Windows")
    add_dependencies(CoreObjects sha_win32 getopt_win32)
    list(APPEND REALM_OBJECT_FILES $<TARGET_OBJECTS:sha_win32>)
elseif(ANDROID)
    list(APPEND REALM_EXTRA_LIBS atomic)
endif()

set(REALM_XCODE_DEPENDENCY "")
if(CMAKE_GENERATOR STREQUAL Xcode)
    # The placeholder file is needed in order to make builds work on Xcode
    # The issue is described in the last sentence here:
    # https://cmake.org/cmake/help/v3.7/command/add_library.html#object-libraries
    # Another problem is that Xcode does not see changes in the CoreObjects
    # target, to solve this we trigger rebuiliding the library by making
    # placeholder.cpp depend on all the sources rather than CoreObjects.
    set(REALM_XCODE_DEPENDENCY "placeholder.cpp")
    add_custom_command(
        OUTPUT ${REALM_XCODE_DEPENDENCY}
        DEPENDS ${REALM_SOURCES} ${REALM_INSTALL_ALL_HEADERS}
        COMMAND echo \"extern int xcode_unused\; //This file is generated by CMake for Xcode builds\" > ${REALM_XCODE_DEPENDENCY}
    )
endif()


add_library(Core STATIC ${REALM_OBJECT_FILES} ${REALM_XCODE_DEPENDENCY})
set_target_properties(Core PROPERTIES OUTPUT_NAME "realm")
add_library(realm ALIAS Core)

target_compile_definitions(Core PUBLIC
  "$<$<CXX_COMPILER_ID:MSVC>:_CRT_SECURE_NO_WARNINGS>"
  "$<$<CXX_COMPILER_ID:MSVC>:_SCL_SECURE_NO_WARNINGS>"
)

# GCC<5.1 does not adhere to the C++11 ABI for std::string but some systems
# still produce non-compliant C++11 ABI even with newer version of GCC (Centos 6 for example).
# This performs a linking test and communicates to dependencies built with newer compilers 
# that the old ABI should be used.
if(UNIX AND NOT APPLE AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    try_compile(USES_OLD_ABI ${CMAKE_BINARY_DIR}/abitest ${CMAKE_CURRENT_LIST_DIR}/../../tools/cmake/abitest abitest)
    if(USES_OLD_ABI)
        message(WARNING "You are compiling on a system that uses a non-compliant C++11 ABI. Downstream components depending on Realm::Core will be built with _GLIBCXX_USE_CXX11_ABI=0.")
        target_compile_definitions(Core INTERFACE "_GLIBCXX_USE_CXX11_ABI=0")
    endif()
endif()

target_include_directories(Core INTERFACE
                           $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src> $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/src>
                           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

target_link_libraries(Core INTERFACE ${REALM_EXTRA_LIBS} ${CMAKE_THREAD_LIBS_INIT})

if(ANDROID OR CMAKE_SYSTEM_NAME MATCHES "^Windows")
    set(REALM_SKIP_SHARED_LIB ON)
endif()

if(NOT REALM_SKIP_SHARED_LIB)
    add_library(CoreShared SHARED ${REALM_OBJECT_FILES} ${REALM_XCODE_DEPENDENCY})
    add_library(realm-shared ALIAS CoreShared)

    target_include_directories(CoreShared INTERFACE
                               $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src> $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/src>
                               $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

    set_target_properties(
            CoreShared PROPERTIES
            OUTPUT_NAME "realm"
    )

    target_link_libraries(CoreShared PRIVATE ${REALM_EXTRA_LIBS} ${CMAKE_THREAD_LIBS_INIT}
    )
endif()

if(REALM_ENABLE_ENCRYPTION AND UNIX AND NOT APPLE)
    if(ANDROID)
        string(TOLOWER "${CMAKE_BUILD_TYPE}" BUILD_TYPE)
        set(OPENSSL_FILENAME "openssl-${BUILD_TYPE}-${ANDROID_OPENSSL_VERSION}-${ANDROID_OPENSSL_BUILD_NUMBER}-Android-${ANDROID_ABI}")
        set(OPENSSL_URL "http://static.realm.io/downloads/openssl/${ANDROID_OPENSSL_VERSION}/Android/${ANDROID_ABI}/${OPENSSL_FILENAME}.tar.gz")

        message(STATUS "Downloading OpenSSL...")
        file(DOWNLOAD "${OPENSSL_URL}" "${CMAKE_BINARY_DIR}/${OPENSSL_FILENAME}.tar.gz")

        message(STATUS "Uncompressing OpenSSL...")
        execute_process(COMMAND ${CMAKE_COMMAND} -E tar xfz "${OPENSSL_FILENAME}.tar.gz")

        set(OPENSSL_ROOT_DIR "${CMAKE_BINARY_DIR}/${OPENSSL_FILENAME}")
        set(OPENSSL_INCLUDE_DIR "${OPENSSL_ROOT_DIR}/include")
        set(OPENSSL_CRYPTO_LIBRARY "${OPENSSL_ROOT_DIR}/lib/libcrypto.a")
        install(
            DIRECTORY ${OPENSSL_ROOT_DIR}
            DESTINATION .
            COMPONENT devel)
    endif()
    find_package(OpenSSL REQUIRED)
    target_include_directories(CoreObjects PUBLIC ${OPENSSL_INCLUDE_DIR})
    target_link_libraries(Core INTERFACE OpenSSL::Crypto)
    if(NOT REALM_SKIP_SHARED_LIB)
        target_link_libraries(CoreShared PRIVATE OpenSSL::Crypto)
    endif()
endif()

if(APPLE)
    target_link_libraries(Core INTERFACE "-framework Foundation")
    if(NOT REALM_SKIP_SHARED_LIB)
        target_link_libraries(CoreShared PRIVATE ${Foundation})
    endif()
endif(APPLE)

install(TARGETS Core EXPORT realm
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT devel)

# include .pdb files with debug symbols on Windows
if(CMAKE_GENERATOR MATCHES "^Visual Studio")
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/CoreObjects.dir/$<CONFIG>/CoreObjects.pdb
            DESTINATION ${CMAKE_INSTALL_LIBDIR}
            COMPONENT devel
            OPTIONAL)
endif()

install(FILES ${REALM_INSTALL_GENERAL_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm
        COMPONENT devel)
install(FILES ${REALM_INSTALL_IMPL_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/impl
        COMPONENT devel)
install(FILES ${REALM_INSTALL_UTIL_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/util
        COMPONENT devel)
install(FILES ${REALM_OTHER_UTIL_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/util
        COMPONENT devel)
install(FILES ${REALM_METRICS_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/metrics
        COMPONENT devel)
install(FILES ${PROJECT_BINARY_DIR}/src/realm/version.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm
        COMPONENT devel)
install(FILES ${PROJECT_BINARY_DIR}/src/realm/util/config.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/util
        COMPONENT devel)

if(NOT REALM_SKIP_SHARED_LIB)
    install(TARGETS CoreShared
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

add_library(QueryParser STATIC ${REALM_PARSER_SOURCES} ${REALM_PARSER_HEADERS})
add_library(realm-parser ALIAS QueryParser)
target_link_libraries(QueryParser PUBLIC Core)
target_include_directories(QueryParser PRIVATE ${PEGTL_INCLUDE_DIR})
set_target_properties(QueryParser PROPERTIES OUTPUT_NAME "realm-parser")
install(FILES ${REALM_PARSER_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/parser
        COMPONENT devel)
install(TARGETS QueryParser EXPORT realm
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT devel)
if(NOT REALM_SKIP_SHARED_LIB)
    add_library(QueryParserShared SHARED ${REALM_PARSER_SOURCES} ${REALM_PARSER_HEADERS})
    add_dependencies(QueryParserShared CoreShared)
    target_link_libraries(QueryParserShared PRIVATE CoreShared)
    target_include_directories(QueryParserShared PRIVATE ${PEGTL_INCLUDE_DIR})
    target_include_directories(QueryParserShared INTERFACE
                               $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src> $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/src>
                               $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    set_target_properties(QueryParserShared PROPERTIES
                          OUTPUT_NAME "realm-parser"
    )
    install(TARGETS QueryParserShared
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT runtime
            RUNTIME DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT runtime)
endif()

if(NOT APPLE AND NOT ANDROID AND NOT CMAKE_SYSTEM_NAME MATCHES "^Windows" AND NOT REALM_BUILD_LIB_ONLY)
    add_executable(RealmConfig config_tool.cpp)
    set_target_properties(RealmConfig PROPERTIES
        OUTPUT_NAME "realm-config"
        DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})

    add_executable(RealmImporter importer_tool.cpp importer.cpp importer.hpp)
    set_target_properties(RealmImporter PROPERTIES
        OUTPUT_NAME "realm-importer"
        DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})
    target_link_libraries(RealmImporter realm)

    add_executable(RealmTrawler realm_trawler.cpp)
    set_target_properties(RealmTrawler PROPERTIES
        OUTPUT_NAME "realm-trawler"
        DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})

    install(TARGETS RealmConfig RealmImporter
            COMPONENT runtime
            DESTINATION ${CMAKE_INSTALL_BINDIR})

    add_executable(realmd realmd.cpp)
    set_target_properties(realmd PROPERTIES DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})
    target_link_libraries(realmd Core)
    install(TARGETS realmd
            COMPONENT runtime
            DESTINATION ${CMAKE_INSTALL_LIBEXECDIR})
endif()
