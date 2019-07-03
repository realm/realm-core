set(REALM_SOURCES
    alloc.cpp
    alloc_slab.cpp
    array.cpp
    array_backlink.cpp
    array_binary.cpp
    array_blob.cpp
    array_blobs_small.cpp
    array_blobs_big.cpp
    array_integer.cpp
    array_unsigned.cpp
    array_string.cpp
    array_string_short.cpp
    array_timestamp.cpp
    bplustree.cpp
    cluster.cpp
    column_binary.cpp
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
    obj.cpp
    object_id.cpp
    query.cpp
    query_engine.cpp
    query_expression.cpp
    replication.cpp
    spec.cpp
    string_data.cpp
    table.cpp
    table_ref.cpp
    obj_list.cpp
    table_view.cpp
    sort_descriptor.cpp
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
    util/sha_crypto.cpp
    util/terminate.cpp
    util/thread.cpp
    util/to_string.cpp
    utilities.cpp
    version.cpp
) # REALM_SOURCES

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
    array_direct.hpp
    array_integer.hpp
    array_key.hpp
    array_list.hpp
    array_string.hpp
    array_string_short.hpp
    array_timestamp.hpp
    array_unsigned.hpp
    binary_data.hpp
    bplustree.hpp
    cluster.hpp
    cluster_tree.hpp
    column_binary.hpp
    column_integer.hpp
    column_fwd.hpp
    column_type.hpp
    column_type_traits.hpp
    data_type.hpp
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
    object_id.hpp
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
    table_view.hpp
    timestamp.hpp
    unicode.hpp
    utilities.hpp
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
    util/call_with_tuple.hpp
    util/fixed_size_buffer.hpp
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
    try_compile(USES_OLD_ABI ${RealmCore_BINARY_DIR}/abitest ${RealmCore_SOURCE_DIR}/tools/cmake/abitest abitest)
    if(USES_OLD_ABI)
        message(WARNING "You are compiling on a system that uses a non-compliant C++11 ABI. Downstream components depending on Realm::Core will be built with _GLIBCXX_USE_CXX11_ABI=0.")
        target_compile_definitions(Core INTERFACE "_GLIBCXX_USE_CXX11_ABI=0")
    endif()
endif()

target_include_directories(Core
    INTERFACE
        $<BUILD_INTERFACE:${RealmCore_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${RealmCore_BINARY_DIR}/src>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

target_link_libraries(Core INTERFACE ${REALM_EXTRA_LIBS} ${CMAKE_THREAD_LIBS_INIT})

if(ANDROID OR CMAKE_SYSTEM_NAME MATCHES "^Windows")
    set(REALM_SKIP_SHARED_LIB ON)
endif()

if(NOT REALM_SKIP_SHARED_LIB)
    add_library(CoreShared SHARED ${REALM_OBJECT_FILES} ${REALM_XCODE_DEPENDENCY})
    add_library(realm-shared ALIAS CoreShared)

    target_include_directories(CoreShared
        INTERFACE
            $<BUILD_INTERFACE:${RealmCore_SOURCE_DIR}/src>
            $<BUILD_INTERFACE:${RealmCore_BINARY_DIR}/src>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

    set_target_properties(
            CoreShared PROPERTIES
            OUTPUT_NAME "realm"
    )

    target_link_libraries(CoreShared PRIVATE ${REALM_EXTRA_LIBS} ${CMAKE_THREAD_LIBS_INIT}
    )
endif()

if(UNIX AND NOT APPLE)
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
install(FILES ${PROJECT_BINARY_DIR}/src/realm/version_numbers.hpp
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

add_subdirectory(parser)
add_subdirectory(exec)
