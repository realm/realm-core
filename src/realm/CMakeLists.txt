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
    array_integer.cpp
    array_key.cpp
    array_mixed.cpp
    array_object_id.cpp
    array_unsigned.cpp
    array_string.cpp
    array_string_short.cpp
    array_timestamp.cpp
    bplustree.cpp
    chunked_binary.cpp
    cluster.cpp
    column_binary.cpp
    decimal128.cpp
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
    set.cpp
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
    array_integer.hpp
    array_mixed.hpp
    array_key.hpp
    array_list.hpp
    array_object_id.hpp
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
    column_binary.hpp
    column_integer.hpp
    column_fwd.hpp
    column_type.hpp
    column_type_traits.hpp
    data_type.hpp
    decimal128.hpp
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
    set.hpp
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

# We add the headers to the library only so they show up in dev
# environments. It won't actually affect compilation process.
add_library(CoreObjects OBJECT ${REALM_SOURCES} ${UTIL_SOURCES} ${REALM_INSTALL_ALL_HEADERS})
set_target_properties(CoreObjects PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)
target_compile_definitions(CoreObjects PRIVATE
  "$<$<CXX_COMPILER_ID:MSVC>:_CRT_SECURE_NO_WARNINGS>"
  "$<$<CXX_COMPILER_ID:MSVC>:_SCL_SECURE_NO_WARNINGS>"
)
target_compile_definitions(CoreObjects PRIVATE
  $<$<CONFIG:Debug>:REALM_DEBUG=1>
  $<$<CONFIG:MinSizeDebug>:REALM_DEBUG=1>
)
target_compile_options(CoreObjects PRIVATE ${REALM_SANITIZER_FLAGS})

set(REALM_OBJECT_FILES $<TARGET_OBJECTS:CoreObjects>)
list(APPEND REALM_OBJECT_FILES $<TARGET_OBJECTS:Bid>)

if(COMMAND set_target_xcode_attributes)
    set_target_xcode_attributes(CoreObjects)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "^Windows")
    add_dependencies(CoreObjects sha_win32 getopt_win32)
    list(APPEND REALM_OBJECT_FILES $<TARGET_OBJECTS:sha_win32>)
    if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
        list(APPEND REALM_EXTRA_LIBS Version.lib psapi.lib)
    endif()
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


add_library(Storage STATIC ${REALM_OBJECT_FILES} ${REALM_XCODE_DEPENDENCY})
set_target_properties(Storage PROPERTIES OUTPUT_NAME "realm")
add_library(realm ALIAS Storage)

target_compile_definitions(Storage PUBLIC
  $<$<CXX_COMPILER_ID:MSVC>:_CRT_SECURE_NO_WARNINGS>
  $<$<CXX_COMPILER_ID:MSVC>:_SCL_SECURE_NO_WARNINGS>
)

target_compile_definitions(Storage PUBLIC
  $<$<CONFIG:Debug>:REALM_DEBUG=1>
  $<$<CONFIG:MinSizeDebug>:REALM_DEBUG=1>
)

if(NOT MSVC)
    target_compile_options(Storage PUBLIC ${REALM_SANITIZER_FLAGS})
    target_link_options(Storage PUBLIC ${REALM_SANITIZER_FLAGS})
endif()

# GCC<5.1 does not adhere to the C++11 ABI for std::string but some systems
# still produce non-compliant C++11 ABI even with newer version of GCC (Centos 6 for example).
# This performs a linking test and communicates to dependencies built with newer compilers 
# that the old ABI should be used.
if(UNIX AND NOT APPLE AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    try_compile(USES_OLD_ABI ${RealmCore_BINARY_DIR}/abitest ${RealmCore_SOURCE_DIR}/tools/cmake/abitest abitest)
    if(USES_OLD_ABI)
        message(WARNING "You are compiling on a system that uses a non-compliant C++11 ABI. Downstream components depending on RealmCore::Storage will be built with _GLIBCXX_USE_CXX11_ABI=0.")
        target_compile_definitions(Storage INTERFACE "_GLIBCXX_USE_CXX11_ABI=0")
    endif()
endif()

target_include_directories(Storage
    INTERFACE
        $<BUILD_INTERFACE:${RealmCore_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${RealmCore_BINARY_DIR}/src>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

target_link_libraries(Storage INTERFACE ${REALM_EXTRA_LIBS} ${CMAKE_THREAD_LIBS_INIT})

if(ANDROID OR CMAKE_SYSTEM_NAME MATCHES "^Windows")
    set(REALM_SKIP_SHARED_LIB ON)
endif()

if(NOT REALM_SKIP_SHARED_LIB)
    add_library(StorageShared SHARED ${REALM_OBJECT_FILES} ${REALM_XCODE_DEPENDENCY})
    add_library(realm-shared ALIAS StorageShared)

    target_include_directories(StorageShared
        INTERFACE
            $<BUILD_INTERFACE:${RealmCore_SOURCE_DIR}/src>
            $<BUILD_INTERFACE:${RealmCore_BINARY_DIR}/src>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

    set_target_properties(
        StorageShared PROPERTIES
            OUTPUT_NAME "realm"
    )

    target_link_libraries(StorageShared PRIVATE ${REALM_EXTRA_LIBS} ${CMAKE_THREAD_LIBS_INIT}
    )
endif()

if(UNIX AND NOT APPLE)
    if(REALM_USE_SYSTEM_OPENSSL)
        # Use system OpenSSL
        find_package(OpenSSL REQUIRED)
    else()
        # Use our OpenSSL
        if(NOT OpenSSL_DIR)
            if(NOT EXISTS ${CMAKE_CURRENT_BINARY_DIR}/openssl/lib/cmake/OpenSSL/OpenSSLConfig.cmake)
                if(ANDROID)
                    set(OPENSSL_URL "http://static.realm.io/downloads/openssl/${OPENSSL_VERSION}/Android/${CMAKE_ANDROID_ARCH_ABI}/openssl.tgz")
                else()
                    set(OPENSSL_URL "http://static.realm.io/downloads/openssl/${OPENSSL_VERSION}/Linux/x86_64/openssl.tgz")
                endif()

                message(STATUS "Getting ${OPENSSL_URL}...")
                file(DOWNLOAD "${OPENSSL_URL}" "${CMAKE_CURRENT_BINARY_DIR}/openssl/openssl.tgz" STATUS download_status)

                list(GET download_status 0 status_code)
                if (NOT "${status_code}" STREQUAL "0")
                    message(FATAL_ERROR "Downloading ${url}... Failed. Status: ${download_status}")
                endif()

                message(STATUS "Uncompressing OpenSSL...")
                execute_process(
                    COMMAND ${CMAKE_COMMAND} -E tar xfz "openssl.tgz"
                    WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/openssl"
                )
            endif()

            set(OpenSSL_DIR "${CMAKE_CURRENT_BINARY_DIR}/openssl/lib/cmake/OpenSSL")
        endif()
        find_package(OpenSSL REQUIRED CONFIG)
    endif()

    get_target_property(OPENSSL_INCLUDE_DIR OpenSSL::Crypto INTERFACE_INCLUDE_DIRECTORIES)

    target_include_directories(CoreObjects SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})
    target_link_libraries(Storage PUBLIC OpenSSL::Crypto)
    if (REALM_ENABLE_SYNC)
	    target_link_libraries(Storage PUBLIC OpenSSL::SSL)
    endif()
    
    if(NOT REALM_SKIP_SHARED_LIB)
        target_link_libraries(StorageShared PUBLIC OpenSSL::Crypto)
    endif()
endif()

if(APPLE)
    target_link_libraries(Storage PUBLIC ${Foundation})
    if(NOT REALM_SKIP_SHARED_LIB)
        target_link_libraries(StorageShared PUBLIC ${Foundation})
    endif()
endif(APPLE)

install(TARGETS Storage EXPORT realm
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
    install(TARGETS StorageShared
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
add_subdirectory(object-store)
if (REALM_ENABLE_SYNC)
    add_subdirectory(sync)
endif()
