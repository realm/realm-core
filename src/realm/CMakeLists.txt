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

    # Configure the optimization level explicitly because CMake doesn't recognize -Oz
    # in CMAKE_CXX_CFLAGS.
    XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL "\$(GCC_OPTIMIZATION_LEVEL_\$(CONFIGURATION))"
    XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL_Debug "0"
    XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL_MinSizeDebug "z"
    XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL_Release "3"
    XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL_RelWithDebInfo "3"
    XCODE_ATTRIBUTE_GCC_OPTIMIZATION_LEVEL_RelMinSize "3"
)

add_library(realm ${REALM_LIBTYPE} $<TARGET_OBJECTS:realm-objects> placeholder.cpp)

set_target_properties(
    realm PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${REALM_SOVERSION}
)

if(REALM_LIBTYPE STREQUAL "STATIC")
    set_target_properties(
        realm PROPERTIES
        # The static library may get included in a dynamic library
        # (the bindings), so build as position independent code.
        POSITION_INDEPENDENT_CODE true

        XCODE_ATTRIBUTE_INSTALL_PATH "/"
        XCODE_ATTRIBUTE_SKIP_INSTALL "NO"
    )
endif()

target_link_libraries(
    realm
    ${REALM_EXTRA_LIBS}
    ${CMAKE_THREAD_LIBS_INIT}
)

if(REALM_ENABLE_ENCRYPTION)
    if(REALM_PLATFORM STREQUAL "Android")
        include(ExternalProject)

        # We patch one file in order not to include too many dependencies
        set(PATCH_CMD sed
                -i.backup
                /OPENSSL_cleanse/d
                crypto/sha/sha256.c
        )

        # Set the config step command
        set(CONFIGURE_CMD ${ssl_conf_wrapper}
                ./config
                no-idea
                no-camellia
                no-seed
                no-bf
                no-cast
                no-des
                no-rc2
                no-rc4
                no-rc5
                no-md2
                no-md4
                no-ripemd
                no-mdc2
                no-rsa
                no-dsa
                no-dh
                no-ec
                no-ecdsa
                no-ecdh
                no-sock
                no-ssl2
                no-ssl3
                no-err
                no-krb5
                no-engine
                no-srtp
                no-speed
                -DOPENSSL_NO_SHA512
                -DOPENSSL_NO_SHA0
                -w
                -fPIC
        )

        # Get the list of include folders provided by the android toolchain
        get_property(inc_dirs DIRECTORY PROPERTY INCLUDE_DIRECTORIES)

        # Join them into a single string
        JOIN("${inc_dirs}" " -I" includes)

        # Get the first element of the STL include list provided by the android toolcahin
        list(GET ANDROID_STL_INCLUDE_DIRS 0 stdarg_folder)

        # Add the tr1 folder. That's where stdarg.h is located
        set(DEPEND_INCLUDES "-I${includes} -I${stdarg_folder}/tr1")

        # Generate the build script. This wrapper is needed in order to be able to pass
        # environment variables and to avoid cmake trying to be smart about auto escaping
        configure_file(${CMAKE_MODULE_PATH}/android-openssl-build.in android-openssl-build)
        set(build_wrapper ${PROJECT_BINARY_DIR}/src/realm/android-openssl-build)

        # Configure the config wrapper
        configure_file(${CMAKE_MODULE_PATH}/openssl-config-wrapper.in openssl-config-wrapper)
        set(ssl_conf_wrapper ${PROJECT_BINARY_DIR}/openssl-config-wrapper)

        ExternalProject_Add(openssl
                URL http://www.openssl.org/source/openssl-${ANDROID_OPENSSL_VERSION}.tar.gz
                PATCH_COMMAND ${PATCH_CMD}
                CONFIGURE_COMMAND ${CONFIGURE_CMD}
                BUILD_COMMAND ${build_wrapper}
                BUILD_IN_SOURCE 1
        )
        ExternalProject_Get_Property(openssl install_dir)
        add_dependencies(realm openssl)
    elseif(NOT APPLE)
        target_link_libraries(realm ${OPENSSL_LIBRARIES})
    endif()
endif()

if(APPLE)
    target_link_libraries(realm ${Foundation})
endif(APPLE)

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
