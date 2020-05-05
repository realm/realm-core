set(SYNC_SOURCES
    encrypt/fingerprint.cpp
    noinst/changeset_index.cpp
    noinst/client_file_access_cache.cpp
    noinst/client_history_impl.cpp
    noinst/client_impl_base.cpp
    noinst/client_reset.cpp
    noinst/client_state_download.cpp
    noinst/common_dir.cpp
    noinst/compact_changesets.cpp
    noinst/compression.cpp
    noinst/file_descriptors.cpp
    noinst/object_id_history_state.cpp
    noinst/protocol_codec.cpp
    sync/access_token.cpp
    sync/auth.cpp
    sync/changeset_cooker.cpp
    sync/changeset_encoder.cpp
    sync/changeset_parser.cpp
    sync/changeset.cpp
    sync/client.cpp
    sync/history.cpp
    sync/instruction_applier.cpp
    sync/instruction_replication.cpp
    sync/instructions.cpp
    sync/object_id.cpp
    sync/object.cpp
    sync/protocol.cpp
    sync/transform.cpp
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
    util/network_ssl.cpp
    util/network.cpp
    util/parent_dir.cpp
    util/platform_info.cpp
    util/random.cpp
    util/resource_limits.cpp
    util/substitute.cpp
    util/system_process.cpp
    util/time.cpp
    util/timestamp_formatter.cpp
    util/timestamp_logger.cpp
    util/uri.cpp
    util/websocket.cpp
)

set(SERVER_SOURCES
    encrypt/encryption_transformer.cpp
    noinst/reopening_file_logger.cpp
    noinst/server_dir.cpp
    noinst/server_file_access_cache.cpp
    noinst/server_history.cpp
    noinst/server_legacy_migration.cpp
    noinst/vacuum.cpp
    sync/access_control.cpp
    sync/metrics.cpp
    sync/server_configuration.cpp
    sync/server.cpp
)

set(SERVER_INSTALL_HEADERS
    sync/access_control.hpp
    sync/access_token.hpp
    sync/auth.hpp
    sync/clock.hpp
    sync/metrics.hpp
    sync/server_configuration.hpp
    sync/server.hpp
    util/scratch_allocator.hpp
)

set(SERVER_NOINST_HEADERS
    encrypt/encryption_transformer.hpp
    noinst/server_dir.hpp
    noinst/server_file_access_cache.hpp
    noinst/server_history.hpp
    noinst/server_impl_base.hpp
    noinst/server_legacy_migration.hpp
    noinst/vacuum.hpp
)

set(IMPL_INSTALL_HEADERS
    impl/clock.hpp
    impl/clamped_hex_dump.hpp
)

set(SYNC_INSTALL_HEADERS
    encrypt/fingerprint.hpp
    sync/changeset_cooker.hpp
    sync/changeset_encoder.hpp
    sync/changeset_parser.hpp
    sync/changeset.hpp
    sync/client.hpp
    sync/crypto_server.hpp
    sync/history.hpp
    sync/instruction_applier.hpp
    sync/instruction_replication.hpp
    sync/instructions.hpp
    sync/object_id.hpp
    sync/object.hpp
    sync/permissions.hpp
    sync/protocol.hpp
    sync/transform.hpp
    sync/version.hpp
)

set(UTIL_INSTALL_HEADERS
    util/allocation_metrics.hpp
    util/cf_str.hpp
    util/circular_buffer.hpp
    util/copy_dir_recursive.hpp
    util/demangle.hpp
    util/duplicating_logger.hpp
    util/enum.hpp
    util/file_is_regular.hpp
    util/flat_map.hpp
    util/get_file_size.hpp
    util/http.hpp
    util/json_parser.hpp
    util/load_file.hpp
    util/misc_ext_errors.hpp
    util/network_ssl.hpp
    util/network.hpp
    util/overloaded.hpp
    util/parent_dir.hpp
    util/platform_info.hpp
    util/quote.hpp
    util/random.hpp
    util/resource_limits.hpp
    util/signal_blocker.hpp
    util/string_view.hpp
    util/substitute.hpp
    util/system_process.hpp
    util/thread_exec_guard.hpp
    util/time.hpp
    util/timestamp_formatter.hpp
    util/timestamp_logger.hpp
    util/uri.hpp
    util/value_reset_guard.hpp
    util/websocket.hpp
)

set(UTIL_METERED_INSTALL_HEADERS
    util/metered/deque.hpp
    util/metered/map.hpp
    util/metered/set.hpp
    util/metered/string.hpp
    util/metered/unordered_map.hpp
    util/metered/unordered_set.hpp
    util/metered/vector.hpp
)

set(NOINST_HEADERS
    noinst/changeset_index.hpp
    noinst/client_file_access_cache.hpp
    noinst/client_history_impl.hpp
    noinst/client_impl_base.hpp
    noinst/client_reset.hpp
    noinst/command_line_util.hpp
    noinst/common_dir.hpp
    noinst/compact_changesets.hpp
    noinst/compression.hpp
    noinst/integer_codec.hpp
    noinst/object_id_history_state.hpp
    noinst/protocol_codec.hpp
    noinst/reopening_file_logger.hpp
    noinst/root_certs.hpp
)

set(SYNC_HEADERS ${IMPL_INSTALL_HEADESR}
    ${SYNC_INSTALL_HEADERS} ${UTIL_INSTALL_HEADERS}
    ${UTIL_METERED_INSTALL_HEADERS} ${NOINST_HEADERS})
set(SYNC_SERVER_HEADERS ${SERVER_INSTALL_HEADERS} ${SERVER_NOINST_HEADERS})

if(APPLE AND NOT REALM_FORCE_OPENSSL)
    list(APPEND SERVER_SOURCES
        sync/crypto_server_apple.mm
    )
    set(PLATFORM_DEFINES REALM_HAVE_OPENSSL=0 REALM_HAVE_SECURE_TRANSPORT=1)
    set(PLATFORM_LIBRARIES ${FOUNDATION} ${SECURITY})
else()
    list(APPEND SYNC_SOURCES
        sync/crypto_server_openssl.cpp
        )
    set(PLATFORM_DEFINES REALM_HAVE_OPENSSL=1)
    set(PLATFORM_LIBRARIES OpenSSL::SSL OpenSSL::Crypto)
endif()

list(APPEND PLATFORM_LIBRARIES ZLIB::ZLIB)
if(WIN32 AND NOT WINDOWS_STORE)
    list(APPEND PLATFORM_LIBRARIES Mincore.lib)
endif()

if(MSVC)
    add_compile_options(/Z7) # embed debug info in static library
endif()

add_library(Sync STATIC ${SYNC_SOURCES} ${SYNC_HEADERS})
set_target_properties(Sync PROPERTIES
    OUTPUT_NAME "realm-sync"
    POSITION_INDEPENDENT_CODE ON)

if (APPLE)
    set_target_properties(Sync PROPERTIES VISIBILITY_INLINES_HIDDEN ON)
endif()

target_compile_definitions(Sync PUBLIC
    ${PLATFORM_DEFINES}
    # Add REALM_DEBUG=1 to compile definitions for Debug / MinSizeDebug targets
    # FIXME: This should be exported by Core's realm-config.cmake instead.
    $<$<CONFIG:Debug>:REALM_DEBUG=1>
    $<$<CONFIG:MinSizeDebug>:REALM_DEBUG=1>)
target_include_directories(Sync PUBLIC
                           $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
                           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

target_link_libraries(Sync PUBLIC RealmCore::Storage ${PLATFORM_LIBRARIES} )

list(APPEND targets_to_install Sync)
if(REALM_BUILD_SYNC_SERVER)
    add_library(SyncServer STATIC ${SERVER_SOURCES} ${SYNC_SERVER_HEADERS})
    set_target_properties(SyncServer PROPERTIES
        OUTPUT_NAME "realm-server"
        POSITION_INDEPENDENT_CODE ON)
    target_link_libraries(SyncServer PUBLIC Sync RealmCore::QueryParser RealmCore::Storage)
    if (REALM_BUILD_DOGLESS)
        message(STATUS "Building Sync Server with Dogless (statsd) support.")
        target_link_libraries(SyncServer PUBLIC Dogless)
    endif()
    list(APPEND targets_to_install SyncServer)
endif()

install(TARGETS ${targets_to_install} EXPORT Sync
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT runtime
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT runtime
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT devel)

install(FILES ${SYNC_INSTALL_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/sync
        COMPONENT devel)

install(FILES ${IMPL_INSTALL_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/impl
        COMPONENT devel)

install(FILES ${SYNC_INSTALL_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/sync
        COMPONENT devel)

install(FILES ${UTIL_INSTALL_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/util
        COMPONENT devel)

install(FILES ${UTIL_METERED_INSTALL_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/realm/util/metered
        COMPONENT devel)

if(REALM_BUILD_COMMANDLINE_TOOLS)
    add_executable(SyncWorker sync/server_command.cpp)
    set_target_properties(SyncWorker PROPERTIES
                          OUTPUT_NAME "realm-sync-worker"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    target_link_libraries(SyncWorker SyncServer)

    add_executable(VacuumCommand noinst/vacuum_command.cpp)
    set_target_properties(VacuumCommand PROPERTIES
                          OUTPUT_NAME "realm-vacuum"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    target_link_libraries(VacuumCommand SyncServer RealmCore::Storage)

    add_executable(EncryptionTransformer encrypt/encryption_transformer_command.cpp)
    set_target_properties(EncryptionTransformer PROPERTIES
                          OUTPUT_NAME "realm-encryption-transformer"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    target_link_libraries(EncryptionTransformer SyncServer RealmCore::Storage)

    add_executable(DumpCommand sync/dump_command.cpp)
    set_target_properties(DumpCommand PROPERTIES
                          OUTPUT_NAME "realm-dump"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
		  target_link_libraries(DumpCommand Sync RealmCore::Storage)

    add_executable(StatCommand sync/stat_command.cpp)
    set_target_properties(StatCommand PROPERTIES
                          OUTPUT_NAME "realm-stat"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    target_link_libraries(StatCommand Sync RealmCore::Storage)

    # FIXME: Port realm-hist to new instruction format
    # add_executable(HistCommand sync/hist_command.cpp)
    # set_target_properties(HistCommand PROPERTIES
    #                       OUTPUT_NAME "realm-hist"
    #                       DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    # target_link_libraries(HistCommand Sync RealmCore::Storage)

    add_executable(RealmUpgrade EXCLUDE_FROM_ALL sync/realm_upgrade.cpp)
    set_target_properties(RealmUpgrade PROPERTIES
                          OUTPUT_NAME "realm-upgrade-6"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    target_link_libraries(RealmUpgrade Sync SyncServer RealmCore::Storage)

    add_executable(VerifyServerFileCommand sync/verify_server_file_command.cpp)
    set_target_properties(VerifyServerFileCommand PROPERTIES
                          OUTPUT_NAME "realm-verify-server-file"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    target_link_libraries(VerifyServerFileCommand SyncServer RealmCore::Storage)

    add_executable(ServerIndexCommand sync/server_index_command.cpp)
    set_target_properties(ServerIndexCommand PROPERTIES
                          OUTPUT_NAME "realm-server-index"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    target_link_libraries(ServerIndexCommand SyncServer RealmCore::Storage)

    install(
        TARGETS
            SyncWorker
            VacuumCommand
            EncryptionTransformer
            DumpCommand
            StatCommand
            # HistCommand
            VerifyServerFileCommand
            ServerIndexCommand
        DESTINATION
            ${CMAKE_INSTALL_BINDIR}
    )
endif()
