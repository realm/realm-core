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
    access_token.cpp
    auth.cpp
    changeset_cooker.cpp
    changeset_encoder.cpp
    changeset_parser.cpp
    changeset.cpp
    client.cpp
    history.cpp
    instruction_applier.cpp
    instruction_replication.cpp
    instructions.cpp
    object_id.cpp
    object.cpp
    protocol.cpp
    transform.cpp
    ../util/network_ssl.cpp
    ../util/network.cpp
    
)

set(SERVER_SOURCES
    encrypt/encryption_transformer.cpp
    noinst/reopening_file_logger.cpp
    noinst/server_dir.cpp
    noinst/server_file_access_cache.cpp
    noinst/server_history.cpp
    noinst/server_legacy_migration.cpp
    noinst/vacuum.cpp
    access_control.cpp
    metrics.cpp
    server_configuration.cpp
    server.cpp
)

set(SERVER_INSTALL_HEADERS
    access_control.hpp
    access_token.hpp
    auth.hpp
    clock.hpp
    metrics.hpp
    server_configuration.hpp
    server.hpp
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
    changeset_cooker.hpp
    changeset_encoder.hpp
    changeset_parser.hpp
    changeset.hpp
    client.hpp
    crypto_server.hpp
    history.hpp
    instruction_applier.hpp
    instruction_replication.hpp
    instructions.hpp
    object_id.hpp
    object.hpp
    permissions.hpp
    protocol.hpp
    transform.hpp
    version.hpp
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
    ${SYNC_INSTALL_HEADERS}
    ${NOINST_HEADERS})
set(SYNC_SERVER_HEADERS ${SERVER_INSTALL_HEADERS} ${SERVER_NOINST_HEADERS})

if(APPLE AND NOT REALM_FORCE_OPENSSL)
    list(APPEND SERVER_SOURCES
        crypto_server_apple.mm
    )
    set(PLATFORM_LIBRARIES ${Foundation} ${Security})
else()
    list(APPEND SYNC_SOURCES
        crypto_server_openssl.cpp
        )
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
    # Add REALM_DEBUG=1 to compile definitions for Debug / MinSizeDebug targets
    # FIXME: This should be exported by Core's realm-config.cmake instead.
    $<$<CONFIG:Debug>:REALM_DEBUG=1>
    $<$<CONFIG:MinSizeDebug>:REALM_DEBUG=1>)
target_include_directories(Sync PUBLIC
                           $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
                           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

target_link_libraries(Sync PUBLIC Storage ${PLATFORM_LIBRARIES} )

list(APPEND targets_to_install Sync)

add_library(SyncServer STATIC EXCLUDE_FROM_ALL ${SERVER_SOURCES} ${SYNC_SERVER_HEADERS})
set_target_properties(SyncServer PROPERTIES
    OUTPUT_NAME "realm-server"
    POSITION_INDEPENDENT_CODE ON)
target_link_libraries(SyncServer PUBLIC Sync QueryParser Storage)
if (REALM_BUILD_DOGLESS)
    message(STATUS "Building Sync Server with Dogless (statsd) support.")
    target_link_libraries(SyncServer PUBLIC Dogless)
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
    target_link_libraries(VacuumCommand SyncServer Storage)

    add_executable(EncryptionTransformer encrypt/encryption_transformer_command.cpp)
    set_target_properties(EncryptionTransformer PROPERTIES
                          OUTPUT_NAME "realm-encryption-transformer"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    target_link_libraries(EncryptionTransformer SyncServer Storage)

    add_executable(DumpCommand sync/dump_command.cpp)
    set_target_properties(DumpCommand PROPERTIES
                          OUTPUT_NAME "realm-dump"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
		  target_link_libraries(DumpCommand Sync Storage)

    add_executable(StatCommand sync/stat_command.cpp)
    set_target_properties(StatCommand PROPERTIES
                          OUTPUT_NAME "realm-stat"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    target_link_libraries(StatCommand Sync Storage)

    # FIXME: Port realm-hist to new instruction format
    # add_executable(HistCommand sync/hist_command.cpp)
    # set_target_properties(HistCommand PROPERTIES
    #                       OUTPUT_NAME "realm-hist"
    #                       DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    # target_link_libraries(HistCommand Sync Storage)

    add_executable(RealmUpgrade EXCLUDE_FROM_ALL sync/realm_upgrade.cpp)
    set_target_properties(RealmUpgrade PROPERTIES
                          OUTPUT_NAME "realm-upgrade-6"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    target_link_libraries(RealmUpgrade Sync SyncServer Storage)

    add_executable(VerifyServerFileCommand sync/verify_server_file_command.cpp)
    set_target_properties(VerifyServerFileCommand PROPERTIES
                          OUTPUT_NAME "realm-verify-server-file"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    target_link_libraries(VerifyServerFileCommand SyncServer Storage)

    add_executable(ServerIndexCommand sync/server_index_command.cpp)
    set_target_properties(ServerIndexCommand PROPERTIES
                          OUTPUT_NAME "realm-server-index"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    target_link_libraries(ServerIndexCommand SyncServer Storage)

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
