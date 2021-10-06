set(SYNC_SOURCES
    config.cpp
    noinst/changeset_index.cpp
    noinst/client_history_impl.cpp
    noinst/client_impl_base.cpp
    noinst/client_reset.cpp
    noinst/client_reset_operation.cpp
    noinst/compact_changesets.cpp
    noinst/compression.cpp
    noinst/file_descriptors.cpp
    noinst/protocol_codec.cpp
    changeset_encoder.cpp
    changeset_parser.cpp
    changeset.cpp
    client.cpp
    history.cpp
    instruction_applier.cpp
    instruction_replication.cpp
    instructions.cpp
    object_id.cpp
    protocol.cpp
    transform.cpp
    ../util/network_ssl.cpp
    ../util/network.cpp
)

set(IMPL_INSTALL_HEADERS
    impl/clock.hpp
    impl/clamped_hex_dump.hpp
)

set(SYNC_INSTALL_HEADERS
    config.hpp
    changeset_encoder.hpp
    changeset_parser.hpp
    changeset.hpp
    client.hpp
    history.hpp
    instruction_applier.hpp
    instruction_replication.hpp
    instructions.hpp
    object_id.hpp
    protocol.hpp
    transform.hpp
)

set(NOINST_HEADERS
    noinst/changeset_index.hpp
    noinst/client_history_impl.hpp
    noinst/client_impl_base.hpp
    noinst/client_reset.hpp
    noinst/client_reset_operation.hpp
    noinst/compact_changesets.hpp
    noinst/compression.hpp
    noinst/integer_codec.hpp
    noinst/protocol_codec.hpp
    noinst/root_certs.hpp
)

set(SYNC_HEADERS ${IMPL_INSTALL_HEADESR}
    ${SYNC_INSTALL_HEADERS}
    ${NOINST_HEADERS})

add_library(Sync STATIC ${SYNC_SOURCES} ${SYNC_HEADERS})
add_library(Realm::Sync ALIAS Sync)

set_target_properties(Sync PROPERTIES
    OUTPUT_NAME "realm-sync"
)

target_link_libraries(Sync PUBLIC Storage)

# Use Zlib if the imported target is defined, otherise use -lz on Apple platforms
if(TARGET ZLIB::ZLIB)
    target_link_libraries(Sync PUBLIC ZLIB::ZLIB)
elseif(APPLE)
    target_link_options(Sync PUBLIC "-lz")
else()
    message(FATAL_ERROR "No zlib dependency defined for Realm::Sync")
endif()

if(APPLE AND NOT REALM_FORCE_OPENSSL)
    target_link_options(Sync INTERFACE "SHELL:-framework Security")
else()
    target_link_libraries(Sync PUBLIC OpenSSL::SSL)
endif()

if(WIN32 AND NOT WINDOWS_STORE)
    target_link_libraries(Sync INTERFACE Mincore.lib)
endif()

install(TARGETS Sync EXPORT realm
    ARCHIVE COMPONENT devel
    PUBLIC_HEADER COMPONENT devel)

install(FILES ${SYNC_INSTALL_HEADERS}
        DESTINATION include/realm/sync
        COMPONENT devel)

install(FILES ${IMPL_INSTALL_HEADERS}
        DESTINATION include/realm/impl
        COMPONENT devel)

install(FILES ${SYNC_INSTALL_HEADERS}
        DESTINATION include/realm/sync
        COMPONENT devel)

install(FILES ${UTIL_INSTALL_HEADERS}
        DESTINATION include/realm/util
        COMPONENT devel)

install(FILES ${UTIL_METERED_INSTALL_HEADERS}
        DESTINATION include/realm/util/metered
        COMPONENT devel)

install(FILES ../../external/mpark/variant.hpp
        DESTINATION include/external/mpark
        COMPONENT devel)
install(FILES ../../external/json/json.hpp
        DESTINATION include/external/json
        COMPONENT devel)

if(REALM_BUILD_COMMANDLINE_TOOLS)
    add_executable(DumpCommand dump_command.cpp)
    set_target_properties(DumpCommand PROPERTIES
                          OUTPUT_NAME "realm-dump"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
		  target_link_libraries(DumpCommand Sync Storage)

    # FIXME: RCORE-331 stat command currently fails to build
    # add_executable(StatCommand stat_command.cpp)
    # set_target_properties(StatCommand PROPERTIES
    #                       OUTPUT_NAME "realm-stat"
    #                       DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    # target_link_libraries(StatCommand Sync Storage)

    add_executable(ApplyToStateCommand apply_to_state_command.cpp)
    set_target_properties(ApplyToStateCommand PROPERTIES
                          OUTPUT_NAME "realm-apply-to-state"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    target_link_libraries(ApplyToStateCommand Sync Storage)

    # FIXME: Port realm-hist to new instruction format
    # add_executable(HistCommand sync/hist_command.cpp)
    # set_target_properties(HistCommand PROPERTIES
    #                       OUTPUT_NAME "realm-hist"
    #                       DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    # target_link_libraries(HistCommand Sync Storage)

    add_executable(PrintChangesetCommand print_changeset_command.cpp)
    set_target_properties(PrintChangesetCommand PROPERTIES
                          OUTPUT_NAME "realm-print-changeset"
                          DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
    target_link_libraries(PrintChangesetCommand Sync Storage)

    install(TARGETS
        DumpCommand
        # StatCommand
        # HistCommand
        ApplyToStateCommand
        COMPONENT runtime
    )
endif()

if (REALM_BUILD_INSPECTOR)
    add_subdirectory(inspector)
endif()

add_subdirectory(noinst/server)
