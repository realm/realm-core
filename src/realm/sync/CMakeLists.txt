set(SYNC_SOURCES
    config.cpp
    noinst/changeset_index.cpp
    noinst/client_history_impl.cpp
    noinst/client_impl_base.cpp
    noinst/client_reset.cpp
    noinst/client_reset_operation.cpp
    noinst/client_reset_recovery.cpp
    noinst/compact_changesets.cpp
    noinst/migration_store.cpp
    noinst/pending_bootstrap_store.cpp
    noinst/protocol_codec.cpp
    noinst/sync_metadata_schema.cpp
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
    subscriptions.cpp
    transform.cpp
    network/default_socket.cpp
    network/http.cpp
    network/network.cpp
    network/network_ssl.cpp
    network/websocket.cpp
)

set(IMPL_INSTALL_HEADERS
    impl/clock.hpp
    impl/clamped_hex_dump.hpp
)

set(SYNC_INSTALL_HEADERS
    binding_callback_thread_observer.hpp
    config.hpp
    changeset_encoder.hpp
    changeset_parser.hpp
    changeset.hpp
    client.hpp
    client_base.hpp
    socket_provider.hpp
    history.hpp
    instruction_applier.hpp
    instruction_replication.hpp
    instructions.hpp
    object_id.hpp
    protocol.hpp
    subscriptions.hpp
    transform.hpp
)

set(SYNC_NETWORK_INSTALL_HEADERS
    network/default_socket.hpp
    network/http.hpp
    network/network.hpp
    network/network_ssl.hpp
    network/websocket.hpp
)

set(NOINST_HEADERS
    trigger.hpp

    noinst/changeset_index.hpp
    noinst/client_history_impl.hpp
    noinst/client_impl_base.hpp
    noinst/client_reset.hpp
    noinst/client_reset_operation.hpp
    noinst/client_reset_recovery.hpp
    noinst/compact_changesets.hpp
    noinst/integer_codec.hpp
    noinst/migration_store.hpp
    noinst/pending_bootstrap_store.hpp
    noinst/protocol_codec.hpp
    noinst/root_certs.hpp
    noinst/sync_metadata_schema.hpp
)

set(SYNC_HEADERS ${IMPL_INSTALL_HEADESR}
    ${SYNC_INSTALL_HEADERS}
    ${NOINST_HEADERS})

add_library(Sync STATIC ${SYNC_SOURCES} ${SYNC_HEADERS})
add_library(Realm::Sync ALIAS Sync)

set_target_properties(Sync PROPERTIES
    OUTPUT_NAME "realm-sync"
)

if(REALM_ENABLE_SYNC_MULTIPLEXING)
    target_compile_definitions(Sync PUBLIC REALM_ENABLE_SYNC_MULTIPLEXING=1)
endif()

target_link_libraries(Sync PUBLIC Storage)

if(APPLE AND NOT REALM_FORCE_OPENSSL)
    target_link_options(Sync INTERFACE "SHELL:-framework Security")
elseif(REALM_HAVE_WOLFSSL)
    target_link_libraries(Sync PUBLIC WolfSSL)
elseif(REALM_HAVE_OPENSSL)
    target_link_libraries(Sync PUBLIC OpenSSL::SSL)
endif()

if(WIN32 AND NOT WINDOWS_STORE)
    target_link_libraries(Sync INTERFACE Version.lib)
    if(CMAKE_VERSION VERSION_LESS "3.21")
        # This is needed for OpenSSL, but CMake's FindOpenSSL didn't declare it
        # on the OpenSSL::Crypto target until CMake 3.21.0. 
        target_link_libraries(Sync INTERFACE Crypt32.lib)
    endif()
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

install(FILES ${SYNC_NETWORK_INSTALL_HEADERS}
        DESTINATION include/realm/sync/network
        COMPONENT devel)

install(FILES ${UTIL_INSTALL_HEADERS}
        DESTINATION include/realm/util
        COMPONENT devel)

install(FILES ../../external/mpark/variant.hpp
        DESTINATION include/external/mpark
        COMPONENT devel)
install(FILES ../../external/json/json.hpp
        DESTINATION include/external/json
        COMPONENT devel)

add_subdirectory(noinst/server)

if(NOT REALM_BUILD_LIB_ONLY AND NOT CMAKE_SYSTEM_NAME MATCHES "^Windows")
	add_subdirectory(tools)
endif()

