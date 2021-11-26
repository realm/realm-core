set(SYNC_SOURCES
    config.cpp
    noinst/changeset_index.cpp
    noinst/client_history_impl.cpp
    noinst/client_impl_base.cpp
    noinst/client_reset.cpp
    noinst/client_reset_operation.cpp
    noinst/compact_changesets.cpp
    noinst/compression.cpp
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
    subscriptions.cpp
    transform.cpp
    ../util/network_ssl.cpp
    ../util/network.cpp
    ../util/websocket.cpp
    ../util/ez_websocket.cpp
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
    client_base.hpp
    history.hpp
    instruction_applier.hpp
    instruction_replication.hpp
    instructions.hpp
    object_id.hpp
    protocol.hpp
    subscriptions.hpp
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

add_subdirectory(noinst/server)

if(NOT REALM_BUILD_LIB_ONLY AND NOT CMAKE_SYSTEM_NAME MATCHES "^Windows")
	add_subdirectory(tools)
endif()

