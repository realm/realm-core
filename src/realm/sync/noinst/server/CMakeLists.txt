set(SERVER_SOURCES
    access_control.cpp
    access_token.cpp
    encrypt_fingerprint.cpp
    encryption_transformer.cpp
    metrics.cpp
    reopening_file_logger.cpp
    server.cpp
    server_configuration.cpp
    server_dir.cpp
    server_file_access_cache.cpp
    server_history.cpp
    server_legacy_migration.cpp
    vacuum.cpp
)

set(SERVER_HEADERS
    access_control.hpp
    access_token.hpp
    clock.hpp
    command_line_util.hpp
    crypto_server.hpp
    encrypt_fingerprint.hpp
    encryption_transformer.hpp
    metrics.hpp
    permissions.hpp
    reopening_file_logger.hpp
    server.hpp
    server_configuration.hpp
    server_dir.hpp
    server_file_access_cache.hpp
    server_history.hpp
    server_impl_base.hpp
    server_legacy_migration.hpp
    vacuum.hpp
)


add_library(SyncServer STATIC EXCLUDE_FROM_ALL ${SERVER_SOURCES} ${SERVER_HEADERS})
add_library(Realm::SyncServer ALIAS SyncServer)

set_target_properties(SyncServer PROPERTIES
    OUTPUT_NAME "realm-server"
)
target_link_libraries(SyncServer PUBLIC Sync QueryParser)
if (REALM_BUILD_DOGLESS)
    message(STATUS "Building Sync Server with Dogless (statsd) support.")
    target_link_libraries(SyncServer PUBLIC Dogless)
endif()

if(APPLE AND NOT REALM_FORCE_OPENSSL)
    target_sources(SyncServer PRIVATE crypto_server_apple.mm)
else()
    target_sources(SyncServer PRIVATE crypto_server_openssl.cpp)
endif()
