set(SERVER_SOURCES
    access_control.cpp
    access_token.cpp
    server.cpp
    server_dir.cpp
    server_file_access_cache.cpp
    server_history.cpp
)

set(SERVER_HEADERS
    access_control.hpp
    access_token.hpp
    clock.hpp
    crypto_server.hpp
    permissions.hpp
    server.hpp
    server_dir.hpp
    server_file_access_cache.hpp
    server_history.hpp
    server_impl_base.hpp
)


add_library(SyncServer STATIC EXCLUDE_FROM_ALL ${SERVER_SOURCES} ${SERVER_HEADERS})
add_library(Realm::SyncServer ALIAS SyncServer)

set_target_properties(SyncServer PROPERTIES
    OUTPUT_NAME "realm-server"
)
target_link_libraries(SyncServer PUBLIC Sync QueryParser)

if(APPLE AND NOT REALM_FORCE_OPENSSL)
    target_sources(SyncServer PRIVATE crypto_server_apple.mm)
elseif(REALM_HAVE_OPENSSL OR REALM_HAVE_WOLFSSL)
    target_sources(SyncServer PRIVATE crypto_server_openssl.cpp)
else()
    target_sources(SyncServer PRIVATE crypto_server_stub.cpp)
endif()
