set(HEADERS
    ../util/event_loop.hpp
    ../util/index_helpers.hpp
    ../util/test_file.hpp
    ../util/test_utils.hpp
)

set(SOURCES
    main.cpp
    object.cpp
    results.cpp

    ../../util/crypt_key.cpp
    ../util/event_loop.cpp
    ../util/test_file.cpp
    ../util/test_utils.cpp
)


if(REALM_ENABLE_SYNC)
    list(APPEND HEADERS
        ../sync/sync_test_utils.hpp
        ../sync/session/session_util.hpp
    )
    list(APPEND SOURCES
        ../sync/sync_test_utils.cpp
        client_reset.cpp
    )
endif()

add_executable(object-store-benchmarks ${SOURCES} ${HEADERS})

target_include_directories(object-store-benchmarks PRIVATE 
    ..
)

if(REALM_ENABLE_SYNC)
    target_link_libraries(object-store-benchmarks SyncServer)
    enable_stdfilesystem(object-store-benchmarks)
endif()

target_link_libraries(object-store-benchmarks ObjectStore Catch2::Catch2)
set_target_properties(object-store-benchmarks PROPERTIES
      EXCLUDE_FROM_ALL 1
      EXCLUDE_FROM_DEFAULT_BUILD 1)

add_dependencies(benchmarks object-store-benchmarks)

# on Apple platforms we use the built-in CFRunLoop
# everywhere else it's libuv, except UWP where it doesn't build
if(NOT APPLE AND NOT WINDOWS_STORE)
    target_include_directories(object-store-benchmarks PRIVATE ${libuv_include_dir})
    target_link_libraries(object-store-benchmarks ${libuv_target})
endif()
