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
    ${CATCH_INCLUDE_DIR}
    ..
)

if(REALM_ENABLE_SYNC)
    target_link_libraries(object-store-benchmarks SyncServer)
endif()

target_link_libraries(object-store-benchmarks ObjectStore)
set_target_properties(object-store-benchmarks PROPERTIES
      EXCLUDE_FROM_ALL 1
      EXCLUDE_FROM_DEFAULT_BUILD 1)
