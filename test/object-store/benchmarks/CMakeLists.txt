include_directories(../../external/catch/single_include .)
include_directories(../util .)

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
    )
endif()

add_executable(benchmarks ${SOURCES} ${HEADERS})
target_compile_definitions(benchmarks PRIVATE ${PLATFORM_DEFINES})

if(REALM_ENABLE_SYNC)
    # It's necessary to explicitly link to realm-sync here to control the order in which libraries are
    # linked to avoid link errors when using GNU ld.
    target_link_libraries(benchmarks realm-sync realm-sync-server realm-parser)
endif()

target_link_libraries(benchmarks realm-object-store ${PLATFORM_LIBRARIES})
set_target_properties(benchmarks PROPERTIES
      EXCLUDE_FROM_ALL 1
      EXCLUDE_FROM_DEFAULT_BUILD 1)
