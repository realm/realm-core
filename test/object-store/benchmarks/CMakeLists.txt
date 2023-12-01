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
        ../util/sync/sync_test_utils.hpp
        ../util/sync/session_util.hpp
    )
    list(APPEND SOURCES
        ../util/sync/sync_test_utils.cpp
        client_reset.cpp
    )
endif()

add_executable(object-store-benchmarks ${SOURCES} ${HEADERS})

target_include_directories(object-store-benchmarks PRIVATE 
    ..
)

if(REALM_ENABLE_SYNC)
    target_link_libraries(object-store-benchmarks SyncServer)
endif()
enable_stdfilesystem(object-store-benchmarks)
target_link_libraries(object-store-benchmarks ObjectStore TestUtil Catch2::Catch2)

add_dependencies(benchmarks object-store-benchmarks)

# on Apple platforms we use the built-in CFRunLoop
# everywhere else it's libuv, except UWP where it doesn't build
if(NOT APPLE AND NOT WINDOWS_STORE AND NOT EMSCRIPTEN AND NOT ANDROID)
    # libuv_target is defined in the parent CMakeLists.txt file
    target_link_libraries(object-store-benchmarks ${libuv_target})
    target_compile_definitions(object-store-benchmarks PRIVATE TEST_SCHEDULER_UV=1)
endif()
