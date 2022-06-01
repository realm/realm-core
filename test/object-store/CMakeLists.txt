set(HEADERS
    util/event_loop.hpp
    util/index_helpers.hpp
    util/test_file.hpp
    util/test_utils.hpp
    collection_fixtures.hpp
)

set(SOURCES
    backup.cpp
    collection_change_indices.cpp
    dictionary.cpp
    frozen_objects.cpp
    index_set.cpp
    list.cpp
    main.cpp
    migrations.cpp
    object.cpp
    object_store.cpp
    primitive_list.cpp
    realm.cpp
    results.cpp
    schema.cpp
    set.cpp
    thread_safe_reference.cpp
    transaction_log_parsing.cpp
    uuid.cpp
    c_api/c_api.cpp
    c_api/c_api.c

    util/event_loop.cpp
    util/test_file.cpp
    util/test_utils.cpp
)

if(REALM_ENABLE_AUTH_TESTS)
    list(APPEND SOURCES util/baas_admin_api.cpp)
endif()

if(REALM_ENABLE_SYNC)
    list(APPEND HEADERS
        sync/flx_sync_harness.hpp
        sync/session/session_util.hpp
        sync/sync_test_utils.hpp
        util/baas_admin_api.hpp
    )
    list(APPEND SOURCES
        bson.cpp
        sync/client_reset.cpp
        sync/file.cpp
        sync/flx_sync.cpp
        sync/metadata.cpp
        sync/remote_mongo_tests.cpp
        sync/session/connection_change_notifications.cpp
        sync/session/progress_notifications.cpp
        sync/session/session.cpp
        sync/session/wait_for_completion.cpp
        sync/sync_manager.cpp
        sync/sync_test_utils.cpp
        sync/user.cpp
        sync/app.cpp
    )
    if(APPLE)
        list(APPEND SOURCES audit.cpp)
    endif()
endif()

add_executable(ObjectStoreTests ${SOURCES} ${HEADERS})
set_target_properties(ObjectStoreTests PROPERTIES
    OUTPUT_NAME realm-object-store-tests
)

if(MSVC)
    # increase the number of sections supported in an obj file for the heavily templated tests
    target_compile_options(ObjectStoreTests PRIVATE /bigobj)
endif()

target_link_libraries(ObjectStoreTests ObjectStore RealmFFIStatic)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
        target_link_libraries(ObjectStoreTests stdc++fs)
    endif()
endif()

create_coverage_target(generate-coverage ObjectStoreTests)

add_test(NAME ObjectStoreTests COMMAND realm-object-store-tests)

if(REALM_ENABLE_SYNC)
    target_link_libraries(ObjectStoreTests SyncServer)
    option(REALM_ENABLE_COMPENSATING_WRITES_TESTS "" OFF)
    if (REALM_ENABLE_COMPENSATING_WRITES_TESTS)
        target_compile_definitions(ObjectStoreTests PRIVATE
            REALM_ENABLE_COMPENSATING_WRITES_TESTS=1
        )
    endif()
    option(REALM_ENABLE_AUTH_TESTS "" OFF)
    if(REALM_ENABLE_AUTH_TESTS)
        if(NOT REALM_MONGODB_ENDPOINT)
            message(FATAL_ERROR "REALM_MONGODB_ENDPOINT must be set when specifying REALM_ENABLE_AUTH_TESTS.")
        endif()

        target_compile_definitions(ObjectStoreTests PRIVATE
            REALM_ENABLE_AUTH_TESTS=1
            REALM_MONGODB_ENDPOINT="${REALM_MONGODB_ENDPOINT}"
        )

        find_package(CURL REQUIRED)
        target_link_libraries(ObjectStoreTests CURL::libcurl)
    endif()
endif()

if(REALM_TEST_SYNC_LOGGING)
    target_compile_definitions(ObjectStoreTests PRIVATE
        TEST_ENABLE_SYNC_LOGGING=1
    )
endif()

target_include_directories(ObjectStoreTests PRIVATE ${CATCH_INCLUDE_DIR} ${JSON_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR})

if(CMAKE_SYSTEM_NAME MATCHES "^Windows")
    # Increase the Catch2 virtual console width because our test names can be very long and they break test reports
    target_compile_definitions(ObjectStoreTests PRIVATE -DCATCH_CONFIG_CONSOLE_WIDTH=300)
endif()

# Resources required for running the tests
file(GLOB REQUIRED_TEST_FILES
     "*.realm")


add_custom_command(TARGET ObjectStoreTests POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy_if_different ${REQUIRED_TEST_FILES} $<TARGET_FILE_DIR:ObjectStoreTests>)

# on Apple platforms we use the built-in CFRunLoop
# everywhere else it's libuv, except UWP where it doesn't build
if(NOT APPLE AND NOT WINDOWS_STORE)
    option(REALM_FETCH_MISSING_DEPENDENCIES "Download missing dependencies with CMake's FetchContent where possible" ON)
    if(REALM_FETCH_MISSING_DEPENDENCIES)
        find_package(LibUV)
    else()
        find_package(LibUV REQUIRED)
    endif()
    if(LibUV_FOUND)
        set(libuv_target LibUV::LibUV)
    elseif(REALM_FETCH_MISSING_DEPENDENCIES)
        message(STATUS "LibUV not found, building from source with FetchContent")
        include(FetchContent)
        set(libUV_Git_TAG "v1.35.0")
        if(MSVC)
            set(liUV_Git_TAG "v1.43.0")
        endif()
        FetchContent_Declare(
            libuv
            GIT_REPOSITORY https://github.com/libuv/libuv.git
            GIT_TAG ${libUV_Git_TAG}
        )
        FetchContent_Populate(libuv)
        add_subdirectory(${libuv_SOURCE_DIR} ${libuv_BINARY_DIR} EXCLUDE_FROM_ALL)
        set(libuv_target uv_a)
    endif()
    target_link_libraries(ObjectStoreTests ${libuv_target})
    # FIXME: ObjectStore itself shouldn't care about this, but we need to refactor scheduler.cpp to make it happen
    target_compile_definitions(ObjectStore PUBLIC REALM_HAVE_UV=1)
    get_property(libuv_include_dir TARGET ${libuv_target} PROPERTY INCLUDE_DIRECTORIES)
    target_include_directories(ObjectStore PRIVATE ${libuv_include_dir})
endif()

if(APPLE)
    target_compile_definitions(ObjectStoreTests PRIVATE _LIBCPP_DISABLE_AVAILABILITY)
endif()

add_subdirectory(notifications-fuzzer)
add_subdirectory(benchmarks)
