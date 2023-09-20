set(HEADERS
    ../util/crypt_key.hpp
    ../util/spawned_process.hpp
    ../util/test_path.hpp
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
    sectioned_results.cpp
    set.cpp
    thread_safe_reference.cpp
    transaction_log_parsing.cpp
    uuid.cpp
    c_api/c_api.cpp
    c_api/c_api.c

    ../util/crypt_key.cpp
    ../util/spawned_process.cpp
    ../util/test_path.cpp
    util/event_loop.cpp
    util/test_file.cpp
    util/test_utils.cpp
)

if (REALM_ENABLE_GEOSPATIAL)
    list(APPEND SOURCES geospatial.cpp)
endif()

file(GLOB RESOURCES RELATIVE ${CMAKE_CURRENT_BINARY_DIR}
    ../*.pem
     *.realm)

if(REALM_ENABLE_AUTH_TESTS)
    list(APPEND SOURCES util/sync/baas_admin_api.cpp)
endif()

if(REALM_ENABLE_SYNC)
    list(APPEND HEADERS
        util/sync/baas_admin_api.hpp
        util/sync/flx_sync_harness.hpp
        util/sync/session_util.hpp
        util/sync/sync_test_utils.hpp
        util/unit_test_transport.hpp
    )
    list(APPEND SOURCES
        bson.cpp
        sync/app.cpp
        sync/client_reset.cpp
        sync/file.cpp
        sync/flx_migration.cpp
        sync/flx_sync.cpp
        sync/metadata.cpp
        sync/migration_store_test.cpp
        sync/remote_mongo_tests.cpp
        sync/session/connection_change_notifications.cpp
        sync/session/progress_notifications.cpp
        sync/session/session.cpp
        sync/session/wait_for_completion.cpp
        sync/sync_manager.cpp
        sync/user.cpp
        util/sync/sync_test_utils.cpp
        util/unit_test_transport.cpp
    )
    if(APPLE)
        list(APPEND SOURCES audit.cpp)
    endif()
endif()

add_executable(ObjectStoreTests ${SOURCES} ${HEADERS} ${RESOURCES})
set_target_properties(ObjectStoreTests PROPERTIES OUTPUT_NAME realm-object-store-tests)
set_target_resources(ObjectStoreTests "${RESOURCES}")

if(MSVC)
    # increase the number of sections supported in an obj file for the heavily templated tests
    target_compile_options(ObjectStoreTests PRIVATE /bigobj)
endif()

target_link_libraries(ObjectStoreTests Catch2::Catch2 ObjectStore RealmFFIStatic)
enable_stdfilesystem(ObjectStoreTests)
create_coverage_target(generate-coverage ObjectStoreTests)

# add_bundled_test(ObjectStoreTests)
add_labeled_test(ObjectStoreTests-local objstore-local ObjectStoreTests "~[baas]")

# Baas server and custom tests are only allowed when REALM_ENABLE_AUTH_TESTS is set
if(REALM_ENABLE_SYNC)
    if(REALM_ENABLE_AUTH_TESTS)
        set(OBJSTORE_TESTS_FILE "${CMAKE_CURRENT_SOURCE_DIR}/objstore-tests.do-not-commit")
        # Rebuild cmake config if the tests file changes
        set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${OBJSTORE_TESTS_FILE})
        # Extract the list of entries from the tests file
        parse_list_file("${OBJSTORE_TESTS_FILE}" OBJSTORE_TEST_LIST)
        # Convert the list from tests file, if any, to object store tests for each line
        if (OBJSTORE_TEST_LIST)
            # Copy the tests file to the build directory to indicate custom tests are set
            file(COPY ${OBJSTORE_TESTS_FILE} DESTINATION ${CMAKE_BINARY_DIR})
            set(_test_cnt 1)
            foreach(_test_item ${OBJSTORE_TEST_LIST})
                message(STATUS "Adding ObjectStoreTests-${_test_cnt}: 'realm-object-store-tests ${_test_item}'")
                separate_arguments(_test_args NATIVE_COMMAND "${_test_item}")
                add_labeled_test("ObjectStoreTests-${_test_cnt}" "objstore-baas" ObjectStoreTests ${_test_args})
                MATH(EXPR _test_cnt "${_test_cnt}+1")
            endforeach()
        else()
            add_labeled_test(ObjectStoreTests-baas objstore-baas ObjectStoreTests "[baas]")
        endif()
    endif()
endif()

if(REALM_ENABLE_SYNC)
    target_link_libraries(ObjectStoreTests SyncServer)
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

if(REALM_TEST_LOGGING)
    target_compile_definitions(ObjectStoreTests PRIVATE
        TEST_ENABLE_LOGGING=1
    )

    if(REALM_TEST_LOGGING_LEVEL)
        message(STATUS "Test logging level: ${REALM_TEST_LOGGING_LEVEL}")
        target_compile_definitions(ObjectStoreTests PRIVATE
            TEST_LOGGING_LEVEL=${REALM_TEST_LOGGING_LEVEL}
        )
    else()
        message(STATUS "Test logging enabled")
    endif()
endif()

target_include_directories(ObjectStoreTests PRIVATE
    ${CATCH_INCLUDE_DIR}
    ${JSON_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/..)

# on Apple platforms we use the built-in CFRunLoop
# on WebAssembly we use an Emscripten-specific Scheduler and runloop
# everywhere else it's libuv, except UWP where it doesn't build
if(NOT APPLE AND NOT EMSCRIPTEN AND NOT WINDOWS_STORE)
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
        # Don't use FetchContent_MakeAvailable since it wants to build libuv.so as well
        FetchContent_Populate(libuv)
        add_subdirectory(${libuv_SOURCE_DIR} ${libuv_BINARY_DIR} EXCLUDE_FROM_ALL)
        set(libuv_target uv_a)
    endif()

    target_link_libraries(ObjectStoreTests ${libuv_target})
    target_compile_definitions(ObjectStoreTests PRIVATE TEST_SCHEDULER_UV=1)

    if (MSVC)
        get_target_property(comp_opts ${libuv_target} COMPILE_OPTIONS)
        string(REGEX REPLACE "/W[1234]" "/W1" comp_opts "${comp_opts}")
        set_target_properties(${libuv_target} PROPERTIES COMPILE_OPTIONS "${comp_opts}")
    endif()
endif()

add_subdirectory(notifications-fuzzer)
add_subdirectory(benchmarks)
