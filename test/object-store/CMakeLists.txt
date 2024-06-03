set(HEADERS
    util/event_loop.hpp
    util/index_helpers.hpp
    util/test_file.hpp
    util/test_utils.hpp
    collection_fixtures.hpp
)

set(SOURCES
    # slowest to compile first
    set.cpp

    backup.cpp
    collection_change_indices.cpp
    dictionary.cpp
    frozen_objects.cpp
    index_set.cpp
    list.cpp
    migrations.cpp
    object.cpp
    object_store.cpp
    primitive_list.cpp
    realm.cpp
    results.cpp
    schema.cpp
    sectioned_results.cpp
    test_runner.cpp
    thread_safe_reference.cpp
    transaction_log_parsing.cpp
    uuid.cpp
    nested_collections.cpp
    c_api/c_api.cpp
    c_api/c_api_file_tests.c

    util/event_loop.cpp
    util/test_file.cpp
    util/test_utils.cpp
)

if (REALM_ENABLE_GEOSPATIAL)
    list(APPEND SOURCES geospatial.cpp)
endif()

file(GLOB RESOURCES "*.realm" "../*.pem")

if(REALM_ENABLE_AUTH_TESTS)
    list(APPEND SOURCES util/sync/baas_admin_api.cpp)
endif()

if(REALM_ENABLE_SYNC)
    list(APPEND HEADERS
        util/sync/baas_admin_api.hpp
        util/sync/session_util.hpp
        util/sync/sync_test_utils.hpp
    )
    list(APPEND SOURCES
        bson.cpp
        sync/file.cpp
        sync/session/connection_change_notifications.cpp
        sync/session/progress_notifications.cpp
        sync/session/session.cpp
        sync/session/wait_for_completion.cpp
        sync/sync_manager.cpp
        util/sync/sync_test_utils.cpp
    )

    if (REALM_APP_SERVICES)
        list(APPEND HEADERS
            util/sync/flx_sync_harness.hpp
            util/unit_test_transport.hpp
        )
        list(APPEND SOURCES
            sync/app.cpp
            sync/client_reset.cpp
            sync/flx_migration.cpp
            sync/flx_schema_migration.cpp
            sync/flx_sync.cpp
            sync/metadata.cpp
            sync/migration_store_test.cpp
            sync/remote_mongo_tests.cpp
            util/unit_test_transport.cpp
        )
    endif()

    if(APPLE)
        list(APPEND SOURCES audit.cpp)
    endif()
endif()

set_property(DIRECTORY PROPERTY TEST_RESOURCES "${RESOURCES}")

add_library(ObjectStoreTestLib OBJECT ${SOURCES} ${HEADERS} ${RESOURCES})

if(MSVC)
    # increase the number of sections supported in an obj file for the heavily templated tests
    target_compile_options(ObjectStoreTestLib PRIVATE /bigobj)
endif()

target_link_libraries(ObjectStoreTestLib Catch2::Catch2 ObjectStore RealmFFIStatic TestUtil)
enable_stdfilesystem(ObjectStoreTestLib)

if(REALM_CURL_CACERTS)
  target_compile_definitions(ObjectStoreTestLib PRIVATE
    REALM_CURL_CACERTS="${REALM_CURL_CACERTS}"
  )
endif()

add_executable(ObjectStoreTests main.cpp ${RESOURCES})
set_target_properties(ObjectStoreTests PROPERTIES OUTPUT_NAME realm-object-store-tests)
target_link_libraries(ObjectStoreTests ObjectStoreTestLib TestUtil)
set_target_resources(ObjectStoreTests "${RESOURCES}")
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
    target_link_libraries(ObjectStoreTestLib SyncServer)
    option(REALM_ENABLE_AUTH_TESTS "" OFF)
    if(REALM_ENABLE_AUTH_TESTS)
        if (NOT REALM_APP_SERVICES)
            message(SEND_ERROR "Unsupported configuration: REALM_ENABLE_AUTH_TESTS=ON and REALM_APP_SERVICES=OFF")
        endif()

        target_compile_definitions(ObjectStoreTestLib PRIVATE
            REALM_ENABLE_AUTH_TESTS=1
        )

        if(REALM_MONGODB_ENDPOINT)
          message(STATUS "Auth tests enabled: ${REALM_MONGODB_ENDPOINT}")
          target_compile_definitions(ObjectStoreTestLib PRIVATE
              REALM_MONGODB_ENDPOINT="${REALM_MONGODB_ENDPOINT}"
          )
        endif()

        if(REALM_ADMIN_ENDPOINT)
            message(STATUS "BAAS admin endpoint: ${REALM_ADMIN_ENDPOINT}")
            target_compile_definitions(ObjectStoreTests PRIVATE
                REALM_ADMIN_ENDPOINT="${REALM_ADMIN_ENDPOINT}"
            )
        endif()

        find_package(CURL REQUIRED)
        target_link_libraries(ObjectStoreTestLib "${CURL_LIBRARY}")
        target_include_directories(ObjectStoreTestLib PRIVATE "${CURL_INCLUDE_DIRS}")
    endif()
endif()

if(REALM_TEST_LOGGING)
    target_compile_definitions(ObjectStoreTestLib PRIVATE
        TEST_ENABLE_LOGGING=1
    )

    if(REALM_TEST_LOGGING_LEVEL)
        message(STATUS "Test logging level: ${REALM_TEST_LOGGING_LEVEL}")
        target_compile_definitions(ObjectStoreTestLib PRIVATE
            TEST_LOGGING_LEVEL=${REALM_TEST_LOGGING_LEVEL}
        )
    endif()
    message(STATUS "Test logging enabled")
endif()

# Optional extra time to add to test timeout values
if(REALM_TEST_TIMEOUT_EXTRA)
    target_compile_definitions(ObjectStoreTests PRIVATE
        TEST_TIMEOUT_EXTRA=${REALM_TEST_TIMEOUT_EXTRA}
    )
    message(STATUS "Test wait timeouts extended by ${REALM_TEST_TIMEOUT_EXTRA} seconds")
endif()

target_include_directories(ObjectStoreTestLib PRIVATE
    ${CATCH_INCLUDE_DIR}
    ${JSON_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/..)

# on Apple platforms we use the built-in CFRunLoop
# on WebAssembly we use an Emscripten-specific Scheduler and runloop
# everywhere else it's libuv, except UWP where it doesn't build
if(NOT APPLE AND NOT EMSCRIPTEN AND NOT WINDOWS_STORE AND NOT ANDROID)
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
        set(libUV_Git_TAG "v1.48.0")
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

    set_target_properties(${libuv_target} PROPERTIES COMPILE_WARNING_AS_ERROR Off)
    target_link_libraries(ObjectStoreTestLib ${libuv_target})
    target_compile_definitions(ObjectStoreTestLib PRIVATE TEST_SCHEDULER_UV=1)

    if (MSVC)
        get_target_property(comp_opts ${libuv_target} COMPILE_OPTIONS)
        string(REGEX REPLACE "/W[1234]" "/W1" comp_opts "${comp_opts}")
        set_target_properties(${libuv_target} PROPERTIES COMPILE_OPTIONS "${comp_opts}")
    endif()
endif()

if(NOT EMSCRIPTEN AND NOT WINDOWS_STORE AND NOT ANDROID)
    add_subdirectory(benchmarks)
endif()
