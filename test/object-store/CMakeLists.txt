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

file(GLOB RESOURCES "*.realm")

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

add_bundled_test(ObjectStoreTests)

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
