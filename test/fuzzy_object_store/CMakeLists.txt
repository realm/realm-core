# the aim is to try to reproduce the corruption bug we have been seeing.
set(TEST_AFL_SOURCES
    afl_runner.cpp
    fuzz_engine.cpp
    fuzz_object.cpp
    fuzz_configurator.cpp
) # TEST_AFL_SOURCES_OBJECT_STORE

set(TEST_LIBFUZZER_SOURCES
    libfuzzer_runner.cpp
    fuzz_engine.cpp
    fuzz_object.cpp
    fuzz_configurator.cpp
) # TEST_LIBFUZZER_SOURCES_OBJECT_STORE

file(GLOB AFL_RUN_SCRIPTS
    "scripts/start_fuzz_afl.sh"
    "scripts/start_lib_fuzzer.sh")

file(COPY ${AFL_RUN_SCRIPTS}
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(GLOB AFL_SEEDS "testcases/*")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testcases)
file(COPY ${AFL_SEEDS}
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/testcases)

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

        # Don't use FetchContent_MakeAvailable since it wants to build libuv.so as well
        FetchContent_Populate(libuv)
        add_subdirectory(${libuv_SOURCE_DIR} ${libuv_BINARY_DIR} EXCLUDE_FROM_ALL)
        set(libuv_target uv_a)
    endif()
endif()

add_executable(realm-afl++ ${TEST_AFL_SOURCES})
target_link_libraries(realm-afl++ TestUtil ObjectStore ${libuv_target}) 

if(REALM_LIBFUZZER)
    if(${CMAKE_CXX_COMPILER_ID} MATCHES "Clang")
        add_executable(realm-libfuzz ${TEST_LIBFUZZER_SOURCES})
        target_link_libraries(realm-libfuzz TestUtil ObjectStore ${libuv_target})
    endif()
endif()
