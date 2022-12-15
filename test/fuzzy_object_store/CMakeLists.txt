set(TEST_LIBFUZZER_SOURCES
    libfuzzer_runner.cpp
    fuzz_engine.cpp
    fuzz_object.cpp
    fuzz_configurator.cpp
) # TEST_LIBFUZZER_SOURCES_OBJECT_STORE

file(GLOB FUZZER_RUN_SCRIPTS
    "scripts/start_lib_fuzzer.sh")

file(COPY ${FUZZER_RUN_SCRIPTS}
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

# on Apple platforms we use the built-in CFRunLoop
# everywhere else it's libuv, except UWP where it doesn't build
if(NOT APPLE AND NOT WINDOWS_STORE)
    find_package(LibUV REQUIRED)
    set(libuv_target LibUV::LibUV)
endif()
 
if(REALM_LIBFUZZER)
    if(${CMAKE_CXX_COMPILER_ID} MATCHES "Clang")
        add_executable(realm-libfuzz ${TEST_LIBFUZZER_SOURCES})
        target_include_directories(realm-libfuzz PRIVATE ${libuv_include_dir})
        target_link_libraries(realm-libfuzz TestUtil ObjectStore ${libuv_target})        
    endif()
endif()
