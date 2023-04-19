set(TEST_CLIENT_SOURCES
    auth.cpp
    peer.cpp
    main.cpp
)

add_executable(TestClient ${TEST_CLIENT_SOURCES})
set_target_properties(TestClient PROPERTIES OUTPUT_NAME "realm-test-client")
target_link_libraries(TestClient Sync)
if (REALM_BUILD_DOGLESS)
    target_link_libraries(TestClient Dogless)
endif()
