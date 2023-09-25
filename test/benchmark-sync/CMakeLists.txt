if(REALM_ENABLE_SYNC)
    add_executable(realm-benchmark-sync bench_transform.cpp ../test_all.cpp)
    add_dependencies(benchmarks realm-benchmark-sync)
    # Sync lib is included with SyncServer
    target_link_libraries(realm-benchmark-sync TestUtil SyncServer)
endif()