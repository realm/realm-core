add_executable(realm-benchmark-crud main.cpp)
target_link_libraries(realm-benchmark-crud ${PLATFORM_LIBRARIES} TestUtil)
add_test(RealmBenchmarkCrud realm-benchmark-crud)
