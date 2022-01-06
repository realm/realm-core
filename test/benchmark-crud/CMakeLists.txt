add_executable(BenchmarkCrud EXCLUDE_FROM_ALL main.cpp)
set_target_properties(BenchmarkCrud PROPERTIES OUTPUT_NAME realm-benchmark-crud)
target_link_libraries(BenchmarkCrud ${PLATFORM_LIBRARIES} TestUtil)
add_test(BenchmarkCrud BenchmarkCrud)
