add_executable(BenchmarkCrud EXCLUDE_FROM_ALL main.cpp)
add_executable(realm-benchmark-crud ALIAS BenchmarkCrud) # TODO: Remove when scripts are updated
set_target_properties(BenchmarkCrud PROPERTIES OUTPUT_NAME realm-benchmark-crud)
target_link_libraries(BenchmarkCrud ${PLATFORM_LIBRARIES} TestUtil)
add_test(BenchmarkCrud BenchmarkCrud)
