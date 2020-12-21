add_executable(realm-benchmark-crud main.cpp)
add_dependencies(benchmarks realm-benchmark-crud)
target_link_libraries(realm-benchmark-crud TestUtil)
