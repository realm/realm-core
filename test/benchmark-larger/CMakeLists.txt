add_executable(realm-benchmark-larger main.cpp)
add_dependencies(benchmarks realm-benchmark-larger)
target_link_libraries(realm-benchmark-larger TestUtil)
