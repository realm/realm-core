add_executable(realm-benchmark-larger EXCLUDE_FROM_ALL main.cpp)
add_dependencies(benchmarks realm-benchmark-larger)
target_link_libraries(realm-benchmark-larger TestUtil)
