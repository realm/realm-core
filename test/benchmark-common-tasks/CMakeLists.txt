add_executable(realm-benchmark-common-tasks main.cpp)
target_link_libraries(realm-benchmark-common-tasks TestUtil QueryParser)
add_dependencies(benchmarks realm-benchmark-common-tasks)

