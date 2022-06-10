add_executable(realm-benchmark-common-tasks main.cpp compatibility.cpp)
target_link_libraries(realm-benchmark-common-tasks TestUtil)
add_dependencies(benchmarks realm-benchmark-common-tasks)

