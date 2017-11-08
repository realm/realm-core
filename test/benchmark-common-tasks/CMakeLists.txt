add_executable(BenchmarkCommonTasks EXCLUDE_FROM_ALL main.cpp compatibility.cpp compatibility.hpp)
set_target_properties(BenchmarkCommonTasks PROPERTIES OUTPUT_NAME realm-benchmark-common-tasks)
target_link_libraries(BenchmarkCommonTasks ${PLATFORM_LIBRARIES} TestUtil)
add_test(BenchmarkCommonTasks BenchmarkCommonTasks)

add_executable(RealmStats EXCLUDE_FROM_ALL stats.cpp)
set_target_properties(RealmStats PROPERTIES OUTPUT_NAME realm-stats)
target_link_libraries(RealmStats ${PLATFORM_LIBRARIES} Core)
file(COPY "collect_stats.py" DESTINATION ${RealmCore_BINARY_DIR})

