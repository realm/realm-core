add_executable(realm-benchmark-common-tasks main.cpp compatibility.cpp)
target_link_libraries(realm-benchmark-common-tasks ${PLATFORM_LIBRARIES} test-util)
add_test(RealmBenchmarkCommonTasks realm-benchmark-common-tasks)

add_executable(realm-stats stats.cpp)
target_link_libraries(realm-stats ${PLATFORM_LIBRARIES} realm)
file(COPY "collect_stats.py"
     DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

