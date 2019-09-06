add_executable(realm-benchmark-common-tasks main.cpp compatibility.cpp)
target_link_libraries(realm-benchmark-common-tasks ${PLATFORM_LIBRARIES} TestUtil)
add_test(RealmBenchmarkCommonTasks realm-benchmark-common-tasks)

add_executable(realm-stats stats.cpp compatibility.cpp)
target_link_libraries(realm-stats ${PLATFORM_LIBRARIES} Core)
file(COPY "collect_stats.py"
     DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

