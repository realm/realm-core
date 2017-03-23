add_executable(realm-benchmark-common-tasks main.cpp compatibility.cpp)
target_link_libraries(realm-benchmark-common-tasks ${PLATFORM_LIBRARIES} test-util)
add_test(RealmBenchmarkCommonTasks realm-benchmark-common-tasks)
