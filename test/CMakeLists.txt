add_subdirectory(util)

file(GLOB NORMAL_TESTS "test_*.cpp")
file(GLOB LARGE_TESTS "large_tests/*.cpp")
set(FUZZY_TESTS fuzz_group.cpp)

set(TESTS ${NORMAL_TESTS} ${LARGE_TESTS} ${FUZZY_TESTS})

if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  include(Findprocps)
  set(EXTRA_TEST_LIBS ${PROCPS_LIBRARY} rt)
  set(EXTRA_TEST_CFLAGS ${PROCPS_INCLUDE_DIR})
endif()

# FIXME: Resources
# FIXME: Benchmarks

add_executable(realm-tests ${TESTS} main.cpp)

target_link_libraries(
  realm-tests
  realm test-util
  ${EXTRA_TEST_LIBS}
)

target_include_directories(
  realm-tests
  PUBLIC ${EXTRA_TEST_CFLAGS}
)

add_test(RealmTests realm-tests)


set(BENCHMARKS benchmark-common-tasks/main.cpp)
add_executable(realm-benchmark-common-tasks ${BENCHMARKS})
target_link_libraries(realm-benchmark-common-tasks realm test-util)
add_test(RealmBenchmarkCommonTasks realm-benchmark-common-tasks)

# FIXME: Add other benchmarks

