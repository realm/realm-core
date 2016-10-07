add_subdirectory(util)

file(GLOB NORMAL_TESTS "test_*.cpp")
file(GLOB LARGE_TESTS "large_tests/*.cpp")
file(GLOB FUZZY_TESTS "fuzz_*.cpp")

set(TESTS ${NORMAL_TESTS} ${LARGE_TESTS} ${FUZZY_TESTS})

if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  include(Findprocps)
  set(EXTRA_TEST_LIBS ${PROCPS_LIBRARY} rt)
  set(EXTRA_TEST_CFLAGS ${PROCPS_INCLUDE_DIR})
endif()

# Resources required for running the tests
file(GLOB REQUIRED_TEST_FILES
     "*.json"
     "*.realm"
     "expect_string.txt")
file(COPY ${REQUIRED_TEST_FILES}
     DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

# FIXME: Benchmarks

add_executable(realm-tests ${TESTS} main.cpp)

target_link_libraries(
  realm-tests
  realm test-util
  ${EXTRA_TEST_LIBS}
  ${PLATFORM_LIBRARIES}
)

include_directories(${PROJECT_BINARY_DIR}/libcrypto/include)

target_include_directories(
  realm-tests
  PUBLIC ${EXTRA_TEST_CFLAGS}
)

add_test(
  NAME RealmTests
  COMMAND realm-tests
)

set(BENCHMARKS benchmark-common-tasks/main.cpp)
add_executable(realm-benchmark-common-tasks ${BENCHMARKS})
target_link_libraries(realm-benchmark-common-tasks realm test-util)
if(ANDROID)
  target_link_libraries(realm-benchmark-common-tasks log android)
endif()
add_test(RealmBenchmarkCommonTasks realm-benchmark-common-tasks)

# FIXME: Add other benchmarks
