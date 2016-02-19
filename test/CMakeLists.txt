add_subdirectory(util)

file(GLOB NORMAL_TESTS "test_*.cpp")
file(GLOB LARGE_TESTS "large_tests/*.cpp")
set(FUZZY_TESTS fuzz_group.cpp)

set(TESTS ${NORMAL_TESTS} ${LARGE_TESTS} ${FUZZY_TESTS})

# FIXME: Resources
# FIXME: Benchmarks
# FIXME: Fuzzy tests

add_executable(realm-tests ${TESTS} main.cpp)
target_link_libraries(realm-tests realm test-util)

add_test(RealmTests realm-tests)

