cmake_minimum_required(VERSION 3.5.1)

project(benchmark-datetime CXX)

################
## Core Build ##
################

set(TAG v0.97.3)
set(FULLTAG "tags/${TAG}")

add_custom_target(
  core-build
  cd ../.. && ./util/build-core.sh "${FULLTAG}"
)

include_directories("../core-builds/${FULLTAG}/include")
link_directories("../core-builds/${FULLTAG}/lib")

################
## Executable ##
################

add_definitions(-Werror -Wextra -Wall -pedantic -std=c++11)
add_executable(bench.bin main.cpp)

add_dependencies(bench.bin core-build)
target_link_libraries(bench.bin realm)
