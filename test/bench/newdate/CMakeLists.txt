cmake_minimum_required(VERSION 3.5.1)

project(benchmark CXX)

add_definitions(-Werror -Wextra -Wall -pedantic -std=c++11)

################
## Core Build ##
################

set(REF "master")

add_custom_target(
  core-build
  cd ../../.. && ./util/build-core.sh "${REF}"
)

include_directories(
    "${CMAKE_CURRENT_LIST_DIR}/../../core-builds/${REF}/include")
link_directories(
    "${CMAKE_CURRENT_LIST_DIR}/../../core-builds/${REF}/lib")

##########
## Util ##
##########

add_subdirectory(
    "${CMAKE_CURRENT_LIST_DIR}/../util"
    "${CMAKE_CURRENT_BINARY_DIR}/util")
include_directories(
    "${CMAKE_CURRENT_LIST_DIR}/../util"
    "${CMAKE_CURRENT_BINARY_DIR}/util")

################
## The Binary ##
################

add_executable(benchmark main.cpp)

add_dependencies(benchmark core-build)
target_link_libraries(benchmark realm)
target_link_libraries(benchmark util)
