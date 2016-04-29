cmake_minimum_required(VERSION 3.5.1)

project(benchmark CXX)

add_definitions(-Werror -Wextra -Wall -pedantic -std=c++11)

################
## Core Build ##
################

set(REF "master")
# set(REF "ea310804")
# Somewhere on the newdate branch
# $ git show ea310804
# commit ea31080482ba3aa7820a22deff8ab47b6c9e9a5f
# Merge: 6dd9059 2737c8f
# Author: Daniel Povlsen <daniel.povlsen@gmail.com>
# Date:   Thu Apr 28 13:36:27 2016 +0200
#
#    Merge pull request #1737 from realm/dp/newdate/negative_nanoseconds
#    
#    Allow negative nanoseconds


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
