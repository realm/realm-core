#!/bin/bash
CURRENT_DIR=$(pwd)
PROJECT_DIR=$(git rev-parse --show-toplevel)
MAX_NODE_SIZE=$1
if [ -z ${MAX_NODE_SIZE} ]; then MAX_NODE_SIZE=1000; fi
cd ${PROJECT_DIR}
mkdir -p build.cov
cd build.cov/
if [ ! -f CMakeCache.txt ]; then
  echo cmake -G Ninja -D REALM_COVERAGE=ON -D CMAKE_CXX_FLAGS=-g -DREALM_MAX_BPNODE_SIZE=${MAX_NODE_SIZE} ..
  cmake -G Ninja -D REALM_COVERAGE=ON -D CMAKE_CXX_FLAGS=-g -DREALM_MAX_BPNODE_SIZE=${MAX_NODE_SIZE} ..
fi
ninja

if [ ! -f coverage-base.info ]; then
  lcov --capture --initial --directory src --output-file ./coverage-base.info
fi
ctest 
lcov --directory src --capture --output-file ./coverage-test.info
lcov --add-tracefile coverage-base.info --add-tracefile coverage-test.info --output-file coverage-total.info
lcov --remove coverage-total.info "/usr/*" "*/external/*" "*/generated/*" "*query_flex*" --output-file coverage-filtered.info
genhtml coverage-filtered.info
firefox index.html > /dev/null 2>&1 &
cd ${CURRENT_DIR}
