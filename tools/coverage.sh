#!/bin/bash
CURRENT_DIR=$(pwd)
PROJECT_DIR=$(git rev-parse --show-toplevel)
MAX_NODE_SIZE=$1
if [ -z ${MAX_NODE_SIZE} ]; then MAX_NODE_SIZE=1000; fi
cd ${PROJECT_DIR}
rm -rf html
mkdir -p build.cov
cd build.cov/
echo cmake -G Ninja -D REALM_COVERAGE=ON -D CMAKE_CXX_FLAGS=-g -DREALM_MAX_BPNODE_SIZE=${MAX_NODE_SIZE} ..
cmake -G Ninja -D REALM_COVERAGE=ON -D CMAKE_CXX_FLAGS=-g -DREALM_MAX_BPNODE_SIZE=${MAX_NODE_SIZE} ..
ninja realm-tests
cd ${PROJECT_DIR}
if [ ! -f coverage-base.info ]; then
  lcov --no-external --capture --initial --directory . --output-file ./coverage-base.info
fi
cd build.cov/test/
./realm-tests 
cd ${PROJECT_DIR}
lcov --no-external --directory . --capture --output-file ./coverage-test.info
lcov --add-tracefile coverage-base.info --add-tracefile coverage-test.info --output-file ./coverage-total.info
lcov --remove coverage-total.info "/usr/*" "${PROJECT_DIR}/test/*" "${PROJECT_DIR}/src/external/*" --output-file coverage-filtered.info
genhtml coverage-filtered.info --output-directory html
firefox html/index.html > /dev/null 2>&1 &
cd ${CURRENT_DIR}
