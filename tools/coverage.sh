#!/bin/bash
CURRENT_DIR=$(pwd)
PROJECT_DIR=$(git rev-parse --show-toplevel)
MAX_NODE_SIZE=$1
if [ -z ${MAX_NODE_SIZE} ]; then MAX_NODE_SIZE=1000; fi
cd ${PROJECT_DIR}
rm -rf html
mkdir -p build.cov
cd build.cov/
echo cmake -G Ninja -D REALM_COVERAGE=ON -DREALM_MAX_BPNODE_SIZE=${MAX_NODE_SIZE} ..
cmake -G Ninja -D REALM_COVERAGE=ON -DREALM_MAX_BPNODE_SIZE=${MAX_NODE_SIZE} ..
if [ ! -f lcov-base.info ]; then
  find -name '*.gc*' -delete
  ninja clean
fi
ninja realm-tests
cd ${PROJECT_DIR}
if [ ! -f lcov-base.info ]; then
  lcov --no-external --capture --initial --directory . --output-file ${PROJECT_DIR}/lcov-base.info
fi
build.cov/test/realm-tests 
lcov --no-external --directory . --capture --output-file ${PROJECT_DIR}/lcov-test.info
lcov --add-tracefile ${PROJECT_DIR}/lcov-base.info --add-tracefile ${PROJECT_DIR}/lcov-test.info --output-file ${PROJECT_DIR}/lcov-total.info
lcov --remove ${PROJECT_DIR}/lcov-total.info  "*/test/*" "*external*" --output-file ${PROJECT_DIR}/coverage.info
cd ${CURRENT_DIR}
