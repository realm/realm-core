#!/bin/bash
CURRENT_DIR=$(pwd)
PROJECT_DIR=$(git rev-parse --show-toplevel)
MAX_NODE_SIZE=$1
if [ -z ${MAX_NODE_SIZE} ]; then MAX_NODE_SIZE=1000; fi
cd ${PROJECT_DIR}
CORE_VERSION=$(grep REALM_CORE_VERSION dependencies.list | cut -d '=' -f 2 | tr -d '\n')
rm -rf html
mkdir -p build.cov
cd build.cov/
if [ ! -f realm-core-v${CORE_VERSION}/lib64/cmake/realm/realm-config.cmake ]; then
  mkdir -p realm-core-v${CORE_VERSION}
  curl -SL https://static.realm.io/downloads/core/realm-core-Debug-v${CORE_VERSION}-Linux-devel.tar.gz | tar -zxC realm-core-v${CORE_VERSION}
fi
cmake -DOPENSSL_ROOT_DIR=/usr/local -DREALM_COVERAGE=ON -D REALM_CORE_BUILDTREE=realm-core-v${CORE_VERSION}/lib64/cmake/realm -DCMAKE_BUILD_TYPE=Debug -G Ninja ..
if [ ! -f ../lcov-base.info ]; then
  find -name '*.gc*' -delete
  ninja clean
fi
ninja SyncTests
cd ${PROJECT_DIR}
if [ ! -f lcov-base.info ]; then
  lcov --no-external --capture --initial --directory . --output-file ${PROJECT_DIR}/lcov-base.info
fi
cd build.cov/test/
./realm-sync-tests 
cd ${PROJECT_DIR}
lcov --no-external --directory . --capture --output-file ${PROJECT_DIR}/lcov-test.info
lcov --add-tracefile ${PROJECT_DIR}/lcov-base.info --add-tracefile ${PROJECT_DIR}/lcov-test.info --output-file ${PROJECT_DIR}/lcov-total.info
lcov --remove ${PROJECT_DIR}/lcov-total.info  "*/test/*" "*realm-core*" "*external*" --output-file ${PROJECT_DIR}/coverage.info
cd ${CURRENT_DIR}
