#!/bin/bash
CURRENT_DIR=$(pwd)
PROJECT_DIR=$(git rev-parse --show-toplevel)
cd ${PROJECT_DIR}
rm -f coverage-*
rm -rf build.cov
rm -rf html
mkdir build.cov
cd build.cov/
cmake -G Ninja -D REALM_COVERAGE=ON -D CMAKE_CXX_FLAGS=-g ..
ninja realm-tests
cd ${PROJECT_DIR}
lcov --no-external --capture --initial --directory . --output-file ./coverage-base.info
cd build.cov/test/
./realm-tests 
cd ${PROJECT_DIR}
lcov --no-external --directory . --capture --output-file ./coverage-test.info
lcov --add-tracefile coverage-base.info --add-tracefile coverage-test.info --output-file ./coverage-total.info
lcov --remove coverage-total.info "/usr/*" "${PROJECT_DIR}/test/*" "${PROJECT_DIR}/src/external/*" --output-file coverage-filtered.info
genhtml coverage-filtered.info --output-directory html
firefox html/index.html > /dev/null 2>&1 &
cd ${CURRENT_DIR}
