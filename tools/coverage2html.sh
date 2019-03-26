#!/bin/bash
CURRENT_DIR=$(pwd)
PROJECT_DIR=$(git rev-parse --show-toplevel)
cd ${PROJECT_DIR}
genhtml coverage.info --output-directory html
firefox html/index.html > /dev/null 2>&1 &
cd ${CURRENT_DIR}

