#!/bin/sh

# This script is used by CI to test coverage for a specific flavor.  It can be
# used locally: `./workspace/test_coverage.sh`

nprocs=4
if [ "$(uname)" = "Linux" ]; then
  nprocs=$(grep -c ^processor /proc/cpuinfo)
fi

set -e

rm -rf coverage.build
mkdir -p coverage.build
cd coverage.build

cmake -DCMAKE_BUILD_TYPE=Coverage ..
make VERBOSE=1 -j${nprocs} generate-coverage-cobertura
