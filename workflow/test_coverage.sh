#!/bin/sh

flavor=${1:-linux}

nprocs=4
if [ "$(uname)" = "Linux" ]; then
  nprocs=$(grep -c ^processor /proc/cpuinfo)
fi

set -e

rm -rf coverage.build
mkdir -p coverage.build
cd coverage.build

if [ ${flavor} = "android" ]; then
  # we're not yet able to run unit tests for android
  cmake -DCMAKE_BUILD_TYPE=Coverage -DREALM_PLATFORM=Android -DANDROID_NDK=/opt/android-ndk ..
  make VERBOSE=1 -j${nprocs}
else
  cmake -DCMAKE_BUILD_TYPE=Coverage ..
  make VERBOSE=1 -j${nprocs} generate-coverage-cobertura
fi
