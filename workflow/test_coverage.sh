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

cmake_flags="-DCMAKE_BUILD_TYPE=Coverage"
if [ ${flavor} = "android" ]; then
  cmake_flags="-DREALM_PLATFORM=Android -DANDROID_NDK=/opt/android-ndk"
fi

cmake -DCMAKE_BUILD_TYPE=Coverage ${cmake_flags} ..
make VERBOSE=1 -j${nprocs} generate-coverage-cobertura
