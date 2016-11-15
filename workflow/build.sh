#!/bin/sh

flavor=${1:-linux}

nprocs=4
if [ "$(uname)" = "Linux" ]; then
  nprocs=$(grep -c ^processor /proc/cpuinfo)
fi

set -e

rm -rf ci.build
mkdir -p ci.build
cd ci.build

cmake_flags=""
if [ ${flavor} = "android" ]; then
  # we're not yet able to run unit tests for android
  cmake_flags="-DREALM_PLATFORM=Android -DANDROID_NDK=/opt/android-ndk"
fi

cmake ${cmake_flags} ..
make VERBOSE=1 -j${nprocs}
