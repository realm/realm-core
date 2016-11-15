#!/bin/sh

# This script is used by CI to build for a specific flavor.  It can be used
# locally: `./workspace/build.sh [linux|android]`

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
  cmake_flags="-DREALM_PLATFORM=Android -DANDROID_NDK=/opt/android-ndk"
fi

cmake ${cmake_flags} ..
make VERBOSE=1 -j${nprocs}
