#!/usr/bin/env bash

set -e -x

readonly build_dir="$PWD"
readonly dmg_file="$build_dir/exfat.dmg"

build_prefix="not found"
if [ -f "$build_dir/test/realm-tests.app/Contents/MacOS/realm-tests" ]; then
    build_prefix="$build_dir/test/"
elif [ -f "$build_dir/test/Release/realm-tests.app/Contents/MacOS/realm-tests" ]; then
    build_prefix="$build_dir/test/Release/"
elif [ -f "$build_dir/test/Debug/realm-tests.app/Contents/MacOS/realm-tests" ]; then
    build_prefix="$build_dir/test/Debug/"
else
    echo 'Run this script from the build directory after building tests'
    exit 1
fi

echo "using path prefix $build_prefix"

function cleanup() {
    rm -f "$dmg_file"
    if [ -n "$device" ]; then
        hdiutil detach "$device"
    fi
}
trap cleanup EXIT

rm -f "$dmg_file"
hdiutil create -fs exFAT -size 400MB "$dmg_file"
hdiutil_out=$(hdiutil attach exfat.dmg)
device=$(echo "$hdiutil_out" | head -n1 | cut -f1 | awk '{$1=$1};1')
path=$(echo "$hdiutil_out" | tail -n1 | cut -f3)

UNITTEST_ENABLE_SYNC_TO_DISK=1 "$build_prefix/realm-tests.app/Contents/MacOS/realm-tests" "$path/"
# one test runner because several sync tests make large uploads which if run together may exceed our 400MB space limit
UNITTEST_THREADS=1 UNITTEST_ENABLE_SYNC_TO_DISK=1 "$build_prefix/realm-sync-tests.app/Contents/MacOS/realm-sync-tests" "$path/"
echo "finished running tests"

