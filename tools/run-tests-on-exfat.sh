#!/usr/bin/env bash

set -e

readonly build_dir="$PWD"
readonly dmg_file="$build_dir/exfat.dmg"

if ! [ -f "$build_dir/test/realm-tests" ]; then
    echo 'Run this script from the build directory after building tests'
    exit 1
fi

function cleanup() {
    rm -f "$dmg_file"
    if [ -n "$device" ]; then
        hdiutil detach "$device"
    fi
}
trap cleanup EXIT

rm -f "$dmg_file"
hdiutil create -fs exFAT -size 100MB "$dmg_file"
hdiutil_out=$(hdiutil attach exfat.dmg)
device=$(echo "$hdiutil_out" | head -n1 | cut -f1 | awk '{$1=$1};1')
path=$(echo "$hdiutil_out" | tail -n1 | cut -f3)

./test/realm-tests "$path/"

