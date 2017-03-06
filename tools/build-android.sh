#!/usr/bin/env bash

set -e

#Set Script Name variable
SCRIPT=$(basename "${BASH_SOURCE[0]}")

function usage {
    echo "Usage: ${SCRIPT} [-b] [-a]"
    echo ""
    echo "Arguments:"
    echo "   -b : build from source. If absent it will expect prebuilt packages"
    echo "   -a : build for arm-v7a only"
    exit 1;
}

# Parse the options
while getopts ":ba" opt; do
    case "${opt}" in
        b) BUILD=1;;
        a) ARM_ONLY=1;;
        *) usage;;
    esac
done

shift $((OPTIND-1))

BUILD_TYPES=( Release Debug )
[[ -z $ARM_ONLY ]] && PLATFORMS=( armeabi armeabi-v7a x86 mips x86_64 arm64-v8a ) || PLATFORMS=( armeabi-v7a )

if [[ ! -z $BUILD ]]; then
    for bt in "${BUILD_TYPES[@]}"; do
        for p in "${PLATFORMS[@]}"; do
            tools/cross_compile.sh -o android -a "${p}" -t "${bt}" -v "$(git describe)"
        done
    done
fi

rm -rf core-android
mkdir core-android

filename=$(find "build-android-armeabi-v7a-Release" -maxdepth 1 -type f -name "realm-core-Release-*-devel.tar.gz")
tar -C core-android -zxvf "${filename}" include LICENSE CHANGELOG.md

for bt in "${BUILD_TYPES[@]}"; do
    [[ "$bt" = "Release" ]] && suffix="" || suffix="-dbg"
    for p in "${PLATFORMS[@]}"; do
        filename=$(find "build-android-${p}-${bt}" -maxdepth 1 -type f -name "realm-core-*-devel.tar.gz")
        tar -C core-android -zxvf "${filename}" "lib/librealm${suffix}.a"
        mkdir -p "core-android/lib/${p}"
        mv "core-android/lib/librealm${suffix}.a" "core-android/lib/${p}/librealm${suffix}.a"
    done
done

rm -f "realm-core-android-$(git describe).tar.gz"
tar -czvf "realm-core-android-$(git describe).tar.gz" core-android
