#!/usr/bin/env bash

set -e

#Set Script Name variable
SCRIPT=$(basename "${BASH_SOURCE[0]}")

function usage {
    echo "Usage: ${SCRIPT} [-b] [-m] [-c <realm-cocoa-folder>]"
    echo ""
    echo "Arguments:"
    echo "   -b : build from source. If absent it will expect prebuilt packages"
    echo "   -m : build for macOS only"
    echo "   -c : copy core to the specified folder"
    exit 1;
}

# Parse the options
while getopts ":bmc:" opt; do
    case "${opt}" in
        b) BUILD=1;;
        m) MACOS_ONLY=1;;
        c) COPY=1
           DESTINATION=${OPTARG};;
        *) usage;;
    esac
done

shift $((OPTIND-1))

BUILD_TYPES=( Release MinSizeDebug )
if [ -z "${MACOS_ONLY}" ]; then
    PLATFORMS=( macos ios watchos tvos )
else
    PLATFORMS=( macos )
fi

if [ ! -z "$BUILD" ]; then
    for bt in "${BUILD_TYPES[@]}"; do
        for p in "${PLATFORMS[@]}"; do
            folder_name="build-${p}-${bt}"
            mkdir -p "${folder_name}"
            (
                cd "${folder_name}" || exit 1
                cmake -D CMAKE_TOOLCHAIN_FILE="../tools/cmake/${p}.toolchain.cmake" \
                      -D CMAKE_BUILD_TYPE="${bt}" \
                      -G Xcode ..
                cmake --build . --config "${bt}" --target package
            )
        done
    done
fi

rm -rf core
mkdir core

tar -C core -Jxvf "build-macos-Release/realm-core-Release-$(git describe)-Darwin-devel.tar.xz" include LICENSE CHANGELOG.md

for bt in "${BUILD_TYPES[@]}"; do
    [ "$bt" = "Release" ] && suffix="" || suffix="-dbg"
    for p in "${PLATFORMS[@]}"; do
        [ "$p" = "macos" ] && infix="macosx" || infix="${p}"
        filename=$(find "build-${p}-${bt}" -maxdepth 1 -type f -name "realm-core-*-devel.tar.xz")
        tar -C core -Jxvf "${filename}" "lib/librealm${suffix}.a"
        mv "core/lib/librealm${suffix}.a" "core/librealm-${infix}${suffix}.a"
        rm -rf core/lib
    done
done

ln -s core/librealm-macosx.a core/librealm.a
ln -s core/librealm-macosx-dbg.a core/librealm-dbg.a

if [ ! -z "${COPY}" ]; then
    rm -rf "${DESTINATION}/core"
    cp -R core "${DESTINATION}"
fi

rm -f "realm-core-cocoa-$(git describe).tar.xz"
tar -cJvf "realm-core-cocoa-$(git describe).tar.xz" core
