#!/usr/bin/env bash

set -e

#Set Script Name variable
SCRIPT=$(basename "${BASH_SOURCE[0]}")
VERSION=$(git describe)

function usage {
    echo "Usage: ${SCRIPT} [-b] [-m] [-c <realm-cocoa-folder>]"
    echo ""
    echo "Arguments:"
    echo "   -b : build from source. If absent it will expect prebuilt packages"
    echo "   -m : build for macOS only"
    echo "   -c : copy core to the specified folder instead of packaging it"
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
[[ -z $MACOS_ONLY ]] && PLATFORMS=( macosx maccatalyst ios watchos tvos ) || PLATFORMS=( macosx )

function build_macos {
    local platform="$1"
    local bt="$2"
    local folder_name="build-${platform}-${bt}"
    mkdir -p "${folder_name}"
    (
        cd "${folder_name}" || exit 1
        rm -f realm-core-*-devel.tar.gz
        cmake -D CMAKE_TOOLCHAIN_FILE="../tools/cmake/$platform.toolchain.cmake" \
              -D CMAKE_BUILD_TYPE="${bt}" \
              -D REALM_VERSION="${VERSION}" \
              -D REALM_SKIP_SHARED_LIB=ON \
              -D REALM_BUILD_LIB_ONLY=ON \
              -G Ninja ..
        cmake --build . --config "${bt}" --target package
    )
}

if [[ ! -z $BUILD ]]; then
    for bt in "${BUILD_TYPES[@]}"; do
        build_macos macosx "$bt"
    done
    if [[ -z $MACOS_ONLY ]]; then
        for bt in "${BUILD_TYPES[@]}"; do
            build_macos maccatalyst "$bt"
        done
        for os in ios watchos tvos; do
            for bt in "${BUILD_TYPES[@]}"; do
                tools/cross_compile.sh -o $os -t $bt -v $(git describe)
            done
        done
    fi
fi

rm -rf core
mkdir core

filename="build-macosx-Release/realm-core-Release-${VERSION}-macosx-devel.tar.gz"
tar -C core -zxvf "${filename}" include doc

for bt in "${BUILD_TYPES[@]}"; do
    [[ "$bt" = "Release" ]] && suffix="" || suffix="-dbg"
    for p in "${PLATFORMS[@]}"; do
        filename="build-${p}-${bt}/realm-core-${bt}-${VERSION}-${p}-devel.tar.gz"
        tar -C core -zxvf "${filename}" "lib/librealm${suffix}.a"
        mv "core/lib/librealm${suffix}.a" "core/librealm-${p}${suffix}.a"
        tar -C core -zxvf "${filename}" "lib/librealm-parser${suffix}.a"
        mv "core/lib/librealm-parser${suffix}.a" "core/librealm-parser-${p}${suffix}.a"
        rm -rf core/lib
    done
done

if [[ ! -z $COPY ]]; then
    rm -rf "${DESTINATION}/core"
    mkdir -p "${DESTINATION}"
    cp -R core "${DESTINATION}"
else
    v=$(git describe --tags)
    rm -f "realm-core-cocoa-${v}.tar.xz"
    tar -cJvf "realm-core-cocoa-${v}.tar.xz" core
fi
