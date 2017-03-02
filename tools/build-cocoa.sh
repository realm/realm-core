#!/usr/bin/env bash

#Set Script Name variable
SCRIPT=$(basename "${BASH_SOURCE[0]}")
DIR=$(dirname "${BASH_SOURCE[0]}")

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
while getopts ":bme:" opt; do
    case "${opt}" in
        b) BUILD=1;;
        m) MACOS_ONLY=1;;
        a) ALL_PLATFORMS=1;;
        c) COPY=1
           DESTINATION=${OPTARG};;
        *) usage;;
    esac
done

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
                      -G Xcode ..
                cmake --build . --config "${bt}" --target package
            )
        done
    done
fi



