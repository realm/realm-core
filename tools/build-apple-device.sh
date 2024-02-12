#!/usr/bin/env bash

set -eo pipefail

# Make sure SDKROOT variable is not set before building
unset SDKROOT

# If set, make sure the DEVELOPER_DIR variable is set to a valid directory
if [[ -n "${DEVELOPER_DIR}" && ! -d "${DEVELOPER_DIR}" ]]; then
    echo "error: DEVELOPER_DIR is not a valid directory: ${DEVELOPER_DIR}"
    exit 1
fi

SCRIPT="$(basename "${BASH_SOURCE[0]}")"

function usage {
    echo "Usage: ${SCRIPT} -p <platform> [-c <configuration>] [-v <version>] [-f <cmake-flags>]"
    echo ""
    echo "Supported platforms:"
    echo "  iphoneos iphonesimulator"
    echo "  tvos tvsimulator"
    echo "  watchos watchsimulator"
    echo "  xros xrsimulator"
    echo "  maccatalyst"
    echo ""
    echo "Arguments:"
    echo "   -p : Platform to build"
    echo "   -c : Configuration to build [Debug|Release|RelWithDebugSymbols]"
    echo "   -v : Version string to use"
    echo "   -f : additional configuration flags to pass to cmake"
    exit 1;
}
buildType="Debug"
CMAKE_FLAGS=''
while getopts ":p:c:v:f:" opt; do
    case "${opt}" in
        p) platform="${OPTARG}";;
        c) buildType="${OPTARG}";;
        f) CMAKE_FLAGS="${CMAKE_FLAGS} ${OPTARG}";;
        v) CMAKE_FLAGS="${CMAKE_FLAGS} -DREALM_VERSION=${OPTARG}";;
        *) usage;;
    esac
done
if [ -z "${platform}" ]; then
    usage
    exit 1
fi

buildDestination="generic/platform=${platform}"
if [ "${platform}" == 'maccatalyst' ]; then
    buildDestination='generic/platform=macOS,variant=Mac Catalyst'
fi

sdkroot=iphoneos
if [[ "${platform}" == xr* ]]; then
    sdkroot=xros
fi

mkdir -p build-xcode-platforms
cd build-xcode-platforms
cmake \
    -D CMAKE_TOOLCHAIN_FILE="../tools/cmake/xcode.toolchain.cmake" \
    -D REALM_BUILD_LIB_ONLY=ON \
    -D CPACK_PACKAGE_DIRECTORY=.. \
    -D CMAKE_XCODE_ATTRIBUTE_SDKROOT="$sdkroot" \
    ${CMAKE_FLAGS} \
    -G Xcode \
    ..

xcodebuild -scheme ALL_BUILD -configuration ${buildType} -destination "${buildDestination}"

PLATFORM_NAME="${platform}" EFFECTIVE_PLATFORM_NAME="-${platform}" cpack -C "${buildType}"
