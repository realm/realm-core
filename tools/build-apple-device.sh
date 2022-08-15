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
VERSION="$(git describe)"

function usage {
    echo "Usage: ${SCRIPT} -p <platform> [-c <configuration>] [-v <version>] [-f <cmake-flags>]"
    echo ""
    echo "Supported platforms:"
    echo "  iphoneos iphonesimulator"
    echo "  tvos tvsimulator"
    echo "  watchos watchsimulator"
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
        v) VERSION="${OPTARG}";;
        f) CMAKE_FLAGS=${OPTARG};;
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

mkdir -p build-xcode-platforms
cd build-xcode-platforms
cmake \
    -D CMAKE_TOOLCHAIN_FILE="../tools/cmake/xcode.toolchain.cmake" \
    -D REALM_VERSION="${VERSION}" \
    -D REALM_BUILD_LIB_ONLY=ON \
    -D CPACK_PACKAGE_DIRECTORY=.. \
    ${CMAKE_FLAGS} \
    -G Xcode \
    ..

xcodebuild -scheme ALL_BUILD -configuration ${buildType} -destination "${buildDestination}"

function combine_library() {
    local path="$1"
    local target="$2"
    local library="$3"
    local out_path="$path/${buildType}-${platform}/${library}"

    # Building just arm64 results in the library being created directly in the
    # final output location rather than in the per-arch directories, but lipo
    # can't operate in-place so we need to move it aside.
    mv "$out_path" "${out_path}.tmp"
    lipo -create -output "$out_path" \
        "${out_path}.tmp" \
        "${path}/RealmCore.build/${buildType}-${platform}/${target}.build/Objects-normal/armv7k/Binary/${library}" \
        "${path}/RealmCore.build/${buildType}-${platform}/${target}.build/Objects-normal/arm64_32/Binary/${library}"
    rm "${out_path}.tmp"
}

if [ -n "$XCODE_14_DEVELOPER_DIR" ] && [ "$platform" == "watchos" ]; then
    DEVELOPER_DIR="$XCODE_14_DEVELOPER_DIR" xcodebuild -scheme ALL_BUILD -configuration ${buildType} -sdk watchos -arch arm64
    [[ "$buildType" = "Release" ]] && suffix="" || suffix="-dbg"
    combine_library src/realm/object-store ObjectStore "librealm-object-store${suffix}.a"
    combine_library src/realm/object-store/c_api RealmFFIStatic "librealm-ffi-static${suffix}.a"
    combine_library src/realm/parser QueryParser "librealm-parser${suffix}.a"
    combine_library src/realm/sync Sync "librealm-sync${suffix}.a"
    combine_library src/realm Storage "librealm${suffix}.a"

    # The bid library has a different directory structure from the other ones
    prefix="src/external/IntelRDFPMathLib20U2/RealmCore.build/${buildType}"
    out_path="$prefix/Bid.build/libBid.a"
    mv "$out_path" "${out_path}.tmp"
    lipo -create -output "$out_path" \
        "${out_path}.tmp" \
        "${prefix}-watchos/Bid.build/Objects-normal/armv7k/Binary/libBid.a" \
        "${prefix}-watchos/Bid.build/Objects-normal/arm64_32/Binary/libBid.a"
    rm "${out_path}.tmp"
fi

PLATFORM_NAME="${platform}" EFFECTIVE_PLATFORM_NAME="-${platform}" cpack -C "${buildType}"
