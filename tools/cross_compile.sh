#!/usr/bin/env bash

set -e

#Set Script Name variable
SCRIPT=$(basename "${BASH_SOURCE[0]}")

function usage {
    echo "Usage: ${SCRIPT} -t <build_type> -o <target_os> [-v <version>] [-a <android_abi>] [-f <cmake_flags>]"
    echo ""
    echo "Arguments:"
    echo "   build_type     Release|Debug|MinSizeDebug|RelWithDebInfo"
    echo "   target_os      android|iphoneos|iphonesimulator|watchos|"
    echo "                  watchsimulator|appletvos|appletvsimulator|"
    echo "                  emscripten"
    echo "   android_abi    armeabi-v7a|x86|x86_64|arm64-v8a"
    exit 1;
}

# Variables
VERSION=
CMAKE_FLAGS=
EMCMAKE=emcmake
CMAKE=${CMAKE:-cmake}

# Parse the options
while getopts ":o:a:t:v:f:" opt; do
    case "${opt}" in
        o)
            OS=${OPTARG}
            [[ "${OS}" == "android" ]] ||
            [[ "${OS}" == "iphoneos" ]] ||
            [[ "${OS}" == "iphonesimulator" ]] ||
            [[ "${OS}" == "watchos" ]] ||
            [[ "${OS}" == "watchsimulator" ]] ||
            [[ "${OS}" == "appletvos" ]] ||
            [[ "${OS}" == "appletvsimulator" ]] || 
            [[ "${OS}" == "emscripten" ]] || usage
            ;;
        a)
            ARCH=${OPTARG}
            [[ "${ARCH}" == "armeabi-v7a" ]] ||
            [[ "${ARCH}" == "x86" ]] ||
            [[ "${ARCH}" == "x86_64" ]] ||
            [[ "${ARCH}" == "arm64-v8a" ]] || usage
            ;;
        t)
            BUILD_TYPE=${OPTARG}
            [[ "${BUILD_TYPE}" == "Debug" ]] ||
            [[ "${BUILD_TYPE}" == "MinSizeDebug" ]] ||
            [[ "${BUILD_TYPE}" == "Release" ]] ||
            [[ "${BUILD_TYPE}" == "RelWithDebInfo" ]] || usage
            ;;
        v) VERSION=${OPTARG};;
        f) CMAKE_FLAGS=${OPTARG};;
        *) usage;;
    esac
done

shift $((OPTIND-1))

# Check for obligatory fields
if [[ -z "${OS}" ]] || [[ -z "${BUILD_TYPE}" ]]; then
    echo "ERROR: options -o <os> and -t <build-type> are required";
    usage
fi

if [[ -n "${VERSION}" ]]; then
    # shellcheck disable=SC2089
    CMAKE_FLAGS="-D REALM_VERSION='${VERSION}' ${CMAKE_FLAGS}"
fi

if [[ "${OS}" == "android" ]]; then
    # Check for android-related obligatory fields
    if [[ -z "${ARCH}" ]]; then
        echo "ERROR: option -a is required for android builds";
        usage
    elif [[ -z "${ANDROID_NDK}" ]]; then
        echo "ERROR: set ANDROID_NDK to the top level path for the Android NDK";
        usage
    fi

    mkdir -p "build-android-${ARCH}-${BUILD_TYPE}"
    cd "build-android-${ARCH}-${BUILD_TYPE}" || exit 1

    # shellcheck disable=SC2086,SC2090
    ${CMAKE} -D CMAKE_SYSTEM_NAME=Android \
             -D CMAKE_ANDROID_NDK="${ANDROID_NDK}" \
             -D CMAKE_INSTALL_PREFIX=install \
             -D CMAKE_BUILD_TYPE="${BUILD_TYPE}" \
             -D CMAKE_ANDROID_ARCH_ABI="${ARCH}" \
             -D CMAKE_TOOLCHAIN_FILE="./tools/cmake/android.toolchain.cmake" \
             -D REALM_ENABLE_ENCRYPTION=On \
             -D CPACK_SYSTEM_NAME="Android-${ARCH}" \
             -D CMAKE_MAKE_PROGRAM=ninja \
             -G Ninja \
             ${CMAKE_FLAGS} \
             ..

    ninja -v
    ninja package
elif [[ "${OS}" == "emscripten" ]]; then
    if [[ -n "${EMSDK}" ]]; then
        EMCMAKE="${EMSDK}/upstream/emscripten/emcmake"
        if ! [[ -e "${EMCMAKE}" ]]; then
            echo "ERROR: emcmake not found: ${EMCMAKE}"
            usage
        fi
    elif ! which emcmake > /dev/null; then
        echo "ERROR: emcmake not found in path or set EMSDK to the"
        echo "       top level emscripten emsdk directory"
        usage
    fi

    if [[ "$(uname -s)" =~ Darwin* ]]; then
        NPROC="$(sysctl -n hw.ncpu)"
    else
        NPROC="$(nproc)"
    fi

    mkdir -p build-emscripten
    cd build-emscripten || exit 1

    # shellcheck disable=SC2086,SC2090
    ${EMCMAKE} ${CMAKE} -D CMAKE_BUILD_TYPE="${BUILD_TYPE}" \
                        -D REALM_COMBINED_TESTS=Off \
                        ${CMAKE_FLAGS} \
                        ..

    make "-j${NPROC}" 2>&1
else
    mkdir -p build-xcode-platforms
    cd build-xcode-platforms || exit 1

    # shellcheck disable=SC2086,SC2090
    ${CMAKE} -D CMAKE_TOOLCHAIN_FILE="../tools/cmake/xcode.toolchain.cmake" \
             -D CMAKE_BUILD_TYPE="${BUILD_TYPE}" \
             -D REALM_NO_TESTS=On \
             -D REALM_BUILD_LIB_ONLY=On \
             ${CMAKE_FLAGS} \
             -G Xcode ..
    xcodebuild -scheme ALL_BUILD -configuration "${BUILD_TYPE}" -destination "generic/platform=${OS}"
    PLATFORM_NAME="${OS}" EFFECTIVE_PLATFORM_NAME="-${OS}" cpack -C "${BUILD_TYPE}"
fi
