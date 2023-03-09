#!/usr/bin/env bash

set -e

#Set Script Name variable
SCRIPT=$(basename "${BASH_SOURCE[0]}")

function usage {
    echo "Usage: ${SCRIPT} -t <build_type> -o <target_os> -v <version> [-a <android_abi>] [-f <cmake_flags>]"
    echo ""
    echo "Arguments:"
    echo "   build_type=<Release|Debug>"
    echo "   target_os=<android|iphoneos|iphonesimulator|watchos|watchsimulator|appletvos|appletvsimulator|qnx>"
    echo "   android_abi=<armeabi-v7a|x86|x86_64|arm64-v8a>"
    exit 1;
}

# Parse the options
while getopts ":o:a:t:v:f:" opt; do
    case "${opt}" in
        o)
            OS=${OPTARG}
            [ "${OS}" == "android" ] ||
            [ "${OS}" == "iphoneos" ] ||
            [ "${OS}" == "iphonesimulator" ] ||
            [ "${OS}" == "watchos" ] ||
            [ "${OS}" == "watchsimulator" ] ||
            [ "${OS}" == "appletvos" ] ||
            [ "${OS}" == "appletvsimulator" ] || 
            [ "${OS}" == "qnx" ] || usage
            ;;
        a)
            ARCH=${OPTARG}
            [ "${ARCH}" == "armeabi-v7a" ] ||
            [ "${ARCH}" == "x86" ] ||
            [ "${ARCH}" == "x86_64" ] ||
            [ "${ARCH}" == "arm64-v8a" ] || usage
            ;;
        t)
            BUILD_TYPE=${OPTARG}
            [ "${BUILD_TYPE}" == "Debug" ] ||
            [ "${BUILD_TYPE}" == "MinSizeDebug" ] ||
            [ "${BUILD_TYPE}" == "Release" ] || usage
            ;;
        v) VERSION=${OPTARG};;
        f) CMAKE_FLAGS=${OPTARG};;
        *) usage;;
    esac
done

shift $((OPTIND-1))

# Check for obligatory fields
if [ -z "${OS}" ] || [ -z "${BUILD_TYPE}" ]; then
    echo "ERROR: options -o, -t and -v are always needed";
    usage
fi

# Check for android-related obligatory fields
if [[ "${OS}" == "android" ]]; then
    if [[ -z "${ARCH}" ]]; then
        echo "ERROR: option -a is needed for android builds";
        usage
    elif [[ -z "${ANDROID_NDK}" ]]; then
        echo "ERROR: set ANDROID_NDK to the top level path for the Android NDK";
        usage
    fi
fi

if [ "${OS}" == "android" ]; then
    mkdir -p "build-android-${ARCH}-${BUILD_TYPE}"
    cd "build-android-${ARCH}-${BUILD_TYPE}" || exit 1
    cmake -D CMAKE_SYSTEM_NAME=Android \
          -D CMAKE_ANDROID_NDK="${ANDROID_NDK}" \
          -D CMAKE_INSTALL_PREFIX=install \
          -D CMAKE_BUILD_TYPE="${BUILD_TYPE}" \
          -D CMAKE_ANDROID_ARCH_ABI="${ARCH}" \
          -D CMAKE_TOOLCHAIN_FILE="./tools/cmake/android.toolchain.cmake" \
          -D REALM_ENABLE_ENCRYPTION=1 \
          -D REALM_VERSION="${VERSION}" \
          -D CPACK_SYSTEM_NAME="Android-${ARCH}" \
          -D CMAKE_MAKE_PROGRAM=ninja \
          -G Ninja \
          ${CMAKE_FLAGS} \
          ..

    ninja -v
    ninja package
elif [ "${OS}" == "qnx" ]; then
    ARCH="x86_64"
    if [[ -z "${QNX_BASE}" ]]; then
        echo "QNX_BASE is not defined"
        exit 1
    fi
    HOST_OS=$(uname -s)
    case "$HOST_OS" in
        Linux)
        QNX_HOST=$QNX_BASE/host/linux/x86_64
            ;;
        Darwin)
        QNX_HOST=$QNX_BASE/host/darwin/x86_64
            ;;
        *)
        QNX_HOST=$QNX_BASE/host/win64/x86_64
            ;;
    esac
    export QNX_HOST    
    export QNX_TARGET=$QNX_BASE/target/qnx7
    export QNX_CONFIGURATION_EXCLUSIVE=$HOME/.qnx
    export QNX_CONFIGURATION=$QNX_CONFIGURATION_EXCLUSIVE
    export PATH=$QNX_HOST/usr/bin:$QNX_CONFIGURATION/bin:$QNX_BASE/jre/bin:$QNX_BASE/host/common/bin:$PATH
    mkdir -p "build-qnx-${ARCH}-${BUILD_TYPE}"
    cd "build-qnx-${ARCH}-${BUILD_TYPE}" || exit 1
    cmake -D CMAKE_SYSTEM_NAME=QNX \
          -D QNX_BASE="${QNX_BASE}" \
          -D CMAKE_INSTALL_PREFIX=install \
          -D CMAKE_BUILD_TYPE="${BUILD_TYPE}" \
          -D CMAKE_TOOLCHAIN_FILE="./tools/cmake/qnx.toolchain.cmake" \
          -D REALM_ENABLE_ENCRYPTION=1 \
          -D REALM_TEST_SYNC_LOGGING=On \
          -D REALM_VERSION="${VERSION}" \
          -D CMAKE_MAKE_PROGRAM=ninja \
          -D CMAKE_BUILD_WITH_INSTALL_RPATH=On \
          -G Ninja \
          ${CMAKE_FLAGS} \
          ..

    ninja -v
#    ninja package
else
    mkdir -p build-xcode-platforms
    cd build-xcode-platforms || exit 1

    cmake -D CMAKE_TOOLCHAIN_FILE="../tools/cmake/xcode.toolchain.cmake" \
          -D CMAKE_BUILD_TYPE="${BUILD_TYPE}" \
          -D REALM_NO_TESTS=1 \
          -D REALM_BUILD_LIB_ONLY=1 \
          -D REALM_VERSION="${VERSION}" \
          ${CMAKE_FLAGS} \
          -G Xcode ..
    xcodebuild -scheme ALL_BUILD -configuration "${BUILD_TYPE}" -destination "generic/platform=${OS}"
    PLATFORM_NAME="${OS}" EFFECTIVE_PLATFORM_NAME="-${OS}" cpack -C "${BUILD_TYPE}"
fi
