#!/bin/bash

set -e

#Set Script Name variable
SCRIPT=$(basename "${BASH_SOURCE[0]}")

function usage {
    echo "Usage: ${SCRIPT} -t <build_type> -o <target_os> -v <version> [-a <android_abi>] [-f <cmake_flags>]"
    echo ""
    echo "Arguments:"
    echo "   build_type=<Release|Debug>"
    echo "   target_os=<android|ios|watchos|tvos>"
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
            [ "${OS}" == "appletvsimulator" ] || usage
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
if [ "${OS}" == "android" ] && [ -z "${ARCH}" ]; then
    echo "ERROR: option -a is needed for android builds";
    usage
fi

if [ "${OS}" == "android" ]; then
    mkdir -p "build-android-${ARCH}-${BUILD_TYPE}"
    cd "build-android-${ARCH}-${BUILD_TYPE}" || exit 1
    cmake -D CMAKE_SYSTEM_NAME=Android \
          -D CMAKE_ANDROID_NDK="${ANDROID_NDK}" \
          -D CMAKE_INSTALL_PREFIX=install \
          -D CMAKE_BUILD_TYPE="${BUILD_TYPE}" \
          -D CMAKE_ANDROID_ARCH_ABI="${ARCH}" \
          -D REALM_ENABLE_ENCRYPTION=1 \
          -D REALM_VERSION="${VERSION}" \
          -D CPACK_SYSTEM_NAME="Android-${ARCH}" \
          -D CMAKE_MAKE_PROGRAM=ninja \
          -G Ninja \
          ${CMAKE_FLAGS} \
          ..

    ninja -v
    ninja package
else
    case "${OS}" in
        iphone*) toolchain="ios";;
        watch*) toolchain="watchos";;
        appletv*) toolchain="tvos";;
    esac
    [[ "${BUILD_TYPE}" = "Release" ]] && suffix="" || suffix="-dbg"

    mkdir -p "build-${OS}-${BUILD_TYPE}"
    cd "build-${OS}-${BUILD_TYPE}" || exit 1

    cmake -D CMAKE_TOOLCHAIN_FILE="../tools/cmake/${toolchain}.toolchain.cmake" \
          -D CMAKE_INSTALL_PREFIX="$(pwd)/install" \
          -D CMAKE_BUILD_TYPE="${BUILD_TYPE}" \
          -D REALM_NO_TESTS=1 \
          -D REALM_BUILD_LIB_ONLY=1 \
          -D REALM_VERSION="${VERSION}" \
          ${CMAKE_FLAGS} \
          -G Xcode ..
    xcodebuild -sdk "${OS}" \
               -configuration "${BUILD_TYPE}" \
               -target install \
               ONLY_ACTIVE_ARCH=NO
    tar -cvzf "realm-${BUILD_TYPE}-${VERSION}-${OS}-devel.tar.gz" -C install lib include
fi
