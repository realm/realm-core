#!/bin/bash

set -e

#Set Script Name variable
SCRIPT=$(basename "${BASH_SOURCE[0]}")

function usage {
    echo "Usage: ${SCRIPT} -t <build_type> -o <target_os> -v <version> [-a <android_abi>] [-f <cmake_flags>]"
    echo ""
    echo "Arguments:"
    echo "   build_type=<Release|Debug|MinSizeDebug>"
    echo "   target_os=<android|ios|watchos|tvos|macos>"
    echo "   android_abi=<armeabi-v7a|x86|x86_64|arm64-v8a>"
    exit 1;
}

# Parse the options
while getopts ":o:a:t:v:f:" opt; do
    case "${opt}" in
        o)
            OS=${OPTARG}
            [ "${OS}" == "android" ] ||
            [ "${OS}" == "ios" ] ||
            [ "${OS}" == "watchos" ] ||
            [ "${OS}" == "tvos" ] ||
            [ "${OS}" == "macos" ] || usage
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
    cmake -D CMAKE_TOOLCHAIN_FILE="${ANDROID_NDK}/build/cmake/android.toolchain.cmake" \
          -D CMAKE_INSTALL_PREFIX=install \
          -D CMAKE_BUILD_TYPE="${BUILD_TYPE}" \
          -D ANDROID_ABI="${ARCH}" \
          -D REALM_ENABLE_ENCRYPTION=1 \
          -D REALM_VERSION="${VERSION}" \
          -D CPACK_SYSTEM_NAME="Android-${ARCH}" \
          -D CMAKE_MAKE_PROGRAM=ninja \
          -G Ninja \
          ${CMAKE_FLAGS} \
          ..

    ninja -v
    ninja package
elif [ "${OS}" == "macos" ]; then 
		    [[ "${BUILD_TYPE}" = "Release" ]] && suffix="" || suffix="-dbg"
	SDK="macosx"

    function configure_xcode {
        cmake -D CMAKE_TOOLCHAIN_FILE="../tools/cmake/${OS}.toolchain.cmake" \
              -D CMAKE_INSTALL_PREFIX="$(pwd)/install" \
              -D CMAKE_BUILD_TYPE="${BUILD_TYPE}" \
              -D REALM_NO_TESTS=1 \
              -D REALM_VERSION="${VERSION}" \
              -D CPACK_SYSTEM_NAME="${SDK}" \
              ${CMAKE_FLAGS} \
              -G Xcode ..
    }

    mkdir -p "build-${OS}-${BUILD_TYPE}"
    cd "build-${OS}-${BUILD_TYPE}" || exit 1

    configure_xcode
    xcodebuild -sdk "${SDK}" \
               -configuration "${BUILD_TYPE}" \
               ONLY_ACTIVE_ARCH=NO
    # mkdir -p "src/realm/${BUILD_TYPE}"
    # mkdir -p "src/realm/parser/${BUILD_TYPE}"
else
    case "${OS}" in
        ios) SDK="iphone";;
        watchos) SDK="watch";;
        tvos) SDK="appletv";;
    esac
    [[ "${BUILD_TYPE}" = "Release" ]] && suffix="" || suffix="-dbg"

    function configure_xcode {
        cmake -D CMAKE_TOOLCHAIN_FILE="../tools/cmake/${OS}.toolchain.cmake" \
              -D CMAKE_INSTALL_PREFIX="$(pwd)/install" \
              -D CMAKE_BUILD_TYPE="${BUILD_TYPE}" \
              -D REALM_NO_TESTS=1 \
              -D REALM_VERSION="${VERSION}" \
              -D CPACK_SYSTEM_NAME="${SDK}os" \
              ${CMAKE_FLAGS} \
              -G Xcode ..
    }

    if [ "${OS}" == "watchos" ] && [ -n "${XCODE12_DEVELOPER_DIR}" ]; then
        mkdir -p "build-${OS}-${BUILD_TYPE}-64"
        pushd "build-${OS}-${BUILD_TYPE}-64" || exit 1
        (
            export DEVELOPER_DIR="$XCODE12_DEVELOPER_DIR"
            configure_xcode
            xcodebuild -sdk "${SDK}simulator" -configuration "${BUILD_TYPE}" ARCHS='x86_64'
        )
        WATCHOS_EXTRA_LIB="$(pwd)/src/realm/${BUILD_TYPE}-${SDK}simulator/librealm${suffix}.a"
        popd
    fi


    mkdir -p "build-${OS}-${BUILD_TYPE}"
    cd "build-${OS}-${BUILD_TYPE}" || exit 1

    configure_xcode
    xcodebuild -sdk "${SDK}os" \
               -configuration "${BUILD_TYPE}" \
               ONLY_ACTIVE_ARCH=NO
    xcodebuild -sdk "${SDK}simulator" \
               -configuration "${BUILD_TYPE}" \
               ONLY_ACTIVE_ARCH=NO
    mkdir -p "src/realm/${BUILD_TYPE}"
    mkdir -p "src/realm/parser/${BUILD_TYPE}"
    lipo -create \
         -output "src/realm/${BUILD_TYPE}/librealm${suffix}.a" \
         "src/realm/${BUILD_TYPE}-${SDK}os/librealm${suffix}.a" \
         "src/realm/${BUILD_TYPE}-${SDK}simulator/librealm${suffix}.a" \
         ${WATCHOS_EXTRA_LIB}
    lipo -create \
         -output "src/realm/parser/${BUILD_TYPE}/librealm-parser${suffix}.a" \
         "src/realm/parser/${BUILD_TYPE}-${SDK}os/librealm-parser${suffix}.a" \
         "src/realm/parser/${BUILD_TYPE}-${SDK}simulator/librealm-parser${suffix}.a"
    cpack -C "${BUILD_TYPE}" || exit 1
fi
