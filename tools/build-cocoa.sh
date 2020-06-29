#!/usr/bin/env bash

set -e

#Set Script Name variable
SCRIPT=$(basename "${BASH_SOURCE[0]}")
VERSION=$(git describe)

function usage {
    echo "Usage: ${SCRIPT} [-b] [-m] [-c <realm-cocoa-folder>] [-f <cmake-flags>]"
    echo ""
    echo "Arguments:"
    echo "   -b : build from source. If absent it will expect prebuilt packages"
    echo "   -m : build for macOS only"
    echo "   -c : copy core to the specified folder instead of packaging it"
    echo "   -f : additional configuration flags to pass to cmake"
    exit 1;
}

# Parse the options
while getopts ":bmc:f:" opt; do
    case "${opt}" in
        b) BUILD=1;;
        m) MACOS_ONLY=1;;
        c) COPY=1
           DESTINATION=${OPTARG};;
        f) CMAKE_FLAGS=${OPTARG};;
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
              ${CMAKE_FLAGS} \
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
                tools/cross_compile.sh -o $os -t $bt -v $(git describe) -f "${CMAKE_FLAGS}"
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

# produce xcframeworks
rm -rf xcframework
mkdir temp
make_core_xcframework="xcodebuild -create-xcframework"
make_core_debug_xcframework="xcodebuild -create-xcframework"
for bt in "${BUILD_TYPES[@]}"; do
    [[ "$bt" = "Release" ]] && suffix="" || suffix="-dbg"
    for p in "${PLATFORMS[@]}"; do
        if [[ "$p" == "macosx" || "$p" == "maccatalyst" ]]; then
            cp build-${p}-${bt}/src/realm/librealm${suffix}.a temp/librealm-${p}${suffix}.a
            cp build-${p}-${bt}/src/realm/parser/librealm-parser${suffix}.a temp/librealm-parser-${p}${suffix}.a
            if [[ "$bt" == "Release" ]]; then
                make_core_xcframework+=" -library temp/librealm-${p}${suffix}.a -headers core/include/"
            else
                make_core_debug_xcframework+=" -library temp/librealm-${p}${suffix}.a -headers core/include/"
            fi
        elif [[ "$p" == "ios" ]]; then
            cp build-${p}-${bt}/src/realm/${bt}-iphoneos/librealm${suffix}.a temp/librealm-iphoneos${suffix}.a
            cp build-${p}-${bt}/src/realm/${bt}-iphonesimulator/librealm${suffix}.a temp/librealm-iphonesimulator${suffix}.a
            if [[ "$bt" == "Release" ]]; then
                make_core_xcframework+=" -library temp/librealm-iphoneos${suffix}.a -headers core/include/"
                make_core_xcframework+=" -library temp/librealm-iphonesimulator${suffix}.a -headers core/include/"
            else
                make_core_debug_xcframework+=" -library temp/librealm-iphoneos${suffix}.a -headers core/include/"
                make_core_debug_xcframework+=" -library temp/librealm-iphonesimulator${suffix}.a -headers core/include/"
            fi
        elif [[ "$p" == "watchos" ]]; then
            cp build-${p}-${bt}/src/realm/${bt}-watchos/librealm${suffix}.a temp/librealm-watchos${suffix}.a
            cp build-${p}-${bt}/src/realm/${bt}-watchsimulator/librealm${suffix}.a temp/librealm-watchsimulator${suffix}.a
            if [[ "$bt" == "Release" ]]; then
                make_core_xcframework+=" -library temp/librealm-watchos${suffix}.a -headers core/include/"
                make_core_xcframework+=" -library temp/librealm-watchsimulator${suffix}.a -headers core/include/"
            else
                make_core_debug_xcframework+=" -library temp/librealm-watchos${suffix}.a -headers core/include/"
                make_core_debug_xcframework+=" -library temp/librealm-watchsimulator${suffix}.a -headers core/include/"
            fi
        elif [[ "$p" == "tvos" ]]; then
            cp build-${p}-${bt}/src/realm/${bt}-appletvos/librealm${suffix}.a temp/librealm-appletvos${suffix}.a
            cp build-${p}-${bt}/src/realm/${bt}-appletvsimulator/librealm${suffix}.a temp/librealm-appletvsimulator${suffix}.a
            if [[ "$bt" == "Release" ]]; then
                make_core_xcframework+=" -library temp/librealm-appletvos${suffix}.a -headers core/include/"
                make_core_xcframework+=" -library temp/librealm-appletvsimulator${suffix}.a -headers core/include/"
            else
                make_core_debug_xcframework+=" -library temp/librealm-appletvos${suffix}.a -headers core/include/"
                make_core_debug_xcframework+=" -library temp/librealm-appletvsimulator${suffix}.a -headers core/include/"
            fi
        fi
    done
done
make_core_xcframework+=" -output core/xcframework/realm-core.xcframework"
make_core_debug_xcframework+=" -output core/xcframework/realm-core-dbg.xcframework"

eval $make_core_xcframework
eval $make_core_debug_xcframework
rm -rf temp
