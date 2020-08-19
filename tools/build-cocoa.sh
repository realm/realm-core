#!/usr/bin/env bash

set -e

#Set Script Name variable
SCRIPT=$(basename "${BASH_SOURCE[0]}")
VERSION=$(git describe)

function usage {
    echo "Usage: ${SCRIPT} [-b] [-m] [-x] [-c <realm-cocoa-folder>] [-f <cmake-flags>]"
    echo ""
    echo "Arguments:"
    echo "   -b : build from source. If absent it will expect prebuilt packages"
    echo "   -m : build for macOS only"
    echo "   -x : build as an xcframework"
    echo "   -c : copy core to the specified folder instead of packaging it"
    echo "   -f : additional configuration flags to pass to cmake"
    exit 1;
}

# Parse the options
while getopts ":bmxc:f:" opt; do
    case "${opt}" in
        b) BUILD=1;;
        m) MACOS_ONLY=1;;
        x) BUILD_XCFRAMEWORK=1;;
        c) COPY=1
           DESTINATION=${OPTARG};;
        f) CMAKE_FLAGS=${OPTARG};;
        *) usage;;
    esac
done

shift $((OPTIND-1))

readonly BUILD_TYPES=( Release MinSizeDebug )
[[ -z $MACOS_ONLY ]] && PLATFORMS=( macosx maccatalyst iphoneos iphonesimulator watchos watchsimulator appletvos appletvsimulator ) || PLATFORMS=( macosx )

readonly device_platforms=( iphoneos iphonesimulator watchos watchsimulator appletvos appletvsimulator )

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

if [[ -n $BUILD ]]; then
    for bt in "${BUILD_TYPES[@]}"; do
        build_macos macosx "$bt"
    done
    if [[ -z $MACOS_ONLY ]]; then
        for bt in "${BUILD_TYPES[@]}"; do
            build_macos maccatalyst "$bt"
        done
        for os in "${device_platforms[@]}"; do
            for bt in "${BUILD_TYPES[@]}"; do
                tools/cross_compile.sh -o "$os" -t "$bt" -v "$(git describe)" -f "${CMAKE_FLAGS}"
            done
        done
    fi
fi

rm -rf core
mkdir core

filename="build-macosx-Release/realm-core-Release-${VERSION}-macosx-devel.tar.gz"
tar -C core -zxvf "${filename}" include doc

function combine_libraries {
    local device_suffix="$1"
    local simulator_suffix="$2"
    local output_suffix="$3"

    # Remove the arm64 slice from the simulator library if it exists as we
    # can't have two arm64 slices in the universal library
    lipo "core/librealm-${simulator_suffix}.a" -remove arm64 -output "core/librealm-${simulator_suffix}.a" || true
    lipo "core/librealm-parser-${simulator_suffix}.a" -remove arm64 -output "core/librealm-parser-${simulator_suffix}.a" || true

    lipo "core/librealm-${device_suffix}.a" \
         "core/librealm-${simulator_suffix}.a" \
         -create -output "out/librealm-${output_suffix}.a"
    lipo "core/librealm-parser-${device_suffix}.a" \
         "core/librealm-parser-${simulator_suffix}.a" \
         -create -output "out/librealm-parser-${output_suffix}.a"
}

# Assemble the legacy fat libraries
for bt in "${BUILD_TYPES[@]}"; do
    [[ "$bt" = "Release" ]] && suffix="" || suffix="-dbg"
    for p in "${PLATFORMS[@]}"; do
        filename="build-${p}-${bt}/realm-core-${bt}-${VERSION}-${p}-devel.tar.gz"
        tar -C core -zxvf "${filename}" "lib/librealm${suffix}.a"
        mv "core/lib/librealm${suffix}.a" "core/librealm-${p}${suffix}.a"
        tar -C core -zxvf "${filename}" "lib/librealm-parser${suffix}.a"
        mv "core/lib/librealm-parser${suffix}.a" "core/librealm-parser-${p}${suffix}.a"
    done

    mkdir -p out
    combine_libraries "iphoneos${suffix}" "iphonesimulator${suffix}" "ios${suffix}"
    combine_libraries "watchos${suffix}" "watchsimulator${suffix}" "watchos${suffix}"
    combine_libraries "appletvos${suffix}" "appletvsimulator${suffix}" "tvos${suffix}"
done
rm core/*.a
mv out/*.a core
rmdir out

# Produce xcframeworks
if [[ -n $BUILD_XCFRAMEWORK ]]; then
    for bt in "${BUILD_TYPES[@]}"; do
        make_core_xcframework=()
        [[ "$bt" = "Release" ]] && suffix="" || suffix="-dbg"
        rm -rf xcf-tmp
        mkdir xcf-tmp
        for p in "${PLATFORMS[@]}"; do
            mkdir "xcf-tmp/$p"
            filename="build-${p}-${bt}/realm-core-${bt}-${VERSION}-${p}-devel.tar.gz"
            tar -C "xcf-tmp/$p" -zxvf "${filename}" "lib/librealm${suffix}.a"
            make_core_xcframework+=( -library "xcf-tmp/$p/lib/librealm${suffix}.a" -headers core/include)
        done
        xcodebuild -create-xcframework "${make_core_xcframework[@]}" -output core/realm-core${suffix}.xcframework
        rm -rf xcf-tmp
    done
fi

# Package artifacts
if [[ -n $COPY ]]; then
    rm -rf "${DESTINATION}/core"
    mkdir -p "${DESTINATION}"
    cp -R core "${DESTINATION}"
else
    v=$(git describe --tags)
    rm -f "realm-core-cocoa-${v}.tar.xz"
    tar -cJvf "realm-core-cocoa-${v}.tar.xz" core
fi
