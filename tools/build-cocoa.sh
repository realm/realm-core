#!/usr/bin/env bash

set -e

#Set Script Name variable
SCRIPT=$(basename "${BASH_SOURCE[0]}")
VERSION=$(git describe)

function usage {
    echo "Usage: ${SCRIPT} [-b] [-m|i] [-x] [-c <realm-cocoa-folder>] [-f <cmake-flags>]"
    echo ""
    echo "Arguments:"
    echo "   -b : build from source. If absent it will expect prebuilt packages"
    echo "   -m : build for macOS only"
    echo "   -i : build for iOS only"
    echo "   -x : build as an xcframework"
    echo "   -c : copy core to the specified folder instead of packaging it"
    echo "   -f : additional configuration flags to pass to cmake"
    exit 1;
}

# Parse the options
while getopts ":bmixc:f:" opt; do
    case "${opt}" in
        b) BUILD=1;;
        m) MACOS_ONLY=1;;
        i) IOS_ONLY=1;;
        x) BUILD_XCFRAMEWORK=1;;
        c) COPY=1
           DESTINATION=${OPTARG};;
        f) CMAKE_FLAGS=${OPTARG};;
        *) usage;;
    esac
done

shift $((OPTIND-1))

BUILD_TYPES=( Release MinSizeDebug )

if [[ -n $MACOS_ONLY ]]; then
    PLATFORMS=( macosx )
elif [[ -n $IOS_ONLY ]]; then
    PLATFORMS=( ios maccatalyst )
else
    PLATFORMS=( macosx maccatalyst ios watchos tvos )
fi

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
    for pl in ${PLATFORMS[*]}; do
    case "${pl}" in
        macosx | maccatalyst)
            for bt in "${BUILD_TYPES[@]}"; do
                build_macos $pl "$bt"
            done
        ;;
        *) # ios watchos tvos
            for bt in "${BUILD_TYPES[@]}"; do
                tools/cross_compile.sh -o "$pl" -t "$bt" -v "$(git describe)" -f "${CMAKE_FLAGS}"
            done
        ;;
    esac
    done
fi

rm -rf core
mkdir core

# copy headers from macosx or maccatalyst (one or both will be there depending on build options)
[[ -n $IOS_ONLY ]] && filename="build-maccatalyst-Release/realm-core-Release-${VERSION}-maccatalyst-devel.tar.gz" \
                   || filename="build-macosx-Release/realm-core-Release-${VERSION}-macosx-devel.tar.gz"
tar -C core -zxvf "${filename}" include doc

for bt in "${BUILD_TYPES[@]}"; do
    [[ "$bt" = "Release" ]] && suffix="" || suffix="-dbg"
    for p in "${PLATFORMS[@]}"; do
        filename="build-${p}-${bt}/realm-core-${bt}-${VERSION}-${p}-devel.tar.gz"
        tar -C core -zxvf "${filename}" "lib/librealm${suffix}.a"
        mv "core/lib/librealm${suffix}.a" "core/librealm-${p}${suffix}.a"
        tar -C core -zxvf "${filename}" "lib/librealm-parser${suffix}.a"
        mv "core/lib/librealm-parser${suffix}.a" "core/librealm-parser-${p}${suffix}.a"
    done
done

function extract_slices {
    local input="$1"
    local output="$2"
    local archs="$3"

    # create-xcframework does not like fat libraries with a single arch but
    # also doesn't like two thin libraries for one platform, so we have to
    # create different types of libraries depending on the arch count
    if [ $(wc -w <<< "$archs") == 2 ]; then
        archs=$(sed 's/extract/thin/' <<< "$archs")
    fi
    lipo $archs -output "$output" "$input"
}

# Produce xcframeworks
if [[ -n $BUILD_XCFRAMEWORK ]]; then
    for bt in "${BUILD_TYPES[@]}"; do
        make_core_xcframework=()
        [[ "$bt" = "Release" ]] && suffix="" || suffix="-dbg"
        rm -rf xcf-tmp
        mkdir xcf-tmp
        for p in "${PLATFORMS[@]}"; do
            source_lib="core/librealm-${p}${suffix}.a"
            if [[ "$p" == "macosx" || "$p" == "maccatalyst" ]]; then
                mkdir "xcf-tmp/$p${suffix}"
                ln "$source_lib" "xcf-tmp/$p${suffix}/librealm.a"
                make_core_xcframework+=( -library xcf-tmp/$p${suffix}/librealm.a -headers core/include)
            else
                device_lib_dir="xcf-tmp/${p}${suffix}-device"
                mkdir "$device_lib_dir"

                device=''
                simulator=''
                for arch in $(lipo -archs "$source_lib"); do
                    if [[ "$arch" == arm* ]]; then
                        device+="-extract $arch "
                    else
                        simulator+="-extract $arch "
                    fi
                done
                extract_slices "$source_lib" "${device_lib_dir}/librealm.a" "$device"
                make_core_xcframework+=( -library "${device_lib_dir}/librealm.a" -headers core/include)

                if [ "$p" != "ios" ]; then
                    sim_lib_dir="xcf-tmp/${p}${suffix}-simulator"
                    mkdir "$sim_lib_dir"
                    extract_slices "$source_lib" "${sim_lib_dir}/librealm.a" "$simulator"
                    make_core_xcframework+=( -library "${sim_lib_dir}/librealm.a" -headers core/include)
                fi
            fi
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
