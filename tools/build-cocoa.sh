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

function add_to_xcframework() {
    local xcf="$1"
    local os="$2"
    local platform="$3"
    local build_type="$4"

    local suffix
    [[ "$bt" = "Release" ]] && suffix="" || suffix="-dbg"

    local variant=""
    if [[ "$platform" = *simulator ]]; then
        variant="-simulator"
    elif [[ "$platform" = *catalyst ]]; then
        variant="-maccatalyst"
    fi

    # Extract the library from the source tar
    local source_tar="build-${platform}-${build_type}/realm-core-${build_type}-${VERSION}-${platform}-devel.tar.gz"
    tar -C "$xcf" -zxvf "${source_tar}" "lib/librealm${suffix}.a"

    # Populate the actual directory structure
    local archs="$(lipo -archs "$xcf/lib/librealm${suffix}.a")"
    local dir="$os-$(echo "$archs" | tr ' ' '_')$variant"
    mkdir -p "$xcf/$dir/Headers"
    mv "$xcf/lib/librealm${suffix}.a" "$xcf/$dir"
    cp -R core/include/* "$xcf/$dir/Headers"

    # Add this library to the Plist
    cat << EOF >> "$xcf/Info.plist"
		<dict>
			<key>HeadersPath</key>
			<string>Headers</string>
			<key>LibraryIdentifier</key>
			<string>$dir</string>
			<key>LibraryPath</key>
			<string>librealm$suffix.a</string>
			<key>SupportedArchitectures</key>
			<array>$(for arch in $archs; do echo "<string>$arch</string>"; done)</array>
			<key>SupportedPlatform</key>
			<string>$os</string>
EOF
    if [[ "$platform" = *simulator ]]; then
        echo >> "$xcf/Info.plist" '			<key>SupportedPlatformVariant</key>'
        echo >> "$xcf/Info.plist" '			<string>simulator</string>'
    elif [[ "$platform" = *catalyst ]]; then
        echo >> "$xcf/Info.plist" '			<key>SupportedPlatformVariant</key>'
        echo >> "$xcf/Info.plist" '			<string>maccatalyst</string>'
    fi
    echo >> "$xcf/Info.plist" '		</dict>'
}

# Produce xcframeworks
if [[ -n $BUILD_XCFRAMEWORK ]]; then
    for bt in "${BUILD_TYPES[@]}"; do
        [[ "$bt" = "Release" ]] && suffix="" || suffix="-dbg"
        xcf="core/realm-core${suffix}.xcframework"
        rm -rf "$xcf"
        mkdir "$xcf"

        cat << EOF > "$xcf/Info.plist"
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>AvailableLibraries</key>
	<array>
EOF
        add_to_xcframework "$xcf" "macos" "macosx" "$bt"

        if [[ -z $MACOS_ONLY ]]; then
            add_to_xcframework "$xcf" "ios" "iphoneos" "$bt"
            add_to_xcframework "$xcf" "ios" "iphonesimulator" "$bt"
            add_to_xcframework "$xcf" "ios" "maccatalyst" "$bt"
            add_to_xcframework "$xcf" "watchos" "watchos" "$bt"
            add_to_xcframework "$xcf" "watchos" "watchsimulator" "$bt"
            add_to_xcframework "$xcf" "tvos" "appletvos" "$bt"
            add_to_xcframework "$xcf" "tvos" "appletvsimulator" "$bt"
        fi
        rmdir "$xcf/lib"

        cat << EOF >> "$xcf/Info.plist"
	</array>
	<key>CFBundlePackageType</key>
	<string>XFWK</string>
	<key>XCFrameworkFormatVersion</key>
	<string>1.0</string>
</dict>
</plist>
EOF
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
