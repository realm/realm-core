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
    echo "   -d : include debug libraries"
    echo "   -c : copy core to the specified folder instead of packaging it"
    echo "   -f : additional configuration flags to pass to cmake"
    exit 1;
}

# Parse the options
while getopts ":bmxdc:f:" opt; do
    case "${opt}" in
        b) BUILD=1;;
        m) MACOS_ONLY=1;;
        x) BUILD_XCFRAMEWORK=1;;
        d) BUILD_DEBUG=1;;
        c) COPY=1
           DESTINATION=${OPTARG};;
        f) CMAKE_FLAGS=${OPTARG};;
        *) usage;;
    esac
done

shift $((OPTIND-1))

[[ -n $BUILD_DEBUG ]] && BUILD_TYPES=( Release Debug ) || BUILD_TYPES=( Release )
[[ -z $MACOS_ONLY ]] && PLATFORMS=( macosx maccatalyst iphoneos iphonesimulator watchos watchsimulator appletvos appletvsimulator ) || PLATFORMS=( macosx )

readonly device_platforms=( iphoneos iphonesimulator watchos watchsimulator appletvos appletvsimulator )

if [[ -n $BUILD ]]; then
    mkdir -p build-macosx
    (
        cd build-macosx
        cmake -D CMAKE_TOOLCHAIN_FILE="../tools/cmake/xcode.toolchain.cmake" \
        -D CMAKE_SYSTEM_NAME=Darwin \
        -D REALM_VERSION="${VERSION}" \
        -D REALM_BUILD_LIB_ONLY=ON \
        -D CPACK_SYSTEM_NAME=macosx \
        -D CPACK_PACKAGE_DIRECTORY=.. \
        ${CMAKE_FLAGS} \
        -G Xcode ..

        for bt in "${BUILD_TYPES[@]}"; do
            xcodebuild -sdk macosx -configuration "${bt}" ONLY_ACTIVE_ARCH=NO
            cpack -C "${bt}"
        done
    )
    if [[ -z $MACOS_ONLY ]]; then
        mkdir -p build-xcode-platforms
        (
            cd build-xcode-platforms
            cmake -D CMAKE_TOOLCHAIN_FILE="../tools/cmake/xcode.toolchain.cmake" \
            -D REALM_VERSION="${VERSION}" \
            -D REALM_BUILD_LIB_ONLY=ON \
            -D CPACK_PACKAGE_DIRECTORY=.. \
            ${CMAKE_FLAGS} \
            -G Xcode ..

            destinations=(-destination "generic/platform=macOS,variant=Mac Catalyst")
            for os in "${device_platforms[@]}"; do
                destinations+=(-destination "generic/platform=${os}")
            done

            for bt in "${BUILD_TYPES[@]}"; do
                xcodebuild -scheme ALL_BUILD -configuration "${bt}" "${destinations[@]}"

                PLATFORM_NAME="maccatalyst" EFFECTIVE_PLATFORM_NAME="-maccatalyst" cpack -C "${bt}"
                for os in "${device_platforms[@]}"; do
                    PLATFORM_NAME="${os}" EFFECTIVE_PLATFORM_NAME="-${os}" cpack -C "${bt}"
                done
            done
        )
    fi
fi

rm -rf core
mkdir core

filename="realm-Release-${VERSION}-macosx-devel.tar.gz"
tar -C core -zxvf "${filename}" include doc

# Overwrite version.txt
echo ${VERSION} > core/version.txt

# Assemble the combined core+sync+os libraries
for bt in "${BUILD_TYPES[@]}"; do
    [[ "$bt" = "Release" ]] && suffix="" || suffix="-dbg"

    for p in "${PLATFORMS[@]}"; do
        # Extract all of the source libraries we need
        filename="realm-${bt}-${VERSION}-${p}-devel.tar.gz"
        # core binary
        tar -C core -zxvf "${filename}" "lib/librealm${suffix}.a"
        mv "core/lib/librealm${suffix}.a" "core/librealm-${p}${suffix}.a"
        # parser binary
        tar -C core -zxvf "${filename}" "lib/librealm-parser${suffix}.a"
        mv "core/lib/librealm-parser${suffix}.a" "core/librealm-parser-${p}${suffix}.a"
        # sync binary
        tar -C core -zxvf "${filename}" "lib/librealm-sync${suffix}.a"
        mv "core/lib/librealm-sync${suffix}.a" "core/librealm-sync-${p}${suffix}.a"
        # object store binary
        tar -C core -zxvf "${filename}" "lib/librealm-object-store${suffix}.a"
        mv "core/lib/librealm-object-store${suffix}.a" "core/librealm-object-store-${p}${suffix}.a"
        rm -r "core/lib"

        # Merge the core, sync & object store libraries together
        libtool -static -o core/librealm-monorepo-${p}${suffix}.a \
          core/librealm-${p}${suffix}.a \
          core/librealm-sync-${p}${suffix}.a \
          core/librealm-object-store-${p}${suffix}.a

        # remove the now merged libraries
        rm -f core/librealm-${p}${suffix}.a \
              core/librealm-sync-${p}${suffix}.a \
              core/librealm-object-store-${p}${suffix}.a
    done
done

function add_to_xcframework() {
    local xcf="$1"
    local library="$2"
    local os="$3"
    local platform="$4"
    local build_type="$5"
    local suffix
    [[ "$build_type" = "Release" ]] && suffix="" || suffix="-dbg"

    local variant=""
    if [[ "$platform" = *simulator ]]; then
        variant="-simulator"
    elif [[ "$platform" = *catalyst ]]; then
        variant="-maccatalyst"
    fi

    local location="core/$library-${platform}${suffix}.a"

    # Populate the actual directory structure
    local archs="$(lipo -archs "${location}")"
    local dir="$os-$(echo "$archs" | tr ' ' '_')$variant"
    mkdir -p "$xcf/$dir/Headers"

    cp ${location} "$xcf/$dir/$library${suffix}.a"
    cp -R core/include/* "$xcf/$dir/Headers"

    # Add this library to the Plist
    cat << EOF >> "$xcf/Info.plist"
		<dict>
			<key>HeadersPath</key>
			<string>Headers</string>
			<key>LibraryIdentifier</key>
			<string>$dir</string>
			<key>LibraryPath</key>
			<string>$library$suffix.a</string>
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

function create_xcframework {
    local framework="$1"
    local library="$2"

    for bt in "${BUILD_TYPES[@]}"; do
        [[ "$bt" = "Release" ]] && suffix="" || suffix="-dbg"
        xcf="core/${framework}${suffix}.xcframework"
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
        add_to_xcframework "$xcf" "$library" "macos" "macosx" "$bt"

        if [[ -z $MACOS_ONLY ]]; then
            add_to_xcframework "$xcf" "$library" "ios" "iphoneos" "$bt"
            add_to_xcframework "$xcf" "$library" "ios" "iphonesimulator" "$bt"
            add_to_xcframework "$xcf" "$library" "ios" "maccatalyst" "$bt"
            add_to_xcframework "$xcf" "$library" "watchos" "watchos" "$bt"
            add_to_xcframework "$xcf" "$library" "watchos" "watchsimulator" "$bt"
            add_to_xcframework "$xcf" "$library" "tvos" "appletvos" "$bt"
            add_to_xcframework "$xcf" "$library" "tvos" "appletvsimulator" "$bt"
        fi

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
}

# Produce xcframeworks
if [[ -n $BUILD_XCFRAMEWORK ]]; then
    create_xcframework realm-monorepo librealm-monorepo
    create_xcframework realm-parser librealm-parser
fi

# Package artifacts
if [[ -n $COPY ]]; then
    rm -rf "${DESTINATION}/core"
    mkdir -p "${DESTINATION}"
    cp -R core "${DESTINATION}"
else
    if [[ -n $BUILD_XCFRAMEWORK ]]; then
        rm -f "realm-parser-cocoa-${VERSION}.tar.xz"
        tar -cJvf "realm-parser-cocoa-${VERSION}.tar.xz" core/realm-parser*.xcframework
        rm -f "realm-monorepo-xcframework-${VERSION}.tar.xz"
        # until realmjs requires an xcframework, only package as .xz
        tar -cJvf "realm-monorepo-xcframework-${VERSION}.tar.xz" core/realm-monorepo*.xcframework
    fi
fi
