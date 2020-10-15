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
        rm -f realm-*-devel.tar.gz
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

filename="build-macosx-Release/realm-Release-${VERSION}-macosx-devel.tar.gz"
tar -C core -zxvf "${filename}" include doc

# Overwrite version.txt
echo ${VERSION} > core/version.txt

function combine_libraries {
    local device_suffix="$1"
    local simulator_suffix="$2"
    local output_suffix="$3"

    # Remove the arm64 slice from the simulator library if it exists as we
    # can't have two arm64 slices in the universal library
    lipo "core/librealm-${simulator_suffix}.a" -remove arm64 -output "core/librealm-${simulator_suffix}.a" || true
    lipo "core/librealm-parser-${simulator_suffix}.a" -remove arm64 -output "core/librealm-parser-${simulator_suffix}.a" || true
    lipo "core/librealm-sync-${simulator_suffix}.a" -remove arm64 -output "core/librealm-sync-${simulator_suffix}.a" || true
    lipo "core/librealm-object-store-${simulator_suffix}.a" -remove arm64 -output "core/librealm-object-store-${simulator_suffix}.a" || true

    # core
    lipo "core/librealm-${device_suffix}.a" \
         "core/librealm-${simulator_suffix}.a" \
         -create -output "core/librealm-${output_suffix}.a"
    # parser
    lipo "core/librealm-parser-${device_suffix}.a" \
         "core/librealm-parser-${simulator_suffix}.a" \
         -create -output "core/librealm-parser-${output_suffix}.a"
    # sync
    lipo "core/librealm-sync-${device_suffix}.a" \
         "core/librealm-sync-${simulator_suffix}.a" \
         -create -output "core/librealm-sync-${output_suffix}.a"
    # object store
    lipo "core/librealm-object-store-${device_suffix}.a" \
         "core/librealm-object-store-${simulator_suffix}.a" \
         -create -output "core/librealm-object-store-${output_suffix}.a"

    # Merge the core, sync & object store libraries together
    libtool -static -o core/librealm-monorepo-${device_suffix}.a \
      core/librealm-${device_suffix}.a \
      core/librealm-sync-${device_suffix}.a \
      core/librealm-object-store-${device_suffix}.a

    libtool -static -o core/librealm-monorepo-${simulator_suffix}.a \
      core/librealm-${simulator_suffix}.a \
      core/librealm-sync-${simulator_suffix}.a \
      core/librealm-object-store-${simulator_suffix}.a
    # remove the now merged libraries, but leave the parser
    rm -f core/librealm-${simulator_suffix}.a
    rm -f core/librealm-${device_suffix}.a
    rm -f core/librealm-${output_suffix}.a
    rm -f core/librealm-sync-${simulator_suffix}.a
    rm -f core/librealm-sync-${device_suffix}.a
    rm -f core/librealm-sync-${output_suffix}.a
    rm -f core/librealm-object-store-${simulator_suffix}.a
    rm -f core/librealm-object-store-${device_suffix}.a
    rm -f core/librealm-object-store-${output_suffix}.a
}

function combine_libraries_macos {
    local build_type="$1"
    # Merge the core, sync & object store libraries together
    libtool -static -o core/librealm-monorepo-macosx${build_type}.a \
      core/librealm-macosx${build_type}.a \
      core/librealm-sync-macosx${build_type}.a \
      core/librealm-object-store-macosx${build_type}.a
    # remove the now merged libraries, but leave the parser
    rm -f core/librealm-macosx${build_type}.a
    rm -f core/librealm-sync-macosx${build_type}.a
    rm -f core/librealm-object-store-macosx${build_type}.a
}

# Assemble the legacy fat libraries
for bt in "${BUILD_TYPES[@]}"; do
    [[ "$bt" = "Release" ]] && suffix="" || suffix="-dbg"
    for p in "${PLATFORMS[@]}"; do
        filename="build-${p}-${bt}/realm-${bt}-${VERSION}-${p}-devel.tar.gz"
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
    done

    # merge the libraries for macos
    combine_libraries_macos ${suffix}

    if [[ -z $MACOS_ONLY ]]; then
    # merge the libraries for maccatalyst
    libtool -static -o core/librealm-monorepo-maccatalyst${suffix}.a \
      core/librealm-maccatalyst${suffix}.a \
      core/librealm-sync-maccatalyst${suffix}.a \
      core/librealm-object-store-maccatalyst${suffix}.a
    rm -f core/librealm-maccatalyst*.a

    # merge the libraries for other Apple platforms
    combine_libraries "iphoneos${suffix}" "iphonesimulator${suffix}" "ios${suffix}"
    combine_libraries "watchos${suffix}" "watchsimulator${suffix}" "watchos${suffix}"
    combine_libraries "appletvos${suffix}" "appletvsimulator${suffix}" "tvos${suffix}"
    fi
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

    if [[ ! -z $BUILD_XCFRAMEWORK ]]; then
        rm -rf "${DESTINATION}/realm-monorepo.xcframework"
        cp -Rc realm-monorepo.xcframework "${DESTINATION}"
        rm -rf "${DESTINATION}/realm-monorepo-dbg.xcframework"
        cp -Rc realm-monorepo-dbg.xcframework "${DESTINATION}"
    fi
else
    rm -f "realm-monorepo-cocoa-${VERSION}.tar.xz"
    # .tar.gz package is used by realm-js, which uses the parser
    tar -czvf "realm-monorepo-cocoa-${VERSION}.tar.gz" --exclude "realm-monorepo*.xcframework" core
    # .tar.xz package is used by cocoa, which doesn't use the parser
    tar -cJvf "realm-monorepo-cocoa-${VERSION}.tar.xz" --exclude "realm-monorepo*.xcframework" core

    if [[ ! -z $BUILD_XCFRAMEWORK ]]; then
        rm -f "realm-parser-cocoa-${VERSION}.tar.xz"
        tar -cJvf "realm-parser-cocoa-${VERSION}.tar.xz" core/realm-parser*.xcframework
        rm -f "realm-monorepo-xcframework-${VERSION}.tar.xz"
        # until realmjs requires an xcframework, only package as .xz
        tar -cJvf "realm-monorepo-xcframework-${VERSION}.tar.xz" core/realm-monorepo.xcframework core/realm-monorepo-dbg.xcframework
    fi
fi
