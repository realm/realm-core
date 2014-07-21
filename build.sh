# NOTE: THIS SCRIPT IS SUPPOSED TO RUN IN A POSIX SHELL

# Enable tracing if TIGHTDB_SCRIPT_DEBUG is set
if [ -e $HOME/.tightdb ]; then
    . $HOME/.tightdb
fi
if [ "$TIGHTDB_SCRIPT_DEBUG" ]; then
    set -x
fi


if ! [ "$TIGHTDB_ORIG_CWD" ]; then
    TIGHTDB_ORIG_CWD="$(pwd)" || exit 1
    export ORIG_CWD
fi

dir="$(dirname "$0")" || exit 1
cd "$dir" || exit 1
TIGHTDB_HOME="$(pwd)" || exit 1
export TIGHTDB_HOME

MODE="$1"
[ $# -gt 0 ] && shift

# enabling replication support in core, now required for objective-c/ios
export TIGHTDB_ENABLE_REPLICATION=1

# Extensions corresponding with additional GIT repositories
EXTENSIONS="java python ruby objc node php c gui"
if [ "$TIGHTDB_ENABLE_REPLICATION" ]; then
    EXTENSIONS="$EXTENSIONS replication"
fi

# Auxiliary platforms
PLATFORMS="iphone"

IPHONE_EXTENSIONS="objc"
IPHONE_PLATFORMS="iPhoneOS iPhoneSimulator"
IPHONE_DIR="iphone-lib"

ANDROID_DIR="android-lib"
ANDROID_PLATFORMS="arm arm-v7a mips x86"

usage()
{
    cat 1>&2 << EOF
Unspecified or bad mode '$MODE'.
Available modes are:
    config clean build build-config-progs build-iphone build-android
    build-osx-framework build-cocoa
    check check-debug show-install install uninstall
    test-installed wipe-installed install-prod install-devel uninstall-prod
    uninstall-devel dist-copy src-dist bin-dist dist-deb dist-status
    dist-pull dist-checkout dist-config dist-clean dist-build
    dist-build-iphone dist-test dist-test-debug dist-install dist-uninstall
    dist-test-installed get-version set-version copy-tools
    build-test-ios-app:     build an iOS app for testing core on device
    test-ios-app:           execute the core tests on device
    leak-test-ios-app:      execute the core tests on device, monitor for leaks
EOF
}

map_ext_name_to_dir()
{
    local ext_name
    ext_name="$1"
    case $ext_name in
        *) echo "tightdb_$ext_name";;
    esac
    return 0
}

word_list_append()
{
    local list_name new_word list
    list_name="$1"
    new_word="$2"
    list="$(eval "printf \"%s\\n\" \"\${$list_name}\"")" || return 1
    if [ "$list" ]; then
        eval "$list_name=\"\$list \$new_word\""
    else
        eval "$list_name=\"\$new_word\""
    fi
    return 0
}

word_list_prepend()
{
    local list_name new_word list
    list_name="$1"
    new_word="$2"
    list="$(eval "printf \"%s\\n\" \"\${$list_name}\"")" || return 1
    if [ "$list" ]; then
        eval "$list_name=\"\$new_word \$list\""
    else
        eval "$list_name=\"\$new_word\""
    fi
    return 0
}

path_list_prepend()
{
    local list_name new_path list
    list_name="$1"
    new_path="$2"
    list="$(eval "printf \"%s\\n\" \"\${$list_name}\"")" || return 1
    if [ "$list" ]; then
        eval "$list_name=\"\$new_path:\$list\""
    else
        eval "$list_name=\"\$new_path\""
    fi
    return 0
}

word_list_reverse()
{
    local arg
    if [ "$#" -gt "0" ]; then
        arg="$1"
        shift
        word_list_reverse "$@"
        echo "$arg"
    fi
}



# Setup OS specific stuff
OS="$(uname)" || exit 1
ARCH="$(uname -m)" || exit 1
MAKE="make"
LD_LIBRARY_PATH_NAME="LD_LIBRARY_PATH"
if [ "$OS" = "Darwin" ]; then
    LD_LIBRARY_PATH_NAME="DYLD_LIBRARY_PATH"
fi
if ! printf "%s\n" "$MODE" | grep -q '^\(src-\|bin-\)\?dist'; then
    NUM_PROCESSORS=""
    if [ "$OS" = "Darwin" ]; then
        NUM_PROCESSORS="$(sysctl -n hw.ncpu)" || exit 1
    else
        if [ -r "/proc/cpuinfo" ]; then
            NUM_PROCESSORS="$(cat /proc/cpuinfo | grep -E 'processor[[:space:]]*:' | wc -l)" || exit 1
        fi
    fi
    if [ "$NUM_PROCESSORS" ]; then
        word_list_prepend MAKEFLAGS "-j$NUM_PROCESSORS" || exit 1
        export MAKEFLAGS

        if ! [ "$UNITTEST_THREADS" ]; then
            export UNITTEST_THREADS="$NUM_PROCESSORS"
        fi
    fi
fi
IS_REDHAT_DERIVATIVE=""
if [ -e /etc/redhat-release ] || grep -q "Amazon" /etc/system-release 2>/dev/null; then
    IS_REDHAT_DERIVATIVE="1"
fi
PLATFORM_HAS_LIBRARY_PATH_ISSUE=""
if [ "$IS_REDHAT_DERIVATIVE" ]; then
    PLATFORM_HAS_LIBRARY_PATH_ISSUE="1"
fi


find_iphone_sdk()
{
    local platform_home sdks version path x version2 sorted highest ambiguous
    platform_home="$1"
    sdks="$platform_home/Developer/SDKs"
    version=""
    dir=""
    ambiguous=""
    cd "$sdks" || return 1
    for x in *; do
        settings="$sdks/$x/SDKSettings"
        version2="$(defaults read "$sdks/$x/SDKSettings" Version)" || return 1
        if ! printf "%s\n" "$version2" | grep -q '^[0-9][0-9]*\(\.[0-9][0-9]*\)\{0,3\}$'; then
            echo "Uninterpretable 'Version' '$version2' in '$settings'" 1>&2
            return 1
        fi
        if [ "$version" ]; then
            sorted="$(printf "%s\n%s\n" "$version" "$version2" | sort -t . -k 1,1nr -k 2,2nr -k 3,3nr -k 4,4nr)" || return 1
            highest="$(printf "%s\n" "$sorted" | head -n 1)" || return 1
            if [ "$highest" = "$version2" ]; then
                if [ "$highest" = "$version" ]; then
                    ambiguous="1"
                else
                    version="$version2"
                    dir="$x"
                    ambiguous=""
                fi
            fi
        else
            version="$version2"
            dir="$x"
        fi
    done
    if [ "$ambiguous" ]; then
        echo "Ambiguous highest SDK version '$version' in '$sdks'" 1>&2
        return 1
    fi
    printf "%s\n" "$dir"
}

# Find the path of most recent version of the installed Android NDKs
find_android_ndk()
{
    local ndks ndks_index current_ndk latest_ndk sorted highest result

    ndks_index=0

    # If homebrew is installed...
    if [ -d "/usr/local/Cellar/android-ndk" ]; then
        ndks[$ndks_index]="/usr/local/Cellar/android-ndk"
        ((ndks_index = ndks_index + 1))
    fi
    if [ -d "/usr/local/android-ndk" ]; then
        ndks[$ndks_index]="/usr/local/android-ndk"
        ((ndks_index = ndks_index + 1))
    fi
    if [ "$ndks_index" -eq 0 ]; then
        return 1
    fi

    latest_ndk=""
    result=""
    for ndk in "${ndks[@]}"; do
        for i in $(cd "$ndk" && echo *); do
            if [ -f "$ndk/$i/RELEASE.TXT" ]; then
                current_ndk=$(sed 's/\(r\)\([1-9]\{1,\}\)\([a-z]\)/\1.\2.\3/' < "$ndk/$i/RELEASE.TXT") || return 1
                sorted="$(printf "%s\n%s\n" "$current_ndk" "$latest_ndk" | sort -t . -k 2,2nr -k 3,3r)" || return 1
                highest="$(printf "%s\n" "$sorted" | head -n 1)" || return 1
                if [ $current_ndk = $highest ]; then
                    result=$ndk/$i
                fi
            fi
        done
    done

    if [ -z $result ]; then
        return 1
    fi

    printf "%s\n" "$result"
}

CONFIG_MK="src/config.mk"

require_config()
{
    cd "$TIGHTDB_HOME" || return 1
    if ! [ -e "$CONFIG_MK" ]; then
        cat 1>&2 <<EOF
ERROR: Found no configuration!
You need to run 'sh build.sh config [PREFIX]'.
EOF
        return 1
    fi
    echo "Using existing configuration in $CONFIG_MK:"
    cat "$CONFIG_MK" | sed 's/^/    /' || return 1
}

auto_configure()
{
    cd "$TIGHTDB_HOME" || return 1
    if [ -e "$CONFIG_MK" ]; then
        require_config || return 1
    else
        echo "No configuration found. Running 'sh build.sh config' for you."
        sh build.sh config || return 1
    fi
}

get_config_param()
{
    local name home line value
    name="$1"
    home="$2"
    if ! [ "$home" ]; then
        home="$TIGHTDB_HOME"
    fi
    cd "$home" || return 1
    if ! [ -e "$CONFIG_MK" ]; then
        cat 1>&2 <<EOF
ERROR: Found no configuration!
You need to run 'sh build.sh config [PREFIX]'.
EOF
        return 1
    fi
    if ! line="$(grep "^$name *=" "$CONFIG_MK")"; then
        cat 1>&2 <<EOF
ERROR: Failed to read configuration parameter '$name'.
Maybe you need to rerun 'sh build.sh config [PREFIX]'.
EOF
        return 1
    fi
    value="$(printf "%s\n" "$line" | cut -d= -f2-)" || return 1
    value="$(printf "%s\n" "$value" | sed 's/^ *//')" || return 1
    printf "%s\n" "$value"
}

get_host_info()
{
    echo "\$ uname -a"
    uname -a
    if [ "$OS" = "Darwin" ]; then
        echo "\$ system_profiler SPSoftwareDataType"
        system_profiler SPSoftwareDataType | grep -v '^ *$'
    elif [ -e "/etc/issue" ]; then
        echo "\$ cat /etc/issue"
        cat "/etc/issue" | grep -v '^ *$'
    fi
}

get_compiler_info()
{
    local CC_CMD CXX_CMD LD_CMD
    CC_CMD="$($MAKE --no-print-directory get-cc)" || return 1
    CXX_CMD="$($MAKE --no-print-directory get-cxx)" || return 1
    LD_CMD="$($MAKE --no-print-directory get-ld)" || return 1
    echo "C compiler is '$CC_CMD' ($(which "$CC_CMD" 2>/dev/null))"
    echo "C++ compiler is '$CXX_CMD' ($(which "$CXX_CMD" 2>/dev/null))"
    echo "Linker is '$LD_CMD' ($(which "$LD_CMD" 2>/dev/null))"
    for x in $(printf "%s\n%s\n%s\n" "$CC_CMD" "$CXX_CMD" "$LD_CMD" | sort -u); do
        echo
        echo "\$ $x --version"
        $x --version 2>&1 | grep -v '^ *$'
    done
    if [ "$OS" = "Darwin" ]; then
        if xcode-select --print-path >/dev/null 2>&1; then
            echo
            echo "\$ xcodebuild -version"
            xcodebuild -version 2>&1 | grep -v '^ *$'
        fi
    fi
}

get_dist_log_path()
{
    local stem temp_dir path dir files max next
    stem="$1"
    temp_dir="$2"
    if [ "$TIGHTDB_DIST_LOG_FILE" ]; then
        path="$TIGHTDB_DIST_LOG_FILE"
    else
        if [ "$TIGHTDB_DIST_HOME" ]; then
            dir="$TIGHTDB_DIST_HOME/log"
        else
            dir="$temp_dir/log"
        fi
        mkdir -p "$dir" || return 1
        files="$(cd "$dir" && (ls *.log 2>/dev/null || true))" || return 1
        max="$(printf "%s\n" "$files" | grep '^[0-9][0-9]*_' | cut -d_ -f1 | sort -n | tail -n1)"
        max="$(printf "%s\n" "$max" | sed 's/^0*//')"
        next="$((max+1))" || return 1
        path="$dir/$(printf "%03d" "$next")_$stem.log"
    fi
    printf "%s\n" "$path"
}


case "$MODE" in

    "config")
        install_prefix="$1"
        if ! [ "$install_prefix" ]; then
            install_prefix="/usr/local"
        fi
        install_exec_prefix="$($MAKE --no-print-directory prefix="$install_prefix" get-exec-prefix)" || exit 1
        install_includedir="$($MAKE --no-print-directory prefix="$install_prefix" get-includedir)" || exit 1
        install_bindir="$($MAKE --no-print-directory prefix="$install_prefix" get-bindir)" || exit 1
        install_libdir="$($MAKE --no-print-directory prefix="$install_prefix" get-libdir)" || exit 1
        install_libexecdir="$($MAKE --no-print-directory prefix="$install_prefix" get-libexecdir)" || exit 1

        tightdb_version="unknown"
        if [ "$TIGHTDB_VERSION" ]; then
            tightdb_version="$TIGHTDB_VERSION"
        elif value="$(git describe 2>/dev/null)"; then
            tightdb_version="$(printf "%s\n" "$value" | sed 's/^v//')" || exit 1
        fi

        enable_replication="no"
        if [ "$TIGHTDB_ENABLE_REPLICATION" ]; then
            enable_replication="yes"
        fi

        enable_alloc_set_zero="no"
        if [ "$TIGHTDB_ENABLE_ALLOC_SET_ZERO" ]; then
            enable_alloc_set_zero="yes"
        fi

        # Find Xcode
        xcode_home="none"
        arm64_supported=""
        if [ "$OS" = "Darwin" ]; then
            if path="$(xcode-select --print-path 2>/dev/null)"; then
                xcode_home="$path"
            fi
            xcodebuild="$xcode_home/usr/bin/xcodebuild"
            version="$("$xcodebuild" -version)" || exit 1
            version="$(printf "%s" "$version" | grep -E '^Xcode +[0-9]+\.[0-9]' | head -n1)"
            version="$(printf "%s" "$version" | sed 's/^Xcode *\([0-9A-Z_.-]*\).*$/\1/')" || exit 1
            if ! printf "%s" "$version" | grep -q -E '^[0-9]+(\.[0-9]+)+$'; then
                echo "Failed to determine Xcode version using \`$xcodebuild -version\`" 1>&2
                exit 1
            fi
            major="$(printf "%s" "$version" | cut -d. -f1)" || exit 1
            if [ "$major" -ge "5" ]; then
                arm64_supported="1"
            fi
        fi

        # Find iPhone SDKs
        iphone_sdks=""
        iphone_sdks_avail="no"
        if [ "$xcode_home" != "none" ]; then
            # Xcode provides the iPhoneOS SDK
            iphone_sdks_avail="yes"
            for x in $IPHONE_PLATFORMS; do
                platform_home="$xcode_home/Platforms/$x.platform"
                if ! [ -e "$platform_home/Info.plist" ]; then
                    echo "Failed to find '$platform_home/Info.plist'"
                    iphone_sdks_avail="no"
                else
                    sdk="$(find_iphone_sdk "$platform_home")" || exit 1
                    if ! [ "$sdk" ]; then
                        echo "Found no SDKs in '$platform_home'"
                        iphone_sdks_avail="no"
                    else
                        if [ "$x" = "iPhoneSimulator" ]; then
                            archs="i386,x86_64"
                        elif [  "$x" = "iPhoneOS" ]; then
                            archs="armv7,armv7s"
                            if [ "$arm64_supported" ]; then
                                archs="$archs,arm64"
                            fi
                        else
                            continue
                        fi
                        word_list_append "iphone_sdks" "$x:$sdk:$archs" || exit 1
                    fi
                fi
            done
        fi

        # Find Android NDK
        if [ "$ANDROID_NDK_HOME" ]; then
            android_ndk_home="$ANDROID_NDK_HOME"
        else
            android_ndk_home="$(find_android_ndk)" || android_ndk_home="none"
        fi

        cat >"$CONFIG_MK" <<EOF
TIGHTDB_VERSION       = $tightdb_version
INSTALL_PREFIX        = $install_prefix
INSTALL_EXEC_PREFIX   = $install_exec_prefix
INSTALL_INCLUDEDIR    = $install_includedir
INSTALL_BINDIR        = $install_bindir
INSTALL_LIBDIR        = $install_libdir
INSTALL_LIBEXECDIR    = $install_libexecdir
ENABLE_REPLICATION    = $enable_replication
ENABLE_ALLOC_SET_ZERO = $enable_alloc_set_zero
XCODE_HOME            = $xcode_home
IPHONE_SDKS           = ${iphone_sdks:-none}
IPHONE_SDKS_AVAIL     = $iphone_sdks_avail
ANDROID_NDK_HOME      = $android_ndk_home
EOF
        if ! [ "$INTERACTIVE" ]; then
            echo "New configuration in $CONFIG_MK:"
            cat "$CONFIG_MK" | sed 's/^/    /' || exit 1
            echo "Done configuring"
        fi
        exit 0
        ;;

    "clean")
        auto_configure || exit 1
        export TIGHTDB_HAVE_CONFIG="1"
        $MAKE clean || exit 1
        if [ "$OS" = "Darwin" ]; then
            for x in $IPHONE_PLATFORMS; do
                $MAKE -C "src/tightdb" clean BASE_DENOM="$x" || exit 1
            done
            $MAKE -C "src/tightdb" clean BASE_DENOM="ios" || exit 1
            if [ -e "$IPHONE_DIR" ]; then
                echo "Removing '$IPHONE_DIR'"
                rm -fr "$IPHONE_DIR/include" || exit 1
                rm -f "$IPHONE_DIR/libtightdb-ios.a" "$IPHONE_DIR/libtightdb-ios-dbg.a" || exit 1
                rm -f "$IPHONE_DIR/tightdb-config" "$IPHONE_DIR/tightdb-config-dbg" || exit 1
                rmdir "$IPHONE_DIR" || exit 1
            fi
        fi
        for x in $ANDROID_PLATFORMS; do
            denom="android-$x"
            $MAKE -C "src/tightdb" clean BASE_DENOM="$denom" || exit 1
        done
        if [ -e "$ANDROID_DIR" ];then
            echo "Removing '$ANDROID_DIR'"
            rm -rf "$ANDROID_DIR"
        fi
        echo "Done cleaning"
        exit 0
        ;;

    "build")
        auto_configure || exit 1
        export TIGHTDB_HAVE_CONFIG="1"
        TIGHTDB_ENABLE_FAT_BINARIES="1" $MAKE || exit 1
        echo "Done building"
        exit 0
        ;;

    "build-config-progs")
        auto_configure || exit 1
        export TIGHTDB_HAVE_CONFIG="1"
        # FIXME: Apparently, there are fluke cases where timestamps
        # are such that <src/tightdb/util/config.h> is not recreated
        # automatically by src/tightdb/Makfile. Using --always-make is
        # a work-around.
        TIGHTDB_ENABLE_FAT_BINARIES="1" $MAKE --always-make -C "src/tightdb" "tightdb-config" "tightdb-config-dbg" || exit 1
        echo "Done building config programs"
        exit 0
        ;;

    "build-osx")
        auto_configure || exit 1
        export TIGHTDB_HAVE_CONFIG="1"
        (
            cd src/tightdb
            export TIGHTDB_ENABLE_FAT_BINARIES="1"
            TIGHTDB_ENABLE_FAT_BINARIES="1" $MAKE libtightdb.a EXTRA_CFLAGS="-fPIC -DPIC" || exit 1
            TIGHTDB_ENABLE_FAT_BINARIES="1" $MAKE libtightdb-dbg.a EXTRA_CFLAGS="-fPIC -DPIC" || exit 1
        ) || exit 1
        exit 0
        ;;

    "build-iphone")
        auto_configure || exit 1
        export TIGHTDB_HAVE_CONFIG="1"
        iphone_sdks_avail="$(get_config_param "IPHONE_SDKS_AVAIL")" || exit 1
        if [ "$iphone_sdks_avail" != "yes" ]; then
            echo "ERROR: Required iPhone SDKs are not available" 1>&2
            exit 1
        fi
        temp_dir="$(mktemp -d /tmp/tightdb.build-iphone.XXXX)" || exit 1
        mkdir "$temp_dir/platforms" || exit 1
        xcode_home="$(get_config_param "XCODE_HOME")" || exit 1
        iphone_sdks="$(get_config_param "IPHONE_SDKS")" || exit 1
        for x in $iphone_sdks; do
            platform="$(printf "%s\n" "$x" | cut -d: -f1)" || exit 1
            sdk="$(printf "%s\n" "$x" | cut -d: -f2)" || exit 1
            archs="$(printf "%s\n" "$x" | cut -d: -f3 | sed 's/,/ /g')" || exit 1
            cflags_arch="-stdlib=libc++"
            for y in $archs; do
                word_list_append "cflags_arch" "-arch $y" || exit 1
            done
            sdk_root="$xcode_home/Platforms/$platform.platform/Developer/SDKs/$sdk"
            $MAKE -C "src/tightdb" "libtightdb-$platform.a" "libtightdb-$platform-dbg.a" BASE_DENOM="$platform" CFLAGS_ARCH="$cflags_arch -isysroot $sdk_root" || exit 1
            mkdir "$temp_dir/platforms/$platform" || exit 1
            cp "src/tightdb/libtightdb-$platform.a"     "$temp_dir/platforms/$platform/libtightdb.a"     || exit 1
            cp "src/tightdb/libtightdb-$platform-dbg.a" "$temp_dir/platforms/$platform/libtightdb-dbg.a" || exit 1
        done
        TIGHTDB_ENABLE_FAT_BINARIES="1" $MAKE -C "src/tightdb" "tightdb-config-ios" "tightdb-config-ios-dbg" BASE_DENOM="ios" CFLAGS_ARCH="-DTIGHTDB_CONFIG_IOS" || exit 1
        mkdir -p "$IPHONE_DIR" || exit 1
        echo "Creating '$IPHONE_DIR/libtightdb-ios.a'"
        lipo "$temp_dir/platforms"/*/"libtightdb.a"     -create -output "$IPHONE_DIR/libtightdb-ios.a"     || exit 1
        echo "Creating '$IPHONE_DIR/libtightdb-ios-dbg.a'"
        lipo "$temp_dir/platforms"/*/"libtightdb-dbg.a" -create -output "$IPHONE_DIR/libtightdb-ios-dbg.a" || exit 1
        echo "Copying headers to '$IPHONE_DIR/include'"
        mkdir -p "$IPHONE_DIR/include" || exit 1
        cp "src/tightdb.hpp" "$IPHONE_DIR/include/" || exit 1
        mkdir -p "$IPHONE_DIR/include/tightdb" || exit 1
        inst_headers="$(cd "src/tightdb" && $MAKE --no-print-directory get-inst-headers)" || exit 1
        (cd "src/tightdb" && tar czf "$temp_dir/headers.tar.gz" $inst_headers) || exit 1
        (cd "$TIGHTDB_HOME/$IPHONE_DIR/include/tightdb" && tar xzmf "$temp_dir/headers.tar.gz") || exit 1
        for x in "tightdb-config" "tightdb-config-dbg"; do
            echo "Creating '$IPHONE_DIR/$x'"
            y="$(printf "%s\n" "$x" | sed 's/tightdb-config/tightdb-config-ios/')" || exit 1
            cp "src/tightdb/$y" "$TIGHTDB_HOME/$IPHONE_DIR/$x" || exit 1
        done
        echo "Done building"
        exit 0
        ;;

    "build-android")
        auto_configure || exit 1
        export TIGHTDB_HAVE_CONFIG="1"
        android_ndk_home="$(get_config_param "ANDROID_NDK_HOME")" || exit 1
        if [ "$android_ndk_home" = "none" ]; then
            cat 1>&2 <<EOF
ERROR: Android NDK was not found during configuration.
Please do one of the following:
 * Install an NDK in /usr/local/android-ndk
 * Provide the path to the NDK in the environment variable ANDROID_NDK_HOME
 * If on OSX and using Homebrew install the package android-sdk
EOF
            exit 1
        fi
        export TIGHTDB_ANDROID="1"
        mkdir -p "$ANDROID_DIR" || exit 1
        for target in $ANDROID_PLATFORMS; do
            temp_dir="$(mktemp -d /tmp/tightdb.build-android.XXXX)" || exit 1
            if [ "$target" = "arm" ]; then
                platform="8"
            else
                platform="9"
            fi
            # Note that `make-standalone-toolchain.sh` is written for
            # `bash` and must therefore be executed by `bash`.
            make_toolchain="$android_ndk_home/build/tools/make-standalone-toolchain.sh"
            bash "$make_toolchain" --platform="android-$platform" --install-dir="$temp_dir" --arch="$target" || exit 1
            android_prefix="$target"
            if [ "$target" = "arm-v7a" ]; then
                android_prefix="arm"
            elif [ "$target" = "mips" ]; then
                android_prefix="mipsel"
            elif [ "$target" = "x86" ]; then
                android_prefix="i686"
            fi
            path="$temp_dir/bin:$PATH"
            cc="$(cd "$temp_dir/bin" && echo $android_prefix-linux-*-gcc)" || exit 1
            cflags_arch=""
            if [ "$target" = "arm" ]; then
                word_list_append "cflags_arch" "-mthumb" || exit 1
            elif [ "$target" = "arm-v7a" ]; then
                word_list_append "cflags_arch" "-mthumb -march=armv7-a -mfloat-abi=softfp -mfpu=vfpv3-d16" || exit 1
            fi
            denom="android-$target"
            PATH="$path" CC="$cc" $MAKE -C "src/tightdb" CC_IS="gcc" BASE_DENOM="$denom" CFLAGS_ARCH="$cflags_arch" "libtightdb-$denom.a" || exit 1
            cp "src/tightdb/libtightdb-$denom.a" "$ANDROID_DIR" || exit 1
            rm -rf "$temp_dir" || exit 1
        done
        echo "Copying headers to '$ANDROID_DIR/include'"
        mkdir -p "$ANDROID_DIR/include" || exit 1
        cp "src/tightdb.hpp" "$ANDROID_DIR/include/" || exit 1
        mkdir -p "$ANDROID_DIR/include/tightdb" || exit 1
        inst_headers="$(cd "src/tightdb" && $MAKE --no-print-directory get-inst-headers)" || exit 1
        temp_dir="$(mktemp -d /tmp/tightdb.build-android.XXXX)" || exit 1
        (cd "src/tightdb" && tar czf "$temp_dir/headers.tar.gz" $inst_headers) || exit 1
        (cd "$TIGHTDB_HOME/$ANDROID_DIR/include/tightdb" && tar xzmf "$temp_dir/headers.tar.gz") || exit 1
        ;;

   "build-cocoa")
        if [ "$OS" != "Darwin" ]; then
            echo "zip for iOS/OSX can only be generated under OS X."
            exit 0
        fi

        # the user can specify where to find realm-cocoa repository
        realm_cocoa_dir="$1"
        if [ -z "$realm_cocoa_dir" ]; then
            realm_cocoa_dir="../realm-cocoa"
        fi

        sh build.sh build-osx || exit 1
        sh build.sh build-iphone || exit 1

        echo "Copying files"
        tmpdir=$(mktemp -d /tmp/$$.XXXXXX) || exit 1
        realm_version="$(sh build.sh get-version)" || exit 1
        BASENAME="core"
        rm -f "$BASENAME-$realm_version.zip" || exit 1
        mkdir -p "$tmpdir/$BASENAME/include" || exit 1
        cp "$IPHONE_DIR/libtightdb-ios.a" "$tmpdir/$BASENAME" || exit 1
        cp "$IPHONE_DIR/libtightdb-ios-dbg.a" "$tmpdir/$BASENAME" || exit 1
        cp -r "$IPHONE_DIR/include/"* "$tmpdir/$BASENAME/include" || exit 1
        for x in $iphone_sdks; do
            platform="$(printf "%s\n" "$x" | cut -d: -f1)" || exit 1
            cp "src/tightdb/libtightdb-$platform.a" "$tmpdir/$BASENAME" || exit 1
            cp "src/tightdb/libtightdb-$platform-dbg.a" "$tmpdir/$BASENAME" || exit 1
        done
        cp src/tightdb/libtightdb.a "$tmpdir/$BASENAME" || exit 1
        cp src/tightdb/libtightdb-dbg.a "$tmpdir/$BASENAME" || exit 1
        command -v pandoc >/dev/null 2>&1 || { echo "Pandoc is required but it's not installed.  Aborting." >&2; exit 1; }
        pandoc -f markdown -t plain -o "$tmpdir/$BASENAME/release_notes.txt" release_notes.md || exit 1
        cp LICENSE "$tmpdir/$BASENAME/"

        echo "Create zip file: '$BASENAME-$realm_version.zip'"
        (cd $tmpdir && zip -r -q "$BASENAME-$realm_version.zip" "$BASENAME") || exit 1
        mv "$tmpdir/$BASENAME-$realm_version.zip" . || exit 1

        echo "Unzipping in '$realm_cocoa_dir'"
        mkdir -p "$realm_cocoa_dir" || exit 1
        rm -rf "$realm_cocoa_dir/$BASENAME" || exit 1
        cur_dir="$(pwd)"
        (cd "$realm_cocoa_dir" && unzip -qq "$cur_dir/$BASENAME-$realm_version.zip") || exit 1

        rm -rf "$tmpdir" || exit 1
        echo "Done"
        exit 0
        ;;

      "build-osx-framework")
        if [ "$OS" != "Darwin" ]; then
            echo "Framework for OS X can only be generated under Mac OS X."
            exit 0
        fi

        realm_version="$(sh build.sh get-version)"
        BASENAME="RealmCore"
        FRAMEWORK="$BASENAME.framework"
        rm -rf "$FRAMEWORK" || exit 1
        rm -f realm-core-osx-*.zip || exit 1

        mkdir -p "$FRAMEWORK/Headers/tightdb" || exit 1
        if [ ! -f "src/tightdb/libtightdb.a" ]; then
            echo "\"src/tightdb/libtightdb.a\" missing."
            echo "Did you forget to build?"
            exit 1
        fi

        cp "src/tightdb/libtightdb.a" "$FRAMEWORK/$BASENAME" || exit 1
        cp "src/tightdb.hpp" "$FRAMEWORK/Headers/tightdb.hpp" || exit 1
        for header in $(cd "src/tightdb" && $MAKE --no-print-directory get-inst-headers); do
            mkdir -p "$(dirname "$FRAMEWORK/Headers/tightdb/$header")" || exit 1
            cp "src/tightdb/$header" "$FRAMEWORK/Headers/tightdb/$header" || exit 1
        done
        find "$FRAMEWORK/Headers" -iregex "^.*\.[ch]\(pp\)\{0,1\}$" \
            -exec sed -i '' -e "s/<tightdb\(.*\)>/<$BASENAME\/tightdb\1>/g" {} \; || exit 1

        zip -r -q realm-core-osx-$realm_version.zip $FRAMEWORK || exit 1
        echo "Core framework for OS X can be found under $FRAMEWORK and realm-core-osx-$realm_version.zip."
        exit 0
        ;;

    "test"|"test-debug"|\
    "check"|"check-debug"|\
    "memcheck"|"memcheck-debug"|\
    "check-doc-examples"|\
    "check-testcase"|"check-testcase-debug"|\
    "memcheck-testcase"|"memcheck-testcase-debug")
        auto_configure || exit 1
        export TIGHTDB_HAVE_CONFIG="1"
        $MAKE "$MODE" || exit 1
        echo "Test passed"
        exit 0
        ;;

    "build-test-ios-app")
        # For more documentation, see test/ios/README.md.

        ARCHS="\$(ARCHS_STANDARD_INCLUDING_64_BIT)"
        while getopts da: OPT; do
            case $OPT in
                d)  DEBUG=1
                    ;;
                a)  ARCHS=$OPTARG
                    ;;
                *)  usage
                    exit 1
                    ;;
            esac
        done

        sh build.sh build-iphone

        TMPL_DIR="test/ios/template"
        TEST_DIR="test/ios/app"
        rm -rf "$TEST_DIR/"* || exit 1
        mkdir -p "$TEST_DIR" || exit 1

        APP="iOSTestCoreApp"
        TEST_APP="${APP}Tests"

        APP_DIR="$TEST_DIR/$APP"
        TEST_APP_DIR="$TEST_DIR/$TEST_APP"

        # Copy the test files into the app tests subdirectory
        PASSIVE_SUBDIRS="$($MAKE -C ./test --no-print-directory get-passive-subdirs)" || exit 1
        PASSIVE_SUBDIRS="$PASSIVE_SUBDIRS android ios" # dirty skip
        PASSIVE_SUBDIRS="$(echo "$PASSIVE_SUBDIRS" | sed -E 's/ +/|/g')" || exit 1
        # Naive copy, i.e. copy everything.
        ## Avoid recursion (extra precaution) and passive subdirs.
        ## Avoid non-source-code files.
        ## Retain directory structure.
        (cd ./test && find -E . \
            ! -iregex "^\./(ios|$PASSIVE_SUBDIRS)/.*$" \
            -a -iregex "^.*\.[ch](pp)?$" \
            -exec rsync -qR {} "../$TEST_APP_DIR" \;) || exit 1
        rm "$TEST_APP_DIR/main.cpp"

        # Gather resources
        RESOURCES="$($MAKE -C ./test --no-print-directory get-test-resources)" || exit 1
        (cd ./test && rsync $RESOURCES "../$APP_DIR") || exit 1
        RESOURCES="$(echo "$RESOURCES" | sed -E "s/(^| )/\1$APP\//g")" || exit 1

        # Set up frameworks, or rather, static libraries.
        rm -rf "$TEST_DIR/$IPHONE_DIR" || exit 1
        cp -r "../tightdb/$IPHONE_DIR" "$TEST_DIR/$IPHONE_DIR" || exit 1
        if [ -n "$DEBUG" ]; then
            FRAMEWORK="$IPHONE_DIR/libtightdb-ios-dbg.a"
        else
            FRAMEWORK="$IPHONE_DIR/libtightdb-ios.a"
        fi
        FRAMEWORKS="'$FRAMEWORK'"
        HEADER_SEARCH_PATHS="'$IPHONE_DIR/include/**'"

        # Other flags
        if [ -n "$DEBUG" ]; then
            OTHER_CPLUSPLUSFLAGS="'-DTIGHTDB_DEBUG'"
        fi
        
        # Initialize app directory
        cp -r "test/ios/template/App/"* "$APP_DIR" || exit 1
        mv "$APP_DIR/App-Info.plist" "$APP_DIR/$APP-Info.plist" || exit 1
        mv "$APP_DIR/App-Prefix.pch" "$APP_DIR/$APP-Prefix.pch" || exit 1

        # Gather all the test sources in a Python-friendly format.
        ## The indentation is to make it look pretty in the Gyp file.
        APP_SOURCES=$(cd $TEST_DIR && find "$TEST_APP" -type f | \
            sed -E "s/^(.*)$/                '\1',/") || exit 1
        TEST_APP_SOURCES="$APP_SOURCES"

        # Prepare for GYP
        ARCHS="$(echo "'$ARCHS'," | sed -E "s/ /', '/g")" || exit 1
        RESOURCES="$(echo "'$RESOURCES'," | sed -E "s/ /', '/g")" || exit 1
        
        # Generate a Gyp file.
        . "$TMPL_DIR/App.gyp.sh"

        # Run gyp, generating an .xcodeproj folder with a project.pbxproj file.
        gyp --depth="$TEST_DIR" "$TEST_DIR/$APP.gyp" || exit 1

        ## Collect the main app id from the project.pbxproj file.
        APP_ID=$(cat "$TEST_DIR/$APP.xcodeproj/project.pbxproj" | tr -d '\n' | \
            egrep -o "remoteGlobalIDString.*?remoteInfo = $APP;" | \
            head -n 1 | \
            sed 's/remoteGlobalIDString = \([A-F0-9]*\);.*/\1/') || exit 1

        ## Collect the test app id from the project.pbxproj file.
        TEST_APP_ID=$(cat "$TEST_DIR/$APP.xcodeproj/project.pbxproj" | tr -d '\n' | \
            egrep -o "remoteGlobalIDString.*?remoteInfo = $TEST_APP;" | \
            head -n 1 | \
            sed 's/remoteGlobalIDString = \([A-F0-9]*\);.*/\1/') || exit 1

        ## Generate a scheme with a test action.
        USER=$(whoami)
        mkdir -p "$TEST_DIR/$APP.xcodeproj/xcuserdata"
        mkdir -p "$TEST_DIR/$APP.xcodeproj/xcuserdata/$USER.xcuserdatad"
        mkdir -p "$TEST_DIR/$APP.xcodeproj/xcuserdata/$USER.xcuserdatad/xcschemes"

        . "$TMPL_DIR/App.scheme.sh"

        echo "The app is now available under $TEST_DIR."
        echo "Use sh build.sh (leak-)test-ios-app to run the app on device."

        exit 0
        ;;

    "test-ios-app")
        # Prerequisites: build-test-ios-app
        # For more documentation, see test/ios/README.md
        (cd "test/ios/app" &&
            if [ $# -eq 0 ]; then
                xcodebuild test -scheme iOSTestCoreApp \
                    -destination "platform=iOS,name=tightdb's iPad"
            else
                xcodebuild test -scheme iOSTestCoreApp "$@"
            fi)
        exit 0
        ;;

    "leak-test-ios-app")
        # Prerequisites: build-test-ios-app
        # For more documentation, see test/ios/README.md
        DEV="tightdb's iPad"
        if [ $# -ne 0 ]; then
            DEV="$@"
        fi
        (cd "test/ios/app" && instruments -t ../template/Leaks.tracetemplate \
            -w "$DEV" iOSTestCoreApp)
        exit 0
        ;;

    "gdb"|"gdb-debug"|\
    "gdb-testcase"|"gdb-testcase-debug"|\
    "performance"|"benchmark"|"benchmark-"*|\
    "lcov"|"gcovr")
        auto_configure || exit 1
        export TIGHTDB_HAVE_CONFIG="1"
        $MAKE "$MODE" || exit 1
        exit 0
        ;;

    "show-install")
        temp_dir="$(mktemp -d /tmp/tightdb.show-install.XXXX)" || exit 1
        mkdir "$temp_dir/fake-root" || exit 1
        DESTDIR="$temp_dir/fake-root" sh build.sh install >/dev/null || exit 1
        (cd "$temp_dir/fake-root" && find * \! -type d >"$temp_dir/list") || exit 1
        sed 's|^|/|' <"$temp_dir/list" || exit 1
        rm -fr "$temp_dir/fake-root" || exit 1
        rm "$temp_dir/list" || exit 1
        rmdir "$temp_dir" || exit 1
        exit 0
        ;;

    "release-notes-prerelease")
        RELEASE_HEADER="# $(sh build.sh get-version) Release notes" || exit 1
        sed -i.bak "1s/.*/$RELEASE_HEADER/" release_notes.md || exit 1
        rm release_notes.md.bak
        exit 0
        ;;

    "release-notes-postrelease")
        cat doc/release_notes_template.md release_notes.md > release_notes.md.new || exit 1
        mv release_notes.md.new release_notes.md || exit 1
        exit 0
        ;;

    "get-version")
        version_file="src/tightdb/version.hpp"
        tightdb_ver_major="$(grep ^"#define TIGHTDB_VER_MAJOR" $version_file | awk '{print $3}')" || exit 1
        tightdb_ver_minor="$(grep ^"#define TIGHTDB_VER_MINOR" $version_file | awk '{print $3}')" || exit 1
        tightdb_ver_patch="$(grep ^"#define TIGHTDB_VER_PATCH" $version_file | awk '{print $3}')" || exit 1
        echo "$tightdb_ver_major.$tightdb_ver_minor.$tightdb_ver_patch"
        exit 0
        ;;

    "set-version")
        tightdb_version="$1"
        version_file="src/tightdb/version.hpp"
        tightdb_ver_major="$(echo "$tightdb_version" | cut -f1 -d.)" || exit 1
        tightdb_ver_minor="$(echo "$tightdb_version" | cut -f2 -d.)" || exit 1
        tightdb_ver_patch="$(echo "$tightdb_version" | cut -f3 -d.)" || exit 1

        # update version.hpp
        printf ",s/#define TIGHTDB_VER_MAJOR .*/#define TIGHTDB_VER_MAJOR $tightdb_ver_major/\nw\nq" | ed -s "$version_file" || exit 1
        printf ",s/#define TIGHTDB_VER_MINOR .*/#define TIGHTDB_VER_MINOR $tightdb_ver_minor/\nw\nq" | ed -s "$version_file" || exit 1
        printf ",s/#define TIGHTDB_VER_PATCH .*/#define TIGHTDB_VER_PATCH $tightdb_ver_patch/\nw\nq" | ed -s "$version_file" || exit 1

        sh tools/add-deb-changelog.sh "$tightdb_version" "$(pwd)/debian/changelog.in" libtightdb || exit 1
        sh build.sh release-notes-prerelease || exit 1
        exit 0
        ;;

    "copy-tools")
        repo="$1"
        if [ -z "$repo" ]; then
            echo "No path to repository set: sh build.sh copy-tools <path-to-repo>"
            exit 1
        fi
        if ! [ -e "$repo" ]; then
            echo "Repository $repo does not exist"
            exit 1
        fi
        mkdir -p $repo/tools || exit 1

        tools="add-deb-changelog.sh"
        for t in $tools; do
            cp tools/$t $repo/tools || exit 1
            sed -i -e "1i # Do not edit here - go to core repository" $repo/tools/$t || exit 1
        done
        exit 0
        ;;

    "install")
        require_config || exit 1
        export TIGHTDB_HAVE_CONFIG="1"
        $MAKE install-only DESTDIR="$DESTDIR" || exit 1
        if [ "$USER" = "root" ] && which ldconfig >/dev/null 2>&1; then
            ldconfig || exit 1
        fi
        echo "Done installing"
        exit 0
        ;;

    "install-prod")
        require_config || exit 1
        export TIGHTDB_HAVE_CONFIG="1"
        $MAKE install-only DESTDIR="$DESTDIR" INSTALL_FILTER="shared-libs,progs" || exit 1
        if [ "$USER" = "root" ] && which ldconfig >/dev/null 2>&1; then
            ldconfig || exit 1
        fi
        echo "Done installing"
        exit 0
        ;;

    "install-devel")
        require_config || exit 1
        export TIGHTDB_HAVE_CONFIG="1"
        $MAKE install-only DESTDIR="$DESTDIR" INSTALL_FILTER="static-libs,dev-progs,headers" || exit 1
        echo "Done installing"
        exit 0
        ;;

    "uninstall")
        require_config || exit 1
        export TIGHTDB_HAVE_CONFIG="1"
        $MAKE uninstall || exit 1
        if [ "$USER" = "root" ] && which ldconfig >/dev/null 2>&1; then
            ldconfig || exit 1
        fi
        echo "Done uninstalling"
        exit 0
        ;;

    "uninstall-prod")
        require_config || exit 1
        export TIGHTDB_HAVE_CONFIG="1"
        $MAKE uninstall INSTALL_FILTER="shared-libs,progs" || exit 1
        if [ "$USER" = "root" ] && which ldconfig >/dev/null 2>&1; then
            ldconfig || exit 1
        fi
        echo "Done uninstalling"
        exit 0
        ;;

    "uninstall-devel")
        require_config || exit 1
        export TIGHTDB_HAVE_CONFIG="1"
        $MAKE uninstall INSTALL_FILTER="static-libs,dev-progs,headers" || exit 1
        echo "Done uninstalling"
        exit 0
        ;;

    "test-installed")
        require_config || exit 1
        install_bindir="$(get_config_param "INSTALL_BINDIR")" || exit 1
        path_list_prepend PATH "$install_bindir" || exit 1
        $MAKE -C "test-installed" clean || exit 1
        $MAKE -C "test-installed" check  || exit 1
        echo "Test passed"
        exit 0
        ;;

    "wipe-installed")
        if [ "$OS" = "Darwin" ]; then
            find /usr/ /Library/Java /System/Library/Java /Library/Python -ipath '*tightdb*' -delete || exit 1
        else
            find /usr/ -ipath '*tightdb*' -delete && ldconfig || exit 1
        fi
        exit 0
        ;;

    "src-dist"|"bin-dist")
        if [ "$MODE" = "bin-dist" ]; then
            PREBUILT_CORE="1"
        fi

        EXTENSION_AVAILABILITY_REQUIRED="1"
        if [ "$#" -eq 1 -a "$1" = "all" ]; then
            INCLUDE_EXTENSIONS="$EXTENSIONS"
            INCLUDE_PLATFORMS="$PLATFORMS"
        elif [ "$#" -eq 1 -a "$1" = "avail" ]; then
            INCLUDE_EXTENSIONS="$EXTENSIONS"
            INCLUDE_PLATFORMS="$PLATFORMS"
            EXTENSION_AVAILABILITY_REQUIRED=""
        elif [ "$#" -eq 1 -a "$1" = "none" ]; then
            INCLUDE_EXTENSIONS=""
            INCLUDE_PLATFORMS=""
        elif [ $# -ge 1 -a "$1" != "not" ]; then
            for x in "$@"; do
                found=""
                for y in $EXTENSIONS $PLATFORMS; do
                    if [ "$x" = "$y" ]; then
                        found="1"
                        break
                    fi
                done
                if ! [ "$found" ]; then
                    echo "Bad extension name '$x'" 1>&2
                    exit 1
                fi
            done
            INCLUDE_EXTENSIONS=""
            for x in $EXTENSIONS; do
                for y in "$@"; do
                    if [ "$x" = "$y" ]; then
                        word_list_append INCLUDE_EXTENSIONS "$x" || exit 1
                        break
                    fi
                done
            done
            INCLUDE_PLATFORMS=""
            for x in $PLATFORMS; do
                for y in "$@"; do
                    if [ "$x" = "$y" ]; then
                        word_list_append INCLUDE_PLATFORMS "$x" || exit 1
                        break
                    fi
                done
            done
        elif [ "$#" -ge 1 -a "$1" = "not" ]; then
            if [ "$#" -eq 1 ]; then
                echo "Please specify which extensions to exclude" 1>&2
                echo "Available extensions are: $EXTENSIONS $PLATFORMS" 1>&2
                exit 1
            fi
            shift
            for x in "$@"; do
                found=""
                for y in $EXTENSIONS $PLATFORMS; do
                    if [ "$x" = "$y" ]; then
                        found="1"
                        break
                    fi
                done
                if ! [ "$found" ]; then
                    echo "Bad extension name '$x'" 1>&2
                    exit 1
                fi
            done
            INCLUDE_EXTENSIONS=""
            for x in $EXTENSIONS; do
                found=""
                for y in "$@"; do
                    if [ "$x" = "$y" ]; then
                        found="1"
                        break
                    fi
                done
                if ! [ "$found" ]; then
                    word_list_append INCLUDE_EXTENSIONS "$x" || exit 1
                fi
            done
            INCLUDE_PLATFORMS=""
            for x in $PLATFORMS; do
                found=""
                for y in "$@"; do
                    if [ "$x" = "$y" ]; then
                        found="1"
                        break
                    fi
                done
                if ! [ "$found" ]; then
                    word_list_append INCLUDE_PLATFORMS "$x" || exit 1
                fi
            done
        else
            cat 1>&2 <<EOF
Please specify which extensions (and auxiliary platforms) to include:
  Specify 'all' to include all extensions.
  Specify 'avail' to include all available extensions.
  Specify 'none' to exclude all extensions.
  Specify 'EXT1  [EXT2]...' to include the specified extensions.
  Specify 'not  EXT1  [EXT2]...' to exclude the specified extensions.
Available extensions: $EXTENSIONS
Available auxiliary platforms: $PLATFORMS
EOF
            exit 1
        fi

        VERSION="$(git describe)" || exit 1
        if ! [ "$TIGHTDB_VERSION" ]; then
            TIGHTDB_VERSION="$(printf "%s\n" "$VERSION" | sed 's/^v//')" || exit 1
            export TIGHTDB_VERSION
        fi
        NAME="tightdb-$TIGHTDB_VERSION"

        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist.XXXX)" || exit 1

        LOG_FILE="$TEMP_DIR/build-dist.log"
        log_message()
        {
            local msg
            msg="$1"
            printf "\n>>>>>>>> %s\n" "$msg" >> "$LOG_FILE"
        }
        message()
        {
            local msg
            msg="$1"
            log_message "$msg"
            printf "%s\n" "$msg"
        }
        warning()
        {
            local msg
            msg="$1"
            message "WARNING: $msg"
        }
        fatal()
        {
            local msg
            msg="$1"
            message "FATAL: $msg"
        }

        if (
            message "Log file is here: $LOG_FILE"
            message "Checking availability of extensions"
            failed=""
            AVAIL_EXTENSIONS=""
            for x in $INCLUDE_EXTENSIONS; do
                EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                if ! [ -e "$EXT_HOME/build.sh" ]; then
                    if [ "$EXTENSION_AVAILABILITY_REQUIRED" ]; then
                        fatal "Missing extension '$EXT_HOME'"
                        failed="1"
                    else
                        warning "Missing extension '$EXT_HOME'"
                    fi
                    continue
                fi
                word_list_append AVAIL_EXTENSIONS "$x" || exit 1
            done
            # Checking that each extension is capable of copying
            # itself to the package
            FAKE_PKG_DIR="$TEMP_DIR/fake_pkg"
            mkdir "$FAKE_PKG_DIR" || exit 1
            NEW_AVAIL_EXTENSIONS=""
            for x in $AVAIL_EXTENSIONS; do
                EXT_DIR="$(map_ext_name_to_dir "$x")" || exit 1
                EXT_HOME="../$EXT_DIR"
                echo "Testing transfer of extension '$x' to package" >> "$LOG_FILE"
                mkdir "$FAKE_PKG_DIR/$EXT_DIR" || exit 1
                if ! sh "$EXT_HOME/build.sh" dist-copy "$FAKE_PKG_DIR/$EXT_DIR" >>"$LOG_FILE" 2>&1; then
                    if [ "$EXTENSION_AVAILABILITY_REQUIRED" ]; then
                        fatal "Transfer of extension '$x' to test package failed"
                        failed="1"
                    else
                        warning "Transfer of extension '$x' to test package failed"
                    fi
                    continue
                fi
                word_list_append NEW_AVAIL_EXTENSIONS "$x" || exit 1
            done
            if [ "$failed" ]; then
                exit 1;
            fi
            AVAIL_EXTENSIONS="$NEW_AVAIL_EXTENSIONS"


            # Check state of working directories
            if [ "$(git status --porcelain)" ]; then
                warning "Dirty working directory '../$(basename "$TIGHTDB_HOME")'"
            fi
            for x in $AVAIL_EXTENSIONS; do
                EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                if [ "$(cd "$EXT_HOME" && git status --porcelain)" ]; then
                    warning "Dirty working directory '$EXT_HOME'"
                fi
            done

            INCLUDE_IPHONE=""
            for x in $INCLUDE_PLATFORMS; do
                if [ "$x" = "iphone" ]; then
                    INCLUDE_IPHONE="1"
                    break
                fi
            done

            message "Continuing with these parts:"
            {
                BRANCH="$(git rev-parse --abbrev-ref HEAD)" || exit 1
                platforms=""
                if [ "$INCLUDE_IPHONE" ]; then
                    platforms="+iphone"
                fi
                echo "core  ->  .  $BRANCH  $VERSION  $platforms"
                for x in $AVAIL_EXTENSIONS; do
                    EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                    EXT_BRANCH="$(cd "$EXT_HOME" && git rev-parse --abbrev-ref HEAD)" || exit 1
                    EXT_VERSION="$(cd "$EXT_HOME" && git describe --always)" || exit 1
                    platforms=""
                    if [ "$INCLUDE_IPHONE" ]; then
                        for y in $IPHONE_EXTENSIONS; do
                            if [ "$x" = "$y" ]; then
                                platforms="+iphone"
                            fi
                        done
                    fi
                    echo "$x  ->  $EXT_HOME  $EXT_BRANCH  $EXT_VERSION  $platforms"
                done
            } >"$TEMP_DIR/continuing_with" || exit 1
            column -t "$TEMP_DIR/continuing_with" >"$TEMP_DIR/continuing_with2" || exit 1
            sed 's/^/  /' "$TEMP_DIR/continuing_with2" >"$TEMP_DIR/continuing_with3" || exit 1
            tee -a "$LOG_FILE" <"$TEMP_DIR/continuing_with3"

            # Setup package directory
            PKG_DIR="$TEMP_DIR/$NAME"
            mkdir "$PKG_DIR" || exit 1
            mkdir "$PKG_DIR/log" || exit 1

            AUGMENTED_EXTENSIONS="$AVAIL_EXTENSIONS"
            word_list_prepend AUGMENTED_EXTENSIONS "c++" || exit 1

            AUGMENTED_EXTENSIONS_IPHONE="c++"
            for x in $AVAIL_EXTENSIONS; do
                for y in $IPHONE_EXTENSIONS; do
                    if [ "$x" = "$y" ]; then
                        word_list_append AUGMENTED_EXTENSIONS_IPHONE "$x" || exit 1
                    fi
                done
            done

            cat >"$PKG_DIR/build" <<EOF
#!/bin/sh

TIGHTDB_ORIG_CWD="\$(pwd)" || exit 1
export ORIG_CWD

dir="\$(dirname "\$0")" || exit 1
cd "\$dir" || exit 1
TIGHTDB_DIST_HOME="\$(pwd)" || exit 1
export TIGHTDB_DIST_HOME

export TIGHTDB_VERSION="$TIGHTDB_VERSION"
export PREBUILT_CORE="$PREBUILT_CORE"
export DISABLE_CHEETAH_CODE_GEN="1"

EXTENSIONS="$AUGMENTED_EXTENSIONS"

if [ \$# -gt 0 -a "\$1" = "interactive" ]; then
    shift
    if [ \$# -eq 0 ]; then
        echo "At least one extension must be specified."
        echo "Available extensions: \$EXTENSIONS"
        exit 1
    fi
    EXT=""
    while [ \$# -gt 0 ]; do
        e=\$1
        if [ \$(echo \$EXTENSIONS | tr " " "\n" | grep -c \$e) -eq 0 ]; then
            echo "\$e is not an available extension."
            echo "Available extensions: \$EXTENSIONS"
            exit 1
        fi
        EXT="\$EXT \$e"
        shift
    done
    INTERACTIVE=1 sh build config \$EXT || exit 1
    INTERACTIVE=1 sh build build || exit 1
    sudo -p "Password for installation: " INTERACTIVE=1 sh build install || exit 1
    echo
    echo "Installation report"
    echo "-------------------"
    echo "The following files have been installed:"
    for x in \$EXT; do
        if [ "\$x" != "c++" -a "\$x" != "c" ]; then
            echo "\$x:"
            sh $debug tightdb_\$x/build.sh install-report
            if [ $? -eq 1 ]; then
                echo " no files has been installed."
            fi
        fi
    done

    echo
    echo "Examples can be copied to the folder tightdb_examples in your home directory (\$HOME)."
    echo "Do you wish to copy examples to your home directory (y/n)?"
    read answer
    if [ \$(echo \$answer | grep -c ^[yY]) -eq 1 ]; then
        mkdir -p \$HOME/tightdb_examples
        for x in \$EXT; do
            if [ "\$x" != "c++" -a "\$x" != "c" ]; then
                cp -a tightdb_\$x/examples \$HOME/tightdb_examples/\$x
            fi
        done
        if [ \$(echo \$EXT | grep -c c++) -eq 1 ]; then
            cp -a tightdb/examples \$HOME/tightdb_examples/c++
        fi
        if [ \$(echo \$EXT | grep -c java) -eq 1 ]; then
            find \$HOME/tightdb_examples/java -name build.xml -exec sed -i -e 's/value="\.\.\/\.\.\/lib"/value="\/usr\/local\/share\/java"/' \{\} \\;
            find \$HOME/tightdb_examples/java -name build.xml -exec sed -i -e 's/"jnipath" value=".*" \/>/"jnipath" value="\/Library\/Java\/Extensions" \/>/' \{\} \\;
        fi

        echo "Examples can be found in \$HOME/tightdb_examples."
        echo "Please consult the README.md files in each subdirectory for information"
        echo "on how to build and run the examples."
    fi
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "clean" ]; then
    sh tightdb/build.sh dist-clean || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "build" ]; then
    sh tightdb/build.sh dist-build || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "build-iphone" -a "$INCLUDE_IPHONE" ]; then
    sh tightdb/build.sh dist-build-iphone || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "test" ]; then
    sh tightdb/build.sh dist-test || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "test-debug" ]; then
    sh tightdb/build.sh dist-test-debug || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "install" ]; then
    sh tightdb/build.sh dist-install || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "test-installed" ]; then
    sh tightdb/build.sh dist-test-installed || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "uninstall" ]; then
    sh tightdb/build.sh dist-uninstall \$EXTENSIONS || exit 1
    exit 0
fi

if [ \$# -ge 1 -a "\$1" = "config" ]; then
    shift
    if [ \$# -eq 1 -a "\$1" = "all" ]; then
        sh tightdb/build.sh dist-config \$EXTENSIONS || exit 1
        exit 0
    fi
    if [ \$# -eq 1 -a "\$1" = "none" ]; then
        sh tightdb/build.sh dist-config || exit 1
        exit 0
    fi
    if [ \$# -ge 1 ]; then
        all_found="1"
        for x in "\$@"; do
            found=""
            for y in \$EXTENSIONS; do
                if [ "\$y" = "\$x" ]; then
                    found="1"
                    break
                fi
            done
            if ! [ "\$found" ]; then
                echo "No such extension '\$x'" 1>&2
                all_found=""
                break
            fi
        done
        if [ "\$all_found" ]; then
            sh tightdb/build.sh dist-config "\$@" || exit 1
            exit 0
        fi
        echo 1>&2
    fi
fi

cat README 1>&2
exit 1
EOF
            chmod +x "$PKG_DIR/build"

            if ! [ "$INTERACTIVE" ]; then
                cat >"$PKG_DIR/README" <<EOF
TightDB version $TIGHTDB_VERSION

Configure specific extensions:    ./build  config  EXT1  [EXT2]...
Configure all extensions:         ./build  config  all
Configure only the core library:  ./build  config  none
Start building from scratch:      ./build  clean
Build configured extensions:      ./build  build
Install what was built:           sudo  ./build  install
Check state of installation:      ./build  test-installed
Uninstall configured extensions:  sudo  ./build  uninstall

The following steps should generally suffice:

    ./build config all
    ./build build
    sudo ./build install

Available extensions are: ${AUGMENTED_EXTENSIONS:-None}

EOF
                if [ "$PREBUILT_CORE" ]; then
                    cat >>"$PKG_DIR/README" <<EOF
During installation, the prebuilt core library will be installed along
with all the extensions that were successfully built. The C++
extension is part of the core library, so the effect of including
'c++' in the 'config' step is simply to request that the C++ header
files (and other files needed for development) are to be installed.
EOF
                else
                    cat >>"$PKG_DIR/README" <<EOF
When building is requested, the core library will be built along with
all the extensions that you have configured. The C++ extension is part
of the core library, so the effect of including 'c++' in the 'config'
step is simply to request that the C++ header files (and other files
needed for development) are to be installed.

For information on prerequisites when building the core library, see
tightdb/README.md.
EOF
                fi

                cat >>"$PKG_DIR/README" <<EOF

For information on prerequisites of the each individual extension, see
the README.md file in the corresponding subdirectory.
EOF

                if [ "$INCLUDE_IPHONE" ]; then
                    cat >>"$PKG_DIR/README" <<EOF

To build TightDB for iPhone, run the following command:

    ./build build-iphone

The following iPhone extensions are availble: ${AUGMENTED_EXTENSIONS_IPHONE:-None}

Files produced for extension EXT will be placed in a subdirectory
named "iphone-EXT".
EOF
                fi

                cat >>"$PKG_DIR/README" <<EOF

Note that each build step creates a new log file in the subdirectory
called "log". When contacting TightDB at <support@tightdb.com> because
of a problem in the installation process, we recommend that you attach
all these log files as a bundle to your mail.
EOF

                for x in $AVAIL_EXTENSIONS; do
                    EXT_DIR="$(map_ext_name_to_dir "$x")" || exit 1
                    EXT_HOME="../$EXT_DIR"
                    if REMARKS="$(sh "$EXT_HOME/build.sh" dist-remarks 2>&1)"; then
                        cat >>"$PKG_DIR/README" <<EOF

Remarks for '$x':

$REMARKS
EOF
                    fi
                done
            fi

            export DISABLE_CHEETAH_CODE_GEN="1"

            mkdir "$PKG_DIR/tightdb" || exit 1
            if [ "$PREBUILT_CORE" ]; then
                message "Building core library"
                PREBUILD_DIR="$TEMP_DIR/prebuild"
                mkdir "$PREBUILD_DIR" || exit 1
                sh "$TIGHTDB_HOME/build.sh" dist-copy "$PREBUILD_DIR" >>"$LOG_FILE" 2>&1 || exit 1
                (cd "$PREBUILD_DIR" && sh build.sh config && sh build.sh build) >>"$LOG_FILE" 2>&1 || exit 1

                if [ "$INCLUDE_IPHONE" ]; then
                    message "Building core library for 'iphone'"
                    (cd "$PREBUILD_DIR" && sh build.sh build-iphone) >>"$LOG_FILE" 2>&1 || exit 1
                fi

                message "Transferring prebuilt core library to package"
                mkdir "$TEMP_DIR/transfer" || exit 1
                cat >"$TEMP_DIR/transfer/include" <<EOF
/README.*
/build.sh
/libtightdb.spec
/config
/Makefile
/src/generic.mk
/src/project.mk
/src/config.mk
/src/Makefile
/src/tightdb.hpp
/src/tightdb/Makefile
/src/tightdb/util/config.sh
/src/tightdb/config_tool.cpp
/test/Makefile
/test/util/Makefile
/test-installed
/doc
EOF
                INST_HEADERS="$(cd "$PREBUILD_DIR/src/tightdb" && TIGHTDB_HAVE_CONFIG="1" $MAKE --no-print-directory get-inst-headers)" || exit 1
                INST_LIBS="$(cd "$PREBUILD_DIR/src/tightdb" && TIGHTDB_HAVE_CONFIG="1" $MAKE --no-print-directory get-inst-libraries)" || exit 1
                INST_PROGS="$(cd "$PREBUILD_DIR/src/tightdb" && TIGHTDB_HAVE_CONFIG="1" $MAKE --no-print-directory get-inst-programs)" || exit 1
                for x in $INST_HEADERS $INST_LIBS $INST_PROGS; do
                    echo "/src/tightdb/$x" >> "$TEMP_DIR/transfer/include"
                done
                grep -E -v '^(#.*)?$' "$TEMP_DIR/transfer/include" >"$TEMP_DIR/transfer/include2" || exit 1
                sed -e 's/\([.\[^$]\)/\\\1/g' -e 's|\*|[^/]*|g' -e 's|^\([^/]\)|^\\(.*/\\)\\{0,1\\}\1|' -e 's|^/|^|' -e 's|$|\\(/.*\\)\\{0,1\\}$|' "$TEMP_DIR/transfer/include2" >"$TEMP_DIR/transfer/include.bre" || exit 1
                (cd "$PREBUILD_DIR" && find -L * -type f) >"$TEMP_DIR/transfer/files1" || exit 1
                grep -f "$TEMP_DIR/transfer/include.bre" "$TEMP_DIR/transfer/files1" >"$TEMP_DIR/transfer/files2" || exit 1
                (cd "$PREBUILD_DIR" && tar czf "$TEMP_DIR/transfer/core.tar.gz" -T "$TEMP_DIR/transfer/files2") || exit 1
                (cd "$PKG_DIR/tightdb" && tar xzmf "$TEMP_DIR/transfer/core.tar.gz") || exit 1
                if [ "$INCLUDE_IPHONE" ]; then
                    cp -R "$PREBUILD_DIR/$IPHONE_DIR" "$PKG_DIR/tightdb/" || exit 1
                fi
                get_host_info >"$PKG_DIR/tightdb/.PREBUILD_INFO" || exit 1

                message "Running test suite for core library"
                if ! (cd "$PREBUILD_DIR" && sh build.sh test) >>"$LOG_FILE" 2>&1; then
                    warning "Test suite failed for core library"
                fi

                message "Running test suite for core library in debug mode"
                if ! (cd "$PREBUILD_DIR" && sh build.sh test-debug) >>"$LOG_FILE" 2>&1; then
                    warning "Test suite failed for core library in debug mode"
                fi
            else
                message "Transferring core library to package"
                sh "$TIGHTDB_HOME/build.sh" dist-copy "$PKG_DIR/tightdb" >>"$LOG_FILE" 2>&1 || exit 1
            fi

            for x in $AVAIL_EXTENSIONS; do
                message "Transferring extension '$x' to package"
                EXT_DIR="$(map_ext_name_to_dir "$x")" || exit 1
                EXT_HOME="../$EXT_DIR"
                mkdir "$PKG_DIR/$EXT_DIR" || exit 1
                sh "$EXT_HOME/build.sh" dist-copy "$PKG_DIR/$EXT_DIR" >>"$LOG_FILE" 2>&1 || exit 1
            done

            message "Zipping the package"
            (cd "$TEMP_DIR" && tar czf "$NAME.tar.gz" "$NAME/") || exit 1

            message "Extracting the package for test"
            TEST_DIR="$TEMP_DIR/test"
            mkdir "$TEST_DIR" || exit 1
            (cd "$TEST_DIR" && tar xzmf "$TEMP_DIR/$NAME.tar.gz") || exit 1
            TEST_PKG_DIR="$TEST_DIR/$NAME"

            install_prefix="$TEMP_DIR/test-install"
            mkdir "$install_prefix" || exit 1

            export TIGHTDB_DIST_LOG_FILE="$LOG_FILE"
            export TIGHTDB_DIST_NONINTERACTIVE="1"
            export TIGHTDB_TEST_INSTALL_PREFIX="$install_prefix"

            error=""
            log_message "Testing './build config all'"
            if ! "$TEST_PKG_DIR/build" config all; then
                [ -e "$TEST_PKG_DIR/tightdb/.DIST_CORE_WAS_CONFIGURED" ] || exit 1
                error="1"
            fi

            log_message "Testing './build clean'"
            if ! "$TEST_PKG_DIR/build" clean; then
                error="1"
            fi

            log_message "Testing './build build'"
            if ! "$TEST_PKG_DIR/build" build; then
                [ -e "$TEST_PKG_DIR/tightdb/.DIST_CORE_WAS_BUILT" ] || exit 1
                error="1"
            fi

            log_message "Testing './build test'"
            if ! "$TEST_PKG_DIR/build" test; then
                error="1"
            fi

            log_message "Testing './build test-debug'"
            if ! "$TEST_PKG_DIR/build" test-debug; then
                error="1"
            fi

            log_message "Testing './build install'"
            if ! "$TEST_PKG_DIR/build" install; then
                [ -e "$TEST_PKG_DIR/tightdb/.DIST_CORE_WAS_INSTALLED" ] || exit 1
                error="1"
            fi

            # When testing against a prebuilt core library, we have to
            # work around the fact that it is not going to be
            # installed in the usual place. While the config programs
            # are rebuilt to reflect the unusual installation
            # directories, other programs (such as `tightdbd`) that
            # use the shared core library, are not, so we have to set
            # the runtime library path. Also, the core library will
            # look for `tightdbd` in the wrong place, so we have to
            # set `TIGHTDB_ASYNC_DAEMON` too.
            if [ "$PREBUILT_CORE" ]; then
                install_libdir="$(get_config_param "INSTALL_LIBDIR" "$TEST_PKG_DIR/tightdb")" || exit 1
                path_list_prepend "$LD_LIBRARY_PATH_NAME" "$install_libdir"  || exit 1
                export "$LD_LIBRARY_PATH_NAME"
                install_libexecdir="$(get_config_param "INSTALL_LIBEXECDIR" "$TEST_PKG_DIR/tightdb")" || exit 1
                export TIGHTDB_ASYNC_DAEMON="$install_libexecdir/tightdbd"
            fi

            log_message "Testing './build test-installed'"
            if ! "$TEST_PKG_DIR/build" test-installed; then
                error="1"
            fi

            # Copy the installation test directory to allow later inspection
            INSTALL_COPY="$TEMP_DIR/test-install-copy"
            cp -R "$TIGHTDB_TEST_INSTALL_PREFIX" "$INSTALL_COPY" || exit 1

            log_message "Testing './build uninstall'"
            if ! "$TEST_PKG_DIR/build" uninstall; then
                error="1"
            fi

            message "Checking that './build uninstall' leaves nothing behind"
            REMAINING_PATHS="$(cd "$TIGHTDB_TEST_INSTALL_PREFIX" && find * \! -type d -o -ipath '*tightdb*')" || exit 1
            if [ "$REMAINING_PATHS" ]; then
                fatal "Files and/or directories remain after uninstallation"
                printf "%s" "$REMAINING_PATHS" >>"$LOG_FILE"
                exit 1
            fi

            if [ "$INCLUDE_IPHONE" ]; then
                message "Testing platform 'iphone'"
                log_message "Testing './build build-iphone'"
                if ! "$TEST_PKG_DIR/build" build-iphone; then
                    error="1"
                fi
            fi

#            if [ "$error" ]; then
#                exit 1
#            fi

            exit 0

        ); then
            message 'SUCCESS!'
            message "Log file is here: $LOG_FILE"
            message "Package is here: $TEMP_DIR/$NAME.tar.gz"
            if [ "$PREBUILT_CORE" ]; then
                message "Distribution type: BINARY (prebuilt core library)"
            else
                message "Distribution type: SOURCE"
            fi
        else
            message 'FAILED!' 1>&2
            message "Log file is here: $LOG_FILE"
            exit 1
        fi
        exit 0
        ;;


#    "dist-check-avail")
#        for x in $EXTENSIONS; do
#            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
#            if [ -e "$EXT_HOME/build.sh" ]; then
#                echo ">>>>>>>> CHECKING AVAILABILITY OF '$x'"
#                if sh "$EXT_HOME/build.sh" check-avail; then
#                    echo 'YES!'
#                fi
#            fi
#        done
#        exit 0
#        ;;


    "dist-config")
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-config.XXXX)" || exit 1
        if ! which "make" >/dev/null 2>&1; then
            echo "ERROR: GNU make must be installed."
            if [ "$OS" = "Darwin" ]; then
                echo "Please install xcode and command-line tools and try again."
                echo "You can download them at https://developer.apple.com/downloads/index.action"
                echo "or consider to use https://github.com/kennethreitz/osx-gcc-installer"
            fi
            exit 1
        fi
        LOG_FILE="$(get_dist_log_path "config" "$TEMP_DIR")" || exit 1
        (
            echo "TightDB version: ${TIGHTDB_VERSION:-Unknown}"
            if [ -e ".PREBUILD_INFO" ]; then
                echo
                echo "PREBUILD HOST INFO:"
                cat ".PREBUILD_INFO"
            fi
            echo
            echo "BUILD HOST INFO:"
            get_host_info || exit 1
            echo
            get_compiler_info || exit 1
            echo
        ) >>"$LOG_FILE"
        ERROR=""
        rm -f ".DIST_CORE_WAS_CONFIGURED" || exit 1
        # When configuration is tested in the context of building a
        # distribution package, we have to reconfigure the core
        # library such that it will install into the temporary
        # directory (an unfortunate and ugly kludge).
        if [ "$PREBUILT_CORE" ] && ! [ "$TIGHTDB_TEST_INSTALL_PREFIX" ]; then
            touch ".DIST_CORE_WAS_CONFIGURED" || exit 1
        else
            if ! [ "$INTERACTIVE" ]; then
                if [ "$PREBUILT_CORE" ]; then
                    echo "RECONFIGURING Prebuilt core library (only for testing)" | tee -a "$LOG_FILE"
                else
                    echo "CONFIGURING Core library" | tee -a "$LOG_FILE"
                fi
            fi
            if [ "$INTERACTIVE" ]; then
                if ! sh "build.sh" config $TIGHTDB_TEST_INSTALL_PREFIX 2>&1 | tee -a "$LOG_FILE"; then
                    ERROR="1"
                fi
            else
                if ! sh "build.sh" config $TIGHTDB_TEST_INSTALL_PREFIX >>"$LOG_FILE" 2>&1; then
                    ERROR="1"
                fi
            fi
            if ! [ "$ERROR" ]; then
                # At this point we have to build the config commands
                # `tightdb-config` and `tightdb-config-dbg` such that
                # they are available during configuration and building
                # of extensions, just as if the core library has been
                # previously installed.
                if ! sh "build.sh" build-config-progs >>"$LOG_FILE" 2>&1; then
                    ERROR="1"
                fi
            fi
            if [ "$ERROR" ]; then
                echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
            else
                touch ".DIST_CORE_WAS_CONFIGURED" || exit 1
            fi
        fi
        # Copy the core library config programs into a dedicated
        # directory such that they are retained across 'clean'
        # operations.
        mkdir -p "config-progs" || exit 1
        for x in "tightdb-config" "tightdb-config-dbg"; do
            rm -f "config-progs/$x" || exit 1
            cp "src/tightdb/$x" "config-progs/" || exit 1
        done
        if ! [ "$ERROR" ]; then
            mkdir "$TEMP_DIR/select" || exit 1
            for x in "$@"; do
                touch "$TEMP_DIR/select/$x" || exit 1
            done
            rm -f ".DIST_CXX_WAS_CONFIGURED" || exit 1
            if [ -e "$TEMP_DIR/select/c++" ]; then
                if [ "$INTERACTIVE" ]; then
                    echo "Configuring extension 'c++'" | tee -a "$LOG_FILE"
                else
                    echo "CONFIGURING Extension 'c++'" | tee -a "$LOG_FILE"
                fi
                touch ".DIST_CXX_WAS_CONFIGURED" || exit 1
            fi
            export TIGHTDB_DIST_INCLUDEDIR="$TIGHTDB_HOME/src"
            export TIGHTDB_DIST_LIBDIR="$TIGHTDB_HOME/src/tightdb"
            path_list_prepend PATH "$TIGHTDB_HOME/config-progs" || exit 1
            export PATH
            for x in $EXTENSIONS; do
                EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                rm -f "$EXT_HOME/.DIST_WAS_CONFIGURED" || exit 1
                if [ -e "$TEMP_DIR/select/$x" ]; then
                    if [ "$INTERACTIVE" ]; then
                        echo "Configuring extension '$x'" | tee -a "$LOG_FILE"
                    else
                        echo "CONFIGURING Extension '$x'" | tee -a "$LOG_FILE"
                    fi
                    if [ "$INTERACTIVE" ]; then
                        if sh "$EXT_HOME/build.sh" config $TIGHTDB_TEST_INSTALL_PREFIX 2>&1 | tee -a "$LOG_FILE"; then
                            touch "$EXT_HOME/.DIST_WAS_CONFIGURED" || exit 1
                        else
                            ERROR="1"
                        fi
                    else
                        if sh "$EXT_HOME/build.sh" config $TIGHTDB_TEST_INSTALL_PREFIX >>"$LOG_FILE" 2>&1; then
                            touch "$EXT_HOME/.DIST_WAS_CONFIGURED" || exit 1
                        else
                            echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                            ERROR="1"
                        fi
                    fi
                fi
            done
            if ! [ "$INTERACTIVE" ]; then
                echo "DONE CONFIGURING" | tee -a "$LOG_FILE"
            fi
        fi
        if ! [ "$TIGHTDB_DIST_NONINTERACTIVE" ]; then
            if ! [ "$INTERACTIVE" ]; then
                if [ "$ERROR" ]; then
                    cat 1>&2 <<EOF

Note: Some parts could not be configured. You may be missing one or
more dependencies. Check the README file for details. If that does not
help, check the log file.
The log file is here: $LOG_FILE
EOF
                fi
                cat <<EOF

Run the following command to build the parts that were successfully
configured:

    ./build build

EOF
            fi
        fi
        if [ "$ERROR" ] && ! [ "$INTERACTIVE" ]; then
            exit 1
        fi
        exit 0
        ;;


    "dist-clean")
        if ! [ -e ".DIST_CORE_WAS_CONFIGURED" ]; then
            cat 1>&2 <<EOF
ERROR: Nothing was configured.
You need to run './build config' first.
EOF
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-clean.XXXX)" || exit 1
        LOG_FILE="$(get_dist_log_path "clean" "$TEMP_DIR")" || exit 1
        ERROR=""
        rm -f ".DIST_CORE_WAS_BUILT" || exit 1
        if ! [ "$PREBUILT_CORE" ]; then
            echo "CLEANING Core library" | tee -a "$LOG_FILE"
            if ! sh "build.sh" clean >>"$LOG_FILE" 2>&1; then
                echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                ERROR="1"
            fi
        fi
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/.DIST_WAS_CONFIGURED" ]; then
                echo "CLEANING Extension '$x'" | tee -a "$LOG_FILE"
                rm -f "$EXT_HOME/.DIST_WAS_BUILT" || exit 1
                if ! sh "$EXT_HOME/build.sh" clean >>"$LOG_FILE" 2>&1; then
                    echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                    ERROR="1"
                fi
            fi
        done
        if ! [ "$INTERACTIVE" ]; then
            echo "DONE CLEANING" | tee -a "$LOG_FILE"
        fi
        if [ "$ERROR" ] && ! [ "$TIGHTDB_DIST_NONINTERACTIVE" ]; then
            echo "Log file is here: $LOG_FILE" 1>&2
        fi
        if [ "$ERROR" ]; then
            exit 1
        fi
        exit 0
        ;;


    "dist-build")
        if ! [ -e ".DIST_CORE_WAS_CONFIGURED" ]; then
            cat 1>&2 <<EOF
ERROR: Nothing was configured.
You need to run './build config' first.
EOF
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-build.XXXX)" || exit 1
        LOG_FILE="$(get_dist_log_path "build" "$TEMP_DIR")" || exit 1
        (
            echo "TightDB version: ${TIGHTDB_VERSION:-Unknown}"
            if [ -e ".PREBUILD_INFO" ]; then
                echo
                echo "PREBUILD HOST INFO:"
                cat ".PREBUILD_INFO"
            fi
            echo
            echo "BUILD HOST INFO:"
            get_host_info || exit 1
            echo
            get_compiler_info || exit 1
            echo
        ) >>"$LOG_FILE"
        rm -f ".DIST_CORE_WAS_BUILT" || exit 1
        if [ "$PREBUILT_CORE" ]; then
            touch ".DIST_CORE_WAS_BUILT" || exit 1
            if [ "$INTERACTIVE" ]; then
                echo "Building core library"
            fi
        else
            if [ "$INTERACTIVE" ]; then
                echo "Building c++ library" | tee -a "$LOG_FILE"
            else
                echo "BUILDING Core library" | tee -a "$LOG_FILE"
            fi
            if sh "build.sh" build >>"$LOG_FILE" 2>&1; then
                touch ".DIST_CORE_WAS_BUILT" || exit 1
            else
                if [ "$INTERACTIVE" ]; then
                    echo '  > Failed!' | tee -a "$LOG_FILE"
                else
                    echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                fi
                if ! [ "$TIGHTDB_DIST_NONINTERACTIVE" ]; then
                    cat 1>&2 <<EOF

Note: The core library could not be built. You may be missing one or
more dependencies. Check the README file for details. If this does not
help, check the log file.
The log file is here: $LOG_FILE
EOF
                fi
                exit 1
            fi
        fi
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/.DIST_WAS_CONFIGURED" ]; then
                if [ "$INTERACTIVE" ]; then
                    echo "Building extension '$x'" | tee -a "$LOG_FILE"
                else
                    echo "BUILDING Extension '$x'" | tee -a "$LOG_FILE"
                fi
                rm -f "$EXT_HOME/.DIST_WAS_BUILT" || exit 1
                if sh "$EXT_HOME/build.sh" build >>"$LOG_FILE" 2>&1; then
                    touch "$EXT_HOME/.DIST_WAS_BUILT" || exit 1
                else
                    if [ "$INTERACTIVE" ]; then
                        echo '  > Failed!' | tee -a "$LOG_FILE"
                    else
                        echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                    fi
                    ERROR="1"
                fi
            fi
        done
        if [ "$INTERACTIVE" ]; then
            echo "Done building" | tee -a "$LOG_FILE"
        else
            echo "DONE BUILDING" | tee -a "$LOG_FILE"
        fi
        if ! [ "$TIGHTDB_DIST_NONINTERACTIVE" ]; then
            if ! [ "$INTERACTIVE" ]; then
                if [ "$ERROR" ]; then
                    cat 1>&2 <<EOF

Note: Some parts failed to build. You may be missing one or more
dependencies. Check the README file for details. If this does not
help, check the log file.
The log file is here: $LOG_FILE

EOF
                fi
                cat <<EOF

Run the following command to install the parts that were successfully
built:

    sudo ./build install

EOF
            fi
        fi
        if [ "$ERROR" ]; then
            exit 1
        fi
        exit 0
        ;;


    "dist-build-iphone")
        if ! [ -e ".DIST_CORE_WAS_CONFIGURED" ]; then
            cat 1>&2 <<EOF
ERROR: Nothing was configured.
You need to run './build config' first.
EOF
            exit 1
        fi
        dist_home="$TIGHTDB_HOME"
        if [ "$TIGHTDB_DIST_HOME" ]; then
            dist_home="$TIGHTDB_DIST_HOME"
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-build-iphone.XXXX)" || exit 1
        LOG_FILE="$(get_dist_log_path "build-iphone" "$TEMP_DIR")" || exit 1
        (
            echo "TightDB version: ${TIGHTDB_VERSION:-Unknown}"
            if [ -e ".PREBUILD_INFO" ]; then
                echo
                echo "PREBUILD HOST INFO:"
                cat ".PREBUILD_INFO"
            fi
            echo
            echo "BUILD HOST INFO:"
            get_host_info || exit 1
            echo
            get_compiler_info || exit 1
            echo
        ) >>"$LOG_FILE"
        rm -f ".DIST_CORE_WAS_BUILT_FOR_IPHONE" || exit 1
        if [ "$PREBUILT_CORE" ]; then
            touch ".DIST_CORE_WAS_BUILT_FOR_IPHONE" || exit 1
        else
            echo "BUILDING Core library for iPhone" | tee -a "$LOG_FILE"
            if sh "build.sh" build-iphone >>"$LOG_FILE" 2>&1; then
                touch ".DIST_CORE_WAS_BUILT_FOR_IPHONE" || exit 1
            else
                echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                if ! [ "$TIGHTDB_DIST_NONINTERACTIVE" ]; then
                    cat 1>&2 <<EOF

Note: You may be missing one or more dependencies. Check the README
file for details. If this does not help, check the log file.
The log file is here: $LOG_FILE
EOF
                fi
                exit 1
            fi
        fi
        if [ -e ".DIST_CXX_WAS_CONFIGURED" ]; then
            mkdir -p "$dist_home/iphone-c++" || exit 1
            cp -R "$IPHONE_DIR"/* "$dist_home/iphone-c++/" || exit 1
        fi
        for x in $IPHONE_EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/.DIST_WAS_CONFIGURED" ]; then
                echo "BUILDING Extension '$x' for iPhone" | tee -a "$LOG_FILE"
                rm -f "$EXT_HOME/.DIST_WAS_BUILT_FOR_IPHONE" || exit 1
                if sh "$EXT_HOME/build.sh" build-iphone >>"$LOG_FILE" 2>&1; then
                    mkdir -p "$dist_home/iphone-$x" || exit 1
                    cp -R "$EXT_HOME/$IPHONE_DIR"/* "$dist_home/iphone-$x/" || exit 1
                    touch "$EXT_HOME/.DIST_WAS_BUILT_FOR_IPHONE" || exit 1
                else
                    echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                    ERROR="1"
                fi
            fi
        done
        if ! [ "$INTERACTIVE" ]; then
            echo "DONE BUILDING" | tee -a "$LOG_FILE"
        fi
        if ! [ "$TIGHTDB_DIST_NONINTERACTIVE" ]; then
            if [ "$ERROR" ]; then
                cat 1>&2 <<EOF

Note: Some parts failed to build. You may be missing one or more
dependencies. Check the README file for details. If this does not
help, check the log file.
The log file is here: $LOG_FILE

Files produced for a successfully built extension EXT have been placed
in a subdirectory named "iphone-EXT".
EOF
            else
                cat <<EOF

Files produced for extension EXT have been placed in a subdirectory
named "iphone-EXT".
EOF
            fi
        fi
        if [ "ERROR" ]; then
            exit 1
        fi
        exit 0
        ;;


    "dist-test"|"dist-test-debug")
        test_mode="test"
        test_msg="TESTING %s"
        async_daemon="tightdbd"
        if [ "$MODE" = "dist-test-debug" ]; then
            test_mode="test-debug"
            test_msg="TESTING %s in debug mode"
            async_daemon="tightdbd-dbg"
        fi
        if ! [ -e ".DIST_CORE_WAS_BUILT" ]; then
            cat 1>&2 <<EOF
ERROR: Nothing to test.
You need to run './build build' first.
EOF
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-$test_mode.XXXX)" || exit 1
        LOG_FILE="$(get_dist_log_path "$test_mode" "$TEMP_DIR")" || exit 1
        (
            echo "TightDB version: ${TIGHTDB_VERSION:-Unknown}"
            if [ -e ".PREBUILD_INFO" ]; then
                echo
                echo "PREBUILD HOST INFO:"
                cat ".PREBUILD_INFO"
            fi
            echo
            echo "BUILD HOST INFO:"
            get_host_info || exit 1
            echo
        ) >>"$LOG_FILE"
        ERROR=""
        if ! [ "$PREBUILT_CORE" ]; then
            printf "$test_msg\n" "Core library" | tee -a "$LOG_FILE"
            if ! sh "build.sh" "$test_mode" >>"$LOG_FILE" 2>&1; then
                echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                ERROR="1"
            fi
        fi
        # We set `LD_LIBRARY_PATH` and `TIGHTDB_ASAYNC_DAEMON` here to be able
        # to test extensions before installation of the core library.
        path_list_prepend "$LD_LIBRARY_PATH_NAME" "$TIGHTDB_HOME/src/tightdb"  || exit 1
        export "$LD_LIBRARY_PATH_NAME"
        export TIGHTDB_ASYNC_DAEMON="$TIGHTDB_HOME/src/tightdb/$async_daemon"
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/.DIST_WAS_BUILT" ]; then
                printf "$test_msg\n" "Extension '$x'" | tee -a "$LOG_FILE"
                if ! sh "$EXT_HOME/build.sh" "$test_mode" >>"$LOG_FILE" 2>&1; then
                    echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                    ERROR="1"
                fi
            fi
        done
        if ! [ "$INTERACTIVE" ]; then
            echo "DONE TESTING" | tee -a "$LOG_FILE"
        fi
        if [ "$ERROR" ] && ! [ "$TIGHTDB_DIST_NONINTERACTIVE" ]; then
            echo "Log file is here: $LOG_FILE" 1>&2
        fi
        if [ "$ERROR" ]; then
            exit 1
        fi
        exit 0
        ;;

    "dist-install")
        if ! [ -e ".DIST_CORE_WAS_BUILT" ]; then
            cat 1>&2 <<EOF
ERROR: Nothing to install.
You need to run './build build' first.
EOF
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-install.XXXX)" || exit 1
        chmod a+rx "$TEMP_DIR" || exit 1
        LOG_FILE="$(get_dist_log_path "install" "$TEMP_DIR")" || exit 1
        touch "$LOG_FILE" || exit 1
        chmod a+r "$LOG_FILE" || exit 1
        (
            echo "TightDB version: ${TIGHTDB_VERSION:-Unknown}"
            if [ -e ".PREBUILD_INFO" ]; then
                echo
                echo "PREBUILD HOST INFO:"
                cat ".PREBUILD_INFO"
            fi
            echo
            echo "BUILD HOST INFO:"
            get_host_info || exit 1
            echo
        ) >>"$LOG_FILE"
        ERROR=""
        NEED_USR_LOCAL_LIB_NOTE=""
        if ! [ "$INTERACTIVE" ]; then
            echo "INSTALLING Core library" | tee -a "$LOG_FILE"
        fi
        if sh build.sh install-prod >>"$LOG_FILE" 2>&1; then
            touch ".DIST_CORE_WAS_INSTALLED" || exit 1
            if [ -e ".DIST_CXX_WAS_CONFIGURED" ]; then
                if [ "$INTERACTIVE" ]; then
                    echo "Installing 'c++' (core)" | tee -a "$LOG_FILE"
                else
                    echo "INSTALLING Extension 'c++'" | tee -a "$LOG_FILE"
                fi
                if sh build.sh install-devel >>"$LOG_FILE" 2>&1; then
                    touch ".DIST_CXX_WAS_INSTALLED" || exit 1
                    NEED_USR_LOCAL_LIB_NOTE="$PLATFORM_HAS_LIBRARY_PATH_ISSUE"
                else
                    if [ "$INTERACTIVE" ]; then
                        echo '  > Failed!' | tee -a "$LOG_FILE"
                    else
                        echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                    fi
                    ERROR="1"
                fi
            fi
            for x in $EXTENSIONS; do
                EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                if [ -e "$EXT_HOME/.DIST_WAS_CONFIGURED" -a -e "$EXT_HOME/.DIST_WAS_BUILT" ]; then
                    if [ "$INTERACTIVE" ]; then
                        echo "Installing extension '$x'" | tee -a "$LOG_FILE"
                    else
                        echo "INSTALLING Extension '$x'" | tee -a "$LOG_FILE"
                    fi
                    if sh "$EXT_HOME/build.sh" install >>"$LOG_FILE" 2>&1; then
                        touch "$EXT_HOME/.DIST_WAS_INSTALLED" || exit 1
                        if [ "$x" = "c" -o "$x" = "objc" ]; then
                            NEED_USR_LOCAL_LIB_NOTE="$PLATFORM_HAS_LIBRARY_PATH_ISSUE"
                        fi
                    else
                        if [ "$INTERACTIVE" ]; then
                            echo '  > Failed!' | tee -a "$LOG_FILE"
                        else
                            echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                        fi
                        ERROR="1"
                    fi
                fi
            done
            if [ "$NEED_USR_LOCAL_LIB_NOTE" ] && ! [ "$TIGHTDB_DIST_NONINTERACTIVE" ]; then
                libdir="$(get_config_param "INSTALL_LIBDIR")" || exit 1
                cat <<EOF

NOTE: Shared libraries have been installed in '$libdir'.

We believe that on your system this directory is not part of the
default library search path. If this is true, you probably have to do
one of the following things to successfully use TightDB in a C, C++,
or Objective-C application:

 - Either run 'export LD_RUN_PATH=$libdir' before building your
   application.

 - Or run 'export LD_LIBRARY_PATH=$libdir' before launching your
   application.

 - Or add '$libdir' to the system-wide library search path by editing
   /etc/ld.so.conf.

EOF
            fi
            if ! [ "$INTERACTIVE" ]; then
                echo "DONE INSTALLING" | tee -a "$LOG_FILE"
            fi
        else
            echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
            ERROR="1"
        fi
        if ! [ "$TIGHTDB_DIST_NONINTERACTIVE" ]; then
            if [ "$ERROR" ]; then
                echo "Log file is here: $LOG_FILE" 1>&2
            else
                if ! [ "$INTERACTIVE" ]; then
                    cat <<EOF

At this point you should run the following command to check that all
installed parts are working properly. If any parts failed to install,
they will be skipped during this test:

    ./build test-installed

EOF
                fi
            fi
        fi
        if [ "$ERROR" ]; then
            exit 1
        fi
        exit 0
        ;;


    "dist-uninstall")
        if ! [ -e ".DIST_CORE_WAS_CONFIGURED" ]; then
            cat 1>&2 <<EOF
ERROR: Nothing was configured.
You need to run './build config' first.
EOF
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-uninstall.XXXX)" || exit 1
        chmod a+rx "$TEMP_DIR" || exit 1
        LOG_FILE="$(get_dist_log_path "uninstall" "$TEMP_DIR")" || exit 1
        touch "$LOG_FILE" || exit 1
        chmod a+r "$LOG_FILE" || exit 1
        (
            echo "TightDB version: ${TIGHTDB_VERSION:-Unknown}"
            if [ -e ".PREBUILD_INFO" ]; then
                echo
                echo "PREBUILD HOST INFO:"
                cat ".PREBUILD_INFO"
            fi
            echo
            echo "BUILD HOST INFO:"
            get_host_info || exit 1
            echo
        ) >>"$LOG_FILE"
        ERROR=""
        for x in $(word_list_reverse $EXTENSIONS); do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/.DIST_WAS_CONFIGURED" ]; then
                echo "UNINSTALLING Extension '$x'" | tee -a "$LOG_FILE"
                if ! sh "$EXT_HOME/build.sh" uninstall >>"$LOG_FILE" 2>&1; then
                    echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                    ERROR="1"
                fi
                rm -f "$EXT_HOME/.DIST_WAS_INSTALLED" || exit 1
            fi
        done
        if [ -e ".DIST_CXX_WAS_CONFIGURED" ]; then
            echo "UNINSTALLING Extension 'c++'" | tee -a "$LOG_FILE"
            if ! sh build.sh uninstall-devel >>"$LOG_FILE" 2>&1; then
                echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                ERROR="1"
            fi
            rm -f ".DIST_CXX_WAS_INSTALLED" || exit 1
        fi
        echo "UNINSTALLING Core library" | tee -a "$LOG_FILE"
        if ! sh build.sh uninstall-prod >>"$LOG_FILE" 2>&1; then
            echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
            ERROR="1"
        fi
        rm -f ".DIST_CORE_WAS_INSTALLED" || exit 1
        echo "DONE UNINSTALLING" | tee -a "$LOG_FILE"
        if [ "$ERROR" ] && ! [ "$TIGHTDB_DIST_NONINTERACTIVE" ]; then
            echo "Log file is here: $LOG_FILE" 1>&2
        fi
        if [ "$ERROR" ]; then
             exit 1
        fi
        exit 0
        ;;


    "dist-test-installed")
        if ! [ -e ".DIST_CORE_WAS_INSTALLED" ]; then
            cat 1>&2 <<EOF
ERROR: Nothing was installed.
You need to run 'sudo ./build install' first.
EOF
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-test-installed.XXXX)" || exit 1
        LOG_FILE="$(get_dist_log_path "test-installed" "$TEMP_DIR")" || exit 1
        (
            echo "TightDB version: ${TIGHTDB_VERSION:-Unknown}"
            if [ -e ".PREBUILD_INFO" ]; then
                echo
                echo "PREBUILD HOST INFO:"
                cat ".PREBUILD_INFO"
            fi
            echo
            echo "BUILD HOST INFO:"
            get_host_info || exit 1
            echo
            get_compiler_info || exit 1
            echo
        ) >>"$LOG_FILE"
        ERROR=""
        if [ -e ".DIST_CXX_WAS_INSTALLED" ]; then
            echo "TESTING Installed extension 'c++'" | tee -a "$LOG_FILE"
            if sh build.sh test-installed >>"$LOG_FILE" 2>&1; then
                echo 'Success!' | tee -a "$LOG_FILE"
            else
                echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                ERROR="1"
            fi
        fi
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/.DIST_WAS_INSTALLED" ]; then
                echo "TESTING Installed extension '$x'" | tee -a "$LOG_FILE"
                if sh "$EXT_HOME/build.sh" test-installed >>"$LOG_FILE" 2>&1; then
                    echo 'Success!' | tee -a "$LOG_FILE"
                else
                    echo 'Failed!' | tee -a "$LOG_FILE" 1>&2
                    ERROR="1"
                fi
            fi
        done
        if ! [ "$INTERACTIVE" ]; then
            echo "DONE TESTING" | tee -a "$LOG_FILE"
        fi
        if [ "$ERROR" ] && ! [ "$TIGHTDB_DIST_NONINTERACTIVE" ]; then
            echo "Log file is here: $LOG_FILE" 1>&2
        fi
        if [ "$ERROR" ]; then
            exit 1
        fi
        exit 0
        ;;


    "dist-status")
        echo ">>>>>>>> STATUS OF 'tightdb'"
        git status
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/build.sh" ]; then
                echo ">>>>>>>> STATUS OF '$EXT_HOME'"
                (cd "$EXT_HOME/"; git status)
            fi
        done
        exit 0
        ;;


    "dist-pull")
        echo ">>>>>>>> PULLING 'tightdb'"
        git pull
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/build.sh" ]; then
                echo ">>>>>>>> PULLING '$EXT_HOME'"
                (cd "$EXT_HOME/"; git pull)
            fi
        done
        exit 0
        ;;


    "dist-checkout")
        if [ "$#" -ne 1 ]; then
            echo "Please specify what you want to checkout" 1>&2
            exit 1
        fi
        WHAT="$1"
        echo ">>>>>>>> CHECKING OUT '$WHAT' OF 'tightdb'"
        git checkout "$WHAT"
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/build.sh" ]; then
                echo ">>>>>>>> CHECKING OUT '$WHAT' OF '$EXT_HOME'"
                (cd "$EXT_HOME/"; git checkout "$WHAT")
            fi
        done
        exit 0
        ;;

    "dist-copy")
        # Copy to distribution package
        TARGET_DIR="$1"
        if ! [ "$TARGET_DIR" -a -d "$TARGET_DIR" ]; then
            echo "Unspecified or bad target directory '$TARGET_DIR'" 1>&2
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.copy.XXXX)" || exit 1
        cat >"$TEMP_DIR/include" <<EOF
/README.md
/build.sh
/Makefile
/src
/test
/test-installed
/doc
/debian
EOF
        cat >"$TEMP_DIR/exclude" <<EOF
.gitignore
/test/test-*
/test/benchmark-*
/test/performance
/test/experiments
/doc/development
EOF
        grep -E -v '^(#.*)?$' "$TEMP_DIR/include" >"$TEMP_DIR/include2" || exit 1
        grep -E -v '^(#.*)?$' "$TEMP_DIR/exclude" >"$TEMP_DIR/exclude2" || exit 1
        sed -e 's/\([.\[^$]\)/\\\1/g' -e 's|\*|[^/]*|g' -e 's|^\([^/]\)|^\\(.*/\\)\\{0,1\\}\1|' -e 's|^/|^|' -e 's|$|\\(/.*\\)\\{0,1\\}$|' "$TEMP_DIR/include2" >"$TEMP_DIR/include.bre" || exit 1
        sed -e 's/\([.\[^$]\)/\\\1/g' -e 's|\*|[^/]*|g' -e 's|^\([^/]\)|^\\(.*/\\)\\{0,1\\}\1|' -e 's|^/|^|' -e 's|$|\\(/.*\\)\\{0,1\\}$|' "$TEMP_DIR/exclude2" >"$TEMP_DIR/exclude.bre" || exit 1
        git ls-files >"$TEMP_DIR/files1" || exit 1
        grep -f "$TEMP_DIR/include.bre" "$TEMP_DIR/files1" >"$TEMP_DIR/files2" || exit 1
        grep -v -f "$TEMP_DIR/exclude.bre" "$TEMP_DIR/files2" >"$TEMP_DIR/files3" || exit 1
        tar czf "$TEMP_DIR/archive.tar.gz" -T "$TEMP_DIR/files3" || exit 1
        (cd "$TARGET_DIR" && tar xzmf "$TEMP_DIR/archive.tar.gz") || exit 1
        if ! [ "$TIGHTDB_DISABLE_MARKDOWN_TO_PDF" ]; then
            (cd "$TARGET_DIR" && pandoc README.md -o README.pdf) || exit 1
        fi
        exit 0
        ;;

    "dist-deb")
        codename=$(lsb_release -s -c)
        (cd debian && sed -e "s/@CODENAME@/$codename/g" changelog.in > changelog) || exit 1
        dpkg-buildpackage -rfakeroot -us -uc || exit 1
        exit 0
        ;;

    *)  usage
        exit 1
        ;;
esac
