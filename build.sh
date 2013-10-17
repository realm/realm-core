# NOTE: THIS SCRIPT IS SUPPOSED TO RUN IN A POSIX SHELL

ORIG_CWD="$(pwd)"
cd "$(dirname "$0")"
TIGHTDB_HOME="$(pwd)"

MODE="$1"
[ $# -gt 0 ] && shift

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

# Used by Makefiles
export TIGHTDB_HAVE_CONFIG="1"

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
LD_LIBRARY_PATH_NAME="LD_LIBRARY_PATH"
if [ "$OS" = "Darwin" ]; then
    LD_LIBRARY_PATH_NAME="DYLD_LIBRARY_PATH"
fi
if ! printf "%s\n" "$MODE" | grep -q '^\(src-\|bin-\)\?dist'; then
    NUM_PROCESSORS=""
    if [ "$OS" = "Darwin" ]; then
        NUM_PROCESSORS="$(sysctl -n hw.ncpu)" || exit 1
    else
        if [ -r /proc/cpuinfo ]; then
            NUM_PROCESSORS="$(cat /proc/cpuinfo | grep -E 'processor[[:space:]]*:' | wc -l)" || exit 1
        fi
    fi
    if [ "$NUM_PROCESSORS" ]; then
        word_list_prepend MAKEFLAGS "-j$NUM_PROCESSORS" || exit 1
    fi
    export MAKEFLAGS
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


require_config()
{
    cd "$TIGHTDB_HOME" || return 1
    if ! [ -e "config" ]; then
        cat 1>&2 <<EOF
ERROR: Found no configuration!
You need to run 'sh build.sh config [PREFIX]'.
EOF
        return 1
    fi
    echo "Using existing configuration:"
    cat "config" | sed 's/^/    /' || return 1
}

auto_configure()
{
    cd "$TIGHTDB_HOME" || return 1
    if [ -e "config" ]; then
        require_config || return 1
    else
        echo "No configuration found. Running 'sh build.sh config'"
        sh build.sh config || return 1
    fi
}

get_config_param()
{
    local name line value
    cd "$TIGHTDB_HOME" || return 1
    name="$1"
    if ! [ -e "config" ]; then
        cat 1>&2 <<EOF
ERROR: Found no configuration!
You need to run 'sh build.sh config [PREFIX]'.
EOF
        return 1
    fi
    if ! line="$(grep "^$name:" "config")"; then
        cat 1>&2 <<EOF
ERROR: Failed to read configuration parameter '$name'.
Maybe you need to rerun 'sh build.sh config [PREFIX]'.
EOF
        return 1
    fi
    value="$(printf "%s\n" "$line" | cut -d: -f2-)" || return 1
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
    CC_CMD="$(make get-cc)" || return 1
    CXX_CMD="$(make get-cxx)" || return 1
    LD_CMD="$(make get-ld)" || return 1
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

new_dist_log_path()
{
    local stem temp_dir dir files
    stem="$1"
    temp_dir="$2"
    if [ "$TIGHTDB_DIST_HOME" ]; then
        dir="$TIGHTDB_DIST_HOME/log"
    else
        dir="$temp_dir/log"
    fi
    mkdir -p "$dir" || return 1
    files="$(cd "$dir" && (ls *.log 2>/dev/null || true))" || return 1
    max="$(printf "%s\n" "$files" | grep '^[0-9][0-9]*_' | cut -d_ -f1 | sort -n | tail -n1)"
    next="$((max+1))" || return 1
    printf "%s\n" "$dir/$(printf "%03d" "$next")_$stem.log"
}


case "$MODE" in

    "config")
        install_prefix="$1"
        if [ -z "$install_prefix" ]; then
            install_prefix="/usr/local"
        fi
        install_exec_prefix="$install_prefix"
        install_includedir="$install_prefix/include"
        install_bindir="$install_exec_prefix/bin"
        install_libdir="$(make prefix="$install_prefix" get-libdir)" || exit 1

        tightdb_version="unknown"
        if [ "$TIGHTDB_VERSION" ]; then
            tightdb_version="$TIGHTDB_VERSION"
        elif value="$(git describe 2>/dev/null)"; then
            tightdb_version="$value"
        fi

        xcode_home="none"
        if [ "$OS" = "Darwin" ]; then
            if path="$(xcode-select --print-path 2>/dev/null)"; then
                xcode_home="$path"
            fi
        fi

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
                    if [ -z "$sdk" ]; then
                        echo "Found no SDKs in '$platform_home'"
                        iphone_sdks_avail="no"
                    else
                        if [ "$x" = "iPhoneSimulator" ]; then
                            arch="i386"
                        else
                            type="$(defaults read-type "$platform_home/Info" "DefaultProperties")" || exit 1
                            if [ "$type" != "Type is dictionary" ]; then
                                echo "Unexpected type of value of key 'DefaultProperties' in '$platform_home/Info.plist'" 1>&2
                                exit 1
                            fi
                            temp_dir="$(mktemp -d "/tmp/tmp.XXXXXXXXXX")" || exit 1
                            chunk="$temp_dir/chunk.plist"
                            defaults read "$platform_home/Info" "DefaultProperties" >"$chunk" || exit 1
                            arch="$(defaults read "$chunk" NATIVE_ARCH)" || exit 1
                            rm -f "$chunk" || exit 1
                            rmdir "$temp_dir" || exit 1
                        fi
                        word_list_append "iphone_sdks" "$x:$sdk:$arch" || exit 1
                    fi
                fi
            done
        fi

        cat >"config" <<EOF
tightdb-version:     $tightdb_version
install-prefix:      $install_prefix
install-exec-prefix: $install_exec_prefix
install-includedir:  $install_includedir
install-bindir:      $install_bindir
install-libdir:      $install_libdir
xcode-home:          $xcode_home
iphone-sdks:         ${iphone_sdks:-none}
iphone-sdks-avail:   $iphone_sdks_avail
EOF
        echo "New configuration:"
        cat "config" | sed 's/^/    /' || exit 1
        echo "Done configuring"
        exit 0
        ;;

    "clean")
        auto_configure || exit 1
        make clean || exit 1
        if [ "$OS" = "Darwin" ]; then
            for x in $IPHONE_PLATFORMS; do
                make -C "src/tightdb" BASE_DENOM="$x" clean || exit 1
            done
            make -C "src/tightdb" BASE_DENOM="ios" clean || exit 1
            if [ -e "$IPHONE_DIR" ]; then
                echo "Removing '$IPHONE_DIR'"
                rm -fr "$IPHONE_DIR/include" || exit 1
                rm -f "$IPHONE_DIR/libtightdb-ios.a" "$IPHONE_DIR/libtightdb-ios-dbg.a" || exit 1
                rm -f "$IPHONE_DIR/tightdb-config" "$IPHONE_DIR/tightdb-config-dbg" || exit 1
                rmdir "$IPHONE_DIR" || exit 1
            fi
        fi
        echo "Done cleaning"
        exit 0
        ;;

    "build")
        auto_configure || exit 1
        TIGHTDB_ENABLE_FAT_BINARIES="1" make || exit 1
        echo "Done building"
        exit 0
        ;;

    "build-iphone")
        auto_configure || exit 1
        iphone_sdks_avail="$(get_config_param "iphone-sdks-avail")" || exit 1
        if [ "$iphone_sdks_avail" != "yes" ]; then
            echo "ERROR: Required iPhone SDKs are not available!" 1>&2
            exit 1
        fi
        temp_dir="$(mktemp -d /tmp/tightdb.build-iphone.XXXX)" || exit 1
        xcode_home="$(get_config_param "xcode-home")" || exit 1
        iphone_sdks="$(get_config_param "iphone-sdks")" || exit 1
        for x in $iphone_sdks; do
            platform="$(printf "%s\n" "$x" | cut -d: -f1)" || exit 1
            sdk="$(printf "%s\n" "$x" | cut -d: -f2)" || exit 1
            arch="$(printf "%s\n" "$x" | cut -d: -f3)" || exit 1
            sdk_root="$xcode_home/Platforms/$platform.platform/Developer/SDKs/$sdk"
            make -C "src/tightdb" "libtightdb-$platform.a" "libtightdb-$platform-dbg.a" BASE_DENOM="$platform" CFLAGS_ARCH="-arch $arch -isysroot $sdk_root" || exit 1
            mkdir "$temp_dir/$platform" || exit 1
            cp "src/tightdb/libtightdb-$platform.a"     "$temp_dir/$platform/libtightdb.a"     || exit 1
            cp "src/tightdb/libtightdb-$platform-dbg.a" "$temp_dir/$platform/libtightdb-dbg.a" || exit 1
        done
        TIGHTDB_ENABLE_FAT_BINARIES="1" make -C "src/tightdb" "tightdb-config-ios" "tightdb-config-ios-dbg" BASE_DENOM="ios" CFLAGS_ARCH="-DTIGHTDB_CONFIG_IOS" || exit 1
        mkdir -p "$IPHONE_DIR" || exit 1
        echo "Creating '$IPHONE_DIR/libtightdb-ios.a'"
        lipo "$temp_dir"/*/"libtightdb.a"     -create -output "$IPHONE_DIR/libtightdb-ios.a"     || exit 1
        echo "Creating '$IPHONE_DIR/libtightdb-ios-dbg.a'"
        lipo "$temp_dir"/*/"libtightdb-dbg.a" -create -output "$IPHONE_DIR/libtightdb-ios-dbg.a" || exit 1
        echo "Copying headers to '$IPHONE_DIR/include'"
        mkdir -p "$IPHONE_DIR/include" || exit 1
        cp "src/tightdb.hpp" "$IPHONE_DIR/include/" || exit 1
        mkdir -p "$IPHONE_DIR/include/tightdb" || exit 1
        inst_headers="$(cd src/tightdb && make get-inst-headers)" || exit 1
        (cd "src/tightdb" && cp $inst_headers "$TIGHTDB_HOME/$IPHONE_DIR/include/tightdb/") || exit 1
        for x in "tightdb-config" "tightdb-config-dbg"; do
            echo "Creating '$IPHONE_DIR/$x'"
            y="$(printf "%s\n" "$x" | sed 's/tightdb-config/tightdb-config-ios/')" || exit 1
            cp "src/tightdb/$y" "$TIGHTDB_HOME/$IPHONE_DIR/$x" || exit 1
        done
        echo "Done building"
        exit 0
        ;;

    "test")
        require_config || exit 1
        make test || exit 1
        echo "Test passed"
        exit 0
        ;;

    "test-debug")
        require_config || exit 1
        make test-debug || exit 1
        echo "Test passed"
        exit 0
        ;;

    "install")
        require_config || exit 1
        install_prefix="$(get_config_param "install-prefix")" || exit 1
        make install DESTDIR="$DESTDIR" prefix="$install_prefix" || exit 1
        if [ "$USER" = "root" ] && which ldconfig >/dev/null 2>&1; then
            ldconfig || exit 1
        fi
        echo "Done installing"
        exit 0
        ;;

    "install-shared")
        require_config || exit 1
        install_prefix="$(get_config_param "install-prefix")" || exit 1
        make install DESTDIR="$DESTDIR" prefix="$install_prefix" INSTALL_FILTER=shared-libs,progs || exit 1
        if [ "$USER" = "root" ] && which ldconfig >/dev/null 2>&1; then
            ldconfig || exit 1
        fi
        echo "Done installing"
        exit 0
        ;;

    "install-devel")
        require_config || exit 1
        install_prefix="$(get_config_param "install-prefix")" || exit 1
        make install DESTDIR="$DESTDIR" prefix="$install_prefix" INSTALL_FILTER=static-libs,dev-progs,headers || exit 1
        echo "Done installing"
        exit 0
        ;;

    "uninstall")
        require_config || exit 1
        install_prefix="$(get_config_param "install-prefix")" || exit 1
        make uninstall prefix="$install_prefix" || exit 1
        if [ "$USER" = "root" ] && which ldconfig >/dev/null 2>&1; then
            ldconfig || exit 1
        fi
        echo "Done uninstalling"
        exit 0
        ;;

    "uninstall-shared")
        require_config || exit 1
        install_prefix="$(get_config_param "install-prefix")" || exit 1
        make uninstall prefix="$install_prefix" INSTALL_FILTER=shared-libs,progs || exit 1
        if [ "$USER" = "root" ] && which ldconfig >/dev/null 2>&1; then
            ldconfig || exit 1
        fi
        echo "Done uninstalling"
        exit 0
        ;;

    "uninstall-devel")
        require_config || exit 1
        install_prefix="$(get_config_param "install-prefix")" || exit 1
        make uninstall prefix="$install_prefix" INSTALL_FILTER=static-libs,dev-progs,extra || exit 1
        echo "Done uninstalling"
        exit 0
        ;;

    "test-installed")
        require_config || exit 1
        install_libdir="$(get_config_param "install-libdir")" || exit 1
        export LD_RUN_PATH="$install_libdir"
        make -C "test-installed" clean || exit 1
        make -C "test-installed" test  || exit 1
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
                if [ -z "$found" ]; then
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
                if [ -z "$found" ]; then
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
                if [ -z "$found" ]; then
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
                if [ -z "$found" ]; then
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
        if [ -z "$TIGHTDB_VERSION" ]; then
            TIGHTDB_VERSION="$VERSION"
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
            INSTALL_ROOT="$TEMP_DIR/install"
            mkdir "$INSTALL_ROOT" || exit 1

            path_list_prepend CPATH                   "$INSTALL_ROOT/include"     || exit 1
            path_list_prepend LIBRARY_PATH            "$INSTALL_ROOT/lib"         || exit 1
            path_list_prepend LIBRARY_PATH            "$INSTALL_ROOT/lib64"       || exit 1
            path_list_prepend "$LD_LIBRARY_PATH_NAME" "$INSTALL_ROOT/lib"         || exit 1
            path_list_prepend "$LD_LIBRARY_PATH_NAME" "$INSTALL_ROOT/lib64"       || exit 1
            path_list_prepend PATH                    "$INSTALL_ROOT/bin"         || exit 1
            export CPATH LIBRARY_PATH "$LD_LIBRARY_PATH_NAME" PATH

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

            BIN_CORE_ARG=""
            if [ "$PREBUILT_CORE" ]; then
                BIN_CORE_ARG=" bin-core"
            fi

            cat >"$PKG_DIR/build" <<EOF
#!/bin/sh

TIGHTDB_DIST_HOME="\$(cd "\$(dirname "\$0")" && pwd)" || exit 1
export TIGHTDB_DIST_HOME

EXTENSIONS="$AUGMENTED_EXTENSIONS"

export TIGHTDB_VERSION="$TIGHTDB_VERSION"

if [ \$# -eq 1 -a "\$1" = "clean" ]; then
    sh tightdb/build.sh dist-clean$BIN_CORE_ARG || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "build" ]; then
    sh tightdb/build.sh dist-build$BIN_CORE_ARG || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "build-iphone" -a "$INCLUDE_IPHONE" ]; then
    sh tightdb/build.sh dist-build-iphone$BIN_CORE_ARG || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "install" ]; then
    sh tightdb/build.sh dist-install$BIN_CORE_ARG || exit 1
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
            if [ -z "\$found" ]; then
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

            mkdir "$PKG_DIR/tightdb" || exit 1
            if [ "$PREBUILT_CORE" ]; then
                message "Building core library"
                (sh build.sh config && sh build.sh clean && sh build.sh build) >>"$LOG_FILE" 2>&1 || exit 1

                message "Running test suite for core library in debug mode"
                if ! sh build.sh test-debug >>"$LOG_FILE" 2>&1; then
                    warning "Test suite failed for core library"
                fi

                if [ "$INCLUDE_IPHONE" ]; then
                    message "Building core library for 'iphone'"
                    sh build.sh build-iphone >>"$LOG_FILE" 2>&1 || exit 1
                fi

                message "Transferring prebuilt core library to package"
                mkdir "$TEMP_DIR/transfer" || exit 1
                cat >"$TEMP_DIR/transfer/include" <<EOF
/README.md
/build.sh
/generic.mk
/config.mk
/Makefile
/src/Makefile
/src/tightdb.hpp
/src/tightdb/Makefile
/test/Makefile
/test-installed
/doc
EOF
                cat >"$TEMP_DIR/transfer/exclude" <<EOF
.gitignore
/doc/development
EOF
                grep -E -v '^(#.*)?$' "$TEMP_DIR/transfer/include" >"$TEMP_DIR/transfer/include2" || exit 1
                grep -E -v '^(#.*)?$' "$TEMP_DIR/transfer/exclude" >"$TEMP_DIR/transfer/exclude2" || exit 1
                sed -e 's/\([.\[^$]\)/\\\1/g' -e 's|\*|[^/]*|g' -e 's|^\([^/]\)|^\\(.*/\\)\\{0,1\\}\1|' -e 's|^/|^|' -e 's|$|\\(/.*\\)\\{0,1\\}$|' "$TEMP_DIR/transfer/include2" >"$TEMP_DIR/transfer/include.bre" || exit 1
                sed -e 's/\([.\[^$]\)/\\\1/g' -e 's|\*|[^/]*|g' -e 's|^\([^/]\)|^\\(.*/\\)\\{0,1\\}\1|' -e 's|^/|^|' -e 's|$|\\(/.*\\)\\{0,1\\}$|' "$TEMP_DIR/transfer/exclude2" >"$TEMP_DIR/transfer/exclude.bre" || exit 1
                git ls-files >"$TEMP_DIR/transfer/files1" || exit 1
                grep -f "$TEMP_DIR/transfer/include.bre" "$TEMP_DIR/transfer/files1" >"$TEMP_DIR/transfer/files2" || exit 1
                grep -v -f "$TEMP_DIR/transfer/exclude.bre" "$TEMP_DIR/transfer/files2" >"$TEMP_DIR/transfer/files3" || exit 1
                tar czf "$TEMP_DIR/transfer/core.tar.gz" -T "$TEMP_DIR/transfer/files3" || exit 1
                (cd "$PKG_DIR/tightdb" && tar xf "$TEMP_DIR/transfer/core.tar.gz") || exit 1
                if [ -z "$(which pandoc)" ]; then
                    echo "pandoc is not installed - not generating README.pdf"
                else
                    (cd "$PKG_DIR/tightdb" && pandoc README.md -o README.pdf) || exit 1
                fi
                printf "\nNO_BUILD_ON_INSTALL = 1\n" >> "$PKG_DIR/tightdb/config.mk"
                INST_HEADERS="$(cd src/tightdb && make get-inst-headers)" || exit 1
                INST_LIBS="$(cd src/tightdb && make get-inst-libraries)" || exit 1
                INST_PROGS="$(cd src/tightdb && make get-inst-programs)" || exit 1
                (cd "src/tightdb" && cp -R -P $INST_HEADERS $INST_LIBS $INST_PROGS "$PKG_DIR/tightdb/src/tightdb/") || exit 1
                if [ "$INCLUDE_IPHONE" ]; then
                    cp -R "$IPHONE_DIR" "$PKG_DIR/tightdb/" || exit 1
                fi
                get_host_info >"$PKG_DIR/tightdb/.PREBUILD_INFO" || exit 1
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
            (cd "$TEST_DIR" && tar xzf "$TEMP_DIR/$NAME.tar.gz") || exit 1
            TEST_PKG_DIR="$TEST_DIR/$NAME"
            cd "$TEST_PKG_DIR" || exit 1

            export DISABLE_CHEETAH_CODE_GEN="1"

            message "Configuring core library"
            sh "$TEST_PKG_DIR/tightdb/build.sh" config "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1 || exit 1

            if [ -z "$PREBUILT_CORE" ]; then
                message "Building core library"
                sh "$TEST_PKG_DIR/tightdb/build.sh" build >>"$LOG_FILE" 2>&1 || exit 1

                message "Running test suite for core library in debug mode"
                if ! sh "$TEST_PKG_DIR/tightdb/build.sh" test-debug >>"$LOG_FILE" 2>&1; then
                    warning "Test suite failed for core library"
                fi
            fi

            message "Installing core library to test location"
            sh "$TEST_PKG_DIR/tightdb/build.sh" install >>"$LOG_FILE" 2>&1 || exit 1

            # This one was added because when building for iOS on
            # Darwin, the libraries libtightdb-ios.a and
            # libtightdb-ios-dbg.a are not installed, and the
            # Objective-C binding needs to be able to find them. Also,
            # when building for iOS, the default search path for
            # header files is not used, so installed headers will not
            # be found. This problem is eliminated by the explicit
            # addition of the temporary header installation directory
            # to CPATH above.
            path_list_prepend LIBRARY_PATH "$TEST_PKG_DIR/tightdb/src/tightdb" || exit 1

            # FIXME: The problem with this one is that it partially
            # destroys the reliability of the build test. We should
            # instead transfer the iOS target files to a special
            # temporary proforma directory, and add that diretory to
            # LIBRARY_PATH and PATH above.

            message "Testing state of core library installation"
            sh "$TEST_PKG_DIR/tightdb/build.sh" test-installed >>"$LOG_FILE" 2>&1 || exit 1

            CONFIGURED_EXTENSIONS=""
            for x in $AVAIL_EXTENSIONS; do
                message "Testing extension '$x'"
                log_message "Configuring extension '$x'"
                EXT_DIR="$(map_ext_name_to_dir "$x")" || exit 1
                if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" config "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1; then
                    warning "Failed to configure extension '$x'"
                else
                    word_list_append CONFIGURED_EXTENSIONS "$x" || exit 1
                    log_message "Building extension '$x'"
                    if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" build >>"$LOG_FILE" 2>&1; then
                        warning "Failed to build extension '$x'"
                    else
                        log_message "Running test suite for extension '$x' in debug mode"
                        if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" test-debug >>"$LOG_FILE" 2>&1; then
                            warning "Test suite failed for extension '$x'"
                        fi
                        log_message "Installing extension '$x' to test location"
                        if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" install >>"$LOG_FILE" 2>&1; then
                            warning "Installation test failed for extension '$x'"
                        else
                            log_message "Testing state of test installation of extension '$x'"
                            if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" test-installed >>"$LOG_FILE" 2>&1; then
                                warning "Post installation test failed for extension '$x'"
                            fi
                        fi
                    fi
                fi
            done

            # Copy the installation test directory to allow later inspection
            INSTALL_COPY="$TEMP_DIR/install_copy"
            cp -R "$INSTALL_ROOT" "$INSTALL_COPY" || exit 1

            message "Testing uninstallation"
            for x in $(word_list_reverse $CONFIGURED_EXTENSIONS); do
                log_message "Uninstalling extension '$x' from test location"
                EXT_DIR="$(map_ext_name_to_dir "$x")" || exit 1
                if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" uninstall >>"$LOG_FILE" 2>&1; then
                    warning "Failed to uninstall extension '$x'"
                fi
            done
            log_message "Uninstalling core library from test location"
            if ! sh "$TEST_PKG_DIR/tightdb/build.sh" uninstall >>"$LOG_FILE" 2>&1; then
                warning "Failed to uninstall core library"
            fi
            REMAINING_PATHS="$(cd "$INSTALL_ROOT" && find * -ipath '*tightdb*')" || exit 1
            if [ "$REMAINING_PATHS" ]; then
                warning "Files and/or directories remain after uninstallation"
                printf "%s" "$REMAINING_PATHS" >>"$LOG_FILE" || exit 1
            fi

            if [ "$INCLUDE_IPHONE" ]; then
                message "Testing platform 'iphone'"
                fail=""
                if [ -z "$PREBUILT_CORE" ]; then
                    log_message "Building core library for 'iphone'"
                    if ! sh "$TEST_PKG_DIR/tightdb/build.sh" build-iphone >>"$LOG_FILE" 2>&1; then
                        warning "Failed to build core library for 'iphone'"
                        fail="1"
                    fi
                fi
                if [ -z "$fail" ]; then
                    for x in $CONFIGURED_EXTENSIONS; do
                        for y in $IPHONE_EXTENSIONS; do
                            if [ "$x" = "$y" ]; then
                                log_message "Building extension '$x' for 'iphone'"
                                EXT_DIR="$(map_ext_name_to_dir "$x")" || exit 1
                                if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" build-iphone >>"$LOG_FILE" 2>&1; then
                                    warning "Failed to build extension '$x' for 'iphone'"
                                fi
                                break
                            fi
                        done
                    done
                fi
            fi

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
#                    echo "YES!"
#                fi
#            fi
#        done
#        exit 0
#        ;;


    "dist-config")
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-config.XXXX)" || exit 1
        LOG_FILE="$(new_dist_log_path "config" "$TEMP_DIR")" || exit 1
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
        echo "CONFIGURING Core library" | tee -a "$LOG_FILE"
        rm -f ".DIST_WAS_CONFIGURED" || exit 1
        if [ -z "$INTERACTIVE" ]; then
            if sh "build.sh" config >>"$LOG_FILE" 2>&1; then
                touch ".DIST_WAS_CONFIGURED" || exit 1
            else
                echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                ERROR="1"
            fi
        else
            if INTERACTIVE=1 sh "build.sh" config 2>&1 | tee -a "$LOG_FILE"; then
                touch ".DIST_WAS_CONFIGURED" || exit 1
            else
                echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                ERROR="1"
            fi
        fi
        if [ -z "$ERROR" ]; then
            mkdir "$TEMP_DIR/select" || exit 1
            for x in "$@"; do
                touch "$TEMP_DIR/select/$x" || exit 1
            done
            rm -f ".DIST_DEVEL_WAS_CONFIGURED" || exit 1
            if [ -e "$TEMP_DIR/select/c++" ]; then
                echo "CONFIGURING Extension 'c++'" | tee -a "$LOG_FILE"
                touch ".DIST_DEVEL_WAS_CONFIGURED" || exit 1
            fi
            for x in $EXTENSIONS; do
                EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                rm -f "$EXT_HOME/.DIST_WAS_CONFIGURED" || exit 1
                if [ -e "$TEMP_DIR/select/$x" ]; then
                    echo "CONFIGURING Extension '$x'" | tee -a "$LOG_FILE"
                    if [ -z "$INTERACTIVE" ]; then
                        if sh "$EXT_HOME/build.sh" config >>"$LOG_FILE" 2>&1; then
                            touch "$EXT_HOME/.DIST_WAS_CONFIGURED" || exit 1
                        else
                            echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                            ERROR="1"
                        fi
                    else
                        if INTERACTIVE=1 sh "$EXT_HOME/build.sh" config 2>&1 | tee -a "$LOG_FILE"; then
                            touch "$EXT_HOME/.DIST_WAS_CONFIGURED" || exit 1
                        else
                            ERROR="1"
                        fi
                    fi
                fi
            done
            echo "DONE CONFIGURING" | tee -a "$LOG_FILE"
        fi
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
        if [ "$ERROR" -a -z "$INTERACTIVE" ]; then
            exit 1
        fi
        exit 0
        ;;


    "dist-clean")
        if ! [ -e ".DIST_WAS_CONFIGURED" ]; then
            cat 1>&2 <<EOF
ERROR: Nothing was configured.
You need to run './build config' first.
EOF
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-clean.XXXX)" || exit 1
        LOG_FILE="$(new_dist_log_path "clean" "$TEMP_DIR")" || exit 1
        ERROR=""
        rm -f ".DIST_WAS_BUILT" || exit 1
        if [ "$1" = "bin-core" ]; then
            shift
        else
            echo "CLEANING Core library" | tee -a "$LOG_FILE"
            if ! sh "build.sh" clean >>"$LOG_FILE" 2>&1; then
                echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                ERROR="1"
            fi
        fi
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/.DIST_WAS_CONFIGURED" ]; then
                echo "CLEANING Extension '$x'" | tee -a "$LOG_FILE"
                rm -f "$EXT_HOME/.DIST_WAS_BUILT" || exit 1
                if ! sh "$EXT_HOME/build.sh" clean >>"$LOG_FILE" 2>&1; then
                    echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                    ERROR="1"
                fi
            fi
        done
        echo "DONE CLEANING" | tee -a "$LOG_FILE"
        if [ "$ERROR" ]; then
            echo "Log file is here: $LOG_FILE" 1>&2
            exit 1
        fi
        exit 0
        ;;


    "dist-build")
        if ! [ -e ".DIST_WAS_CONFIGURED" ]; then
            cat 1>&2 <<EOF
ERROR: Nothing was configured.
You need to run './build config' first.
EOF
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-build.XXXX)" || exit 1
        LOG_FILE="$(new_dist_log_path "build" "$TEMP_DIR")" || exit 1
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
        export DISABLE_CHEETAH_CODE_GEN="1"
        if [ "$1" != "bin-core" ]; then
            echo "BUILDING Core library" | tee -a "$LOG_FILE"
            rm -f ".DIST_WAS_BUILT" || exit 1
            if sh "build.sh" build >>"$LOG_FILE" 2>&1; then
                touch ".DIST_WAS_BUILT" || exit 1
            else
                echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                cat 1>&2 <<EOF

Note: The core library could not be built. You may be missing one or
more dependencies. Check the README file for details. If this does not
help, check the log file.
The log file is here: $LOG_FILE
EOF
                exit 1
            fi
        fi
        LIBDIR="$(make get-libdir)" || exit 1
        path_list_prepend CPATH        "$TIGHTDB_HOME/src"         || exit 1
        path_list_prepend LIBRARY_PATH "$TIGHTDB_HOME/src/tightdb" || exit 1
        path_list_prepend PATH         "$TIGHTDB_HOME/src/tightdb" || exit 1
        path_list_prepend LD_RUN_PATH  "$LIBDIR"                   || exit 1
        export CPATH LIBRARY_PATH PATH LD_RUN_PATH
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/.DIST_WAS_CONFIGURED" ]; then
                echo "BUILDING Extension '$x'" | tee -a "$LOG_FILE"
                rm -f "$EXT_HOME/.DIST_WAS_BUILT" || exit 1
                if sh "$EXT_HOME/build.sh" build >>"$LOG_FILE" 2>&1; then
                    touch "$EXT_HOME/.DIST_WAS_BUILT" || exit 1
                else
                    echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                    ERROR="1"
                fi
            fi
        done
        echo "DONE BUILDING" | tee -a "$LOG_FILE"
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
        if [ "$ERROR" ]; then
            exit 1
        fi
        exit 0
        ;;


    "dist-build-iphone")
        if ! [ -e ".DIST_WAS_CONFIGURED" ]; then
            cat 1>&2 <<EOF
ERROR: Nothing was configured.
You need to run './build config' first.
EOF
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-build-iphone.XXXX)" || exit 1
        LOG_FILE="$(new_dist_log_path "build-iphone" "$TEMP_DIR")" || exit 1
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
        export DISABLE_CHEETAH_CODE_GEN="1"
        if [ "$1" != "bin-core" ]; then
            echo "BUILDING Core library for iPhone" | tee -a "$LOG_FILE"
            if ! sh "build.sh" build-iphone >>"$LOG_FILE" 2>&1; then
                echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                cat 1>&2 <<EOF

Note: You may be missing one or more dependencies. Check the README
file for details. If this does not help, check the log file.
The log file is here: $LOG_FILE
EOF
                exit 1
            fi
        fi
        if [ -e ".DIST_DEVEL_WAS_CONFIGURED" ]; then
            mkdir -p "$ORIG_CWD/iphone-c++" || exit 1
            cp -R "$IPHONE_DIR"/* "$ORIG_CWD/iphone-c++/" || exit 1
        fi
        for x in $IPHONE_EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/.DIST_WAS_CONFIGURED" ]; then
                echo "BUILDING Extension '$x' for iPhone" | tee -a "$LOG_FILE"
                if sh "$EXT_HOME/build.sh" build-iphone >>"$LOG_FILE" 2>&1; then
                    mkdir -p "$ORIG_CWD/iphone-$x" || exit 1
                    cp -R "$EXT_HOME/$IPHONE_DIR"/* "$ORIG_CWD/iphone-$x/" || exit 1
                else
                    echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                    ERROR="1"
                fi
            fi
        done
        echo "DONE BUILDING" | tee -a "$LOG_FILE"
        if [ "$ERROR" ]; then
            cat 1>&2 <<EOF

Note: Some parts failed to build. You may be missing one or more
dependencies. Check the README file for details. If this does not
help, check the log file.
The log file is here: $LOG_FILE

Files produced for a successfully built extension EXT have been placed
in a subdirectory named "iphone-EXT".
EOF
            exit 1
        fi
        cat <<EOF

Files produced for extension EXT have been placed in a subdirectory
named "iphone-EXT".
EOF
        exit 0
        ;;


    "dist-install")
        NOTHING_TO_INSTALL=""
        if ! [ -e ".DIST_WAS_CONFIGURED" ]; then
            NOTHING_TO_INSTALL="1"
        elif ! [ "$1" = "bin-core" -o -e ".DIST_WAS_BUILT" ]; then
            NOTHING_TO_INSTALL="1"
        fi
        if [ "$NOTHING_TO_INSTALL" ]; then
            cat 1>&2 <<EOF
ERROR: Nothing to install.
You need to run './build build' first.
EOF
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-install.XXXX)" || exit 1
        chmod a+rx "$TEMP_DIR" || exit 1
        LOG_FILE="$(new_dist_log_path "install" "$TEMP_DIR")" || exit 1
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
        export DISABLE_CHEETAH_CODE_GEN="1"
        ERROR=""
        NEED_USR_LOCAL_LIB_NOTE=""
        echo "INSTALLING Core library" | tee -a "$LOG_FILE"
        if sh build.sh install-shared >>"$LOG_FILE" 2>&1; then
            touch ".DIST_WAS_INSTALLED" || exit 1
            if [ -e ".DIST_DEVEL_WAS_CONFIGURED" ]; then
                echo "INSTALLING Extension 'c++'" | tee -a "$LOG_FILE"
                if sh build.sh install-devel >>"$LOG_FILE" 2>&1; then
                    touch ".DIST_DEVEL_WAS_INSTALLED" || exit 1
                    NEED_USR_LOCAL_LIB_NOTE="$PLATFORM_HAS_LIBRARY_PATH_ISSUE"
                else
                    echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                    ERROR="1"
                fi
            fi
            for x in $EXTENSIONS; do
                EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                if [ -e "$EXT_HOME/.DIST_WAS_CONFIGURED" -a -e "$EXT_HOME/.DIST_WAS_BUILT" ]; then
                    echo "INSTALLING Extension '$x'" | tee -a "$LOG_FILE"
                    if sh "$EXT_HOME/build.sh" install >>"$LOG_FILE" 2>&1; then
                        touch "$EXT_HOME/.DIST_WAS_INSTALLED" || exit 1
                        if [ "$x" = "c" -o "$x" = "objc" ]; then
                            NEED_USR_LOCAL_LIB_NOTE="$PLATFORM_HAS_LIBRARY_PATH_ISSUE"
                        fi
                    else
                        echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                        ERROR="1"
                    fi
                fi
            done
            if [ "$NEED_USR_LOCAL_LIB_NOTE" ]; then
                if [ -z "$INTERACTIVE" ]; then
                    LIBDIR="$(make get-libdir)" || exit 1
                    cat <<EOF

NOTE: Shared libraries have been installed in '$LIBDIR'.

We believe that on your system this directory is not part of the
default library search path. If this is true, you probably have to do
one of the following things to successfully use TightDB in a C, C++,
or Objective-C application:

 - Either run 'export LD_RUN_PATH=$LIBDIR' before building your
   application.

 - Or run 'export LD_LIBRARY_PATH=$LIBDIR' before launching your
   application.

 - Or add '$LIBDIR' to the system-wide library search path by editing
   /etc/ld.so.conf.

EOF
                fi
            fi
            echo "DONE INSTALLING" | tee -a "$LOG_FILE"
        else
            echo "Failed!" | tee -a "$LOG_FILE" 1>&2
            ERROR="1"
        fi
        if [ "$ERROR" ]; then
            echo "Log file is here: $LOG_FILE" 1>&2
            exit 1
        fi
        if [ -z "$INTERACTIVE" ]; then
            cat <<EOF

At this point you should run the following command to check that all
installed parts are working properly. If any parts failed to install,
they will be skipped during this test:

    ./build test-installed

EOF
        else
            echo
            echo "Installation summary"
            echo "===================="
            for x in $EXTENSIONS; do
                EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                if [ -e "$EXT_HOME/.DIST_WAS_CONFIGURED" -a -e "$EXT_HOME/.DIST_WAS_BUILT" -a -e "$EXT_HOME/.DIST_WAS_INSTALLED "]; then
                    sh "$EXT_HOME/build.sh" install-report
                fi
            done
        fi
        exit 0
        ;;


    "dist-uninstall")
        if ! [ -e ".DIST_WAS_CONFIGURED" ]; then
            cat 1>&2 <<EOF
ERROR: Nothing was configured.
You need to run './build config' first.
EOF
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-uninstall.XXXX)" || exit 1
        chmod a+rx "$TEMP_DIR" || exit 1
        LOG_FILE="$(new_dist_log_path "uninstall" "$TEMP_DIR")" || exit 1
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
                    echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                    ERROR="1"
                fi
                rm -f "$EXT_HOME/.DIST_WAS_INSTALLED" || exit 1
            fi
        done
        if [ -e ".DIST_DEVEL_WAS_CONFIGURED" ]; then
            echo "UNINSTALLING Extension 'c++'" | tee -a "$LOG_FILE"
            if ! sh build.sh uninstall-devel >>"$LOG_FILE" 2>&1; then
                echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                ERROR="1"
            fi
            rm -f ".DIST_DEVEL_WAS_INSTALLED" || exit 1
        fi
        echo "UNINSTALLING Core library" | tee -a "$LOG_FILE"
        if ! sh build.sh uninstall-shared >>"$LOG_FILE" 2>&1; then
            echo "Failed!" | tee -a "$LOG_FILE" 1>&2
            ERROR="1"
        fi
        rm -f ".DIST_WAS_INSTALLED" || exit 1
        echo "DONE UNINSTALLING" | tee -a "$LOG_FILE"
        if [ "$ERROR" ]; then
            echo "Log file is here: $LOG_FILE" 1>&2
            exit 1
        fi
        exit 0
        ;;


    "dist-test-installed")
        if ! [ -e ".DIST_WAS_INSTALLED" ]; then
            cat 1>&2 <<EOF
ERROR: Nothing was installed.
You need to run 'sudo ./build install' first.
EOF
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-test-installed.XXXX)" || exit 1
        LOG_FILE="$(new_dist_log_path "test-installed" "$TEMP_DIR")" || exit 1
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
        if [ -e ".DIST_DEVEL_WAS_INSTALLED" ]; then
            echo "TESTING Installed extension 'c++'" | tee -a "$LOG_FILE"
            if sh build.sh test-installed >>"$LOG_FILE" 2>&1; then
                echo "Success!" | tee -a "$LOG_FILE"
            else
                echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                ERROR="1"
            fi
        fi
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/.DIST_WAS_INSTALLED" ]; then
                echo "TESTING Installed extension '$x'" | tee -a "$LOG_FILE"
                if sh "$EXT_HOME/build.sh" test-installed >>"$LOG_FILE" 2>&1; then
                    echo "Success!" | tee -a "$LOG_FILE"
                else
                    echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                    ERROR="1"
                fi
            fi
        done
        echo "DONE TESTING" | tee -a "$LOG_FILE"
        if [ "$ERROR" ]; then
            echo "Log file is here: $LOG_FILE" 1>&2
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
/generic.mk
/config.mk
/Makefile
/src
/test
/test-installed
/doc
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
        (cd "$TARGET_DIR" && tar xzf "$TEMP_DIR/archive.tar.gz") || exit 1
        (cd "$TARGET_DIR" && pandoc README.md -o README.pdf) || exit 1
        exit 0
        ;;

    "dist-deb")
        codename=$(lsb_release -s -c)
        (cd debian && sed -e "s/@CODENAME@/$codename/g" changelog.in > changelog) || exit 1
        dpkg-buildpackage -rfakeroot -us -uc || exit 1
        exit 0
        ;;

    *)
        echo "Unspecified or bad mode '$MODE'" 1>&2
        echo "Available modes are: config clean build build-iphone test test-debug install uninstall test-installed wipe-installed" 1>&2
        echo "As well as: install-shared install-devel uninstall-shared uninstall-devel dist-copy" 1>&2
        echo "As well as: src-dist bin-dist dist-deb dist-status dist-pull dist-checkout" 1>&2
        echo "As well as: dist-config dist-clean dist-build dist-build-iphone dist-install dist-uninstall dist-test-installed" 1>&2
        exit 1
        ;;
esac
