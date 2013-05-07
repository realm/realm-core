# NOTE: THIS SCRIPT IS SUPPOSED TO RUN IN A POSIX SHELL

cd "$(dirname "$0")"
TIGHTDB_HOME="$(pwd)"

MODE="$1"
[ $# -gt 0 ] && shift

EXTENSIONS="java python objc node php c gui"

if [ "$TIGHTDB_ENABLE_REPLICATION" ]; then
    EXTENSIONS="$EXTENSIONS replication"
fi


map_ext_name_to_dir()
{
    local ext_name
    ext_name="$1"
    case $ext_name in
        "java") echo "tightdb_java2";;
        *)      echo "tightdb_$ext_name";;
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
NEED_USR_LOCAL_LIB_NOTE=""
USE_LIB64=""
IS_REDHAT_DERIVATIVE=""
if [ -e /etc/redhat-release ] || grep -q "Amazon" /etc/system-release 2>/dev/null; then
    IS_REDHAT_DERIVATIVE="1"
fi
if [ "$IS_REDHAT_DERIVATIVE" ]; then
    NEED_USR_LOCAL_LIB_NOTE="1"
fi
if [ "$IS_REDHAT_DERIVATIVE" -o -e /etc/SuSE-release ]; then
    if [ "$ARCH" = "x86_64" -o "$ARCH" = "ia64" ]; then
        USE_LIB64="1"
    fi
fi



case "$MODE" in

    "clean")
        make clean || exit 1
        if [ "$OS" = "Darwin" ]; then
            PLATFORMS="iPhoneOS iPhoneSimulator"
            for x in $PLATFORMS; do
                make -C "src/tightdb" BASE_DENOM="$x" clean || exit 1
            done
            make -C "src/tightdb" BASE_DENOM="ios" clean || exit 1
        fi
        exit 0
        ;;

    "build")
        TIGHTDB_ENABLE_FAT_BINARIES="1" make || exit 1
        if [ "$OS" = "Darwin" ]; then
            TEMP_DIR="$(mktemp -d /tmp/tightdb.build.XXXX)" || exit 1
            # Xcode provides the iPhoneOS SDK
            XCODE_HOME="$(xcode-select --print-path)" || exit 1
            PLATFORMS="iPhoneOS iPhoneSimulator"
            for x in $PLATFORMS; do
                PLATFORM_HOME="$XCODE_HOME/Platforms/$x.platform"
                if ! [ -e "$PLATFORM_HOME/Info.plist" ]; then
                    echo "Failed to find '$PLATFORM_HOME/Info.plist'" 1>&2
                    exit 1
                fi
                mkdir "$TEMP_DIR/$x" || exit 1
                for y in "$PLATFORM_HOME/Developer/SDKs"/*; do
                    VERSION="$(defaults read "$y/SDKSettings" Version)" || exit 1
                    if ! printf "%s\n" "$VERSION" | grep -q '^[0-9][0-9]*\(\.[0-9][0-9]*\)\{0,3\}$'; then
                        echo "Uninterpretable version '$VERSION' in '$y'" 1>&2
                        exit 1
                    fi
                    if [ -e "$TEMP_DIR/$x/$VERSION" ]; then
                        echo "Ambiguous version '$VERSION' in '$y'" 1>&2
                        exit 1
                    fi
                    printf "%s\n" "$y" >"$TEMP_DIR/$x/$VERSION"
                    printf "%s\n" "$VERSION" >>"$TEMP_DIR/$x/versions"
                done
                if ! [ -e "$TEMP_DIR/$x/versions" ]; then
                    echo "Found no SDKs in '$PLATFORM_HOME'" 1>&2
                    exit 1
                fi
                sort -t . -k 1,1nr -k 2,2nr -k 3,3nr -k 4,4nr "$TEMP_DIR/$x/versions" >"$TEMP_DIR/$x/versions-sorted" || exit 1
                LATEST="$(cat "$TEMP_DIR/$x/versions-sorted" | head -n 1)" || exit 1
                (cd "$TEMP_DIR/$x" && ln "$LATEST" "sdk_root") || exit 1
                if [ "$x" = "iPhoneSimulator" ]; then
                    ARCH="i386"
                else
                    TYPE="$(defaults read-type "$PLATFORM_HOME/Info" "DefaultProperties")" || exit 1
                    if [ "$TYPE" != "Type is dictionary" ]; then
                        echo "Unexpected type of value of key 'DefaultProperties' in '$PLATFORM_HOME/Info.plist'" 1>&2
                        exit 1
                    fi
                    CHUNK="$(defaults read "$PLATFORM_HOME/Info" "DefaultProperties")" || exit 1
                    defaults write "$TEMP_DIR/$x/chunk" "$CHUNK" || exit 1
                    ARCH="$(defaults read "$TEMP_DIR/$x/chunk" NATIVE_ARCH)" || exit 1
                fi
                printf "%s\n" "$ARCH" >"$TEMP_DIR/$x/arch"
            done
            for x in $PLATFORMS; do
                PLATFORM_HOME="$XCODE_HOME/Platforms/$x.platform"
                SDK_ROOT="$(cat "$TEMP_DIR/$x/sdk_root")" || exit 1
                ARCH="$(cat "$TEMP_DIR/$x/arch")" || exit 1
                make -C "src/tightdb" BASE_DENOM="$x" CFLAGS_ARCH="-arch $ARCH -isysroot $SDK_ROOT" "libtightdb-$x.a" "libtightdb-$x-dbg.a" || exit 1
                cp "src/tightdb/libtightdb-$x.a"     "$TEMP_DIR/$x/libtightdb.a"     || exit 1
                cp "src/tightdb/libtightdb-$x-dbg.a" "$TEMP_DIR/$x/libtightdb-dbg.a" || exit 1
            done
            lipo "$TEMP_DIR"/*/"libtightdb.a"     -create -output "src/tightdb/libtightdb-ios.a"     || exit 1
            lipo "$TEMP_DIR"/*/"libtightdb-dbg.a" -create -output "src/tightdb/libtightdb-ios-dbg.a" || exit 1
            make -C "src/tightdb" BASE_DENOM="ios" "tightdb-config-ios" "tightdb-config-ios-dbg" || exit 1
        fi
        exit 0
        ;;

    "test")
        make test || exit 1
        exit 0
        ;;

    "install")
        PREFIX="$1"
        if ! [ "$PREFIX" ]; then
            PREFIX="/usr/local"
        fi
        if [ "$USE_LIB64" ]; then
            LIBDIR="$PREFIX/lib64"
        else
            LIBDIR="$PREFIX/lib"
        fi
        make prefix="$PREFIX" libdir="$LIBDIR" install || exit 1
        if [ "$USER" = "root" ] && which ldconfig >/dev/null; then
            ldconfig || exit 1
        fi
        exit 0
        ;;

    "test-installed")
        PREFIX="$1"
        make -C "test-installed" clean || exit 1
        make -C "test-installed" test  || exit 1
        exit 0
        ;;

    "src-dist"|"bin-dist")
        if [ "$MODE" = "bin-dist" ]; then
            PREBUILT_CORE="1"
        fi

        EXTENSION_AVAILABILITY_REQUIRED="1"
        if [ "$#" -eq 1 -a "$1" = "all" ]; then
            INCLUDE_EXTENSIONS="$EXTENSIONS"
        elif [ "$#" -eq 1 -a "$1" = "avail" ]; then
            INCLUDE_EXTENSIONS="$EXTENSIONS"
            EXTENSION_AVAILABILITY_REQUIRED=""
        elif [ "$#" -eq 1 -a "$1" = "none" ]; then
            INCLUDE_EXTENSIONS=""
        elif [ $# -ge 1 -a "$1" != "not" ]; then
            for x in "$@"; do
                found=""
                for y in $EXTENSIONS; do
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
        elif [ "$#" -ge 1 -a "$1" = "not" ]; then
            if [ "$#" -eq 1 ]; then
                echo "Please specify which extensions to exclude" 1>&2
                exit 1
            fi
            shift
            for x in "$@"; do
                found=""
                for y in $EXTENSIONS; do
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
        else
            echo "Please specify which extensions to include" 1>&2
            echo "Specify 'all' to include all extensions." 1>&2
            echo "Specify 'avail' to include all available extensions." 1>&2
            echo "Specify 'none' to exclude all extensions." 1>&2
            echo "Specify 'EXT1  [EXT2]...' to include the specified extensions." 1>&2
            echo "Specify 'not  EXT1  [EXT2]...' to exclude the specified extensions." 1>&2
            exit 1
        fi

        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist.XXXX)" || exit 1
        LOG_FILE="$TEMP_DIR/build.log"
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


        message "Checking availability of extensions"
        failed=""
        AVAIL_EXTENSIONS=""
        for x in $INCLUDE_EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if ! [ -r "$EXT_HOME/build.sh" ]; then
                if [ "$EXTENSION_AVAILABILITY_REQUIRED" ]; then
                    echo "Missing extension '$EXT_HOME'" 1>&2
                    failed="1"
                else
                    warning "Missing extension '$EXT_HOME'"
                fi
                continue
            fi
            word_list_append AVAIL_EXTENSIONS "$x" || exit 1
        done
        if [ "$failed" ]; then
            exit 1;
        fi


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
                    echo "Transfer of extension '$x' to test package failed" 1>&2
                    exit 1
                else
                    warning "Transfer of extension '$x' to test package failed"
                fi
                continue
            fi
            word_list_append NEW_AVAIL_EXTENSIONS "$x" || exit 1
        done
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

        BRANCH="$(git rev-parse --abbrev-ref HEAD)" || exit 1
        VERSION="$(git describe)" || exit 1

        if [ -z "$AVAIL_EXTENSIONS" ]; then
            message "Continuing with no extensions"
        else
            message "Continuing with these parts:"
            {
                echo "core  ->  .  $BRANCH  $VERSION"
                for x in $AVAIL_EXTENSIONS; do
                    EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                    EXT_BRANCH="$(cd "$EXT_HOME" && git rev-parse --abbrev-ref HEAD)" || exit 1
                    EXT_VERSION="$(cd "$EXT_HOME" && git describe --always)" || exit 1
                    echo "$x  ->  $EXT_HOME  $EXT_BRANCH  $EXT_VERSION"
                done
            } >"$TEMP_DIR/continuing_with" || exit 1
            column -t "$TEMP_DIR/continuing_with" >"$TEMP_DIR/continuing_with2" || exit 1
            sed 's/^/  /' "$TEMP_DIR/continuing_with2" >"$TEMP_DIR/continuing_with3" || exit 1
            tee -a "$LOG_FILE" <"$TEMP_DIR/continuing_with3"
        fi


        # Setup package directory
        NAME="tightdb-$VERSION"
        PKG_DIR="$TEMP_DIR/$NAME"
        mkdir "$PKG_DIR" || exit 1
        INSTALL_ROOT="$TEMP_DIR/install"
        mkdir "$INSTALL_ROOT" || exit 1
        mkdir "$INSTALL_ROOT/include" "$INSTALL_ROOT/lib" "$INSTALL_ROOT/lib64" "$INSTALL_ROOT/bin" || exit 1

        # These three were added because when building for iOS on
        # Darwin, the binary targets are not installed.
        path_list_prepend LIBRARY_PATH            "$TIGHTDB_HOME/src/tightdb" || exit 1
        path_list_prepend PATH                    "$TIGHTDB_HOME/src/tightdb" || exit 1
        # FIXME: The problem with these is that they partially destroy
        # the value of the build test. We should instead transfer the
        # iOS target files to a special temporary proforma directory,
        # and add that diretory to LIBRARY_PATH and PATH above.

        path_list_prepend CPATH                   "$INSTALL_ROOT/include"     || exit 1
        path_list_prepend LIBRARY_PATH            "$INSTALL_ROOT/lib"         || exit 1
        path_list_prepend LIBRARY_PATH            "$INSTALL_ROOT/lib64"       || exit 1
        path_list_prepend "$LD_LIBRARY_PATH_NAME" "$INSTALL_ROOT/lib"         || exit 1
        path_list_prepend "$LD_LIBRARY_PATH_NAME" "$INSTALL_ROOT/lib64"       || exit 1
        path_list_prepend PATH                    "$INSTALL_ROOT/bin"         || exit 1
        export CPATH LIBRARY_PATH "$LD_LIBRARY_PATH_NAME" PATH

        if (
                BIN_CORE_ARG=""
                if [ "$PREBUILT_CORE" ]; then
                    BIN_CORE_ARG=" bin-core"
                fi
                cat <<EOI > "$PKG_DIR/build"
#!/bin/sh

EXTENSIONS="$AVAIL_EXTENSIONS"

if [ \$# -eq 1 -a "\$1" = "clean" ]; then
    sh tightdb/build.sh dist-clean$BIN_CORE_ARG || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "install" ]; then
    sh tightdb/build.sh dist-install || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "test" ]; then
    sh tightdb/build.sh dist-test-installed || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "all" ]; then
    sh tightdb/build.sh dist-build$BIN_CORE_ARG \$EXTENSIONS || exit 1
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
        sh tightdb/build.sh dist-build$BIN_CORE_ARG "\$@" || exit 1
        exit 0
    fi
    echo 1>&2
fi

cat README 1>&2
exit 1
EOI
                chmod +x "$PKG_DIR/build"

                cat <<EOI >"$PKG_DIR/README"
Build specific extensions: ./build  EXT1  [EXT2]...
Build everything:          ./build  all
Install what was built:    sudo  ./build  install
Test installation:         ./build  test
Start from scratch:        ./build  clean

Available extensions are: ${AVAIL_EXTENSIONS:-None}

EOI
                if [ "$PREBUILT_CORE" ]; then
                    cat <<EOI >>"$PKG_DIR/README"
During installation, the prebuilt core library will be installed along
with all the extensions that you have built yourself.
EOI
                else
                    cat <<EOI >>"$PKG_DIR/README"
When building is requested, the core library will be built along
with all the extensions that you have specified.
EOI
                fi

                for x in $AVAIL_EXTENSIONS; do
                    EXT_DIR="$(map_ext_name_to_dir "$x")" || exit 1
                    EXT_HOME="../$EXT_DIR"
                    if REMARKS="$(sh "$EXT_HOME/build.sh" dist-remarks 2>&1)"; then
                        cat <<EOI >>"$PKG_DIR/README"

Remarks for '$x':

$REMARKS
EOI
                    fi
                done

                mkdir "$PKG_DIR/tightdb" || exit 1
                if [ "$PREBUILT_CORE" ]; then
                    message "Building core library"
                    (sh build.sh clean && sh build.sh build) >>"$LOG_FILE" 2>&1 || exit 1

                    message "Running test suite for core library"
                    sh build.sh test >>"$LOG_FILE" 2>&1 || exit 1

                    message "Transfering prebuilt core library to package"
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
                    printf "\nNO_BUILD_ON_INSTALL = 1\n" >> "$PKG_DIR/tightdb/config.mk"
                    INST_HEADERS="$(cd src/tightdb && make get-inst-headers)" || exit 1
                    INST_LIBRARIES="$(cd src/tightdb && make get-inst-libraries)" || exit 1
                    (cd "src/tightdb" && cp -R -P $INST_HEADERS $INST_LIBRARIES "$PKG_DIR/tightdb/src/tightdb/") || exit 1
                    cp "src/tightdb/tightdb-config" "src/tightdb/tightdb-config-dbg" "$PKG_DIR/tightdb/src/tightdb/" || exit 1
                    if [ "$OS" = "Darwin" ]; then
                        cp "src/tightdb/libtightdb-ios.a" "src/tightdb/libtightdb-ios-dbg.a" "$PKG_DIR/tightdb/src/tightdb/" || exit 1
                        cp "src/tightdb/tightdb-config-ios" "src/tightdb/tightdb-config-ios-dbg" "$PKG_DIR/tightdb/src/tightdb/" || exit 1
                    fi
                else
                    message "Transfering core library to package"
                    sh "$TIGHTDB_HOME/build.sh" dist-copy "$PKG_DIR/tightdb" >>"$LOG_FILE" 2>&1 || exit 1
                fi

                for x in $AVAIL_EXTENSIONS; do
                    message "Transfering extension '$x' to package"
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

                if ! [ "$PREBUILT_CORE" ]; then
                    message "Building core library"
                    sh "$TEST_PKG_DIR/tightdb/build.sh" build >>"$LOG_FILE" 2>&1 || exit 1

                    message "Running test suite for core library"
                    sh "$TEST_PKG_DIR/tightdb/build.sh" test >>"$LOG_FILE" 2>&1 || exit 1
                fi

                message "Installing core library to test location"
                sh "$TEST_PKG_DIR/tightdb/build.sh" install "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1 || exit 1

                message "Testing state of core library installation"
                sh "$TEST_PKG_DIR/tightdb/build.sh" test-installed "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1 || exit 1

                for x in $AVAIL_EXTENSIONS; do
                    message "Testing extension '$x'"
                    log_message "Building extension '$x'"
                    EXT_DIR="$(map_ext_name_to_dir "$x")" || exit 1
                    if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" build >>"$LOG_FILE" 2>&1; then
                        warning "Failed to build extension '$x'"
                    else
                        log_message "Running test suite for extension '$x'"
                        if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" test >>"$LOG_FILE" 2>&1; then
                            warning "Test suite failed for extension '$x'"
                        fi
                        log_message "Installing extension '$x' to test location"
                        if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" install "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1; then
                            warning "Installation test failed for extension '$x'"
                        else
                            log_message "Testing state of test installation of extension '$x'"
                            if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" test-installed "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1; then
                                warning "Post installation test failed for extension '$x'"
                            fi
                        fi
                    fi
                done

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


    "dist-clean")
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-clean.XXXX)" || exit 1
        LOG_FILE="$TEMP_DIR/clean.log"
        ERROR=""
        rm -f ".TO_BE_INSTALLED" || exit 1
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
            if [ -r "$EXT_HOME/build.sh" ]; then
                echo "CLEANING Extension '$x'" | tee -a "$LOG_FILE"
                rm -f "$EXT_HOME/.TO_BE_INSTALLED" || exit 1
                if ! sh "$EXT_HOME/build.sh" clean >>"$LOG_FILE" 2>&1; then
                    echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                    ERROR="1"
                fi
            fi
        done
        if [ "$ERROR" ]; then
            echo "Log file is here: $LOG_FILE" 1>&2
            exit 1
        fi
        exit 0
        ;;


#    "dist-check-avail")
#        for x in $EXTENSIONS; do
#            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
#            if [ -r "$EXT_HOME/build.sh" ]; then
#                echo ">>>>>>>> CHECKING AVAILABILITY OF '$x'"
#                if sh "$EXT_HOME/build.sh" check-avail; then
#                    echo "YES!"
#                fi
#            fi
#        done
#        exit 0
#        ;;


    "dist-build")
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-build.XXXX)" || exit 1
        LOG_FILE="$TEMP_DIR/build.log"
        mkdir "$TEMP_DIR/select" || exit 1
        ERROR=""
        if [ "$1" = "bin-core" ]; then
            shift
            touch ".TO_BE_INSTALLED" || exit 1
        else
            echo "BUILDING Core library" | tee -a "$LOG_FILE"
            rm -f ".TO_BE_INSTALLED" || exit 1
            if sh "build.sh" build >>"$LOG_FILE" 2>&1; then
                touch ".TO_BE_INSTALLED" || exit 1
            else
                echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                ERROR="1"
            fi
        fi
        if ! [ "$ERROR" ]; then
            for x in "$@"; do
                touch "$TEMP_DIR/select/$x" || exit 1
            done
            if [ "$USE_LIB64" ]; then
                LIBDIR="/usr/local/lib64"
            else
                LIBDIR="/usr/local/lib"
            fi
            path_list_prepend CPATH        "$TIGHTDB_HOME/src"         || exit 1
            path_list_prepend LIBRARY_PATH "$TIGHTDB_HOME/src/tightdb" || exit 1
            path_list_prepend PATH         "$TIGHTDB_HOME/src/tightdb" || exit 1
            path_list_prepend LD_RUN_PATH  "$LIBDIR"                   || exit 1
            export CPATH LIBRARY_PATH PATH LD_RUN_PATH
            for x in $EXTENSIONS; do
                if [ -e "$TEMP_DIR/select/$x" ]; then
                    echo "BUILDING Extension '$x'" | tee -a "$LOG_FILE"
                    EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                    rm -f "$EXT_HOME/.TO_BE_INSTALLED" || exit 1
                    if sh "$EXT_HOME/build.sh" build >>"$LOG_FILE" 2>&1; then
                        touch "$EXT_HOME/.TO_BE_INSTALLED" || exit 1
                    else
                        echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                        ERROR="1"
                    fi
                fi
            done
        fi
        if [ "$ERROR" ]; then
            cat 1>&2 <<EOF
One or more parts failed to build. You may be missing one or more
dependencies. Check the README file for details. If this does not
help, check the log file.
The log file is here: $LOG_FILE
EOF
            exit 1
        fi
        exit 0
        ;;


    "dist-install")
        if ! [ -e ".TO_BE_INSTALLED" ]; then
            echo "Nothing to install" 1>&2
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-install.XXXX)" || exit 1
        chmod a+rx "$TEMP_DIR" || exit 1
        LOG_FILE="$TEMP_DIR/install.log"
        ERROR=""
        echo "INSTALLING Core library" | tee -a "$LOG_FILE"
        if sh build.sh install >>"$LOG_FILE" 2>&1; then
            touch ".WAS_INSTALLED" || exit 1
            if [ "$NEED_USR_LOCAL_LIB_NOTE" ]; then
                if [ "$USE_LIB64" ]; then
                    LIBDIR="/usr/local/lib64"
                else
                    LIBDIR="/usr/local/lib"
                fi
                cat <<EOF
NOTE:
Libraries have been installed in $LIBDIR.
On your system this directory is not in the library search path
by default, so you may have to add it to /etc/ld.so.conf manually.
EOF
            fi
            for x in $EXTENSIONS; do
                EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                if [ -e "$EXT_HOME/.TO_BE_INSTALLED" ]; then
                    echo "INSTALLING Extension '$x'" | tee -a "$LOG_FILE"
                    if sh "$EXT_HOME/build.sh" install >>"$LOG_FILE" 2>&1; then
                        touch "$EXT_HOME/.WAS_INSTALLED" || exit 1
                    else
                        echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                        ERROR="1"
                    fi
                fi
            done
        else
            echo "Failed!" | tee -a "$LOG_FILE" 1>&2
            ERROR="1"
        fi
        if [ "$ERROR" ]; then
            echo "Log file is here: $LOG_FILE" 1>&2
            exit 1
        fi
        exit 0
        ;;


    "dist-test-installed")
        if ! [ -e ".WAS_INSTALLED" ]; then
            echo "Nothing was installed" 1>&2
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-test-installed.XXXX)" || exit 1
        LOG_FILE="$TEMP_DIR/test.log"
        ERROR=""
        echo "TESTING Installation of core library" | tee -a "$LOG_FILE"
        if sh build.sh test-installed >>"$LOG_FILE" 2>&1; then
            echo "SUCCESS!"  | tee -a "$LOG_FILE"
        else
            echo "FAILED!!!" | tee -a "$LOG_FILE" 1>&2
            ERROR="1"
        fi
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/.WAS_INSTALLED" ]; then
                echo "TESTING Installation of extension '$x'" | tee -a "$LOG_FILE"
                if sh "$EXT_HOME/build.sh" test-installed >>"$LOG_FILE" 2>&1; then
                    echo "SUCCESS!"  | tee -a "$LOG_FILE"
                else
                    echo "FAILED!!!" | tee -a "$LOG_FILE" 1>&2
                    ERROR="1"
                fi
            fi
        done
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
            if [ -r "$EXT_HOME/build.sh" ]; then
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
            if [ -r "$EXT_HOME/build.sh" ]; then
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
            if [ -r "$EXT_HOME/build.sh" ]; then
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
/test/experiments
/test/benchmark-*
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
        exit 0
        ;;

    *)
        echo "Unspecified or bad mode '$MODE'" 1>&2
        echo "Available modes are: clean build test install test-installed" 1>&2
        echo "As well as: src-dist bin-dist dist-clean dist-build dist-install dist-test-installed dist-status dist-pull dist-checkout dist-copy" 1>&2
        exit 1
        ;;

esac
