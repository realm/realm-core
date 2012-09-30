# NOTE: THIS SCRIPT IS SUPPOSED TO RUN IN A POSIX SHELL

cd "$(dirname "$0")"
TIGHTDB_HOME="$(pwd)"

MODE="$1"
[ $# -gt 0 ] && shift

EXTENSIONS="java python objc node php gui"

NUM_PROCESSORS=""
MAKE="make"
ARCH_FLAGS=""
LD_LIBRARY_PATH_NAME="LD_LIBRARY_PATH"


# Setup OS specific stuff
OS="$(uname)" || exit 1
if [ "$OS" = "Darwin" ]; then
    MAKE="$MAKE CC=clang"
    # Construct fat binaries on Darwin
    ARCH_FLAGS="-arch i386 -arch x86_64"
    LD_LIBRARY_PATH_NAME="DYLD_LIBRARY_PATH"
    NUM_PROCESSORS="$(sysctl -n hw.ncpu)" || exit 1
else
    if [ -r /proc/cpuinfo ]; then
        NUM_PROCESSORS="$(cat /proc/cpuinfo | egrep 'processor[[:space:]]*:' | wc -l)" || exit 1
    fi
fi

if [ "$NUM_PROCESSORS" ]; then
    MAKE="$MAKE -j$NUM_PROCESSORS"
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



case "$MODE" in

    "clean")
        $MAKE clean || exit 1
        exit 0
        ;;

    "build")
        $MAKE EXTRA_CFLAGS="$ARCH_FLAGS" EXTRA_LDFLAGS="$ARCH_FLAGS" || exit 1
        exit 0
        ;;

    "test")
        $MAKE test || exit 1
        exit 0
        ;;

    "install")
        PREFIX="$1"
        INSTALL=install
        if [ "$PREFIX" ]; then
            INSTALL="prefix=$PREFIX $INSTALL"
        fi
        $MAKE $INSTALL || exit 1
        if [ "$USER" = "root" ] && which ldconfig >/dev/null; then
            ldconfig
        fi
        exit 0
        ;;

    "test-installed")
        PREFIX="$1"
        $MAKE test-installed || exit 1
        exit 0
        ;;


    "dist")
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist.XXXX)" || exit 1
        LOG_FILE="$TEMP_DIR/build.log"

        echo "Checking availability of extensions" | tee -a "$LOG_FILE"
        AVAIL_EXTENSIONS=""
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if ! [ -r "$EXT_HOME/build.sh" ]; then
                echo ">>>>>>>> WARNING: Missing extension '$EXT_HOME'" | tee -a "$LOG_FILE"
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
                echo ">>>>>>>> WARNING: Transfer of extension '$x' to test package failed" | tee -a "$LOG_FILE"
                continue
            fi
            word_list_append NEW_AVAIL_EXTENSIONS "$x" || exit 1
        done
        AVAIL_EXTENSIONS="$NEW_AVAIL_EXTENSIONS"


        # Check state of working directories
        if [ "$(git status --porcelain)" ]; then
            echo ">>>>>>>> WARNING: Dirty working directory '../$(basename "$TIGHTDB_HOME")'" | tee -a "$LOG_FILE"
        fi
        for x in $AVAIL_EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ "$(cd "$EXT_HOME" && git status --porcelain)" ]; then
                echo ">>>>>>>> WARNING: Dirty working directory '$EXT_HOME'" | tee -a "$LOG_FILE"
            fi
        done

        BRANCH="$(git rev-parse --abbrev-ref HEAD)" || exit 1
        VERSION="$(git describe)" || exit 1

        if [ -z "$AVAIL_EXTENSIONS" ]; then
            echo "Continuing with no extensions" | tee -a "$LOG_FILE"
        else
            echo "Continuing with these parts:"
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


        path_list_prepend CPATH                   "$INSTALL_ROOT/include" || exit 1
        path_list_prepend LIBRARY_PATH            "$INSTALL_ROOT/lib"     || exit 1
        path_list_prepend "$LD_LIBRARY_PATH_NAME" "$INSTALL_ROOT/lib"     || exit 1
        export CPATH LIBRARY_PATH "$LD_LIBRARY_PATH_NAME"


        if (
                echo "Building core library" | tee -a "$LOG_FILE"
                (sh build.sh clean && sh build.sh build) >>"$LOG_FILE" 2>&1 || exit 1

                echo "Running test suite for core library" | tee -a "$LOG_FILE"
                sh build.sh test >>"$LOG_FILE" 2>&1 || exit 1

                echo "Transfering prebuilt core library to package" | tee -a "$LOG_FILE"
                tar czf "$TEMP_DIR/core.tar.gz" src/tightdb/Makefile src/tightdb/*.h src/tightdb/*.hpp src/tightdb/libtightdb* src/Makefile src/*.hpp test/Makefile test-installed/Makefile test-installed/*.cpp config.mk generic.mk Makefile build.sh || exit 1
                mkdir "$PKG_DIR/tightdb" || exit 1
                (cd "$PKG_DIR/tightdb" && tar xf "$TEMP_DIR/core.tar.gz") || exit 1
                printf "\nNO_BUILD_ON_INSTALL = 1\n" >> "$PKG_DIR/tightdb/config.mk"
                cat <<EOI > "$PKG_DIR/build"
#!/bin/sh

EXTENSIONS="$AVAIL_EXTENSIONS"

if [ \$# -eq 1 -a "\$1" = "clean" ]; then
    sh tightdb/build.sh dist-clean || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "install" ]; then
    shift
    sh tightdb/build.sh dist-install || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "test" ]; then
    shift
    sh tightdb/build.sh dist-test-installed || exit 1
    exit 0
fi

if [ \$# -eq 1 -a "\$1" = "all" ]; then
    sh tightdb/build.sh dist-build \$EXTENSIONS || exit 1
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
        sh tightdb/build.sh dist-build "\$@" || exit 1
        exit 0
    fi
    echo 1>&2
fi

cat README 1>&2
exit 1
EOI
                chmod +x "$PKG_DIR/build"

                cat <<EOI >"$PKG_DIR/README"
Build specific extensions:   ./build  EXT1  [EXT2]...
Build all extensions:        ./build  all
Install what was built:      sudo  ./build  install
Test installation:           ./build  test
Start from scratch:          ./build  clean

Available extensions are: $AVAIL_EXTENSIONS

During installation, the prebuilt core library will be installed along
with all the extensions that you have built yourself.
EOI

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

                for x in $AVAIL_EXTENSIONS; do
                    echo "Transfering extension '$x' to package" | tee -a "$LOG_FILE"
                    EXT_DIR="$(map_ext_name_to_dir "$x")" || exit 1
                    EXT_HOME="../$EXT_DIR"
                    mkdir "$PKG_DIR/$EXT_DIR" || exit 1
                    sh "$EXT_HOME/build.sh" dist-copy "$PKG_DIR/$EXT_DIR" >>"$LOG_FILE" 2>&1 || exit 1
                done

                echo "Zipping the package" | tee -a "$LOG_FILE"
                (cd "$TEMP_DIR" && tar czf "$NAME.tar.gz" "$NAME/") || exit 1

                echo "Extracting the package for test" | tee -a "$LOG_FILE"
                TEST_DIR="$TEMP_DIR/test"
                mkdir "$TEST_DIR" || exit 1
                (cd "$TEST_DIR" && tar xzf "$TEMP_DIR/$NAME.tar.gz") || exit 1
                TEST_PKG_DIR="$TEST_DIR/$NAME"
                cd "$TEST_PKG_DIR" || exit 1

                echo "Installing core library to test location" | tee -a "$LOG_FILE"
                sh "$TEST_PKG_DIR/tightdb/build.sh" install "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1 || exit 1

                echo "Testing state of core library installation" | tee -a "$LOG_FILE"
                sh "$TEST_PKG_DIR/tightdb/build.sh" test-installed "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1 || exit 1

                for x in $AVAIL_EXTENSIONS; do
                    echo "Testing extension '$x'"
                    echo "Building extension '$x'" >> "$LOG_FILE"
                    EXT_DIR="$(map_ext_name_to_dir "$x")" || exit 1
                    if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" build >>"$LOG_FILE" 2>&1; then
                        echo ">>>>>>>> WARNING: Failed to build extension '$x'" | tee -a "$LOG_FILE"
                    else
                        echo "Testing extension '$x'" >> "$LOG_FILE"
                        if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" test >>"$LOG_FILE" 2>&1; then
                            echo ">>>>>>>> WARNING: Test suite failed for extension '$x'" | tee -a "$LOG_FILE"
                        fi
                        echo "Installing extension '$x' to test location" >> "$LOG_FILE"
                        if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" install "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1; then
                            echo ">>>>>>>> WARNING: Installation test failed for extension '$x'" | tee -a "$LOG_FILE"
                        else
                            echo "Testing state of extension '$x' installation" >> "$LOG_FILE"
                            if ! sh "$TEST_PKG_DIR/$EXT_DIR/build.sh" test-installed "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1; then
                                echo ">>>>>>>> WARNING: Post installation test failed for extension '$x'" | tee -a "$LOG_FILE"
                            fi
                        fi
                    fi
                done

                exit 0

            ); then
            echo "Log file is here: $LOG_FILE"
            echo "Package is here: $TEMP_DIR/$NAME.tar.gz"
            echo 'Done!'
        else
            echo "Log file is here: $LOG_FILE"
            echo 'FAILED!' 1>&2
            exit 1
        fi
        exit 0
        ;;


    "dist-clean")
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-clean.XXXX)" || exit 1
        LOG_FILE="$TEMP_DIR/clean.log"
        ERROR=""
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -r "$EXT_HOME/build.sh" ]; then
                echo ">>>>>>>> CLEANING '$x'" | tee -a "$LOG_FILE"
                rm -f "$EXT_HOME/TO_BE_INSTALLED" || exit 1
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


    "dist-check-avail")
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -r "$EXT_HOME/build.sh" ]; then
                echo ">>>>>>>> CHECKING AVAILABILITY OF '$x'"
                if sh "$EXT_HOME/build.sh" check-avail; then
                    echo "YES!"
                fi
            fi
        done
        exit 0
        ;;


    "dist-build")
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-build.XXXX)" || exit 1
        LOG_FILE="$TEMP_DIR/build.log"
        mkdir "$TEMP_DIR/select" || exit 1
        if [ $# -eq 0 ]; then
            for x in $EXTENSIONS; do
                EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                if [ -r "$EXT_HOME/build.sh" ]; then
                    touch "$TEMP_DIR/select/$x" || exit 1
                fi
            done
        else
            for x in "$@"; do
                EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                if [ -e "$EXT_HOME/build.sh" ]; then
                    touch "$TEMP_DIR/select/$x" || exit 1
                else
                    echo "ERROR: No such extension '$x'" 1>&2
                    exit 1
                fi
            done
        fi
        path_list_prepend CPATH                   "$TIGHTDB_HOME/src"         || exit 1
        path_list_prepend LIBRARY_PATH            "$TIGHTDB_HOME/src/tightdb" || exit 1
        path_list_prepend "$LD_LIBRARY_PATH_NAME" "$TIGHTDB_HOME/src/tightdb" || exit 1
        export CPATH LIBRARY_PATH "$LD_LIBRARY_PATH_NAME"
        ERROR=""
        for x in $EXTENSIONS; do
            if [ -e "$TEMP_DIR/select/$x" ]; then
                echo ">>>>>>>> BUILDING '$x'" | tee -a "$LOG_FILE"
                EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                rm -f "$EXT_HOME/TO_BE_INSTALLED" || exit 1
                if sh "$EXT_HOME/build.sh" build >>"$LOG_FILE" 2>&1; then
                    touch "$EXT_HOME/TO_BE_INSTALLED" || exit 1
                else
                    echo "Failed!" | tee -a "$LOG_FILE" 1>&2
                    ERROR="1"
                fi
            fi
        done
        if [ "$ERROR" ]; then
            cat 1>&2 <<EOF
Some extensions failed to build. You may be missing one or more
dependencies. Check the README file details. If this does not help,
check the log file.
The log file is here: $LOG_FILE
EOF
            exit 1
        fi
        exit 0
        ;;


    "dist-install")
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-install.XXXX)" || exit 1
        chmod a+rx "$TEMP_DIR" || exit 1
        LOG_FILE="$TEMP_DIR/install.log"
        ERROR=""
        echo ">>>>>>>> INSTALLING 'tightdb'" | tee -a "$LOG_FILE"
        if sh build.sh install >>"$LOG_FILE" 2>&1; then
            touch "WAS_INSTALLED" || exit 1
            for x in $EXTENSIONS; do
                EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
                if [ -e "$EXT_HOME/TO_BE_INSTALLED" ]; then
                    echo ">>>>>>>> INSTALLING '$x'" | tee -a "$LOG_FILE"
                    if sh "$EXT_HOME/build.sh" install >>"$LOG_FILE" 2>&1; then
                        touch "$EXT_HOME/WAS_INSTALLED" || exit 1
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
        if ! [ -e "WAS_INSTALLED" ]; then
            echo "Nothing was installed" 1>&2
            exit 1
        fi
        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist-test-installed.XXXX)" || exit 1
        LOG_FILE="$TEMP_DIR/test.log"
        ERROR=""
        echo ">>>>>>>> TESTING INSTALLATION OF 'tightdb'" | tee -a "$LOG_FILE"
        if sh build.sh test-installed >>"$LOG_FILE" 2>&1; then
            echo "SUCCESS!"  | tee -a "$LOG_FILE"
        else
            echo "FAILED!!!" | tee -a "$LOG_FILE" 1>&2
            ERROR="1"
        fi
        for x in $EXTENSIONS; do
            EXT_HOME="../$(map_ext_name_to_dir "$x")" || exit 1
            if [ -e "$EXT_HOME/WAS_INSTALLED" ]; then
                echo ">>>>>>>> TESTING INSTALLATION OF '$x'" | tee -a "$LOG_FILE"
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


    *)
        echo "Unspecified or bad mode '$MODE'" 1>&2
        echo "Available modes are: clean build test install test-installed" 1>&2
        echo "As well as: dist dist-clean dist-build dist-install dist-test-installed dist-status dist-pull" 1>&2
        exit 1
        ;;

esac
