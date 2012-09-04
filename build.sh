cd "$(dirname "$0")"
TIGHTDB_HOME="$(pwd)"

MODE="$1"
[ $# -gt 0 ] && shift

EXTENSIONS="tightdb_java2 tightdb_python tightdb_objc tightdb_node tightdb_php tightdb_gui"

MAKE="make -j8"
ARCH_FLAGS=""
LD_LIBRARY_PATH_NAME="LD_LIBRARY_PATH"


# Setup OS specific stuff
OS="$(uname -s)" || exit 1
if [ "$OS" = "Darwin" ]; then
    MAKE="$MAKE CC=clang"
    # Construct fat binaries on Darwin
    ARCH_FLAGS="-arch i386 -arch x86_64"
    LD_LIBRARY_PATH_NAME="DYLD_LIBRARY_PATH"
fi



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
            EXT_HOME="../$x"
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
            EXT_HOME="../$x"
            echo "Testing transfer of extension '$x' to package" >> "$LOG_FILE"
            mkdir "$FAKE_PKG_DIR/$x" || exit 1
            if ! sh "../$x/build.sh" dist-copy "$FAKE_PKG_DIR/$x" >>"$LOG_FILE" 2>&1; then
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
            EXT_HOME="../$x"
            if [ "$(cd "$EXT_HOME" && git status --porcelain)" ]; then
                echo ">>>>>>>> WARNING: Dirty working directory '$EXT_HOME'" | tee -a "$LOG_FILE"
            fi
        done


        {
            if [ -z "$AVAIL_EXTENSIONS" ]; then
                echo "Continuing with no extensions"
            else
                echo "Continuing with these extensions:"
                {
                    for x in $AVAIL_EXTENSIONS; do
                        EXT_HOME="../$x"
                        echo "  $x  ->  $EXT_HOME"
                    done
                } | column -t || exit 1
            fi
        } | tee -a "$LOG_FILE" || exit 1


        # Setup package directory
        VERSION="$(git describe)" || exit 1
        echo "tightdb version: $VERSION"
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
                echo "NO_BUILD_ON_INSTALL = 1" >> "$PKG_DIR/tightdb/config.mk"
                cat <<EOI > "$PKG_DIR/Makefile"
all: build
build:
	@sh tightdb/build.sh dist-build
install:
	@sh tightdb/build.sh dist-install \$(prefix)
clean:
	@sh tightdb/build.sh dist-clean
EOI

                for x in $AVAIL_EXTENSIONS; do
                    echo "Transfering extension '$x' to package" | tee -a "$LOG_FILE"
                    mkdir "$PKG_DIR/$x" || exit 1
                    sh "../$x/build.sh" dist-copy "$PKG_DIR/$x" >>"$LOG_FILE" 2>&1 || exit 1
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
                    if ! sh "$TEST_PKG_DIR/$x/build.sh" build >>"$LOG_FILE" 2>&1; then
                        echo ">>>>>>>> WARNING: Failed to build extension '$x'" | tee -a "$LOG_FILE"
                    else
                        echo "Testing extension '$x'" >> "$LOG_FILE"
                        if ! sh "$TEST_PKG_DIR/$x/build.sh" test >>"$LOG_FILE" 2>&1; then
                            echo ">>>>>>>> WARNING: Test suite failed for extension '$x'" | tee -a "$LOG_FILE"
                        fi
                        echo "Installing extension '$x' to test location" >> "$LOG_FILE"
                        if ! sh "$TEST_PKG_DIR/$x/build.sh" install "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1; then
                            echo ">>>>>>>> WARNING: Installation failed for extension '$x'" | tee -a "$LOG_FILE"
                        else
                            echo "Testing state of extension '$x' installation" >> "$LOG_FILE"
                            if ! sh "$TEST_PKG_DIR/$x/build.sh" test-installed "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1; then
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
        rm -f successfull_extensions
        for x in $EXTENSIONS; do
            EXT_HOME="../$x"
            if [ -r "$EXT_HOME/build.sh" ]; then
                echo ">>>>>>>> CLEANING '$x'"
                sh "$EXT_HOME/build.sh" clean
            fi
        done
        exit 0
        ;;


    "dist-build")
        path_list_prepend CPATH                   "$TIGHTDB_HOME/src"         || exit 1
        path_list_prepend LIBRARY_PATH            "$TIGHTDB_HOME/src/tightdb" || exit 1
        path_list_prepend "$LD_LIBRARY_PATH_NAME" "$TIGHTDB_HOME/src/tightdb" || exit 1
        export CPATH LIBRARY_PATH "$LD_LIBRARY_PATH_NAME"
        AVAIL_EXTENSIONS=""
        for x in $EXTENSIONS; do
            EXT_HOME="../$x"
            if [ -r "$EXT_HOME/build.sh" ]; then
                echo ">>>>>>>> BUILDING '$x'"
                if sh "$EXT_HOME/build.sh" build; then
                    if [ "$AVAIL_EXTENSIONS" ]; then
                        AVAIL_EXTENSIONS="$AVAIL_EXTENSIONS $x"
                    else
                        AVAIL_EXTENSIONS="$x"
                    fi
                fi
            fi
        done
        printf "%s\n" "$AVAIL_EXTENSIONS" > successfull_extensions
        exit 0
        ;;


    "dist-install")
        PREFIX="$1"
        INSTALL=install
        if [ "$PREFIX" ]; then
            INSTALL="$INSTALL $PREFIX"
        fi
        echo ">>>>>>>> INSTALLING 'tightdb'"
        sh build.sh $INSTALL || exit 1
        touch successfull_extensions
        for x in $(cat successfull_extensions); do
            echo ">>>>>>>> INSTALLING '$x'"
            sh "../$x/build.sh" $INSTALL
        done
        exit 0
        ;;


    "dist-test-installed")
        PREFIX="$1"
        TEST_INSTALLED=install
        if [ "$PREFIX" ]; then
            TEST_INSTALLED="$TEST_INSTALLED $PREFIX"
        fi
        echo ">>>>>>>> TESTING INSTALLATION OF 'tightdb'"
        sh build.sh $TEST_INSTALLED || exit 1
        touch successfull_extensions
        for x in $(cat successfull_extensions); do
            echo ">>>>>>>> TESTING INSTALLATION OF '$x'"
            if sh "../$x/build.sh" $TEST_INSTALLED >/dev/null 2>&1; then
                echo "OK"
            else
                echo "FAILED!!!"
            fi
        done
        exit 0
        ;;


    "dist-status")
        echo ">>>>>>>> STATUS OF 'tightdb'"
        git status
        for x in $EXTENSIONS; do
            EXT_HOME="../$x"
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
            EXT_HOME="../$x"
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
