cd "$(dirname "$0")"
TIGHTDB_HOME="$(pwd)"

MODE="$1"
[ $# -gt 0 ] && shift

EXTENSIONS="tightdb_java2 tightdb_python tightdb_objc tightdb_node tightdb_php"

MAKE="make -j8"

# Setup OS specific stuff
OS="$(uname -s)" || exit 1
if [ "$OS" = "Darwin" ]; then
    MAKE="$MAKE CC=clang"
fi



case "$MODE" in

    "clean")
        $MAKE clean || exit 1
        ;;

    "build")
        $MAKE || exit 1
        ;;

    "test")
        $MAKE test || exit 1
        ;;

    "install")
        PREFIX="$1"
        INSTALL=install
        if [ "$PREFIX" ]; then
            INSTALL="prefix=$PREFIX $INSTALL"
        fi
        $MAKE $INSTALL || exit 1
        ;;

    "test-installed")
        PREFIX="$1"
        $MAKE test-installed || exit 1
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
            if [ "$AVAIL_EXTENSIONS" ]; then
                AVAIL_EXTENSIONS="$AVAIL_EXTENSIONS $x"
            else
                AVAIL_EXTENSIONS="$x"
            fi
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
            if [ "$NEW_AVAIL_EXTENSIONS" ]; then
                NEW_AVAIL_EXTENSIONS="$NEW_AVAIL_EXTENSIONS $x"
            else
                NEW_AVAIL_EXTENSIONS="$x"
            fi
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
                } | column -t
            fi
        } | tee -a "$LOG_FILE"


        # Setup package directory
        VERSION="$(git describe)" || exit 1
        echo "tightdb version: $VERSION"
        NAME="tightdb-$VERSION"
        PKG_DIR="$TEMP_DIR/$NAME"
        mkdir "$PKG_DIR" || exit 1
        INSTALL_ROOT="$TEMP_DIR/install"


        if [ "$CPATH" ]; then
            export CPATH="$INSTALL_ROOT/include:$CPATH"
        else
            export CPATH="$INSTALL_ROOT/include"
        fi
        if [ "$LIBRARY_PATH" ]; then
            export LIBRARY_PATH="$INSTALL_ROOT/lib:$LIBRARY_PATH"
        else
            export LIBRARY_PATH="$INSTALL_ROOT/lib"
        fi
        if [ "$LD_LIBRARY_PATH" ]; then
            export LD_LIBRARY_PATH="$INSTALL_ROOT/lib:$LD_LIBRARY_PATH"
        else
            export LD_LIBRARY_PATH="$INSTALL_ROOT/lib"
        fi


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
	@sh tightdb/build.sh dist-build \$(prefix)
install:
	@sh tightdb/build.sh dist-install
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
                    if ! sh "$TEST_PKG_DIR/$x/build.sh" build "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1; then
                        echo ">>>>>>>> WARNING: Failed to build extension '$x'" | tee -a "$LOG_FILE"
                    else
                        echo "Testing extension '$x'" >> "$LOG_FILE"
                        if ! sh "$TEST_PKG_DIR/$x/build.sh" test "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1; then
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
        PREFIX="$1"
        BUILD=build
        if [ "$PREFIX" ]; then
            BUILD="$BUILD $PREFIX"
        fi
        echo "$PREFIX" > install_prefix
        if [ "$CPATH" ]; then
            export CPATH="$TIGHTDB_HOME/src:$CPATH"
        else
            export CPATH="$TIGHTDB_HOME/src"
        fi
        if [ "$LIBRARY_PATH" ]; then
            export LIBRARY_PATH="$TIGHTDB_HOME/src/tightdb:$LIBRARY_PATH"
        else
            export LIBRARY_PATH="$TIGHTDB_HOME/src/tightdb"
        fi
        if [ "$LD_LIBRARY_PATH" ]; then
            export LD_LIBRARY_PATH="$TIGHTDB_HOME/src/tightdb:$LD_LIBRARY_PATH"
        else
            export LD_LIBRARY_PATH="$TIGHTDB_HOME/src/tightdb"
        fi
        AVAIL_EXTENSIONS=""
        for x in $EXTENSIONS; do
            EXT_HOME="../$x"
            if [ -r "$EXT_HOME/build.sh" ]; then
                echo ">>>>>>>> BUILDING '$x'"
                if sh "$EXT_HOME/build.sh" $BUILD; then
                    if [ "$AVAIL_EXTENSIONS" ]; then
                        AVAIL_EXTENSIONS="$AVAIL_EXTENSIONS $x"
                    else
                        AVAIL_EXTENSIONS="$x"
                    fi
                fi
            fi
        done
        echo "$AVAIL_EXTENSIONS" > successfull_extensions
        exit 0
        ;;


    "dist-install")
        touch install_prefix
        PREFIX="$(cat install_prefix)"
        INSTALL=install
        if [ "$PREFIX" ]; then
            INSTALL="$INSTALL $PREFIX"
        fi
        echo ">>>>>>>> INSTALLING 'tightdb'"
        sh build.sh $INSTALL || exit 1
        touch successfull_extensions
        for x in $(cat successfull_extensions); do
            EXT_HOME="../$x"
            if [ -r "$EXT_HOME/build.sh" ]; then
                echo ">>>>>>>> INSTALLING '$x'"
                sh "$EXT_HOME/build.sh" $INSTALL
            fi
        done
        exit 0
        ;;


    *)
        echo "Unspecified or bad mode '$MODE'" 1>&2
        echo "Available modes are: clean build test install test-installed" 1>&2
        echo "As well as: dist dist-clean dist-build dist-install" 1>&2
        exit 1
        ;;

esac
