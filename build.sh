cd "$(dirname "$0")"
TIGHTDB_HOME="$(pwd)"

MODE="$1"
[ $# -gt 0 ] && shift

LANG_BINDINGS="tightdb_java2 tightdb_python tightdb_objc tightdb_node tightdb_php"

MAKE="make -j8"



case "$MODE" in

    "dist")

        # Setup OS specific stuff
        OS="$(uname -s)" || exit 1
        if [ "$OS" = "Darwin" ]; then
            MAKE="$MAKE CC=clang"
        fi

        TEMP_DIR="$(mktemp -d /tmp/tightdb.dist.XXXX)" || exit 1
        LOG_FILE="$TEMP_DIR/build.log"


        echo "Checking availability of language bindings" | tee -a "$LOG_FILE"
        AVAIL_LANG_BINDINGS=""
        for x in $LANG_BINDINGS; do
            LANG_BIND_HOME="../$x"
            if ! [ -r "$LANG_BIND_HOME/build.sh" ]; then
                echo ">>>>>>>> WARNING: Missing language binding '$LANG_BIND_HOME'" | tee -a "$LOG_FILE"
                continue
            fi
            if [ "$AVAIL_LANG_BINDINGS" ]; then
                AVAIL_LANG_BINDINGS="$AVAIL_LANG_BINDINGS $x"
            else
                AVAIL_LANG_BINDINGS="$x"
            fi
        done


        # Checking that each language binding is capable of copying
        # itself to the package
        FAKE_PKG_DIR="$TEMP_DIR/fake_pkg"
        mkdir "$FAKE_PKG_DIR" || exit 1
        NEW_AVAIL_LANG_BINDINGS=""
        for x in $AVAIL_LANG_BINDINGS; do
            LANG_BIND_HOME="../$x"
            echo "Testing transfer of language binding '$x' to package" >> "$LOG_FILE"
            mkdir "$FAKE_PKG_DIR/$x" || exit 1
            if ! sh "../$x/build.sh" copy "$FAKE_PKG_DIR/$x" >>"$LOG_FILE" 2>&1; then
                echo ">>>>>>>> WARNING: Transfer of language binding '$x' to test package failed" | tee -a "$LOG_FILE"
                continue
            fi
            if [ "$NEW_AVAIL_LANG_BINDINGS" ]; then
                NEW_AVAIL_LANG_BINDINGS="$NEW_AVAIL_LANG_BINDINGS $x"
            else
                NEW_AVAIL_LANG_BINDINGS="$x"
            fi
        done
        AVAIL_LANG_BINDINGS="$NEW_AVAIL_LANG_BINDINGS"


        # Check state of working directories
        if [ "$(git status --porcelain)" ]; then
            echo ">>>>>>>> WARNING: Dirty working directory '../$(basename "$TIGHTDB_HOME")'" | tee -a "$LOG_FILE"
        fi
        for x in $AVAIL_LANG_BINDINGS; do
            LANG_BIND_HOME="../$x"
            if [ "$(cd "$LANG_BIND_HOME" && git status --porcelain)" ]; then
                echo ">>>>>>>> WARNING: Dirty working directory '$LANG_BIND_HOME'" | tee -a "$LOG_FILE"
            fi
        done


        {
            if [ -z "$AVAIL_LANG_BINDINGS" ]; then
                echo "Continuing with no language bindings"
            else
                echo "Continuing with these language bindings:"
                {
                    for x in $AVAIL_LANG_BINDINGS; do
                        LANG_BIND_HOME="../$x"
                        echo "  $x  ->  $LANG_BIND_HOME"
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
                ($MAKE clean && $MAKE) >>"$LOG_FILE" 2>&1 || exit 1

                echo "Testing core library" | tee -a "$LOG_FILE"
                $MAKE test >>"$LOG_FILE" 2>&1 || exit 1

                echo "Transfering prebuilt core library to package" | tee -a "$LOG_FILE"
                tar czf "$TEMP_DIR/core.tar.gz" src/tightdb/*.h src/tightdb/*.hpp src/tightdb/libtightdb* src/tightdb/Makefile src/*.hpp src/Makefile test/Makefile config.mk generic.mk Makefile build.sh || exit 1
                (cd "$PKG_DIR" && tar xf "$TEMP_DIR/core.tar.gz") || exit 1
                echo "NO_BUILD_ON_INSTALL = 1" >> "$PKG_DIR/config.mk"

                for x in $AVAIL_LANG_BINDINGS; do
                    echo "Transfering language binding '$x' to package" | tee -a "$LOG_FILE"
                    mkdir "$PKG_DIR/$x" || exit 1
                    sh "../$x/build.sh" copy "$PKG_DIR/$x" >>"$LOG_FILE" 2>&1 || exit 1
                done

                echo "Building the package" | tee -a "$LOG_FILE"
                (cd "$TEMP_DIR" && tar czf "$NAME.tar.gz" "$NAME/") || exit 1

                echo "Extracting package for test" | tee -a "$LOG_FILE"
                TEST_DIR="$TEMP_DIR/test"
                mkdir "$TEST_DIR" || exit 1
                (cd "$TEST_DIR" && tar xzf "$TEMP_DIR/$NAME.tar.gz") || exit 1
                TEST_PKG_DIR="$TEST_DIR/$NAME"
                cd "$TEST_PKG_DIR" || exit 1

                echo "Installing core library to test location" | tee -a "$LOG_FILE"
                (cd "$TEST_PKG_DIR" && $MAKE prefix="$INSTALL_ROOT" install >>"$LOG_FILE" 2>&1) || exit 1

                for x in $AVAIL_LANG_BINDINGS; do
                    echo "Testing language binding '$x'"
                    echo "Building language binding '$x'" >> "$LOG_FILE"
                    if ! sh "$TEST_PKG_DIR/$x/build.sh" build >>"$LOG_FILE" 2>&1; then
                        echo ">>>>>>>> WARNING: Failed to build language binding '$x'" | tee -a "$LOG_FILE"
                    else
                        echo "Testing language binding '$x'" >> "$LOG_FILE"
                        if ! sh "$TEST_PKG_DIR/$x/build.sh" test >>"$LOG_FILE" 2>&1; then
                            echo ">>>>>>>> WARNING: Test suite failed for language binding '$x'" | tee -a "$LOG_FILE"
                        fi
                        echo "Installing language binding '$x' to test location" >> "$LOG_FILE"
                        if ! sh "$TEST_PKG_DIR/$x/build.sh" install "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1; then
                            echo ">>>>>>>> WARNING: Installation failed for language binding '$x'" | tee -a "$LOG_FILE"
                        else
                            echo "Testing installation of language binding '$x'" >> "$LOG_FILE"
                            if ! sh "$TEST_PKG_DIR/$x/build.sh" test-installed "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1; then
                                echo ">>>>>>>> WARNING: Post installation test failed for language binding '$x'" | tee -a "$LOG_FILE"
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


    *)
        echo "Unspecified or bad mode '$MODE'" 1>&2
        exit 1
        ;;

esac
