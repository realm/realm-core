cd "$(dirname "$0")"
TIGHTDB_HOME="$(pwd)"

LANG_BINDINGS="tightdb_java2 tightdb_python tightdb_objc tightdb_node tightdb_php"

MAKE="make -j8"


# Setup OS specific stuff
OS="$(uname -s)" || exit 1
if [ "$OS" = "Darwin" ]; then
    MAKE="$MAKE CC=clang"
fi


# Check availability of language bindings
for x in $LANG_BINDINGS; do
    LANG_BIND_HOME="$TIGHTDB_HOME/../$x"
    if ! [ -r "$LANG_BIND_HOME/build_dist.sh" ]; then
        echo ">>>>>>>> WARNING: Missing language binding '$x'" 1>&2
        continue
    fi
    if [ "$AVAIL_LANG_BINDINGS" ]; then
        AVAIL_LANG_BINDINGS="$AVAIL_LANG_BINDINGS $x"
    else
        AVAIL_LANG_BINDINGS="$x"
    fi
done


# Check state of working directories
if [ "$(git status --porcelain)" ]; then
    echo ">>>>>>>> WARNING: Dirty working directory '$TIGHTDB_HOME'" 1>&2
fi
for x in $AVAIL_LANG_BINDINGS; do
    LANG_BIND_HOME="$TIGHTDB_HOME/../$x"
    if [ "$(cd "$LANG_BIND_HOME" && git status --porcelain)" ]; then
        echo ">>>>>>>> WARNING: Dirty working directory '$LANG_BIND_HOME'" 1>&2
    fi
done


# Setup package directory
VERSION="$(git describe)" || exit 1
echo "tightdb version: $VERSION"
NAME="tightdb-$VERSION"
TEMP_DIR="$(mktemp -d /tmp/temp.XXXX)" || exit 1
PKG_DIR="$TEMP_DIR/$NAME"
mkdir "$PKG_DIR" || exit 1
LOG_FILE="$TEMP_DIR/build.log"
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
    $MAKE test-release >>"$LOG_FILE" 2>&1 || exit 1

    echo "Transfering prebuilt core library to package" | tee -a "$LOG_FILE"
    tar czf "$TEMP_DIR/core.tar.gz" src/tightdb/*.h src/tightdb/*.hpp src/tightdb/libtightdb* src/tightdb/Makefile src/*.hpp src/Makefile config.mk Makefile || exit 1
    (cd "$PKG_DIR" && tar xf "$TEMP_DIR/core.tar.gz") || exit 1

    for x in $AVAIL_LANG_BINDINGS; do
        echo "Transfering language binding '$x' to package" | tee -a "$LOG_FILE"
        mkdir "$PKG_DIR/$x" || exit 1
        sh "../$x/build_dist.sh" copy "$PKG_DIR/$x" >>"$LOG_FILE" 2>&1 || exit 1
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
        echo "Building language binding '$x'" | tee -a "$LOG_FILE"
        if ! sh "$TEST_PKG_DIR/$x/build_dist.sh" build >>"$LOG_FILE" 2>&1; then
            echo ">>>>>>>> WARNING: Failed to build language binding '$x'" 1>&2
        else
            echo "Testing language binding '$x'" | tee -a "$LOG_FILE"
            if ! sh "$TEST_PKG_DIR/$x/build_dist.sh" test >>"$LOG_FILE" 2>&1; then
                echo ">>>>>>>> WARNING: Testing failed for language binding '$x'" 1>&2
            fi
            echo "Installing language binding '$x' to test location" | tee -a "$LOG_FILE"
            if ! sh "$TEST_PKG_DIR/$x/build_dist.sh" install "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1; then
                echo ">>>>>>>> WARNING: Installation failed for language binding '$x'" 1>&2
            else
                echo "Testing installation of language binding '$x'" | tee -a "$LOG_FILE"
                if ! sh "$TEST_PKG_DIR/$x/build_dist.sh" test-installed "$INSTALL_ROOT" >>"$LOG_FILE" 2>&1; then
                    echo ">>>>>>>> WARNING: Post installation test failed for language binding '$x'" 1>&2
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
