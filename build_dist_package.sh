cd "$(dirname "$0")"
TIGHTDB_HOME="$(pwd)"

#LANG_BINDINGS="tightdb_java2 tightdb_node tightdb_python tightdb_php"
LANG_BINDINGS="tightdb_java2"

MAKE="make -j8"

# Setup OS specific stuff
OS="$(uname -s)" || exit 1
if [ "$OS" = "Darwin" ]; then
    MAKE="$MAKE CC=gcc"
fi


# Check that all language bindings are available
for x in $LANG_BINDINGS; do
    if ! [ -r "../$x/build.sh" ]; then
        echo "Missing language binding '$x'" 1>&2
        exit 1
    fi
done


# Setup package directory
VERSION="$(git describe)" || exit 1
NAME="tightdb-$VERSION"
TEMP_DIR="$(mktemp -d /tmp/temp.XXXX)" || exit 1
PKG_DIR="$TEMP_DIR/$NAME"
mkdir "$PKG_DIR" || exit 1
LOG_FILE="$TEMP_DIR/build.log"
export INSTALL_ROOT="$TEMP_DIR/install"


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

    echo "Installing core library to temporary directory" | tee -a "$LOG_FILE"
    $MAKE prefix="$INSTALL_ROOT" install >>"$LOG_FILE" 2>&1 || exit 1

    for x in $LANG_BINDINGS; do
        echo "Testing language binding '$x'" | tee -a "$LOG_FILE"
        sh "../$x/build.sh" >>"$LOG_FILE" 2>&1 || exit 1
    done

    echo "Transfering core library to package directory" | tee -a "$LOG_FILE"
    tar czf "$TEMP_DIR/core.tar.gz" src/tightdb/*.h src/tightdb/*.hpp src/tightdb/libtightdb* src/tightdb/Makefile src/*.hpp src/Makefile config.mk Makefile || exit 1
    (cd "$PKG_DIR" && tar xf "$TEMP_DIR/core.tar.gz") || exit 1

    echo "Building the package" | tee -a "$LOG_FILE"
    (cd "$TEMP_DIR" && tar czf "$NAME.tar.gz" "$NAME/") || exit 1

); then
    echo "Package is here: $TEMP_DIR/$NAME.tar.gz"
    echo 'Done!'
else
    echo "Log file is here: $LOG_FILE"
    echo 'FAILED!' 1>&2
    exit 1
fi
