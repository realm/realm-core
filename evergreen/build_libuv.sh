#!/bin/bash
set -o pipefail
set -o errexit
set -o xtrace

BASE_PATH=$(cd $(dirname "$0"); pwd)
source $BASE_PATH/cmake_vars_utils.sh

CMAKE=${CMAKE:=cmake}
GENERATOR="${GENERATOR:=Unix Makefiles}"
if [ -n "$CC" ]; then
    CC="-DCMAKE_C_COMPILER=$CC"
fi

while getopts 'p:e:b:nj:c:' c
do
    case $c in
        p) PREFIX=$(realpath $OPTARG) ;;
        e) EXTRA_ARGS=$OPTARG ;;
        b) BRANCH=$OPTARG ;;
        n) NO_CLEANUP=true ;;
        j) JOBS="-j$OPTARG" ;;
        c) BUILD_CONFIG=$OPTARG ;;
    esac
done

if [ -z $PREFIX ]; then
    echo "Error: must set install prefix"
    exit 2
fi
if [ -z $BRANCH ]; then
    echo "Error: must set branch of libuv to build"
    exit 2
fi

if [ "$OS" = "Windows_NT" ]; then
    PREFIX=$(cygpath -ma $PREFIX)
else
    PREFIX=$($BASE_PATH/abspath.sh $PREFIX)
fi

BUILD_CONFIG=${BUILD_CONFIG:=Debug}
SOURCE_DIR=$(mktemp -d libuv-XXXXX)
cleanup() {
    cd $TOP_DIR
    rm -rf $SOURCE_DIR
}
if [ -n $NO_CLEANUP ]; then
    trap cleanup EXIT
    trap cleanup SIGINT
fi

git clone git@github.com:libuv/libuv.git --branch $BRANCH --depth 1 $SOURCE_DIR
cd $SOURCE_DIR

$CMAKE \
    -B build \
    -G "$GENERATOR" \
    -DCMAKE_INSTALL_PREFIX="$PREFIX" \
    -DCMAKE_INSTALL_LIBDIR="lib" \
    -DCMAKE_BUILD_TYPE="$BUILD_CONFIG" \
    -DLIBUV_BUILD_TESTS=Off \
    -DLIBUV_BUILD_BENCH=Off \
    $CC $EXTRA_ARGS
$CMAKE --build build $JOBS --config "$BUILD_CONFIG"
$CMAKE --install build --config "$BUILD_CONFIG"

if [ "$OS" = "Windows_NT" ]; then
    set_cmake_var libuv_vars LibUV_LIBRARY PATH "$(cygpath -ma $PREFIX/lib/Debug/uv.lib)"
    set_cmake_var libuv_vars LibUV_INCLUDE_DIR PATH "$(cygpath -ma $PREFIX/include)"
else
    set_cmake_var libuv_vars LibUV_LIBRARY PATH "$PREFIX/lib/libuv.so"
    set_cmake_var libuv_vars LibUV_INCLUDE_DIR PATH "$PREFIX/include"
fi
