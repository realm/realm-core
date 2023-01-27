#!/usr/bin/env bash

SCRIPT=$(basename "${BASH_SOURCE[0]}")
ROOT_DIR=$(git rev-parse --show-toplevel)
BUILD_DIR="build.realm.fuzzer.libfuzz"

build_mode="Debug"
corpus=""
fuzz_test="realm-libfuzz"

if [ "$#" -ne 2 ]; then
    echo "Usage: ${SCRIPT} <build_mode> <corpus>"
    echo "build mode  : either Debug or Release. Default ${build_mode}."
    echo "corpus (initial path seed for fuzzing)  : e.g ./test/test.txt. Default no seed used."
fi

if ! [[ -z "$1" ]]; then 
    build_mode="$1"
fi
if ! [[ -z "$2" ]]; then 
    corpus="$2"
fi

echo "Building..."

cd "${ROOT_DIR}" || exit

mkdir -p "${BUILD_DIR}"
    
cd "${BUILD_DIR}" || exit
if [ -z ${REALM_MAX_BPNODE_SIZE} ]; then
    REALM_MAX_BPNODE_SIZE=$(python -c "import random; print ((random.randint(4,999), 1000)[bool(random.randint(0,1))])")
fi

cmake -D REALM_LIBFUZZER=ON \
      -D CMAKE_BUILD_TYPE=${build_mode} \
      -D CMAKE_C_COMPILER=clang \
      -D CMAKE_CXX_COMPILER=clang++ \
      -D REALM_MAX_BPNODE_SIZE="${REALM_MAX_BPNODE_SIZE}" \
      -D REALM_ENABLE_ENCRYPTION=ON \
      -G Ninja \
      ..

ninja "${fuzz_test}"
EXEC=$(find . -name ${fuzz_test})
echo "Going to fuzz with LibFuzz: ${PWD}/${EXEC}"
./${EXEC} ${corpus}