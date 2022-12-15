# The Fuzz Framework project

This project is an attempt to put together all the small fuzzers we have already scattered around the code.
There are two goals:
    1. To be able to run all the fuzzers, collect crashes reports and fix possible bugs that the fuzzer might find.
    2. To be able to replace libfuzzer with google fuzz test (https://github.com/google/fuzztest) at some point.

AFL++ support is not dropped yet, but since we want to integrate things inside evergreen and follow the same approach we implement for address/thread sanitazer we prefer to use libfuzzer and clang.
## Prerequisites

In case you want to use AFL++, then you should install the latest version of the American Fuzzy Lop ++ (AFL++).
Please use this quick guide: https://aflplus.plus/building/ it requires llvm >= 9.0.

For using libfuzzer, the only pre-requisite is having a recent version of clang.
## Running
Note REALM_MAX_BPNODE_SIZE is the max number of nodes contained in the b+tree. It determines the depth of the tree and its fanout. 
This number should be random generated.

If you don't want to build manually, you can skip this section and jump to the `Scripts` section.
Run the fuzzer via AFL++:
`
cd <realm-core-src>
mkdir build
cd build
cmake -D CMAKE_BUILD_TYPE=${build_mode} \
      -D CMAKE_C_COMPILER=afl-cc \
      -D CMAKE_CXX_COMPILER=afl-c++ \
      -D REALM_MAX_BPNODE_SIZE="${REALM_MAX_BPNODE_SIZE}" \
      -D REALM_ENABLE_ENCRYPTION=ON \
      -G Ninja \
      ..
cmake --build . --target realm-afl++
 afl-fuzz -t "$time_out" \
        -m "$memory" \
        -i "${ROOT_DIR}/test/fuzzy_object_store/testcases" \
        -o "${FINDINGS_DIR}" \
        realm-afl++ @@
`

Run the fuzzer via libFuzzer (only with Clang)
`
cd <realm-core-src>
mkdir build
cd build
cmake -D REALM_LIBFUZZER=ON \
      -D CMAKE_BUILD_TYPE=${build_mode} \
      -D CMAKE_C_COMPILER=clang \
      -D CMAKE_CXX_COMPILER=clang++ \
      -D REALM_MAX_BPNODE_SIZE="${REALM_MAX_BPNODE_SIZE}" \
      -D REALM_ENABLE_ENCRYPTION=ON \
      -G Ninja \
      ..
cmake --build . --target realm-libfuzz
./realm_libfuzz <corpus>
`

## Scripts

`sh start_fuzz_afl.sh`
Builds `realm-core` and `object-store` in `Debug` mode using the afl++ compiler `afl-cc` and starts 1 instance of `afl-fuzz`.
It expects `AFLPlusPlus` to be installed in your system and in general added to your `PATH`. 
Optionally, the following arguments can be passed to the script:
1) `<num_fuzzers>` the number of fuzzers to launch (by default 1).
2) `<build_mode>` either `Release` or `Debug`.

`sh start_lib_fuzzer.sh`
Builds `realm-core` and `object-store` in `Debug` mode using the clang compiler and starts `realm-libfuzz`.
Optionally, the following arguments can be passed to the script:
1) `<build_mode>` either `Release` or `Debug`. 
2) `<corpus>` essentially  initial set of inputs for improving fuzzer efficiency.

## See Also

[AFL++ github](https://github.com/AFLplusplus/AFLplusplus)
[LibFuzzer](https://github.com/google/fuzzing/blob/master/tutorial/libFuzzerTutorial.md)
[Google Fuzz Test](https://github.com/google/fuzztest)
