# How to Run the Fuzz Tests

## Prerequisites

You should install the latest version of the American Fuzzy Lop ++ (AFL++) tool.
Compile it following this quick guide: https://aflplus.plus/building/ it requires llvm >= 9.0.

## Launching afl++

`sh start_fuzz_afl.sh`

Builds `realm-core` and `object-store` in `Release` mode using the afl++ compiler `afl-cc` and starts 1 instance of `afl-fuzz`.
It expects `AFLPlusPlus` to be installed in your system and in general added to your `PATH`. Optionally the following arguments can be passed to the script:
1) `<num_fuzzers>` the number of fuzzers to launch (by default 1).
2) `<build_mode>` either `Release` or `Debug`.

## Stopping afl++
`sh simple_stop.sh`

Kills the running `afl-fuzz` instances and converts any found crashes and / or hangs into `.cpp` unit tests, stored in `findings/cpp_unit_tests/`

Turn these into regular unit tests (most easily done in `test_lang_bind_helper.cpp`) to debug further.

## See Also

[AFL++ github](https://github.com/AFLplusplus/AFLplusplus)
