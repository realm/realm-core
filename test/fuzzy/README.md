# How to Run the Fuzz Tests

## Prerequisites

You should install the latest version of the American Fuzzy Lop (AFL) tool.
You can sometimes find this tool from your native package manager:

- On Mac OS X: brew install afl-fuzz
- On Ubuntu: apt-get install afl-clang

If it is not available natively or you want a more recent version,
check http://lcamtuf.coredump.cx/afl/ for release notes and install instructions.

## The Simple Way

`sh simple_start.sh`

Builds `realm-core` and the fuzz target with `afl-clang++` and starts N instances of `afl-fuzz`, where N is the number of logical CPU processors.

`sh simple_stop.sh`

Kills the running `afl-fuzz` instances and converts any found crashes and / or hangs into `.cpp` unit tests, stored in `findings/cpp_unit_tests/`

Turn these into regular unit tests (most easily done in `test_lang_bind_helper.cpp`) to debug further.

## See Also

[AFL's README](http://lcamtuf.coredump.cx/afl/README.txt)
