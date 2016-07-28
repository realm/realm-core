# How to Run the Fuzz Tests

## The Simple Way

`sh simple_start.sh`

Builds `realm-core` and the fuzz target with `afl-g++` on Linux (or `afl-clang++` on OS X) and starts N instances of `afl-fuzz`, where N is the number of logical CPU processors.

`sh simple_stop.sh`

Kills the running `afl-fuzz` instances and converts any found crashes and / or hangs into `.cpp` unit tests, stored in `findings/cpp_unit_tests/`

Turn these into regular unit tests (most easily done in `test_lang_bind_helper.cpp`) to debug further.

## See Also

[AFL's README](http://lcamtuf.coredump.cx/afl/README.txt)
