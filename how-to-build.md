This file explains how to build and install the Realm core library.

_NOTE_: This is going to change very soon as a new build system based
on CMake will be introduced.

Prerequisites
-------------

To build the Realm core library, you need the standard set of build
tools. This includes a C/C++ compiler and GNU make. Realm is
thoroughly tested with both GCC and Clang. It is known to work with
GCC 4.2 and newer, as well as with Clang 3.0 and newer.

To run the benchmarking suite (make benchmark) on Linux, you will need
the development part of the 'procps' library.

The following is a suggestion of how to install the prerequisites on
each of our major platforms:

### Ubuntu 10.04 and 12.04

    sudo apt-get install build-essential
    sudo apt-get install libproc-dev
    sudo apt-get install libssl-dev

### Linux Mint 15, 16, Ubuntu 13.04, 13.10

    sudo apt-get install build-essential
    sudo apt-get install libprocps0-dev
    sudo apt-get install libssl-dev

### Linux Mint 17, 17.1, Ubuntu 14.04

    sudo apt-get install build-essential
    sudo apt-get install libprocps3-dev
    sudo apt-get install libssl-dev

### Fedora 17, 18, 19, 20, Amazon Linux 2012.09

    sudo yum install gcc gcc-c++
    sudo yum install procps-devel

### OS X 10.10 and 10.11

On OS X, Clang is used as the C/C++ compiler by default. Clang is installed
as part of Xcode. Xcode 7.0 or newer is required, and can be installed via
the Mac App Store.

Configure, build & test
-------------------------

Run the following commands to configure, build and test core:

    sh build.sh config
    sh build.sh build
    sh build.sh test


Building for Android
--------------------

Building for Android required the NDK of version >= r10d installed.

It can be built using the following command:

    sh build.sh build-android

Building for iOS, watchOS and tvOS
----------------------------------

Realm core may be built for iOS, watchOS and tvOS from an OS X machine.
See the OS X section of [Prerequisites](#prerequisites) above for information
about the version of Xcode that is required.

To build for iOS:

    sh build.sh build-ios

This produces the following files and directories:

    ios-lib/include/
    ios-lib/librealm-ios.a
    ios-lib/librealm-ios-dbg.a
    ios-lib/realm-config
    ios-lib/realm-config-dbg

The `include` directory holds a copy of the header files, which are
identical to the ones installed by `sh build.sh install`. There are
two versions of the static library, one that is compiled with
optimization, and one that is compiled for debugging. Each one
contains code compiled for both iOS devices and the iOS simulator.
Each one also comes with a `config` program that can be used to
enquire about required compiler and linker flags.

To build for watchOS:

    sh build.sh build-watchos

The output is placed in watchos-lib, and is structured the same as
is described for iOS above.

To build for tvOS:

    sh build.sh build-tvos

The output is placed in tvos-lib, and is structured the same as
is described for iOS above.

Configuration
-------------

It is possible to install into a non-default location by running the
following command before building and installing:

    sh build.sh config [PREFIX]

Here, `PREFIX` is the installation prefix. If it is not specified, it
defaults to `/usr/local`.

Normally the Realm version is taken to be what is returned by `git
describe`. To override this, set `REALM_VERSION` as in the following
examples:

    REALM_VERSION=0.1.4 sh build.sh config
    REALM_VERSION=0.1.4 sh build.sh bin-dist all

To use a nondefault compiler, or a compiler in a nondefault location,
set the environment variable `CC` before calling `sh build.sh build`
or `sh build.sh bin-dist`, as in the following example:

    CC=clang sh build.sh bin-dist all

Testing
-------

The core library comes with a suite of unit tests. You can run it in one of the
following ways:

    sh build.sh check
    sh build.sh check-debug
    sh build.sh memcheck
    sh build.sh memcheck-debug

The `mem` versions will run the suite inside Valgrind.

There are a number of environment variable that can be use the customize the
execution. For example, here is how to run only the `Foo` test and those whose
names start with `Bar`, then how run all tests whose names start with `Foo`,
except `Foo2` and those whose names end with an `X`:

    UNITTEST_FILTER="Foo Bar*" sh build.sh check-debug
    UNITTEST_FILTER="Foo* - Foo2 *X" sh build.sh check-debug

These are the available variables:

 - `UNITTEST_FILTER` can be used to exclude one or more tests from a particular
   run. For more information about the syntax, see the documentation of
   `realm::test_util::unit_test::create_wildcard_filter()` in
   `test/util/unit_test.hpp`.

 - Set `UNITTEST_PROGRESS` to a non-empty value to enable reporting of progress
   (write the name of each test as it is executed).

 - If you set `UNITTEST_SHUFFLE` to a non-empty value, the tests will be
   executed in a random order. This requires, of course, that all executed tests
   are independant of each other. Note that unless you also set
   `UNITTEST_RANDOM_SEED=random`, you will get the same random order in each
   sucessive run.

 - You may set `UNITTEST_RANDOM_SEED` to `random` or to some unsigned integer
   (at least 32 bits will be accepted). If you specify `random`, the global
   pseudorandom number generator will be seeded with a nondeterministic value
   (one that generally will be different in each sucessive run). If you specify
   an integer, it will be seeded with that integer.

 - Set `UNITTEST_REPEAT` to the number of times you want to execute the tests
   selected by the filter. It defaults to 1.

 - Set `UNITTEST_THREADS` to the number of test threads to use. The default
   is 1. Using more than one thread requires that all executed tests are
   thread-safe and independant of each other.

 - Set `UNITTEST_KEEP_FILES` to a non-empty value to disable automatic removal
   of test files.

 - Set `UNITTEST_XML` to a non-empty value to dump the test results to a JUnit
   XML file. For details, see
   `realm::test_util::unit_test::create_junit_reporter()` in
   `test/util/unit_test.hpp`.

 - Set `UNITTEST_LOG_LEVEL` to adjust the log level threshold for custom intra
   test logging. Valid values are `all`, `trace`, `debug`, `info`, `warn`,
   `error`, `fatal`, `off`. The default threshold is `off` meaning that nothing
   is logged.

 - Set `UNITTEST_LOG_TO_FILES` to a non-empty value to redirect log messages
   (including progress messages) to log files. One log file will be created per
   test thread (`UNITTEST_THREADS`). The files will be named
   `test_logs_%1/thread_%2_.log` where `%1` is a timestamp and `%2` is the test
   thread number.

 - Set `UNITTEST_ABORT_ON_FAILURE` to a non-empty value to termination of the
   testing process as soon as a check fails or an unexpected exception is thrown
   in a test.

Memory debugging:

Realm currently allows for uninitialized data to be written to a database
file. This is not an error (technically), but it does cause Valgrind to report
errors. To avoid these 'false positives' during testing and debugging, set
`REALM_ENABLE_ALLOC_SET_ZERO` to a nonempty value during configuration as in the
following example:

    REALM_ENABLE_ALLOC_SET_ZERO=1 sh build.sh config

Measuring test coverage:

You can measure how much of the code is tested by executing:

    sh build.sh lcov

It will generate a html page under cover_html directory. Be aware that the lcov
tool will not generate correct results if you build from a directory path that contains
symlinked elements.

Packaging for OS X
-------------------

You can create a framework for Mac OS X after you have built the
core library (the `build` target). The framework is useful when
creating OS X application. The command is:

    sh build.sh build-osx-framework


Install
-------

You can install core itself on linux if needed, but be aware that the API exposed
is not stable or supported!

    sudo sh build.sh install

Headers will be installed in:

    /usr/local/include/realm/

Except for `realm.hpp` which is installed as:

    /usr/local/include/realm.hpp

The following libraries will be installed:

    /usr/local/lib/librealm.so
    /usr/local/lib/librealm-dbg.so
    /usr/local/lib/librealm.a

Note: '.so' is replaced by '.dylib' on OS X.

The following programs will be installed:

    /usr/local/bin/realm-import
    /usr/local/bin/realm-import-dbg
    /usr/local/bin/realm-config
    /usr/local/bin/realm-config-dbg
    /usr/local/libexec/realmd
    /usr/local/libexec/realmd-dbg

The `realm-import` tool lets you load files containing
comma-separated values into Realm. The next two are used
transparently by the Realm library when `async` transactions are
enabled. The two `config` programs provide the necessary compiler
flags for an application that needs to link against Realm. They work
with GCC and other compilers, such as Clang, that are mostly command
line compatible with GCC. Here is an example:

    g++  my_app.cpp  `realm-config --cflags --libs`

Here is a more comple set of build-related commands:

    sh build.sh config
    sh build.sh clean
    sh build.sh build
    sh build.sh show-install
    sudo sh build.sh install
    sh build.sh test-intalled
    sudo sh build.sh uninstall
