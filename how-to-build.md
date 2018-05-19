This file explains how to build and install the Realm core library.

## Prerequisites

To build the Realm core library, you need CMake 3.4 or newer and a
standard set of build tools. This includes a C/C++ compiler and a
build system like GNU make. Realm is thoroughly tested with both GCC
and Clang. It is known to work with GCC 4.9 and newer, as well as with
Clang 3.9 (Apple Clang 8.0) and newer. Your compiler must support C++14.

To run the benchmarking suite (make benchmark) on Linux, you will need
the development part of the 'procps' library.

The following is a suggestion of how to install the prerequisites on
each of our major platforms:

### Linux Mint 18-18.2, Ubuntu 16.04

    sudo apt-get install build-essential
    sudo apt-get install libprocps4-dev
    sudo apt-get install libssl-dev
    sudo apt-get install cmake

### Linux Mint 17-17.3, Ubuntu 14.04

    sudo apt-get install build-essential
    sudo apt-get install libprocps3-dev
    sudo apt-get install libssl-dev
    sudo apt-get install cmake3

### Fedora 24, 25, 26

    sudo dnf install gcc-c++
    sudo dnf install procps-devel
    sudo dnf install openssl-devel
    sudo dnf install cmake

### OS X 10.10, 10.11, 10.12

On OS X, Clang is used as the C/C++ compiler by default. Clang is installed
as part of Xcode. Xcode 7.0 or newer is required, and can be installed via
the Mac App Store.

Setting up a development environment can convienently be achieved using a
package manager called brew. See https://brew.sh for install instructions.

    brew install cmake

### Windows

On Windows, navigate to the following websites in your browser
to download the appropriate installers.

- Visual Studio 2017: https://www.visualstudio.com/
- CMake: https://cmake.org/download/

## Configure, build & test

To get submodule dependencies:

    git submodule update --init --recursive

Run the following commands to configure, build and test core:

    mkdir build.<buildtype>
    cd build.<buildtype>
    cmake -D CMAKE_BUILD_TYPE=<buildtype> ..
    cmake --build .
    ctest

Where `buildtype` is either `debug` or `delease`
    
## Building for Android, iOS, watchOS and tvOS

Building for Android required the NDK r10e installed and ANDROID_NDK set
to the directory where it's installed.

These targets can be built using the cross_compile.sh command.
'Release' can be replaced with 'Debug' to produce a debug build. :

    tools/cross_compile.sh -o android -a armeabi-v7a -t Release -v <X.Y.Z>

The command shows the available options simply with:

    tools/cross_compile.sh

These commands produce a tarball containing the realm static library
and its include files. The string after '-v' just denotes the version part
of the name of the tarball produced - it's optional.

## Testing

The core library comes with a suite of unit tests. You can run the unit tests like this:

    cd build-dir/test
    ./realm-tests

or run both unit tests and performance tests with just:

    ctest

There are a number of environment variable that can be use the customize the
execution. For example, here is how to run only the `Foo` test and those whose
names start with `Bar`, then how run all tests whose names start with `Foo`,
except `Foo2` and those whose names end with an `X`:

    UNITTEST_FILTER="Foo Bar*" ./realm-tests
    UNITTEST_FILTER="Foo* - Foo2 *X" ./realm-tests

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

### Memory debugging:

Realm currently allows for uninitialized data to be written to a database
file. This is not an error (technically), but it does cause Valgrind to report
errors. To avoid these 'false positives' during testing and debugging, set
`REALM_ENABLE_ALLOC_SET_ZERO` to a nonempty value during configuration as in the
following example:

    cmake -D REALM_ENABLE_ALLOC_SET_ZERO=ON -D CMAKE_BUILD_TYPE=Debug ..

### Measuring test coverage:

You can measure how much of the code is tested by adding the `-D REALM_COVERAGE=ON` option to the cmake call that generates the project.
This will allow to produce coverage information which is then digestable by gcovr or lcov:

    cd test
    ./realm-tests
    gcovr --filter='.*src/realm.*'

Alternatively you can run the script `tools/coverage.sh`.

## Install

You can install core itself on Linux if needed, but be aware that the API exposed
is not stable or supported!

    sudo cmake --build --target install

Headers will be installed in:

    /usr/local/include/realm/

Except for `realm.hpp` which is installed as:

    /usr/local/include/realm.hpp

The following libraries will be installed:

    /usr/local/lib/librealm.so
    /usr/local/lib/librealm.a

_Note: '.so' is replaced by '.dylib' on OS X._

The following programs will be installed:

    /usr/local/bin/realm-import
    /usr/local/bin/realm-config
    /usr/local/libexec/realmd

### Configuration

It is possible to install into a non-default location by running the
following command before building and installing:

    cmake -D CMAKE_INSTALL_PREFIX=/your/dir ..

Here, `CMAKE_INSTALL_PREFIX` is the installation prefix. If it is not specified, it
defaults to `/usr/local` on Linux and macOS.

CMake can automatically detect your compiler and its location but it allows
all kinds of customizations. For a brief overview you can reference to this
CMake [wiki page](http://www.vtk.org/Wiki/CMake_Useful_Variables#Compilers_and_Tools)

## Other tools

The `realm-import` tool lets you load files containing
comma-separated values into Realm.

The next two are used transparently by the Realm library when `async` transactions are
enabled. The two `config` programs provide the necessary compiler
flags for an application that needs to link against Realm. They work
with GCC and other compilers, such as Clang, that are mostly command
line compatible with GCC. Here is an example:

    g++  my_app.cpp  `realm-config --cflags --libs`

## cmake options

The CMake build system supports a big variety of features and targets
that go beyond the purpose of this document. It supports the creation of projects
in several formats:

 - make
 - ninja
 - Xcode
 - Visual Studio

In CMake jargon these are called generators are are described
[here](https://cmake.org/cmake/help/v3.7/manual/cmake-generators.7.html)
