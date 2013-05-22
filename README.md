TightDB
=======

Dependencies
------------

### Ubuntu 10.04, 12.04

    sudo apt-get install build-essential
    #   For regenerating <tightdb/table-macros.hpp>
    sudo apt-get install python-cheetah
    #   For testing:
    sudo apt-get install libunittest++-dev
    #   For benchmarking:
    sudo apt-get install libproc-dev

### Fedora 17

    sudo yum install gcc gcc-c++
    #   For regenerating <tightdb/table-macros.hpp>
    sudo yum install python-cheetah
    #  For benchmarking:
    sudo yum install procps-devel

### OS X 10.8

    Install Xcode
    Install command line tools (via Xcode)

Note: The TightDB source code comes bundled with a fallback version of
UnitTest++ which will be used when testing, if the 'pkg-config'
program does not exists, or if 'pkg-config unittest++ --exists' does
not succeed.


Building, testing, and installing
---------------------------------

    sh build.sh clean
    sh build.sh build
    sh build.sh test
    sudo sh build.sh install
    sh build.sh test-intalled

Headers are installed in:

    /usr/local/include/tightdb/

Except for `tightdb.hpp` which is installed as:

    /usr/local/include/tightdb.hpp

The following libraries are installed:

    /usr/local/lib/libtightdb.so
    /usr/local/lib/libtightdb-dbg.so
    /usr/local/lib/libtightdb.a

Note: '.so' is replaced by '.dylib' on OS X.

The following programs are installed:

    /usr/local/bin/tightdb-config
    /usr/local/bin/tightdb-config-dbg

These programs provide the necessary compiler flags for an application
that needs to link against TightDB. They work with GCC and other
compilers, such as Clang, that are mostly command line compatible with
GCC. Here is an example:

    g++  my_app.cpp  `tightdb-config --cflags --libs`


Building a distribution package
-------------------------------

In general, it is necessary (and crucial) to properly update the
versions of the following shared libraries (do this by editing the the
indicated Makefiles):

    libtightdb.so      (/tightdb/src/tightdb/Makefile)
    libtightdb-c.so    (/tightdb_c/src/tightdb/c/Makefile)
    libtightdb-objc.so (/tightdb_objc/src/tightdb/objc/Makefile)

Please note that these versions are completely independent of each
other and of the package version. When the library versions are set
correctly, do one of the following:

    sh build.sh src-dist all   # Source distribution
    sh build.sh bin-dist all   # Prebuilt core library

If everything went well, consider tagging and then rebuilding the
package:

    git tag -a 'bNNN' -m "New tag for 'Build NNN'"
    git push --tags

This will produce a package whose name and whose top-level directory
is named according to the tag.


Configuration
-------------

To use a nondefault compiler, or a compiler in a nondefault location,
set the environment variable `CC` before calling `sh build.sh build`
or `sh build.sh bin-dist`, as in the following example:

    CC=clang sh build.sh bin-dist all

There are also a number of environment variables that serve to enable
or disable special features during building:

Set `TIGHTDB_ENABLE_REPLICATION` to a nonempty value to enable
replication. For example:

    TIGHTDB_ENABLE_REPLICATION=1 sh build.sh src-dist all
