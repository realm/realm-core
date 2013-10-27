TightDB
=======

This README file explains how to build and install the TightDB core
library.


Prerequisites
-------------

To build the TightDB core library, you need the standard set of build
tools. This includes a C/C++ compiler and GNU make. TightDB is
thoroughly tested with both GCC and Clang. It is known to work with
GCC 4.2 and newer, as well as with Clang 3.0 and newer.

If you are going to modify the TightDB core library, you will need
Cheetah for Python (http://www.cheetahtemplate.org). It is needed
because some source files are generated from Cheetah templates.

To run the test suite, you will need "UnitTest++"
(http://unittest-cpp.sourceforge.net), however, a bundled fallback
version will be used if `pkg-config unittest++ --exists` fails.

Finally, to run the benchmarking suite (make benchmark) on Linux, you
will need the development part of the 'procps' library.

The following is a suggestion of how to install the prerequisites on
each of our major platforms:

### Ubuntu 10.04 and 12.04

    sudo apt-get install build-essential
    sudo apt-get install python-cheetah
    sudo apt-get install libunittest++-dev
    sudo apt-get install libproc-dev

### Ubuntu 13.04, Linux Mint 15

    sudo apt-get install build-essential
    sudo apt-get install python-cheetah
    sudo apt-get install libunittest++-dev
    sudo apt-get install libprocps0-dev

### Fedora 17, 18, 19, Amazon Linux 2012.09

    sudo yum install gcc gcc-c++
    sudo yum install python-cheetah
    sudo yum install procps-devel

### Mac OS X 10.7 and 10.8

On Mac OS X, the build procedure uses Clang as the C/C++
compiler. Clang comes with Xcode, so install Xcode if it is not
already installed. If you have a version that preceeds 4.2, we
recommend that you upgrade. This will ensure that the Clang version is
at least 3.0. Run the following command in the command prompt to see
if you have Xcode installed, and, if so, what version it is:

    xcodebuild -version

Make sure you also install "Command line tools" found under the
preferences pane "Downloads" in Xcode.

Download the latest version of Python Cheetah
(https://pypi.python.org/packages/source/C/Cheetah/Cheetah-2.4.4.tar.gz),
then:

    tar xf Cheetah-2.4.4.tar.gz
    cd Cheetah-2.4.4/
    sudo python setup.py install



Building, testing, and installing
---------------------------------

    sh build.sh config
    sh build.sh clean
    sh build.sh build
    sh build.sh test
    sh build.sh test-debug
    sudo sh build.sh install
    sh build.sh test-installed

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

    /usr/local/bin/tightdb-import
    /usr/local/bin/tightdbd
    /usr/local/bin/tightdbd-dbg
    /usr/local/bin/tightdb-config
    /usr/local/bin/tightdb-config-dbg

The `tightdb-import` tool lets you load files containing
comma-separated values into TightDB. The next two are used
transparently by the TightDB library when `async` transactions are
enabled. The two `config` programs provide the necessary compiler
flags for an application that needs to link against TightDB. They work
with GCC and other compilers, such as Clang, that are mostly command
line compatible with GCC. Here is an example:

    g++  my_app.cpp  `tightdb-config --cflags --libs`

After building, you might want to see exactly what will be installed,
without actually installing anything. This can be done as follows:

    DESTDIR=/tmp/check sh build.sh install && find /tmp/check -type f



Building for iPhone
-------------------

On Mac OS X it is possible to build a version of the TightDB core
library for iOS (the iPhone OS). It requires that the iPhoneOS and
iPhoneSimulator SDKs for Xcode are installed.

Run the following command to build the TightDB core library for
iPhone:

    sh build.sh build-iphone

This produces the following files and directories:

    iphone-lib/include/
    iphone-lib/libtightdb-ios.a
    iphone-lib/libtightdb-ios-dbg.a
    iphone-lib/tightdb-config
    iphone-lib/tightdb-config-dbg

The `include` directory holds a copy of the header files, which are
identical to the ones installed by `sh build.sh install`. There are
two versions of the static library, one that is compiled with
optimization, and one that is compiled for debugging. Each one
contains code compiled for both iPhone and for the iPhone
simulator. Each one also comes with a `config` program that can be
used to enquire about required compiler and linker flags.



Configuration
-------------

It is possible to install into a non-default location by running the
following command before building and installing:

    sh build.sh config [PREFIX]

Here, `PREFIX` is the installation prefix. If it is not specified, it
defaults to `/usr/local`.

Normally the TightDB version is taken to be what is returned by `git
describe`. To override this, set `TIGHTDB_VERSION` as in the following
examples:

    TIGHTDB_VERSION=x.y.z sh build.sh config
    TIGHTDB_VERSION=x.y.z sh build.sh bin-dist all

To use a nondefault compiler, or a compiler in a nondefault location,
set the environment variable `CC` before calling `sh build.sh build`
or `sh build.sh bin-dist`, as in the following example:

    CC=clang sh build.sh bin-dist all

There are also a number of environment variables that serve to enable
or disable special features during building:

Set `TIGHTDB_ENABLE_REPLICATION` to a nonempty value to enable
replication. For example:

    TIGHTDB_ENABLE_REPLICATION=1 sh build.sh src-dist all



Packaging
---------

It is possible to create Debian/Ubuntu packages (`.deb`) by running the
following command:

    dpkg-buildpackage -rfakeroot

The packages will be signed by the maintainer's signature. It is also
possible to create packages without signature:

    dpkg-buildpackage -rfakeroot -us -uc



Building a distribution package
-------------------------------

In general, it is necessary (and crucial) to properly update the
versions of the following shared libraries:

    libtightdb.so      (/tightdb/src/tightdb/Makefile)
    libtightdb-c.so    (/tightdb_c/src/tightdb/c/Makefile)
    libtightdb-objc.so (/tightdb_objc/src/tightdb/objc/Makefile)

Do this by editing the the indicated Makefiles.

Please note that these versions are completely independent of each
other and of the package version. When the library versions are set
correctly, do one of the following:

    sh build.sh src-dist all   # Source distribution
    sh build.sh bin-dist all   # Prebuilt core library

If everything went well, consider tagging and then rebuilding the
package:

    git tag -a 'bNNN' -m "New tag for 'Build NNN'"
    git push --tags

This will produce a package whose name, and whose top-level directory
is named according to the tag.

During the building of a distribution package, some Markdown documents
are converted to PDF format, and this is done using the Pandoc
utility. See below for instructions on how to install Pandoc. On some
platforms, however, Pandoc installation is unfeasible (e.g. Amazon
Linux). In those cases you may set `TIGHTDB_DISABLE_MARKDOWN_TO_PDF`
to a nonempty value to disable the conversion to PDF.

### Ubuntu 10.04, 12.04, and 13.04

    sudo apt-get install texlive-latex-base texlive-latex-extra pandoc

### Fedora 17

    sudo yum install pandoc-markdown2pdf

### Fedora 18, 19

    sudo yum install pandoc-pdf texlive

## Mac OSX

Install Pandoc and XeLaTeX (aka MacTeX) by following the instructions
on http://johnmacfarlane.net/pandoc/installing.html. This boils down
to installing the following two packages:

 - http://pandoc.googlecode.com/files/pandoc-1.12.0.2.dmg
 - http://mirror.ctan.org/systems/mac/mactex/mactex-basic.pkg

When done, you need to restart the terminal session.
