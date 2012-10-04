TightDB
=======

Dependencies
------------

### Ubuntu 12.04

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


Building and installing
-----------------------

    sh build.sh clean
    sh build.sh build
    sudo sh build.sh install


Building a distribution package
-------------------------------

    sh build.sh dist

If everything went well, consider tagging and then making the package again:

    git tag -a 'bNNN' -m "New tag for 'Build NNN'"
    git push --tags

This will produce a package whose name and whose top-level directory
is named according to the tag.
