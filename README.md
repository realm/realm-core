TightDB
=======

Dependencies
------------

### Ubuntu 12.04

    sudo apt-get install build-essential
    # For testing:
    sudo apt-get install libunittest++-dev
    sudo apt-get install libproc-dev

### OS X 10.8

    Install Xcode
    Install command line tools (via Xcode)


Building and installing
-----------------------

    sh build.sh clean
    sh build.sh
    sudo sh build.sh install


Building a distribution package
-------------------------------

    sh build.sh dist
