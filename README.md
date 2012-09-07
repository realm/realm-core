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
    sh build.sh build
    sudo sh build.sh install


Building a distribution package
-------------------------------

### Consider tagging before making the package:

    git tag -a 'bNNN' -m "New tag for 'Build NNN'"
    git push --tags

### Then:

    sh build.sh dist
