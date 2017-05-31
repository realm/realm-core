# Sha-2

Public domain SHA-224/256/384/512 processors in C++ without external
dependencies. Adapted from
[LibTomCrypt](http://libtom.org/?page=features&whatfile=crypt).

## Usage

The processors are contained in the files sha{224,256,384,512}.cpp. Compile and
link the files needed.

* SHA-224: sha224.cpp and sha256.cpp
* SHA-256: sha256.cpp
* SHA-384: sha384.cpp and sha512.cpp
* SHA-512: sha512.cpp

The example program in sum.cpp is a crude emulation of sha{224,256,384,512}sum
from GNU Core Utilities.

## License

Sha-2 is public domain.
