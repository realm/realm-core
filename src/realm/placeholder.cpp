// This file is used to work around a limitation in Xcode that results in it failing
// to produce the output file of a library target unless the target itself contains at
// least one source file. This situation is encountered when building libraries whose
// contents are provided by a CMake object library target, as in this situation the
// input files are specified as part of the linker flags for the Xcode target rather
// than via the usual target membership approach.

extern int unused;
