<<<<<<< HEAD
ifeq ($(CXX),g++)
CC := gcc
else ifneq ($(filter g++-%,$(CXX)),)
CC := $(patsubst g++-%,gcc-%,$(CXX))
else ifeq ($(CXX),clang)
CC := $(CXX)
else ifneq ($(filter clang-%,$(CXX)),)
CC := $(CXX)
endif

ifeq ($(CC),gcc)
CXX := g++
else ifneq ($(filter gcc-%,$(CC)),)
CXX := $(patsubst gcc-%,g++-%,$(CC))
else ifeq ($(CC),clang)
CXX := $(CC)
else ifneq ($(filter clang-%,$(CC)),)
CXX := $(CC)
endif

# Linker - use the C++ compiler by default
LD = $(CXX)

ARFLAGS = csr


ifneq ($(filter gcc%,$(CC)),)
ifneq ($(filter g++%,$(CXX)),)

# These compiler flags are those that are common to all build modes
# (STATIC, SHARED, DEBUG, and COVERAGE). Note: '-ansi' implies C++03
# for modern versions of GCC.
CFLAGS          = -ansi -pedantic -Wall -Wextra -Wno-long-long
CXXFLAGS        = $(CFLAGS)

# These compiler flags are those that are special to each build mode.
CFLAGS_OPTIMIZE = -O3 -msse4.2 -DUSE_SSE42
# FIXME: '-fno-elide-constructors' currently causes failure in TightDB
#CFLAGS_DEBUG    = -ggdb3 -fno-elide-constructors -DTIGHTDB_DEBUG -DMAX_LIST_SIZE=4
CFLAGS_DEBUG    = -ggdb3 -DTIGHTDB_DEBUG -DUSE_SSE42 -msse4.2 -DMAX_LIST_SIZE=4
CFLAGS_COVERAGE = --coverage -msse4.2 -DUSE_SSE42 -DTIGHTDB_DEBUG -DMAX_LIST_SIZE=4

# Extra compiler flags used for both C and C++ when building a shared library.
CFLAGS_SHARED   = -fPIC -DPIC

# Extra compiler and linker flags used to enable support for PTHREADS.
CFLAGS_PTHREAD  = -pthread
LDFLAGS_PTHREAD = $(CFLAGS_PTHREAD)

endif
endif

ifneq ($(filter clang%,$(CC)),)
ifneq ($(filter clang%,$(CXX)),)

# These compiler flags are those that are common to all build modes
# (STATIC, SHARED, DEBUG, and COVERAGE).
CFLAGS          = -Weverything -Wno-long-long -Wno-sign-conversion -Wno-cast-align -Wno-shadow -Wno-unreachable-code -Wno-overloaded-virtual -Wno-unused-macros -Wno-conditional-uninitialized -Wno-global-constructors -Wno-missing-prototypes -Wno-shorten-64-to-32 -Wno-padded -Wno-exit-time-destructors -Wno-weak-vtables -Wno-unused-member-function
CXXFLAGS        = $(CFLAGS) -std=c++03
LDFLAGS         = -lstdc++

# These compiler flags are those that are special to each build mode.
CFLAGS_OPTIMIZE = -O3 -msse4.2 -DUSE_SSE42
# Note: '-fno-elide-constructors' currently causes failure in TightDB
#CFLAGS_DEBUG    = -ggdb3 -fno-elide-constructors -DTIGHTDB_DEBUG -DMAX_LIST_SIZE=4
CFLAGS_DEBUG    = -ggdb3 -DTIGHTDB_DEBUG -DMAX_LIST_SIZE=4
CFLAGS_COVERAGE = --coverage -msse4.2 -DUSE_SSE42 -DTIGHTDB_DEBUG -DMAX_LIST_SIZE=4
=======
SOURCE_ROOT = src
>>>>>>> d62517b4304f7c52f14900802134d37cfce00934

ifneq ($(CC_AND_CXX_ARE_GCC_LIKE),)
CFLAGS_DEFAULT   += -Wextra -ansi -pedantic -Wno-long-long -msse4.2
# FIXME: '-fno-elide-constructors' currently causes TightDB to fail
#CFLAGS_DEBUG     += -fno-elide-constructors
CFLAGS_PTHREAD   += -pthread
endif

CFLAGS_DEFAULT   += -DUSE_SSE42
CFLAGS_DEBUG     += -DTIGHTDB_DEBUG -DMAX_LIST_SIZE=4
CFLAGS_COVERAGE  += -DTIGHTDB_DEBUG -DMAX_LIST_SIZE=4
