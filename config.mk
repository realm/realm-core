ifeq ($(CXX), g++)
CC = gcc
else ifeq ($(CXX), clang)
CC = clang
endif

ifeq ($(CC), gcc)
CXX = g++
else ifeq ($(CC), clang)
CXX = clang
endif

# Linker - use the C++ compiler by default
LD = $(CXX)

ARFLAGS = csr


ifeq ($(CC), gcc)
ifeq ($(CXX), g++)

# These compiler flags are those that are common to all build modes
# (STATIC, SHARED, DEBUG, and COVERAGE). Note: '-ansi' implies C++03
# for modern versions of GCC.
CFLAGS          = -ansi -pedantic -Wall -Wextra -Wno-long-long
CXXFLAGS        = $(CFLAGS)

# These compiler flags are those that are special to each build mode.
CFLAGS_OPTIMIZE = -ggdb3 -O3 -msse4.2 -DUSE_SSE -DNDEBUG
# Note: '-fno-elide-constructors' currently causes failure in TightDB
#CFLAGS_DEBUG    = -ggdb3 -fno-elide-constructors -D_DEBUG -DMAX_LIST_SIZE=4
CFLAGS_DEBUG    = -ggdb3 -D_DEBUG -DMAX_LIST_SIZE=4
CFLAGS_COVERAGE = --coverage -msse4.2 -DUSE_SSE -D_DEBUG -DMAX_LIST_SIZE=4

# Extra compiler flags used for both C and C++ when building a shared library.
CFLAGS_SHARED   = -fPIC -DPIC

# Extra compiler and linker flags used to enable support for PTHREADS.
CFLAGS_PTHREAD  = -pthread
LDFLAGS_PTHREAD = $(CFLAGS_PTHREAD)

CC_STATIC       = $(CC) $(CFLAGS_OPTIMIZE) $(CFLAGS_PTHREAD)
CC_SHARED       = $(CC) $(CFLAGS_SHARED) $(CFLAGS_OPTIMIZE) $(CFLAGS_PTHREAD)
CC_DEBUG        = $(CC) $(CFLAGS_DEBUG) $(CFLAGS_PTHREAD)
CC_COVERAGE     = $(CC) $(CFLAGS_COVERAGE) $(CFLAGS_PTHREAD)

CXX_STATIC      = $(CXX) $(CFLAGS_OPTIMIZE) $(CFLAGS_PTHREAD)
CXX_SHARED      = $(CXX) $(CFLAGS_SHARED) $(CFLAGS_OPTIMIZE) $(CFLAGS_PTHREAD)
CXX_DEBUG       = $(CXX) $(CFLAGS_DEBUG) $(CFLAGS_PTHREAD)
CXX_COVERAGE    = $(CXX) $(CFLAGS_COVERAGE) $(CFLAGS_PTHREAD)

LD_STATIC       = $(LD) $(LDFLAGS_PTHREAD)
LD_SHARED       = $(LD) -shared $(CFLAGS_SHARED) $(CFLAGS_OPTIMIZE) $(LDFLAGS_PTHREAD)
LD_DEBUG        = $(LD) $(LDFLAGS_PTHREAD)
LD_COVERAGE     = $(LD) --coverage $(LDFLAGS_PTHREAD)

endif
endif

ifeq ($(CC), clang)
ifeq ($(CXX), clang)

# These compiler flags are those that are common to all build modes
# (STATIC, SHARED, DEBUG, and COVERAGE). Note: '-ansi' implies C++03
# for modern versions of GCC.
CFLAGS          = -Weverything -Wno-long-long -Wno-sign-conversion -Wno-cast-align -Wno-shadow -Wno-unreachable-code -Wno-overloaded-virtual -Wno-unused-macros -Wno-conditional-uninitialized -Wno-global-constructors -Wno-missing-prototypes -Wno-shorten-64-to-32 -Wno-padded -Wno-exit-time-destructors -Wno-weak-vtables -Wno-unused-member-function
CXXFLAGS        = $(CFLAGS) -std=c++03
LDFLAGS         = -lstdc++

# These compiler flags are those that are special to each build mode.
CFLAGS_OPTIMIZE = -O3 -msse4.2 -DUSE_SSE -DNDEBUG
# Note: '-fno-elide-constructors' currently causes failure in TightDB
#CFLAGS_DEBUG    = -ggdb3 -fno-elide-constructors -D_DEBUG -DMAX_LIST_SIZE=4
CFLAGS_DEBUG    = -ggdb3 -D_DEBUG -DMAX_LIST_SIZE=4
CFLAGS_COVERAGE = --coverage -msse4.2 -DUSE_SSE -D_DEBUG -DMAX_LIST_SIZE=4

# Extra compiler flags used for both C and C++ when building a shared library.
CFLAGS_SHARED   = -fPIC -DPIC

# Extra compiler and linker flags used to enable support for PTHREADS.
CFLAGS_PTHREAD  = -pthread
LDFLAGS_PTHREAD = $(CFLAGS_PTHREAD)

CC_STATIC       = $(CC) $(CFLAGS_OPTIMIZE) $(CFLAGS_PTHREAD)
CC_SHARED       = $(CC) $(CFLAGS_SHARED) $(CFLAGS_OPTIMIZE) $(CFLAGS_PTHREAD)
CC_DEBUG        = $(CC) $(CFLAGS_DEBUG) $(CFLAGS_PTHREAD)
CC_COVERAGE     = $(CC) $(CFLAGS_COVERAGE) $(CFLAGS_PTHREAD)

CXX_STATIC      = $(CXX) $(CFLAGS_OPTIMIZE) $(CFLAGS_PTHREAD)
CXX_SHARED      = $(CXX) $(CFLAGS_SHARED) $(CFLAGS_OPTIMIZE) $(CFLAGS_PTHREAD)
CXX_DEBUG       = $(CXX) $(CFLAGS_DEBUG) $(CFLAGS_PTHREAD)
CXX_COVERAGE    = $(CXX) $(CFLAGS_COVERAGE) $(CFLAGS_PTHREAD)

LD_STATIC       = $(LD) $(LDFLAGS_PTHREAD)
LD_SHARED       = $(LD) -shared $(CFLAGS_SHARED) $(CFLAGS_OPTIMIZE) $(LDFLAGS_PTHREAD)
LD_DEBUG        = $(LD) $(LDFLAGS_PTHREAD)
LD_COVERAGE     = $(LD) --coverage $(LDFLAGS_PTHREAD)

endif
endif




# clang -Weverything -Wno-missing-prototypes -Wno-shorten-64-to-32 -Wno-padded -Wno-exit-time-destructors -Wno-weak-vtables -std=c++03 foo.cpp -lstdc++
