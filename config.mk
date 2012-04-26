CXXFLAGS_COMMON   = -ansi -Wall -Wextra -pedantic -Wno-long-long
CXXFLAGS_OPTIMIZE = -DNDEBUG -O3 -DUSE_SSE -msse4.2
CXXFLAGS_DEBUG    = -D_DEBUG -ggdb3 -DMAX_LIST_SIZE=4 #-fno-elide-constructors
CXXFLAGS_COVERAGE = -D_DEBUG -DMAX_LIST_SIZE=4 -DUSE_SSE -msse4.2

# clang -Weverything -Wno-missing-prototypes -Wno-shorten-64-to-32 -Wno-padded -Wno-exit-time-destructors -Wno-weak-vtables -std=c++03 foo.cpp -lstdc++
