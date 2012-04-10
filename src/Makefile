# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

CXXFLAGS          = -Wall -pthread -fPIC -DPIC
CXXFLAGS_OPTIMIZE = -DNDEBUG -O3 -DUSE_SSE -msse4.2 -I.
CXXFLAGS_DEBUG    = -D_DEBUG -ggdb3 -I.
CXXFLAGS_COVER    = -D_DEBUG -DUSE_SSE -msse4.2 -I$(abspath .)

GEN_HEADERS       = tightdb.h
SOURCES           = $(wildcard *.cpp)
LIB_SHARED        = libtightdb.so
LIB_STATIC        = libtightdb.a
LIB_STATIC_DEBUG  = libtightdb_d.a
LIB_STATIC_COVER  = libtightdb_c.a


TARGETS = $(LIB_SHARED) $(LIB_STATIC)
DEBUG_TARGETS = $(LIB_STATIC_DEBUG)
COVER_TARGETS = $(LIB_STATIC_COVER)

OBJECTS = $(SOURCES:.cpp=.o)
DEBUG_OBJECTS = $(SOURCES:.cpp=.dbg.o)
COVER_OBJECTS = $(SOURCES:.cpp=.cov.o)


all: $(TARGETS) $(GEN_HEADERS)
.PHONY: all

debug: $(DEBUG_TARGETS) $(GEN_HEADERS)
.PHONY: debug

cover: $(COVER_TARGETS) $(GEN_HEADERS)
.PHONY: cover

clean:
	$(RM) *.d *.o *.gcno *.gcda $(TARGETS) $(DEBUG_TARGETS) $(COVER_TARGETS)
.PHONY: clean


$(OBJECTS) $(DEBUG_OBJECTS) $(COVER_OBJECTS): Makefile

$(GEN_HEADERS): Makefile
$(OBJECTS) $(DEBUG_OBJECTS) $(COVER_OBJECTS): $(GEN_HEADERS)


# Code generation
tightdb.h: tightdb-gen.sh tightdb-gen.py Makefile
	$(SHELL) tightdb-gen.sh tightdb.h


# Shared library

$(LIB_SHARED): $(OBJECTS)
# FIXME: add -Wl,-soname and -Wl,-rpath
	$(CXX) -shared $(OBJECTS) -o $@


# Static library

$(LIB_STATIC): $(OBJECTS)
	ar crs $@ $(OBJECTS)

$(LIB_STATIC_DEBUG): $(DEBUG_OBJECTS)
	ar crs $@ $(DEBUG_OBJECTS)

$(LIB_STATIC_COVER): $(COVER_OBJECTS)
	ar crs $@ $(COVER_OBJECTS)


# Compiling + dependency generation

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CXXFLAGS_OPTIMIZE) -MMD -MP -c $< -o $@

%.dbg.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CXXFLAGS_DEBUG) -MMD -MP -c $< -o $@

%.cov.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CXXFLAGS_COVER) --coverage -MMD -MP -c $(abspath $<) -o $(abspath $@)

-include $(SOURCES:.cpp=.d) $(SOURCES:.cpp=.dbg.d) $(SOURCES:.cpp=.cov.d)
