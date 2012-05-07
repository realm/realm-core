# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

ROOT = ..
CONFIG_MK = $(ROOT)/config.mk
include $(CONFIG_MK)

CXXFLAGS           ?= $(CXXFLAGS_COMMON)
CXXFLAGS_COMMON_2   = -pthread -fPIC -DPIC $(CXXFLAGS)
CXXFLAGS_OPTIMIZE  += $(CXXFLAGS_COMMON_2) -I. -I../include
CXXFLAGS_DEBUG     += $(CXXFLAGS_COMMON_2) -I. -I../include
CXXFLAGS_COVERAGE  += $(CXXFLAGS_COMMON_2) -I$(abspath .)  -I$(abspath ../include)

GEN_HEADERS         = ../include/tightdb.hpp
SOURCES             = $(wildcard *.cpp)
LIB_SHARED          = libtightdb.so
LIB_STATIC          = libtightdb.a
LIB_STATIC_DEBUG    = libtightdb_d.a
LIB_STATIC_COVERAGE = libtightdb_c.a


TARGETS = $(LIB_SHARED) $(LIB_STATIC)
DEBUG_TARGETS = $(LIB_STATIC_DEBUG)
COVER_TARGETS = $(LIB_STATIC_COVERAGE)

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


$(OBJECTS) $(DEBUG_OBJECTS) $(COVER_OBJECTS): Makefile $(CONFIG_MK)

$(GEN_HEADERS): Makefile $(CONFIG_MK)
$(OBJECTS) $(DEBUG_OBJECTS) $(COVER_OBJECTS): $(GEN_HEADERS)


# Code generation
../include/tightdb.hpp: tightdb-gen.sh tightdb-gen.py
	$(SHELL) tightdb-gen.sh ../include/tightdb.hpp


# Shared library

$(LIB_SHARED): $(OBJECTS)
# FIXME: add -Wl,-soname and -Wl,-rpath
	$(CXX) -shared $(OBJECTS) -o $@


# Static library

$(LIB_STATIC): $(OBJECTS)
	ar crs $@ $(OBJECTS)

$(LIB_STATIC_DEBUG): $(DEBUG_OBJECTS)
	ar crs $@ $(DEBUG_OBJECTS)

$(LIB_STATIC_COVERAGE): $(COVER_OBJECTS)
	ar crs $@ $(COVER_OBJECTS)


# Compiling + dependency generation

%.o: %.cpp
	$(CXX) $(CXXFLAGS_OPTIMIZE) -MMD -MP -c $< -o $@

%.dbg.o: %.cpp
	$(CXX) $(CXXFLAGS_DEBUG) -MMD -MP -c $< -o $@

%.cov.o: %.cpp
	$(CXX) --coverage $(CXXFLAGS_COVERAGE) -MMD -MP -c $(abspath $<) -o $(abspath $@)

-include $(SOURCES:.cpp=.d) $(SOURCES:.cpp=.dbg.d) $(SOURCES:.cpp=.cov.d)
