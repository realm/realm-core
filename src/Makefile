# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

ROOT = ..

INC_FLAGS     = -I. -I../include
INC_FLAGS_ABS = -I$(abspath .) -I$(abspath ../include)

SOURCES      = $(wildcard *.c *.cpp)
LIB_SHARED   = libtightdb.so
LIB_STATIC   = libtightdb.a
LIB_DEBUG    = libtightdb_d.a
LIB_COVERAGE = libtightdb_c.a
TARGETS = $(LIB_STATIC) $(LIB_SHARED) $(LIB_DEBUG) $(LIB_COVERAGE)

all: $(LIB_SHARED)
.PHONY: all

static: $(LIB_STATIC)
.PHONY: static

debug: $(LIB_DEBUG)
.PHONY: debug

cover: $(LIB_COVERAGE)
.PHONY: cover

# Code generation
TIGHTDB_HPP = ../include/tightdb.hpp
$(TIGHTDB_HPP): Makefile $(CONFIG_MK)
# FIXME: Is this really necessary?
$(OBJECTS): $(TIGHTDB_HPP)
$(TIGHTDB_HPP): tightdb-gen.sh tightdb-gen.py
	$(SHELL) tightdb-gen.sh $(TIGHTDB_HPP)



CONFIG_MK = $(ROOT)/config.mk
include $(CONFIG_MK)

STATIC_OBJECTS = $(patsubst %.c,%.o,$(patsubst %.cpp,%.o,$(SOURCES)))
SHARED_OBJECTS   = $(STATIC_OBJECTS:.o=.dyn.o)
DEBUG_OBJECTS    = $(STATIC_OBJECTS:.o=.dbg.o)
COVERAGE_OBJECTS = $(STATIC_OBJECTS:.o=.cov.o)
OBJECTS = $(STATIC_OBJECTS) $(SHARED_OBJECTS) $(DEBUG_OBJECTS) $(COVERAGE_OBJECTS)

$(OBJECTS): Makefile $(CONFIG_MK)


clean:
	$(RM) *.d *.o *.gcno *.gcda $(TARGETS)
.PHONY: clean


# Library creation

$(LIB_STATIC): $(STATIC_OBJECTS)
	$(AR) $(ARFLAGS) $@ $(STATIC_OBJECTS)

$(LIB_SHARED): $(SHARED_OBJECTS)
        # FIXME: add -Wl,-soname and -Wl,-rpath
	$(LD_SHARED) $(SHARED_OBJECTS) -o $@

$(LIB_DEBUG): $(DEBUG_OBJECTS)
	$(AR) $(ARFLAGS) $@ $(DEBUG_OBJECTS)

$(LIB_COVERAGE): $(COVERAGE_OBJECTS)
	$(AR) $(ARFLAGS) $@ $(COVERAGE_OBJECTS)


# Compiling + automatic dependencies

%.o: %.c
	$(CC_STATIC) $(CFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.o: %.cpp
	$(CXX_STATIC) $(CXXFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.dyn.o: %.c
	$(CC_SHARED) $(CFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.dyn.o: %.cpp
	$(CXX_SHARED) $(CXXFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.dbg.o: %.c
	$(CC_DEBUG) $(CFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.dbg.o: %.cpp
	$(CXX_DEBUG) $(CXXFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.cov.o: %.c
	$(CC_COVERAGE) $(CFLAGS) $(INC_FLAGS_ABS) -MMD -MP -c $(abspath $<) -o $(abspath $@)

%.cov.o: %.cpp
	$(CXX_COVERAGE) $(CXXFLAGS) $(INC_FLAGS_ABS) -MMD -MP -c $(abspath $<) -o $(abspath $@)

-include $(OBJECTS:.o=.d)
