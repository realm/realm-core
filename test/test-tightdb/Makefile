# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

ROOT = ../..
CONFIG_MK = $(ROOT)/config.mk
include $(CONFIG_MK)

CXXFLAGS           ?= $(CXXFLAGS_COMMON)
CXXFLAGS_COMMON_2   = -pthread $(CXXFLAGS) -I../UnitTest++/src -I$(ROOT)/src -I$(ROOT)/include
CXXFLAGS_OPTIMIZE  += $(CXXFLAGS_COMMON_2)
CXXFLAGS_DEBUG     += $(CXXFLAGS_COMMON_2)
LDFLAGS             = -pthread -lproc -L../UnitTest++ -lUnitTest++

SOURCES             = $(wildcard *.cpp)
LIBTIGHTDB          = $(ROOT)/src/libtightdb.a
LIBTIGHTDB_DEBUG    = $(ROOT)/src/libtightdb_d.a
TARGET              = test-tightdb
DEBUG_TARGET        = test-tightdb-debug

ifeq ($(MSYSTEM), MINGW32)
	SOURCES += ../Support/win32/mem.cpp
else
	SOURCES += ../Support/posix/mem.cpp
endif


OBJECTS = $(SOURCES:.cpp=.o)
DEBUG_OBJECTS = $(SOURCES:.cpp=.dbg.o)


all: $(TARGET)
.PHONY: all

debug: $(DEBUG_TARGET)
.PHONY: debug

clean:
	$(RM) *.d *.o $(TARGET) $(DEBUG_TARGET)
.PHONY: clean


$(OBJECTS) $(DEBUG_OBJECTS): Makefile $(CONFIG_MK)


# Linking

$(TARGET): $(OBJECTS) $(LIBTIGHTDB)
	$(CXX) $(OBJECTS) $(LIBTIGHTDB) $(LDFLAGS) -o $@

$(DEBUG_TARGET): $(DEBUG_OBJECTS) $(LIBTIGHTDB_DEBUG)
	$(CXX) $(DEBUG_OBJECTS) $(LIBTIGHTDB_DEBUG) $(LDFLAGS) -o $@


# Compiling + dependency generation

%.o: %.cpp
	$(CXX) $(CXXFLAGS_OPTIMIZE) -MMD -MP -c $< -o $@

%.dbg.o: %.cpp
	$(CXX) $(CXXFLAGS_DEBUG) -MMD -MP -c $< -o $@

-include $(SOURCES:.cpp=.d) $(SOURCES:.cpp=.dbg.d)
