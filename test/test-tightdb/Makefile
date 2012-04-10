# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

CXXFLAGS          = -Wall -pthread -I../UnitTest++/src
LDFLAGS           = -pthread -lproc -L../UnitTest++ -lUnitTest++
CXXFLAGS_OPTIMIZE = -DNDEBUG -O3 -DUSE_SSE -msse4.2 -I../../src
CXXFLAGS_DEBUG    = -D_DEBUG -ggdb3 -DMAX_LIST_SIZE=4 -I../../src

SOURCES           = $(wildcard *.cpp)
LIBTIGHTDB        = ../../src/libtightdb.a
LIBTIGHTDB_DEBUG  = ../../src/libtightdb_d.a
TARGET            = test-tightdb
DEBUG_TARGET      = test-tightdb-debug

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
	$(RM) *.d *.o $(TARGET)
.PHONY: clean


$(OBJECTS) $(DEBUG_OBJECTS): Makefile


# Linking

$(TARGET): $(OBJECTS) $(LIBTIGHTDB)
	$(CXX) $(OBJECTS) $(LIBTIGHTDB) $(LDFLAGS) -o $@

$(DEBUG_TARGET): $(DEBUG_OBJECTS) $(LIBTIGHTDB_DEBUG)
	$(CXX) $(DEBUG_OBJECTS) $(LIBTIGHTDB_DEBUG) $(LDFLAGS) -o $@


# Compiling + dependency generation

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CXXFLAGS_OPTIMIZE) -MMD -MP -c $< -o $@

%.dbg.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CXXFLAGS_DEBUG) -MMD -MP -c $< -o $@

-include $(SOURCES:.cpp=.d) $(SOURCES:.cpp=.dbg.d)
