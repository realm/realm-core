# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

ROOT = ../..

INC_FLAGS     = -I$(ROOT)/src -I$(ROOT)/include -I../UnitTest++/src
INC_FLAGS_ABS = -I$(abspath $(ROOT)/src) -I$(abspath $(ROOT)/include) -I../UnitTest++/src
LIB_FLAGS     = -lproc -L../UnitTest++ -lUnitTest++

SOURCES       = $(wildcard *.c *.cpp)
TARGET        = test-stl
TARGETS       = $(TARGET)

ifeq ($(MSYSTEM), MINGW32)
	SOURCES += ../Support/win32/mem.cpp
else
	SOURCES += ../Support/posix/mem.cpp
endif

all: $(TARGET)
.PHONY: all



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


# Linking

$(TARGET): $(STATIC_OBJECTS)
	$(LD_STATIC) $(STATIC_OBJECTS) $(LDFLAGS) $(LIB_FLAGS) -o $@


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
