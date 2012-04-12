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
CXXFLAGS_OPTIMIZE  += -pthread $(CXXFLAGS) -I../UnitTest++/src
LDFLAGS             = -pthread -lproc -ldl -L../UnitTest++ -lUnitTest++

SOURCES             = sqlite3.c $(wildcard *.cpp)
TARGET              = test-sqlite3

ifeq ($(MSYSTEM), MINGW32)
	SOURCES += ../Support/win32/mem.cpp
else
	SOURCES += ../Support/posix/mem.cpp
endif


OBJECTS = $(patsubst %.c,%.o,$(patsubst %.cpp,%.o,$(SOURCES)))

all: $(TARGET)
.PHONY: all

clean:
	$(RM) *.d *.o $(TARGET)
.PHONY: clean


$(OBJECTS): Makefile $(CONFIG_MK)


# Linking

$(TARGET): $(OBJECTS)
	$(CXX) $(OBJECTS) $(LDFLAGS) -o $@


# Compiling + dependency generation

%.o: %.c
	$(CC) $(CXXFLAGS_OPTIMIZE) -MMD -MP -c $< -o $@

%.o: %.cpp
	$(CXX) $(CXXFLAGS_OPTIMIZE) -MMD -MP -c $< -o $@

-include $(patsubst %.c,%.d,$(patsubst %.cpp,%.d,$(SOURCES)))
