# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

CXXFLAGS          = -Wall -pthread -DNDEBUG -O3 -msse4.2 -I../UnitTest++/src
LDFLAGS           = -pthread -lproc -L../UnitTest++ -lUnitTest++

SOURCES           = $(wildcard *.cpp)
TARGET            = test-stl

ifeq ($(MSYSTEM), MINGW32)
	SOURCES += ../Support/win32/mem.cpp
else
	SOURCES += ../Support/posix/mem.cpp
endif


OBJECTS = $(SOURCES:.cpp=.o)


all: $(TARGET)
.PHONY: all

clean:
	$(RM) *.d *.o $(TARGET)
.PHONY: clean


$(OBJECTS): Makefile


# Linking

$(TARGET): $(OBJECTS)
	$(CXX) $(OBJECTS) $(LDFLAGS) -o $@


# Compiling + dependency generation

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -MMD -MP -c $< -o $@

-include $(SOURCES:.cpp=.d)
