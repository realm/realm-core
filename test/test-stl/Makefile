# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

CXXFLAGS          = -Wall -pthread -DNDEBUG -O3 -I../UnitTest++/src
CXXFLAGS         += -DUSE_SSE -msse4.2
LDFLAGS           = -pthread -lproc -L../UnitTest++ -lUnitTest++

TARGET            = test-stl

SOURCES           = $(wildcard *.cpp)
OBJECTS           = $(SOURCES:.cpp=.o)

ifeq ($(MSYSTEM), MINGW32)
	SOURCES += ../Support/win32/mem.cpp
else
	SOURCES += ../Support/posix/mem.cpp
endif


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
