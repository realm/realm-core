# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file 
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

# Compiler and flags
#CXXFLAGS  = -Wall -Weffc++ -Wextra -std=c++0x
CXXFLAGS  = -std=c++0x
CXXLIBS   = -L../UnitTest++ -lUnitTest++ -lproc
CXXINC    = -I../UnitTest++/src -I../../src -I./
CXX       = g++ $(CXXFLAGS)

# Files
EXECUTABLE = test-tightdb

HEADERS    = $(wildcard ../../src/*.h)
SOURCES    = $(wildcard ../../src/*.cpp)
TEST_H     = $(wildcard *.h)
TEST_SRC   = $(wildcard *.cpp)
ifeq ($(MSYSTEM), MINGW32)
	TEST_SRC += ../Support/win32/mem.cpp
else
	TEST_SRC += ../Support/posix/mem.cpp
endif
TEST_C     = $(wildcard *.c)
SURFIXES   = %.cpp %.h %.c
OBJECTS    = $(SOURCES:.o=.cpp) $(HEADERS:.o=.h) \
             $(TEST_SRC:.o=.cpp) $(TEST_H:.o=.h) $(TEST_SRC:.o=.c)

# Targets
all: CXXFLAGS += -DNDEBUG -O3
all: $(EXECUTABLE)

test: all
	./$(EXECUTABLE)

debug: CXXFLAGS += -DDEBUG -g3 -ggdb
debug: $(EXECUTABLE)

clean:
	@rm -f core *.o $(EXECUTABLE)

# Linking
$(EXECUTABLE): $(OBJECTS)
	@$(CXX) $(OBJECTS) $(CXXLIBS) $(CXXINC) -o $@

# Compilation
%.o: $(SURFIXES)
	@$(CXX) -c $^
