# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file 
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#
# g++ -std=c++0x *.cpp -I./UnitTest++/src -L./UnitTest++/ -l UnitTest++ -o main

# Compiler and flags
#CXXFLAGS  = -Wall -Weffc++ -Wextra -std=c++0x
CXXFLAGS  = -std=c++0x
CXXLIBS   = -L./UnitTest++ -lUnitTest++
CXXINC    = -I./UnitTest++/src -I../src
CXX       = g++ $(CXXFLAGS)

# Files
EXECUTABLE = tightdb-tests

HEADERS    = $(wildcard ../src/*.h)
SOURCES    = $(wildcard ../src/*.cpp)
TEST_H     = $(wildcard *.h)
TEST_SRC   = $(wildcard *.cpp)
SURFIXES   = %.cpp %.h
OBJECTS    = $(SOURCES:.o=.cpp) $(HEADERS:.o=.h) \
             $(TEST_SRC:.o=.cpp) $(TEST_H:.o=.h)

# Linking
$(EXECUTABLE): $(OBJECTS)
	@$(CXX) $(OBJECTS) $(CXXLIBS) $(CXXINC) -o $@

# Compilation
%.o: $(SURFIXES)
	@$(CXX) -c $^

# Targets
all: CXXFLAGS += -DNDEBUG -O3
all: $(EXECUTABLE)

test: all
	@make -C test-tightdb
	@make -C test-stl_vector
	@./run_tests.sh

debug: CXXFLAGS += -DDEBUG -g3 -ggdb
debug: $(EXECUTABLE)

clean:
	@rm -f core *.o $(EXECUTABLE)
