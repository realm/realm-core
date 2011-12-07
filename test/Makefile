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
CXXLIBS   = -L./UnitTest++ -lUnitTest++
CXXINC    = -I./UnitTest++/src -I../src
CXX       = g++ $(CXXFLAGS)

# Files
EXECUTABLE = tightdb-tests

# Note for gcov: You must not pass .h files to g++ or you'll get errors about missing data files.
# Also, never use relative paths, or you'll get "time stamp" errors

# HEADERS    = $(abspath $(wildcard ../src/*.h))
SOURCES    = $(abspath $(wildcard ../src/*.cpp))
# TEST_H     = $(abspath $(wildcard *.h))
TEST_SRC   = $(abspath $(wildcard *.cpp large_tests/*.cpp))
SURFIXES   = %.cpp %.h
OBJECTS    = $(SOURCES:.o=.cpp) $(TEST_SRC:.o=.cpp) 

# Targets
all: CXXFLAGS += -DNDEBUG -O3
all: $(EXECUTABLE)
	@make -C test-tightdb
	@make -C test-stl

test: all
	@./run_tests.sh

debug: CXXFLAGS += -DDEBUG -g3 -ggdb
debug: $(EXECUTABLE)

gcov: CXXFLAGS += -DDEBUG -g3 -ggdb --coverage
gcov: clean $(EXECUTABLE)
	@./$(EXECUTABLE)
	@lcov --directory . --capture --output-file app.info
	@genhtml --output-directory cov_htmp app.info

clean:
	@(cd UnitTest++ && make clean)
	@(cd test-stl && make clean)
	@(cd test-tightdb && make clean)
	@(cd test-sqlite3 && make clean)
	@rm -f core *.o *.gcda *.gcno *.gcov $(EXECUTABLE)

# Linking
$(EXECUTABLE): $(OBJECTS)
	@make -C UnitTest++ > /dev/null
	@$(CXX) $(OBJECTS) $(CXXLIBS) $(CXXINC) -o $@

# Compilation
%.o: $(SURFIXES)
	@$(CXX) -c $^

