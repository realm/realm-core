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
CXXLIBS   =
CXXINC    =
CXX       = g++ $(CXXFLAGS)

# Files
LIB_SHARED = libtightdb.so.1
LIB_STATIC = libtightdb.a

HEADERS    = src/tightdb.h
#SOURCES    = $(wildcard src/*.cpp)
OBJECT     = tightdb.o
OBJ_SHARED = tightdb-shared.o

# Targets
all: static

static: CXXFLAGS += -DNDEBUG -O3
static: $(LIB_STATIC)
	@echo "Created static library: $(LIB_STATIC)" 
	@rm -f $(OBJECT)

shared: CXXFLAGS += -DNDEBUG -O3
shared: $(LIB_SHARED)
	@echo "Created shared library: $(LIB_SHARED)"
	@rm -f $(OBJ_SHARED)

test: all
	@(cd test && make)
	@(cd test && ./run_tests.sh)

debug: CXXFLAGS += -DDEBUG -g3 -ggdb
debug: all
	@(cd test && make debug)

clean:
	@rm -f core *.o *.so *.1 *.a
	@(cd test && make clean)

# Compiling
$(OBJECT): $(HEADERS)
	@$(CXX) -c $(HEADERS) -o $@

$(OBJ_SHARED): $(HEADERS)
	$(CXX) -fPIC -c $(HEADERS) -o $@

# Archive static object
$(LIB_STATIC): $(OBJECT)
	@ar crs $(LIB_STATIC) $^

# Linking
$(LIB_SHARED): $(OBJ_SHARED)
	$(CXX) -shared -fPIC -Wl,-soname,$(LIB_SHARED) $(OBJ_SHARED) -o $(LIB_SHARED)

