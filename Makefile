# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file 
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

# Compiler and flags
# CXXFLAGS  = -Wall -Weffc++ -std=c++0x
 CXXFLAGS  = -Wall -std=c++0x -lpthread
#CXXFLAGS  = -std=c++0x
CXXLIBS   = -L./src
CXXINC    = -I./src
CXX       = g++
CXXCMD    = $(CXX) $(CXXFLAGS) $(CXXINC) 

# Files
#LIB_SHARED = libtightdb.so.1
LIB_SHARED = libtightdb.a
#LIB_STATIC = libtightdb.a

SOURCES    = $(wildcard src/*.cpp)
OBJECTS    = $(SOURCES:.cpp=.o)
OBJ_SHARED = $(SOURCES:.cpp=.so)
GENERATED_HEADERS = src/tightdb.h

# Targets
all: static
all: shared
all: src/tightdb.h

static: CXXFLAGS += -DNDEBUG -O3
static: $(LIB_STATIC)
	@echo "Created static library: $(LIB_STATIC)" 
#	@rm -f $(OBJECTS)

shared: CXXFLAGS += -DNDEBUG -O3
shared: $(LIB_SHARED)
	@echo "Created shared library: $(LIB_SHARED)"
#	@rm -f $(OBJ_SHARED)

test: all
	@(cd test && make)
	@(cd test && ./run_tests.sh)

debug: CXXFLAGS += -DDEBUG -g3 -ggdb
debug: all
	@(cd test && make debug)

clean:
	@rm -f core *.o *.d *.so *.1 *.a
	@rm -f core src/*.o src/*.d src/*.so src/*.1 src/*.a
	@(cd test && make clean)

# Code generation
src/tightdb.h: src/tightdb-gen.py
	python src/tightdb-gen.py 50 >src/tightdb.h

# Compiling
%.o: %.cpp
	@$(CXXCMD) -o $@ -c $<

%.d: %.cpp $(GENERATED_HEADERS)
	@$(CXXCMD) -M -MF $@ -MG -MP $< 

%.so: %.cpp
	@$(CXXCMD) -fPIC -fno-strict-aliasing -o $@ -c $<

-include $(SOURCES:.cpp=.d)

# Archive static object
$(LIB_STATIC): $(OBJECTS)
	ar crs $(LIB_STATIC) $^

# Linking
$(LIB_SHARED): $(OBJ_SHARED)
#	$(CXXCMD) -shared -fPIC -rdynamic -Wl,-export-dynamic,-soname,$@ $^ -o $@
	$(CXXCMD) -shared -fPIC -rdynamic -Wl,-export-dynamic $^ -o $@
