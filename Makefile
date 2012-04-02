# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file 
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

# Compiler and flags
# CXXFLAGS  = -Wall -Weffc++ -std=c++0x
CXXFLAGS  = -Wall -pthread
#CXXFLAGS += -DUSE_SSE -msse4.2
CXXLIBS   = -L./src
CXXINC    = -I./src
CXX       = g++
CXXCMD    = $(CXX) $(CXXFLAGS) $(CXXINC) 

# Files
LIB_STATIC = libtightdb.a
LIB_SHARED = libtightdb.so

SOURCES    = $(wildcard src/*.cpp)
OBJECTS    = $(SOURCES:.cpp=.o)
OBJ_SHARED = $(SOURCES:.cpp=.so)
GENERATED_HEADERS = src/tightdb.h

nodebug: CXXFLAGS += -DNDEBUG -O3
nodebug: all
.PHONY: nodebug

debug: CXXFLAGS += -D_DEBUG -g3 -ggdb
debug: all
#	@(cd test && make debug)
.PHONY: debug

# Targets
all: src/tightdb.h
all: $(LIB_STATIC) # Comment out to disable building of static library
# all: $(LIB_SHARED) # Comment out to disable building of shared library
.PHONY: all

test: clean debug
	@(cd test && make)
	@(cd test && ./run_tests.sh)
.PHONY: test

clean:
	@rm -f core *.o *.so *.d *.1 *.a
	@rm -f core src/*.o src/*.so src/*.d src/*.1 src/*.a
	@(cd test && make clean)
.PHONY: clean

# Code generation
src/tightdb.h: src/tightdb-gen.sh src/tightdb-gen.py
	@sh src/tightdb-gen.sh src/tightdb.h

# Compiling
%.o: %.cpp
	$(CXXCMD) -o $@ -c $<

%.so: %.cpp
	$(CXXCMD) -fPIC -fno-strict-aliasing -o $@ -c $<

%.d: %.cpp $(GENERATED_HEADERS)
	@$(CXXCMD) -MM -MF $@ -MG -MP -MT $*.o -MT $*.so $<

-include $(SOURCES:.cpp=.d)

# Archive static object
$(LIB_STATIC): $(OBJECTS)
	@echo "Creating static library: $(LIB_STATIC)" 
	ar crs $(LIB_STATIC) $^
#	@rm -f $(OBJECTS)

# Linking
$(LIB_SHARED): $(OBJ_SHARED)
	@echo "Creating shared library: $(LIB_SHARED)"
#	$(CXXCMD) -shared -fPIC -rdynamic -Wl,-export-dynamic,-soname,$@ $^ -o $@
	$(CXXCMD) -shared -fPIC -rdynamic -Wl,-export-dynamic $^ -o $@
#	@rm -f $(OBJ_SHARED)
