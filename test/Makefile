# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

CXXFLAGS          = -Wall -pthread -IUnitTest++/src
LDFLAGS           = -pthread -LUnitTest++ -lUnitTest++
CXXFLAGS_OPTIMIZE = -DNDEBUG -O3 -DUSE_SSE -msse4.2 -I../src
CXXFLAGS_DEBUG    = -D_DEBUG -ggdb3 -DMAX_LIST_SIZE=4 -I../src
CXXFLAGS_COVER    = -D_DEBUG -DUSE_SSE -msse4.2 -I$(abspath ../src)

SOURCES           = $(wildcard *.cpp large_tests/*.cpp)
LIBTIGHTDB        = ../src/libtightdb.a
LIBTIGHTDB_DEBUG  = ../src/libtightdb_d.a
LIBTIGHTDB_COVER  = ../src/libtightdb_c.a
TARGET            = tightdb-tests
DEBUG_TARGET      = tightdb-tests-debug
COVER_TARGET      = tightdb-tests-cover


OBJECTS = $(SOURCES:.cpp=.o)
DEBUG_OBJECTS = $(SOURCES:.cpp=.dbg.o)
COVER_OBJECTS = $(SOURCES:.cpp=.cov.o)


all: $(TARGET)
.PHONY: all

debug: $(DEBUG_TARGET)
.PHONY: debug

cover: $(COVER_TARGET)
.PHONY: cover

CLEAN_EXTRA = clean/UnitTest++ clean/test-tightdb clean/test-stl
clean: clean/local $(CLEAN_EXTRA)
clean/local:
	$(RM) *.d *.o *.gcno *.gcda $(TARGET) $(DEBUG_TARGET) $(COVER_TARGET)
	$(RM) large_tests/*.d large_tests/*.o large_tests/*.gcno large_tests/*.gcda
$(CLEAN_EXTRA): clean/%:
	@$(MAKE) -C $* clean
.PHONY: clean clean/local $(CLEAN_EXTRA)


test: debug
	./tightdb-tests-debug
.PHONY: test

memtest: debug
	valgrind --quiet --error-exitcode=1 --track-origins=yes --leak-check=yes --leak-resolution=low ./tightdb-tests-debug
.PHONY: test

benchmark: test-tightdb test-stl
	@echo ""
	@echo ".:: TightDB ::."
	test-tightdb/test-tightdb
	@echo ""
	@echo ".:: STL Vector ::."
	test-stl/test-stl
test-tightdb test-stl: UnitTest++
	@$(MAKE) -C $@
.PHONY: benchmark test-tightdb test-stl


UnitTest++:
	$(MAKE) -C UnitTest++ >/dev/null
.PHONY: UnitTest++
$(TARGET) $(DEBUG_TARGET) $(COVER_TARGET): | UnitTest++


$(OBJECTS) $(DEBUG_OBJECTS) $(COVER_OBJECTS): Makefile


# Linking

$(TARGET): $(OBJECTS) $(LIBTIGHTDB)
	$(CXX) $(OBJECTS) $(LIBTIGHTDB) $(LDFLAGS) -o $@

$(DEBUG_TARGET): $(DEBUG_OBJECTS) $(LIBTIGHTDB_DEBUG)
	$(CXX) $(DEBUG_OBJECTS) $(LIBTIGHTDB_DEBUG) $(LDFLAGS) -o $@

$(COVER_TARGET): $(COVER_OBJECTS) $(LIBTIGHTDB_COVER)
	$(CXX) --coverage $(COVER_OBJECTS) $(LIBTIGHTDB_COVER) $(LDFLAGS) -o $@


# Compiling + dependency generation

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CXXFLAGS_OPTIMIZE) -MMD -MP -c $< -o $@

%.dbg.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CXXFLAGS_DEBUG) -MMD -MP -c $< -o $@

%.cov.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CXXFLAGS_COVER) --coverage -MMD -MP -c $(abspath $<) -o $(abspath $@)

-include $(SOURCES:.cpp=.d) $(SOURCES:.cpp=.dbg.d) $(SOURCES:.cpp=.cov.d)
