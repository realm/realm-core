# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

ROOT = ..
CONFIG_MK = $(ROOT)/config.mk
include $(CONFIG_MK)

CXXFLAGS           ?= $(CXXFLAGS_COMMON)
CXXFLAGS_COMMON_2   = -pthread $(CXXFLAGS) -IUnitTest++/src
CXXFLAGS_OPTIMIZE  += $(CXXFLAGS_COMMON_2) -I$(ROOT)/src -I$(ROOT)/include
CXXFLAGS_DEBUG     += $(CXXFLAGS_COMMON_2) -I$(ROOT)/src -I$(ROOT)/include
CXXFLAGS_COVERAGE  += $(CXXFLAGS_COMMON_2) -I$(abspath $(ROOT)/src) -I$(abspath $(ROOT)/include)
LDFLAGS             = -pthread -LUnitTest++ -lUnitTest++

SOURCES             = $(wildcard *.cpp large_tests/*.cpp)
LIBTIGHTDB          = $(ROOT)/src/libtightdb.a
LIBTIGHTDB_DEBUG    = $(ROOT)/src/libtightdb_d.a
LIBTIGHTDB_COVERAGE = $(ROOT)/src/libtightdb_c.a
TARGET              = tightdb-tests
DEBUG_TARGET        = tightdb-tests-debug
COVER_TARGET        = tightdb-tests-cover


OBJECTS = $(SOURCES:.cpp=.o)
DEBUG_OBJECTS = $(SOURCES:.cpp=.dbg.o)
COVER_OBJECTS = $(SOURCES:.cpp=.cov.o)


all: $(TARGET)
.PHONY: all

debug: $(DEBUG_TARGET)
.PHONY: debug

cover: $(COVER_TARGET)
.PHONY: cover

CLEAN_EXTRA = clean/UnitTest++ clean/test-tightdb clean/test-sqlite3 clean/test-stl
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
	valgrind --quiet --error-exitcode=1 --track-origins=yes --leak-check=yes --leak-resolution=low ./tightdb-tests-debug --no-error-exit-staus
.PHONY: test

benchmark: test-tightdb test-sqlite3 test-stl
	@echo ""
	@echo ".:: TightDB ::."
	test-tightdb/test-tightdb
	@echo ""
	@echo ".:: SQLite 3 ::."
	test-sqlite3/test-sqlite3
	@echo ""
	@echo ".:: STL Vector ::."
	test-stl/test-stl
test-tightdb test-sqlite3 test-stl: UnitTest++
	@$(MAKE) -C $@
.PHONY: benchmark test-tightdb test-sqlite3 test-stl


UnitTest++:
	$(MAKE) -C UnitTest++ >/dev/null
.PHONY: UnitTest++
$(TARGET) $(DEBUG_TARGET) $(COVER_TARGET): | UnitTest++


$(OBJECTS) $(DEBUG_OBJECTS) $(COVER_OBJECTS): Makefile $(CONFIG_MK)


# Linking

$(TARGET): $(OBJECTS) $(LIBTIGHTDB)
	$(CXX) $(OBJECTS) $(LIBTIGHTDB) $(LDFLAGS) -o $@

$(DEBUG_TARGET): $(DEBUG_OBJECTS) $(LIBTIGHTDB_DEBUG)
	$(CXX) $(DEBUG_OBJECTS) $(LIBTIGHTDB_DEBUG) $(LDFLAGS) -o $@

$(COVER_TARGET): $(COVER_OBJECTS) $(LIBTIGHTDB_COVERAGE)
	$(CXX) --coverage $(COVER_OBJECTS) $(LIBTIGHTDB_COVERAGE) $(LDFLAGS) -o $@


# Compiling + dependency generation

%.o: %.cpp
	$(CXX) $(CXXFLAGS_OPTIMIZE) -MMD -MP -c $< -o $@

%.dbg.o: %.cpp
	$(CXX) $(CXXFLAGS_DEBUG) -MMD -MP -c $< -o $@

%.cov.o: %.cpp
	$(CXX) --coverage $(CXXFLAGS_COVERAGE) -MMD -MP -c $(abspath $<) -o $(abspath $@)

-include $(SOURCES:.cpp=.d) $(SOURCES:.cpp=.dbg.d) $(SOURCES:.cpp=.cov.d)
