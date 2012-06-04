# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

ROOT = ..

INC_FLAGS     = -I$(ROOT)/src -IUnitTest++/src
INC_FLAGS_ABS = -I$(abspath $(ROOT)/src) -IUnitTest++/src
LIB_FLAGS     = -LUnitTest++ -lUnitTest++

SOURCES             = $(wildcard *.c *.cpp large_tests/*.cpp)
LIBTIGHTDB          = $(ROOT)/src/tightdb/libtightdb.a
LIBTIGHTDB_DEBUG    = $(ROOT)/src/tightdb/libtightdb_d.a
LIBTIGHTDB_COVERAGE = $(ROOT)/src/tightdb/libtightdb_c.a
TARGET              = tightdb-tests
DEBUG_TARGET        = tightdb-tests-debug
COVERAGE_TARGET     = tightdb-tests-cover
TARGETS = $(TARGET) $(DEBUG_TARGET) $(COVERAGE_TARGET)

all: $(TARGET)
.PHONY: all

debug: $(DEBUG_TARGET)
.PHONY: debug

cover: $(COVERAGE_TARGET)
.PHONY: cover

test: debug
	./tightdb-tests-debug
.PHONY: test

test-release: all
	./tightdb-tests
.PHONY: test-release

memtest: debug
	valgrind --quiet --error-exitcode=1 --track-origins=yes --leak-check=yes --leak-resolution=low ./tightdb-tests-debug --no-error-exit-staus
.PHONY: memtest

memtest-release: all
	valgrind --quiet --error-exitcode=1 --track-origins=yes --leak-check=yes --leak-resolution=low ./tightdb-tests --no-error-exit-staus
.PHONY: memtest-release

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
	$(MAKE) -C UnitTest++ CXX=g++ CXXFLAGS=-g -Wall -W -ansi LDFLAGS= >/dev/null
.PHONY: UnitTest++
$(TARGETS): | UnitTest++



CONFIG_MK = $(ROOT)/config.mk
include $(CONFIG_MK)

STATIC_OBJECTS = $(patsubst %.c,%.o,$(patsubst %.cpp,%.o,$(SOURCES)))
SHARED_OBJECTS   = $(STATIC_OBJECTS:.o=.dyn.o)
DEBUG_OBJECTS    = $(STATIC_OBJECTS:.o=.dbg.o)
COVERAGE_OBJECTS = $(STATIC_OBJECTS:.o=.cov.o)
OBJECTS = $(STATIC_OBJECTS) $(SHARED_OBJECTS) $(DEBUG_OBJECTS) $(COVERAGE_OBJECTS)

$(OBJECTS): Makefile $(CONFIG_MK)


CLEAN_EXTRA = clean/UnitTest++ clean/test-tightdb clean/test-sqlite3 clean/test-stl
clean: clean/local $(CLEAN_EXTRA)
clean/local:
	$(RM) *.d *.o *.gcno *.gcda $(TARGETS)
	$(RM) large_tests/*.d large_tests/*.o large_tests/*.gcno large_tests/*.gcda
$(CLEAN_EXTRA): clean/%:
	@$(MAKE) -C $* clean
.PHONY: clean clean/local $(CLEAN_EXTRA)


# Linking

$(TARGET): $(STATIC_OBJECTS) $(LIBTIGHTDB)
	$(LD_STATIC) $(STATIC_OBJECTS) $(LIBTIGHTDB) $(LDFLAGS) $(LIB_FLAGS) -o $@

$(DEBUG_TARGET): $(DEBUG_OBJECTS) $(LIBTIGHTDB_DEBUG)
	$(LD_DEBUG) $(DEBUG_OBJECTS) $(LIBTIGHTDB_DEBUG) $(LDFLAGS) $(LIB_FLAGS) -o $@

$(COVERAGE_TARGET): $(COVERAGE_OBJECTS) $(LIBTIGHTDB_COVERAGE)
	$(LD_COVERAGE) $(COVERAGE_OBJECTS) $(LIBTIGHTDB_COVERAGE) $(LDFLAGS) $(LIB_FLAGS) -o $@


# Compiling + automatic dependencies

%.o: %.c
	$(CC_STATIC) $(CFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.o: %.cpp
	$(CXX_STATIC) $(CXXFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.dyn.o: %.c
	$(CC_SHARED) $(CFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.dyn.o: %.cpp
	$(CXX_SHARED) $(CXXFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.dbg.o: %.c
	$(CC_DEBUG) $(CFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.dbg.o: %.cpp
	$(CXX_DEBUG) $(CXXFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.cov.o: %.c
	$(CC_COVERAGE) $(CFLAGS) $(INC_FLAGS_ABS) -MMD -MP -c $(abspath $<) -o $(abspath $@)

%.cov.o: %.cpp
	$(CXX_COVERAGE) $(CXXFLAGS) $(INC_FLAGS_ABS) -MMD -MP -c $(abspath $<) -o $(abspath $@)

-include $(OBJECTS:.o=.d)
