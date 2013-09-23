PASSIVE_SUBDIRS = util test-tightdb test-sqlite3 test-stl performance \
benchmark-insert-add benchmark-insert-get-set benchmark-prealloc experiments

test_tightdb_DEPS = util
test_sqlite3_DEPS = util
test_stl_DEPS     = util

TEST_PROGRAMS = tightdb-tests

tightdb_tests_SOURCES = $(wildcard *.c *.cpp large_tests/*.cpp)
tightdb_tests_LIBS    = ../src/tightdb/libtightdb.a

EXTRA_CLEAN_DIRS = large_tests

ifeq ($(shell pkg-config unittest++ --exists 2>/dev/null && echo yes),yes)
tightdb_tests_CFLAGS  = -Wno-sign-compare $(shell pkg-config unittest++ --cflags)
tightdb_tests_LDFLAGS = $(shell pkg-config unittest++ --libs)
else
PASSIVE_SUBDIRS   += UnitTest++
experiments_DEPS  += UnitTest++
tightdb_tests_CFLAGS  = -IUnitTest++/src
tightdb_tests_LDFLAGS = -LUnitTest++ -lUnitTest++
tightdb_tests_DEPS    = UnitTest++
endif

include ../generic.mk

.PHONY: benchmark
benchmark: subdir/test-tightdb/minimal subdir/test-sqlite3/minimal subdir/test-stl/minimal
	@echo ""
	@echo ".:: TightDB ::."
	test-tightdb/test-tightdb
	@echo ""
	@echo ".:: SQLite 3 ::."
	test-sqlite3/test-sqlite3
	@echo ""
	@echo ".:: STL Vector ::."
	test-stl/test-stl

.PHONY: UnitTest++
UnitTest++:
	$(MAKE) -C UnitTest++ CXXFLAGS="-g -Wall -W -ansi" LDFLAGS= >/dev/null

# Build and run the performance matrix benchmarking program
.PHONY: performance
performance: subdir/util/minimal
	@$(MAKE) -C performance test

# Build the add/insert benchmark
.PHONY: benchmark-insert-add
benchmark-insert-add: subdir/util/minimal
	@$(MAKE) -C benchmark-insert-add

# Build and run the insert/get/set benchmark
.PHONY: benchmark-insert-get-set
benchmark-insert-get-set: subdir/util/minimal
	@$(MAKE) -C benchmark-insert-get-set test

# Build and run the prealloc benchmark
.PHONY: benchmark-prealloc
benchmark-prealloc:
	@$(MAKE) -C benchmark-prealloc test

# Build and run whatever is in experiements/testcase.cpp
.PHONY: testcase testcase-debug
testcase:
	@$(MAKE) -C experiments test
testcase-debug:
	@$(MAKE) -C experiments test-debug
