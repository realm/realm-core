SUBDIRS = util
PASSIVE_SUBDIRS = test-tightdb test-sqlite3 test-stl performance \
benchmark-insert-add benchmark-insert-get-set benchmark-prealloc \
benchmark-index benchmark-transaction experiments

DIR_DEPS          = util
test_tightdb_DEPS = util
test_sqlite3_DEPS = util
test_stl_DEPS     = util

TEST_PROGRAMS = tightdb-tests

TEST_SOURCES := $(wildcard *.c *.cpp large_tests/*.cpp)
tightdb_tests_SOURCES = $(TEST_SOURCES)
tightdb_tests_LIBS    = util/test-util.a

EXTRA_CLEAN_DIRS = large_tests

ifeq ($(shell pkg-config unittest++ --exists 2>/dev/null && echo yes),yes)
UNITTESTPP_CFLAGS  := $(shell pkg-config unittest++ --cflags)
UNITTESTPP_LDFLAGS := $(shell pkg-config unittest++ --libs)
tightdb_tests_CFLAGS  = -Wno-sign-compare $(UNITTESTPP_CFLAGS)
tightdb_tests_LDFLAGS = $(UNITTESTPP_LDFLAGS)
else
PASSIVE_SUBDIRS += UnitTest++
tightdb_tests_CFLAGS  = -IUnitTest++/src
tightdb_tests_LDFLAGS = -LUnitTest++ -lUnitTest++
tightdb_tests_DEPS    = UnitTest++
endif

include ../src/generic.mk

.PHONY: benchmark
benchmark: test-norun/subdir/test-tightdb test-norun/subdir/test-sqlite3 test-norun/subdir/test-stl
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
	$(MAKE) -C UnitTest++ CXXFLAGS="-g -Wall -W -ansi" LDFLAGS="" >/dev/null

# Build and run the performance matrix benchmarking program
.PHONY: performance
performance: test-norun/subdir/util
	@$(MAKE) -C performance test

# Build the add/insert benchmark
.PHONY: benchmark-insert-add
benchmark-insert-add: test-norun/subdir/util
	@$(MAKE) -C benchmark-insert-add test-norun

# Build and run the insert/get/set benchmark
.PHONY: benchmark-insert-get-set
benchmark-insert-get-set: test-norun/subdir/util
	@$(MAKE) -C benchmark-insert-get-set test

# Build and run the prealloc benchmark
.PHONY: benchmark-prealloc
benchmark-prealloc: test-norun/subdir/util
	@$(MAKE) -C benchmark-prealloc test

# Build the index benchmark
.PHONY: benchmark-index
benchmark-index: test-norun/subdir/util
	@$(MAKE) -C benchmark-index test-norun

# Build the transaction benchmark
.PHONY: benchmark-transaction
benchmark-transaction: test-norun/subdir/util
	@$(MAKE) -C benchmark-transaction test-norun

# Build and run whatever is in experiements/testcase.cpp
.PHONY: testcase testcase-debug
testcase: test-norun/subdir/util
	@$(MAKE) -C experiments test
testcase-debug: test-debug-norun/subdir/util
	@$(MAKE) -C experiments test-debug
