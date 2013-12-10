SUBDIRS = util
PASSIVE_SUBDIRS = test-tightdb test-sqlite3 test-stl performance \
benchmark-insert-add benchmark-insert-get-set benchmark-prealloc \
benchmark-index benchmark-transaction experiments

DIR_DEPS          = util
test_tightdb_DEPS = util
test_sqlite3_DEPS = util
test_stl_DEPS     = util

check_PROGRAMS = tightdb-tests

tightdb_tests_SOURCES := $(wildcard *.c *.cpp large_tests/*.cpp)
tightdb_tests_LIBS    := util/test-util.a

ifeq ($(shell pkg-config unittest++ --exists 2>/dev/null && echo yes),yes)
UNITTESTPP_CFLAGS  := $(shell pkg-config unittest++ --cflags)
UNITTESTPP_LDFLAGS := $(shell pkg-config unittest++ --libs)
tightdb_tests_CFLAGS  = -Wno-sign-compare $(UNITTESTPP_CFLAGS)
tightdb_tests_LDFLAGS = $(UNITTESTPP_LDFLAGS)
else
PASSIVE_SUBDIRS += UnitTest++
tightdb_tests_CFLAGS  = -IUnitTest++/src
tightdb_tests_LDFLAGS = -LUnitTest++ -lUnitTest++
tightdb_tests_DEPS    = fallback-UnitTest++
endif

.PHONY: benchmark
benchmark: check-norun/subdir/test-tightdb check-norun/subdir/test-sqlite3 check-norun/subdir/test-stl
	@echo ""
	@echo ".:: TightDB ::."
	test-tightdb/test-tightdb
	@echo ""
	@echo ".:: SQLite 3 ::."
	test-sqlite3/test-sqlite3
	@echo ""
	@echo ".:: STL Vector ::."
	test-stl/test-stl

FALLBACK_UNITTESTPP_CFLAGS = -g -Wall -W -ansi $(CFLAGS_ARCH)
FALLBACK_UNITTESTPP_LDFLAGS = $(LDFLAGS_ARCH)

# FIXME: The fallback version of UnitTest++ is used on Darwin, but
# when compiling with Clang (the only option on OS X 10.9) the
# self-test test suite of UnitTest++ fails, so we have to ignore
# errors from the build command below, hence the leading '-'
.PHONY: fallback-UnitTest++
fallback-UnitTest++:
	-$(MAKE) -C UnitTest++ CXX="$(CXX)" CXXFLAGS="$(FALLBACK_UNITTESTPP_CFLAGS)" LDFLAGS="$(FALLBACK_UNITTESTPP_LDFLAGS)" >/dev/null

# Build and run the performance matrix benchmarking program
.PHONY: performance
performance: check-norun/subdir/util
	@$(MAKE) -C performance check

# Build the add/insert benchmark
.PHONY: benchmark-insert-add
benchmark-insert-add: check-norun/subdir/util
	@$(MAKE) -C benchmark-insert-add check-norun

# Build and run the insert/get/set benchmark
.PHONY: benchmark-insert-get-set
benchmark-insert-get-set: check-norun/subdir/util
	@$(MAKE) -C benchmark-insert-get-set check

# Build and run the prealloc benchmark
.PHONY: benchmark-prealloc
benchmark-prealloc: check-norun/subdir/util
	@$(MAKE) -C benchmark-prealloc check

# Build the index benchmark
.PHONY: benchmark-index
benchmark-index: check-norun/subdir/util
	@$(MAKE) -C benchmark-index check-norun

# Build the transaction benchmark
.PHONY: benchmark-transaction
benchmark-transaction: check-norun/subdir/util
	@$(MAKE) -C benchmark-transaction check-norun

# Build and run whatever is in experiements/testcase.cpp
.PHONY: testcase testcase-debug
testcase: check-norun/subdir/util
	@$(MAKE) -C experiments check
testcase-debug: check-debug-norun/subdir/util
	@$(MAKE) -C experiments check-debug


include ../src/generic.mk
