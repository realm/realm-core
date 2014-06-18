SUBDIRS = util
PASSIVE_SUBDIRS = test-tightdb test-sqlite3 test-stl performance \
benchmark-insert-add benchmark-insert-get-set benchmark-prealloc \
benchmark-index benchmark-transaction experiments android ios

RESOURCES=expect_json.json expect_string.txt

DIR_DEPS          = util
test_tightdb_DEPS = util
test_sqlite3_DEPS = util
test_stl_DEPS     = util

check_PROGRAMS = tightdb-tests

TESTS := $(wildcard test_*.cpp large_tests/*.cpp)

tightdb_tests_SOURCES = $(TESTS) main.cpp
tightdb_tests_LIBS    = util/test-util.a

# Run test suite inside `gdb`
.PHONY: gdb gdb-debug
gdb: check-norun
	gdb tightdb-tests
gdb-debug: check-debug-norun
	gdb tightdb-tests-dbg

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
.PHONY: check-testcase check-testcase-debug memcheck-testcase memcheck-testcase-debug
.PHONY: gdb-testcase gdb-testcase-debug
check-testcase: check-norun/subdir/util
	@$(MAKE) -C experiments check
check-testcase-debug: check-debug-norun/subdir/util
	@$(MAKE) -C experiments check-debug
memcheck-testcase: check-norun/subdir/util
	@$(MAKE) -C experiments memcheck
memcheck-testcase-debug: check-debug-norun/subdir/util
	@$(MAKE) -C experiments memcheck-debug
gdb-testcase: check-norun/subdir/util
	@$(MAKE) -C experiments gdb-testcase
gdb-testcase-debug: check-debug-norun/subdir/util
	@$(MAKE) -C experiments gdb-testcase-debug

.PHONY: get-test-resources get-passive-subdirs
get-test-resources:
	@echo $(RESOURCES)
get-passive-subdirs:
	@echo $(PASSIVE_SUBDIRS)

include ../src/generic.mk
