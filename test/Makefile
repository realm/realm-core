SUBDIRS = util
PASSIVE_SUBDIRS = \
test-tightdb \
test-sqlite3 \
test-stl \
performance \
benchmark-insert-add \
benchmark-crud \
benchmark-prealloc \
benchmark-index \
benchmark-transaction \
benchmark-row-accessor \
experiments

RESOURCES=expect_json.json expect_string.txt

DIR_DEPS          = util
test_realm_DEPS = util
test_sqlite3_DEPS = util
test_stl_DEPS     = util

check_PROGRAMS = realm-tests

TESTS := $(wildcard test_*.cpp large_tests/*.cpp)

realm_tests_SOURCES = $(TESTS) main.cpp
realm_tests_LIBS    = util/test-util.a

# Run test suite inside `gdb`
.PHONY: gdb gdb-debug
gdb: check-norun
	gdb realm-tests
gdb-debug: check-debug-norun
	gdb realm-tests-dbg

.PHONY: benchmark
benchmark: check-norun/subdir/test-tightdb check-norun/subdir/test-sqlite3 check-norun/subdir/test-stl
	@echo ""
	@echo ".:: Realm ::."
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

# Build and run the CRUD benchmarking program
.PHONY: benchmark-crud
benchmark-crud: check-norun/subdir/util
	@$(MAKE) -C benchmark-crud check

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

# Build and run the "row accessor" benchmarking program
.PHONY: benchmark-row-accessor
benchmark-row-accessor: check-norun/subdir/util
	@$(MAKE) -C benchmark-row-accessor check

# Build and run the "common tasks" benchmarking program
.PHONY: benchmark-common-tasks
benchmark-common-tasks: check-norun/subdir/util
	@$(MAKE) -C benchmark-common-tasks check

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
