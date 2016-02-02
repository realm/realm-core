SUBDIRS = src test
test_DEPS = src

PASSIVE_SUBDIRS = doc/ref_cpp/examples
doc_ref_cpp_examples_DEPS = src

# Continue support for obsolete `test`-based goal names.  You should use the new
# `check`-based names instead.
.PHONY: test test-debug
test: check
test-debug: check-debug

# Run test suite inside `gdb`
.PHONY: gdb gdb-debug
gdb: check-norun/subdir/src
	@$(MAKE) -C test gdb
gdb-debug: check-debug-norun/subdir/src
	@$(MAKE) -C test gdb-debug

# Build and run the benchmarking programs
.PHONY: benchmark
benchmark: check-norun/subdir/src
	@$(MAKE) -C test benchmark

# Build and run the performance matrix benchmarking program
.PHONY: performance
performance: check-norun/subdir/src
	@$(MAKE) -C test performance

# Build the add/insert benchmarking program
.PHONY: benchmark-insert-add
benchmark-insert-add: check-norun/subdir/src
	@$(MAKE) -C test benchmark-insert-add

# Build and run the CRUD benchmarking program
.PHONY: benchmark-crud
benchmark-crud: check-norun/subdir/src
	@$(MAKE) -C test benchmark-crud

# Build and run the prealloc benchmarking program
.PHONY: benchmark-prealloc
benchmark-prealloc: check-norun/subdir/src
	@$(MAKE) -C test benchmark-prealloc

# Build the index benchmarking program
.PHONY: benchmark-index
benchmark-index: check-norun/subdir/src
	@$(MAKE) -C test benchmark-index

# Build the transaction benchmarking program
.PHONY: benchmark-transaction
benchmark-transaction: check-norun/subdir/src
	@$(MAKE) -C test benchmark-transaction

# Build and run the "row accessor" benchmarking program
.PHONY: benchmark-row-accessor
benchmark-row-accessor: check-norun/subdir/src
	@$(MAKE) -C test benchmark-row-accessor

# Build and run the "common" benchmarking program
.PHONY: benchmark-common-tasks
benchmark-common-tasks: check-norun/subdir/src
	@$(MAKE) -C test benchmark-common-tasks

# Build and run the "util network" benchmarking program
.PHONY: benchmark-util-network
benchmark-util-network: check-norun/subdir/src
	@$(MAKE) -C test benchmark-util-network

# Run coverage analysis after building everything, this time using LCOV
.PHONY: lcov
lcov: check-cover
	lcov --capture --directory . --output-file /tmp/realm.lcov
	lcov --extract /tmp/realm.lcov '$(abspath .)/src/*' --output-file /tmp/realm-clean.lcov
	rm -fr cover_html
	genhtml --prefix $(abspath .) --output-directory cover_html /tmp/realm-clean.lcov

# Run coverage analysis after building everything, this time using GCOVR
.PHONY: gcovr
gcovr: check-cover
	gcovr --filter='.*src/realm.*' -x >gcovr.xml

# Build and run whatever is in test/experiements/testcase.cpp
.PHONY: check-testcase check-testcase-debug memcheck-testcase memcheck-testcase-debug
.PHONY: gdb-testcase gdb-testcase-debug
check-testcase: check-norun/subdir/src
	@$(MAKE) -C test check-testcase
check-testcase-debug: check-debug-norun/subdir/src
	@$(MAKE) -C test check-testcase-debug
memcheck-testcase: check-norun/subdir/src
	@$(MAKE) -C test memcheck-testcase
memcheck-testcase-debug: check-debug-norun/subdir/src
	@$(MAKE) -C test memcheck-testcase-debug
gdb-testcase: check-norun/subdir/src
	@$(MAKE) -C test gdb-testcase
gdb-testcase-debug: check-debug-norun/subdir/src
	@$(MAKE) -C test gdb-testcase-debug

# Used by build.sh
.PHONY: get-exec-prefix get-includedir get-bindir get-libdir get-libexecdir
get-exec-prefix:
	@echo $(exec_prefix)
get-includedir:
	@echo $(includedir)
get-bindir:
	@echo $(bindir)
get-libdir:
	@echo $(libdir)
get-libexecdir:
	@echo $(libexecdir)
.PHONY: get-cc get-cxx get-ld
get-cc:
	@echo $(CC)
get-cxx:
	@echo $(CXX)
get-ld:
	@echo $(LD)


include src/generic.mk
