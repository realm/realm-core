SUBDIRS = src test convert
test_DEPS = src
convert_DEPS = src

include src/generic.mk

# Build and run the benchmarking programs
.PHONY: benchmark
benchmark: test-norun/subdir/src
	@$(MAKE) -C test benchmark

# Build and run the performance matrix benchmarking program
.PHONY: performance
performance: test-norun/subdir/src
	@$(MAKE) -C test performance

# Build the add/insert benchmarking program
.PHONY: benchmark-insert-add
benchmark-insert-add: test-norun/subdir/src
	@$(MAKE) -C test benchmark-insert-add

# Build and run the insert/get/set benchmarking program
.PHONY: benchmark-insert-get-set
benchmark-insert-get-set: test-norun/subdir/src
	@$(MAKE) -C test benchmark-insert-get-set

# Build and run the prealloc benchmarking program
.PHONY: benchmark-prealloc
benchmark-prealloc: test-norun/subdir/src
	@$(MAKE) -C test benchmark-prealloc

# Build the index benchmarking program
.PHONY: benchmark-index
benchmark-index: test-norun/subdir/src
	@$(MAKE) -C test benchmark-index

# Build the transaction benchmarking program
.PHONY: benchmark-transaction
benchmark-transaction: test-norun/subdir/src
	@$(MAKE) -C test benchmark-transaction

# Run coverage analysis after building everything, this time using LCOV
.PHONY: lcov
lcov: test-cover
	lcov --capture --directory . --output-file /tmp/tightdb.lcov
	lcov --extract /tmp/tightdb.lcov '$(abspath .)/src/*' --output-file /tmp/tightdb-clean.lcov
	rm -fr cover_html
	genhtml --prefix $(abspath .) --output-directory cover_html /tmp/tightdb-clean.lcov

# Run coverage analysis after building everything, this time using GCOVR
.PHONY: gcovr
gcovr: test-cover
	gcovr -r src -x >gcovr.xml

# Build and run whatever is in test/experiements/testcase.cpp
.PHONY: testcase testcase-debug
testcase: test-norun/subdir/src
	@$(MAKE) -C test testcase
testcase-debug: test-debug-norun/subdir/src
	@$(MAKE) -C test testcase-debug

# Check documentation examples
.PHONY: check-doc-ex clean-doc-ex
check-doc-ex: test-debug-norun/subdir/src
	@$(MAKE) -C doc/ref_cpp/examples test-debug
clean-doc-ex:
	@$(MAKE) -C doc/ref_cpp/examples clean

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
