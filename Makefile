SUBDIRS = src test
test_DEPS = src

include generic.mk

# Run and run the benchmarking programs
.PHONY: benchmark
benchmark: minimal
	@$(MAKE) -C test benchmark

# Run and run the performance matrix benchmarking program
.PHONY: performance
performance: minimal
	@$(MAKE) -C test performance

# Build the add/insert benchmarking program
.PHONY: benchmark-insert-add
benchmark-insert-add: minimal
	@$(MAKE) -C test benchmark-insert-add

# Build and run the insert/get/set benchmarking program
.PHONY: benchmark-insert-get-set
benchmark-insert-get-set: minimal
	@$(MAKE) -C test benchmark-insert-get-set

# Build and run the prealloc benchmarking program
.PHONY: benchmark-prealloc
benchmark-prealloc: minimal
	@$(MAKE) -C test benchmark-prealloc

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
testcase: minimal
	@$(MAKE) -C test testcase
testcase-debug: debug
	@$(MAKE) -C test testcase-debug

# Check documentation examples
.PHONY: check-doc-ex clean-doc-ex
check-doc-ex: debug
	@$(MAKE) -C doc/ref_cpp/examples test-debug
clean-doc-ex:
	@$(MAKE) -C doc/ref_cpp/examples clean

# Used by build.sh
.PHONY: get-libdir get-cc get-cxx get-ld
get-libdir:
	@echo $(libdir)
get-cc:
	@echo $(CC)
get-cxx:
	@echo $(CXX)
get-ld:
	@echo $(LD)
