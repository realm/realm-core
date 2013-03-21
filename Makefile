SUBDIRS = src test
test_DEPS = src

include generic.mk

# Run the benchmarking programs
.PHONY: benchmark
benchmark: minimal
	@$(MAKE) -C test benchmark

# Run the performance matrix benchmarking program
.PHONY: performance
performance: minimal
	@$(MAKE) -C test/performance run

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
