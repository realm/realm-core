SUBDIRS = src test
test_DEPS = src

include generic.mk

# Run the benchmarking programs
.PHONY: benchmark
benchmark: static
	@$(MAKE) -C test benchmark

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
