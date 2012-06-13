# Note:
# $@  The name of the target file (the one before the colon)
# $<  The name of the first (or only) prerequisite file
#     (the first one after the colon)
# $^  The names of all the prerequisite files (space separated)
# $*  The stem (the bit which matches the % wildcard in the rule definition.
#

SUBDIRS = src


all: $(SUBDIRS)
.PHONY: all

install: SUBDIRS_MODE = install
install: all
.PHONY: install

uninstall: SUBDIRS_MODE = uninstall
uninstall: all
.PHONY: uninstall

# Build optimized shared library
shared: SUBDIRS_MODE = shared
shared: all
.PHONY: shared

# Build optimized static library
static: SUBDIRS_MODE = static
static: all
.PHONY: static

# Build static library compiled for debugging
debug: SUBDIRS_MODE = debug
debug: all
.PHONY: debug

# Build static library compiled for coverage analysis
cover: SUBDIRS_MODE = cover
cover: all
.PHONY: cover

clean: SUBDIRS_MODE = clean
clean: $(SUBDIRS) clean/test
clean/test:
	@$(MAKE) -C test clean
.PHONY: clean clean/test


# Run the unit tests after building everything in debug mode
test: debug
	@$(MAKE) -C test test
.PHONY: test

# Run the unit tests after building everything in release mode
test-release: static
	@$(MAKE) -C test test-release
.PHONY: test-release

# Run valgrind on the unit tests after building everything
memtest: debug
	@$(MAKE) -C test memtest
.PHONY: memtest

# Run valgrind on the unit tests after building everything in release mode
memtest-release: static
	@$(MAKE) -C test memtest-release
.PHONY: memtest-release

# Run the benchmarking progrems after building everything
benchmark: static
	@$(MAKE) -C test benchmark
.PHONY: benchmark

# Run coverage analysis after building everything, this time using LCOV
lcov: cover
	@$(MAKE) -C test cover
	find -name '*.gcda' -delete
	cd test && ./tightdb-tests-cover --no-error-exit-staus
	lcov --capture --directory . --output-file /tmp/tightdb.lcov
	lcov --extract /tmp/tightdb.lcov '$(abspath .)/src/*' --output-file /tmp/tightdb-clean.lcov
	rm -fr cover_html
	genhtml --prefix $(abspath .) --output-directory cover_html /tmp/tightdb-clean.lcov
.PHONY: lcov

# Run coverage analysis after building everything, this time using GCOVR
gcovr: cover
	@$(MAKE) -C test cover
	find -name '*.gcda' -delete
	cd test && ./tightdb-tests-cover --no-error-exit-staus
	gcovr -r src -x >gcovr.xml
.PHONY: gcovr

$(SUBDIRS):
	@$(MAKE) -C $@ $(SUBDIRS_MODE)
.PHONY: $(SUBDIRS)
