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

debug: SUBDIRS_MODE = debug
debug: all
.PHONY: debug

cover: SUBDIRS_MODE = cover
cover: all
.PHONY: cover

clean: SUBDIRS_MODE = clean
clean: $(SUBDIRS) clean/test
clean/test:
	@$(MAKE) -C test clean
.PHONY: clean clean/test


test: debug
	@$(MAKE) -C test test
.PHONY: test

benchmark: all
	@$(MAKE) -C test benchmark
.PHONY: benchmark

lcov: cover
	@$(MAKE) -C test cover
	find -name '*.gcda' -delete
	cd test && ./tightdb-tests-cover
	lcov --capture --directory . --output-file /tmp/tightdb.lcov
	lcov --extract /tmp/tightdb.lcov '$(abspath .)/src/*' --output-file /tmp/tightdb-clean.lcov
	rm -fr cover_html
	genhtml --prefix $(abspath .) --output-directory cover_html /tmp/tightdb-clean.lcov
.PHONY: lcov


$(SUBDIRS):
	@$(MAKE) -C $@ $(SUBDIRS_MODE)
.PHONY: $(SUBDIRS)
