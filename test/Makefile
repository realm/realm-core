PASSIVE_SUBDIRS = test-tightdb test-sqlite3 test-stl

TEST_PROGRAMS = tightdb-tests

tightdb_tests_SOURCES = $(wildcard *.c *.cpp large_tests/*.cpp)
tightdb_tests_LIBADD = ../src/tightdb/libtightdb.a

EXTRA_CLEAN_DIRS = large_tests

ifeq ($(shell pkg-config unittest++ --exists 2>/dev/null && echo yes),yes)
ADD_CFLAGS_INCLUDE    = -Wno-sign-compare $(shell pkg-config unittest++ --cflags)
tightdb_tests_LDFLAGS = $(shell pkg-config unittest++ --libs)
else
PASSIVE_SUBDIRS      += UnitTest++
test_tightdb_DEPS     = UnitTest++
test_sqlite3_DEPS     = UnitTest++
test_stl              = UnitTest++
ADD_CFLAGS_INCLUDE    = -IUnitTest++/src
tightdb_tests_LDFLAGS = -LUnitTest++ -lUnitTest++
tightdb_tests_DEPS    = UnitTest++
endif

include ../generic.mk

.PHONY: benchmark
benchmark: subdir/test-tightdb/default subdir/test-sqlite3/default subdir/test-stl/default
	@echo ""
	@echo ".:: TightDB ::."
	test-tightdb/test-tightdb
	@echo ""
	@echo ".:: SQLite 3 ::."
	test-sqlite3/test-sqlite3
	@echo ""
	@echo ".:: STL Vector ::."
	test-stl/test-stl

.PHONY: UnitTest++
UnitTest++:
	$(MAKE) -C UnitTest++ CXXFLAGS="-g -Wall -W -ansi" LDFLAGS= >/dev/null
