check_PROGRAMS = testcase innotest

testcase_SOURCES = testcase.cpp run-testcase.cpp
testcase_LIBS = ../util/test-util.a

innotest_SOURCES = innotest.cpp
innotest_LIBS = ../../src/tightdb/libtightdb.a
innotest_LDFLAGS = -lprofiler
include ../../src/generic.mk
