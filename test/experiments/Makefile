TEST_PROGRAMS = testcase

testcase_SOURCES = testcase.cpp
testcase_LIBS = ../../src/tightdb/libtightdb.a

ifeq ($(shell pkg-config unittest++ --exists 2>/dev/null && echo yes),yes)
testcase_CFLAGS  = -Wno-sign-compare $(shell pkg-config unittest++ --cflags)
testcase_LDFLAGS = $(shell pkg-config unittest++ --libs)
else
testcase_CFLAGS  = -I../UnitTest++/src
testcase_LDFLAGS = -L../UnitTest++ -lUnitTest++
endif

include ../../generic.mk
