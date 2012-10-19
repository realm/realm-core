NOINST_PROGRAMS = test-tightdb

test_tightdb_SOURCES = $(wildcard *.c *.cpp)
test_tightdb_LIBS  = ../../src/tightdb/libtightdb.a

ifeq ($(MSYSTEM), MINGW32)
test_tightdb_SOURCES += ../Support/win32/mem.cpp
else
test_tightdb_SOURCES += ../Support/posix/mem.cpp
test_tightdb_LDFLAGS = -lproc
endif

ifeq ($(shell pkg-config unittest++ --exists 2>/dev/null && echo yes),yes)
ADD_CFLAGS_INCLUDE    = -Wno-sign-compare $(shell pkg-config unittest++ --cflags)
test_tightdb_LDFLAGS += $(shell pkg-config unittest++ --libs)
else
ADD_CFLAGS_INCLUDE    = -I../UnitTest++/src
test_tightdb_LDFLAGS += -L../UnitTest++ -lUnitTest++
endif

include ../../generic.mk
