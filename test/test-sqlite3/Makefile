NOINST_PROGRAMS = test-sqlite3

test_sqlite3_SOURCES = sqlite3.c $(wildcard *.cpp)
test_sqlite3_LIBADD  = ../../src/tightdb/libtightdb.a
test_sqlite3_LDFLAGS = -lproc -ldl

ifeq ($(MSYSTEM), MINGW32)
	test_sqlite3_SOURCES += ../Support/win32/mem.cpp
else
	test_sqlite3_SOURCES += ../Support/posix/mem.cpp
endif

ifeq ($(shell pkg-config unittest++ --exists 2>/dev/null && echo yes),yes)
ADD_CFLAGS_INCLUDE    = -Wno-sign-compare $(shell pkg-config unittest++ --cflags)
test_sqlite3_LDFLAGS += $(shell pkg-config unittest++ --libs)
else
ADD_CFLAGS_INCLUDE    = -I../UnitTest++/src
test_sqlite3_LDFLAGS += -L../UnitTest++ -lUnitTest++
endif

include ../../generic.mk

sqlite3.o: sqlite3.c
	gcc -O3 -MMD -MP -c $< -o $@
