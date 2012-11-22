NOINST_PROGRAMS = test-stl

test_stl_SOURCES = $(wildcard *.c *.cpp)

ifeq ($(MSYSTEM), MINGW32)
test_stl_SOURCES += ../Support/win32/mem.cpp
else
test_stl_SOURCES += ../Support/posix/mem.cpp
test_stl_LDFLAGS = -lproc
endif

ifeq ($(shell pkg-config unittest++ --exists 2>/dev/null && echo yes),yes)
test_stl_CFLAGS  += -Wno-sign-compare $(shell pkg-config unittest++ --cflags)
test_stl_LDFLAGS += $(shell pkg-config unittest++ --libs)
else
test_stl_CFLAGS  += -I../UnitTest++/src
test_stl_LDFLAGS += -L../UnitTest++ -lUnitTest++
endif

include ../../generic.mk
