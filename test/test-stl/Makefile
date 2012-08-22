NOINST_PROGRAMS = test-stl

test_stl_SOURCES = $(wildcard *.c *.cpp)
test_stl_LDFLAGS = -lproc

ifeq ($(MSYSTEM), MINGW32)
	test_stl_SOURCES += ../Support/win32/mem.cpp
else
	test_stl_SOURCES += ../Support/posix/mem.cpp
endif

ifeq ($(shell pkg-config unittest++ --exists && echo yes),yes)
ADD_CFLAGS_INCLUDE = -Wno-sign-compare $(shell pkg-config unittest++ --cflags)
test_stl_LDFLAGS  += $(shell pkg-config unittest++ --libs)
else
ADD_CFLAGS_INCLUDE = -I../UnitTest++/src
test_stl_LDFLAGS  += -L../UnitTest++ -lUnitTest++
endif

include ../../generic.mk
