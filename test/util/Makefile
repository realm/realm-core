NOINST_LIBRARIES = test-util

test_util_SOURCES = mem.cpp timer.cpp benchmark_results.cpp

ifeq ($(shell uname),Linux)

test_util_LDFLAGS = -lrt

ifeq ($(shell pkg-config libprocps --exists 2>/dev/null && echo yes),yes)
test_util_CFLAGS  += $(shell pkg-config libprocps --cflags)
test_util_LDFLAGS += $(shell pkg-config libprocps --libs)
else
test_util_LDFLAGS += -lproc
endif

mem_o_CFLAGS = -DTIGHTDB_HAVE_LIBPROCPS

endif

include ../../generic.mk
