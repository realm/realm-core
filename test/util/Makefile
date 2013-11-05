TEST_LIBRARIES = test-util.a

test_util_a_SOURCES = mem.cpp timer.cpp benchmark_results.cpp misc.cpp
test_util_a_LIBS = ../../src/tightdb/libtightdb.a

ifeq ($(shell uname),Linux)

test_util_a_LDFLAGS = -lrt

ifeq ($(shell pkg-config libprocps --exists 2>/dev/null && echo yes),yes)
test_util_a_CFLAGS  += $(shell pkg-config libprocps --cflags)
test_util_a_LDFLAGS += $(shell pkg-config libprocps --libs)
else
test_util_a_LDFLAGS += -lproc
endif

mem_o_CFLAGS = -DTIGHTDB_HAVE_LIBPROCPS

endif

include ../../src/generic.mk
