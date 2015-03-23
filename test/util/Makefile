check_LIBRARIES = test-util.a

test_util_a_SOURCES = \
demangle.cpp \
mem.cpp \
timer.cpp \
random.cpp \
benchmark_results.cpp \
wildcard.cpp \
unit_test.cpp \
test_path.cpp \
test_only.cpp \
misc.cpp \
verified_integer.cpp \
verified_string.cpp \
resource_limits.cpp \
jsmn.cpp


test_util_a_LIBS = ../../src/tightdb/librealm.a

ifeq ($(shell uname),Linux)

test_util_a_LDFLAGS = -lrt

ifeq ($(shell pkg-config libprocps --exists 2>/dev/null && echo yes),yes)
PROCPS_CFLAGS  := $(shell pkg-config libprocps --cflags)
PROCPS_LDFLAGS := $(shell pkg-config libprocps --libs)
test_util_a_CFLAGS  += $(PROCPS_CFLAGS)
test_util_a_LDFLAGS += $(PROCPS_LDFLAGS)
else
test_util_a_LDFLAGS += -lproc
endif

mem_o_CFLAGS = -DREALM_HAVE_LIBPROCPS

endif

include ../../src/generic.mk
