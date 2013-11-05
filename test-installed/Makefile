TEST_PROGRAMS = test-installed

TIGHTDB_INCLUDEDIR    = $(shell tightdb-config --includedir)
TIGHTDB_CFLAGS        = $(shell tightdb-config --cflags)
TIGHTDB_CFLAGS_DEBUG  = $(shell tightdb-config-dbg --cflags)
TIGHTDB_LIBDIR        = $(shell tightdb-config --libdir)
TIGHTDB_LDFLAGS       = $(shell tightdb-config --libs)
TIGHTDB_LDFLAGS_DEBUG = $(shell tightdb-config-dbg --libs)

test_installed_SOURCES       = test.cpp
test_installed_CFLAGS        = -I$(TIGHTDB_INCLUDEDIR)
test_installed_CFLAGS_OPTIM  = $(TIGHTDB_CFLAGS)
test_installed_CFLAGS_DEBUG  = $(TIGHTDB_CFLAGS_DEBUG)
test_installed_CFLAGS_COVER  = $(TIGHTDB_CFLAGS_DEBUG)
test_installed_LDFLAGS       = -L$(TIGHTDB_LIBDIR) -Wl,-rpath,$(TIGHTDB_LIBDIR)
test_installed_LDFLAGS_OPTIM = $(TIGHTDB_LDFLAGS)
test_installed_LDFLAGS_DEBUG = $(TIGHTDB_LDFLAGS_DEBUG)
test_installed_LDFLAGS_COVER = $(TIGHTDB_LDFLAGS_DEBUG)

include generic.mk
