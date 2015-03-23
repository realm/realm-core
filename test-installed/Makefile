check_PROGRAMS = test-installed

REALM_INCLUDEDIR    := $(shell realm-config     --includedir)
REALM_CFLAGS        := $(shell realm-config     --cflags)
REALM_CFLAGS_DEBUG  := $(shell realm-config-dbg --cflags)
REALM_LIBDIR        := $(shell realm-config     --libdir)
REALM_LDFLAGS       := $(shell realm-config     --libs)
REALM_LDFLAGS_DEBUG := $(shell realm-config-dbg --libs)

test_installed_SOURCES       = test.cpp
test_installed_CFLAGS        = -I$(REALM_INCLUDEDIR)
test_installed_CFLAGS_OPTIM  = $(REALM_CFLAGS)
test_installed_CFLAGS_DEBUG  = $(REALM_CFLAGS_DEBUG)
test_installed_CFLAGS_COVER  = $(REALM_CFLAGS_DEBUG)
test_installed_LDFLAGS       = -L$(REALM_LIBDIR) -Wl,-rpath,$(REALM_LIBDIR)
test_installed_LDFLAGS_OPTIM = $(REALM_LDFLAGS)
test_installed_LDFLAGS_DEBUG = $(REALM_LDFLAGS_DEBUG)
test_installed_LDFLAGS_COVER = $(REALM_LDFLAGS_DEBUG)

include generic.mk
