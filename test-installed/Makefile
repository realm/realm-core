TEST_PROGRAMS = test-installed

test_installed_SOURCES       = test.cpp
test_installed_LDFLAGS       = $(shell tightdb-config     --libs)
test_installed_LDFLAGS_DEBUG = $(shell tightdb-config-dbg --libs)
test_installed_LDFLAGS_COVER = $(shell tightdb-config-dbg --libs)

include generic.mk
