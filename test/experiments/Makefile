check_PROGRAMS = testcase

testcase_SOURCES = testcase.cpp run-testcase.cpp
testcase_LIBS = ../util/test-util.a

# Run test suite inside `gdb`/`lldb`
.PHONY: gdb-testcase gdb-testcase-debug lldb-testcase lldb-testcase-debug
gdb-testcase: check-norun
	gdb testcase
gdb-testcase-debug: check-debug-norun
	gdb testcase-dbg
lldb-testcase: check-norun
	lldb testcase
lldb-testcase-debug: check-debug-norun
	lldb testcase-dbg

# check_PROGRAMS += innotest
# innotest_SOURCES = innotest.cpp
# innotest_LIBS = ../../src/realm/librealm.a
#innotest_LDFLAGS = -lprofiler

include ../../src/generic.mk
