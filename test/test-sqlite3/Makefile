TEST_PROGRAMS = test-sqlite3

test_sqlite3_SOURCES = sqlite3.c test-sqlite3.cpp
test_sqlite3_LIBS  = ../util/test-util.a
test_sqlite3_LDFLAGS = -ldl

include ../../generic.mk

sqlite3.o: sqlite3.c
	gcc -O3 -MMD -MP -c $< -o $@
