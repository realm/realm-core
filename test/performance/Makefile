NOINST_PROGRAMS = performance

performance_SOURCES = matrix.cpp
performance_LIBS    = ../../src/tightdb/libtightdb.a
performance_LDFLAGS = -lrt

include ../../generic.mk

.PHONY: run
run: default
	./performance
