ROOT = ../..

INC_FLAGS     = -I$(ROOT)/src
INC_FLAGS_ABS = -I$(abspath $(ROOT)/src)

INSTALLED_HEADERS = $(wildcard *.h *.hpp)

LIBTIGHTDB_SOURCES  = $(wildcard *.c *.cpp)
LIBTIGHTDB_SHARED   = libtightdb.so
LIBTIGHTDB_STATIC   = libtightdb.a
LIBTIGHTDB_DEBUG    = libtightdb_d.a
LIBTIGHTDB_COVERAGE = libtightdb_c.a

LIBTIGHTDB_STATIC_OBJECTS   = $(patsubst %.c,%.o,$(patsubst %.cpp,%.o,$(LIBTIGHTDB_SOURCES)))
LIBTIGHTDB_SHARED_OBJECTS   = $(LIBTIGHTDB_STATIC_OBJECTS:.o=.dyn.o)
LIBTIGHTDB_DEBUG_OBJECTS    = $(LIBTIGHTDB_STATIC_OBJECTS:.o=.dbg.o)
LIBTIGHTDB_COVERAGE_OBJECTS = $(LIBTIGHTDB_STATIC_OBJECTS:.o=.cov.o)
LIBTIGHTDB_OBJECTS = $(LIBTIGHTDB_SHARED_OBJECTS) $(LIBTIGHTDB_STATIC_OBJECTS) $(LIBTIGHTDB_DEBUG_OBJECTS) $(LIBTIGHTDB_COVERAGE_OBJECTS)

OBJECTS           = $(LIBTIGHTDB_OBJECTS)
INSTALLED_TARGETS = $(LIBTIGHTDB_SHARED) $(LIBTIGHTDB_STATIC) $(LIBTIGHTDB_DEBUG)
TARGETS           = $(INSTALLED_TARGETS) $(LIBTIGHTDB_COVERAGE)

all: shared
.PHONY: all

install: $(INSTALLED_TARGETS) installdirs
	$(INSTALL_LIB) $(INSTALLED_TARGETS) $(DESTDIR)$(libdir)/
	$(INSTALL_HEADER) $(INSTALLED_HEADERS) $(DESTDIR)$(includedir)/tightdb/
.PHONY: install

installdirs:
	$(INSTALL_DIR) $(DESTDIR)$(libdir)/ $(DESTDIR)$(includedir)/tightdb/
.PHONY: installdirs

uninstall:
	rm -f $(DESTDIR)$(includedir)/tightdb/*.hpp
	rm -f $(DESTDIR)$(includedir)/tightdb/*.h
	rmdir $(DESTDIR)$(includedir)/tightdb
	rm -f $(DESTDIR)$(libdir)/$(LIBTIGHTDB_SHARED)
	rm -f $(DESTDIR)$(libdir)/$(LIBTIGHTDB_STATIC)
	rm -f $(DESTDIR)$(libdir)/$(LIBTIGHTDB_DEBUG)
.PHONY: uninstall

shared: $(LIBTIGHTDB_SHARED)
.PHONY: shared

static: $(LIBTIGHTDB_STATIC)
.PHONY: static

debug: $(LIBTIGHTDB_DEBUG)
.PHONY: debug

cover: $(LIBTIGHTDB_COVERAGE)
.PHONY: cover

clean:
	$(RM) *.d *.o *.gcno *.gcda $(TARGETS)
.PHONY: clean



CONFIG_MK = $(ROOT)/config.mk
include $(CONFIG_MK)

$(OBJECTS): Makefile $(CONFIG_MK)



# Code generation
TABLE_MACROS_HPP = table_macros.hpp
$(TABLE_MACROS_HPP): build/table_macros.hpp.sh build/table_macros.hpp.py Makefile $(CONFIG_MK)
	$(SHELL) build/table_macros.hpp.sh $(TABLE_MACROS_HPP)
$(OBJECTS): $(TABLE_MACROS_HPP)


# Library creation

$(LIBTIGHTDB_STATIC): $(LIBTIGHTDB_STATIC_OBJECTS)
	$(AR) $(ARFLAGS) $@ $(LIBTIGHTDB_STATIC_OBJECTS)

$(LIBTIGHTDB_SHARED): $(LIBTIGHTDB_SHARED_OBJECTS)
        # FIXME: add -Wl,-soname and -Wl,-rpath
	$(LD_SHARED) $(LIBTIGHTDB_SHARED_OBJECTS) -o $@

$(LIBTIGHTDB_DEBUG): $(LIBTIGHTDB_DEBUG_OBJECTS)
	$(AR) $(ARFLAGS) $@ $(LIBTIGHTDB_DEBUG_OBJECTS)

$(LIBTIGHTDB_COVERAGE): $(LIBTIGHTDB_COVERAGE_OBJECTS)
	$(AR) $(ARFLAGS) $@ $(LIBTIGHTDB_COVERAGE_OBJECTS)


# Compiling + automatic dependencies

%.o: %.c
	$(CC_STATIC) $(CFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.o: %.cpp
	$(CXX_STATIC) $(CXXFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.dyn.o: %.c
	$(CC_SHARED) $(CFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.dyn.o: %.cpp
	$(CXX_SHARED) $(CXXFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.dbg.o: %.c
	$(CC_DEBUG) $(CFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.dbg.o: %.cpp
	$(CXX_DEBUG) $(CXXFLAGS) $(INC_FLAGS) -MMD -MP -c $< -o $@

%.cov.o: %.c
	$(CC_COVERAGE) $(CFLAGS) $(INC_FLAGS_ABS) -MMD -MP -c $(abspath $<) -o $(abspath $@)

%.cov.o: %.cpp
	$(CXX_COVERAGE) $(CXXFLAGS) $(INC_FLAGS_ABS) -MMD -MP -c $(abspath $<) -o $(abspath $@)

-include $(OBJECTS:.o=.d)
