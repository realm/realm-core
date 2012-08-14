ROOT = ..

SUBDIRS = tightdb

INSTALLED_HEADERS = $(wildcard *.h *.hpp)

CONFIG_MK = $(ROOT)/config.mk
include $(CONFIG_MK)


all: $(SUBDIRS)
.PHONY: all

install: SUBDIRS_MODE = install
install: all
	$(INSTALL_HEADER) $(INSTALLED_HEADERS) $(DESTDIR)$(includedir)/
.PHONY: install

uninstall: SUBDIRS_MODE = uninstall
uninstall: all
	rm $(DESTDIR)$(includedir)/tightdb.hpp
.PHONY: uninstall

shared: SUBDIRS_MODE = shared
shared: all
.PHONY: shared

static: SUBDIRS_MODE = static
static: all
.PHONY: static

debug: SUBDIRS_MODE = debug
debug: all
.PHONY: debug

cover: SUBDIRS_MODE = cover
cover: all
.PHONY: cover

clean: SUBDIRS_MODE = clean
clean: $(SUBDIRS)
.PHONY: clean


$(SUBDIRS):
	@$(MAKE) -C $@ $(SUBDIRS_MODE)
.PHONY: $(SUBDIRS)
