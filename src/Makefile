SUBDIRS = tightdb


all: $(SUBDIRS)
.PHONY: all

install: SUBDIRS_MODE = install
install: all
.PHONY: install

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
