EXAMPLES := $(wildcard *.cpp)

check_PROGRAMS := $(patsubst %.cpp,%,$(EXAMPLES))

PROG_SUFFIX = .example

EXTRA_CLEAN = *.example

define EXAMPLE_PROPS
$(1)_SOURCES = $(2)
$(1)_LIBS    = ../../../src/realm/librealm.a
endef

$(foreach x,$(EXAMPLES),$(eval $(call EXAMPLE_PROPS,$(subst -,_,$(patsubst %.cpp,%,$(x))),$(x))))

include ../../../src/generic.mk
