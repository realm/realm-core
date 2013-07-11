EXAMPLES = $(wildcard *.cpp)

TEST_PROGRAMS = $(patsubst %.cpp,%,$(EXAMPLES))

define EXAMPLE_PROPS
$(1)_SOURCES = $(2)
$(1)_LIBS    = ../../../src/tightdb/libtightdb.a
endef

$(foreach x,$(EXAMPLES),$(eval $(call EXAMPLE_PROPS,$(subst -,_,$(patsubst %.cpp,%,$(x))),$(x))))

include ../../../generic.mk
