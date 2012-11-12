INST_HEADERS = $(wildcard *.h *.hpp)

TABLE_MACROS_HPP = table_macros.hpp

GENERATED_SOURCES = $(TABLE_MACROS_HPP)

lib_LIBRARIES = libtightdb.a
bin_PROGRAMS  = tightdb-config

libtightdb_a_SOURCES = error.cpp terminate.cpp string_buffer.cpp file.cpp utf8.cpp utilities.cpp \
alloc_slab.cpp array.cpp array_string.cpp array_string_long.cpp array_binary.cpp array_blob.cpp \
column.cpp column_string.cpp column_string_enum.cpp column_binary.cpp column_table.cpp \
column_mixed.cpp spec.cpp table.cpp table_view.cpp group.cpp group_writer.cpp group_shared.cpp \
index.cpp index_string.cpp query.cpp c-table.cpp

ifneq ($(TIGHTDB_ENABLE_REPLICATION),)
libtightdb_a_SOURCES += replication.cpp
endif

tightdb_config_SOURCES = config.cpp

include ../../generic.mk

# Code generation
$(TABLE_MACROS_HPP): build/table_macros.hpp.sh build/table_macros.hpp.py Makefile
	$(SHELL) build/table_macros.hpp.sh $(TABLE_MACROS_HPP)

uninstall/extra:
	$(RM) $(includedir)/tightdb/*.hpp
	$(RM) $(includedir)/tightdb/*.h
	-rmdir $(includedir)/tightdb
