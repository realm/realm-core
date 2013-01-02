TABLE_MACROS_HPP = table_macros.hpp

GENERATED_SOURCES = $(TABLE_MACROS_HPP)

INST_HEADERS = config.h meta.hpp assert.hpp error.hpp terminate.hpp type_list.hpp tuple.hpp \
safe_int_ops.hpp bind_ptr.hpp string_buffer.hpp file.hpp pthread_helpers.hpp utf8.hpp \
utilities.hpp alloc.hpp alloc_slab.hpp array.hpp array_string.hpp array_string_long.hpp \
array_binary.hpp array_blob.hpp column_type.hpp column_fwd.hpp column.hpp column_tpl.hpp \
column_string.hpp column_string_enum.hpp column_binary.hpp column_table.hpp column_mixed.hpp \
spec.hpp date.hpp binary_data.hpp mixed.hpp table.hpp table_ref.hpp table_basic_fwd.hpp \
table_accessors.hpp table_basic.hpp table_view.hpp table_view_basic.hpp $(TABLE_MACROS_HPP) \
group.hpp group_writer.hpp group_shared.hpp index.hpp index_string.hpp query.hpp \
query_conditions.hpp replication.hpp lang_bind_helper.hpp tightdb_nmmintrin.h

lib_LIBRARIES = libtightdb.a
bin_PROGRAMS  = tightdb-config

libtightdb_a_SOURCES = error.cpp terminate.cpp string_buffer.cpp file.cpp utf8.cpp utilities.cpp \
alloc_slab.cpp array.cpp array_string.cpp array_string_long.cpp array_binary.cpp array_blob.cpp \
column.cpp column_string.cpp column_string_enum.cpp column_binary.cpp column_table.cpp \
column_mixed.cpp spec.cpp table.cpp table_view.cpp group.cpp group_writer.cpp group_shared.cpp \
index.cpp index_string.cpp query.cpp

ifneq ($(TIGHTDB_ENABLE_REPLICATION),)
libtightdb_a_SOURCES += replication.cpp
endif

tightdb_config_SOURCES = config.cpp

include ../../generic.mk

# Code generation
$(TABLE_MACROS_HPP): build/table_macros.hpp.sh build/table_macros.hpp.py
	$(SHELL) build/table_macros.hpp.sh $(TABLE_MACROS_HPP)

uninstall/extra:
	$(RM) $(DESTDIR)$(includedir)/tightdb/*.hpp
	$(RM) $(DESTDIR)$(includedir)/tightdb/*.h
	-rmdir $(DESTDIR)$(includedir)/tightdb
