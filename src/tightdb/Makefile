TABLE_MACROS_HPP = table_macros.hpp

GENERATED_SOURCES = $(TABLE_MACROS_HPP)

INST_HEADERS = config.h meta.hpp assert.hpp exceptions.hpp terminate.hpp type_list.hpp tuple.hpp \
bind.hpp safe_int_ops.hpp unique_ptr.hpp bind_ptr.hpp string_buffer.hpp file.hpp thread.hpp \
utf8.hpp utilities.hpp alloc.hpp alloc_slab.hpp array.hpp array_string.hpp data_type.hpp \
column_type.hpp column_fwd.hpp spec.hpp date.hpp string_data.hpp binary_data.hpp mixed.hpp \
table.hpp table_ref.hpp table_basic_fwd.hpp table_accessors.hpp table_basic.hpp table_view.hpp \
table_view_basic.hpp $(TABLE_MACROS_HPP) group.hpp group_shared.hpp query.hpp \
query_conditions.hpp lang_bind_helper.hpp tightdb_nmmintrin.h replication.hpp

lib_LIBRARIES = libtightdb.a
bin_PROGRAMS  = tightdb-config

libtightdb_a_SOURCES = terminate.cpp string_buffer.cpp file.cpp thread.cpp utf8.cpp \
utilities.cpp alloc.cpp alloc_slab.cpp array.cpp array_string.cpp array_string_long.cpp \
array_binary.cpp array_blob.cpp column.cpp column_string.cpp column_string_enum.cpp \
column_binary.cpp column_table.cpp column_mixed.cpp spec.cpp table.cpp table_view.cpp group.cpp \
group_writer.cpp group_shared.cpp index.cpp index_string.cpp query.cpp lang_bind_helper.cpp

ifneq ($(TIGHTDB_ENABLE_REPLICATION),)
libtightdb_a_SOURCES += replication.cpp
endif

# Format: CURRENT[:REVISION[:AGE]]
#
# At each new public release:
#   If the interface has changed at all:
#     Increment CURRENT and reset REVISION to zero
#     Let COMPAT be the least number such that the new library (in
#       its binary form) can be used as a drop-in replacement for
#       all previous releases whose CURRENT is greater than or equal
#       to COMPAT
#     If COMPAT + AGE < CURRENT:
#       Increment AGE
#     Else:
#       Reset AGE to zero
#   Else:
#     Increment REVISION
#
# The meaning of this version string is identical to the one defined
# by GNU Libtool. See also
# http://www.gnu.org/software/libtool/manual/libtool.html#Libtool-versioning
libtightdb_a_VERSION = 2:0:0

tightdb_config_SOURCES = config.cpp

include ../../generic.mk

# Code generation
ifeq ($(DISABLE_UPDATE_TABLE_MACROS_HPP),)
$(TABLE_MACROS_HPP): build/table_macros.hpp.sh build/table_macros.hpp.py
	$(SHELL) build/table_macros.hpp.sh $(TABLE_MACROS_HPP)
endif

uninstall/extra:
	$(RM) $(DESTDIR)$(includedir)/tightdb/*.hpp
	$(RM) $(DESTDIR)$(includedir)/tightdb/*.h
	-rmdir $(DESTDIR)$(includedir)/tightdb

.PHONY: get-inst-headers get-inst-libraries
get-inst-headers:
	@echo $(INST_HEADERS)
get-inst-libraries:
	@echo $(filter-out libtightdb-cov.% libtightdb-dbg.a,$(TARGETS_LIB_STATIC) $(TARGETS_LIB_SHARED_ALIASES))
