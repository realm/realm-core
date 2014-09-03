REPLICATION_HPP = $(if $(TIGHTDB_ENABLE_REPLICATION),replication.hpp)
REPLICATION_CPP = $(if $(TIGHTDB_ENABLE_REPLICATION),replication.cpp)

nobase_subinclude_HEADERS = util/features.h util/meta.hpp util/assert.hpp util/terminate.hpp \
util/type_list.hpp util/tuple.hpp util/type_traits.hpp util/bind.hpp util/safe_int_ops.hpp \
util/unique_ptr.hpp util/bind_ptr.hpp util/buffer.hpp util/string_buffer.hpp \
util/memory_stream.hpp util/logger.hpp util/thread.hpp util/file.hpp util/utf8.hpp exceptions.hpp \
utilities.hpp alloc.hpp alloc_slab.hpp array.hpp array_string.hpp data_type.hpp column_type.hpp \
column_fwd.hpp spec.hpp impl/destroy_guard.hpp impl/output_stream.hpp datetime.hpp \
string_data.hpp binary_data.hpp mixed.hpp table.hpp table_ref.hpp table_basic_fwd.hpp \
table_accessors.hpp table_basic.hpp table_view.hpp table_view_basic.hpp table_macros.hpp row.hpp \
descriptor_fwd.hpp descriptor.hpp group.hpp group_shared.hpp $(REPLICATION_HPP) query.hpp \
query_conditions.hpp lang_bind_helper.hpp tightdb_nmmintrin.h importer.hpp version.hpp unicode.hpp \
commit_log.hpp link_view_fwd.hpp link_view.hpp views.hpp

nobase_subinclude_HEADERS_EXTRA_UNINSTALL = *.h *.hpp util/*.h util/*.hpp impl/*.h impl/*.hpp

# Temporary hack due to public dependency on query_engine.hpp in
# Lasses new "query expressions" implementation. None of these should
# really be installed.
nobase_subinclude_HEADERS += array_basic.hpp array_basic_tpl.hpp array_binary.hpp array_blob.hpp \
array_blobs_big.hpp array_string_long.hpp column.hpp column_tpl.hpp column_basic.hpp \
column_basic_tpl.hpp column_binary.hpp column_string_enum.hpp column_string.hpp column_table.hpp \
column_link.hpp column_linklist.hpp column_linkbase.hpp column_backlink.hpp column_mixed.hpp \
column_mixed_tpl.hpp group_writer.hpp index_string.hpp query_engine.hpp query_expression.hpp

lib_LIBRARIES    = libtightdb.a
libexec_PROGRAMS = tightdbd
bin_PROGRAMS     = tightdb-import
DEV_PROGRAMS     = tightdb-config

libtightdb_a_SOURCES = util/terminate.cpp util/string_buffer.cpp util/memory_stream.cpp \
util/thread.cpp util/file.cpp utilities.cpp alloc.cpp alloc_slab.cpp array.cpp \
array_string.cpp array_string_long.cpp array_binary.cpp array_blob.cpp array_blobs_big.cpp \
column.cpp column_string.cpp column_string_enum.cpp column_binary.cpp column_table.cpp \
column_mixed.cpp column_link_base.cpp column_link.cpp column_linklist.cpp column_backlink.cpp \
link_view.cpp spec.cpp impl/output_stream.cpp table.cpp table_view.cpp row.cpp descriptor.cpp \
group.cpp group_writer.cpp group_shared.cpp $(REPLICATION_CPP) index_string.cpp query.cpp \
lang_bind_helper.cpp importer.cpp commit_log.cpp version.cpp unicode.cpp views.cpp

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
libtightdb_a_VERSION = 9:0:0

tightdb_config_SOURCES = config_tool.cpp

tightdbd_SOURCES = tightdbd.cpp
tightdbd_LIBS = libtightdb.a

tightdb_import_SOURCES = importer_tool.cpp
tightdb_import_LIBS    = libtightdb.a

CONFIG_H = util/config.h
CONFIG_SH = $(CONFIG_H:.h=.sh)
EXTRA_CLEAN += $(CONFIG_H)
ifneq ($(TIGHTDB_HAVE_CONFIG),)
nobase_subinclude_HEADERS += $(CONFIG_H)
GENERATED_SOURCES += $(CONFIG_H)
$(CONFIG_H): ../config.mk $(CONFIG_SH)
	$(SHELL) $(CONFIG_SH) $< $@
endif


ifeq ($(DISABLE_CHEETAH_CODE_GEN),)
GENERATED_SOURCES += table_macros.hpp
table_macros.hpp: table_macros.hpp.cheetah cheetah.sh
	$(SHELL) cheetah.sh $< $@
endif


# Used by ../../build.sh
.PHONY: get-inst-headers get-inst-libraries get-inst-programs
get-inst-headers:
	@echo $(nobase_subinclude_HEADERS)
get-inst-libraries:
	@echo $(filter-out libtightdb-cov.% libtightdb-dbg.a,$(TARGETS_LIB_STATIC) $(TARGETS_LIB_SHARED_ALIASES))
get-inst-programs:
	@echo $(filter-out %-cov,$(TARGETS_PROG) $(TARGETS_DEV_PROG))


include ../generic.mk
