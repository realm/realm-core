REPLICATION_HPP = $(if $(TIGHTDB_ENABLE_REPLICATION),replication.hpp)
REPLICATION_CPP = $(if $(TIGHTDB_ENABLE_REPLICATION),replication.cpp)

nobase_subinclude_HEADERS = \
	$(REPLICATION_HPP) \
	alloc.hpp \
	alloc_slab.hpp \
	array.hpp \
	array_string.hpp \
	binary_data.hpp \
	column_fwd.hpp \
	column_type.hpp \
	commit_log.hpp \
	data_type.hpp \
	datetime.hpp \
	descriptor.hpp \
	descriptor_fwd.hpp \
	exceptions.hpp \
	group.hpp \
	group_shared.hpp \
	impl/destroy_guard.hpp \
	impl/output_stream.hpp \
	importer.hpp \
	lang_bind_helper.hpp \
	link_view.hpp \
	link_view_fwd.hpp \
	mixed.hpp \
	query.hpp \
	query_conditions.hpp \
	row.hpp \
	spec.hpp \
	string_data.hpp \
	table.hpp \
	table_accessors.hpp \
	table_basic.hpp \
	table_basic_fwd.hpp \
	table_macros.hpp \
	table_ref.hpp \
	table_view.hpp \
	table_view_basic.hpp \
	tightdb_nmmintrin.h \
	unicode.hpp \
	util/assert.hpp \
	util/bind.hpp \
	util/bind_ptr.hpp \
	util/buffer.hpp \
	util/features.h \
	util/file.hpp \
	util/logger.hpp \
	util/memory_stream.hpp \
	util/meta.hpp \
	util/platform_specific_condvar.hpp \
	util/safe_int_ops.hpp \
	util/shared_ptr.hpp \
	util/string_buffer.hpp \
	util/terminate.hpp \
	util/thread.hpp \
	util/tuple.hpp \
	util/type_list.hpp \
	util/type_traits.hpp \
	util/unique_ptr.hpp \
	util/utf8.hpp \
	utilities.hpp \
	version.hpp \
	views.hpp \


nobase_subinclude_HEADERS_EXTRA_UNINSTALL = *.h *.hpp util/*.h util/*.hpp impl/*.h impl/*.hpp

# Temporary hack due to public dependency on query_engine.hpp in
# Lasses new "query expressions" implementation. None of these should
# really be installed.
nobase_subinclude_HEADERS += \
	array_basic.hpp \
	array_basic_tpl.hpp \
	array_binary.hpp \
	array_blob.hpp \
	array_blobs_big.hpp \
	array_string_long.hpp \
	column.hpp \
	column_backlink.hpp \
	column_basic.hpp \
	column_basic_tpl.hpp \
	column_binary.hpp \
	column_link.hpp \
	column_linkbase.hpp \
	column_linklist.hpp \
	column_mixed.hpp \
	column_mixed_tpl.hpp \
	column_string.hpp \
	column_string_enum.hpp \
	column_table.hpp \
	column_tpl.hpp \
	group_writer.hpp \
	index_string.hpp \
	query_engine.hpp \
	query_expression.hpp \


lib_LIBRARIES    = libtightdb.a
libexec_PROGRAMS = tightdbd
bin_PROGRAMS     = tightdb-import
DEV_PROGRAMS     = tightdb-config

libtightdb_a_SOURCES = \
	$(REPLICATION_CPP) \
	alloc.cpp \
	alloc_slab.cpp \
	array.cpp \
	array_binary.cpp \
	array_blob.cpp \
	array_blobs_big.cpp \
	array_string.cpp \
	array_string_long.cpp \
	column.cpp \
	column_backlink.cpp \
	column_binary.cpp \
	column_link.cpp \
	column_link_base.cpp \
	column_linklist.cpp \
	column_mixed.cpp \
	column_string.cpp \
	column_string_enum.cpp \
	column_table.cpp \
	commit_log.cpp \
	descriptor.cpp \
	exceptions.cpp \
	group.cpp \
	group_shared.cpp \
	group_writer.cpp \
	impl/output_stream.cpp \
	importer.cpp \
	index_string.cpp \
	lang_bind_helper.cpp \
	link_view.cpp \
	query.cpp \
	query_engine.cpp \
	row.cpp \
	spec.cpp \
	table.cpp \
	table_view.cpp \
	unicode.cpp \
	util/encrypted_file_mapping.cpp \
	util/errno.cpp \
	util/file.cpp \
	util/file_mapper.cpp \
	util/memory_stream.cpp \
	util/platform_specific_condvar.cpp \
	util/string_buffer.cpp \
	util/terminate.cpp \
	util/thread.cpp \
	utilities.cpp \
	version.cpp \
	views.cpp \


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
