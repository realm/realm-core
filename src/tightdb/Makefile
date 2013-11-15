REPLICATION_HPP = $(if $(ENABLE_REPLICATION),replication.hpp)
REPLICATION_CPP = $(if $(ENABLE_REPLICATION),replication.cpp)

INST_HEADERS = config.h meta.hpp assert.hpp exceptions.hpp terminate.hpp type_list.hpp tuple.hpp \
bind.hpp safe_int_ops.hpp unique_ptr.hpp bind_ptr.hpp buffer.hpp string_buffer.hpp file.hpp \
thread.hpp utf8.hpp utilities.hpp alloc.hpp alloc_slab.hpp array.hpp array_string.hpp \
data_type.hpp column_type.hpp column_fwd.hpp spec.hpp datetime.hpp string_data.hpp binary_data.hpp \
mixed.hpp table.hpp table_ref.hpp table_basic_fwd.hpp table_accessors.hpp table_basic.hpp \
table_view.hpp table_view_basic.hpp table_macros.hpp group.hpp group_shared.hpp $(REPLICATION_HPP) \
query.hpp query_conditions.hpp lang_bind_helper.hpp tightdb_nmmintrin.h importer.hpp

# Temporary hack due to public dependency on query_engine.hpp in
# Lasses new "query expressions" implementation. None of these should
# really be installed.
INST_HEADERS += array_basic.hpp array_basic_tpl.hpp array_binary.hpp array_blob.hpp \
array_blobs_big.hpp array_string_long.hpp column_basic.hpp column_basic_tpl.hpp column_binary.hpp \
column.hpp column_mixed.hpp column_mixed_tpl.hpp column_string_enum.hpp column_string.hpp \
column_table.hpp column_tpl.hpp group_writer.hpp index_string.hpp query_engine.hpp \
query_expression.hpp file_interprocess_managed.hpp

lib_LIBRARIES    = libtightdb.a
libexec_PROGRAMS = tightdbd
bin_PROGRAMS     = tightdb-import
DEV_PROGRAMS     = tightdb-config

libtightdb_a_SOURCES = terminate.cpp string_buffer.cpp file.cpp thread.cpp utf8.cpp \
utilities.cpp alloc.cpp alloc_slab.cpp array.cpp array_string.cpp array_string_long.cpp \
array_binary.cpp array_blob.cpp array_blobs_big.cpp column.cpp column_string.cpp \
column_string_enum.cpp column_binary.cpp column_table.cpp column_mixed.cpp spec.cpp table.cpp \
table_view.cpp group.cpp group_writer.cpp group_shared.cpp $(REPLICATION_CPP) index_string.cpp \
query.cpp lang_bind_helper.cpp importer.cpp file_interprocess_managed.cpp

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
libtightdb_a_VERSION = 4:0:0

tightdb_config_SOURCES = config_tool.cpp

tightdbd_SOURCES = tightdbd.cpp
tightdbd_LIBS = libtightdb.a

tightdb_import_SOURCES = importer_tool.cpp
tightdb_import_LIBS    = libtightdb.a

ifneq ($(TIGHTDB_HAVE_CONFIG),)
INST_HEADERS += build_config.h
GENERATED_SOURCES += build_config.h
endif

ifeq ($(DISABLE_CHEETAH_CODE_GEN),)
GENERATED_SOURCES += table_macros.hpp
endif


include ../generic.mk


# Code generation

ifneq ($(TIGHTDB_HAVE_CONFIG),)
build_config.h: ../config-dyn.mk config.sh
	$(SHELL) config.sh $< $@
endif

ifeq ($(DISABLE_CHEETAH_CODE_GEN),)
table_macros.hpp: table_macros.hpp.cheetah cheetah.sh
	$(SHELL) cheetah.sh $< $@
endif


clean/extra:
	$(RM) build_config.h

uninstall/extra:
	$(RM) $(DESTDIR)$(includedir)/tightdb/*.hpp
	$(RM) $(DESTDIR)$(includedir)/tightdb/*.h
	-rmdir $(DESTDIR)$(includedir)/tightdb


# Used by ../../build.sh
.PHONY: get-inst-headers get-inst-libraries get-inst-programs
get-inst-headers:
	@echo $(INST_HEADERS)
get-inst-libraries:
	@echo $(filter-out libtightdb-cov.% libtightdb-dbg.a,$(TARGETS_LIB_STATIC) $(TARGETS_LIB_SHARED_ALIASES))
get-inst-programs:
	@echo $(filter-out %-cov,$(TARGETS_PROG) $(TARGETS_DEV_PROG))
