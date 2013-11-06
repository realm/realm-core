SOURCE_ROOT = .
ENABLE_INSTALL_STATIC_LIBS = 1
ENABLE_INSTALL_DEBUG_LIBS  = 1
ENABLE_INSTALL_DEBUG_PROGS = 1

# Construct fat binaries on Darwin when using Clang
ifneq ($(TIGHTDB_ENABLE_FAT_BINARIES),)
ifneq ($(call CC_CXX_AND_LD_ARE,clang),)
ifeq ($(shell uname),Darwin)
CFLAGS_ARCH += -arch i386 -arch x86_64
endif
endif
endif

# FIXME: '-fno-elide-constructors' currently causes TightDB to fail
#CFLAGS_DEBUG   += -fno-elide-constructors
CFLAGS_PTHREAD += -pthread
CFLAGS_GENERAL += -Wextra -ansi -pedantic -Wno-long-long

# Note: While CFLAGS (those specified above) can be overwritten by
# setting the CFLAGS variable on the command line, PROJECT_CFLAGS are
# retained.

ifneq ($(TIGHTDB_HAVE_CONFIG),)
PROJECT_CFLAGS += -DTIGHTDB_HAVE_CONFIG
endif
ifneq ($(TIGHTDB_ENABLE_REPLICATION),)
PROJECT_CFLAGS += -DTIGHTDB_ENABLE_REPLICATION
endif

PROJECT_CFLAGS_DEBUG = -DTIGHTDB_DEBUG
PROJECT_CFLAGS_COVER = -DTIGHTDB_DEBUG -DTIGHTDB_COVER

# Load dynamic configuration
ifneq ($(TIGHTDB_HAVE_CONFIG),)
CONFIG_DYN_MK = $(GENERIC_MK_DIR)/config-dyn.mk
DEP_MAKEFILES += $(CONFIG_DYN_MK)
include $(CONFIG_DYN_MK)
prefix      = $(INSTALL_PREFIX)
exec_prefix = $(INSTALL_EXEC_PREFIX)
includedir  = $(INSTALL_INCLUDEDIR)
bindir      = $(INSTALL_BINDIR)
libdir      = $(INSTALL_LIBDIR)
libexecdir  = $(INSTALL_LIBEXECDIR)
endif
