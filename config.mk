SOURCE_ROOT = src
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

ifneq ($(TIGHTDB_ENABLE_REPLICATION),)
PROJECT_CFLAGS += -DTIGHTDB_ENABLE_REPLICATION
endif

#PROJECT_CFLAGS_DEBUG = -DTIGHTDB_DEBUG -DTIGHTDB_MAX_LIST_SIZE=4
PROJECT_CFLAGS_DEBUG = -DTIGHTDB_DEBUG
PROJECT_CFLAGS_COVER = -DTIGHTDB_DEBUG -DTIGHTDB_MAX_LIST_SIZE=4
