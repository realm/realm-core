INCLUDE_ROOT = .
ENABLE_INSTALL_STATIC_LIBS = 1
ENABLE_INSTALL_DEBUG_LIBS  = 1
ENABLE_INSTALL_DEBUG_PROGS = 1

ifeq ($(OS),Darwin)
  CFLAGS_ARCH += -mmacosx-version-min=10.8 -stdlib=libc++ -Wno-nested-anon-types
  VALGRIND_FLAGS += --dsymutil=yes --suppressions=$(GENERIC_MK_DIR)/../test/corefoundation-yosemite.suppress
endif

CFLAGS_DEBUG += -fno-elide-constructors
CFLAGS_PTHREADS += -pthread
CFLAGS_GENERAL += -Wextra -pedantic -Wundef -Wshadow
CFLAGS_CXX = -std=c++14

# Avoid a warning from Clang when linking on OS X. By default,
# `LDFLAGS_PTHREADS` inherits its value from `CFLAGS_PTHREADS`, so we
# have to override that with an empty value.
ifeq ($(OS),Darwin)
  ifeq ($(LD_IS),clang)
    LDFLAGS_PTHREADS = $(EMPTY)
  endif
endif

# Clang specific flags
ifeq ($(COMPILER_IS),clang)
# While -Wunreachable-code is accepted by GCC, it is ignored and will be removed
# in the future.
  CFLAGS_GENERAL += -Wunreachable-code
  CFLAGS_GENERAL += -Wshorten-64-to-32
  CFLAGS_GENERAL += -Wold-style-cast
  CFLAGS_GENERAL += -Wconditional-uninitialized
  CFLAGS_GENERAL += -Wextra-semi
endif

# CoreFoundation is required for logging
ifeq ($(OS),Darwin)
  PROJECT_LDFLAGS += -framework CoreFoundation
endif

# Android logging
ifeq ($(REALM_ANDROID),)
  PROJECTS_LDFLAGS += -llog
endif

# Note: While CFLAGS (those specified above) can be overwritten by
# setting the CFLAGS variable on the command line, PROJECT_CFLAGS are
# retained.

ifneq ($(REALM_HAVE_CONFIG),)
  PROJECT_CFLAGS += -DREALM_HAVE_CONFIG
endif

PROJECT_CFLAGS_DEBUG = -DREALM_DEBUG
PROJECT_CFLAGS_COVER = -DREALM_DEBUG -DREALM_COVER \
                       -fno-elide-constructors

ifeq ($(COMPILER_IS),gcc)
  PROJECT_CFLAGS_COVER += -fno-inline \
                          -fno-inline-small-functions \
                          -fno-default-inline
endif

# Load dynamic configuration
ifneq ($(REALM_HAVE_CONFIG),)
  CONFIG_MK = $(GENERIC_MK_DIR)/config.mk
  DEP_MAKEFILES += $(CONFIG_MK)
  include $(CONFIG_MK)
  prefix      = $(INSTALL_PREFIX)
  exec_prefix = $(INSTALL_EXEC_PREFIX)
  includedir  = $(INSTALL_INCLUDEDIR)
  bindir      = $(INSTALL_BINDIR)
  libdir      = $(INSTALL_LIBDIR)
  libexecdir  = $(INSTALL_LIBEXECDIR)
  ifeq ($(ENABLE_ENCRYPTION),yes)
    ifeq ($(OS),Linux)
      PROJECT_LDFLAGS += -lcrypto
    endif
  endif
endif

ifneq ($(REALM_ANDROID),)
  PROJECT_CFLAGS += -fPIC -DPIC -Wno-maybe-uninitialized
  # android.toolchain.cmake has `-fsigned-char` by default, we have to use the same
  # to avoid lto linking problems.
  CFLAGS_OPTIM = -Os -ffunction-sections -fdata-sections -DNDEBUG -fsigned-char -fvisibility=hidden
  ifeq ($(ENABLE_ENCRYPTION),yes)
    PROJECT_CFLAGS += -I../../openssl/include
  endif
endif
