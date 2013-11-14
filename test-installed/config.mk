ifeq ($(OS),Darwin)
CFLAGS_ARCH += -mmacosx-version-min=10.7
endif

CFLAGS_PTHREAD += -pthread
CFLAGS_GENERAL += -Wextra -ansi -pedantic -Wno-long-long
