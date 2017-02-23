CXXFLAGS = -pthread -Wall -pedantic -std=c++14 -ggdb -O3 -MMD

ifeq ($(DEBUG),1)
CXXFLAGS += -O0
endif

SRCS = \
  cuckoo.cpp \
  hash.cpp \
  memory.cpp \
  table.cpp \
  tree.cpp \
  array.cpp \
  snapshot.cpp \
  db.cpp \
  direct_map.cpp \
  object.cpp \

TEST_SRCS = \
  main.cpp

OBJS = $(SRCS:%.cpp=%.o)
DEPS = $(SRCS:%.cpp=%.d)
TEST_OBJS = $(TEST_SRCS:%.cpp=%.o)
TEST_DEPS = $(TEST_SRCS:%.cpp=%.d)

all: test perf lib

run: test
	./test

perf: test_perf
	rm -f perf.core2
	./test_perf

lib: libstorage.a

libstorage.a: $(OBJS)
	ar rcs libstorage.a $(OBJS)

$(OBJS): Makefile

test : main.o libstorage.a
	g++ -pthread main.o -L. -lstorage -o test

test_perf : test_perf.o libstorage.a
	g++ -pthread test_perf.o -L. -lstorage -o test_perf

clean:
	rm -f $(OBJS)
	rm -f $(DEPS)
	rm -f $(TEST_OBJS)
	rm -f $(TEST_DEPS)
	rm -f ./test
	rm -f ./testing.core2

-include $(DEPS)
