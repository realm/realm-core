CXXFLAGS = -pthread -Wall -pedantic -std=c++14 -ggdb -O2 -MMD

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
  main.cpp

OBJS = $(SRCS:%.cpp=%.o)
DEPS = $(SRCS:%.cpp=%.d)

all: test

run: test
	./test

$(OBJS): Makefile

test : $(OBJS)
	g++ $(CXXFLAGS) -o $@ $^

clean:
	rm -f $(OBJS)
	rm -f $(DEPS)
	rm -f ./test
	rm -f ./testing.core2

-include $(DEPS)
