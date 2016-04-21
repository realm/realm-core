TAG=v0.97.3
FULLTAG=tags/$(TAG)
CC=g++
CFLAGS=-Werror -Wextra -Wall -pedantic -std=c++11 \
	-I../core-builds/$(FULLTAG)/include
LDFLAGS= -lrealm \
	-L../core-builds/$(FULLTAG)/lib

OBJECTS=timer.o results.o benchmark.o

all: bench.bin

#	cd .. && ./util/build-core.sh $(FULLTAG)
bench.bin: $(OBJECTS) main.cpp
	$(CC) $(CFLAGS) $? -o $@ $(LDFLAGS)

%.o: %.cpp
	$(CC) $(CFLAGS) -c $? -o $@

clean:
	rm -f *.bin
	rm -f *.o

.PHONY: all clean
