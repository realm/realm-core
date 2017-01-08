run: test
	./test

test : cuckoo.o hash.o memory.o table.o tree.o main.o array.o snapshot.o db.o direct_map.o object.o
	g++ -pthread -Wall -pedantic -std=c++14 -ggdb -o test cuckoo.o hash.o memory.o \
	table.o tree.o array.o snapshot.o db.o direct_map.o object.o main.o

%.o : %.cpp *.hpp
	g++ -pthread -Wall -pedantic -std=c++14 -ggdb -O2 -c $<

clean:
	rm *.o
	rm ./test
	rm ./testing.core2

