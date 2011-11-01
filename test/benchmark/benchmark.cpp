#include <stdio.h>
#include "../../src/tightdb.h"
#include "../../test/UnitTest++/src/UnitTest++.h"
#include "../../test/UnitTest++/src/Win32/TimeHelpers.h"
#include "../Support/mem.h"
#include "../Support/number_names.h"
#include <assert.h>
#include <string>
#include <vector>
#include <algorithm>
#include "../../src/win32/stdint.h"
#include <map>

//using namespace std;


// Get and Set are too fast (50ms/M) for normal 64-bit rand*rand*rand*rand*rand (5-10ms/M)
uint64_t rand2() { 
	static int64_t seed = 2862933555777941757ULL; 
	static int64_t seed2 = 0;
	seed = (2862933555777941757ULL * seed + 3037000493ULL); 
	seed2++;
	return seed * seed2 + seed2; 
}

TDB_TABLE_1(IntegerTable,
			Int,        first)

UnitTest::Timer timer;
int ITEMS = 50000;
int RANGE = 50000;

void tightdb(void);
void stl(void);

volatile uint64_t writethrough;

void main(void) {
	printf("=== TightDB ===\n");
	tightdb();
	printf("Memory usage: %lld bytes\n", (int64_t)GetMemUsage()); // %zu doesn't work in vc
	printf("=== STL ===\n");
	stl();
	getchar();
}


void stl(void) {
	std::vector<uint64_t>integers;

	timer.Start();
	for (size_t i = 0; i < ITEMS; ++i) {
		size_t p = rand2() % (i + 1);
		integers.push_back((int64_t)rand2() % RANGE); 
	}

//	printf("Memory usage: %lld bytes\n", (int64_t)GetMemUsage()); // %zu doesn't work in vc
	printf("Add: %dms\n", timer.GetTimeInMs());


	integers.clear();
	timer.Start();
	for (size_t i = 0; i < ITEMS; ++i) {
		size_t p = rand2() % (i + 1);
		integers.insert(integers.begin() + p, (int64_t)rand2() % RANGE); 
	}
	printf("Insert: %dms\n", timer.GetTimeInMs());

	timer.Start();
	uint64_t dummy = 0;
	for (size_t i = 0; i < ITEMS; ++i) {
		size_t p = rand2() % ITEMS;
		dummy += integers[p];
	}
	writethrough = dummy;
	printf("Get: %dms\n", timer.GetTimeInMs());


	timer.Start();
	for (size_t i = 0; i < ITEMS; ++i) {
		size_t p = rand2() % ITEMS;
		integers[p] = rand2() % RANGE;
	}
	printf("Set: %dms\n", timer.GetTimeInMs());


	uint64_t distance_sum = 0;
	timer.Start();
	for (size_t i = 0; i < ITEMS; ++i) {
		uint64_t f = rand2() % RANGE;
		volatile std::vector<uint64_t>::iterator it = std::find(integers.begin(), integers.end(), f);
/*
		int j;
		for(j = 0; j < integers.size(); j++)
			if(integers[j] == f)
				break;
		distance_sum += j;
*/

	}
//	printf("avg dist=%llu in ", distance_sum / ITEMS);
	printf("Find: %dms\n", timer.GetTimeInMs());


	timer.Start();
	for (size_t i = 0; i < ITEMS; ++i) {
		std::vector<uint64_t>::iterator it = integers.begin();
		while(it != integers.end())
		{
			it = std::find(it + 1, integers.end(), rand2() % RANGE);
		}						

	}
	printf("FindAll: %dms\n", timer.GetTimeInMs());


	timer.Start();
	for (size_t i = 0; i < ITEMS; ++i) {
		size_t p = rand2() % (ITEMS - i);
		integers.erase(integers.begin() + p);
	}
	printf("Delete: %dms\n", timer.GetTimeInMs());
	printf("\n");

	integers.clear();

	// by keeping values in left element we can use 'find' on values like in the other tests
	std::multimap<uint64_t, size_t> ints;

	timer.Start();
	for (size_t i = 0; i < ITEMS; ++i) {
		size_t p = rand2() % (i + 1);
		ints.insert(std::pair<uint64_t, size_t>(i, p));
	}
//	printf("Indexed Memory usage: %lld bytes\n", (int64_t)GetMemUsage()); // %zu doesn't work in vc
	printf("Indexed Add*: %dms\n", timer.GetTimeInMs());

	ints.clear();
	timer.Start();
	for (size_t i = 0; i < ITEMS; ++i) {
		size_t p = rand2() % (i + 1);
		ints.insert(std::pair<uint64_t, size_t>(p, i));
	}
	printf("Indexed Insert*: %dms\n", timer.GetTimeInMs());

	timer.Start();
	for (size_t i = 0; i < ITEMS; ++i) {
		size_t p = rand2() % RANGE;
		std::multimap<uint64_t, size_t>::const_iterator it = ints.find(p);
	}
	printf("Indexed Find: %dms\n", timer.GetTimeInMs());


	timer.Start();
	for (size_t i = 0; i < ITEMS; ++i) {
		size_t p = rand2() % RANGE;

		std::vector<uint64_t> result;
		typedef std::multimap<uint64_t, size_t>::iterator imm;
		std::pair<imm, imm> range = ints.equal_range(p);
//		for (imm i = range.first; i != range.second; ++i)
//			printf("%d %d\n", i->second, i->first); // sanity check
	}
	printf("Indexed FindAll: %dms\n", timer.GetTimeInMs());


}




void tightdb(void) {
	IntegerTable integers;
	volatile uint64_t force;
	int overhead; // Time of computing 1 rand and 1 modulo and doing a loop (is ~0ms with new rand)



	uint64_t dummy = 0;

	for(int index = 0; index < 2; index++)
	{
		std::string indexed;
		integers.Clear();
		if(index == 1)
		{
			integers.SetIndex(0);
			indexed = "Indexed ";
		}

		overhead = 0;
		
		timer.Start();
		for (size_t i = 0; i < ITEMS; ++i) {
			size_t p = rand2() % (i + 1);
			integers.Add((int64_t)rand2() % RANGE); 
		}
//		printf((indexed + "Memory usage: %lld bytes\n").c_str(), (int64_t)GetMemUsage()); // %zu doesn't work in vc
		printf((indexed + "Add: %dms\n").c_str(), timer.GetTimeInMs() - overhead);


		//integers.Clear();
		timer.Start();
		for (size_t i = 0; i < ITEMS; ++i) {
			size_t p = rand2() % (i + 1);
			integers.InsertInt(0, p, (int64_t)rand2() % RANGE); 
			

		}
		printf((indexed + "Insert: %dms\n").c_str(), timer.GetTimeInMs() - overhead);

		timer.Start();
		for (size_t i = 0; i < ITEMS; ++i) {
			size_t p = rand2() % ITEMS;
			dummy += integers.Get64(0, p);
		}
		force = dummy;
		printf((indexed + "Get: %dms\n").c_str(), timer.GetTimeInMs() - overhead);


		timer.Start();
		for (size_t i = 0; i < ITEMS; ++i) {
			size_t p = rand2() % ITEMS;
			integers.Set64(0, p, rand2() % RANGE);
		}
		force = dummy;
		printf((indexed + "Set: %dms\n").c_str(), timer.GetTimeInMs() - overhead);

		uint64_t distance_sum = 0;
		timer.Start();
		for (size_t i = 0; i < ITEMS; ++i) {
			uint64_t f = rand2() % RANGE;
			integers.first.Find(f);
/*
			int j;
			for(j = 0; j < integers.GetSize(); j++)
				if(integers.Get64(0, j) == f)
					break;
			distance_sum += j;
*/
		}
//		printf("avg dist=%llu in ", distance_sum / ITEMS);
		printf((indexed + "Find: %dms\n").c_str(), timer.GetTimeInMs() - overhead);


		timer.Start();
		for (size_t i = 0; i < ITEMS; ++i) {
			integers.first.FindAll(rand2() % RANGE);
		}
		printf((indexed + "FindAll: %dms\n").c_str(), timer.GetTimeInMs() - overhead);


		timer.Start();
		for (size_t i = 0; i < ITEMS; ++i) {
			size_t p = rand2() % (ITEMS - i);
			integers.DeleteRow(p);
		}
		printf((indexed + "Delete: %dms\n").c_str(), timer.GetTimeInMs() - overhead);
		printf("\n");
	}
}
