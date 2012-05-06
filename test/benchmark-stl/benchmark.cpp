#include <stdio.h>
#include "../../include/tightdb.hpp"
#include "../../test/UnitTest++/src/UnitTest++.h"
#include "../../test/UnitTest++/src/Win32/TimeHelpers.h"
#include "../Support/mem.hpp"
#include "../Support/number_names.hpp"
#include <assert.h>
#include <string>
#include <vector>
#include <algorithm>
#include "../../src/win32/stdint.h"
#include <map>

using namespace tightdb;




// Get and Set are too fast (50ms/M) for normal 64-bit rand*rand*rand*rand*rand (5-10ms/M)
uint64_t rand2()
{
    static int64_t seed = 2862933555777941757ULL;
    static int64_t seed2 = 0;
    seed = (2862933555777941757ULL * seed + 3037000493ULL);
    seed2++;
    return seed * seed2 + seed2;
}

TIGHTDB_TABLE_1(IntegerTable,
                first, Int)

UnitTest::Timer timer;
int ITEMS = 50000;
int RANGE = 50000;

//void tightdb();
void stl();

volatile uint64_t writethrough;

int main()
{
    stl();
//    getchar();
}


void stl()
{
    std::vector<uint64_t>integers;

    timer.Start();
    for (size_t i = 0; i < ITEMS; ++i) {
        size_t p = rand2() % (i + 1);
        integers.push_back((int64_t)rand2() % RANGE);
    }

//  printf("Memory usage: %lld bytes\n", (int64_t)GetMemUsage()); // %zu doesn't work in vc
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
//  printf("avg dist=%llu in ", distance_sum / ITEMS);
    printf("Linear Find: %dms\n", timer.GetTimeInMs());


    timer.Start();
    for (size_t i = 0; i < ITEMS; ++i) {
        std::vector<uint64_t>::iterator it = integers.begin();
        uint64_t f = rand2() % RANGE;
        while(it != integers.end())
        {
            it = std::find(it + 1, integers.end(), f);
        }

    }
    printf("Linear FindAll: %dms\n", timer.GetTimeInMs());


    timer.Start();
    for (size_t i = 0; i < ITEMS; ++i) {
        size_t p = rand2() % (ITEMS - i);
        integers.remove(integers.begin() + p);
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
//  printf("Indexed Memory usage: %lld bytes\n", (int64_t)GetMemUsage()); // %zu doesn't work in vc
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
//      for (imm i = range.first; i != range.second; ++i)
//          printf("%d %d\n", i->second, i->first); // sanity check
    }
    printf("Indexed FindAll: %dms\n", timer.GetTimeInMs());
}
