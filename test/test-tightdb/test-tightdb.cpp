#include <stdio.h>
#include <assert.h>
#include <UnitTest++.h>
#include <tightdb/table_macros.hpp>
#include "../Support/mem.hpp"
#include "../Support/number_names.hpp"

using namespace std;
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

TIGHTDB_TABLE_1(StringTable,
                first, String)

enum Days {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun
};

TIGHTDB_TABLE_4(TestTable,
                first,  Int,
                second, String,
                third,  Int,
                fourth, Enum<Days>)

int main()
{
    TestTable table;

    // Build large table
    for (size_t i = 0; i < 250000; ++i) {
        // create random string
        const size_t n = rand() % 1000;// * 10 + rand();
        const string s = number_name(n);

        table.add(n, s.c_str(), 100, Wed);
    }
    table.add(0, "abcde", 100, Wed);

    printf("Memory usage: %lld bytes\n", (long long)GetMemUsage()); // %zu doesn't work in vc

    UnitTest::Timer timer;

    // Search small integer column
    {
        timer.Start();

        // Do a search over entire column (value not found)
        for (size_t i = 0; i < 100; ++i) {
            const size_t res = table.cols().fourth.find_first(Tue);
            if (res != size_t(-1)) {
                printf("error");
            }
        }

        const int search_time = timer.GetTimeInMs();
        printf("Search (small integer): %dms\n", search_time);
    }

    // Search byte-size integer column
    {
        timer.Start();

        // Do a search over entire column (value not found)
        for (size_t i = 0; i < 100; ++i) {
            const size_t res = table.cols().third.find_first(50);
            if (res != size_t(-1)) {
                printf("error");
            }
        }

        const int search_time = timer.GetTimeInMs();
        printf("Search (byte-size integer): %dms\n", search_time);
    }

    // Search string column
    {
        timer.Start();

        // Do a search over entire column (value not found)
        for (size_t i = 0; i < 100; ++i) {
            const size_t res = table.cols().second.find_first("abcde");
            if (res != 250000) {
                printf("error");
            }
        }

        const int search_time = timer.GetTimeInMs();
        printf("Search (string): %dms\n", search_time);
    }

    // Add index
    {
        timer.Start();

        table.cols().first.set_index();

        const int search_time = timer.GetTimeInMs();
        printf("Add index: %dms\n", search_time);
    }

    printf("Memory usage2: %lld bytes\n", (long long)GetMemUsage()); // %zu doesn't work in vc

    // Search with index
    {
        timer.Start();

        for (size_t i = 0; i < 100000; ++i) {
            const size_t n = rand() % 1000;
            const size_t res = table.cols().first.find_first(n);
            if (res == 2500002) { // to avoid above find being optimized away
                printf("error");
            }
        }

        const int search_time = timer.GetTimeInMs();
        printf("Search index: %dms\n", search_time);
    }
#ifdef _MSC_VER
    getchar();
#endif
    return 0;
}
