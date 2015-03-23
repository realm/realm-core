#include <iostream>

#include <tightdb.hpp>

#include "../util/timer.hpp"
#include "../util/mem.hpp"
#include "../util/number_names.hpp"

using namespace std;
using namespace realm;

namespace {

// Get and Set are too fast (50ms/M) for normal 64-bit rand*rand*rand*rand*rand (5-10ms/M)
uint64_t rand2()
{
    static int64_t seed = 2862933555777941757ULL;
    static int64_t seed2 = 0;
    seed = (2862933555777941757ULL * seed + 3037000493ULL);
    seed2++;
    return seed * seed2 + seed2;
}

REALM_TABLE_1(IntegerTable,
                first, Int)

REALM_TABLE_1(StringTable,
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

REALM_TABLE_4(TestTable,
                first,  Int,
                second, String,
                third,  Int,
                fourth, Enum<Days>)

} // anonymous namespace


int main()
{
    TestTable table;

    // Build large table
    for (size_t i = 0; i < 250000; ++i) {
        // create random string
        const size_t n = rand() % 1000;// * 10 + rand();
        const string s = test_util::number_name(n);

        table.add(n, s.c_str(), 100, Wed);
    }
    table.add(0, "abcde", 100, Wed);

    cout << "Memory usage: "<<test_util::get_mem_usage()<<" bytes\n";

    test_util::Timer timer;

    // Search small integer column
    {
        timer.reset();

        // Do a search over entire column (value not found)
        for (size_t i = 0; i < 100; ++i) {
            const size_t res = table.column().fourth.find_first(Tue);
            if (res != size_t(-1)) {
                cout << "error\n";
            }
        }

        cout << "Search (small integer): "<<timer<<"\n";
    }

    // Search byte-size integer column
    {
        timer.reset();

        // Do a search over entire column (value not found)
        for (size_t i = 0; i < 100; ++i) {
            const size_t res = table.column().third.find_first(50);
            if (res != size_t(-1)) {
                cout << "error\n";
            }
        }

        cout << "Search (byte-size integer): "<<timer<<"\n";
    }

    // Search string column
    {
        timer.reset();

        // Do a search over entire column (value not found)
        for (size_t i = 0; i < 100; ++i) {
            const size_t res = table.column().second.find_first("abcde");
            if (res != 250000) {
                cerr << "error\n";
            }
        }

        cout << "Search (string): "<<timer<<"\n";
    }

    // Add index
    {
        timer.reset();

        table.column().first.add_search_index();

        cout << "Add index: "<<timer<<"\n";
    }

    cout << "Memory usage2: "<<test_util::get_mem_usage()<<" bytes\n";

    // Search with index
    {
        timer.reset();

        for (size_t i = 0; i < 100000; ++i) {
            const size_t n = rand() % 1000;
            const size_t res = table.column().first.find_first(n);
            if (res == 2500002) { // to avoid above find being optimized away
                cout << "error\n";
            }
        }

        cout << "Search index: "<<timer<<"\n";
    }

#ifdef _MSC_VER
    cin.get();
#endif
}
