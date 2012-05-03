
#include <UnitTest++.h>
#include "../testsettings.hpp"
#include "../Support/number_names.hpp"
#include "column.hpp"

#include "verified_string.hpp"

#if TEST_DURATION > 0

using namespace tightdb;

string randstring(void);

namespace {

// Support functions for monkey test
uint64_t rand2(int bitwidth = 64)
{
    uint64_t i = (int64_t)rand() * (int64_t)rand() * (int64_t)rand() * (int64_t)rand() * (int64_t)rand();
    if(bitwidth < 64) {
        const uint64_t mask = ((1ULL << bitwidth) - 1ULL);
        i &= mask;
    }
    return i;
}

}

string randstring(void)
{
    // If there are in the order of MAX_LIST_SIZE different strings, then we'll get a good
    // distribution btw. arrays with no matches and arrays with multiple matches, when
    // testing Find/FindAll
    int64_t t = (rand() % 100) * 100;
    size_t len = (rand() % 10) * 100 + 1;
    string s;
    while(s.length() < len)
        s += number_name(t);

    s = s.substr(0, len);
    return s;
}

TEST(ColumnString_monkeytest2)
{
    const uint64_t ITER = 16 * 5000 * TEST_DURATION * TEST_DURATION * TEST_DURATION;
    const uint64_t SEED = 123;

    VerifiedString a;
    Array res;

    srand(SEED);
    unsigned int trend = 5;

    for(size_t iter = 0; iter < ITER; iter++) {

//          if(rand() % 10 == 0) printf("Input bitwidth around ~%d, , a.Size()=%d\n", (int)current_bitwidth, (int)a.Size());

        if (!(rand2() % (ITER / 100))) {
            trend = (unsigned int)rand2() % 10;

            a.Find(randstring().c_str());
            a.FindAll(res, randstring().c_str());
        }

        if (rand2() % 10 > trend && a.Size() < ITER / 100) {
            if(rand2() % 2 == 0) {
                // Insert
                const size_t pos = rand2() % (a.Size() + 1);
                a.Insert(pos, randstring().c_str());
            }
            else {
                // Add
                a.add(randstring().c_str());
            }
        }
        else if(a.Size() > 0) {
            // Delete
            const size_t i = rand2() % a.Size();
            a.Delete(i);
        }
    }

    // Cleanup
    a.Destroy();
    res.Destroy();
}

#endif
