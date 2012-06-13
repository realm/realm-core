#include "../testsettings.hpp"

#if TEST_DURATION > 0

#include "column.hpp"
#include <UnitTest++.h>
#include <vector>
#include <algorithm>
#include "verified_integer.hpp"
#include "query_conditions.hpp"

using namespace tightdb;

namespace {

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

TEST(LESS)
{
    const size_t LEN = 300;
    Array a;
    for(size_t t = 0; t < LEN; t++)
        a.add(100);
    
    a.Set(132, 50);
    size_t f = a.Query<LESS>(100, 0, 137);


    for(size_t from = 0; from < LEN; from++) {
        for(size_t to = from + 1; to <= LEN; to++) {
            for(size_t match = 0; match < LEN; match++) { 
                a.Set(match, 50);
                size_t f = a.Query<LESS>(100, from, to);
                a.Set(match, 100);
                if(match >= from && match < to) {
                    CHECK_EQUAL(match, f);
                    assert(match == f);
                }
                else {
                    CHECK_EQUAL(f, -1);
                    assert(f == -1);
                }
            }
        }    

    }
}



TEST(Find1)
{
    const size_t LEN = 300;
    Array a;
    for(size_t t = 0; t < LEN; t++)
        a.add(100);
    
    for(size_t from = 0; from < LEN; from++) {
        for(size_t to = from + 1; to <= LEN; to++) {
            for(size_t match = 0; match < LEN; match++) { 
                a.Set(match, 200);
                size_t f = a.find_first(200, from, to);
                a.Set(match, 100);
                if(match >= from && match < to) {
                    CHECK_EQUAL(match, f);
                    assert(match == f);
                }
                else {
                    CHECK_EQUAL(f, -1);
                    assert(f == -1);
                }
            }
        }    

    }
}


TEST(MinMax)
{
    const size_t LEN = 300;
    Array a;
    for(size_t t = 0; t < LEN; t++)
        a.add(100);
    
    for(size_t from = 0; from < LEN; from++) {
        for(size_t to = from + 1; to <= LEN; to++) {
            for(size_t match = 0; match < LEN; match++) {
                int64_t val = 0;
                a.Set(match, 200);
                bool b = a.maximum(val, from, to);
                a.Set(match, 100);
                CHECK_EQUAL(true, b);
                if(match >= from && match < to)
                    assert(val == 200);
                else
                    assert(val == 100);
            }
        }    

    }
}


TEST(Column_monkeytest2)
{
    const uint64_t ITER_PER_BITWIDTH = 16 * 1000 * TEST_DURATION * TEST_DURATION * TEST_DURATION;
    const uint64_t SEED = 123;

    VerifiedInteger a;
    Array res;

    srand(SEED);
    size_t current_bitwidth = 0;
    unsigned int trend = 5;

    for(current_bitwidth = 0; current_bitwidth < 65; current_bitwidth++) {
        for(size_t iter = 0; iter < ITER_PER_BITWIDTH; iter++) {

//          if(rand() % 10 == 0) printf("Input bitwidth around ~%d, , a.Size()=%d\n", (int)current_bitwidth, (int)a.Size());

            if (!(rand2() % (ITER_PER_BITWIDTH / 100))) {
                trend = (unsigned int)rand2() % 10;
                a.find_first(rand2(current_bitwidth));
                a.find_all(res, rand2(current_bitwidth));
                size_t start = rand2() % (a.Size() + 1);
                a.Sum(start, start + rand2() % (a.Size() + 1 - start));
                a.maximum(start, start + rand2() % (a.Size() + 1 - start));
                a.minimum(start, start + rand2() % (a.Size() + 1 - start));
            }

            if (rand2() % 10 > trend && a.Size() < ITER_PER_BITWIDTH / 100) {
                uint64_t l = rand2(current_bitwidth);
                if(rand2() % 2 == 0) {
                    // Insert
                    const size_t pos = rand2() % (a.Size() + 1);
                    a.Insert(pos, l);
                }
                else {
                    // Add
                    a.add(l);
                }
            }
            else if(a.Size() > 0) {
                // Delete
                const size_t i = rand2() % a.Size();
                a.Delete(i);
            }
        }
    }

    // Cleanup
    a.Destroy();
    res.Destroy();
}

#endif
