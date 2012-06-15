#include "../testsettings.hpp"

#if TEST_DURATION > 0

#include "tightdb/column.hpp"
#include <UnitTest++.h>
#include <vector>
#include <algorithm>
#include "verified_integer.hpp"
#include "tightdb/query_conditions.hpp"

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
    int64_t v[13] = {0, 1, 3, 15, 100, 30000, 1000000LL, 1000LL*1000LL*1000LL*1000LL, -15, -100, -30000, -1000000ULL, -1000ULL*1000LL*1000LL*1000LL};

    for (size_t w = 0; w < 13; w++) {
        const size_t LEN = 64 * 8; // to create at least 64 bytes of data (2 * 128-bit SSE chunks + 64 bit chunk before and after + some unaligned data before and after)
        Array a;
        for(size_t t = 0; t < LEN; t++)
            a.add(v[w]);

        size_t LEN2 = 64 * 8 / (a.GetBitWidth() == 0 ? 1 : a.GetBitWidth());

        for(size_t from = 0; from < LEN2; from++) {
            for(size_t to = from + 1; to <= LEN2; to++) {
                for(size_t match = 0; match < LEN2; match++) { 

                    // LESS
                    a.Set(match, v[w] - 1);
                    size_t f = a.Query<LESS>(v[w], from, to);
                    a.Set(match, v[w]);
                    if(match >= from && match < to) {
                        assert(match == f);
                    }
                    else {
                        assert(f == -1);
                    }

                    if(to == 9 && match == 1)
                        printf("");

                    // FIND
                    a.Set(match, v[w]-1);
                    f = a.find_first(v[w]-1, from, to);
                    a.Set(match, v[w]);
                    if(match >= from && match < to) {
                        assert(match == f);
                    }
                    else {
                        assert(f == -1);
                    }

                    // MIN
                    int64_t val = 0;
                    a.Set(match, v[w]-1);
                    bool b = a.minimum(val, from, to);
                    a.Set(match, v[w]);
                    CHECK_EQUAL(true, b);
                    if(match >= from && match < to)
                        assert(val == v[w]-1);
                    else
                        assert(val == v[w]);


                    // MAX
                    a.Set(match, v[w]+1);
                    b = a.maximum(val, from, to);
                    a.Set(match, v[w]);
                    CHECK_EQUAL(true, b);
                    if(match >= from && match < to)
                        assert(val == v[w]+1);
                    else
                        assert(val == v[w]);

                }
            }    

        }
        a.Destroy();
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
