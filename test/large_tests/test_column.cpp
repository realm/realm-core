#include "../testsettings.hpp"

#if TEST_DURATION > 0

#include "tightdb/column.hpp"
#include <UnitTest++.h>
#include <vector>
#include <algorithm>
#include "verified_integer.hpp"
#include "tightdb/query_conditions.hpp"

using namespace tightdb;

#define LL_MAX (9223372036854775807LL)
#define LL_MIN (-LL_MAX - 1)


namespace {

uint64_t rand2(int bitwidth = 64)
{
    uint64_t i = (int64_t)rand() * (int64_t)rand() * (int64_t)rand() * (int64_t)rand() * (int64_t)rand();
    if (bitwidth < 64) {
        const uint64_t mask = ((1ULL << bitwidth) - 1ULL);
        i &= mask;
    }
    return i;
}

}

#if 1

TEST(LESS)
{
    // Interesting boundary values to test
    int64_t v[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                     30, 31, 32, 33, 62, 63, 64, 65, 126, 127, 128, 129, 254, 255,
                     256, 257, 32765, 32766, 32767, 32768, 32769, 65533, 65534, 65535, 65536, 65537, 2147483648LL, 
                     2147483647LL, 2147483646LL, 2147483649LL, 4294967296LL, 4294967295LL,
                     4294967297LL, 4294967294LL, 9223372036854775807LL, 9223372036854775806LL, 
                     -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12, -13, -14, -15, -16, -17,
                     -30, -31, -32, -33, -62, -63, -64, -65, -126, -127, -128, -129, -254, -255,
                     -256, -257, -32766, -32767, -32768, -32769, -65535, -65536, -65537, -2147483648LL, 
                     -2147483647LL, -2147483646LL, -2147483649LL, -4294967296LL, -4294967295LL,
                     4294967297LL, -4294967294LL, -9223372036854775807LL, -9223372036854775808LL, -9223372036854775806LL, 
    
    };

    for (size_t w = 5; w < sizeof(v) / sizeof(*v); w++) {
       printf("%d ", w);
        
        const size_t LEN = 64 * 20 + 1000; 
        Array a;
        for (size_t t = 0; t < LEN; t++)
            a.add(v[w]);
        
        // to create at least 64 bytes of data (2 * 128-bit SSE chunks + 64 bit chunk before and after + some unaligned data before and after)
        size_t LEN2 = 64 * 8 / (a.GetBitWidth() == 0 ? 1 : a.GetBitWidth());

        Array akku;
        QueryState state;
        state.state = int64_t(&akku);

        for (size_t from = 0; from < LEN2; from++) {
            for (size_t to = from + 1; to <= LEN2; to++) {
                for (size_t match = (from > 8 ? from - 8 : 0); match < (to > 8 ? to - 8 : 8); match++) { 

                    if (v[w] != LL_MIN) {
                        // LESS
                        a.Set(match, v[w] - 1);
                        size_t f = a.find_first<LESS>(v[w], from, to);
                        a.Set(match, v[w]);
                        if (match >= from && match < to) {
                            TIGHTDB_ASSERT(match == f);
                        }
                        else {
                            TIGHTDB_ASSERT(f == size_t(-1));
                        }
                    }

                    if (v[w] != LL_MAX) {
                        // GREATER
                        a.Set(match, v[w] + 1);
                        size_t f = a.find_first<GREATER>(v[w], from, to);
                        a.Set(match, v[w]);
                        if (match >= from && match < to) {
                            TIGHTDB_ASSERT(match == f);
                        }
                        else {
                            TIGHTDB_ASSERT(f == size_t(-1));
                        }
                    }

                    // FIND
                    a.Set(match, v[w]-1);
                    size_t f = a.find_first(v[w]-1, from, to);
                    a.Set(match, v[w]);
                    if (match >= from && match < to) {
                        TIGHTDB_ASSERT(match == f);
                    }
                    else {
                        TIGHTDB_ASSERT(f == size_t(-1));
                    }

                    if (v[w] != LL_MIN) {
                        // MIN
                        int64_t val = 0;
                        a.Set(match, v[w]-1);
                        bool b = a.minimum(val, from, to);
                        a.Set(match, v[w]);
                        CHECK_EQUAL(true, b);
                        if (match >= from && match < to)
                            TIGHTDB_ASSERT(val == v[w]-1);
                        else
                            TIGHTDB_ASSERT(val == v[w]);
                    }
    
                    // MAX
                    if (v[w] != LL_MAX) {
                        int64_t val = 0;
                        a.Set(match, v[w]+1);
                        bool b = a.maximum(val, from, to);
                        a.Set(match, v[w]);
                        CHECK_EQUAL(true, b);
                        if (match >= from && match < to)
                            TIGHTDB_ASSERT(val == v[w]+1);
                        else
                            TIGHTDB_ASSERT(val == v[w]);
                    }

                    // SUM
                    int64_t val = 0;
                    a.Set(match, v[w]+1);
                    val = a.sum(from, to);
                    a.Set(match, v[w]);
                    int64_t intended;
                    if (match >= from && match < to)
                        intended = (to - from - 1) * v[w] + v[w] + 1;
                    else
                        intended = (to - from) * v[w];      

                    TIGHTDB_ASSERT(intended == val);


                    // Find all, LESS
                    if (v[w] != LL_MIN) {
                        for (size_t off = 1; off < 8; off++) {

                            a.Set(match, v[w] - 1);
                            a.Set(match + off, v[w] - 1);

                            akku.Clear();
                            a.find(COND_LESS, TDB_FINDALL, v[w], from, to, 0, &state);

                            a.Set(match, v[w]);
                            a.Set(match + off, v[w]);

                            if (match >= from && match < to) {
                                TIGHTDB_ASSERT(akku.GetAsSizeT(0) == match);
                            }
                            if (match + off >= from && match + off < to) {
                                TIGHTDB_ASSERT(akku.GetAsSizeT(0) == match + off || akku.GetAsSizeT(1) == match + off);
                            }

                            a.Set(match, v[w]);
                            a.Set(match + off, v[w]);

                        }
                    }


                    // Find all, GREATER
                    if (v[w] != LL_MAX) {
                        for (size_t off = 1; off < 8; off++) {

                            a.Set(match, v[w] + 1);
                            a.Set(match + off, v[w] + 1);

                            akku.Clear();
                            a.find(COND_GREATER, TDB_FINDALL, v[w], from, to, 0, &state);

                            a.Set(match, v[w]);
                            a.Set(match + off, v[w]);

                            if (match >= from && match < to) {
                                TIGHTDB_ASSERT(akku.GetAsSizeT(0) == match);
                            }
                            if (match + off >= from && match + off < to) {
                                TIGHTDB_ASSERT(akku.GetAsSizeT(0) == match + off || akku.GetAsSizeT(1) == match + off);
                            }

                            a.Set(match, v[w]);
                            a.Set(match + off, v[w]);
                        }
                    }


                    // Find all, EQUAL
                    if (v[w] != LL_MAX) {
                        for (size_t off = 1; off < 8; off++) {
                            a.Set(match, v[w] + 1);
                            a.Set(match + off, v[w] + 1);

                            akku.Clear();
                            a.find(COND_EQUAL, TDB_FINDALL, v[w] + 1, from, to, 0, &state);

                            a.Set(match, v[w]);
                            a.Set(match + off, v[w]);

                            if (match >= from && match < to) {
                                TIGHTDB_ASSERT(akku.GetAsSizeT(0) == match);
                            }
                            if (match + off >= from && match + off < to) {
                                TIGHTDB_ASSERT(akku.GetAsSizeT(0) == match + off || akku.GetAsSizeT(1) == match + off);
                            }

                            a.Set(match, v[w]);
                            a.Set(match + off, v[w]);
                        }
                    }
                }
            }    

        }
        a.Destroy();
    }
}


#endif

TEST(Column_monkeytest2)
{
    const uint64_t ITER_PER_BITWIDTH = 16 * 1000 * TEST_DURATION * TEST_DURATION;
    const uint64_t SEED = 123;

    VerifiedInteger a;
    Array res;

    srand(SEED);
    size_t current_bitwidth = 0;
    unsigned int trend = 5;

    for (current_bitwidth = 0; current_bitwidth < 65; current_bitwidth++) {
        for (size_t iter = 0; iter < ITER_PER_BITWIDTH; iter++) {

//          if (rand() % 10 == 0) printf("Input bitwidth around ~%d, , a.Size()=%d\n", (int)current_bitwidth, (int)a.Size());

            if (!(rand2() % (ITER_PER_BITWIDTH / 100))) {
                trend = (unsigned int)rand2() % 10;
                a.find_first(rand2((int)current_bitwidth));
                a.find_all(res, rand2((int)current_bitwidth));
                size_t start = rand2() % (a.Size() + 1);
                a.Sum(start, start + rand2() % (a.Size() + 1 - start));
                a.maximum(start, start + rand2() % (a.Size() + 1 - start));
                a.minimum(start, start + rand2() % (a.Size() + 1 - start));
            }

            if (rand2() % 10 > trend && a.Size() < ITER_PER_BITWIDTH / 100) {
                uint64_t l = rand2((int)current_bitwidth);
                if (rand2() % 2 == 0) {
                    // Insert
                    const size_t pos = rand2() % (a.Size() + 1);
                    a.Insert(pos, l);
                }
                else {
                    // Add
                    a.add(l);
                }
            }
            else if (a.Size() > 0) {
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
