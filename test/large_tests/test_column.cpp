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
<<<<<<< HEAD
<<<<<<< HEAD
    int64_t v[13] = {0, 1, 3, 15, 100, 30000, 1000000LL, 1000LL*1000LL*1000LL*1000LL, -15, -100, -30000, -1000000ULL, -1000ULL*1000LL*1000LL*1000LL};

    for (size_t w = 0; w < 13; w++) {
        const size_t LEN = 64 * 8; // to create at least 64 bytes of data (2 * 128-bit SSE chunks + 64 bit chunk before and after + some unaligned data before and after)
=======
    // Interesting boundary values to test
    int64_t v[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                     30, 31, 32, 33, 62, 63, 64, 65, 126, 127, 128, 129, 254, 255,
                     256, 257, 32767, 32768, 32769, 65535, 65536, 65537, 2147483648LL, 
                     2147483647LL, 2147483646LL, 2147483649LL, 4294967296LL, 4294967295LL,
                     4294967297LL, 4294967294LL, 9223372036854775807LL, 9223372036854775806LL, 
                     -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12, -13, -14, -15, -16, -17,
                     -30, -31, -32, -33, -62, -63, -64, -65, -126, -127, -128, -129, -254, -255,
                     -256, -257, -32767, -32768, -32769, -65535, -65536, -65537, -2147483648LL, 
                     -2147483647LL, -2147483646LL, -2147483649LL, -4294967296LL, -4294967295LL,
                     4294967297LL, -4294967294LL, -9223372036854775807LL, -9223372036854775808LL, -9223372036854775806LL, 
    
    };

    for (size_t w = 0; w < sizeof(v) / sizeof(*v); w++) {
        cout << w << " ";
        
        const size_t LEN = 64 * 20 + 1000; 
>>>>>>> 031050b6c43f2f69d4d823e9a45a9b3b1f320d33
        Array a;
        for(size_t t = 0; t < LEN; t++)
            a.add(v[w]);
        
        // to create at least 64 bytes of data (2 * 128-bit SSE chunks + 64 bit chunk before and after + some unaligned data before and after)
        size_t LEN2 = 64 * 8 / (a.GetBitWidth() == 0 ? 1 : a.GetBitWidth());

        for(size_t from = 0; from < LEN2; from++) {
            for(size_t to = from + 1; to <= LEN2; to++) {
                for(size_t match = (from > 8 ? from - 8 : 0); match < (to > 8 ? to - 8 : 8); match++) { 

                    if(v[w] != -9223372036854775808LL) {
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
                    }

                    if(v[w] != 9223372036854775807LL) {
                        // GREATER
                        a.Set(match, v[w] + 1);
                        size_t f = a.Query<GREATER>(v[w], from, to);
                        a.Set(match, v[w]);
                        if(match >= from && match < to) {
                            assert(match == f);
                        }
                        else {
                            assert(f == -1);
                        }
                    }

                    // FIND
                    a.Set(match, v[w]-1);
                    size_t f = a.find_first(v[w]-1, from, to);
                    a.Set(match, v[w]);
                    if(match >= from && match < to) {
                        assert(match == f);
                    }
                    else {
                        assert(f == -1);
                    }

                    if(v[w] != -9223372036854775808LL) {
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
                    }
    
                    // MAX
                    if(v[w] != 9223372036854775807LL) {
                        int64_t val = 0;
                        a.Set(match, v[w]+1);
                        bool b = a.maximum(val, from, to);
                        a.Set(match, v[w]);
                        CHECK_EQUAL(true, b);
                        if(match >= from && match < to)
                            assert(val == v[w]+1);
                        else
                            assert(val == v[w]);
                    }

                    // SUM
                    int64_t val = 0;
                    a.Set(match, v[w]+1);
                    val = a.sum(from, to);
                    a.Set(match, v[w]);
                    int64_t intended;
                    if(match >= from && match < to)
                        intended = (to - from - 1) * v[w] + v[w] + 1;
                    else
                        intended = (to - from) * v[w];                   
                    assert(intended == val);

enum {COND_EQUAL, COND_NOTEQUAL, COND_GREATER, COND_LESS};

                    // Find all, LESS
                    if(v[w] != -9223372036854775808LL) {
                        for(size_t off = 1; off < 8; off++) {
                            Array akku;

                            a.Set(match, v[w] - 1);
                            a.Set(match + off, v[w] - 1);

                            a.find_all(COND_LESS, &akku, v[w], 0, from, to);

                            a.Set(match, v[w]);
                            a.Set(match + off, v[w]);

                            if(match >= from && match < to) {
                                assert(akku.Get(0) == match);
                            }
                            if(match + off >= from && match + off < to) {
                                assert(akku.Get(0) == match + off || akku.Get(1) == match + off);
                            }

                            a.Set(match, v[w]);
                            a.Set(match + off, v[w]);
                            akku.Destroy();

                        }
                    }


                                        // Find all, GREATER
                    if(v[w] != 9223372036854775807LL) {
                        for(size_t off = 1; off < 8; off++) {
                            Array akku;

                            a.Set(match, v[w] + 1);
                            a.Set(match + off, v[w] + 1);

                            a.find_all(COND_GREATER, &akku, v[w], 0, from, to);

                            a.Set(match, v[w]);
                            a.Set(match + off, v[w]);

                            if(match >= from && match < to) {
                                assert(akku.Get(0) == match);
                            }
                            if(match + off >= from && match + off < to) {
                                assert(akku.Get(0) == match + off || akku.Get(1) == match + off);
                            }

                            a.Set(match, v[w]);
                            a.Set(match + off, v[w]);
                            akku.Destroy();

                        }
                    }


                                        // Find all, EQUAL
                    if(v[w] != 9223372036854775807LL) {
                        for(size_t off = 1; off < 8; off++) {
                            Array akku;

                            a.Set(match, v[w] + 1);
                            a.Set(match + off, v[w] + 1);

                            a.find_all(COND_EQUAL, &akku, v[w] + 1, 0, from, to);

                            a.Set(match, v[w]);
                            a.Set(match + off, v[w]);

                            if(match >= from && match < to) {
                                assert(akku.Get(0) == match);
                            }
                            if(match + off >= from && match + off < to) {
                                assert(akku.Get(0) == match + off || akku.Get(1) == match + off);
                            }

                            a.Set(match, v[w]);
                            a.Set(match + off, v[w]);
                            akku.Destroy();

                        }
                    }
                }
            }    
=======
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
                    TIGHTDB_ASSERT(match == f);
                }
                else {
                    CHECK_EQUAL(f, -1);
                    TIGHTDB_ASSERT(f == -1);
                }
            }
        }
>>>>>>> 7ac938a0da8d9c2751d913470ec408769735f722

        }
        a.Destroy();
    }
}



<<<<<<< HEAD
=======
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
                    TIGHTDB_ASSERT(match == f);
                }
                else {
                    CHECK_EQUAL(f, -1);
                    TIGHTDB_ASSERT(f == -1);
                }
            }
        }

    }
}
>>>>>>> 7ac938a0da8d9c2751d913470ec408769735f722

TEST(Column_monkeytest2)
{
    const uint64_t ITER_PER_BITWIDTH = 16 * 1000 * TEST_DURATION * TEST_DURATION;
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
