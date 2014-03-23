#include "../testsettings.hpp"
#ifdef TEST_COLUMN_LARGE

#include <algorithm>
#include <vector>

#include <tightdb/column.hpp>
#include <tightdb/query_conditions.hpp>

#include "../util/unit_test.hpp"
#include "../util/test_only.hpp"

#include "verified_integer.hpp"

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

} // anonymous namespace


TEST(ColumnLarge_Less)
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
                     4294967297LL, -4294967294LL, -9223372036854775807LL, (-9223372036854775807LL - 1), -9223372036854775806LL,
                     /* (-9223372036854775807LL - 1) because -9223372036854775808LL is buggy; it's seen as a minus token and then a right-hand-side
                     exceeding long long's range. Furthermore, std::numeric_limits<int64_t>::min is typedef'ed to 'long long' which cannot be used in
                     initializer list for int64_t */
    };



    for (size_t w = 0; w < sizeof(v) / sizeof(*v); w++) {
        const size_t LEN = 64 * 20 + 1000;
        Array a;
        for (size_t t = 0; t < LEN; t++)
            a.add(v[w]);

        // to create at least 64 bytes of data (2 * 128-bit SSE chunks + 64 bit chunk before and after + some unaligned data before and after)
        size_t LEN2 = 64 * 8 / (a.get_width() == 0 ? 1 : a.get_width());

        Array akku;
        QueryState<int64_t> state;
        state.m_state = int64_t(&akku);

        for (size_t from = 0; from < LEN2; from++) {
            for (size_t to = from + 1; to <= LEN2; to++) {
                for (size_t match = (from > 8 ? from - 8 : 0); match < (to > 8 ? to - 8 : 8); match++) {

                    if (v[w] != LL_MIN) {
                        // LESS
                        a.set(match, v[w] - 1);
                        state.init(act_ReturnFirst, &akku, size_t(-1));
                        a.find(cond_Less, act_ReturnFirst, v[w], from, to, 0, &state);
                        size_t f = state.m_state;
                        a.set(match, v[w]);
                        if (match >= from && match < to) {
                            CHECK(match == f);
                        }
                        else {
                            CHECK(f == size_t(-1));
                        }
                    }

                    if (v[w] != LL_MAX) {
                        // GREATER
                        a.set(match, v[w] + 1);
                        state.init(act_ReturnFirst, &akku, size_t(-1));
                        a.find(cond_Greater, act_ReturnFirst, v[w], from, to, 0, &state);
                        size_t f = state.m_state;
                        a.set(match, v[w]);
                        if (match >= from && match < to) {
                            CHECK(match == f);
                        }
                        else {
                            CHECK(f == size_t(-1));
                        }
                    }

                    // FIND
                    a.set(match, v[w]-1);
                    size_t f = a.find_first(v[w]-1, from, to);
                    a.set(match, v[w]);
                    if (match >= from && match < to) {
                        CHECK(match == f);
                    }
                    else {
                        CHECK(f == size_t(-1));
                    }

                    if (v[w] != LL_MIN) {
                        // MIN
                        int64_t val = 0;
                        a.set(match, v[w]-1);
                        bool b = a.minimum(val, from, to);
                        a.set(match, v[w]);
                        CHECK_EQUAL(true, b);
                        if (match >= from && match < to)
                            CHECK(val == v[w]-1);
                        else
                            CHECK(val == v[w]);
                    }

                    // MAX
                    if (v[w] != LL_MAX) {
                        int64_t val = 0;
                        a.set(match, v[w]+1);
                        bool b = a.maximum(val, from, to);
                        a.set(match, v[w]);
                        CHECK_EQUAL(true, b);
                        if (match >= from && match < to)
                            CHECK(val == v[w]+1);
                        else
                            CHECK(val == v[w]);
                    }

                    // SUM
                    int64_t val = 0;
                    a.set(match, v[w]+1);
                    val = a.sum(from, to);
                    a.set(match, v[w]);
                    int64_t intended;
                    if (match >= from && match < to)
                        intended = (to - from - 1) * v[w] + v[w] + 1;
                    else
                        intended = (to - from) * v[w];

                    CHECK(intended == val);


                    // Find all, LESS
                    if (v[w] != LL_MIN) {
                        for (size_t off = 1; off < 8; off++) {

                            a.set(match, v[w] - 1);
                            a.set(match + off, v[w] - 1);

                            akku.clear();
                            state.init(act_FindAll, &akku, size_t(-1));
                            a.find(cond_Less, act_FindAll, v[w], from, to, 0, &state);

                            a.set(match, v[w]);
                            a.set(match + off, v[w]);

                            if (match >= from && match < to) {
                                CHECK(size_t(akku.get(0)) == match);
                            }
                            if (match + off >= from && match + off < to) {
                                CHECK(size_t(akku.get(0)) == match + off || size_t(akku.get(1)) == match + off);
                            }

                            a.set(match, v[w]);
                            a.set(match + off, v[w]);

                        }
                    }


                    // Find all, GREATER
                    if (v[w] != LL_MAX) {
                        for (size_t off = 1; off < 8; off++) {

                            a.set(match, v[w] + 1);
                            a.set(match + off, v[w] + 1);

                            akku.clear();
                            state.init(act_FindAll, &akku, size_t(-1));
                            a.find(cond_Greater, act_FindAll, v[w], from, to, 0, &state);

                            a.set(match, v[w]);
                            a.set(match + off, v[w]);

                            if (match >= from && match < to) {
                                CHECK(size_t(akku.get(0)) == match);
                            }
                            if (match + off >= from && match + off < to) {
                                CHECK(size_t(akku.get(0)) == match + off || size_t(akku.get(1)) == match + off);
                            }

                            a.set(match, v[w]);
                            a.set(match + off, v[w]);
                        }
                    }


                    // Find all, EQUAL
                    if (v[w] != LL_MAX) {
                        for (size_t off = 1; off < 8; off++) {
                            a.set(match, v[w] + 1);
                            a.set(match + off, v[w] + 1);

                            akku.clear();
                            state.init(act_FindAll, &akku, size_t(-1));
                            a.find(cond_Equal, act_FindAll, v[w] + 1, from, to, 0, &state);

                            a.set(match, v[w]);
                            a.set(match + off, v[w]);

                            if (match >= from && match < to) {
                                CHECK(size_t(akku.get(0)) == match);
                            }
                            if (match + off >= from && match + off < to) {
                                CHECK(size_t(akku.get(0)) == match + off || size_t(akku.get(1)) == match + off);
                            }

                            a.set(match, v[w]);
                            a.set(match + off, v[w]);
                        }
                    }
                }
            }

        }
        a.destroy();
    }
}


ONLY(ColumnLarge_Monkey2)
{
    const uint64_t ITER_PER_BITWIDTH = 16 * 1000 * 20;
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
                trend = unsigned(rand2()) % 10;
                a.find_first(rand2(int(current_bitwidth)));
                a.find_all(res, rand2(int(current_bitwidth)));
                size_t start = rand2() % (a.size() + 1);
                a.sum(start, start + rand2() % (a.size() + 1 - start));
                a.maximum(start, start + rand2() % (a.size() + 1 - start));
                a.minimum(start, start + rand2() % (a.size() + 1 - start));
            }

            if (rand2() % 10 > trend && a.size() < ITER_PER_BITWIDTH / 100) {
                uint64_t l = rand2(int(current_bitwidth));
                if (rand2() % 2 == 0) {
                    // Insert
                    size_t pos = rand2() % (a.size() + 1);
                    a.insert(pos, l);
                }
                else {
                    // Add
                    a.add(l);
                }
            }
            else if (a.size() > 0) {
                // Delete
                size_t i = rand2() % a.size();
                a.erase(i);
            }
        }
    }

    // Cleanup
    a.destroy();
    res.destroy();
}

#endif // TEST_COLUMN_LARGE
