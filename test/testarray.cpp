#include <map>
#include <string>
#include <vector>
#include <algorithm>

#include <UnitTest++.h>

#include <tightdb/array.hpp>
#include <tightdb/column.hpp>
#include <tightdb/query_conditions.hpp>
#include "testsettings.hpp"

using namespace std;
using namespace tightdb;

namespace {

struct db_setup_array {
    static Array c;
};

Array db_setup_array::c;

void hasZeroByte(int64_t value, size_t reps)
{
    Array a;
    Array r;

    for (size_t i = 0; i < reps - 1; i++){
        a.add(value);
    }

    a.add(0);

    size_t t = a.find_first(0);
    CHECK_EQUAL(a.Size() - 1, t);

    r.Clear();
    a.find_all(r, 0);
    CHECK_EQUAL(int64_t(a.Size() - 1), r.Get(0));

    // Cleanup
    a.Destroy();
    r.Destroy();
}

} // anonymous namespace


TEST_FIXTURE(db_setup_array, Array_Add0)
{
    c.add(0);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Size(), (size_t)1);
    CHECK_EQUAL(0, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add1)
{
    c.add(1);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Size(), 2);
    CHECK_EQUAL(1, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add2)
{
    c.add(2);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 2);
    CHECK_EQUAL(c.Size(), 3);
    CHECK_EQUAL(2, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add3)
{
    c.add(3);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 2);
    CHECK_EQUAL(c.Get(3), 3);
    CHECK_EQUAL(c.Size(), 4);
    CHECK_EQUAL(2, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add4)
{
    c.add(4);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 2);
    CHECK_EQUAL(c.Get(3), 3);
    CHECK_EQUAL(c.Get(4), 4);
    CHECK_EQUAL(c.Size(), 5);
    CHECK_EQUAL(4, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add5)
{
    c.add(16);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 2);
    CHECK_EQUAL(c.Get(3), 3);
    CHECK_EQUAL(c.Get(4), 4);
    CHECK_EQUAL(c.Get(5), 16);
    CHECK_EQUAL(c.Size(), 6);
    CHECK_EQUAL(8, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add6)
{
    c.add(256);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 2);
    CHECK_EQUAL(c.Get(3), 3);
    CHECK_EQUAL(c.Get(4), 4);
    CHECK_EQUAL(c.Get(5), 16);
    CHECK_EQUAL(c.Get(6), 256);
    CHECK_EQUAL(c.Size(), 7);
    CHECK_EQUAL(16, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add7)
{
    c.add(65536);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 2);
    CHECK_EQUAL(c.Get(3), 3);
    CHECK_EQUAL(c.Get(4), 4);
    CHECK_EQUAL(c.Get(5), 16);
    CHECK_EQUAL(c.Get(6), 256);
    CHECK_EQUAL(c.Get(7), 65536);
    CHECK_EQUAL(c.Size(), 8);
    CHECK_EQUAL(32, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add8)
{
    c.add(4294967296LL);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 2);
    CHECK_EQUAL(c.Get(3), 3);
    CHECK_EQUAL(c.Get(4), 4);
    CHECK_EQUAL(c.Get(5), 16);
    CHECK_EQUAL(c.Get(6), 256);
    CHECK_EQUAL(c.Get(7), 65536);
    CHECK_EQUAL(c.Get(8), 4294967296LL);
    CHECK_EQUAL(c.Size(), 9);
    CHECK_EQUAL(64, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_AddNeg1)
{
    c.Clear();
    c.add(-1);

    CHECK_EQUAL(c.Size(), 1);
    CHECK_EQUAL(c.Get(0), -1);
    CHECK_EQUAL(8, c.GetBitWidth());
}

TEST(Array_AddNeg1_1)
{
    Array c;

    c.add(1);
    c.add(2);
    c.add(3);
    c.add(-128);

    CHECK_EQUAL(c.Size(), 4);
    CHECK_EQUAL(c.Get(0), 1);
    CHECK_EQUAL(c.Get(1), 2);
    CHECK_EQUAL(c.Get(2), 3);
    CHECK_EQUAL(c.Get(3), -128);
    CHECK_EQUAL(8, c.GetBitWidth());

    // Cleanup
    c.Destroy();
}

TEST_FIXTURE(db_setup_array, Array_AddNeg2)
{
    c.add(-256);

    CHECK_EQUAL(c.Size(), 2);
    CHECK_EQUAL(c.Get(0), -1);
    CHECK_EQUAL(c.Get(1), -256);
    CHECK_EQUAL(16, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_AddNeg3)
{
    c.add(-65536);

    CHECK_EQUAL(c.Size(), 3);
    CHECK_EQUAL(c.Get(0), -1);
    CHECK_EQUAL(c.Get(1), -256);
    CHECK_EQUAL(c.Get(2), -65536);
    CHECK_EQUAL(32, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_AddNeg4)
{
    c.add(-4294967296LL);

    CHECK_EQUAL(c.Size(), 4);
    CHECK_EQUAL(c.Get(0), -1);
    CHECK_EQUAL(c.Get(1), -256);
    CHECK_EQUAL(c.Get(2), -65536);
    CHECK_EQUAL(c.Get(3), -4294967296LL);
    CHECK_EQUAL(64, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Set)
{
    c.Set(0, 3);
    c.Set(1, 2);
    c.Set(2, 1);
    c.Set(3, 0);

    CHECK_EQUAL(c.Size(), 4);
    CHECK_EQUAL(c.Get(0), 3);
    CHECK_EQUAL(c.Get(1), 2);
    CHECK_EQUAL(c.Get(2), 1);
    CHECK_EQUAL(c.Get(3), 0);
}

TEST_FIXTURE(db_setup_array, Array_Insert1)
{
    // Set up some initial values
    c.Clear();
    c.add(0);
    c.add(1);
    c.add(2);
    c.add(3);

    // Insert in middle
    c.Insert(2, 16);

    CHECK_EQUAL(c.Size(), 5);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 16);
    CHECK_EQUAL(c.Get(3), 2);
    CHECK_EQUAL(c.Get(4), 3);
}

TEST_FIXTURE(db_setup_array, Array_Insert2)
{
    // Insert at top
    c.Insert(0, 256);

    CHECK_EQUAL(c.Size(), 6);
    CHECK_EQUAL(c.Get(0), 256);
    CHECK_EQUAL(c.Get(1), 0);
    CHECK_EQUAL(c.Get(2), 1);
    CHECK_EQUAL(c.Get(3), 16);
    CHECK_EQUAL(c.Get(4), 2);
    CHECK_EQUAL(c.Get(5), 3);
}

TEST_FIXTURE(db_setup_array, Array_Insert3)
{
    // Insert at bottom
    c.Insert(6, 65536);

    CHECK_EQUAL(c.Size(), 7);
    CHECK_EQUAL(c.Get(0), 256);
    CHECK_EQUAL(c.Get(1), 0);
    CHECK_EQUAL(c.Get(2), 1);
    CHECK_EQUAL(c.Get(3), 16);
    CHECK_EQUAL(c.Get(4), 2);
    CHECK_EQUAL(c.Get(5), 3);
    CHECK_EQUAL(c.Get(6), 65536);
}

/*
TEST_FIXTURE(db_setup_array, Array_Index1)
{
    // Create index
    Column index;
    c.BuildIndex(index);

    CHECK_EQUAL(0, c.FindWithIndex(256));
    CHECK_EQUAL(1, c.FindWithIndex(0));
    CHECK_EQUAL(2, c.FindWithIndex(1));
    CHECK_EQUAL(3, c.FindWithIndex(16));
    CHECK_EQUAL(4, c.FindWithIndex(2));
    CHECK_EQUAL(5, c.FindWithIndex(3));
    CHECK_EQUAL(6, c.FindWithIndex(65536));

    c.ClearIndex();
}
*/

TEST_FIXTURE(db_setup_array, Array_Delete1)
{
    // Delete from middle
    c.Delete(3);

    CHECK_EQUAL(c.Size(), 6);
    CHECK_EQUAL(c.Get(0), 256);
    CHECK_EQUAL(c.Get(1), 0);
    CHECK_EQUAL(c.Get(2), 1);
    CHECK_EQUAL(c.Get(3), 2);
    CHECK_EQUAL(c.Get(4), 3);
    CHECK_EQUAL(c.Get(5), 65536);
}

TEST_FIXTURE(db_setup_array, Array_Delete2)
{
    // Delete from top
    c.Delete(0);

    CHECK_EQUAL(c.Size(), 5);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 2);
    CHECK_EQUAL(c.Get(3), 3);
    CHECK_EQUAL(c.Get(4), 65536);
}

TEST_FIXTURE(db_setup_array, Array_Delete3)
{
    // Delete from bottom
    c.Delete(4);

    CHECK_EQUAL(c.Size(), 4);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 2);
    CHECK_EQUAL(c.Get(3), 3);
}

TEST_FIXTURE(db_setup_array, Array_DeleteAll)
{
    // Delete all items one at a time
    c.Delete(0);
    c.Delete(0);
    c.Delete(0);
    c.Delete(0);

    CHECK(c.is_empty());
    CHECK_EQUAL(0, c.Size());
}

TEST_FIXTURE(db_setup_array, Array_Find1)
{
    // Look for a non-existing value
    size_t res = c.find_first(10);

    CHECK_EQUAL(res, -1);
}

TEST_FIXTURE(db_setup_array, Array_Find2)
{
    // zero-bit width
    c.Clear();
    c.add(0);
    c.add(0);

    size_t res = c.find_first(0);
    CHECK_EQUAL(res, 0);
}

TEST_FIXTURE(db_setup_array, Array_Find3)
{
    // expand to 1-bit width
    c.add(1);

    size_t res = c.find_first(1);
    CHECK_EQUAL(res, 2);
}

TEST_FIXTURE(db_setup_array, Array_Find4)
{
    // expand to 2-bit width
    c.add(2);

    size_t res = c.find_first(2);
    CHECK_EQUAL(res, 3);
}

TEST_FIXTURE(db_setup_array, Array_Find5)
{
    // expand to 4-bit width
    c.add(4);

    size_t res = c.find_first(4);
    CHECK_EQUAL(res, 4);
}

TEST_FIXTURE(db_setup_array, Array_Find6)
{
    // expand to 8-bit width
    c.add(16);

    // Add some more to make sure we
    // can search in 64bit chunks
    c.add(16);
    c.add(7);

    size_t res = c.find_first(7);
    CHECK_EQUAL(7, res);
}

TEST_FIXTURE(db_setup_array, Array_Find7)
{
    // expand to 16-bit width
    c.add(256);

    size_t res = c.find_first(256);
    CHECK_EQUAL(8, res);
}

TEST_FIXTURE(db_setup_array, Array_Find8)
{
    // expand to 32-bit width
    c.add(65536);

    size_t res = c.find_first(65536);
    CHECK_EQUAL(9, res);
}

TEST_FIXTURE(db_setup_array, Array_Find9)
{
    // expand to 64-bit width
    c.add(4294967296LL);

    size_t res = c.find_first(4294967296LL);
    CHECK_EQUAL(10, res);
}

/* Partial find is not fully implemented yet
#define PARTIAL_COUNT 100
TEST_FIXTURE(db_setup_array, Array_PartialFind1)
{
    c.Clear();

    for (size_t i = 0; i < PARTIAL_COUNT; ++i) {
        c.add(i);
    }

    CHECK_EQUAL(-1, c.find_first(PARTIAL_COUNT+1, 0, PARTIAL_COUNT));
    CHECK_EQUAL(-1, c.find_first(0, 1, PARTIAL_COUNT));
    CHECK_EQUAL(PARTIAL_COUNT-1, c.find_first(PARTIAL_COUNT-1, PARTIAL_COUNT-1, PARTIAL_COUNT));
}
*/

TEST_FIXTURE(db_setup_array, Array_Destroy)
{
    // clean up (ALWAYS PUT THIS LAST)
    c.Destroy();
}

TEST(Array_Sort)
{
    // Create Array with random values
    Array a;
    a.add(25);
    a.add(12);
    a.add(50);
    a.add(3);
    a.add(34);
    a.add(0);
    a.add(17);
    a.add(51);
    a.add(2);
    a.add(40);

    a.sort();

    CHECK_EQUAL(0, a.Get(0));
    CHECK_EQUAL(2, a.Get(1));
    CHECK_EQUAL(3, a.Get(2));
    CHECK_EQUAL(12, a.Get(3));
    CHECK_EQUAL(17, a.Get(4));
    CHECK_EQUAL(25, a.Get(5));
    CHECK_EQUAL(34, a.Get(6));
    CHECK_EQUAL(40, a.Get(7));
    CHECK_EQUAL(50, a.Get(8));
    CHECK_EQUAL(51, a.Get(9));

    // Cleanup
    a.Destroy();
}

/** find_all() int tests spread out over bitwidth
 *
 */


TEST(findallint0)
{
    Array a;
    Array r;

    const int value = 0;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++){
        a.add(0);
    }

    a.find_all(r, value);
    CHECK_EQUAL(vReps, r.Size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.Size()){
        if (a.Get(i) == value)
            CHECK_EQUAL(int64_t(i), r.Get(j++));
        i += 1;
    }

    // Cleanup
    a.Destroy();
    r.Destroy();
}

TEST(findallint1)
{
    Array a;
    Array r;

    const int value = 1;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++){
        a.add(0);
        a.add(0);
        a.add(1);
        a.add(0);
    }

    a.find_all(r, value);
    CHECK_EQUAL(vReps, r.Size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.Size()){
        if (a.Get(i) == value)
            CHECK_EQUAL(int64_t(i), r.Get(j++));
        i += 1;
    }

    // Cleanup
    a.Destroy();
    r.Destroy();
}

TEST(findallint2)
{
    Array a;
    Array r;

    const int value = 3;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++){
        a.add(0);
        a.add(1);
        a.add(2);
        a.add(3);
    }

    a.find_all(r, value);
    CHECK_EQUAL(vReps, r.Size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.Size()){
        if (a.Get(i) == value)
            CHECK_EQUAL(int64_t(i), r.Get(j++));
        i += 1;
    }

    // Cleanup
    a.Destroy();
    r.Destroy();
}

TEST(findallint3)
{
    Array a;
    Array r;

    const int value = 10;
    const int vReps = 5;
    // 0, 4, 8
    for (int i = 0; i < vReps; i++){
        a.add(10);
        a.add(11);
        a.add(12);
        a.add(13);
    }

    a.find_all(r, value);
    CHECK_EQUAL(vReps, r.Size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.Size()){
        if (a.Get(i) == value)
            CHECK_EQUAL(int64_t(i), r.Get(j++));
        i += 1;
    }

    // Cleanup
    a.Destroy();
    r.Destroy();
}

TEST(findallint4)
{
    Array a;
    Array r;

    const int value = 20;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++){
        // 8 bitwidth
        a.add(20);
        a.add(21);
        a.add(22);
        a.add(23);
    }

    a.find_all(r, value);
    CHECK_EQUAL(vReps, r.Size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.Size()){
        if (a.Get(i) == value)
            CHECK_EQUAL(int64_t(i), r.Get(j++));
        i += 1;
    }

    // Cleanup
    a.Destroy();
    r.Destroy();
}

TEST(findallint5)
{
    Array a;
    Array r;

    const int value = 303;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++){
        // 16 bitwidth
        a.add(300);
        a.add(301);
        a.add(302);
        a.add(303);
    }

    a.find_all(r, value);
    CHECK_EQUAL(vReps, r.Size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.Size()){
        if (a.Get(i) == value)
            CHECK_EQUAL(int64_t(i), r.Get(j++));
        i += 1;
    }

    // Cleanup
    a.Destroy();
    r.Destroy();
}

TEST(findallint6)
{
    Array a;
    Array r;

    const int value = 70000;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++){
        // 32 bitwidth
        a.add(70000);
        a.add(70001);
        a.add(70002);
        a.add(70003);
    }

    a.find_all(r, value);
    CHECK_EQUAL(vReps, r.Size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.Size()){
        if (a.Get(i) == value)
            CHECK_EQUAL(int64_t(i), r.Get(j++));
        i += 1;
    }

    // Cleanup
    a.Destroy();
    r.Destroy();
}

TEST(findallint7)
{
    Array a;
    Array r;

    const int64_t value = 4300000003ULL;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++){
        // 64 bitwidth
        a.add(4300000000ULL);
        a.add(4300000001ULL);
        a.add(4300000002ULL);
        a.add(4300000003ULL);
    }

    a.find_all(r, value);
    CHECK_EQUAL(vReps, r.Size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.Size()){
        if (a.Get(i) == value)
            CHECK_EQUAL(int64_t(i), r.Get(j++));
        i += 1;
    }

    // Cleanup
    a.Destroy();
    r.Destroy();
}

// Tests the case where a value does *not* exist in one entire 64-bit chunk (triggers the 'if (hasZeroByte) break;' condition)
TEST(FindhasZeroByte)
{
    // we want at least 1 entire 64-bit chunk-test, and we also want a remainder-test, so we chose n to be a prime > 64
    size_t n = 73;
    hasZeroByte(1, n); // width = 1
    hasZeroByte(3, n); // width = 2
    hasZeroByte(13, n); // width = 4
    hasZeroByte(100, n); // 8
    hasZeroByte(10000, n); // 16
    hasZeroByte(100000, n); // 32
    hasZeroByte(8000000000LL, n); // 64
}

// New find test for SSE search, to trigger partial finds (see FindSSE()) before and after the aligned data area
TEST(FindSSE)
{
    Array a;
    for (uint64_t i = 0; i < 100; i++) {
        a.add(10000);
    }

    for (size_t i = 0; i < 100; i++) {
        a.Set(i, 123);
        size_t t = a.find_first(123);
        TIGHTDB_ASSERT(t == i);
        a.Set(i, 10000);
        (void)t;
    }
    a.Destroy();
}


TEST(Sum0)
{
    Array a;
    for (int i = 0; i < 64 + 7; i++) {
        a.add(0);
    }
    CHECK_EQUAL(0, a.sum(0, a.Size()));
    a.Destroy();
}

TEST(Sum1)
{
    int64_t s1 = 0;
    Array a;
    for (int i = 0; i < 256 + 7; i++)
        a.add(i % 2);

    s1 = 0;
    for (int i = 0; i < 256 + 7; i++)
        s1 += a.Get(i);
    CHECK_EQUAL(s1, a.sum(0, a.Size()));

    s1 = 0;
    for (int i = 3; i < 100; i++)
        s1 += a.Get(i);
    CHECK_EQUAL(s1, a.sum(3, 100));

    a.Destroy();
}

TEST(Sum2)
{
    int64_t s1 = 0;
    Array a;
    for (int i = 0; i < 256 + 7; i++)
        a.add(i % 4);

    s1 = 0;
    for (int i = 0; i < 256 + 7; i++)
        s1 += a.Get(i);
    CHECK_EQUAL(s1, a.sum(0, a.Size()));

    s1 = 0;
    for (int i = 3; i < 100; i++)
        s1 += a.Get(i);
    CHECK_EQUAL(s1, a.sum(3, 100));

    a.Destroy();
}


TEST(Sum4)
{
    int64_t s1 = 0;
    Array a;
    for (int i = 0; i < 256 + 7; i++)
        a.add(i % 16);

    s1 = 0;
    for (int i = 0; i < 256 + 7; i++)
        s1 += a.Get(i);
    CHECK_EQUAL(s1, a.sum(0, a.Size()));

    s1 = 0;
    for (int i = 3; i < 100; i++)
        s1 += a.Get(i);
    CHECK_EQUAL(s1, a.sum(3, 100));

    a.Destroy();
}

TEST(Sum16)
{
    int64_t s1 = 0;
    Array a;
    for (int i = 0; i < 256 + 7; i++)
        a.add(i % 30000);

    s1 = 0;
    for (int i = 0; i < 256 + 7; i++)
        s1 += a.Get(i);
    CHECK_EQUAL(s1, a.sum(0, a.Size()));

    s1 = 0;
    for (int i = 3; i < 100; i++)
        s1 += a.Get(i);
    CHECK_EQUAL(s1, a.sum(3, 100));

    a.Destroy();
}

TEST(Greater)
{
    Array a;

    size_t items = 400;

    for (items = 2; items < 200; items += 7)
    {

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(0);
        }
        size_t t = a.find_first<GREATER>(0, 0, (size_t)-1);
        CHECK_EQUAL(-1, t);


        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(0);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 1);

			size_t t = a.find_first<GREATER>(0, 0, (size_t)-1);
            TIGHTDB_ASSERT(i == t);

            CHECK_EQUAL(i, t);
            a.Set(i, 0);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(2);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 3);
            size_t t = a.find_first<GREATER>(2, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 2);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(10);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 11);
            size_t t = a.find_first<GREATER>(10, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 10);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(100);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 110);
            size_t t = a.find_first<GREATER>(100, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 100);
        }
        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(200);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 210);
            size_t t = a.find_first<GREATER>(200, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 200);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(10000);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 11000);
            size_t t = a.find_first<GREATER>(10000, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 10000);
        }
        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(40000);
        }

        for (size_t i = 0; i < items; i++) {
            a.Set(i, 41000);
            size_t t = a.find_first<GREATER>(40000, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 40000);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(1000000);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 1100000);
            size_t t = a.find_first<GREATER>(1000000, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 1000000);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(1000ULL*1000ULL*1000ULL*1000ULL);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 1000ULL*1000ULL*1000ULL*1000ULL + 1ULL);
            size_t t = a.find_first<GREATER>(1000ULL*1000ULL*1000ULL*1000ULL, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 1000ULL*1000ULL*1000ULL*1000ULL);
        }

    }
    a.Destroy();
}




TEST(Less)
{
    Array a;

    size_t items = 400;

    for (items = 2; items < 200; items += 7)
    {

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(0);
        }
        size_t t = a.find_first<LESS>(0, 0, (size_t)-1);
        CHECK_EQUAL(-1, t);


        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(1);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 0);
            size_t t = a.find_first<LESS>(1, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 1);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(3);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 2);
            size_t t = a.find_first<LESS>(3, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 3);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(11);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 10);
            size_t t = a.find_first<LESS>(11, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 11);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(110);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 100);
            size_t t = a.find_first<LESS>(110, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 110);
        }
        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(210);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 200);
            size_t t = a.find_first<LESS>(210, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 210);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(11000);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 10000);
            size_t t = a.find_first<LESS>(11000, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 11000);
        }
        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(41000);
        }

        for (size_t i = 0; i < items; i++) {
            a.Set(i, 40000);
            size_t t = a.find_first<LESS>(41000, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 41000);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(1100000);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 1000000);
            size_t t = a.find_first<LESS>(1100000, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 1100000);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(1000ULL*1000ULL*1000ULL*1000ULL);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 1000ULL*1000ULL*1000ULL*1000ULL - 1ULL);
            size_t t = a.find_first<LESS>(1000ULL*1000ULL*1000ULL*1000ULL, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 1000ULL*1000ULL*1000ULL*1000ULL);
        }

    }
    a.Destroy();
}


TEST(NotEqual1)
{
    Array a;
    
    a.Clear();
    for (size_t i = 0; i < 100; i++) {
        a.add(0x33);
    }
    a.Set(50, 0x44);
    size_t t = a.find_first<NOTEQUAL>(0x33, 0, (size_t)-1);
    CHECK_EQUAL(50, t);
    a.Destroy();
}

TEST(NotEqual)
{
    Array a;

    size_t items = 400;

    for (items = 2; items < 200; items += 7)
    {
        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(0);
        }
        size_t t = a.find_first<NOTEQUAL>(0, 0, (size_t)-1);
        CHECK_EQUAL(-1, t);


        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(0);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 1);
            size_t t = a.find_first<NOTEQUAL>(0, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 0);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(2);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 3);
            size_t t = a.find_first<NOTEQUAL>(2, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 2);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(10);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 11);
            size_t t = a.find_first<NOTEQUAL>(10, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 10);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(100);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 110);
            size_t t = a.find_first<NOTEQUAL>(100, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 100);
        }
        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(200);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 210);
            size_t t = a.find_first<NOTEQUAL>(200, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 200);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(10000);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 11000);
            size_t t = a.find_first<NOTEQUAL>(10000, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 10000);
        }
        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(40000);
        }

        for (size_t i = 0; i < items; i++) {
            a.Set(i, 41000);
            size_t t = a.find_first<NOTEQUAL>(40000, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 40000);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(1000000);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 1100000);
            size_t t = a.find_first<NOTEQUAL>(1000000, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 1000000);
        }

        a.Clear();
        for (size_t i = 0; i < items; i++) {
            a.add(1000ULL*1000ULL*1000ULL*1000ULL);
        }
        for (size_t i = 0; i < items; i++) {
            a.Set(i, 1000ULL*1000ULL*1000ULL*1000ULL + 1ULL);
            size_t t = a.find_first<NOTEQUAL>(1000ULL*1000ULL*1000ULL*1000ULL, 0, (size_t)-1);
            CHECK_EQUAL(i, t);
            a.Set(i, 1000ULL*1000ULL*1000ULL*1000ULL);
        }

    }
    a.Destroy();
}




TEST(ArraySort)
{
    // negative values
    Array a;

    for (size_t t = 0; t < 400; t++)
        a.add(rand() % 300 - 100);

    size_t orig_size = a.Size();
    a.sort();

    CHECK(a.Size() == orig_size);
    for (size_t t = 1; t < a.Size(); t++)
        CHECK(a.Get(t) >= a.Get(t - 1));

    a.Destroy();
}


TEST(ArraySort2)
{
    // 64 bit values
    Array a;

    for (size_t t = 0; t < 400; t++)
        a.add((int64_t)rand() * (int64_t)rand() * (int64_t)rand() * (int64_t)rand() * (int64_t)rand() * (int64_t)rand() * (int64_t)rand() * (int64_t)rand());

    size_t orig_size = a.Size();
    a.sort();

    CHECK(a.Size() == orig_size);
    for (size_t t = 1; t < a.Size(); t++)
        CHECK(a.Get(t) >= a.Get(t - 1));

    a.Destroy();
}

TEST(ArraySort3)
{
    // many values
    Array a;

    for (size_t t = 0; t < 1000000ULL; t++)
        a.add(rand());

    size_t orig_size = a.Size();
    a.sort();

    CHECK(a.Size() == orig_size);
    for (size_t t = 1; t < a.Size(); t++)
        CHECK(a.Get(t) >= a.Get(t - 1));

    a.Destroy();
}


TEST(ArraySort4)
{
    // same values
    Array a;

    for (size_t t = 0; t < 1000; t++)
        a.add(0);

    size_t orig_size = a.Size();
    a.sort();

    CHECK(a.Size() == orig_size);
    for (size_t t = 1; t < a.Size(); t++)
        CHECK(a.Get(t) == 0);

    a.Destroy();
}

TEST(ArrayCopy)
{
    Array a;
    a.add(0);
    a.add(1);
    a.add(2);
    a.add(3);
    a.add(4);

    Array b;
    b.Copy(a);

#ifdef TIGHTDB_DEBUG
    b.Verify();
#endif

    CHECK_EQUAL(5, b.Size());
    CHECK_EQUAL(0, b.Get(0));
    CHECK_EQUAL(1, b.Get(1));
    CHECK_EQUAL(2, b.Get(2));
    CHECK_EQUAL(3, b.Get(3));
    CHECK_EQUAL(4, b.Get(4));

    // With sub-arrays
    Array c(coldef_HasRefs);
    c.add(a.GetRef());

    Array d;
    d.Copy(c);

#ifdef TIGHTDB_DEBUG
    b.Verify();
#endif

    CHECK(d.HasRefs());
    CHECK_EQUAL(1, d.Size());

    const Array e = d.GetSubArray(0);
    CHECK_EQUAL(5, e.Size());
    CHECK_EQUAL(0, e.Get(0));
    CHECK_EQUAL(1, e.Get(1));
    CHECK_EQUAL(2, e.Get(2));
    CHECK_EQUAL(3, e.Get(3));
    CHECK_EQUAL(4, e.Get(4));

    //a.Destroy() // will be destroyed as sub-array by c
    b.Destroy();
    c.Destroy();
    d.Destroy();
    //e.Destroy() // will be destroyed as sub-array by d
}

TEST(ArrayCount)
{
    Array a;

    // 0 bit width
    for (size_t i = 0; i < 150; ++i) {
        a.add(0);
    }
    const size_t c1 = a.count(0);
    const size_t c2 = a.count(1);
    CHECK_EQUAL(150, c1);
    CHECK_EQUAL(0, c2);
    CHECK_EQUAL(0, a.count(-1));
    CHECK_EQUAL(0, a.count(2));

    // 1 bit width
    for (size_t i = 0; i < 100; ++i) {
        if (i % 2) a.Set(i, 1);
    }
    const size_t c3 = a.count(0);
    const size_t c4 = a.count(1);
    CHECK_EQUAL(100, c3);
    CHECK_EQUAL(50, c4);
    CHECK_EQUAL(0, a.count(-1));
    CHECK_EQUAL(0, a.count(4));

    // 2 bit width
    for (size_t i = 0; i < 100; ++i) {
        if (i % 2) a.Set(i, 2);
    }
    const size_t c5 = a.count(0);
    const size_t c6 = a.count(2);
    CHECK_EQUAL(100, c5);
    CHECK_EQUAL(50, c6);
    CHECK_EQUAL(0, a.count(-1));
    CHECK_EQUAL(0, a.count(4));

    // 4 bit width
    for (size_t i = 0; i < 100; ++i) {
        if (i % 2) a.Set(i, 7);
    }
    const size_t c7 = a.count(0);
    const size_t c8 = a.count(7);
    CHECK_EQUAL(100, c7);
    CHECK_EQUAL(50, c8);
    CHECK_EQUAL(0, a.count(-1));
    CHECK_EQUAL(0, a.count(4));

    // 8 bit width
    for (size_t i = 0; i < 100; ++i) {
        if (i % 2) a.Set(i, 100);
    }
    const size_t c9 = a.count(0);
    const size_t c10 = a.count(100);
    CHECK_EQUAL(100, c9);
    CHECK_EQUAL(50, c10);
    CHECK_EQUAL(0, a.count(-1));
    CHECK_EQUAL(0, a.count(128));
    CHECK_EQUAL(0, a.count(-128));

    // 16 bit width
    for (size_t i = 0; i < 100; ++i) {
        if (i % 2) a.Set(i, 500);
    }
    const size_t c11 = a.count(0);
    const size_t c12 = a.count(500);
    CHECK_EQUAL(100, c11);
    CHECK_EQUAL(50, c12);
    CHECK_EQUAL(0, a.count(-1));
    CHECK_EQUAL(0, a.count(0xFFFF));
    CHECK_EQUAL(0, a.count(-0xFFFF));

    // 32 bit width
    for (size_t i = 0; i < 100; ++i) {
        if (i % 2) a.Set(i, 0x1FFFF);
    }
    const size_t c13 = a.count(0);
    const size_t c14 = a.count(0x1FFFF);
    CHECK_EQUAL(100, c13);
    CHECK_EQUAL(50, c14);
    CHECK_EQUAL(0, a.count(-1));
    CHECK_EQUAL(0, a.count(0xFFFFFFFF));
    CHECK_EQUAL(0, a.count(-0xFFFFFFFFll));

    // Clean-up
    a.Destroy();
}
