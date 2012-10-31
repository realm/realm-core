#include <vector>
#include <algorithm>
#include <UnitTest++.h>
#include <tightdb/column.hpp>
#include "testsettings.hpp"

using namespace tightdb;

struct db_setup {
    static Column c;
};

// Pre-declare local functions

Column db_setup::c;

TEST_FIXTURE(db_setup, Column_IsEmpty)
{
    CHECK(c.is_empty());
    CHECK_EQUAL(c.Size(), (size_t)0);
}

TEST_FIXTURE(db_setup, Column_Add0)
{
    c.add(0);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Size(), (size_t)1);
}

TEST_FIXTURE(db_setup, Column_Add1)
{
    c.add(1);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Size(), 2);
}

TEST_FIXTURE(db_setup, Column_Add2)
{
    c.add(2);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 2);
    CHECK_EQUAL(c.Size(), 3);
}

TEST_FIXTURE(db_setup, Column_Add3)
{
    c.add(3);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 2);
    CHECK_EQUAL(c.Get(3), 3);
    CHECK_EQUAL(c.Size(), 4);
}

TEST_FIXTURE(db_setup, Column_Add4)
{
    c.add(4);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 2);
    CHECK_EQUAL(c.Get(3), 3);
    CHECK_EQUAL(c.Get(4), 4);
    CHECK_EQUAL(c.Size(), 5);
}

TEST_FIXTURE(db_setup, Column_Add5)
{
    c.add(16);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 2);
    CHECK_EQUAL(c.Get(3), 3);
    CHECK_EQUAL(c.Get(4), 4);
    CHECK_EQUAL(c.Get(5), 16);
    CHECK_EQUAL(c.Size(), 6);
}

TEST_FIXTURE(db_setup, Column_Add6)
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
}

TEST_FIXTURE(db_setup, Column_Add7)
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
}

TEST_FIXTURE(db_setup, Column_Add8)
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
}

TEST_FIXTURE(db_setup, Column_AddNeg1)
{
    c.Clear();
    c.add(-1);

    CHECK_EQUAL(c.Size(), 1);
    CHECK_EQUAL(c.Get(0), -1);
}

TEST_FIXTURE(db_setup, Column_AddNeg2)
{
    c.add(-256);

    CHECK_EQUAL(c.Size(), 2);
    CHECK_EQUAL(c.Get(0), -1);
    CHECK_EQUAL(c.Get(1), -256);
}

TEST_FIXTURE(db_setup, Column_AddNeg3)
{
    c.add(-65536);

    CHECK_EQUAL(c.Size(), 3);
    CHECK_EQUAL(c.Get(0), -1);
    CHECK_EQUAL(c.Get(1), -256);
    CHECK_EQUAL(c.Get(2), -65536);
}

TEST_FIXTURE(db_setup, Column_AddNeg4)
{
    c.add(-4294967296LL);

    CHECK_EQUAL(c.Size(), 4);
    CHECK_EQUAL(c.Get(0), -1);
    CHECK_EQUAL(c.Get(1), -256);
    CHECK_EQUAL(c.Get(2), -65536);
    CHECK_EQUAL(c.Get(3), -4294967296LL);
}

TEST_FIXTURE(db_setup, Column_Set)
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

TEST_FIXTURE(db_setup, Column_Insert1)
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

TEST_FIXTURE(db_setup, Column_Insert2)
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

TEST_FIXTURE(db_setup, Column_Insert3)
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
TEST_FIXTURE(db_setup, Column_Index1)
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

TEST_FIXTURE(db_setup, Column_Delete1)
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

TEST_FIXTURE(db_setup, Column_Delete2)
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

TEST_FIXTURE(db_setup, Column_Delete3)
{
    // Delete from bottom
    c.Delete(4);

    CHECK_EQUAL(c.Size(), 4);
    CHECK_EQUAL(c.Get(0), 0);
    CHECK_EQUAL(c.Get(1), 1);
    CHECK_EQUAL(c.Get(2), 2);
    CHECK_EQUAL(c.Get(3), 3);
}

TEST_FIXTURE(db_setup, Column_DeleteAll)
{
    // Delete all items one at a time
    c.Delete(0);
    c.Delete(0);
    c.Delete(0);
    c.Delete(0);

    CHECK(c.is_empty());
    CHECK_EQUAL(0, c.Size());
}


TEST_FIXTURE(db_setup, Column_Find1)
{
    // Look for a non-existing value
    size_t res = c.find_first(10);

    CHECK_EQUAL(res, -1);
}

TEST_FIXTURE(db_setup, Column_Find2)
{
    // zero-bit width
    c.Clear();
    c.add(0);
    c.add(0);

    size_t res = c.find_first(0);
    CHECK_EQUAL(res, 0);
}

TEST_FIXTURE(db_setup, Column_Find3)
{
    // expand to 1-bit width
    c.add(1);

    size_t res = c.find_first(1);
    CHECK_EQUAL(res, 2);
}

TEST_FIXTURE(db_setup, Column_Find4)
{
    // expand to 2-bit width
    c.add(2);

    size_t res = c.find_first(2);
    CHECK_EQUAL(res, 3);
}

TEST_FIXTURE(db_setup, Column_Find5)
{
    // expand to 4-bit width
    c.add(4);

    size_t res = c.find_first(4);
    CHECK_EQUAL(res, 4);
}

TEST_FIXTURE(db_setup, Column_Find6)
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

TEST_FIXTURE(db_setup, Column_Find7)
{
    // expand to 16-bit width
    c.add(256);

    size_t res = c.find_first(256);
    CHECK_EQUAL(8, res);
}

TEST_FIXTURE(db_setup, Column_Find8)
{
    // expand to 32-bit width
    c.add(65536);

    size_t res = c.find_first(65536);
    CHECK_EQUAL(9, res);
}

TEST_FIXTURE(db_setup, Column_Find9)
{
    // expand to 64-bit width
    c.add(4294967296LL);

    size_t res = c.find_first(4294967296LL);
    CHECK_EQUAL(10, res);
}

TEST_FIXTURE(db_setup, Column_FindLeafs)
{
    Column a;

    // Create values that span multible leafs
    // we use 5 to ensure that we get two levels
    // when testing with MAX_LIST_SIZE=4
    for (size_t i = 0; i < MAX_LIST_SIZE*5; ++i) {
        a.add(0);
    }

    // Set sentinel values at before and after each break
    a.Set(0, 1);
    a.Set(MAX_LIST_SIZE-1, 2);
    a.Set(MAX_LIST_SIZE, 3);
    a.Set(MAX_LIST_SIZE*2-1, 4);
    a.Set(MAX_LIST_SIZE*2, 5);
    a.Set(MAX_LIST_SIZE*3-1, 6);
    a.Set(MAX_LIST_SIZE*3, 7);
    a.Set(MAX_LIST_SIZE*4-1, 8);
    a.Set(MAX_LIST_SIZE*4, 9);
    a.Set(MAX_LIST_SIZE*5-1, 10);

    const size_t res1 = a.find_first(1);
    const size_t res2 = a.find_first(2);
    const size_t res3 = a.find_first(3);
    const size_t res4 = a.find_first(4);
    const size_t res5 = a.find_first(5);
    const size_t res6 = a.find_first(6);
    const size_t res7 = a.find_first(7);
    const size_t res8 = a.find_first(8);
    const size_t res9 = a.find_first(9);
    const size_t res10 = a.find_first(10);

    CHECK_EQUAL(0, res1);
    CHECK_EQUAL(MAX_LIST_SIZE-1, res2);
    CHECK_EQUAL(MAX_LIST_SIZE, res3);
    CHECK_EQUAL(MAX_LIST_SIZE*2-1, res4);
    CHECK_EQUAL(MAX_LIST_SIZE*2, res5);
    CHECK_EQUAL(MAX_LIST_SIZE*3-1, res6);
    CHECK_EQUAL(MAX_LIST_SIZE*3, res7);
    CHECK_EQUAL(MAX_LIST_SIZE*4-1, res8);
    CHECK_EQUAL(MAX_LIST_SIZE*4, res9);
    CHECK_EQUAL(MAX_LIST_SIZE*5-1, res10);

    a.Destroy();
}

/* Partial find is not fully implemented yet
#define PARTIAL_COUNT 100
TEST_FIXTURE(db_setup, Column_PartialFind1)
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

TEST_FIXTURE(db_setup, Column_HeaderParse)
{
    Column column(c.GetRef(), (Array*)NULL, 0);
    const bool isEqual = (c == column);
    CHECK(isEqual);
}

TEST_FIXTURE(db_setup, Column_Destroy)
{
    // clean up (ALWAYS PUT THIS LAST)
    c.Destroy();
}

/*
TEST(Column_Sort)
{
    // Create Column with random values
    Column a;
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
*/

/** find_all() int tests spread out over bitwidth
 *
 */

TEST(Column_FindAll_IntMin)
{
    Column c;
    Array r;

    const int value = 0;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++){
        c.add(0);
    }

    c.find_all(r, value);
    CHECK_EQUAL(vReps, r.Size());

    size_t i = 0;
    size_t j = 0;
    while (i < c.Size()){
        if (c.Get(i) == value)
            CHECK_EQUAL(int64_t(i), r.Get(j++));
        i += 1;
    }

    // Cleanup
    c.Destroy();
    r.Destroy();
}

TEST(Column_FindAll_IntMax)
{
    Column c;
    Array r;

    const int64_t value = 4300000003ULL;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++){
        // 64 bitwidth
        c.add(4300000000ULL);
        c.add(4300000001ULL);
        c.add(4300000002ULL);
        c.add(4300000003ULL);
    }

    c.find_all(r, value);
    CHECK_EQUAL(vReps, r.Size());

    size_t i = 0;
    size_t j = 0;
    while (i < c.Size()){
        if (c.Get(i) == value)
            CHECK_EQUAL(int64_t(i), r.Get(j++));
        i += 1;
    }

    // Cleanup
    c.Destroy();
    r.Destroy();
}

/*
TEST(Column_FindHamming)
{
    Column col;
    for (size_t i = 0; i < 10; ++i) {
        col.add(0x5555555555555555LL);
        col.add(0x3333333333333333LL);
    }

    Array res;
    col.find_all_hamming(res, 0x3333333333333332LL, 2);

    CHECK_EQUAL(10, res.Size()); // Half should match

    // Clean up
    col.Destroy();
    res.Destroy();
}
*/


TEST(Column_Average)
{
    Column c;
    c.add(10);
    CHECK_EQUAL(10, c.average());
    
    c.add(30);
    CHECK_EQUAL(0, c.average(0,0));     // None
    CHECK_EQUAL(10, c.average(0,1));    // first
    CHECK_EQUAL(0, c.average(1,1));     // None
    CHECK_EQUAL(30, c.average(1,2));    // second
    CHECK_EQUAL(20, c.average(0,2));    // both
}

TEST(Column_Sum_Average)
{
    Column c;
    int64_t sum = 0;

    // Sum of 0 elements
    CHECK_EQUAL(0, c.sum());
    CHECK_EQUAL(0, c.average());

    // Sum of 1 elements
    c.add(123);
    CHECK_EQUAL(123, c.sum());
    CHECK_EQUAL(123, c.average());

    c.Clear();

    for (int i = 0; i < 100; i++) {
        c.add(i);
    }

    // Sum of entire range, using default args
    sum = 0;
    for (int i = 0; i < 100; i++) {
        sum += c.Get(i);
    }
    CHECK_EQUAL(sum, c.sum());
    CHECK_EQUAL(sum/100.0, c.average());

    // Sum of entire range, given explicit range
    sum = 0;
    for (int i = 0; i < 100; i++) {
        sum += c.Get(i);
    }
    CHECK_EQUAL(sum, c.sum(0, 100));
    CHECK_EQUAL(sum/100.0, c.average(0,100));

    // Start to N
    sum = 0;
    for (int i = 0; i < 63; i++) {
        sum += c.Get(i);
    }
    CHECK_EQUAL(sum, c.sum(0, 63));
    CHECK_EQUAL(sum/63.0, c.average(0, 63));

    // N to end
    sum = 0;
    for (int i = 47; i < 100; i++) {
        sum += c.Get(i);
    }
    CHECK_EQUAL(sum, c.sum(47, 100));
    CHECK_EQUAL(sum/(100.0-47.0), c.average(47, 100));

    // N to M
    sum = 0;
    for (int i = 55; i < 79; i++) {
        sum += c.Get(i);
    }
    CHECK_EQUAL(sum, c.sum(55, 79));
    CHECK_EQUAL(sum/(79.0-55.0), c.average(55, 79));

    c.Destroy();
}


TEST(Column_Max)
{
    Column c;
    int64_t t = c.maximum();
//    CHECK_EQUAL(0, t); // max on empty range returns zero // edit: is undefined!

    c.add(1);
    t = c.maximum();
    CHECK_EQUAL(1, t);

    c.Destroy();
}


TEST(Column_Max2)
{
    Column c;

    for (int i = 0; i < 100; i++) {
        c.add(10);
    }
    c.Set(20, 20);
    c.Set(50, 11); // Max must select *first* occurence of largest value
    c.Set(51, 11);
    c.Set(81, 20);

    int64_t t = c.maximum(51, 81);
    CHECK_EQUAL(11, t);

    c.Destroy();
}

TEST(Column_Min)
{
    Column c;
    int64_t t = c.minimum();
//    CHECK_EQUAL(0, t); // min on empty range returns zero // update: is undefined

    c.add(1);
    t = c.minimum();
    CHECK_EQUAL(1, t);

    c.Destroy();
}


TEST(Column_Min2)
{
    Column c;

    for (int i = 0; i < 100; i++) {
        c.add(10);
    }
    c.Set(20, 20);
    c.Set(50, 9); // Max must select *first* occurence of lowest value
    c.Set(51, 9);
    c.Set(81, 20);

    int64_t t = c.minimum(51, 81);
    CHECK_EQUAL(9, t);

    c.Destroy();
}

/*
TEST(Column_Sort2)
{
    Column c;

    for (size_t t = 0; t < 9*MAX_LIST_SIZE; t++)
        c.add(rand() % 300 - 100);

    c.sort();

    for (size_t t = 1; t < 9*MAX_LIST_SIZE; t++) {
        CHECK(c.Get(t) >= c.Get(t - 1));
    }

    c.Destroy();
}
*/


#if TEST_DURATION > 0

TEST(Column_prepend_many)
{
    // Test against a "Assertion failed: start < m_len, file src\Array.cpp, line 276" bug
    Column a;

    for (size_t items = 0; items < 3000; ++items) {
        a.Clear();
        for (size_t j = 0; j < items + 1; ++j) {
            a.Insert(0, j);
        }
        a.Insert(items, 444);
    }
    a.Destroy();
}

#endif
