#include "testsettings.hpp"
#ifdef TEST_COLUMN

#include <vector>
#include <algorithm>

#include <tightdb/column.hpp>

#include "test.hpp"

using namespace tightdb;
using namespace tightdb::test_util;


// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.


TEST(Column_Basic)
{
    ref_type ref = Column::create(Allocator::get_default());
    Column c(Allocator::get_default(), ref);

    // TEST(Column_IsEmpty)

    CHECK_EQUAL(0U, c.size());
    CHECK(c.is_empty());


    // TEST(Column_Add0)

    c.add(0);
    CHECK_EQUAL(0, c.get(0));
    CHECK_EQUAL(1U, c.size());
    CHECK(!c.is_empty());


    // TEST(Column_Add1)

    c.add(1);
    CHECK_EQUAL(0, c.get(0));
    CHECK_EQUAL(1, c.get(1));
    CHECK_EQUAL(2U, c.size());


    // TEST(Column_Add2)

    c.add(2);
    CHECK_EQUAL(0, c.get(0));
    CHECK_EQUAL(1, c.get(1));
    CHECK_EQUAL(2, c.get(2));
    CHECK_EQUAL(3U, c.size());


    // TEST(Column_Add3)

    c.add(3);
    CHECK_EQUAL(0, c.get(0));
    CHECK_EQUAL(1, c.get(1));
    CHECK_EQUAL(2, c.get(2));
    CHECK_EQUAL(3, c.get(3));
    CHECK_EQUAL(4U, c.size());


    // TEST(Column_Add4)

    c.add(4);
    CHECK_EQUAL(0, c.get(0));
    CHECK_EQUAL(1, c.get(1));
    CHECK_EQUAL(2, c.get(2));
    CHECK_EQUAL(3, c.get(3));
    CHECK_EQUAL(4, c.get(4));
    CHECK_EQUAL(5U, c.size());


    // TEST(Column_Add5)

    c.add(16);
    CHECK_EQUAL(0,  c.get(0));
    CHECK_EQUAL(1,  c.get(1));
    CHECK_EQUAL(2,  c.get(2));
    CHECK_EQUAL(3,  c.get(3));
    CHECK_EQUAL(4,  c.get(4));
    CHECK_EQUAL(16, c.get(5));
    CHECK_EQUAL(6U, c.size());


    // TEST(Column_Add6)

    c.add(256);
    CHECK_EQUAL(0,   c.get(0));
    CHECK_EQUAL(1,   c.get(1));
    CHECK_EQUAL(2,   c.get(2));
    CHECK_EQUAL(3,   c.get(3));
    CHECK_EQUAL(4,   c.get(4));
    CHECK_EQUAL(16,  c.get(5));
    CHECK_EQUAL(256, c.get(6));
    CHECK_EQUAL(7U, c.size());


    // TEST(Column_Add7)

    c.add(65536);
    CHECK_EQUAL(0,     c.get(0));
    CHECK_EQUAL(1,     c.get(1));
    CHECK_EQUAL(2,     c.get(2));
    CHECK_EQUAL(3,     c.get(3));
    CHECK_EQUAL(4,     c.get(4));
    CHECK_EQUAL(16,    c.get(5));
    CHECK_EQUAL(256,   c.get(6));
    CHECK_EQUAL(65536, c.get(7));
    CHECK_EQUAL(8U, c.size());


    // TEST(Column_Add8)

    c.add(4294967296LL);
    CHECK_EQUAL(0,            c.get(0));
    CHECK_EQUAL(1,            c.get(1));
    CHECK_EQUAL(2,            c.get(2));
    CHECK_EQUAL(3,            c.get(3));
    CHECK_EQUAL(4,            c.get(4));
    CHECK_EQUAL(16,           c.get(5));
    CHECK_EQUAL(256,          c.get(6));
    CHECK_EQUAL(65536,        c.get(7));
    CHECK_EQUAL(4294967296LL, c.get(8));
    CHECK_EQUAL(9U, c.size());


    // TEST(Column_AddNeg1)

    c.clear();

    c.add(-1);

    CHECK_EQUAL(1U, c.size());
    CHECK_EQUAL(-1, c.get(0));


    // TEST(Column_AddNeg2)

    c.add(-256);

    CHECK_EQUAL(2U, c.size());
    CHECK_EQUAL(-1,   c.get(0));
    CHECK_EQUAL(-256, c.get(1));


    // TEST(Column_AddNeg3)

    c.add(-65536);

    CHECK_EQUAL(3U, c.size());
    CHECK_EQUAL(-1,     c.get(0));
    CHECK_EQUAL(-256,   c.get(1));
    CHECK_EQUAL(-65536, c.get(2));


    // TEST(Column_AddNeg4)

    c.add(-4294967296LL);

    CHECK_EQUAL(4U, c.size());
    CHECK_EQUAL(-1,            c.get(0));
    CHECK_EQUAL(-256,          c.get(1));
    CHECK_EQUAL(-65536,        c.get(2));
    CHECK_EQUAL(-4294967296LL, c.get(3));


    // TEST(Column_Set)

    c.set(0, 3);
    c.set(1, 2);
    c.set(2, 1);
    c.set(3, 0);

    CHECK_EQUAL(4U, c.size());
    CHECK_EQUAL(3, c.get(0));
    CHECK_EQUAL(2, c.get(1));
    CHECK_EQUAL(1, c.get(2));
    CHECK_EQUAL(0, c.get(3));


    // TEST(Column_Insert1)

    // Set up some initial values
    c.clear();
    c.add(0);
    c.add(1);
    c.add(2);
    c.add(3);

    // Insert in middle
    c.insert(2, 16);

    CHECK_EQUAL(5U, c.size());
    CHECK_EQUAL(0,  c.get(0));
    CHECK_EQUAL(1,  c.get(1));
    CHECK_EQUAL(16, c.get(2));
    CHECK_EQUAL(2,  c.get(3));
    CHECK_EQUAL(3,  c.get(4));


    // TEST(Column_Insert2)

    // Insert at top
    c.insert(0, 256);

    CHECK_EQUAL(6U, c.size());
    CHECK_EQUAL(256, c.get(0));
    CHECK_EQUAL(0,   c.get(1));
    CHECK_EQUAL(1,   c.get(2));
    CHECK_EQUAL(16,  c.get(3));
    CHECK_EQUAL(2,   c.get(4));
    CHECK_EQUAL(3,   c.get(5));


    // TEST(Column_Insert3)

    // Insert at bottom
    c.insert(6, 65536);

    CHECK_EQUAL(7U,    c.size());
    CHECK_EQUAL(256,   c.get(0));
    CHECK_EQUAL(0,     c.get(1));
    CHECK_EQUAL(1,     c.get(2));
    CHECK_EQUAL(16,    c.get(3));
    CHECK_EQUAL(2,     c.get(4));
    CHECK_EQUAL(3,     c.get(5));
    CHECK_EQUAL(65536, c.get(6));


/*
    // TEST(Column_Index1)

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
*/


    // TEST(Column_Delete1)

    // Delete from middle
    c.erase(3, 3 == c.size()-1);

    CHECK_EQUAL(6U, c.size());
    CHECK_EQUAL(256,   c.get(0));
    CHECK_EQUAL(0,     c.get(1));
    CHECK_EQUAL(1,     c.get(2));
    CHECK_EQUAL(2,     c.get(3));
    CHECK_EQUAL(3,     c.get(4));
    CHECK_EQUAL(65536, c.get(5));


    // TEST(Column_Delete2)

    // Delete from top
    c.erase(0, 0 == c.size()-1);

    CHECK_EQUAL(5U, c.size());
    CHECK_EQUAL(0,     c.get(0));
    CHECK_EQUAL(1,     c.get(1));
    CHECK_EQUAL(2,     c.get(2));
    CHECK_EQUAL(3,     c.get(3));
    CHECK_EQUAL(65536, c.get(4));


    // TEST(Column_Delete3)

    // Delete from bottom
    c.erase(4, 4 == c.size()-1);

    CHECK_EQUAL(4U, c.size());
    CHECK_EQUAL(0, c.get(0));
    CHECK_EQUAL(1, c.get(1));
    CHECK_EQUAL(2, c.get(2));
    CHECK_EQUAL(3, c.get(3));


    // TEST(Column_DeleteAll)

    // Delete all items one at a time
    c.erase(0, 0 == c.size()-1);
    c.erase(0, 0 == c.size()-1);
    c.erase(0, 0 == c.size()-1);
    c.erase(0, 0 == c.size()-1);

    CHECK(c.is_empty());
    CHECK_EQUAL(0U, c.size());


    // TEST(Column_Find1)

    // Look for a non-existing value
    CHECK_EQUAL(size_t(-1), c.find_first(10));


    // TEST(Column_Find2)

    // zero-bit width
    c.clear();
    c.add(0);
    c.add(0);

    CHECK_EQUAL(0, c.find_first(0));


    // TEST(Column_Find3)

    // expand to 1-bit width
    c.add(1);

    CHECK_EQUAL(2, c.find_first(1));


    // TEST(Column_Find4)

    // expand to 2-bit width
    c.add(2);

    CHECK_EQUAL(3, c.find_first(2));


    // TEST(Column_Find5)

    // expand to 4-bit width
    c.add(4);

    CHECK_EQUAL(4, c.find_first(4));


    // TEST(Column_Find6)

    // expand to 8-bit width
    c.add(16);

    // Add some more to make sure we
    // can search in 64bit chunks
    c.add(16);
    c.add(7);

    CHECK_EQUAL(7, c.find_first(7));


    // TEST(Column_Find7)

    // expand to 16-bit width
    c.add(256);

    CHECK_EQUAL(8, c.find_first(256));


    // TEST(Column_Find8)

    // expand to 32-bit width
    c.add(65536);

    CHECK_EQUAL(9, c.find_first(65536));


    // TEST(Column_Find9)

    // expand to 64-bit width
    c.add(4294967296LL);

    CHECK_EQUAL(10, c.find_first(4294967296LL));


// Partial find is not fully implemented yet
/*
    // TEST(Column_PartialFind1)

    c.clear();

    size_t partial_count = 100;
    for (size_t i = 0; i < partial_count; ++i)
        c.add(i);

    CHECK_EQUAL(-1, c.find_first(partial_count+1, 0, partial_count));
    CHECK_EQUAL(-1, c.find_first(0, 1, partial_count));
    CHECK_EQUAL(partial_count-1, c.find_first(partial_count-1, partial_count-1, partial_count));
*/


    // TEST(Column_HeaderParse)

    Column column(Allocator::get_default(), c.get_ref());
    bool is_equal = c.compare_int(column);
    CHECK(is_equal);


    // TEST(Column_Destroy)

    c.destroy();
}


TEST(Column_FindLeafs)
{
    ref_type ref = Column::create(Allocator::get_default());
    Column a(Allocator::get_default(), ref);

    // Create values that span multible leaves
    // we use 5 to ensure that we get two levels
    // when testing with REALM_MAX_BPNODE_SIZE=4
    for (size_t i = 0; i < REALM_MAX_BPNODE_SIZE*5; ++i)
        a.add(0);

    // Set sentinel values at before and after each break
    a.set(0, 1);
    a.set(REALM_MAX_BPNODE_SIZE-1, 2);
    a.set(REALM_MAX_BPNODE_SIZE, 3);
    a.set(REALM_MAX_BPNODE_SIZE*2-1, 4);
    a.set(REALM_MAX_BPNODE_SIZE*2, 5);
    a.set(REALM_MAX_BPNODE_SIZE*3-1, 6);
    a.set(REALM_MAX_BPNODE_SIZE*3, 7);
    a.set(REALM_MAX_BPNODE_SIZE*4-1, 8);
    a.set(REALM_MAX_BPNODE_SIZE*4, 9);
    a.set(REALM_MAX_BPNODE_SIZE*5-1, 10);

    size_t res1 = a.find_first(1);
    size_t res2 = a.find_first(2);
    size_t res3 = a.find_first(3);
    size_t res4 = a.find_first(4);
    size_t res5 = a.find_first(5);
    size_t res6 = a.find_first(6);
    size_t res7 = a.find_first(7);
    size_t res8 = a.find_first(8);
    size_t res9 = a.find_first(9);
    size_t res10 = a.find_first(10);

    CHECK_EQUAL(0, res1);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE-1, res2);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE, res3);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE*2-1, res4);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE*2, res5);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE*3-1, res6);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE*3, res7);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE*4-1, res8);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE*4, res9);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE*5-1, res10);

    a.destroy();
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

    CHECK_EQUAL(0, a.get(0));
    CHECK_EQUAL(2, a.get(1));
    CHECK_EQUAL(3, a.get(2));
    CHECK_EQUAL(12, a.get(3));
    CHECK_EQUAL(17, a.get(4));
    CHECK_EQUAL(25, a.get(5));
    CHECK_EQUAL(34, a.get(6));
    CHECK_EQUAL(40, a.get(7));
    CHECK_EQUAL(50, a.get(8));
    CHECK_EQUAL(51, a.get(9));

    // Cleanup
    a.destroy();
}
*/

/** find_all() int tests spread out over bitwidth
 *
 */

TEST(Column_FindAllIntMin)
{
    ref_type ref_c = Column::create(Allocator::get_default());
    ref_type ref_r = Column::create(Allocator::get_default());
    Column c(Allocator::get_default(), ref_c);
    Column r(Allocator::get_default(), ref_r);

    const int value = 0;
    const int reps = 5;

    for (int i = 0; i < reps; i++)
        c.add(0);

    c.find_all(r, value);
    CHECK_EQUAL(reps, r.size());

    size_t i = 0;
    size_t j = 0;
    while (i < c.size()) {
        if (c.get(i) == value)
            CHECK_EQUAL(int64_t(i), r.get(j++));
        i += 1;
    }

    // Cleanup
    c.destroy();
    r.destroy();
}

TEST(Column_FindAllIntMax)
{
    ref_type ref_c = Column::create(Allocator::get_default());
    ref_type ref_r = Column::create(Allocator::get_default());
    Column c(Allocator::get_default(), ref_c);
    Column r(Allocator::get_default(), ref_r);

    const int64_t value = 4300000003ULL;
    const int reps = 5;

    for (int i = 0; i < reps; i++) {
        // 64 bitwidth
        c.add(4300000000ULL);
        c.add(4300000001ULL);
        c.add(4300000002ULL);
        c.add(4300000003ULL);
    }

    c.find_all(r, value);
    CHECK_EQUAL(reps, r.size());

    size_t i = 0;
    size_t j = 0;
    while (i < c.size()) {
        if (c.get(i) == value)
            CHECK_EQUAL(int64_t(i), r.get(j++));
        i += 1;
    }

    // Cleanup
    c.destroy();
    r.destroy();
}


TEST(Column_LowerUpperBound)
{
    // Create column with sorted members
    ref_type ref = Column::create(Allocator::get_default());
    Column col(Allocator::get_default(), ref);
    col.add(5);
    for (size_t i = 5; i < 100; i += 5)
        col.add(i);

    // before first entry
    CHECK_EQUAL(0, col.lower_bound_int(0));
    CHECK_EQUAL(0, col.upper_bound_int(0));

    // first entry (duplicate)
    CHECK_EQUAL(0, col.lower_bound_int(5));
    CHECK_EQUAL(2, col.upper_bound_int(5));

    // middle entry
    CHECK_EQUAL(10, col.lower_bound_int(50));
    CHECK_EQUAL(11, col.upper_bound_int(50));

    // non-existent middle entry
    CHECK_EQUAL(11, col.lower_bound_int(52));
    CHECK_EQUAL(11, col.upper_bound_int(52));

    // last entry
    CHECK_EQUAL(19, col.lower_bound_int(95));
    CHECK_EQUAL(20, col.upper_bound_int(95));

    // beyond last entry
    CHECK_EQUAL(20, col.lower_bound_int(96));
    CHECK_EQUAL(20, col.upper_bound_int(96));

    // Clean up
    col.destroy();
}


TEST(Column_Average)
{
    ref_type ref = Column::create(Allocator::get_default());
    Column c(Allocator::get_default(), ref);
    c.add(10);
    CHECK_EQUAL(10, c.average());

    c.add(30);
    CHECK_EQUAL(0, c.average(0,0));     // None
    CHECK_EQUAL(10, c.average(0,1));    // first
    CHECK_EQUAL(0, c.average(1,1));     // None
    CHECK_EQUAL(30, c.average(1,2));    // second
    CHECK_EQUAL(20, c.average(0,2));    // both

    c.destroy();
}

TEST(Column_SumAverage)
{
    ref_type ref = Column::create(Allocator::get_default());
    Column c(Allocator::get_default(), ref);
    int64_t sum = 0;

    // Sum of 0 elements
    CHECK_EQUAL(0, c.sum());
    CHECK_EQUAL(0, c.average());

    // Sum of 1 elements
    c.add(123);
    CHECK_EQUAL(123, c.sum());
    CHECK_EQUAL(123, c.average());

    c.clear();

    for (int i = 0; i < 100; i++)
        c.add(i);

    // Sum of entire range, using default args
    sum = 0;
    for (int i = 0; i < 100; i++)
        sum += c.get(i);
    CHECK_EQUAL(sum, c.sum());
    CHECK_EQUAL(sum/100.0, c.average());

    // Sum of entire range, given explicit range
    sum = 0;
    for (int i = 0; i < 100; i++)
        sum += c.get(i);
    CHECK_EQUAL(sum, c.sum(0, 100));
    CHECK_EQUAL(sum/100.0, c.average(0,100));

    // Start to N
    sum = 0;
    for (int i = 0; i < 63; i++)
        sum += c.get(i);
    CHECK_EQUAL(sum, c.sum(0, 63));
    CHECK_EQUAL(sum/63.0, c.average(0, 63));

    // N to end
    sum = 0;
    for (int i = 47; i < 100; i++)
        sum += c.get(i);
    CHECK_EQUAL(sum, c.sum(47, 100));
    CHECK_EQUAL(sum/(100.0-47.0), c.average(47, 100));

    // N to M
    sum = 0;
    for (int i = 55; i < 79; i++)
        sum += c.get(i);
    CHECK_EQUAL(sum, c.sum(55, 79));
    CHECK_EQUAL(sum/(79.0-55.0), c.average(55, 79));

    c.destroy();
}


TEST(Column_Max)
{
    ref_type ref = Column::create(Allocator::get_default());
    Column c(Allocator::get_default(), ref);
    int64_t t = c.maximum();
//    CHECK_EQUAL(0, t); // max on empty range returns zero // edit: is undefined!

    c.add(1);
    t = c.maximum();
    CHECK_EQUAL(1, t);

    c.destroy();
}


TEST(Column_Max2)
{
    ref_type ref = Column::create(Allocator::get_default());
    Column c(Allocator::get_default(), ref);

    for (int i = 0; i < 100; i++)
        c.add(10);
    c.set(20, 20);
    c.set(50, 11); // Max must select *first* occurence of largest value
    c.set(51, 11);
    c.set(81, 20);

    int64_t t = c.maximum(51, 81);
    CHECK_EQUAL(11, t);

    c.destroy();
}

TEST(Column_Min)
{
    ref_type ref = Column::create(Allocator::get_default());
    Column c(Allocator::get_default(), ref);
    int64_t t = c.minimum();
//    CHECK_EQUAL(0, t); // min on empty range returns zero // update: is undefined

    c.add(1);
    t = c.minimum();
    CHECK_EQUAL(1, t);

    c.destroy();
}


TEST(Column_Min2)
{
    ref_type ref = Column::create(Allocator::get_default());
    Column c(Allocator::get_default(), ref);

    for (int i = 0; i < 100; i++)
        c.add(10);
    c.set(20, 20);
    c.set(50, 9); // Max must select *first* occurence of lowest value
    c.set(51, 9);
    c.set(81, 20);

    int64_t t = c.minimum(51, 81);
    CHECK_EQUAL(9, t);

    c.destroy();
}


/*
TEST(Column_Sort2)
{
    Column c;

    Random random(random_int<unsigned long>()); // Seed from slow global generator
    for (size_t t = 0; t < 9*REALM_MAX_BPNODE_SIZE; t++)
        c.add(random.draw_int(-100, 199));

    c.sort();

    for (size_t t = 1; t < 9*REALM_MAX_BPNODE_SIZE; t++)
        CHECK(c.get(t) >= c.get(t - 1));

    c.destroy();
}
*/


TEST_IF(Column_PrependMany, TEST_DURATION >= 1)
{
    // Test against a "Assertion failed: start < m_len, file src\Array.cpp, line 276" bug
    ref_type ref = Column::create(Allocator::get_default());
    Column a(Allocator::get_default(), ref);

    for (size_t items = 0; items < 3000; ++items) {
        a.clear();
        for (size_t j = 0; j < items + 1; ++j)
            a.insert(0, j);
        a.insert(items, 444);
    }
    a.destroy();
}

#endif // TEST_COLUMN
