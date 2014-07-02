#include "testsettings.hpp"
#ifdef TEST_ARRAY

#include <cstdlib>
#include <algorithm>
#include <string>
#include <vector>
#include <map>

#include <tightdb/array.hpp>
#include <tightdb/column.hpp>
#include <tightdb/query_conditions.hpp>

#include "test.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::test_util;
using unit_test::TestResults;


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


namespace {

void has_zero_byte(TestResults& test_results, int64_t value, size_t reps)
{
    Array a;
    Column r;

    for (size_t i = 0; i < reps - 1; ++i)
        a.add(value);

    a.add(0);

    size_t t = a.find_first(0);
    CHECK_EQUAL(a.size() - 1, t);

    r.clear();
    a.find_all(&r, 0);
    CHECK_EQUAL(int64_t(a.size() - 1), r.get(0));

    // Cleanup
    a.destroy();
    r.destroy();
}

} // anonymous namespace


TEST(Array_General)
{
    Array c;

    // TEST(Array_Add0)

    c.add(0);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.size(), 1);
    CHECK_EQUAL(0, c.get_width());


    // TEST(Array_Add1)

    c.add(1);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.size(), 2);
    CHECK_EQUAL(1, c.get_width());


    // TEST(Array_Add2)

    c.add(2);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 2);
    CHECK_EQUAL(c.size(), 3);
    CHECK_EQUAL(2, c.get_width());


    // TEST(Array_Add3)

    c.add(3);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 2);
    CHECK_EQUAL(c.get(3), 3);
    CHECK_EQUAL(c.size(), 4);
    CHECK_EQUAL(2, c.get_width());


    // TEST(Array_Add4)

    c.add(4);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 2);
    CHECK_EQUAL(c.get(3), 3);
    CHECK_EQUAL(c.get(4), 4);
    CHECK_EQUAL(c.size(), 5);
    CHECK_EQUAL(4, c.get_width());


    // TEST(Array_Add5)

    c.add(16);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 2);
    CHECK_EQUAL(c.get(3), 3);
    CHECK_EQUAL(c.get(4), 4);
    CHECK_EQUAL(c.get(5), 16);
    CHECK_EQUAL(c.size(), 6);
    CHECK_EQUAL(8, c.get_width());


    // TEST(Array_Add6)

    c.add(256);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 2);
    CHECK_EQUAL(c.get(3), 3);
    CHECK_EQUAL(c.get(4), 4);
    CHECK_EQUAL(c.get(5), 16);
    CHECK_EQUAL(c.get(6), 256);
    CHECK_EQUAL(c.size(), 7);
    CHECK_EQUAL(16, c.get_width());


    // TEST(Array_Add7)

    c.add(65536);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 2);
    CHECK_EQUAL(c.get(3), 3);
    CHECK_EQUAL(c.get(4), 4);
    CHECK_EQUAL(c.get(5), 16);
    CHECK_EQUAL(c.get(6), 256);
    CHECK_EQUAL(c.get(7), 65536);
    CHECK_EQUAL(c.size(), 8);
    CHECK_EQUAL(32, c.get_width());


    // TEST(Array_Add8)

    c.add(4294967296LL);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 2);
    CHECK_EQUAL(c.get(3), 3);
    CHECK_EQUAL(c.get(4), 4);
    CHECK_EQUAL(c.get(5), 16);
    CHECK_EQUAL(c.get(6), 256);
    CHECK_EQUAL(c.get(7), 65536);
    CHECK_EQUAL(c.get(8), 4294967296LL);
    CHECK_EQUAL(c.size(), 9);
    CHECK_EQUAL(64, c.get_width());


    // TEST(Array_AddNeg1)

    c.clear();
    c.add(-1);

    CHECK_EQUAL(c.size(), 1);
    CHECK_EQUAL(c.get(0), -1);
    CHECK_EQUAL(8, c.get_width());


    // TEST(Array_AddNeg2)

    c.add(-256);

    CHECK_EQUAL(c.size(), 2);
    CHECK_EQUAL(c.get(0), -1);
    CHECK_EQUAL(c.get(1), -256);
    CHECK_EQUAL(16, c.get_width());


    // TEST(Array_AddNeg3)

    c.add(-65536);

    CHECK_EQUAL(c.size(), 3);
    CHECK_EQUAL(c.get(0), -1);
    CHECK_EQUAL(c.get(1), -256);
    CHECK_EQUAL(c.get(2), -65536);
    CHECK_EQUAL(32, c.get_width());


    // TEST(Array_AddNeg4)

    c.add(-4294967296LL);

    CHECK_EQUAL(c.size(), 4);
    CHECK_EQUAL(c.get(0), -1);
    CHECK_EQUAL(c.get(1), -256);
    CHECK_EQUAL(c.get(2), -65536);
    CHECK_EQUAL(c.get(3), -4294967296LL);
    CHECK_EQUAL(64, c.get_width());


    // TEST(Array_Set)

    c.set(0, 3);
    c.set(1, 2);
    c.set(2, 1);
    c.set(3, 0);

    CHECK_EQUAL(c.size(), 4);
    CHECK_EQUAL(c.get(0), 3);
    CHECK_EQUAL(c.get(1), 2);
    CHECK_EQUAL(c.get(2), 1);
    CHECK_EQUAL(c.get(3), 0);


    // TEST(Array_Insert1)

    // Set up some initial values
    c.clear();
    c.add(0);
    c.add(1);
    c.add(2);
    c.add(3);

    // Insert in middle
    c.insert(2, 16);

    CHECK_EQUAL(c.size(), 5);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 16);
    CHECK_EQUAL(c.get(3), 2);
    CHECK_EQUAL(c.get(4), 3);


    // TEST(Array_Insert2)

    // Insert at top
    c.insert(0, 256);

    CHECK_EQUAL(c.size(), 6);
    CHECK_EQUAL(c.get(0), 256);
    CHECK_EQUAL(c.get(1), 0);
    CHECK_EQUAL(c.get(2), 1);
    CHECK_EQUAL(c.get(3), 16);
    CHECK_EQUAL(c.get(4), 2);
    CHECK_EQUAL(c.get(5), 3);


    // TEST(Array_Insert3)

    // Insert at bottom
    c.insert(6, 65536);

    CHECK_EQUAL(c.size(), 7);
    CHECK_EQUAL(c.get(0), 256);
    CHECK_EQUAL(c.get(1), 0);
    CHECK_EQUAL(c.get(2), 1);
    CHECK_EQUAL(c.get(3), 16);
    CHECK_EQUAL(c.get(4), 2);
    CHECK_EQUAL(c.get(5), 3);
    CHECK_EQUAL(c.get(6), 65536);


    // TEST(Array_Index1)

/*
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


    // TEST(Array_Delete1)

    // Delete from middle
    c.erase(3);

    CHECK_EQUAL(c.size(), 6);
    CHECK_EQUAL(c.get(0), 256);
    CHECK_EQUAL(c.get(1), 0);
    CHECK_EQUAL(c.get(2), 1);
    CHECK_EQUAL(c.get(3), 2);
    CHECK_EQUAL(c.get(4), 3);
    CHECK_EQUAL(c.get(5), 65536);


    // TEST(Array_Delete2)

    // Delete from top
    c.erase(0);

    CHECK_EQUAL(c.size(), 5);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 2);
    CHECK_EQUAL(c.get(3), 3);
    CHECK_EQUAL(c.get(4), 65536);


    // TEST(Array_Delete3)

    // Delete from bottom
    c.erase(4);

    CHECK_EQUAL(c.size(), 4);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 2);
    CHECK_EQUAL(c.get(3), 3);


    // TEST(Array_DeleteAll)

    // Delete all items one at a time
    c.erase(0);
    c.erase(0);
    c.erase(0);
    c.erase(0);

    CHECK(c.is_empty());
    CHECK_EQUAL(0, c.size());


    // TEST(Array_Find1)

    // Look for a non-existing value
    CHECK_EQUAL(size_t(-1), c.find_first(10));


    // TEST(Array_Find2)

    // zero-bit width
    c.clear();
    c.add(0);
    c.add(0);

    CHECK_EQUAL(0, c.find_first(0));


    // TEST(Array_Find3)

    // expand to 1-bit width
    c.add(1);

    CHECK_EQUAL(2, c.find_first(1));


    // TEST(Array_Find4)

    // expand to 2-bit width
    c.add(2);

    CHECK_EQUAL(3, c.find_first(2));


    // TEST(Array_Find5)

    // expand to 4-bit width
    c.add(4);

    CHECK_EQUAL(4, c.find_first(4));


    // TEST(Array_Find6)

    // expand to 8-bit width
    c.add(16);

    // Add some more to make sure we
    // can search in 64bit chunks
    c.add(16);
    c.add(7);

    CHECK_EQUAL(7, c.find_first(7));


    // TEST(Array_Find7)

    // expand to 16-bit width
    c.add(256);

    CHECK_EQUAL(8, c.find_first(256));


    // TEST(Array_Find8)

    // expand to 32-bit width
    c.add(65536);

    CHECK_EQUAL(9, c.find_first(65536));


    // TEST(Array_Find9)

    // expand to 64-bit width
    c.add(4294967296LL);

    CHECK_EQUAL(10, c.find_first(4294967296LL));


// Partial find is not fully implemented yet
/*
    // TEST(Array_PartialFind1)

    c.clear();

    size_t partial_count = 100;
    for (size_t i = 0; i != partial_count; ++i)
        c.add(i);

    CHECK_EQUAL(-1, c.find_first(partial_count+1, 0, partial_count));
    CHECK_EQUAL(-1, c.find_first(0, 1, partial_count));
    CHECK_EQUAL(partial_count-1, c.find_first(partial_count-1, partial_count-1, partial_count));
*/

    // TEST(Array_Destroy)

    c.destroy();
}


TEST(Array_AddNeg1_1)
{
    Array c;

    c.add(1);
    c.add(2);
    c.add(3);
    c.add(-128);

    CHECK_EQUAL(c.size(), 4);
    CHECK_EQUAL(c.get(0), 1);
    CHECK_EQUAL(c.get(1), 2);
    CHECK_EQUAL(c.get(2), 3);
    CHECK_EQUAL(c.get(3), -128);
    CHECK_EQUAL(8, c.get_width());

    c.destroy();
}




// Oops, see Array_LowerUpperBound
TEST(Array_UpperLowerBound)
{
    // Tests Array::upper_bound() and Array::lower_bound()
    // This test is independent of TIGHTDB_MAX_LIST_SIZE
    Array a;
    vector<int> v;
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    // we use 4 as constant in order to make border case sequences of
    // v, v, v and v, v+1, v+2, etc, probable
    for (int i=0; i< (1000 * (1 + TEST_DURATION * TEST_DURATION * TEST_DURATION *
                              TEST_DURATION * TEST_DURATION)) ; i++) {
        int elements = random.draw_int_mod(64);
        int val = random.draw_int_mod(4); // random start value

        a.clear();
        v.clear();

        for (int e = 0; e < elements; e++) {
            a.add(val);
            v.push_back(val);
            val += random.draw_int_mod(4);
        }

        int64_t searches = val; // val exceeds last value by random.draw_int_mod(4)
        for (int64_t s = 0; s < searches; s++) {
            size_t uarr = a.upper_bound_int(s);
            size_t larr = a.lower_bound_int(s);
            size_t uvec = upper_bound(v.begin(), v.end(), s) - v.begin();
            size_t lvec = lower_bound(v.begin(), v.end(), s) - v.begin();

            CHECK_EQUAL(uvec, uarr);
            CHECK_EQUAL(lvec, larr);
        }
    }
    a.destroy();
}


TEST(Array_LowerUpperBound)
{
    Array a;
    a.add(10);
    a.add(20);
    a.add(30);
    a.add(40);
    a.add(50);
    a.add(60);
    a.add(70);
    a.add(80);

    CHECK_EQUAL(0, a.lower_bound_int(0));  CHECK_EQUAL(0, a.upper_bound_int(0));
    CHECK_EQUAL(0, a.lower_bound_int(1));  CHECK_EQUAL(0, a.upper_bound_int(1));
    CHECK_EQUAL(0, a.lower_bound_int(9));  CHECK_EQUAL(0, a.upper_bound_int(9));
    CHECK_EQUAL(0, a.lower_bound_int(10)); CHECK_EQUAL(1, a.upper_bound_int(10));
    CHECK_EQUAL(1, a.lower_bound_int(11)); CHECK_EQUAL(1, a.upper_bound_int(11));
    CHECK_EQUAL(1, a.lower_bound_int(19)); CHECK_EQUAL(1, a.upper_bound_int(19));
    CHECK_EQUAL(1, a.lower_bound_int(20)); CHECK_EQUAL(2, a.upper_bound_int(20));
    CHECK_EQUAL(2, a.lower_bound_int(21)); CHECK_EQUAL(2, a.upper_bound_int(21));
    CHECK_EQUAL(2, a.lower_bound_int(29)); CHECK_EQUAL(2, a.upper_bound_int(29));
    CHECK_EQUAL(2, a.lower_bound_int(30)); CHECK_EQUAL(3, a.upper_bound_int(30));
    CHECK_EQUAL(3, a.lower_bound_int(31)); CHECK_EQUAL(3, a.upper_bound_int(31));
    CHECK_EQUAL(3, a.lower_bound_int(32)); CHECK_EQUAL(3, a.upper_bound_int(32));
    CHECK_EQUAL(3, a.lower_bound_int(39)); CHECK_EQUAL(3, a.upper_bound_int(39));
    CHECK_EQUAL(3, a.lower_bound_int(40)); CHECK_EQUAL(4, a.upper_bound_int(40));
    CHECK_EQUAL(4, a.lower_bound_int(41)); CHECK_EQUAL(4, a.upper_bound_int(41));
    CHECK_EQUAL(4, a.lower_bound_int(42)); CHECK_EQUAL(4, a.upper_bound_int(42));
    CHECK_EQUAL(4, a.lower_bound_int(49)); CHECK_EQUAL(4, a.upper_bound_int(49));
    CHECK_EQUAL(4, a.lower_bound_int(50)); CHECK_EQUAL(5, a.upper_bound_int(50));
    CHECK_EQUAL(5, a.lower_bound_int(51)); CHECK_EQUAL(5, a.upper_bound_int(51));
    CHECK_EQUAL(5, a.lower_bound_int(52)); CHECK_EQUAL(5, a.upper_bound_int(52));
    CHECK_EQUAL(5, a.lower_bound_int(59)); CHECK_EQUAL(5, a.upper_bound_int(59));
    CHECK_EQUAL(5, a.lower_bound_int(60)); CHECK_EQUAL(6, a.upper_bound_int(60));
    CHECK_EQUAL(6, a.lower_bound_int(61)); CHECK_EQUAL(6, a.upper_bound_int(61));
    CHECK_EQUAL(6, a.lower_bound_int(62)); CHECK_EQUAL(6, a.upper_bound_int(62));
    CHECK_EQUAL(6, a.lower_bound_int(69)); CHECK_EQUAL(6, a.upper_bound_int(69));
    CHECK_EQUAL(6, a.lower_bound_int(70)); CHECK_EQUAL(7, a.upper_bound_int(70));
    CHECK_EQUAL(7, a.lower_bound_int(71)); CHECK_EQUAL(7, a.upper_bound_int(71));
    CHECK_EQUAL(7, a.lower_bound_int(72)); CHECK_EQUAL(7, a.upper_bound_int(72));
    CHECK_EQUAL(7, a.lower_bound_int(78)); CHECK_EQUAL(7, a.upper_bound_int(78));
    CHECK_EQUAL(7, a.lower_bound_int(79)); CHECK_EQUAL(7, a.upper_bound_int(79));
    CHECK_EQUAL(7, a.lower_bound_int(80)); CHECK_EQUAL(8, a.upper_bound_int(80));
    CHECK_EQUAL(8, a.lower_bound_int(81)); CHECK_EQUAL(8, a.upper_bound_int(81));
    CHECK_EQUAL(8, a.lower_bound_int(82)); CHECK_EQUAL(8, a.upper_bound_int(82));

    a.destroy();
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

/** find_all() int tests spread out over bitwidth
 *
 */


TEST(Array_FindAllInt0)
{
    Array a;
    Column r;

    const int value = 0;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++){
        a.add(0);
    }

    a.find_all(&r, value);
    CHECK_EQUAL(vReps, r.size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.size()){
        if (a.get(i) == value)
            CHECK_EQUAL(int64_t(i), r.get(j++));
        i += 1;
    }

    // Cleanup
    a.destroy();
    r.destroy();
}

TEST(Array_FindAllInt1)
{
    Array a;
    Column r;

    const int value = 1;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++){
        a.add(0);
        a.add(0);
        a.add(1);
        a.add(0);
    }

    a.find_all(&r, value);
    CHECK_EQUAL(vReps, r.size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.size()){
        if (a.get(i) == value)
            CHECK_EQUAL(int64_t(i), r.get(j++));
        i += 1;
    }

    // Cleanup
    a.destroy();
    r.destroy();
}

TEST(Array_FindAllInt2)
{
    Array a;
    Column r;

    const int value = 3;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++){
        a.add(0);
        a.add(1);
        a.add(2);
        a.add(3);
    }

    a.find_all(&r, value);
    CHECK_EQUAL(vReps, r.size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.size()){
        if (a.get(i) == value)
            CHECK_EQUAL(int64_t(i), r.get(j++));
        i += 1;
    }

    // Cleanup
    a.destroy();
    r.destroy();
}

TEST(Array_FindAllInt3)
{
    Array a;
    Column r;

    const int value = 10;
    const int vReps = 5;
    // 0, 4, 8
    for (int i = 0; i < vReps; i++){
        a.add(10);
        a.add(11);
        a.add(12);
        a.add(13);
    }

    a.find_all(&r, value);
    CHECK_EQUAL(vReps, r.size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.size()){
        if (a.get(i) == value)
            CHECK_EQUAL(int64_t(i), r.get(j++));
        i += 1;
    }

    // Cleanup
    a.destroy();
    r.destroy();
}

TEST(Array_FindAllInt4)
{
    Array a;
    Column r;

    const int value = 20;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++){
        // 8 bitwidth
        a.add(20);
        a.add(21);
        a.add(22);
        a.add(23);
    }

    a.find_all(&r, value);
    CHECK_EQUAL(vReps, r.size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.size()){
        if (a.get(i) == value)
            CHECK_EQUAL(int64_t(i), r.get(j++));
        i += 1;
    }

    // Cleanup
    a.destroy();
    r.destroy();
}

TEST(Array_FindAllInt5)
{
    Array a;
    Column r;

    const int value = 303;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++){
        // 16 bitwidth
        a.add(300);
        a.add(301);
        a.add(302);
        a.add(303);
    }

    a.find_all(&r, value);
    CHECK_EQUAL(vReps, r.size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.size()){
        if (a.get(i) == value)
            CHECK_EQUAL(int64_t(i), r.get(j++));
        i += 1;
    }

    // Cleanup
    a.destroy();
    r.destroy();
}

TEST(Array_FindAllInt6)
{
    Array a;
    Column r;

    const int value = 70000;
    const int vReps = 5;

    for (int i = 0; i < vReps; ++i) {
        // 32 bitwidth
        a.add(70000);
        a.add(70001);
        a.add(70002);
        a.add(70003);
    }

    a.find_all(&r, value);
    CHECK_EQUAL(vReps, r.size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.size()) {
        if (a.get(i) == value)
            CHECK_EQUAL(int64_t(i), r.get(j++));
        i += 1;
    }

    // Cleanup
    a.destroy();
    r.destroy();
}

TEST(Array_FindAllInt7)
{
    Array a;
    Column r;

    const int64_t value = 4300000003ULL;
    const int vReps = 5;

    for (int i = 0; i < vReps; ++i) {
        // 64 bitwidth
        a.add(4300000000ULL);
        a.add(4300000001ULL);
        a.add(4300000002ULL);
        a.add(4300000003ULL);
    }

    a.find_all(&r, value);
    CHECK_EQUAL(vReps, r.size());

    size_t i = 0;
    size_t j = 0;
    while (i < a.size()) {
        if (a.get(i) == value)
            CHECK_EQUAL(int64_t(i), r.get(j++));
        i += 1;
    }

    // Cleanup
    a.destroy();
    r.destroy();
}

// Tests the case where a value does *not* exist in one entire 64-bit chunk (triggers the 'if (has_zero_byte()) break;' condition)
TEST(Array_FindHasZeroByte)
{
    // we want at least 1 entire 64-bit chunk-test, and we also want a remainder-test, so we chose n to be a prime > 64
    size_t n = 73;
    has_zero_byte(test_results, 1, n); // width = 1
    has_zero_byte(test_results, 3, n); // width = 2
    has_zero_byte(test_results, 13, n); // width = 4
    has_zero_byte(test_results, 100, n); // 8
    has_zero_byte(test_results, 10000, n); // 16
    has_zero_byte(test_results, 100000, n); // 32
    has_zero_byte(test_results, 8000000000LL, n); // 64
}

// New find test for SSE search, to trigger partial finds (see FindSSE()) before and after the aligned data area
TEST(Array_FindSSE)
{
    Array a;
    for (uint64_t i = 0; i < 100; ++i) {
        a.add(10000);
    }

    for (size_t i = 0; i < 100; ++i) {
        a.set(i, 123);
        size_t t = a.find_first(123);
        TIGHTDB_ASSERT(t == i);
        a.set(i, 10000);
        static_cast<void>(t);
    }
    a.destroy();
}


TEST(Array_Sum0)
{
    Array a;
    for (int i = 0; i < 64 + 7; ++i) {
        a.add(0);
    }
    CHECK_EQUAL(0, a.sum(0, a.size()));
    a.destroy();
}

TEST(Array_Sum1)
{
    int64_t s1 = 0;
    Array a;
    for (int i = 0; i < 256 + 7; ++i)
        a.add(i % 2);

    s1 = 0;
    for (int i = 0; i < 256 + 7; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(0, a.size()));

    s1 = 0;
    for (int i = 3; i < 100; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(3, 100));

    a.destroy();
}

TEST(Array_Sum2)
{
    int64_t s1 = 0;
    Array a;
    for (int i = 0; i < 256 + 7; ++i)
        a.add(i % 4);

    s1 = 0;
    for (int i = 0; i < 256 + 7; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(0, a.size()));

    s1 = 0;
    for (int i = 3; i < 100; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(3, 100));

    a.destroy();
}


TEST(Array_Sum4)
{
    int64_t s1 = 0;
    Array a;
    for (int i = 0; i < 256 + 7; ++i)
        a.add(i % 16);

    s1 = 0;
    for (int i = 0; i < 256 + 7; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(0, a.size()));

    s1 = 0;
    for (int i = 3; i < 100; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(3, 100));

    a.destroy();
}

TEST(Array_Sum16)
{
    int64_t s1 = 0;
    Array a;
    for (int i = 0; i < 256 + 7; ++i)
        a.add(i % 30000);

    s1 = 0;
    for (int i = 0; i < 256 + 7; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(0, a.size()));

    s1 = 0;
    for (int i = 3; i < 100; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(3, 100));

    a.destroy();
}

TEST(Array_Greater)
{
    Array a;

    size_t items = 400;

    for (items = 2; items < 200; items += 7)
    {

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(0);
        }
        size_t t = a.find_first<Greater>(0, 0, size_t(-1));
        CHECK_EQUAL(size_t(-1), t);


        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(0);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 1);

            size_t t = a.find_first<Greater>(0, 0, size_t(-1));
            TIGHTDB_ASSERT(i == t);

            CHECK_EQUAL(i, t);
            a.set(i, 0);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(2);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 3);
            size_t t = a.find_first<Greater>(2, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 2);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(10);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 11);
            size_t t = a.find_first<Greater>(10, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 10);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(100);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 110);
            size_t t = a.find_first<Greater>(100, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 100);
        }
        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(200);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 210);
            size_t t = a.find_first<Greater>(200, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 200);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(10000);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 11000);
            size_t t = a.find_first<Greater>(10000, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 10000);
        }
        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(40000);
        }

        for (size_t i = 0; i < items; ++i) {
            a.set(i, 41000);
            size_t t = a.find_first<Greater>(40000, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 40000);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(1000000);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 1100000);
            size_t t = a.find_first<Greater>(1000000, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 1000000);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(1000ULL*1000ULL*1000ULL*1000ULL);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 1000ULL*1000ULL*1000ULL*1000ULL + 1ULL);
            size_t t = a.find_first<Greater>(1000ULL*1000ULL*1000ULL*1000ULL, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 1000ULL*1000ULL*1000ULL*1000ULL);
        }

    }
    a.destroy();
}




TEST(Array_Less)
{
    Array a;

    size_t items = 400;

    for (items = 2; items < 200; items += 7)
    {

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(0);
        }
        size_t t = a.find_first<Less>(0, 0, size_t(-1));
        CHECK_EQUAL(size_t(-1), t);


        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(1);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 0);
            size_t t = a.find_first<Less>(1, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 1);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(3);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 2);
            size_t t = a.find_first<Less>(3, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 3);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(11);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 10);
            size_t t = a.find_first<Less>(11, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 11);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(110);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 100);
            size_t t = a.find_first<Less>(110, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 110);
        }
        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(210);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 200);
            size_t t = a.find_first<Less>(210, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 210);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(11000);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 10000);
            size_t t = a.find_first<Less>(11000, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 11000);
        }
        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(41000);
        }

        for (size_t i = 0; i < items; ++i) {
            a.set(i, 40000);
            size_t t = a.find_first<Less>(41000, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 41000);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(1100000);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 1000000);
            size_t t = a.find_first<Less>(1100000, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 1100000);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(1000ULL*1000ULL*1000ULL*1000ULL);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 1000ULL*1000ULL*1000ULL*1000ULL - 1ULL);
            size_t t = a.find_first<Less>(1000ULL*1000ULL*1000ULL*1000ULL, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 1000ULL*1000ULL*1000ULL*1000ULL);
        }

    }
    a.destroy();
}


TEST(Array_NotEqual1)
{
    Array a;

    a.clear();
    for (size_t i = 0; i < 100; ++i) {
        a.add(0x33);
    }
    a.set(50, 0x44);
    size_t t = a.find_first<NotEqual>(0x33, 0, size_t(-1));
    CHECK_EQUAL(50, t);
    a.destroy();
}

TEST(Array_NotEqual)
{
    Array a;

    size_t items = 400;

    for (items = 2; items < 200; items += 7)
    {
        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(0);
        }
        size_t t = a.find_first<NotEqual>(0, 0, size_t(-1));
        CHECK_EQUAL(size_t(-1), t);


        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(0);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 1);
            size_t t = a.find_first<NotEqual>(0, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 0);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(2);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 3);
            size_t t = a.find_first<NotEqual>(2, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 2);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(10);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 11);
            size_t t = a.find_first<NotEqual>(10, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 10);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(100);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 110);
            size_t t = a.find_first<NotEqual>(100, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 100);
        }
        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(200);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 210);
            size_t t = a.find_first<NotEqual>(200, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 200);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(10000);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 11000);
            size_t t = a.find_first<NotEqual>(10000, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 10000);
        }
        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(40000);
        }

        for (size_t i = 0; i < items; ++i) {
            a.set(i, 41000);
            size_t t = a.find_first<NotEqual>(40000, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 40000);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(1000000);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 1100000);
            size_t t = a.find_first<NotEqual>(1000000, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 1000000);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(1000ULL*1000ULL*1000ULL*1000ULL);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 1000ULL*1000ULL*1000ULL*1000ULL + 1ULL);
            size_t t = a.find_first<NotEqual>(1000ULL*1000ULL*1000ULL*1000ULL, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 1000ULL*1000ULL*1000ULL*1000ULL);
        }

    }
    a.destroy();
}




TEST(Array_Sort1)
{
    // negative values
    Array a;

    Random random(random_int<unsigned long>()); // Seed from slow global generator
    for (size_t t = 0; t < 400; ++t)
        a.add(random.draw_int(-100, 199));

    size_t orig_size = a.size();
    a.sort();

    CHECK(a.size() == orig_size);
    for (size_t t = 1; t < a.size(); ++t)
        CHECK(a.get(t) >= a.get(t - 1));

    a.destroy();
}


TEST(Array_Sort2)
{
    // 64 bit values
    Array a;

    Random random(random_int<unsigned long>()); // Seed from slow global generator
    for (size_t t = 0; t < 400; ++t) {
        int_fast64_t v = 1;
        for (int i = 0; i != 8; ++i)
            v *= int64_t(random.draw_int_max(RAND_MAX));
        a.add(v);
    }

    size_t orig_size = a.size();
    a.sort();

    CHECK(a.size() == orig_size);
    for (size_t t = 1; t < a.size(); ++t)
        CHECK(a.get(t) >= a.get(t - 1));

    a.destroy();
}

TEST(Array_Sort3)
{
    // many values
    Array a;

    Random random(random_int<unsigned long>()); // Seed from slow global generator
    for (size_t t = 0; t < 1000000ULL; ++t)
        a.add(random.draw_int_max(RAND_MAX));

    size_t orig_size = a.size();
    a.sort();

    CHECK(a.size() == orig_size);
    for (size_t t = 1; t < a.size(); ++t)
        CHECK(a.get(t) >= a.get(t - 1));

    a.destroy();
}


TEST(Array_Sort4)
{
    // same values
    Array a;

    for (size_t t = 0; t < 1000; ++t)
        a.add(0);

    size_t orig_size = a.size();
    a.sort();

    CHECK(a.size() == orig_size);
    for (size_t t = 1; t < a.size(); ++t)
        CHECK(a.get(t) == 0);

    a.destroy();
}

TEST(Array_Copy)
{
    Array a;
    a.add(0);
    a.add(1);
    a.add(2);
    a.add(3);
    a.add(4);

    Array b(a, Allocator::get_default());

#ifdef TIGHTDB_DEBUG
    b.Verify();
#endif

    CHECK_EQUAL(5, b.size());
    CHECK_EQUAL(0, b.get(0));
    CHECK_EQUAL(1, b.get(1));
    CHECK_EQUAL(2, b.get(2));
    CHECK_EQUAL(3, b.get(3));
    CHECK_EQUAL(4, b.get(4));

    // With sub-arrays
    Array c(Array::type_HasRefs);
    c.add(a.get_ref());

    Array d(c, Allocator::get_default());

#ifdef TIGHTDB_DEBUG
    b.Verify(); // FIXME: Should this not have been d.Verify()?
#endif

    CHECK(d.has_refs());
    CHECK_EQUAL(1, d.size());

    Array e(d.get_alloc());
    e.init_from_ref(to_ref(d.get(0)));
    CHECK_EQUAL(5, e.size());
    CHECK_EQUAL(0, e.get(0));
    CHECK_EQUAL(1, e.get(1));
    CHECK_EQUAL(2, e.get(2));
    CHECK_EQUAL(3, e.get(3));
    CHECK_EQUAL(4, e.get(4));

    b.destroy();
    c.destroy_deep();
    d.destroy_deep();
}

TEST(Array_Count)
{
    Array a;

    // 0 bit width
    for (size_t i = 0; i < 150; ++i)
        a.add(0);
    const size_t c1 = a.count(0);
    const size_t c2 = a.count(1);
    CHECK_EQUAL(150, c1);
    CHECK_EQUAL(0, c2);
    CHECK_EQUAL(0, a.count(-1));
    CHECK_EQUAL(0, a.count(2));

    // 1 bit width
    for (size_t i = 0; i < 100; ++i) {
        if (i % 2) a.set(i, 1);
    }
    const size_t c3 = a.count(0);
    const size_t c4 = a.count(1);
    CHECK_EQUAL(100, c3);
    CHECK_EQUAL(50, c4);
    CHECK_EQUAL(0, a.count(-1));
    CHECK_EQUAL(0, a.count(4));

    // 2 bit width
    for (size_t i = 0; i < 100; ++i) {
        if (i % 2) a.set(i, 2);
    }
    const size_t c5 = a.count(0);
    const size_t c6 = a.count(2);
    CHECK_EQUAL(100, c5);
    CHECK_EQUAL(50, c6);
    CHECK_EQUAL(0, a.count(-1));
    CHECK_EQUAL(0, a.count(4));

    // 4 bit width
    for (size_t i = 0; i < 100; ++i) {
        if (i % 2) a.set(i, 7);
    }
    const size_t c7 = a.count(0);
    const size_t c8 = a.count(7);
    CHECK_EQUAL(100, c7);
    CHECK_EQUAL(50, c8);
    CHECK_EQUAL(0, a.count(-1));
    CHECK_EQUAL(0, a.count(4));

    // 8 bit width
    for (size_t i = 0; i < 100; ++i) {
        if (i % 2) a.set(i, 100);
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
        if (i % 2) a.set(i, 500);
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
        if (i % 2) a.set(i, 0x1FFFF);
    }
    const size_t c13 = a.count(0);
    const size_t c14 = a.count(0x1FFFF);
    CHECK_EQUAL(100, c13);
    CHECK_EQUAL(50, c14);
    CHECK_EQUAL(0, a.count(-1));
    CHECK_EQUAL(0, a.count(0xFFFFFFFF));
    CHECK_EQUAL(0, a.count(-0xFFFFFFFFll));

    // Clean-up
    a.destroy();
}

#endif // TEST_ARRAY
