/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include "testsettings.hpp"
#ifdef TEST_ARRAY

#include <cstdlib>
#include <algorithm>
#include <string>
#include <vector>
#include <map>

#include <realm/array_with_find.hpp>
#include <realm/array_unsigned.hpp>
#include <realm/column_integer.hpp>
#include <realm/query_conditions.hpp>
#include <realm/array_direct.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::test_util;
using unit_test::TestContext;


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

void has_zero_byte(TestContext& test_context, int64_t value, size_t reps)
{
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);
    IntegerColumn r(Allocator::get_default());
    r.create();

    for (size_t i = 0; i < reps - 1; ++i)
        a.add(value);

    a.add(0);

    size_t t = a.find_first(0);
    CHECK_EQUAL(a.size() - 1, t);

    r.clear();
    ArrayWithFind(a).find_all(&r, 0);
    CHECK_EQUAL(int64_t(a.size() - 1), r.get(0));

    // Cleanup
    a.destroy();
    r.destroy();
}

} // anonymous namespace

TEST(Array_Bits)
{
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(0), 0);
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(1), 1);
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(2), 2);
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(3), 2);
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(4), 3);
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(5), 3);
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(7), 3);
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(8), 4);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(0), 1);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(1), 2);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(-1), 1);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(-2), 2);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(-3), 3);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(-4), 3);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(3), 3);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(4), 4);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(7), 4);
}

TEST(Array_General)
{
    Array c(Allocator::get_default());
    c.create(Array::type_Normal);

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


TEST(Array_Unsigned)
{
    ArrayUnsigned c(Allocator::get_default());
    c.create(0, 0);

    // TEST(Array_Add0)

    c.add(0);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.size(), 1);
    CHECK_EQUAL(8, c.get_width());

    // TEST(Array_Add1)

    c.add(1);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.size(), 2);
    CHECK_EQUAL(8, c.get_width());

    // TEST(Array_Add2)

    c.add(0xff);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 0xff);
    CHECK_EQUAL(c.size(), 3);
    CHECK_EQUAL(8, c.get_width());

    // TEST(Array_Add3)

    c.add(0x100);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 0xff);
    CHECK_EQUAL(c.get(3), 0x100);
    CHECK_EQUAL(c.size(), 4);
    CHECK_EQUAL(16, c.get_width());

    // TEST(Array_Add4)

    c.add(0x10000);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 0xff);
    CHECK_EQUAL(c.get(3), 0x100);
    CHECK_EQUAL(c.get(4), 0x10000);
    CHECK_EQUAL(c.size(), 5);
    CHECK_EQUAL(32, c.get_width());

    // TEST(Array_Insert3)

    c.insert(3, 0x100000000);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 0xff);
    CHECK_EQUAL(c.get(3), 0x100000000);
    CHECK_EQUAL(c.get(4), 0x100);
    CHECK_EQUAL(c.get(5), 0x10000);
    CHECK_EQUAL(c.size(), 6);
    CHECK_EQUAL(64, c.get_width());

    // TEST(Array_Insert3)

    c.insert(5, 7);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 0xff);
    CHECK_EQUAL(c.get(3), 0x100000000);
    CHECK_EQUAL(c.get(4), 0x100);
    CHECK_EQUAL(c.get(5), 7);
    CHECK_EQUAL(c.get(6), 0x10000);
    CHECK_EQUAL(c.size(), 7);

    c.erase(3);
    CHECK_EQUAL(c.get(0), 0);
    CHECK_EQUAL(c.get(1), 1);
    CHECK_EQUAL(c.get(2), 0xff);
    CHECK_EQUAL(c.get(3), 0x100);
    CHECK_EQUAL(c.get(4), 7);
    CHECK_EQUAL(c.get(5), 0x10000);
    CHECK_EQUAL(c.size(), 6);

    c.truncate(0);
    CHECK_EQUAL(c.size(), 0);
    CHECK_EQUAL(8, c.get_width());
    c.add(1);
    c.add(2);
    c.add(2);
    c.add(3);

    CHECK_EQUAL(c.lower_bound(1), 0);
    CHECK_EQUAL(c.lower_bound(2), 1);
    CHECK_EQUAL(c.lower_bound(3), 3);

    CHECK_EQUAL(c.upper_bound(1), 1);
    CHECK_EQUAL(c.upper_bound(2), 3);
    CHECK_EQUAL(c.upper_bound(3), 4);

    c.destroy();
}


TEST(Array_AddNeg1_1)
{
    Array c(Allocator::get_default());
    c.create(Array::type_Normal);

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
    // This test is independent of REALM_MAX_BPNODE_SIZE
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);
    std::vector<int> v;
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    // we use 4 as constant in order to make border case sequences of
    // v, v, v and v, v+1, v+2, etc, probable
    for (int i = 0; i < (1000 * (1 + TEST_DURATION * TEST_DURATION * TEST_DURATION * TEST_DURATION * TEST_DURATION));
         i++) {
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
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);

    a.add(10);
    a.add(20);
    a.add(30);
    a.add(40);
    a.add(50);
    a.add(60);
    a.add(70);
    a.add(80);

    // clang-format off
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
    // clang-format on

    a.destroy();
}


/** find_all() int tests spread out over bitwidth
 *
 */


TEST(Array_FindAllInt0)
{
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);
    ArrayWithFind f(a);

    IntegerColumn r(Allocator::get_default());
    r.create();

    const int value = 0;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++) {
        a.add(0);
    }

    r.clear();
    f.find_all(&r, 1, 0, 0, 0);
    CHECK_EQUAL(0, r.size());

    r.clear();
    f.find_all(&r, 1, 0, vReps - 1, vReps - 1);
    CHECK_EQUAL(0, r.size());

    r.clear();
    f.find_all(&r, value);
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

TEST(Array_FindAllInt1)
{
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);
    ArrayWithFind f(a);

    IntegerColumn r(Allocator::get_default());
    r.create();

    const int value = 1;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++) {
        a.add(0);
        a.add(0);
        a.add(1);
        a.add(0);
    }

    f.find_all(&r, value);
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

TEST(Array_FindAllInt2)
{
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);
    ArrayWithFind f(a);

    IntegerColumn r(Allocator::get_default());
    r.create();

    const int value = 3;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++) {
        a.add(0);
        a.add(1);
        a.add(2);
        a.add(3);
    }

    f.find_all(&r, value);
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

TEST(Array_FindAllInt3)
{
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);
    ArrayWithFind f(a);

    IntegerColumn r(Allocator::get_default());
    r.create();

    const int value = 10;
    const int vReps = 5;
    // 0, 4, 8
    for (int i = 0; i < vReps; i++) {
        a.add(10);
        a.add(11);
        a.add(12);
        a.add(13);
    }

    f.find_all(&r, value);
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

TEST(Array_FindAllInt4)
{
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);
    ArrayWithFind f(a);

    IntegerColumn r(Allocator::get_default());
    r.create();

    const int value = 20;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++) {
        // 8 bitwidth
        a.add(20);
        a.add(21);
        a.add(22);
        a.add(23);
    }

    f.find_all(&r, value);
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

TEST(Array_FindAllInt5)
{
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);
    ArrayWithFind f(a);

    IntegerColumn r(Allocator::get_default());
    r.create();

    const int value = 303;
    const int vReps = 5;

    for (int i = 0; i < vReps; i++) {
        // 16 bitwidth
        a.add(300);
        a.add(301);
        a.add(302);
        a.add(303);
    }

    f.find_all(&r, value);
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

TEST(Array_FindAllInt6)
{
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);
    ArrayWithFind f(a);

    IntegerColumn r(Allocator::get_default());
    r.create();

    const int value = 70000;
    const int vReps = 5;

    for (int i = 0; i < vReps; ++i) {
        // 32 bitwidth
        a.add(70000);
        a.add(70001);
        a.add(70002);
        a.add(70003);
    }

    f.find_all(&r, value);
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
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);
    ArrayWithFind f(a);

    IntegerColumn r(Allocator::get_default());
    r.create();

    const int64_t value = 4300000003ULL;
    const int vReps = 5;

    for (int i = 0; i < vReps; ++i) {
        // 64 bitwidth
        a.add(4300000000ULL);
        a.add(4300000001ULL);
        a.add(4300000002ULL);
        a.add(4300000003ULL);
    }

    f.find_all(&r, value);
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

// Tests the case where a value does *not* exist in one entire 64-bit chunk (triggers the 'if (has_zero_byte())
// break;' condition)
TEST(Array_FindHasZeroByte)
{
    // we want at least 1 entire 64-bit chunk-test, and we also want a remainder-test, so we chose n to be a prime >
    // 64
    size_t n = 73;
    has_zero_byte(test_context, 1, n);            // width = 1
    has_zero_byte(test_context, 3, n);            // width = 2
    has_zero_byte(test_context, 13, n);           // width = 4
    has_zero_byte(test_context, 100, n);          // 8
    has_zero_byte(test_context, 10000, n);        // 16
    has_zero_byte(test_context, 100000, n);       // 32
    has_zero_byte(test_context, 8000000000LL, n); // 64
}

// New find test for SSE search, to trigger partial finds (see find_sse()) before and after the aligned data area
TEST(Array_find_sse)
{
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);

    for (uint64_t i = 0; i < 100; ++i) {
        a.add(10000);
    }

    for (size_t i = 0; i < 100; ++i) {
        a.set(i, 123);
        size_t t = a.find_first(123);
        REALM_ASSERT(t == i);
        a.set(i, 10000);
        static_cast<void>(t);
    }
    a.destroy();
}


TEST(Array_Greater)
{
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);

    size_t items = 400;

    for (items = 2; items < 200; items += 7) {

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(0);
        }

        {
            size_t t = a.find_first<Greater>(0, 0, size_t(-1));
            CHECK_EQUAL(size_t(-1), t);
        }

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(0);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 1);

            size_t t = a.find_first<Greater>(0, 0, size_t(-1));
            REALM_ASSERT(i == t);

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
            a.add(1000ULL * 1000ULL * 1000ULL * 1000ULL);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 1000ULL * 1000ULL * 1000ULL * 1000ULL + 1ULL);
            size_t t = a.find_first<Greater>(1000ULL * 1000ULL * 1000ULL * 1000ULL, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 1000ULL * 1000ULL * 1000ULL * 1000ULL);
        }
    }
    a.destroy();
}


TEST(Array_Less)
{
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);

    size_t items = 400;

    for (items = 2; items < 200; items += 7) {

        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(0);
        }

        {
            size_t t = a.find_first<Less>(0, 0, size_t(-1));
            CHECK_EQUAL(size_t(-1), t);
        }

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
            a.add(1000ULL * 1000ULL * 1000ULL * 1000ULL);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 1000ULL * 1000ULL * 1000ULL * 1000ULL - 1ULL);
            size_t t = a.find_first<Less>(1000ULL * 1000ULL * 1000ULL * 1000ULL, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 1000ULL * 1000ULL * 1000ULL * 1000ULL);
        }
    }
    a.destroy();
}


TEST(Array_NotEqual1)
{
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);

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
    Array a(Allocator::get_default());
    a.create(Array::type_Normal);

    size_t items = 400;

    for (items = 2; items < 200; items += 7) {
        a.clear();
        for (size_t i = 0; i < items; ++i) {
            a.add(0);
        }

        {
            size_t t = a.find_first<NotEqual>(0, 0, size_t(-1));
            CHECK_EQUAL(size_t(-1), t);
        }

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
            a.add(1000ULL * 1000ULL * 1000ULL * 1000ULL);
        }
        for (size_t i = 0; i < items; ++i) {
            a.set(i, 1000ULL * 1000ULL * 1000ULL * 1000ULL + 1ULL);
            size_t t = a.find_first<NotEqual>(1000ULL * 1000ULL * 1000ULL * 1000ULL, 0, size_t(-1));
            CHECK_EQUAL(i, t);
            a.set(i, 1000ULL * 1000ULL * 1000ULL * 1000ULL);
        }
    }
    a.destroy();
}


TEST(Array_Large)
{
    Array c(Allocator::get_default());
    c.create(Array::type_Normal);

    // TEST(Array_Add0)

    c.add(0x1234567890);
    for (int i = 0; i < 0x300000; i++) {
        c.add(i);
    }
    CHECK_EQUAL(c.size(), 0x300001);
    CHECK_EQUAL(c.get(0x300000), 0x300000 - 1);
    c.destroy();
}

TEST(Array_set_type)
{
    Array c(Allocator::get_default());
    c.create(Array::type_Normal);

    c.set_type(Array::type_Normal);
    CHECK_EQUAL(c.get_type(), Array::type_Normal);

    c.set_type(Array::type_InnerBptreeNode);
    CHECK_EQUAL(c.get_type(), Array::type_InnerBptreeNode);

    c.set_type(Array::type_HasRefs);
    CHECK_EQUAL(c.get_type(), Array::type_HasRefs);

    c.destroy();
}

TEST(Array_get_sum)
{
    Array c(Allocator::get_default());
    c.create(Array::type_Normal);

    // simple sum1
    for (int i = 0; i < 0x10; i++)
        c.add(i);
    CHECK_EQUAL(c.get_sum(), 120);
    c.clear();

    const auto size = realm::max_array_size / 4;

    // test multiple chunks w=1
    for (uint64_t i = 0; i < size; ++i)
        c.add(0x1);
    CHECK_EQUAL(c.get_sum(), size);

    // test multiple chunks w=2
    c.clear();
    for (uint64_t i = 0; i < size; ++i)
        c.add(0x3);
    CHECK_EQUAL(c.get_sum(), 0x3 * size);

    // test multiple chunks w=4
    c.clear();
    for (uint64_t i = 0; i < size; ++i)
        c.add(0x13);
    CHECK_EQUAL(c.get_sum(), 0x13 * size);

    // test multiple chunks w=8
    c.clear();
    for (uint64_t i = 0; i < size; ++i)
        c.add(0x100);
    CHECK_EQUAL(c.get_sum(), 0x100 * size);

    // test multiple chunks w=16
    c.clear();
    for (uint64_t i = 0; i < size; ++i) {
        c.add(0x10000);
    }
    CHECK_EQUAL(c.get_sum(), 0x10000LL * size);

    // test multiple chunks w=32
    c.clear();
    for (uint64_t i = 0; i < size; ++i)
        c.add(0x100000);
    CHECK_EQUAL(c.get_sum(), 0x100000LL * size);

    // test multiple chunks w=64
    c.clear();
    for (uint64_t i = 0; i < size; ++i)
        c.add(8000000000LL);
    CHECK_EQUAL(c.get_sum(), (size * 8000000000LL));

    // test generic case
    c.clear();
    uint64_t expected = 0;
    for (uint64_t i = 0; i < 0x30000; ++i) {
        c.add(i);
        expected += i;
    }
    CHECK_EQUAL(c.get_sum(), expected);

    c.destroy();
}

// NONCONCURRENT because if run in parallel with other tests which request large amounts of
// memory, there may be a std::bad_alloc on low memory machines
NONCONCURRENT_TEST(Array_count)
{
    struct TestArray : public Array {
        explicit TestArray(Allocator& allocator)
            : Array(allocator)
        {
        }
        size_t count(int64_t v)
        {
            return Array::count(v);
        }
    };

    TestArray c(Allocator::get_default());
    c.create(Array::type_Normal);
    const size_t size = realm::max_array_size;

    // test multiple chunks w=1
    c.clear();
    for (uint64_t i = 0; i < size; ++i)
        c.add(0x1);
    CHECK_EQUAL(c.count(0x1), size);

    // test multiple chunks w=2
    c.clear();
    for (uint64_t i = 0; i < size; ++i)
        c.add(0x3);
    CHECK_EQUAL(c.count(0x3), size);

    // test multiple chunks w=4
    c.clear();
    for (uint64_t i = 0; i < size; ++i)
        c.add(0x13);
    CHECK_EQUAL(c.count(0x13), size);

    // test multiple chunks w=8
    c.clear();
    for (uint64_t i = 0; i < size; ++i)
        c.add(0x100);
    CHECK_EQUAL(c.count(0x100), size);

    // test multiple chunks w=16
    c.clear();
    for (uint64_t i = 0; i < size; ++i)
        c.add(0x10000);
    CHECK_EQUAL(c.count(0x10000), size);

    // test w=32 (number of chunks does not matter)
    c.clear();
    const size_t size_32_64_bit = 10;
    for (uint64_t i = 0; i < size_32_64_bit; ++i)
        c.add(100000);
    CHECK_EQUAL(c.count(100000), size_32_64_bit);

    // test w=64 (number of chunks does not matter)
    c.clear();
    for (uint64_t i = 0; i < size_32_64_bit; ++i)
        c.add(8000000000LL);
    CHECK_EQUAL(c.count(8000000000LL), size_32_64_bit);

    c.destroy();
}

TEST(DirectBitFields)
{
    uint64_t a[2];
    a[0] = a[1] = 0;
    {
        bf_iterator it(a, 0, 7, 7, 8);
        REALM_ASSERT(*it == 0);
        auto it2(it);
        ++it2;
        it2.set_value(127 + 128);
        REALM_ASSERT(*it == 0);
        ++it;
        REALM_ASSERT(*it == 127);
        ++it;
        REALM_ASSERT(*it == 0);
    }
    // reverse polarity
    a[0] = a[1] = -1ULL;
    {
        bf_iterator it(a, 0, 7, 7, 8);
        REALM_ASSERT(*it == 127);
        auto it2(it);
        ++it2;
        it2.set_value(42 + 128);
        REALM_ASSERT(*it == 127);
        ++it;
        REALM_ASSERT(*it == 42);
        ++it;
        REALM_ASSERT(*it == 127);
    }
}

TEST(B_Array_creation)
{
    //    using Encoding = NodeHeader::Encoding;
    //    Array array(Allocator::get_default());
    //    auto& allocator = array.get_alloc();
    //    auto mem = allocator.alloc(10);
    //    init_header(mem.get_addr(), Encoding::Flex, 6, 1, 1, 1, 1);
    //    array.init_from_mem(mem);
    //    auto array_header = array.get_header();
    //    auto encoding = array.get_encoding(array_header);
    //    REALM_ASSERT(encoding == Encoding::Flex); // this is missing << operator in order to be printed in case of
    //    error CHECK_EQUAL(array.get_flags(array_header), 6); CHECK_EQUAL(array.get_elementA_size(array_header), 1);
    //    CHECK_EQUAL(array.get_elementB_size(array_header), 1);
    //    CHECK_EQUAL(array.get_arrayA_num_elements(array_header), 1);
    //    CHECK_EQUAL(array.get_arrayB_num_elements(array_header), 1);
    //    // set flags explicitely (this should not change kind and encoding)
    //    array.set_flags(array_header, 5);
    //    auto flags = array.get_flags(array_header);
    //    CHECK_EQUAL(flags, 5);
    //    REALM_ASSERT(array.get_encoding(array_header) == Encoding::Flex);
    //    allocator.free_(mem);
}

TEST(B_Array_encoding)
{
    using Encoding = NodeHeader::Encoding;
    Array array(Allocator::get_default());
    auto mem = array.get_alloc().alloc(10);
    init_header(mem.get_addr(), Encoding::Flex, 7, 1, 1, 1, 1);
    array.init_from_mem(mem);
    auto array_header = array.get_header();
    auto encoding = array.get_encoding(array_header);
    CHECK(encoding == Encoding::Flex);

    Array another_array(Allocator::get_default());
    another_array.init_from_ref(array.get_ref());
    auto another_header = another_array.get_header();
    auto another_encoding = another_array.get_encoding(another_header);
    CHECK(encoding == another_encoding);

    array.get_alloc().free_(mem);
}

TEST(Array_Bits)
{
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(0), 0);
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(1), 1);
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(2), 2);
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(3), 2);
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(4), 3);
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(5), 3);
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(7), 3);
    CHECK_EQUAL(NodeHeader::unsigned_to_num_bits(8), 4);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(0), 1);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(1), 2);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(-1), 1);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(-2), 2);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(-3), 3);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(-4), 3);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(3), 3);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(4), 4);
    CHECK_EQUAL(NodeHeader::signed_to_num_bits(7), 4);
}

TEST(Array_cares_about)
{
    std::vector<uint64_t> expected{
        18446744073709551615ULL, 18446744073709551615ULL, 18446744073709551615ULL, 9223372036854775807ULL,
        18446744073709551615U,   1152921504606846975ULL,  1152921504606846975ULL,  9223372036854775807ULL,
        18446744073709551615ULL, 9223372036854775807ULL,  1152921504606846975ULL,  36028797018963967ULL,
        1152921504606846975ULL,  4503599627370495ULL,     72057594037927935ULL,    1152921504606846975ULL,
        18446744073709551615ULL, 2251799813685247ULL,     18014398509481983ULL,    144115188075855871ULL,
        1152921504606846975ULL,  9223372036854775807ULL,  17592186044415ULL,       70368744177663ULL,
        281474976710655ULL,      1125899906842623ULL,     4503599627370495ULL,     18014398509481983ULL,
        72057594037927935ULL,    288230376151711743ULL,   1152921504606846975ULL,  18446744073709551615ULL,
        18446744073709551615ULL, 8589934591ULL,           17179869183ULL,          34359738367ULL,
        68719476735ULL,          137438953471ULL,         274877906943ULL,         549755813887ULL,
        1099511627775ULL,        2199023255551ULL,        4398046511103ULL,        8796093022207ULL,
        17592186044415ULL,       35184372088831ULL,       70368744177663ULL,       140737488355327ULL,
        281474976710655ULL,      562949953421311ULL,      1125899906842623ULL,     2251799813685247ULL,
        4503599627370495ULL,     9007199254740991ULL,     18014398509481983ULL,    36028797018963967ULL,
        72057594037927935ULL,    144115188075855871ULL,   288230376151711743ULL,   576460752303423487ULL,
        1152921504606846975ULL,  2305843009213693951ULL,  4611686018427387903ULL,  9223372036854775807ULL,
        18446744073709551615ULL};
    std::vector<uint64_t> res;
    for (size_t i = 0; i <= 64; i++) {
        res.push_back(cares_about(i));
    }
    CHECK_EQUAL(res, expected);
}

TEST(AlignDirectBitFields)
{
    uint64_t a[2];
    a[0] = a[1] = 0;
    {
        bf_iterator it(a, 0, 7, 7, 8);
        REALM_ASSERT(*it == 0);
        auto it2(it);
        ++it2;
        it2.set_value(127 + 128);
        REALM_ASSERT(*it == 0);
        ++it;
        REALM_ASSERT(*it == 127);
        ++it;
        REALM_ASSERT(*it == 0);
    }
    // reverse polarity
    a[0] = a[1] = -1ULL;
    {
        bf_iterator it(a, 0, 7, 7, 8);
        REALM_ASSERT(*it == 127);
        auto it2(it);
        ++it2;
        it2.set_value(42 + 128);
        REALM_ASSERT(*it == 127);
        ++it;
        REALM_ASSERT(*it == 42);
        ++it;
        REALM_ASSERT(*it == 127);
    }
}

TEST(UnalignBitFields)
{
    uint8_t a[16];
    for (size_t i = 0; i < 16; ++i)
        a[i] = i + 1;
    {
        size_t cnt = 1;
        unaligned_word_iter it((uint64_t*)&a, 0);
        for (size_t i = 1; i <= 2; ++i) {
            auto v = it.get(64); // get the first word which contains 64 bits (asking for less bits does not matter)
            // now extract all the values
            for (size_t j = 0; j < 8; ++j) {
                const auto single_v = v & 0xFF;
                CHECK_EQUAL(single_v, cnt);
                cnt += 1;
                v >>= 8;
            }
            it.bump(64); // go to the second word
        }
    }
    {
        // reverse polarity
        auto cnt = 0;
        for (size_t i = 0; i < 16; ++i)
            a[i] = uint8_t(-1) - i;
        unaligned_word_iter it((uint64_t*)&a, 0);
        for (size_t i = 1; i <= 2; ++i) {
            // get the first word which contains 64 bits.
            //(asking for 8 bits and bump by 8 bits is another viable way of approaching this)
            auto v = it.get(64);
            // now extract all the values
            for (size_t j = 0; j < 8; ++j) {
                const auto single_v = v & 0xFF;
                CHECK_EQUAL(single_v, (uint8_t)(-1) - cnt);
                cnt += 1;
                v >>= 8;
            }
            it.bump(64); // go to the second word
        }
    }
}

TEST(TestSignValuesStoredIterator)
{
    {
        // positive values are easy.
        uint64_t a[2];
        bf_iterator it(a, 0, 8, 8, 0);
        for (size_t i = 0; i < 16; ++i) {
            it.set_value(i);
            ++it;
        }
        it.move(0);
        for (size_t i = 0; i < 16; ++i) {
            auto v = *it;
            CHECK_EQUAL(v, i);
            ++it;
        }
    }
    {
        // negative values require a bit more work
        uint64_t a[2];
        bf_iterator it(a, 0, 8, 8, 0);
        for (size_t i = 0; i < 16; ++i) {
            it.set_value(-i);
            ++it;
        }
        it.move(0);
        for (int64_t i = 0; i < 16; ++i) {
            const auto sv = sign_extend_value(8, *it);
            CHECK_EQUAL(sv, -i);
            ++it;
        }
        it.move(0);
        const auto sign_mask = 1ULL << (7);
        for (int64_t i = 0; i < 16; ++i) {
            const auto sv = sign_extend_field_by_mask(sign_mask, *it);
            CHECK_EQUAL(sv, -i);
            ++it;
        }
    }
}

TEST(TestSignValuesStoredUnalignIterator)
{
    // we don't use the unaligned iterator for writing, so there is no API for it.
    // Maybe it is something we could evaluate, if it makes faster to rewrite the array
    // when we are compressing via an unaligned iterator.
    // But in theory it should be the same.
    {
        // positive values are easy.
        uint8_t a_positive[16]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        unaligned_word_iter it((uint64_t*)&a_positive, 0);
        for (size_t i = 0; i < 16; ++i) {
            const auto v = it.get(8) & 0xFF; //
            CHECK_EQUAL(v, i);
            it.bump(8);
        }
    }
    {
        int8_t a_negative[16]{0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12, -13, -14, -15};
        unaligned_word_iter it((uint64_t*)&a_negative, 0);
        for (int8_t i = 0; i < 16; ++i) {
            const auto v = it.get(8) & 0xFF;
            const auto sv = sign_extend_value(8, v);
            CHECK_EQUAL(sv, -i);
            it.bump(8);
        }
    }
}

TEST(VerifyIterationAcrossWords)
{
    uint64_t a[4]{0, 0, 0, 0}; // 4 64 bit words, let's store N elements of 5bits each
    bf_iterator it(a, 0, 5, 5, 0);
    // 51 is the max amount of values we can fit in 4 words. Writting beyond this point is likely going
    // to crash. Writing beyond the 4 words is not possible in practice because the Array has boundery checks
    // and enough memory is reserved during compression.
    srand((unsigned)time(0)); // no need to use complex stuff
    std::vector<int64_t> values;
    for (size_t i = 0; i < 51; i++) {
        int64_t randomNumber = rand() % 16; // max value that can fit in 5 bits (4 bit for the value + 1 sign)
        values.push_back(randomNumber);
        it.set_value(randomNumber);
        ++it;
    }

    {
        // normal bf iterator
        it.move(0); // reset to first value.
        // go through the memory, 5 bits by 5 bits.
        // every 12 values, we will read the value across
        // 2 words.
        for (size_t i = 0; i < 51; ++i) {
            const auto v = sign_extend_value(5, *it);
            CHECK_EQUAL(v, values[i]);
            ++it;
        }
    }

    {
        // unaligned iterator
        unaligned_word_iter u_it(a, 0);
        for (size_t i = 0; i < 51; ++i) {
            const auto v = sign_extend_value(5, u_it.get(5) & 0x1F);
            CHECK_EQUAL(v, values[i]);
            u_it.bump(5);
        }
    }
}

TEST(LowerBoundCorrectness)
{
    constexpr auto size = 16;
    int64_t a[size]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

    std::vector<size_t> expected_default_lb;
    for (const auto v : a) {
        const auto pos = lower_bound<64>((const char*)(&a), size, v);
        expected_default_lb.push_back(pos);
    }

    // now simulate the compression of a in less bits
    uint64_t buff[2] = {0, 0};
    bf_iterator it(buff, 0, 5, 5, 0); // 5 bits because 4 bits for the values + 1 bit for the sign
    for (size_t i = 0; i < 16; ++i) {
        it.set_value(i);
        CHECK_EQUAL(*it, a[i]);
        ++it;
    }
    struct MyClass {
        uint64_t* _data;
        int64_t get(size_t ndx) const
        {
            bf_iterator it(_data, 0, 5, 5, ndx);
            return sign_extend_value(5, *it);
        }
    };
    // a bit of set up here.
    MyClass my_class;
    my_class._data = buff;
    using Fetcher = impl::CompressedDataFetcher<MyClass>;
    Fetcher my_fetcher;
    my_fetcher.ptr = &my_class;

    // verify that the fetcher returns the same values
    for (size_t i = 0; i < size; ++i) {
        CHECK_EQUAL(my_fetcher.ptr->get(i), a[i]);
    }

    std::vector<size_t> diffent_width_lb;
    for (const auto v : a) {
        const auto pos = lower_bound((const char*)buff, size, v, my_fetcher);
        diffent_width_lb.push_back(pos);
    }

    CHECK_EQUAL(expected_default_lb, diffent_width_lb);
}

TEST(UpperBoundCorrectness)
{
    constexpr auto size = 16;
    int64_t a[size]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    std::vector<size_t> expected_default_ub;
    for (const auto v : a) {
        const auto pos = upper_bound<64>((const char*)(&a), size, v);
        expected_default_ub.push_back(pos);
    }

    // now simulate the compression of a in less bits
    uint64_t buff[2] = {0, 0};
    bf_iterator it(buff, 0, 5, 5, 0); // 5 bits because 4 bits for the values + 1 bit for the sign
    for (size_t i = 0; i < size; ++i) {
        it.set_value(i);
        CHECK_EQUAL(*it, a[i]);
        ++it;
    }
    struct MyClass {
        uint64_t* _data;
        int64_t get(size_t ndx) const
        {
            bf_iterator it(_data, 0, 5, 5, ndx);
            return sign_extend_value(5, *it);
        }
    };
    // a bit of set up here.
    MyClass my_class;
    my_class._data = buff;
    using Fetcher = impl::CompressedDataFetcher<MyClass>;
    Fetcher my_fetcher;
    my_fetcher.ptr = &my_class;

    // verify that the fetcher returns the same values
    for (size_t i = 0; i < size; ++i) {
        CHECK_EQUAL(my_fetcher.ptr->get(i), a[i]);
    }

    std::vector<size_t> diffent_width_ub;
    for (const auto v : a) {
        const auto pos = upper_bound((const char*)buff, size, v, my_fetcher);
        diffent_width_ub.push_back(pos);
    }

    CHECK_EQUAL(expected_default_ub, diffent_width_ub);
}

TEST(ParallelSearchEqualMatch)
{
    uint64_t buff[2] = {0, 0};
    constexpr size_t width = 1;
    constexpr size_t size = 128;
    constexpr int64_t key = -1;
    bf_iterator it(buff, 0, width, width, 0);
    for (size_t i = 0; i < size; ++i) {
        it.set_value(1); // this is equivalent to set it to -1
        ++it;
    }
    it.move(0);
    for (size_t i = 0; i < size; ++i) {
        auto v = sign_extend_value(width, *it);
        CHECK_EQUAL(v, -1);
        ++it;
    }
    const auto mask = 1ULL << (width - 1);
    const auto msb = populate(width, mask);
    const auto search_vector = populate(width, key);

    static auto vector_compare_eq = [](auto msb, auto a, auto b) {
        return find_all_fields_EQ(msb, a, b);
    };

    size_t start = 0;
    const auto end = size;
    std::vector<size_t> parallel_result;
    while (start < end) {
        start = parallel_subword_find(vector_compare_eq, buff, size_t{0}, width, msb, search_vector, start, end);
        if (start != end)
            parallel_result.push_back(start);
        start += 1;
    }

    // perform the same check but with a normal iteration
    start = 0;
    std::vector<size_t> linear_scan_result;
    while (start < end) {
        it.move(start);
        const auto sv = sign_extend_value(width, *it);
        if (sv == key)
            linear_scan_result.push_back(start);
        ++start;
    }

    CHECK(!parallel_result.empty());
    CHECK(!linear_scan_result.empty());
    CHECK_EQUAL(linear_scan_result, parallel_result);
}

TEST(ParallelSearchEqualNoMatch)
{
    uint64_t buff[2] = {0, 0};
    constexpr size_t width = 2;
    constexpr size_t size = 64;
    constexpr int64_t key = 2;
    bf_iterator it(buff, 0, width, width, 0);
    for (size_t i = 0; i < size; ++i) {
        it.set_value(1);
        ++it;
    }
    it.move(0);
    for (size_t i = 0; i < size; ++i) {
        auto v = sign_extend_value(width, *it);
        CHECK_EQUAL(v, 1);
        ++it;
    }
    const auto mask = 1ULL << (width - 1);
    const auto msb = populate(width, mask);
    const auto search_vector = populate(width, key);

    static auto vector_compare_eq = [](auto msb, auto a, auto b) {
        return find_all_fields_EQ(msb, a, b);
    };

    size_t start = 0;
    const auto end = size;
    std::vector<size_t> parallel_result;
    while (start < end) {
        start = parallel_subword_find(vector_compare_eq, buff, size_t{0}, width, msb, search_vector, start, end);
        if (start != end)
            parallel_result.push_back(start);
        start += 1;
    }

    // perform the same check but with a normal iteration
    start = 0;
    std::vector<size_t> linear_scan_result;
    while (start < end) {
        it.move(start);
        const auto sv = sign_extend_value(width, *it);
        if (sv == key)
            linear_scan_result.push_back(start);
        ++start;
    }

    CHECK(parallel_result.empty());
    CHECK(linear_scan_result.empty());
}

TEST(ParallelSearchNotEqual)
{
    uint64_t buff[2] = {0, 0};
    constexpr size_t width = 2;
    constexpr size_t size = 64;
    constexpr int64_t key = 2;
    bf_iterator it(buff, 0, width, width, 0);
    for (size_t i = 0; i < size; ++i) {
        it.set_value(1);
        ++it;
    }
    it.move(0);
    for (size_t i = 0; i < size; ++i) {
        auto v = sign_extend_value(width, *it);
        CHECK_EQUAL(v, 1);
        ++it;
    }
    const auto mask = 1ULL << (width - 1);
    const auto msb = populate(width, mask);
    const auto search_vector = populate(width, key);

    static auto vector_compare_neq = [](auto msb, auto a, auto b) {
        return find_all_fields_NE(msb, a, b);
    };

    size_t start = 0;
    const auto end = size;
    std::vector<size_t> parallel_result;
    while (start < end) {
        start = parallel_subword_find(vector_compare_neq, buff, size_t{0}, width, msb, search_vector, start, end);
        if (start != end)
            parallel_result.push_back(start);
        start += 1;
    }

    // perform the same check but with a normal iteration
    start = 0;
    std::vector<size_t> linear_scan_result;
    while (start < end) {
        it.move(start);
        const auto sv = sign_extend_value(width, *it);
        if (sv != key)
            linear_scan_result.push_back(start);
        ++start;
    }

    CHECK(!parallel_result.empty());
    CHECK(!linear_scan_result.empty());
    CHECK_EQUAL(parallel_result, linear_scan_result);
}

TEST(ParallelSearchLessThan)
{
    uint64_t buff[2] = {0, 0};
    constexpr size_t width = 4;
    constexpr size_t size = 32;
    constexpr int64_t key = 3;
    bf_iterator it(buff, 0, width, width, 0);
    for (size_t i = 0; i < size; ++i) {
        it.set_value(2);
        ++it;
    }
    it.move(0);
    for (size_t i = 0; i < size; ++i) {
        auto v = sign_extend_value(width, *it);
        CHECK_EQUAL(v, 2);
        ++it;
    }
    const auto mask = 1ULL << (width - 1);
    const auto msb = populate(width, mask);
    const auto search_vector = populate(width, key);

    static auto vector_compare_neq = [](auto msb, auto a, auto b) {
        return find_all_fields_signed_LT(msb, a, b);
    };

    size_t start = 0;
    const auto end = size;
    std::vector<size_t> parallel_result;
    while (start < end) {
        start = parallel_subword_find(vector_compare_neq, buff, size_t{0}, width, msb, search_vector, start, end);
        if (start != end)
            parallel_result.push_back(start);
        start += 1;
    }

    // perform the same check but with a normal iteration
    start = 0;
    std::vector<size_t> linear_scan_result;
    while (start < end) {
        it.move(start);
        const auto sv = sign_extend_value(width, *it);
        if (sv < key)
            linear_scan_result.push_back(start);
        ++start;
    }
    CHECK(!parallel_result.empty());
    CHECK(!linear_scan_result.empty());
    CHECK_EQUAL(parallel_result, linear_scan_result);
}

TEST(ParallelSearchGreaterThan)
{
    uint64_t buff[2] = {0, 0};
    constexpr size_t width = 4;
    constexpr size_t size = 32;
    constexpr int64_t key = 2;
    bf_iterator it(buff, 0, width, width, 0);
    for (size_t i = 0; i < size; ++i) {
        it.set_value(3);
        ++it;
    }
    it.move(0);
    for (size_t i = 0; i < size; ++i) {
        auto v = sign_extend_value(width, *it);
        CHECK_EQUAL(v, 3);
        ++it;
    }
    const auto mask = 1ULL << (width - 1);
    const auto msb = populate(width, mask);
    const auto search_vector = populate(width, key);

    static auto vector_compare_neq = [](auto msb, auto a, auto b) {
        return find_all_fields_signed_GT(msb, a, b);
    };

    size_t start = 0;
    const auto end = size;
    std::vector<size_t> parallel_result;
    while (start < end) {
        start = parallel_subword_find(vector_compare_neq, buff, size_t{0}, width, msb, search_vector, start, end);
        if (start != end)
            parallel_result.push_back(start);
        start += 1;
    }

    // perform the same check but with a normal iteration
    start = 0;
    std::vector<size_t> linear_scan_result;
    while (start < end) {
        it.move(start);
        const auto sv = sign_extend_value(width, *it);
        if (sv > key)
            linear_scan_result.push_back(start);
        ++start;
    }
    CHECK(!parallel_result.empty());
    CHECK(!linear_scan_result.empty());
    CHECK_EQUAL(parallel_result, linear_scan_result);
}


#endif // TEST_ARRAY
