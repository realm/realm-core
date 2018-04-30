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
#ifdef TEST_COLUMN

#include <vector>
#include <algorithm>

#include <realm/column_integer.hpp>
#include <realm/bplustree.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::test_util;

using realm::util::unwrap;


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
    IntegerColumn c(Allocator::get_default());
    c.create();

    // TEST(Column_IsEmpty)

    CHECK_EQUAL(0U, c.size());
    CHECK(c.is_empty());


    // TEST(Column_Add0)

    c.add(0);
    CHECK_EQUAL(0LL, c.get(0));
    CHECK_EQUAL(1U, c.size());
    CHECK(!c.is_empty());


    // TEST(Column_Add1)

    c.add(1);
    CHECK_EQUAL(0LL, c.get(0));
    CHECK_EQUAL(1LL, c.get(1));
    CHECK_EQUAL(2U, c.size());


    // TEST(Column_Add2)

    c.add(2);
    CHECK_EQUAL(0LL, c.get(0));
    CHECK_EQUAL(1LL, c.get(1));
    CHECK_EQUAL(2LL, c.get(2));
    CHECK_EQUAL(3U, c.size());


    // TEST(Column_Add3)

    c.add(3);
    CHECK_EQUAL(0LL, c.get(0));
    CHECK_EQUAL(1LL, c.get(1));
    CHECK_EQUAL(2LL, c.get(2));
    CHECK_EQUAL(3LL, c.get(3));
    CHECK_EQUAL(4U, c.size());


    // TEST(Column_Add4)

    c.add(4);
    CHECK_EQUAL(0LL, c.get(0));
    CHECK_EQUAL(1LL, c.get(1));
    CHECK_EQUAL(2LL, c.get(2));
    CHECK_EQUAL(3LL, c.get(3));
    CHECK_EQUAL(4LL, c.get(4));
    CHECK_EQUAL(5U, c.size());


    // TEST(Column_Add5)

    c.add(16);
    CHECK_EQUAL(0LL, c.get(0));
    CHECK_EQUAL(1LL, c.get(1));
    CHECK_EQUAL(2LL, c.get(2));
    CHECK_EQUAL(3LL, c.get(3));
    CHECK_EQUAL(4LL, c.get(4));
    CHECK_EQUAL(16LL, c.get(5));
    CHECK_EQUAL(6U, c.size());


    // TEST(Column_Add6)

    c.add(256);
    CHECK_EQUAL(0LL, c.get(0));
    CHECK_EQUAL(1LL, c.get(1));
    CHECK_EQUAL(2LL, c.get(2));
    CHECK_EQUAL(3LL, c.get(3));
    CHECK_EQUAL(4LL, c.get(4));
    CHECK_EQUAL(16LL, c.get(5));
    CHECK_EQUAL(256LL, c.get(6));
    CHECK_EQUAL(7U, c.size());


    // TEST(Column_Add7)

    c.add(65536);
    CHECK_EQUAL(0LL, c.get(0));
    CHECK_EQUAL(1LL, c.get(1));
    CHECK_EQUAL(2LL, c.get(2));
    CHECK_EQUAL(3LL, c.get(3));
    CHECK_EQUAL(4LL, c.get(4));
    CHECK_EQUAL(16LL, c.get(5));
    CHECK_EQUAL(256LL, c.get(6));
    CHECK_EQUAL(65536LL, c.get(7));
    CHECK_EQUAL(8U, c.size());


    // TEST(Column_Add8)

    c.add(4294967296LL);
    CHECK_EQUAL(0LL, c.get(0));
    CHECK_EQUAL(1LL, c.get(1));
    CHECK_EQUAL(2LL, c.get(2));
    CHECK_EQUAL(3LL, c.get(3));
    CHECK_EQUAL(4LL, c.get(4));
    CHECK_EQUAL(16LL, c.get(5));
    CHECK_EQUAL(256LL, c.get(6));
    CHECK_EQUAL(65536LL, c.get(7));
    CHECK_EQUAL(4294967296LL, c.get(8));
    CHECK_EQUAL(9U, c.size());


    // TEST(Column_AddNeg1)

    c.clear();

    c.add(-1);

    CHECK_EQUAL(1U, c.size());
    CHECK_EQUAL(-1LL, c.get(0));


    // TEST(Column_AddNeg2)

    c.add(-256);

    CHECK_EQUAL(2U, c.size());
    CHECK_EQUAL(-1LL, c.get(0));
    CHECK_EQUAL(-256LL, c.get(1));


    // TEST(Column_AddNeg3)

    c.add(-65536);

    CHECK_EQUAL(3U, c.size());
    CHECK_EQUAL(-1LL, c.get(0));
    CHECK_EQUAL(-256LL, c.get(1));
    CHECK_EQUAL(-65536LL, c.get(2));


    // TEST(Column_AddNeg4)

    c.add(-4294967296LL);

    CHECK_EQUAL(4U, c.size());
    CHECK_EQUAL(-1LL, c.get(0));
    CHECK_EQUAL(-256LL, c.get(1));
    CHECK_EQUAL(-65536LL, c.get(2));
    CHECK_EQUAL(-4294967296LL, c.get(3));


    // TEST(Column_Set)

    c.set(0, 3);
    c.set(1, 2);
    c.set(2, 1);
    c.set(3, 0);

    CHECK_EQUAL(4U, c.size());
    CHECK_EQUAL(3LL, c.get(0));
    CHECK_EQUAL(2LL, c.get(1));
    CHECK_EQUAL(1LL, c.get(2));
    CHECK_EQUAL(0LL, c.get(3));


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
    CHECK_EQUAL(0LL, c.get(0));
    CHECK_EQUAL(1LL, c.get(1));
    CHECK_EQUAL(16LL, c.get(2));
    CHECK_EQUAL(2LL, c.get(3));
    CHECK_EQUAL(3LL, c.get(4));


    // TEST(Column_Insert2)

    // Insert at top
    c.insert(0, 256);

    CHECK_EQUAL(6U, c.size());
    CHECK_EQUAL(256LL, c.get(0));
    CHECK_EQUAL(0LL, c.get(1));
    CHECK_EQUAL(1LL, c.get(2));
    CHECK_EQUAL(16LL, c.get(3));
    CHECK_EQUAL(2LL, c.get(4));
    CHECK_EQUAL(3LL, c.get(5));


    // TEST(Column_Insert3)

    // Insert at bottom
    c.insert(6, 65536);

    CHECK_EQUAL(7U, c.size());
    CHECK_EQUAL(256LL, c.get(0));
    CHECK_EQUAL(0LL, c.get(1));
    CHECK_EQUAL(1LL, c.get(2));
    CHECK_EQUAL(16LL, c.get(3));
    CHECK_EQUAL(2LL, c.get(4));
    CHECK_EQUAL(3LL, c.get(5));
    CHECK_EQUAL(65536LL, c.get(6));


    // TEST(Column_Delete1)

    // Delete from middle
    c.erase(3);

    CHECK_EQUAL(6U, c.size());
    CHECK_EQUAL(256LL, c.get(0));
    CHECK_EQUAL(0LL, c.get(1));
    CHECK_EQUAL(1LL, c.get(2));
    CHECK_EQUAL(2LL, c.get(3));
    CHECK_EQUAL(3LL, c.get(4));
    CHECK_EQUAL(65536LL, c.get(5));


    // TEST(Column_Delete2)

    // Delete from top
    c.erase(0);

    CHECK_EQUAL(5U, c.size());
    CHECK_EQUAL(0LL, c.get(0));
    CHECK_EQUAL(1LL, c.get(1));
    CHECK_EQUAL(2LL, c.get(2));
    CHECK_EQUAL(3LL, c.get(3));
    CHECK_EQUAL(65536LL, c.get(4));


    // TEST(Column_Delete3)

    // Delete from bottom
    c.erase(4);

    CHECK_EQUAL(4U, c.size());
    CHECK_EQUAL(0LL, c.get(0));
    CHECK_EQUAL(1LL, c.get(1));
    CHECK_EQUAL(2LL, c.get(2));
    CHECK_EQUAL(3LL, c.get(3));


    // TEST(Column_DeleteAll)

    // Delete all items one at a time
    c.erase(0);
    c.erase(0);
    c.erase(0);
    c.erase(0);

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

    // TEST(Column_Destroy)

    c.destroy();
}

TEST(Column_FindLeafs)
{
    IntegerColumn a(Allocator::get_default());
    a.create();

    // Create values that span multible leaves
    // we use 5 to ensure that we get two levels
    // when testing with REALM_MAX_BPNODE_SIZE=4
    for (size_t i = 0; i < REALM_MAX_BPNODE_SIZE * 5; ++i)
        a.add(0);

    // Set sentinel values at before and after each break
    a.set(0, 1);
    a.set(REALM_MAX_BPNODE_SIZE - 1, 2);
    a.set(REALM_MAX_BPNODE_SIZE, 3);
    a.set(REALM_MAX_BPNODE_SIZE * 2 - 1, 4);
    a.set(REALM_MAX_BPNODE_SIZE * 2, 5);
    a.set(REALM_MAX_BPNODE_SIZE * 3 - 1, 6);
    a.set(REALM_MAX_BPNODE_SIZE * 3, 7);
    a.set(REALM_MAX_BPNODE_SIZE * 4 - 1, 8);
    a.set(REALM_MAX_BPNODE_SIZE * 4, 9);
    a.set(REALM_MAX_BPNODE_SIZE * 5 - 1, 10);

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
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE - 1, res2);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE, res3);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE * 2 - 1, res4);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE * 2, res5);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE * 3 - 1, res6);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE * 3, res7);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE * 4 - 1, res8);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE * 4, res9);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE * 5 - 1, res10);

    a.destroy();
}


TEST(Column_SwapRows)
{
    // Normal case
    {
        IntegerColumn c(Allocator::get_default());
        c.create();

        c.add(-21);
        c.add(30);
        c.add(10);
        c.add(5);

        CHECK_EQUAL(c.get(1), 30);
        CHECK_EQUAL(c.get(2), 10);
        CHECK_EQUAL(c.size(), 4); // size should not change

        c.swap(1, 2);

        CHECK_EQUAL(c.get(1), 10);
        CHECK_EQUAL(c.get(2), 30);
        CHECK_EQUAL(c.size(), 4);

        c.destroy();
    }

    // First two elements
    {
        IntegerColumn c(Allocator::get_default());
        c.create();

        c.add(30);
        c.add(10);
        c.add(5);

        c.swap(0, 1);

        CHECK_EQUAL(c.get(0), 10);
        CHECK_EQUAL(c.get(1), 30);
        CHECK_EQUAL(c.size(), 3); // size should not change

        c.destroy();
    }

    // Last two elements
    {
        IntegerColumn c(Allocator::get_default());
        c.create();

        c.add(5);
        c.add(30);
        c.add(10);

        c.swap(1, 2);

        CHECK_EQUAL(c.get(1), 10);
        CHECK_EQUAL(c.get(2), 30);
        CHECK_EQUAL(c.size(), 3); // size should not change

        c.destroy();
    }

    // Indices in wrong order
    {
        IntegerColumn c(Allocator::get_default());
        c.create();

        c.add(5);
        c.add(30);
        c.add(10);

        c.swap(2, 1);

        CHECK_EQUAL(c.get(1), 10);
        CHECK_EQUAL(c.get(2), 30);
        CHECK_EQUAL(c.size(), 3); // size should not change

        c.destroy();
    }
}

TEST_IF(Column_PrependMany, TEST_DURATION >= 1)
{
    // Test against a "Assertion failed: start < m_len, file src\Array.cpp, line 276" bug
    IntegerColumn a(Allocator::get_default());
    a.create();

    for (size_t items = 0; items < 3000; ++items) {
        a.clear();
        for (size_t j = 0; j < items + 1; ++j)
            a.insert(0, j);
        a.insert(items, 444);
    }
    a.destroy();
}


TEST(Column_Iterators)
{
    std::vector<int64_t> list;
    IntegerColumn c(Allocator::get_default());
    c.create();

    Random random(random_int<long>());
    const size_t num_elements = 1000;

    for (size_t i = 0; i < num_elements; i++) {
        long r = random.draw_int<long>();
        list.emplace_back(r);
        c.add(r);
    }

    auto std_begin = list.cbegin();
    auto std_it = list.cbegin();
    auto std_end = list.cend();
    auto realm_begin = c.cbegin();
    auto realm_it = c.cbegin();
    auto realm_end = c.cend();

    CHECK_EQUAL(std_end - std_it, realm_end - realm_it);

    while (std_it != std_end) {
        CHECK_EQUAL(*std_it, *realm_it);
        auto std_found = std::find(std_begin, std_end, *std_it);
        auto realm_found = std::find(realm_begin, realm_end, *realm_it);
        CHECK_EQUAL(std_end - std_found, realm_end - realm_found);
        CHECK_EQUAL(std_found - std_begin, realm_found - realm_begin);
        size_t ndx = c.find_first(*realm_it);
        if (ndx == npos) {
            CHECK_EQUAL(realm_found, realm_end);
        }
        else {
            CHECK_EQUAL(ndx, realm_found - realm_begin);
            CHECK_EQUAL(ndx, realm_found.get_position());
        }

        ++std_it;
        ++realm_it;
    }

    // operator +
    std_it = std_begin + 5;
    realm_it = realm_begin + 5;
    CHECK_EQUAL(std_end - std_it, realm_end - realm_it);
    CHECK_EQUAL(std_it - std_begin, realm_it - realm_begin);
    CHECK_EQUAL(realm_it.get_position(), 5);
    // operator +=
    std_it += 10;
    realm_it += 10;
    CHECK_EQUAL(std_end - std_it, realm_end - realm_it);
    CHECK_EQUAL(std_it - std_begin, realm_it - realm_begin);
    CHECK_EQUAL(realm_it.get_position(), 15);
    // operator -=
    std_it -= 10;
    realm_it -= 10;
    CHECK_EQUAL(std_end - std_it, realm_end - realm_it);
    CHECK_EQUAL(std_it - std_begin, realm_it - realm_begin);
    CHECK_EQUAL(realm_it.get_position(), 5);
    // operator -
    std_it = std_it - 5;
    realm_it = realm_it - 5;
    CHECK_EQUAL(std_end - std_it, realm_end - realm_it);
    CHECK_EQUAL(std_it - std_begin, realm_it - realm_begin);
    CHECK_EQUAL(realm_it.get_position(), 0);
    // operator ++
    std_it++;
    ++std_it;
    realm_it++;
    ++realm_it;
    CHECK_EQUAL(std_end - std_it, realm_end - realm_it);
    CHECK_EQUAL(std_it - std_begin, realm_it - realm_begin);
    CHECK_EQUAL(realm_it.get_position(), 2);
    // operator --
    std_it--;
    --std_it;
    realm_it--;
    --realm_it;
    CHECK_EQUAL(std_end - std_it, realm_end - realm_it);
    CHECK_EQUAL(std_it - std_begin, realm_it - realm_begin);
    CHECK_EQUAL(realm_it.get_position(), 0);
    // operator [] offset
    int64_t std_value = (std_it + 10)[5];
    int64_t realm_value = (realm_it + 10)[5];
    CHECK_EQUAL(std_value, realm_value);
    std_value = (std_it + 10)[-5];
    realm_value = (realm_it + 10)[-5];
    CHECK_EQUAL(std_value, realm_value);
    // operator equality
    auto realm_next = realm_it + 1;
    CHECK(realm_next == realm_next);
    CHECK(realm_it == realm_it);
    CHECK(realm_it != realm_next);
    CHECK(realm_next != realm_it);
    CHECK_EQUAL(realm_next, realm_it + 1);
    // operator <
    CHECK(realm_it < realm_next);
    CHECK(!(realm_it < realm_it));
    CHECK(!(realm_next < realm_it));
    CHECK(realm_it <= realm_next);
    CHECK(realm_it <= realm_it);
    CHECK(!(realm_next <= realm_it));
    // operator >
    CHECK(realm_next > realm_it);
    CHECK(!(realm_next > realm_next));
    CHECK(!(realm_it > realm_next));
    CHECK(realm_next >= realm_it);
    CHECK(realm_next >= realm_next);
    CHECK(!(realm_it >= realm_next));

    c.destroy();
}

#endif // TEST_COLUMN
