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
#ifdef TEST_COLUMN_STRING

#include <vector>
#include <realm/index_string.hpp>
#include <realm/bplustree.hpp>
#include <realm/array_string.hpp>
#include <realm/array_key.hpp>

#include "test.hpp"
#include "test_string_types.hpp"

using namespace realm;
using namespace realm::test_util;

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


TEST(ColumnString_Basic)
{
    string_column test_resources;
    auto& c = test_resources.get_column();

    // TEST(ColumnString_MultiEmpty)

    c.add("");
    c.add("");
    c.add("");
    c.add("");
    c.add("");
    c.add("");
    CHECK_EQUAL(6, c.size());

    CHECK_EQUAL("", c.get(0));
    CHECK_EQUAL("", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("", c.get(4));
    CHECK_EQUAL("", c.get(5));


    // TEST(ColumnString_SetExpand4)

    c.set(0, "hey");

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL("hey", c.get(0));
    CHECK_EQUAL("", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("", c.get(4));
    CHECK_EQUAL("", c.get(5));


    // TEST(ColumnString_SetExpand8)

    c.set(1, "test");

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL("hey", c.get(0));
    CHECK_EQUAL("test", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("", c.get(4));
    CHECK_EQUAL("", c.get(5));


    // TEST(ColumnString_Add0)

    c.clear();
    c.add(StringData());

    StringData default_string_value = StringData();

    CHECK_EQUAL(default_string_value, c.get(0));
    CHECK_EQUAL(1, c.size());


    // TEST(ColumnString_Add1)

    c.add("a");
    CHECK_EQUAL(default_string_value, c.get(0));
    CHECK_EQUAL("a", c.get(1));
    CHECK_EQUAL(2, c.size());


    // TEST(ColumnString_Add2)

    c.add("bb");
    CHECK_EQUAL(default_string_value, c.get(0));
    CHECK_EQUAL("a", c.get(1));
    CHECK_EQUAL("bb", c.get(2));
    CHECK_EQUAL(3, c.size());


    // TEST(ColumnString_Add3)

    c.add("ccc");
    CHECK_EQUAL(default_string_value, c.get(0));
    CHECK_EQUAL("a", c.get(1));
    CHECK_EQUAL("bb", c.get(2));
    CHECK_EQUAL("ccc", c.get(3));
    CHECK_EQUAL(4, c.size());


    // TEST(ColumnString_Add4)

    c.add("dddd");
    CHECK_EQUAL(default_string_value, c.get(0));
    CHECK_EQUAL("a", c.get(1));
    CHECK_EQUAL("bb", c.get(2));
    CHECK_EQUAL("ccc", c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL(5, c.size());


    // TEST(ColumnString_Add8)

    c.add("eeeeeeee");
    CHECK_EQUAL(default_string_value, c.get(0));
    CHECK_EQUAL("a", c.get(1));
    CHECK_EQUAL("bb", c.get(2));
    CHECK_EQUAL("ccc", c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL("eeeeeeee", c.get(5));
    CHECK_EQUAL(6, c.size());


    // TEST(ColumnString_Add16)

    c.add("ffffffffffffffff");
    CHECK_EQUAL(default_string_value, c.get(0));
    CHECK_EQUAL("a", c.get(1));
    CHECK_EQUAL("bb", c.get(2));
    CHECK_EQUAL("ccc", c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL("eeeeeeee", c.get(5));
    CHECK_EQUAL("ffffffffffffffff", c.get(6));
    CHECK_EQUAL(7, c.size());


    // TEST(ColumnString_Add32)

    c.add("gggggggggggggggggggggggggggggggg");

    CHECK_EQUAL(default_string_value, c.get(0));
    CHECK_EQUAL("a", c.get(1));
    CHECK_EQUAL("bb", c.get(2));
    CHECK_EQUAL("ccc", c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL("eeeeeeee", c.get(5));
    CHECK_EQUAL("ffffffffffffffff", c.get(6));
    CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.get(7));
    CHECK_EQUAL(8, c.size());


    // TEST(ColumnString_Add64)

    // Add a string longer than 64 bytes to trigger long strings
    c.add("xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx");

    CHECK_EQUAL(default_string_value, c.get(0));
    CHECK_EQUAL("a", c.get(1));
    CHECK_EQUAL("bb", c.get(2));
    CHECK_EQUAL("ccc", c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL("eeeeeeee", c.get(5));
    CHECK_EQUAL("ffffffffffffffff", c.get(6));
    CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.get(7));
    CHECK_EQUAL("xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx", c.get(8));
    CHECK_EQUAL(9, c.size());


    // TEST(ColumnString_Set1)

    c.set(0, "ccc");
    c.set(1, "bb");
    c.set(2, "a");
    c.set(3, "");

    CHECK_EQUAL(9, c.size());

    CHECK_EQUAL("ccc", c.get(0));
    CHECK_EQUAL("bb", c.get(1));
    CHECK_EQUAL("a", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL("eeeeeeee", c.get(5));
    CHECK_EQUAL("ffffffffffffffff", c.get(6));
    CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.get(7));
    CHECK_EQUAL("xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx", c.get(8));


    // TEST(ColumnString_Insert1)

    // Insert in middle
    c.insert(4, "xx");

    CHECK_EQUAL(10, c.size());

    CHECK_EQUAL("ccc", c.get(0));
    CHECK_EQUAL("bb", c.get(1));
    CHECK_EQUAL("a", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("xx", c.get(4));
    CHECK_EQUAL("dddd", c.get(5));
    CHECK_EQUAL("eeeeeeee", c.get(6));
    CHECK_EQUAL("ffffffffffffffff", c.get(7));
    CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.get(8));
    CHECK_EQUAL("xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx", c.get(9));


    // TEST(ColumnString_Delete1)

    // Delete from end
    c.erase(9);

    CHECK_EQUAL(9, c.size());

    CHECK_EQUAL("ccc", c.get(0));
    CHECK_EQUAL("bb", c.get(1));
    CHECK_EQUAL("a", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("xx", c.get(4));
    CHECK_EQUAL("dddd", c.get(5));
    CHECK_EQUAL("eeeeeeee", c.get(6));
    CHECK_EQUAL("ffffffffffffffff", c.get(7));
    CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.get(8));


    // TEST(ColumnString_Delete2)

    // Delete from top
    c.erase(0);

    CHECK_EQUAL(8, c.size());

    CHECK_EQUAL("bb", c.get(0));
    CHECK_EQUAL("a", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("xx", c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL("eeeeeeee", c.get(5));
    CHECK_EQUAL("ffffffffffffffff", c.get(6));
    CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.get(7));


    // TEST(ColumnString_Delete3)

    // Delete from middle
    c.erase(3);

    CHECK_EQUAL(7, c.size());

    CHECK_EQUAL("bb", c.get(0));
    CHECK_EQUAL("a", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("dddd", c.get(3));
    CHECK_EQUAL("eeeeeeee", c.get(4));
    CHECK_EQUAL("ffffffffffffffff", c.get(5));
    CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.get(6));


    // TEST(ColumnString_DeleteAll)

    // Delete all items one at a time
    c.erase(0);
    CHECK_EQUAL(6, c.size());
    c.erase(0);
    CHECK_EQUAL(5, c.size());
    c.erase(0);
    CHECK_EQUAL(4, c.size());
    c.erase(0);
    CHECK_EQUAL(3, c.size());
    c.erase(0);
    CHECK_EQUAL(2, c.size());
    c.erase(0);
    CHECK_EQUAL(1, c.size());
    c.erase(0);
    CHECK_EQUAL(0, c.size());

    CHECK(c.is_empty());


    // TEST(ColumnString_Insert2)

    // Create new list
    c.clear();
    c.add("a");
    c.add("b");
    c.add("c");
    c.add("d");

    // Insert in top with expansion
    c.insert(0, "xxxxx");

    CHECK_EQUAL("xxxxx", c.get(0));
    CHECK_EQUAL("a", c.get(1));
    CHECK_EQUAL("b", c.get(2));
    CHECK_EQUAL("c", c.get(3));
    CHECK_EQUAL("d", c.get(4));
    CHECK_EQUAL(5, c.size());


    // TEST(ColumnString_Insert3)

    // Insert in middle with expansion
    c.insert(3, "xxxxxxxxxx");

    CHECK_EQUAL("xxxxx", c.get(0));
    CHECK_EQUAL("a", c.get(1));
    CHECK_EQUAL("b", c.get(2));
    CHECK_EQUAL("xxxxxxxxxx", c.get(3));
    CHECK_EQUAL("c", c.get(4));
    CHECK_EQUAL("d", c.get(5));
    CHECK_EQUAL(6, c.size());


    // TEST(ColumnString_SetLeafToLong)

    // Test "Replace string array with long string array" when doing
    // it through LeafSet()
    c.clear();

    {
        c.add("foobar");
        c.add("bar abc");
        c.add("baz");

        c.set(1, "40 chars  40 chars  40 chars  40 chars  ");

        CHECK_EQUAL(c.size(), c.size());
        CHECK_EQUAL("foobar", c.get(0));
        CHECK_EQUAL("40 chars  40 chars  40 chars  40 chars  ", c.get(1));
        CHECK_EQUAL("baz", c.get(2));
    }


    // TEST(ColumnString_SetLeafToBig)

    // Test "Replace string array with long string array" when doing
    // it through LeafSet()
    c.clear();

    {
        c.add("foobar");
        c.add("bar abc");
        c.add("baz");

        c.set(1, "70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");

        CHECK_EQUAL(c.size(), c.size());
        CHECK_EQUAL("foobar", c.get(0));
        CHECK_EQUAL("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ", c.get(1));
        CHECK_EQUAL("baz", c.get(2));
    }
}

TEST(ColumnString_AdaptiveStringLeak)
{
    string_column test_resources;
    auto& col = test_resources.get_column();

    std::string large_str(100, 'a'); // use constant larger than 'medium_string_max_size'
    for (size_t i = 0; i != 2 * REALM_MAX_BPNODE_SIZE; ++i) {
        col.insert(0, large_str);
    }
}


TEST(ColumnString_Find1)
{
    string_column test_resources;
    auto& c = test_resources.get_column();

    c.add("a");
    c.add("bc");
    c.add("def");
    c.add("ghij");
    c.add("klmop");

    size_t res1 = c.find_first("");
    CHECK_EQUAL(size_t(-1), res1);

    size_t res2 = c.find_first("xlmno hiuh iuh uih i huih i biuhui");
    CHECK_EQUAL(size_t(-1), res2);

    size_t res3 = c.find_first("klmop");
    CHECK_EQUAL(4, res3);
}

TEST(ColumnString_Find2)
{
    string_column test_resources;
    auto& c = test_resources.get_column();

    c.add("a");
    c.add("bc");
    c.add("def");
    c.add("ghij");
    c.add("klmop");

    // Add a string longer than 64 bytes to expand to long strings
    c.add("xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx");

    size_t res1 = c.find_first("");
    CHECK_EQUAL(size_t(-1), res1);

    size_t res2 = c.find_first("xlmno hiuh iuh uih i huih i biuhui");
    CHECK_EQUAL(size_t(-1), res2);

    size_t res3 = c.find_first("klmop");
    CHECK_EQUAL(4, res3);

    size_t res4 = c.find_first("xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx");
    CHECK_EQUAL(5, res4);
}


// First test if width expansion (nulls->empty string, nulls->non-empty string, empty string->non-empty string, etc)
// works. Then do a fuzzy test at the end.
TEST(ColumnString_Null)
{
    using TEST_TYPE = string_array;
    {
        TEST_TYPE test_resources;
        typename TEST_TYPE::ColumnTestType& a = test_resources.get_column();

        a.add("");
        size_t t = a.find_first("", 0, 1);
        CHECK_EQUAL(t, 0);
    }

    {
        TEST_TYPE test_resources;
        typename TEST_TYPE::ColumnTestType& a = test_resources.get_column();

        a.add("foo");
        a.add("");
        a.add(realm::null());

        CHECK_EQUAL(a.is_null(0), false);
        CHECK_EQUAL(a.is_null(1), false);
        CHECK_EQUAL(a.is_null(2), true);
        CHECK(a.get(0) == "foo");

        // Test set
        a.set_null(0);
        a.set_null(1);
        a.set_null(2);
        CHECK_EQUAL(a.is_null(1), true);
        CHECK_EQUAL(a.is_null(0), true);
        CHECK_EQUAL(a.is_null(2), true);
    }

    {
        TEST_TYPE test_resources;
        typename TEST_TYPE::ColumnTestType& a = test_resources.get_column();

        a.add(realm::null());
        a.add("");
        a.add("foo");

        CHECK_EQUAL(a.is_null(0), true);
        CHECK_EQUAL(a.is_null(1), false);
        CHECK_EQUAL(a.is_null(2), false);
        CHECK(a.get(2) == "foo");

        // Test insert
        a.insert(0, realm::null());
        a.insert(2, realm::null());
        a.insert(4, realm::null());

        CHECK_EQUAL(a.is_null(0), true);
        CHECK_EQUAL(a.is_null(1), true);
        CHECK_EQUAL(a.is_null(2), true);
        CHECK_EQUAL(a.is_null(3), false);
        CHECK_EQUAL(a.is_null(4), true);
        CHECK_EQUAL(a.is_null(5), false);
    }

    {
        TEST_TYPE test_resources;
        typename TEST_TYPE::ColumnTestType& a = test_resources.get_column();

        a.add("");
        a.add(realm::null());
        a.add("foo");

        CHECK_EQUAL(a.is_null(0), false);
        CHECK_EQUAL(a.is_null(1), true);
        CHECK_EQUAL(a.is_null(2), false);
        CHECK(a.get(2) == "foo");

        a.erase(0);
        CHECK_EQUAL(a.is_null(0), true);
        CHECK_EQUAL(a.is_null(1), false);

        a.erase(0);
        CHECK_EQUAL(a.is_null(0), false);
    }

    Random random(random_int<unsigned long>());

    for (size_t t = 0; t < 50; t++) {
        TEST_TYPE test_resources;
        typename TEST_TYPE::ColumnTestType& a = test_resources.get_column();

        // vector that is kept in sync with the ArrayString so that we can compare with it
        std::vector<std::string> v;

        // ArrayString capacity starts at 128 bytes, so we need lots of elements
        // to test if relocation works
        for (size_t i = 0; i < 100; i++) {
            unsigned char rnd = static_cast<unsigned char>(
                random.draw_int<unsigned int>()); //    = 1234 * ((i + 123) * (t + 432) + 423) + 543;

            // Add more often than removing, so that we grow
            if (rnd < 80 && a.size() > 0) {
                size_t del = rnd % a.size();
                a.erase(del);
                v.erase(v.begin() + del);
            }
            else {
                // Generate string with good probability of being empty or realm::null()
                static const char str[] =
                    "This string must be longer than 64 bytes in order to test the BinaryBlob type of strings";
                size_t len;

                if (random.draw_int<int>() > 100)
                    len = rnd % sizeof(str);
                else
                    len = 0;

                StringData sd;
                std::string stdstr;

                if (random.draw_int<int>() > 100) {
                    sd = realm::null();
                    stdstr = "null";
                }
                else {
                    sd = StringData(str, len);
                    stdstr = std::string(str, len);
                }

                if (random.draw_int<int>() > 100) {
                    a.add(sd);
                    v.push_back(stdstr);
                }
                else if (a.size() > 0) {
                    size_t pos = rnd % a.size();
                    a.insert(pos, sd);
                    v.insert(v.begin() + pos, stdstr);
                }

                CHECK_EQUAL(a.size(), v.size());
                for (size_t a_i = 0; a_i < a.size(); a_i++) {
                    if (v[a_i] == "null") {
                        CHECK(a.is_null(a_i));
                        CHECK(a.get(a_i).data() == nullptr);
                    }
                    else {
                        CHECK(a.get(a_i) == v[a_i]);
                    }
                }
            }
        }
    }
}

TEST(ColumnString_SwapRows)
{
    // Normal case
    {
        string_column test_resources;
        auto& c = test_resources.get_column();

        c.add("a");
        c.add("b");
        c.add("c");
        c.add("d");

        CHECK_EQUAL(c.get(1), "b");
        CHECK_EQUAL(c.get(2), "c");
        CHECK_EQUAL(c.size(), 4); // size should not change

        c.swap(1, 2);

        CHECK_EQUAL(c.get(1), "c");
        CHECK_EQUAL(c.get(2), "b");
        CHECK_EQUAL(c.size(), 4);
    }

    // First two elements
    {
        string_column test_resources;
        auto& c = test_resources.get_column();

        c.add("a");
        c.add("b");
        c.add("c");

        c.swap(0, 1);

        CHECK_EQUAL(c.get(0), "b");
        CHECK_EQUAL(c.get(1), "a");
        CHECK_EQUAL(c.size(), 3); // size should not change
    }

    // Last two elements
    {
        string_column test_resources;
        auto& c = test_resources.get_column();

        c.add("a");
        c.add("b");
        c.add("c");

        c.swap(1, 2);

        CHECK_EQUAL(c.get(1), "c");
        CHECK_EQUAL(c.get(2), "b");
        CHECK_EQUAL(c.size(), 3); // size should not change
    }

    // Indices in wrong order
    {
        string_column test_resources;
        auto& c = test_resources.get_column();

        c.add("a");
        c.add("b");
        c.add("c");

        c.swap(2, 1);

        CHECK_EQUAL(c.get(1), "c");
        CHECK_EQUAL(c.get(2), "b");
        CHECK_EQUAL(c.size(), 3); // size should not change
    }

    // Column with duplicate values
    {
        string_column test_resources;
        auto& c = test_resources.get_column();

        c.add("a");
        c.add("a");
        c.add("c");

        c.swap(0, 1);

        CHECK_EQUAL(c.get(0), "a");
        CHECK_EQUAL(c.get(1), "a");
    }
}

TEST(ColumnString_Index)
{
    string_column test_resources;
    auto& asc = test_resources.get_column();

    // 17 elements, to test node splits with REALM_MAX_BPNODE_SIZE = 3 or other small number
    asc.add("HEJSA"); // 0
    asc.add("1");
    asc.add("HEJSA");
    asc.add("3");
    asc.add("HEJSA");
    asc.add("5");
    asc.add("HEJSA");
    asc.add("7");
    asc.add("HEJSA");
    asc.add("9");
    asc.add("HEJSA");
    asc.add("11");
    asc.add("HEJSA");
    asc.add("13");
    asc.add("HEJSA");
    asc.add("15");
    asc.add("HEJSA"); // 16

    size_t ndx0 = asc.find_first("HEJS");
    size_t ndx1 = asc.find_first("HEJSA");
    size_t ndx2 = asc.find_first("1");
    size_t ndx3 = asc.find_first("15");
    CHECK_EQUAL(not_found, ndx0);
    CHECK_EQUAL(0, ndx1);
    CHECK_EQUAL(1, ndx2);
    CHECK_EQUAL(15, ndx3);

    // Set some values
    asc.set(1, "one");
    asc.set(15, "fifteen");
    size_t set1 = asc.find_first("1");
    size_t set2 = asc.find_first("15");
    size_t set3 = asc.find_first("one");
    size_t set4 = asc.find_first("fifteen");
    CHECK_EQUAL(not_found, set1);
    CHECK_EQUAL(not_found, set2);
    CHECK_EQUAL(1, set3);
    CHECK_EQUAL(15, set4);

    // Insert some values
    asc.insert(0, "top");
    asc.insert(8, "middle");
    asc.add("bottom");
    size_t ins1 = asc.find_first("top");
    size_t ins2 = asc.find_first("middle");
    size_t ins3 = asc.find_first("bottom");
    CHECK_EQUAL(0, ins1);
    CHECK_EQUAL(8, ins2);
    CHECK_EQUAL(19, ins3);

    // Delete some values
    asc.erase(0);  // top
    asc.erase(7);  // middle
    asc.erase(17); // bottom
    size_t del1 = asc.find_first("top");
    size_t del2 = asc.find_first("middle");
    size_t del3 = asc.find_first("bottom");
    size_t del4 = asc.find_first("HEJSA");
    size_t del5 = asc.find_first("fifteen");
    CHECK_EQUAL(not_found, del1);
    CHECK_EQUAL(not_found, del2);
    CHECK_EQUAL(not_found, del3);
    CHECK_EQUAL(0, del4);
    CHECK_EQUAL(15, del5);

    // Remove all
    asc.clear();
    size_t c1 = asc.find_first("HEJSA");
    size_t c2 = asc.find_first("fifteen");
    CHECK_EQUAL(not_found, c1);
    CHECK_EQUAL(not_found, c2);
}

/**
 * This test ensures that StringColumn::EraseLeafElem is called. It is called when you
 * have some leaves.
 */
TEST(ColumnString_NonLeafRoot)
{
    // Small strings
    {
        BPlusTree<StringData> c(Allocator::get_default());
        c.create();

        for (int i = 0; i < (REALM_MAX_BPNODE_SIZE + 2); i++) {
            std::string s = util::to_string(i);
            c.add(StringData(s));
        }

        CHECK_EQUAL(c.find_first("3"), 3);
        CHECK_EQUAL(c.find_first("5000"), not_found);

        CHECK_EQUAL(c.get(REALM_MAX_BPNODE_SIZE), util::to_string(REALM_MAX_BPNODE_SIZE));
        CHECK_EQUAL(c.get(REALM_MAX_BPNODE_SIZE + 1), util::to_string(REALM_MAX_BPNODE_SIZE + 1));
        c.erase(REALM_MAX_BPNODE_SIZE);
        CHECK_EQUAL(c.get(REALM_MAX_BPNODE_SIZE), util::to_string(REALM_MAX_BPNODE_SIZE + 1));
        c.erase(REALM_MAX_BPNODE_SIZE);
        CHECK_EQUAL(c.size(), REALM_MAX_BPNODE_SIZE);

        c.destroy();
    }
    // Medium strings
    {
        BPlusTree<StringData> c(Allocator::get_default());
        c.create();

        c.add("This is a medium long string");
        for (int i = 1; i < (REALM_MAX_BPNODE_SIZE + 2); i++) {
            std::string s = util::to_string(i);
            c.add(s);
        }

        CHECK_EQUAL(c.find_first("3"), 3);

        CHECK_EQUAL(c.get(REALM_MAX_BPNODE_SIZE), util::to_string(REALM_MAX_BPNODE_SIZE));
        CHECK_EQUAL(c.get(REALM_MAX_BPNODE_SIZE + 1), util::to_string(REALM_MAX_BPNODE_SIZE + 1));
        c.erase(REALM_MAX_BPNODE_SIZE);
        CHECK_EQUAL(c.get(REALM_MAX_BPNODE_SIZE), util::to_string(REALM_MAX_BPNODE_SIZE + 1));
        c.erase(REALM_MAX_BPNODE_SIZE);
        CHECK_EQUAL(c.size(), REALM_MAX_BPNODE_SIZE);

        c.destroy();
    }
    // Big strings
    {
        BPlusTree<StringData> c(Allocator::get_default());
        c.create();

        c.add("This is a rather long string, that should not be very much shorter");
        for (int i = 1; i < (REALM_MAX_BPNODE_SIZE + 2); i++) {
            std::string s = util::to_string(i);
            c.add(s);
        }

        CHECK_EQUAL(c.find_first("3"), 3);

        CHECK_EQUAL(c.get(REALM_MAX_BPNODE_SIZE), util::to_string(REALM_MAX_BPNODE_SIZE));
        CHECK_EQUAL(c.get(REALM_MAX_BPNODE_SIZE + 1), util::to_string(REALM_MAX_BPNODE_SIZE + 1));
        c.erase(REALM_MAX_BPNODE_SIZE);
        CHECK_EQUAL(c.get(REALM_MAX_BPNODE_SIZE), util::to_string(REALM_MAX_BPNODE_SIZE + 1));
        c.erase(REALM_MAX_BPNODE_SIZE);
        CHECK_EQUAL(c.size(), REALM_MAX_BPNODE_SIZE);

        c.destroy();
    }
    // Upgrade leaf from medium to big
    {
        BPlusTree<StringData> c(Allocator::get_default());
        c.create();

        for (int i = 0; i < (REALM_MAX_BPNODE_SIZE + 2); i++) {
            std::string s = util::to_string(i);
            c.add(s);
        }
        c.set(REALM_MAX_BPNODE_SIZE, "This is a medium long string");
        c.set(REALM_MAX_BPNODE_SIZE + 1, "This is a rather long string, that should not be very much shorter");
        CHECK_EQUAL(c.get(0), "0");
        CHECK_EQUAL(c.get(REALM_MAX_BPNODE_SIZE), "This is a medium long string");
        CHECK_EQUAL(c.get(REALM_MAX_BPNODE_SIZE + 1),
                    "This is a rather long string, that should not be very much shorter");

        c.destroy();
    }
    // Upgrade leaf from small to big while inserting
    {
        BPlusTree<StringData> c(Allocator::get_default());
        c.create();

        for (int i = 0; i < REALM_MAX_BPNODE_SIZE + 1; i++) {
            std::string s = util::to_string(i);
            c.add(s);
        }
        c.add("This is a rather long string, that should not be very much shorter");

        CHECK_EQUAL(c.get(REALM_MAX_BPNODE_SIZE + 1),
                    "This is a rather long string, that should not be very much shorter");

        c.destroy();
    }
    // Upgrade leaf from medium to big while inserting
    {
        BPlusTree<StringData> c(Allocator::get_default());
        c.create();

        c.add("This is a medium long string");
        for (int i = 1; i < REALM_MAX_BPNODE_SIZE + 1; i++) {
            std::string s = util::to_string(i);
            c.add(s);
        }
        c.add("This is a rather long string, that should not be very much shorter");

        CHECK_EQUAL(c.get(REALM_MAX_BPNODE_SIZE + 1),
                    "This is a rather long string, that should not be very much shorter");

        c.destroy();
    }
}

#endif // TEST_COLUMN_STRING
