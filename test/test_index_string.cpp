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
#ifdef TEST_INDEX_STRING

#include <realm.hpp>
#include <realm/index_string.hpp>
#include <realm/column_linklist.hpp>
#include <realm/column_string.hpp>
#include <realm/query_expression.hpp>
#include <realm/util/to_string.hpp>
#include <set>
#include "test.hpp"
#include "util/misc.hpp"
#include "util/random.hpp"

using namespace realm;
using namespace util;
using namespace realm;
using namespace realm::util;
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

// strings used by tests
const char s1[] = "John";
const char s2[] = "Brian";
const char s3[] = "Samantha";
const char s4[] = "Tom";
const char s5[] = "Johnathan";
const char s6[] = "Johnny";
const char s7[] = "Sam";

// integers used by integer index tests
const int64_t ints[] = {0x1111,     0x11112222, 0x11113333, 0x1111333, 0x111122223333ull, 0x1111222233334ull,
                        0x22223333, 0x11112227, 0x11112227, 0x78923};

using nullable = std::true_type;
using non_nullable = std::false_type;

} // anonymous namespace

TEST(StringIndex_NonIndexable)
{
    // Create a column with string values
    Group group;
    TableRef table = group.add_table("table");
    TableRef target_table = group.add_table("target");
    table->add_column_link(type_Link, "link", *target_table);
    table->add_column_link(type_LinkList, "linkList", *target_table);
    table->add_column(type_Double, "double");
    table->add_column(type_Float, "float");
    table->add_column(type_Binary, "binary");

    for (size_t i = 0; i < table->get_column_count(); i++) {
        CHECK_LOGIC_ERROR(table->add_search_index(i), LogicError::illegal_combination);
    }
}

TEST_TYPES(StringIndex_IsEmpty, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;

    // Create a column with string values
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, nullable);

    // Create a new index on column
    const StringIndex& ndx = *col.create_search_index();

    CHECK(ndx.is_empty());

    // Clean up
    col.destroy();
}

TEST_TYPES(StringIndex_BuildIndex, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;

    // Create a column with string values
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, nullable);

    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value
    col.add(s5); // common prefix
    col.add(s6); // common prefix

    // Create a new index on column
    const StringIndex& ndx = *col.create_search_index();

    const size_t r1 = ndx.find_first(s1);
    const size_t r2 = ndx.find_first(s2);
    const size_t r3 = ndx.find_first(s3);
    const size_t r4 = ndx.find_first(s4);
    const size_t r5 = ndx.find_first(s5);
    const size_t r6 = ndx.find_first(s6);

    CHECK_EQUAL(0, r1);
    CHECK_EQUAL(1, r2);
    CHECK_EQUAL(2, r3);
    CHECK_EQUAL(3, r4);
    CHECK_EQUAL(5, r5);
    CHECK_EQUAL(6, r6);

    // Clean up
    col.destroy();
}

TEST_TYPES(StringIndex_DeleteAll, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;

    // Create a column with string values
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, nullable);

    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value
    col.add(s5); // common prefix
    col.add(s6); // common prefix

    // Create a new index on column
    const StringIndex& ndx = *col.create_search_index();

    // Delete all entries
    // (reverse order to avoid ref updates)
    col.erase(6);
    col.erase(5);
    col.erase(4);
    col.erase(3);
    col.erase(2);
    col.erase(1);
    col.erase(0);
#ifdef REALM_DEBUG
    CHECK(ndx.is_empty());
#else
    static_cast<void>(ndx);
#endif

    // Re-insert values
    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value
    col.add(s5); // common prefix
    col.add(s6); // common prefix

    // Delete all entries
    // (in order to force constant ref updating)
    col.erase(0);
    col.erase(0);
    col.erase(0);
    col.erase(0);
    col.erase(0);
    col.erase(0);
    col.erase(0);
#ifdef REALM_DEBUG
    CHECK(ndx.is_empty());
#else
    static_cast<void>(ndx);
#endif

    // Clean up
    col.destroy();
}

TEST_TYPES(StringIndex_Delete, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;

    // Create a column with random values
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, nullable);

    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value

    // Create a new index on column
    const StringIndex& ndx = *col.create_search_index();

    // Delete first item (in index)
    col.erase(1);

    CHECK_EQUAL(0, col.find_first(s1));
    CHECK_EQUAL(1, col.find_first(s3));
    CHECK_EQUAL(2, col.find_first(s4));
    CHECK_EQUAL(not_found, ndx.find_first(s2));

    // Delete last item (in index)
    col.erase(2);

    CHECK_EQUAL(0, col.find_first(s1));
    CHECK_EQUAL(1, col.find_first(s3));
    CHECK_EQUAL(not_found, col.find_first(s4));
    CHECK_EQUAL(not_found, col.find_first(s2));

    // Delete middle item (in index)
    col.erase(1);

    CHECK_EQUAL(0, col.find_first(s1));
    CHECK_EQUAL(not_found, col.find_first(s3));
    CHECK_EQUAL(not_found, col.find_first(s4));
    CHECK_EQUAL(not_found, col.find_first(s2));

    // Delete all items
    col.erase(0);
    col.erase(0);
#ifdef REALM_DEBUG
    CHECK(ndx.is_empty());
#endif

    // Clean up
    col.destroy();
}

TEST_TYPES(StringIndex_MoveLastOver, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;

    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, nullable);

    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value
    col.add(s1); // duplicate value

    col.create_search_index();

    {
        InternalFindResult result;
        FindRes fr = col.find_all_no_copy(s1, result);
        CHECK_EQUAL(fr, FindRes_column);
        if (fr != FindRes_column)
            return;

        IntegerColumn matches(col.get_alloc(), result.payload);

        CHECK_EQUAL(3, result.end_ndx - result.start_ndx);
        CHECK_EQUAL(3, matches.size());
        CHECK_EQUAL(0, matches.get(0));
        CHECK_EQUAL(4, matches.get(1));
        CHECK_EQUAL(5, matches.get(2));
    }

    // Remove a non-s1 row and change the order of the s1 rows
    col.move_last_over(1);

    {
        InternalFindResult result;
        FindRes fr = col.find_all_no_copy(s1, result);
        CHECK_EQUAL(fr, FindRes_column);
        if (fr != FindRes_column)
            return;

        IntegerColumn matches(col.get_alloc(), result.payload);

        CHECK_EQUAL(3, result.end_ndx - result.start_ndx);
        CHECK_EQUAL(3, matches.size());
        CHECK_EQUAL(0, matches.get(0));
        CHECK_EQUAL(1, matches.get(1));
        CHECK_EQUAL(4, matches.get(2));
    }

    // Move a s1 row over a s1 row
    col.move_last_over(1);

    {
        InternalFindResult result;
        FindRes fr = col.find_all_no_copy(s1, result);
        CHECK_EQUAL(fr, FindRes_column);
        if (fr != FindRes_column)
            return;

        IntegerColumn matches(col.get_alloc(), result.payload);

        CHECK_EQUAL(2, result.end_ndx - result.start_ndx);
        CHECK_EQUAL(2, matches.size());
        CHECK_EQUAL(0, matches.get(0));
        CHECK_EQUAL(1, matches.get(1));
    }

    col.destroy();
}

TEST_TYPES(StringIndex_ClearEmpty, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;

    // Create a column with string values
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, nullable);

    // Create a new index on column
    const StringIndex& ndx = *col.create_search_index();

    // Clear to remove all entries
    col.clear();
#ifdef REALM_DEBUG
    CHECK(ndx.is_empty());
#else
    static_cast<void>(ndx);
#endif

    // Clean up
    col.destroy();
}

TEST_TYPES(StringIndex_Clear, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;

    // Create a column with string values
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, nullable);

    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value
    col.add(s5); // common prefix
    col.add(s6); // common prefix

    // Create a new index on column
    const StringIndex& ndx = *col.create_search_index();

    // Clear to remove all entries
    col.clear();
#ifdef REALM_DEBUG
    CHECK(ndx.is_empty());
#else
    static_cast<void>(ndx);
#endif

    // Re-insert values
    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value
    col.add(s5); // common prefix
    col.add(s6); // common prefix

    const size_t r1 = ndx.find_first(s1);
    const size_t r2 = ndx.find_first(s2);
    const size_t r3 = ndx.find_first(s3);
    const size_t r4 = ndx.find_first(s4);
    const size_t r5 = ndx.find_first(s5);
    const size_t r6 = ndx.find_first(s6);

    CHECK_EQUAL(0, r1);
    CHECK_EQUAL(1, r2);
    CHECK_EQUAL(2, r3);
    CHECK_EQUAL(3, r4);
    CHECK_EQUAL(5, r5);
    CHECK_EQUAL(6, r6);

    // Clean up
    col.destroy();
}

TEST_TYPES(StringIndex_Insert, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;

    // Create a column with random values
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, nullable);

    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value

    // Create a new index on column
    col.create_search_index();

    // Insert item in top of column
    col.insert(0, s5);

    CHECK_EQUAL(0, col.find_first(s5));
    CHECK_EQUAL(1, col.find_first(s1));
    CHECK_EQUAL(2, col.find_first(s2));
    CHECK_EQUAL(3, col.find_first(s3));
    CHECK_EQUAL(4, col.find_first(s4));
    // CHECK_EQUAL(5, ndx.find_first(s1)); // duplicate

    // Append item in end of column
    col.insert(6, s6);

    CHECK_EQUAL(0, col.find_first(s5));
    CHECK_EQUAL(1, col.find_first(s1));
    CHECK_EQUAL(2, col.find_first(s2));
    CHECK_EQUAL(3, col.find_first(s3));
    CHECK_EQUAL(4, col.find_first(s4));
    CHECK_EQUAL(6, col.find_first(s6));

    // Insert item in middle
    col.insert(3, s7);

    CHECK_EQUAL(0, col.find_first(s5));
    CHECK_EQUAL(1, col.find_first(s1));
    CHECK_EQUAL(2, col.find_first(s2));
    CHECK_EQUAL(3, col.find_first(s7));
    CHECK_EQUAL(4, col.find_first(s3));
    CHECK_EQUAL(5, col.find_first(s4));
    CHECK_EQUAL(7, col.find_first(s6));

    // Clean up
    col.destroy();
}

TEST_TYPES(StringIndex_Set, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;

    // Create a column with random values
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, nullable);

    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value

    // Create a new index on column
    col.create_search_index();

    // Set top value
    col.set(0, s5);

    CHECK_EQUAL(0, col.find_first(s5));
    CHECK_EQUAL(1, col.find_first(s2));
    CHECK_EQUAL(2, col.find_first(s3));
    CHECK_EQUAL(3, col.find_first(s4));
    CHECK_EQUAL(4, col.find_first(s1));

    // Set bottom value
    col.set(4, s6);

    CHECK_EQUAL(not_found, col.find_first(s1));
    CHECK_EQUAL(0, col.find_first(s5));
    CHECK_EQUAL(1, col.find_first(s2));
    CHECK_EQUAL(2, col.find_first(s3));
    CHECK_EQUAL(3, col.find_first(s4));
    CHECK_EQUAL(4, col.find_first(s6));

    // Set middle value
    col.set(2, s7);

    CHECK_EQUAL(not_found, col.find_first(s3));
    CHECK_EQUAL(not_found, col.find_first(s1));
    CHECK_EQUAL(0, col.find_first(s5));
    CHECK_EQUAL(1, col.find_first(s2));
    CHECK_EQUAL(2, col.find_first(s7));
    CHECK_EQUAL(3, col.find_first(s4));
    CHECK_EQUAL(4, col.find_first(s6));

    // Clean up
    col.destroy();
}

TEST_TYPES(StringIndex_Count, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;

    // Create a column with duplcate values
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, nullable);

    col.add(s1);
    col.add(s2);
    col.add(s2);
    col.add(s3);
    col.add(s3);
    col.add(s3);
    col.add(s4);
    col.add(s4);
    col.add(s4);
    col.add(s4);

    // Create a new index on column
    col.create_search_index();

    // Counts
    const size_t c0 = col.count(s5);
    const size_t c1 = col.count(s1);
    const size_t c2 = col.count(s2);
    const size_t c3 = col.count(s3);
    const size_t c4 = col.count(s4);
    CHECK_EQUAL(0, c0);
    CHECK_EQUAL(1, c1);
    CHECK_EQUAL(2, c2);
    CHECK_EQUAL(3, c3);
    CHECK_EQUAL(4, c4);

    // Clean up
    col.destroy();
}

TEST_TYPES(StringIndex_Distinct, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;

    // Create a column with duplcate values
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, nullable);

    col.add(s1);
    col.add(s2);
    col.add(s2);
    col.add(s3);
    col.add(s3);
    col.add(s3);
    col.add(s4);
    col.add(s4);
    col.add(s4);
    col.add(s4);

    // Create a new index on column
    StringIndex& ndx = *col.create_search_index();

    // Get view of unique values
    // (sorted in alphabetical order, each ref to first match)
    ref_type results_ref = IntegerColumn::create(Allocator::get_default());
    IntegerColumn results(Allocator::get_default(), results_ref);
    ndx.distinct(results);

    CHECK_EQUAL(4, results.size());
    CHECK_EQUAL(1, results.get(0)); // s2 = Brian
    CHECK_EQUAL(0, results.get(1)); // s1 = John
    CHECK_EQUAL(3, results.get(2)); // s3 = Samantha
    CHECK_EQUAL(6, results.get(3)); // s4 = Tom

    // Clean up
    results.destroy();
    col.destroy();
}

TEST_TYPES(StringIndex_FindAllNoCopy, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;

    // Create a column with duplcate values
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, nullable);

    col.add(s1);
    col.add(s2);
    col.add(s2);
    col.add(s3);
    col.add(s3);
    col.add(s3);
    col.add(s4);
    col.add(s4);
    col.add(s4);
    col.add(s4);

    // Create a new index on column
    StringIndex& ndx = *col.create_search_index();

    InternalFindResult ref_2;
    FindRes res1 = ndx.find_all_no_copy(StringData("not there"), ref_2);
    CHECK_EQUAL(FindRes_not_found, res1);

    FindRes res2 = ndx.find_all_no_copy(s1, ref_2);
    CHECK_EQUAL(FindRes_single, res2);
    CHECK_EQUAL(0, ref_2.payload);

    FindRes res3 = ndx.find_all_no_copy(s4, ref_2);
    CHECK_EQUAL(FindRes_column, res3);
    const IntegerColumn results(Allocator::get_default(), ref_type(ref_2.payload));
    CHECK_EQUAL(4, ref_2.end_ndx - ref_2.start_ndx);
    CHECK_EQUAL(4, results.size());
    CHECK_EQUAL(6, results.get(0));
    CHECK_EQUAL(7, results.get(1));
    CHECK_EQUAL(8, results.get(2));
    CHECK_EQUAL(9, results.get(3));

    // Clean up
    col.destroy();
}

// If a column contains a specific value in multiple rows, then the index will store a list of these row numbers
// in form of a column. If you call find_all() on an index, it will return a *reference* to that column instead
// of copying it to you, as a performance optimization.
TEST(StringIndex_FindAllNoCopy2_Int)
{
    // Create a column with duplcate values
    ref_type ref = IntegerColumn::create(Allocator::get_default());
    IntegerColumn col(Allocator::get_default(), ref);

    for (size_t t = 0; t < sizeof(ints) / sizeof(ints[0]); t++)
        col.add(ints[t]);

    // Create a new index on column
    col.create_search_index();
    StringIndex& ndx = *col.get_search_index();
    InternalFindResult results;

    for (size_t t = 0; t < sizeof(ints) / sizeof(ints[0]); t++) {
        FindRes res = ndx.find_all_no_copy(ints[t], results);

        size_t real = 0;
        for (size_t y = 0; y < sizeof(ints) / sizeof(ints[0]); y++) {
            if (ints[t] == ints[y])
                real++;
        }

        if (real == 1) {
            CHECK_EQUAL(res, FindRes_single);
            CHECK_EQUAL(ints[t], ints[results.payload]);
        }
        else if (real > 1) {
            CHECK_EQUAL(FindRes_column, res);
            const IntegerColumn results_column(Allocator::get_default(), ref_type(results.payload));
            CHECK_EQUAL(real, results.end_ndx - results.start_ndx);
            CHECK_EQUAL(real, results_column.size());
            for (size_t y = 0; y < real; y++)
                CHECK_EQUAL(ints[t], ints[results_column.get(y)]);
        }
    }

    // Clean up
    col.destroy();
}

// If a column contains a specific value in multiple rows, then the index will store a list of these row numbers
// in form of a column. If you call find_all() on an index, it will return a *reference* to that column instead
// of copying it to you, as a performance optimization.
TEST(StringIndex_FindAllNoCopy2_IntNull)
{
    // Create a column with duplcate values
    ref_type ref = IntNullColumn::create(Allocator::get_default());
    IntNullColumn col(Allocator::get_default(), ref);

    for (size_t t = 0; t < sizeof(ints) / sizeof(ints[0]); t++)
        col.add(ints[t]);
    col.insert(npos, null{});

    // Create a new index on column
    col.create_search_index();
    StringIndex& ndx = *col.get_search_index();
    InternalFindResult results;

    for (size_t t = 0; t < sizeof(ints) / sizeof(ints[0]); t++) {
        FindRes res = ndx.find_all_no_copy(ints[t], results);

        size_t real = 0;
        for (size_t y = 0; y < sizeof(ints) / sizeof(ints[0]); y++) {
            if (ints[t] == ints[y])
                real++;
        }

        if (real == 1) {
            CHECK_EQUAL(res, FindRes_single);
            CHECK_EQUAL(ints[t], ints[results.payload]);
        }
        else if (real > 1) {
            CHECK_EQUAL(FindRes_column, res);
            const IntegerColumn results2(Allocator::get_default(), ref_type(results.payload));
            CHECK_EQUAL(real, results.end_ndx - results.start_ndx);
            CHECK_EQUAL(real, results2.size());
            for (size_t y = 0; y < real; y++)
                CHECK_EQUAL(ints[t], ints[results2.get(y)]);
        }
    }

    FindRes res = ndx.find_all_no_copy(null{}, results);
    CHECK_EQUAL(FindRes_single, res);
    CHECK_EQUAL(results.payload, col.size() - 1);

    // Clean up
    col.destroy();
}

TEST(StringIndex_FindAllNoCopyCommonPrefixStrings)
{
    // Create a column with duplcate values
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref);
    StringIndex& ndx = *col.create_search_index();

    auto test_prefix_find = [&](std::string prefix) {
        std::string prefix_b = prefix + "b";
        std::string prefix_c = prefix + "c";
        std::string prefix_d = prefix + "d";
        std::string prefix_e = prefix + "e";
        StringData spb(prefix_b);
        StringData spc(prefix_c);
        StringData spd(prefix_d);
        StringData spe(prefix_e);

        size_t start_row = col.size();
        col.add(spb);
        col.add(spc);
        col.add(spc);
        col.add(spe);
        col.add(spe);
        col.add(spe);

        InternalFindResult results;
        FindRes res = ndx.find_all_no_copy(spb, results);
        CHECK_EQUAL(res, FindRes_single);
        CHECK_EQUAL(results.payload, start_row);

        res = ndx.find_all_no_copy(spc, results);
        CHECK_EQUAL(res, FindRes_column);
        CHECK_EQUAL(results.end_ndx - results.start_ndx, 2);
        const IntegerColumn results_c(Allocator::get_default(), ref_type(results.payload));
        CHECK_EQUAL(results_c.get(results.start_ndx), start_row + 1);
        CHECK_EQUAL(results_c.get(results.start_ndx + 1), start_row + 2);
        CHECK_EQUAL(col.get(size_t(results_c.get(results.start_ndx))), spc);
        CHECK_EQUAL(col.get(size_t(results_c.get(results.start_ndx + 1))), spc);

        res = ndx.find_all_no_copy(spd, results);
        CHECK_EQUAL(res, FindRes_not_found);

        res = ndx.find_all_no_copy(spe, results);
        CHECK_EQUAL(res, FindRes_column);
        CHECK_EQUAL(results.end_ndx - results.start_ndx, 3);
        const IntegerColumn results_e(Allocator::get_default(), ref_type(results.payload));
        CHECK_EQUAL(results_e.get(results.start_ndx), start_row + 3);
        CHECK_EQUAL(results_e.get(results.start_ndx + 1), start_row + 4);
        CHECK_EQUAL(results_e.get(results.start_ndx + 2), start_row + 5);
        CHECK_EQUAL(col.get(size_t(results_e.get(results.start_ndx))), spe);
        CHECK_EQUAL(col.get(size_t(results_e.get(results.start_ndx + 1))), spe);
        CHECK_EQUAL(col.get(size_t(results_e.get(results.start_ndx + 2))), spe);
    };

    std::string std_max(StringIndex::s_max_offset, 'a');
    std::string std_over_max = std_max + "a";
    std::string std_under_max(StringIndex::s_max_offset >> 1, 'a');

    test_prefix_find(std_max);
    test_prefix_find(std_over_max);
    test_prefix_find(std_under_max);

    // Clean up
    col.destroy();
}


TEST(StringIndex_Count_Int)
{
    // Create a column with duplcate values
    ref_type ref = IntegerColumn::create(Allocator::get_default());
    IntegerColumn col(Allocator::get_default(), ref);

    for (size_t t = 0; t < sizeof(ints) / sizeof(ints[0]); t++)
        col.add(ints[t]);

    // Create a new index on column
    col.create_search_index();
    StringIndex& ndx = *col.get_search_index();

    for (size_t t = 0; t < sizeof(ints) / sizeof(ints[0]); t++) {
        size_t count = ndx.count(ints[t]);

        size_t real = 0;
        for (size_t y = 0; y < sizeof(ints) / sizeof(ints[0]); y++) {
            if (ints[t] == ints[y])
                real++;
        }

        CHECK_EQUAL(real, count);
    }
    // Clean up
    col.destroy();
}


TEST(StringIndex_Distinct_Int)
{
    // Create a column with duplcate values
    ref_type ref = IntegerColumn::create(Allocator::get_default());
    IntegerColumn col(Allocator::get_default(), ref);

    for (size_t t = 0; t < sizeof(ints) / sizeof(ints[0]); t++)
        col.add(ints[t]);

    // Create a new index on column
    col.create_search_index();


    StringIndex& ndx = *col.get_search_index();

    ref_type results_ref = IntegerColumn::create(Allocator::get_default());
    IntegerColumn results(Allocator::get_default(), results_ref);

    ndx.distinct(results);

    std::set<int64_t> s;
    for (size_t t = 0; t < sizeof(ints) / sizeof(ints[0]); t++) {
        s.insert(ints[t]);
    }

    CHECK_EQUAL(s.size(), results.size());

    // Clean up
    col.destroy();
    results.destroy();
}


TEST(StringIndex_Set_Add_Erase_Insert_Int)
{
    ref_type ref = IntegerColumn::create(Allocator::get_default());
    IntegerColumn col(Allocator::get_default(), ref);

    col.add(1);
    col.add(2);
    col.add(3);
    col.add(2);

    // Create a new index on column
    col.create_search_index();
    StringIndex& ndx = *col.get_search_index();

    size_t f = ndx.find_first(int64_t(2));
    CHECK_EQUAL(1, f);

    col.set(1, 5);

    f = ndx.find_first(int64_t(2));
    CHECK_EQUAL(3, f);

    col.erase(1);

    f = ndx.find_first(int64_t(2));
    CHECK_EQUAL(2, f);

    col.insert(1, 5);
    CHECK_EQUAL(col.get(1), 5);

    f = ndx.find_first(int64_t(2));
    CHECK_EQUAL(3, f);

    col.add(7);
    CHECK_EQUAL(col.get(4), 7);
    col.set(4, 10);
    CHECK_EQUAL(col.get(4), 10);

    f = ndx.find_first(int64_t(10));
    CHECK_EQUAL(col.size() - 1, f);

    col.add(9);
    f = ndx.find_first(int64_t(9));
    CHECK_EQUAL(col.size() - 1, f);

    col.clear();
    f = ndx.find_first(int64_t(2));
    CHECK_EQUAL(not_found, f);

    // Clean up
    col.destroy();
}

TEST(StringIndex_FuzzyTest_Int)
{
    ref_type ref = IntegerColumn::create(Allocator::get_default());
    IntegerColumn col(Allocator::get_default(), ref);
    Random random(random_int<unsigned long>());
    const size_t n = static_cast<size_t>(1.2 * REALM_MAX_BPNODE_SIZE);

    col.create_search_index();

    for (size_t t = 0; t < n; ++t) {
        col.add(random.draw_int_max(0xffffffffffffffff));
    }

    for (size_t t = 0; t < n; ++t) {
        int64_t r;
        if (random.draw_bool())
            r = col.get(t);
        else
            r = random.draw_int_max(0xffffffffffffffff);

        size_t m = col.find_first(r);
        for (size_t t_2 = 0; t_2 < n; ++t_2) {
            if (col.get(t_2) == r) {
                CHECK_EQUAL(t_2, m);
                break;
            }
        }
    }
    col.destroy();
}

namespace {

// Generate string where the bit pattern in bits is converted to NUL bytes. E.g. (length=2):
// bits=0 -> "\0\0", bits=1 -> "\x\0", bits=2 -> "\0\x", bits=3 -> "\x\x", where x is a random byte
StringData create_string_with_nuls(const size_t bits, const size_t length, char* tmp, Random& random)
{
    for (size_t i = 0; i < length; ++i) {
        bool insert_nul_at_pos = (bits & (size_t(1) << i)) == 0;
        if (insert_nul_at_pos) {
            tmp[i] = '\0';
        }
        else {
            // Avoid stray \0 chars, since we are already testing all combinations.
            // All casts are necessary to preserve the bitpattern.
            tmp[i] = static_cast<char>(static_cast<unsigned char>(random.draw_int<unsigned int>(1, UCHAR_MAX)));
        }
    }
    return StringData(tmp, length);
}

} // anonymous namespace


// Test for generated strings of length 1..16 with all combinations of embedded NUL bytes
TEST_TYPES(StringIndex_EmbeddedZeroesCombinations, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;
    constexpr unsigned int seed = 42;

    // String index
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, nullable);
    const StringIndex& ndx = *col.create_search_index();

    const size_t MAX_LENGTH = 16; // Test medium
    char tmp[MAX_LENGTH];         // this is a bit of a hack, that relies on the string being copied in column.add()

    for (size_t length = 1; length <= MAX_LENGTH; ++length) {

        {
            Random random(seed);
            const size_t combinations = size_t(1) << length;
            for (size_t i = 0; i < combinations; ++i) {
                StringData str = create_string_with_nuls(i, length, tmp, random);
                col.add(str);
            }
        }

        // check index up to this length
        size_t expected_index = 0;
        for (size_t l = 1; l <= length; ++l) {
            Random random(seed);
            const size_t combinations = size_t(1) << l;
            for (size_t i = 0; i < combinations; ++i) {
                StringData needle = create_string_with_nuls(i, l, tmp, random);
                CHECK_EQUAL(ndx.find_first(needle), expected_index);
                CHECK(strncmp(col.get(expected_index).data(), needle.data(), l) == 0);
                CHECK_EQUAL(col.get(expected_index).size(), needle.size());
                expected_index++;
            }
        }
    }

    col.destroy();
}

// Tests for a bug with strings containing zeroes
TEST_TYPES(StringIndex_EmbeddedZeroes, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;

    // String index
    ref_type ref2 = StringColumn::create(Allocator::get_default());
    StringColumn col2(Allocator::get_default(), ref2, nullable);
    const StringIndex& ndx2 = *col2.create_search_index();

    // FIXME: re-enable once embedded nuls work
    col2.add(StringData("\0", 1));
    col2.add(StringData("\1", 1));
    col2.add(StringData("\0\0", 2));
    col2.add(StringData("\0\1", 2));
    col2.add(StringData("\1\0", 2));

    CHECK_EQUAL(ndx2.find_first(StringData("\0", 1)), 0);
    CHECK_EQUAL(ndx2.find_first(StringData("\1", 1)), 1);
    CHECK_EQUAL(ndx2.find_first(StringData("\2", 1)), not_found);
    CHECK_EQUAL(ndx2.find_first(StringData("\0\0", 2)), 2);
    CHECK_EQUAL(ndx2.find_first(StringData("\0\1", 2)), 3);
    CHECK_EQUAL(ndx2.find_first(StringData("\1\0", 2)), 4);
    CHECK_EQUAL(ndx2.find_first(StringData("\1\0\0", 3)), not_found);

    // Integer index (uses String index internally)
    int64_t v = 1ULL << 41;
    ref_type ref = IntegerColumn::create(Allocator::get_default());
    IntegerColumn col(Allocator::get_default(), ref);
    col.create_search_index();
    col.add(1ULL << 40);
    StringIndex& ndx = *col.get_search_index();
    size_t f = ndx.find_first(v);
    CHECK_EQUAL(f, not_found);

    col.destroy();
    col2.destroy();
}

TEST(StringIndex_Null)
{
    // Create a column with string values
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, true);

    col.add("");
    col.add(realm::null());

    const StringIndex& ndx = *col.create_search_index();

    const size_t r1 = ndx.find_first(realm::null());
    CHECK_EQUAL(r1, 1);

    col.destroy();
}

TEST_TYPES(StringIndex_Zero_Crash, non_nullable, nullable)
{
    constexpr bool nullable = TEST_TYPE::value;

    // StringIndex could crash if strings ended with one or more 0-bytes
    Table table;
    table.add_column(type_String, "", nullable);
    table.add_empty_row(3);

    table.set_string(0, 0, StringData(""));
    table.set_string(0, 1, StringData("\0", 1));
    table.set_string(0, 2, StringData("\0\0", 2));
    table.add_search_index(0);

    size_t t;

    t = table.find_first_string(0, StringData(""));
    CHECK_EQUAL(0, t);

    t = table.find_first_string(0, StringData("\0", 1));
    CHECK_EQUAL(1, t);

    t = table.find_first_string(0, StringData("\0\0", 2));
    CHECK_EQUAL(2, t);
}

TEST_TYPES(StringIndex_Zero_Crash2, std::true_type, std::false_type)
{
    Random random(random_int<unsigned long>());

    constexpr bool add_common_prefix = TEST_TYPE::value;

    for (size_t iter = 0; iter < 10 + TEST_DURATION * 100; iter++) {
        // StringIndex could crash if strings ended with one or more 0-bytes
        Table table;
        table.add_column(type_String, "", true);

        table.add_search_index(0);

        for (size_t i = 0; i < 100 + TEST_DURATION * 1000; i++) {
            unsigned char action = static_cast<unsigned char>(random.draw_int_max<unsigned int>(100));
            if (action == 0) {
                //                table.remove_search_index(0);
                table.add_search_index(0);
            }
            else if (action > 48 && table.size() < 10) {
                // Generate string with equal probability of being empty, null, short, medium and long, and with
                // their contents having equal proability of being either random or a duplicate of a previous
                // string. When it's random, each char must have equal probability of being 0 or non-0e
                static std::string buf =
                    "This string is around 90 bytes long, which falls in the long-string type of Realm strings";

                std::string copy = buf;

                static std::string buf2 =
                    "                                                                                         ";
                std::string copy2 = buf2;
                StringData sd;

                size_t len = random.draw_int_max<size_t>(3);
                if (len == 0)
                    len = 0;
                else if (len == 1)
                    len = 7;
                else if (len == 2)
                    len = 27;
                else
                    len = random.draw_int_max<size_t>(90);

                copy = copy.substr(0, len);
                if (add_common_prefix) {
                    std::string prefix(StringIndex::s_max_offset, 'a');
                    copy = prefix + copy;
                }

                if (random.draw_int_max<int>(1) == 0) {
                    // duplicate string
                    sd = StringData(copy);
                }
                else {
                    // random string
                    for (size_t t = 0; t < len; t++) {
                        if (random.draw_int_max<int>(100) > 20)
                            copy2[t] = 0; // zero byte
                        else
                            copy2[t] = static_cast<char>(random.draw_int<int>()); // random byte
                    }
                    // no generated string can equal "null" (our vector magic value for null) because
                    // len == 4 is not possible
                    copy2 = copy2.substr(0, len);
                    if (add_common_prefix) {
                        std::string prefix(StringIndex::s_max_offset, 'a');
                        copy2 = prefix + copy2;
                    }
                    sd = StringData(copy2);
                }

                size_t pos = random.draw_int_max<size_t>(table.size());
                table.insert_empty_row(pos);
                table.set_string(0, pos, sd);
                table.verify();
            }
            else if (table.size() > 0) {
                // delete
                size_t row = random.draw_int_max<size_t>(table.size() - 1);
                table.remove(row);
            }

            action = static_cast<unsigned char>(random.draw_int_max<unsigned int>(100));
            if (table.size() > 0) {
                // Search for value that exists
                size_t row = random.draw_int_max<size_t>(table.size() - 1);
                StringData sd = table.get_string(0, row);
                size_t t = table.find_first_string(0, sd);
                StringData sd2 = table.get_string(0, t);
                CHECK_EQUAL(sd, sd2);
            }
        }
    }
}

TEST(StringIndex_Integer_Increasing)
{
    const size_t rows = 2000 + 1000000 * TEST_DURATION;

    // StringIndex could crash if strings ended with one or more 0-bytes
    Table table;
    table.add_column(type_Int, "int");
    table.add_search_index(0);

    std::vector<int64_t> reference;

    for (size_t row = 0; row < rows; row++) {
        int64_t r = fastrand(0x100000);
        table.add_empty_row();
        table.set_int(0, row, r);
        reference.push_back(r);
    }

    std::sort(reference.begin(), reference.end());

    for (size_t row = 0; row < rows; row++) {
        int64_t v = table.get_int(0, row);
        size_t c = table.count_int(0, v);

        size_t start = std::lower_bound(reference.begin(), reference.end(), v) - reference.begin();
        size_t ref_count = 0;
        for (size_t t = start; t < reference.size(); t++) {
            if (reference[t] == v)
                ref_count++;
        }

        CHECK_EQUAL(c, ref_count);
    }
}

TEST(StringIndex_Duplicate_Values)
{
    // Create a column with random values
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, true);

    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);

    // Create a new index on column
    const StringIndex& ndx = *col.create_search_index();

    CHECK(!ndx.has_duplicate_values());

    col.add(s1); // duplicate value

    CHECK(ndx.has_duplicate_values());

    // remove and test again.
    col.erase(4);
    CHECK(!ndx.has_duplicate_values());
    col.add(s1);
    CHECK(ndx.has_duplicate_values());
    col.erase(0);
    CHECK(!ndx.has_duplicate_values());
    col.clear();

    // check emptied set
    CHECK(ndx.is_empty());
    CHECK(!ndx.has_duplicate_values());

    const size_t num_rows = 100;

    for (size_t i = 0; i < num_rows; ++i) {
        std::string to_insert(util::to_string(i));
        col.add(to_insert);
    }
    CHECK(!ndx.has_duplicate_values());

    std::string a_string = "a";
    for (size_t i = 0; i < num_rows; ++i) {
        col.add(a_string);
        a_string += "a";
    }
    std::string str_num_rows(util::to_string(num_rows));
    CHECK(!ndx.has_duplicate_values());
    col.add(a_string);
    col.add(a_string);
    CHECK(ndx.has_duplicate_values());
    col.erase(col.size() - 1);
    CHECK(!ndx.has_duplicate_values());

    // Insert into the middle unique value of num_rows
    col.insert(num_rows / 2, str_num_rows);

    CHECK(!ndx.has_duplicate_values());

    // Set the next element to be num_rows too
    col.set(num_rows / 2 + 1, str_num_rows);

    CHECK(ndx.has_duplicate_values());

    col.clear();
    CHECK(!ndx.has_duplicate_values());
    CHECK(col.size() == 0);

    // Clean up
    col.destroy();
}

namespace {

void verify_single_move_last_over(TestContext& test_context, StringColumn& col, size_t index)
{
    std::string value = col.get(col.size() - 1);
    size_t orig_size = col.size();
    col.move_last_over(index);
    CHECK(col.get(index) == value);
    CHECK(col.size() == orig_size - 1);
}

} // unnamed namespace

TEST(StringIndex_MoveLastOver_DoUpdateRef)
{
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, true);

    // create subindex of repeated elements on a leaf
    size_t num_initial_repeats = 100;
    for (size_t i = 0; i < num_initial_repeats; ++i) {
        std::string str_i(util::to_string(i));
        col.add(str_i);
    }

    // common test strings
    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s5); // common prefix
    col.add(s6); // common prefix
    col.add(s7);

    // Add random data to get sufficient internal nodes
    // 256 is 4 levels deep on a base 4 tree
    const size_t num_new_rand = 256;
    Random random(random_int<unsigned long>());
    for (size_t i = 0; i < num_new_rand; ++i) {
        std::string str_rand(util::to_string(random.draw_int<size_t>()));
        col.add(str_rand);
    }

    // Add a bunch of repeated data
    const size_t num_repeats = 25;
    const size_t num_repeated = 25;
    for (size_t i = 0; i < num_repeats; ++i) {
        for (size_t j = 0; j < num_repeated; ++j) {
            std::string str_i(util::to_string(i));
            col.add(str_i);
        }
    }

    // force build the search index
    col.create_search_index();

    // switch out entire first leaf on a tree where MAX_BPNODE_SIZE == 4
    verify_single_move_last_over(test_context, col, 0);
    verify_single_move_last_over(test_context, col, 1);
    verify_single_move_last_over(test_context, col, 2);
    verify_single_move_last_over(test_context, col, 3);
    verify_single_move_last_over(test_context, col, 4);
    verify_single_move_last_over(test_context, col, 5);

    // move_last_over for last index should remove the last item
    size_t last_size = col.size();
    col.move_last_over(col.size() - 1);
    CHECK(col.size() == last_size - 1);

    // randomly remove remaining elements until col.size() == 1
    while (col.size() > 1) {
        size_t random_index = random.draw_int<size_t>(0, col.size() - 2);
        verify_single_move_last_over(test_context, col, random_index);
    }

    // remove final element
    col.move_last_over(0);
    CHECK(col.size() == 0);

    col.destroy();
}

TEST(StringIndex_MaxBytes)
{
    std::string std_max(StringIndex::s_max_offset, 'a');
    std::string std_over_max(std_max + "a");
    std::string std_under_max(StringIndex::s_max_offset >> 1, 'a');
    StringData max(std_max);
    StringData over_max(std_over_max);
    StringData under_max(std_under_max);

    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, true);

    const StringIndex& ndx = *col.create_search_index();

    CHECK_EQUAL(col.size(), 0);

    auto duplicate_check = [&](size_t num_dups, StringData s) {
        CHECK(col.size() == 0);
        for (size_t i = 0; i < num_dups; ++i) {
            col.add(s);
        }
        CHECK_EQUAL(col.size(), num_dups);
        CHECK(ndx.has_duplicate_values() == (num_dups > 1));
        ref_type results_ref = IntegerColumn::create(Allocator::get_default());
        IntegerColumn results(Allocator::get_default(), results_ref);
        ndx.distinct(results);
        CHECK_EQUAL(results.size(), 1);
        CHECK_EQUAL(results.get(0), 0);
        CHECK_EQUAL(col.get(0), s);
        CHECK_EQUAL(col.count(s), num_dups);
        CHECK_EQUAL(col.find_first(s), 0);
        results.clear();
        col.find_all(results, s);
        CHECK_EQUAL(results.size(), num_dups);
        results.destroy();
        col.clear();
    };

    std::vector<size_t> num_duplicates_list = {
        1, 10, REALM_MAX_BPNODE_SIZE - 1, REALM_MAX_BPNODE_SIZE, REALM_MAX_BPNODE_SIZE + 1,
    };
    for (auto& dups : num_duplicates_list) {
        duplicate_check(dups, under_max);
        duplicate_check(dups, max);
        duplicate_check(dups, over_max);
    }
    col.destroy();
}


// There is a corner case where two very long strings are
// inserted into the string index which are identical except
// for the characters at the end (they have an identical very
// long prefix). This was causing a stack overflow because of
// the recursive nature of the insert function.
TEST(StringIndex_InsertLongPrefix)
{
    ref_type ref = StringColumn::create(Allocator::get_default());
    StringColumn col(Allocator::get_default(), ref, true);

    const StringIndex& ndx = *col.create_search_index();

    col.add("test_index_string1");
    col.add("test_index_string2");

    CHECK(col.has_search_index());
    CHECK_EQUAL(col.find_first("test_index_string1"), 0);
    CHECK_EQUAL(col.find_first("test_index_string2"), 1);

    std::string std_base(107, 'a');
    std::string std_base_b = std_base + "b";
    std::string std_base_c = std_base + "c";
    StringData base_b(std_base_b);
    StringData base_c(std_base_c);
    col.add(base_b);
    col.add(base_c);

    CHECK_EQUAL(col.find_first(base_b), 2);
    CHECK_EQUAL(col.find_first(base_c), 3);

    // To trigger the bug, the length must be more than 10000.
    // Array::destroy_deep() will stack overflow at around recursion depths of
    // lengths > 90000 on mac and less on android devices.
    std::string std_base2(100000, 'a');
    std::string std_base2_b = std_base2 + "b";
    std::string std_base2_c = std_base2 + "c";
    StringData base2(std_base2);
    StringData base2_b(std_base2_b);
    StringData base2_c(std_base2_c);
    col.add(base2_b);
    col.add(base2_c);

    CHECK_EQUAL(col.find_first(base2_b), 4);
    CHECK_EQUAL(col.find_first(base2_c), 5);

    col.add(base2);
    CHECK(!ndx.has_duplicate_values());
    ndx.verify();
    col.add(base2_b); // adds a duplicate in the middle of the list

    CHECK(ndx.has_duplicate_values());
    ref_type results_ref = IntegerColumn::create(Allocator::get_default());
    IntegerColumn results(Allocator::get_default(), results_ref);
    ndx.distinct(results);
    CHECK_EQUAL(results.size(), 7);
    CHECK_EQUAL(col.find_first(base2_b), 4);
    results.clear();
    ndx.find_all(results, base2_b);
    CHECK_EQUAL(results.size(), 2);
    CHECK_EQUAL(results.get(0), 4);
    CHECK_EQUAL(results.get(1), 7);
    results.clear();
    CHECK_EQUAL(ndx.count(base2_b), 2);
    col.verify();

    col.erase(7);
    CHECK_EQUAL(col.find_first(base2_b), 4);
    CHECK_EQUAL(ndx.count(base2_b), 1);
    ndx.distinct(results);
    CHECK_EQUAL(results.size(), 7); // unchanged
    results.clear();
    ndx.find_all(results, base2_b);
    CHECK_EQUAL(results.size(), 1);
    CHECK_EQUAL(results.get(0), 4);
    results.clear();
    col.verify();

    col.set_string(6, base2_b);
    CHECK_EQUAL(ndx.count(base2_b), 2);
    CHECK_EQUAL(col.find_first(base2_b), 4);
    ndx.distinct(results);
    CHECK_EQUAL(results.size(), 6);
    results.clear();
    ndx.find_all(results, base2_b);
    CHECK_EQUAL(results.size(), 2);
    CHECK_EQUAL(results.get(0), 4);
    CHECK_EQUAL(results.get(1), 6);
    col.verify();

    results.destroy();

    col.clear(); // calls recursive function Array::destroy_deep()
    col.destroy();
}

TEST(StringIndex_InsertLongPrefixAndQuery)
{
    constexpr int half_node_size = REALM_MAX_BPNODE_SIZE / 2;
    Group g;
    auto t = g.add_table("StringsOnly");
    t->add_column(type_String, "first");
    t->add_search_index(0);

    std::string base(StringIndex::s_max_offset, 'a');
    std::string str_a = base + "aaaaa";
    std::string str_a0 = base + "aaaa0";
    std::string str_ax = base + "aaaax";
    std::string str_b = base + "bbbbb";
    std::string str_c = base + "ccccc";
    std::string str_c0 = base + "cccc0";
    std::string str_cx = base + "ccccx";

    for (int i = 0; i < half_node_size * 3; i++) {
        auto ndx = t->add_empty_row(3);
        t->set_string(0, ndx, str_a);
        t->set_string(0, ndx + 1, str_b);
        t->set_string(0, ndx + 2, str_c);
    }
    auto ndx = t->add_empty_row(3);
    t->set_string(0, ndx, str_ax);
    t->set_string(0, ndx + 1, str_ax);
    t->set_string(0, ndx + 2, str_a0);
    /*
    {
        std::ofstream o("index.dot");
        index->to_dot(o, "");
    }
    */

    auto ndx_a = t->where().equal(0, StringData(str_a)).find();
    auto cnt = t->count_string(0, StringData(str_a));
    auto tw_a = t->where().equal(0, StringData(str_a)).find_all();
    CHECK_EQUAL(ndx_a, 0);
    CHECK_EQUAL(cnt, half_node_size * 3);
    CHECK_EQUAL(tw_a.size(), half_node_size * 3);
    ndx_a = t->where().equal(0, StringData(str_c0)).find();
    CHECK_EQUAL(ndx_a, npos);
    ndx_a = t->where().equal(0, StringData(str_cx)).find();
    CHECK_EQUAL(ndx_a, npos);
    // Find string that is 'less' than strings in the table, but with identical last key
    tw_a = t->where().equal(0, StringData(str_c0)).find_all();
    CHECK_EQUAL(tw_a.size(), 0);
    // Find string that is 'greater' than strings in the table, but with identical last key
    tw_a = t->where().equal(0, StringData(str_cx)).find_all();
    CHECK_EQUAL(tw_a.size(), 0);

    // Same as above, but just for 'count' method
    cnt = t->count_string(0, StringData(str_c0));
    CHECK_EQUAL(cnt, 0);
    cnt = t->count_string(0, StringData(str_cx));
    CHECK_EQUAL(cnt, 0);
}


TEST(StringIndex_Fuzzy)
{
    constexpr size_t chunkcount = 50;
    constexpr size_t rowcount = 100 + 1000 * TEST_DURATION;

    for (size_t main_rounds = 0; main_rounds < 2 + 10 * TEST_DURATION; main_rounds++) {

        Group g;

        auto t = g.add_table("StringsOnly");
        t->add_column(type_String, "first");
        t->add_column(type_String, "second");

        t->add_search_index(0);

        std::string strings[chunkcount];

        for (size_t j = 0; j < chunkcount; j++) {
            size_t len = fastrand() % REALM_MAX_BPNODE_SIZE;

            for (size_t i = 0; i < len; i++)
                strings[j] += char(fastrand());
        }

        for (size_t rows = 0; rows < rowcount; rows++) {
            // Strings consisting of 2 concatenated strings are very interesting
            size_t chunks;
            if (fastrand() % 2 == 0)
                chunks = fastrand() % 4;
            else
                chunks = 2;

            std::string str;

            for (size_t c = 0; c < chunks; c++) {
                str += strings[fastrand() % chunkcount];
            }

            t->add_empty_row();
            t->set_string(0, t->size() - 1, str);
            t->set_string(1, t->size() - 1, str);
        }

        for (size_t rounds = 0; rounds < 1 + 10 * TEST_DURATION; rounds++) {
            for (size_t r = 0; r < t->size(); r++) {

                TableView tv0 = (t->column<String>(0) == t->get_string(0, r)).find_all();
                TableView tv1 = (t->column<String>(1) == t->get_string(1, r)).find_all();

                CHECK_EQUAL(tv0.size(), tv1.size());

                for (size_t v = 0; v < tv0.size(); v++) {
                    CHECK_EQUAL(tv0.get_source_ndx(v), tv1.get_source_ndx(v));
                }
            }


            for (size_t r = 0; r < 5 + 1000 * TEST_DURATION; r++) {
                size_t chunks;
                if (fastrand() % 2 == 0)
                    chunks = fastrand() % 4;
                else
                    chunks = 2;

                std::string str;

                for (size_t c = 0; c < chunks; c++) {
                    str += strings[fastrand() % chunkcount];
                }

                TableView tv0 = (t->column<String>(0) == str).find_all();
                TableView tv1 = (t->column<String>(1) == str).find_all();

                CHECK_EQUAL(tv0.size(), tv1.size());

                for (size_t v = 0; v < tv0.size(); v++) {
                    CHECK_EQUAL(tv0.get_source_ndx(v), tv1.get_source_ndx(v));
                }
            }
            if (t->size() > 10)
                t->remove(0);

            size_t r1 = fastrand() % t->size();
            size_t r2 = fastrand() % t->size();

            std::string str1 = t->get_string(0, r2);
            std::string str2 = t->get_string(0, r2);

            t->set_string(0, r1, StringData(str1));
            t->set_string(1, r1, StringData(str2));

            r1 = fastrand() % t->size();
            r2 = fastrand() % t->size();

            t.get()->swap_rows(r1, r2);
        }
    }
}


#endif // TEST_INDEX_STRING
