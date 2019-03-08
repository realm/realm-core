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
#ifdef TEST_COLUMN_TIMESTAMP

#include <realm/column_timestamp.hpp>
#include <realm.hpp>

#include "test.hpp"

using namespace realm;


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


TEST_TYPES(TimestampColumn_Basic, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable_toggle);
    TimestampColumn c(nullable_toggle, Allocator::get_default(), ref);
    c.add(Timestamp(123, 123));
    Timestamp ts = c.get(0);
    CHECK(ts == Timestamp(123, 123));
    c.destroy();
}

TEST(TimestampColumn_Basic_Nulls)
{
    constexpr bool nullable = true;
    constexpr bool non_nullable = false;

    // Test that default value is null() for nullable column and non-null for non-nullable column
    Table t;
    t.add_column(type_Timestamp, "date", non_nullable);
    t.add_column(type_Timestamp, "date", nullable);

    t.add_empty_row();
    CHECK(!t.is_null(0, 0));
    CHECK(t.is_null(1, 0));

    CHECK_THROW_ANY(t.set_null(0, 0));
    t.set_null(1, 0);

    CHECK_THROW_ANY(t.set_timestamp(0, 0, Timestamp{}));
}

TEST(TimestampColumn_Relocate)
{
    constexpr bool nullable = true;

    // Fill so much data in a column that it relocates, to check if relocation propagates up correctly
    Table t;
    t.add_column(type_Timestamp, "date", nullable);

    for (unsigned int i = 0; i < 10000; i++) {
        t.add_empty_row();
        t.set_timestamp(0, i, Timestamp(i, i));
    }
}

TEST_TYPES(TimestampColumn_Compare, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable_toggle);
    TimestampColumn c(nullable_toggle, Allocator::get_default(), ref);

    for (unsigned int i = 0; i < 10000; i++) {
        c.add(Timestamp(i, i));
    }

    CHECK(c.compare(c));

    {
        ref_type ref2 = TimestampColumn::create(Allocator::get_default(), 0, nullable_toggle);
        TimestampColumn c2(nullable_toggle, Allocator::get_default(), ref2);
        CHECK_NOT(c.compare(c2));
        c2.destroy();
    }

    c.destroy();
}

TEST_TYPES(TimestampColumn_Index, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable_toggle);
    TimestampColumn c(nullable_toggle, Allocator::get_default(), ref);
    StringIndex* index = c.create_search_index();
    CHECK(index);

    for (int32_t i = 0; i < 100; ++i) {
        c.add(Timestamp{i + 10000, i});
    }

    Timestamp last_value{10099, 99};

    CHECK_EQUAL(index->find_first(last_value), 99);

    index->destroy();
    c.destroy_search_index();
    c.destroy();
}

TEST_TYPES(TimestampColumn_Is_Nullable, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable_toggle);
    TimestampColumn c(nullable_toggle, Allocator::get_default(), ref);
    CHECK_EQUAL(c.is_nullable(), nullable_toggle);
    c.destroy();
}

TEST(TimestampColumn_Set_Null_With_Index)
{
    constexpr bool nullable = true;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable);
    TimestampColumn c(nullable, Allocator::get_default(), ref);
    c.add(Timestamp{1, 1});
    CHECK(!c.is_null(0));

    StringIndex* index = c.create_search_index();
    CHECK(index);

    c.set_null(0);
    CHECK(c.is_null(0));

    index->destroy();
    c.destroy_search_index();
    c.destroy();
}

TEST_TYPES(TimestampColumn_Insert_Rows_With_Index, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable_toggle);
    TimestampColumn c(nullable_toggle, Allocator::get_default(), ref);

    StringIndex* index = c.create_search_index();
    CHECK(index);

    c.insert_rows(0, 1, 0, true);
    c.set(0, Timestamp{1, 1});
    c.insert_rows(1, 1, 1, true);

    index->destroy();
    c.destroy_search_index();
    c.destroy();
}

TEST(TimestampColumn_Move_Last_Over)
{
    constexpr bool nullable = true;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable);
    TimestampColumn c(nullable, Allocator::get_default(), ref);
    StringIndex* index = c.create_search_index();
    CHECK(index);

    c.add(Timestamp{1, 1});
    c.add(Timestamp{2, 2});
    c.add(Timestamp{3, 3});
    c.set_null(2);
    c.move_last_row_over(0, 3, false);
    CHECK(c.is_null(0));

    index->destroy();
    c.destroy_search_index();
    c.destroy();
}

TEST_TYPES(TimestampColumn_Clear, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable_toggle);
    TimestampColumn c(nullable_toggle, Allocator::get_default(), ref);
    StringIndex* index = c.create_search_index();
    CHECK(index);

    c.add(Timestamp{1, 1});
    c.add(Timestamp{2, 2});
    c.clear(2, false);
    c.add(Timestamp{3, 3});

    Timestamp last_value{3, 3};
    CHECK_EQUAL(c.get(0), last_value);

    index->destroy();
    c.destroy_search_index();
    c.destroy();
}

TEST(TimestampColumn_StringIndex)
{
    constexpr bool nullable = true;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable);
    TimestampColumn c(nullable, Allocator::get_default(), ref);

    Timestamp first(123, 123);
    Timestamp second(1234, 1234);

    CHECK_EQUAL(c.size(), 0);
    CHECK_EQUAL(first, first);
    CHECK_NOT_EQUAL(first, second);

    CHECK(!c.has_search_index());

    c.add(first);

    CHECK_EQUAL(c.size(), 1);

    Timestamp ts = c.get(0);
    CHECK(ts == Timestamp(123, 123));

    StringIndex* index = c.create_search_index();
    c.add(second);

    CHECK_EQUAL(index->count(second), 1);
    CHECK_EQUAL(c.size(), 2);

    c.erase_rows(1, 1, c.size(), false);
    CHECK_EQUAL(c.size(), 1);
    CHECK_EQUAL(index->count(second), 0);
    c.erase_rows(0, 1, c.size(), false);
    CHECK_EQUAL(index->count(first), 0);
    CHECK_EQUAL(c.size(), 0);

    c.add(first);
    c.add(second);

    c.swap_rows(0, 1);

    CHECK_EQUAL(c.get(0), second);
    CHECK_EQUAL(c.get(1), first);
    CHECK_EQUAL(index->find_first(second), 0);
    CHECK_EQUAL(index->find_first(first), 1);

    c.set(0, first);
    c.set(1, second);

    CHECK_EQUAL(c.get(0), first);
    CHECK_EQUAL(c.get(1), second);
    CHECK_EQUAL(index->find_first(first), 0);
    CHECK_EQUAL(index->find_first(second), 1);

    c.set_null(0);

    CHECK(c.get(0).is_null());
    CHECK_EQUAL(index->find_first(first), realm::npos);

    c.move_last_row_over(0, c.size(), true);

    CHECK(c.size() == 1);
    CHECK_EQUAL(c.get(0), second);
    CHECK_EQUAL(index->find_first(second), 0);

    c.clear(1, false);
    CHECK(c.size() == 0);
    CHECK_EQUAL(index->find_first(second), realm::npos);

    c.insert_rows(0, 5, 0, false);
    CHECK(c.size() == 5);
    c.set(4, first);
    CHECK_EQUAL(c.get(4), first);
    CHECK_EQUAL(index->find_first(first), 4);

    c.destroy();
}

TEST_TYPES(TimestampColumn_SwapRows, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable_toggle);
    TimestampColumn c(nullable_toggle, Allocator::get_default(), ref);
    StringIndex* index = c.create_search_index();
    CHECK(index);

    Timestamp one{1, 1};
    Timestamp three{3, 3};
    c.add(one);
    c.add(Timestamp{2, 2});
    c.add(three);


    CHECK_EQUAL(c.get(0), one);
    CHECK_EQUAL(c.get(2), three);
    c.swap_rows(0, 2);
    CHECK_EQUAL(c.get(2), one);
    CHECK_EQUAL(c.get(0), three);

    index->destroy();
    c.destroy_search_index();
    c.destroy();
}

TEST_TYPES(TimestampColumn_DeleteWithIndex, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable_toggle);
    TimestampColumn c(nullable_toggle, Allocator::get_default(), ref);
    StringIndex* index = c.create_search_index();
    CHECK(index);


    c.add(Timestamp{2, 2});
    c.erase_rows(0, 1, 1, false);
    CHECK_EQUAL(c.size(), 0);

    index->destroy();
    c.destroy_search_index();
    c.destroy();
}


// Bug found by AFL during development of TimestampColumn
TEST_TYPES(TimestampColumn_DeleteAfterSetWithIndex, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable_toggle);
    TimestampColumn c(nullable_toggle, Allocator::get_default(), ref);
    StringIndex* index = c.create_search_index();
    CHECK(index);

    c.add(Timestamp{1, 1});
    c.set(0, Timestamp{2, 2});
    c.erase_rows(0, 1, 1, false);
    CHECK_EQUAL(c.size(), 0);

    index->destroy();
    c.destroy_search_index();
    c.destroy();
}


// Bug found by AFL during development of TimestampColumn
TEST(TimestampColumn_DeleteAfterSetNullWithIndex)
{
    constexpr bool nullable = true;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable);
    TimestampColumn c(nullable, Allocator::get_default(), ref);
    StringIndex* index = c.create_search_index();
    CHECK(index);

    c.add(Timestamp{0, 0});
    c.set_null(0);
    c.add(Timestamp{1, 1});
    c.add(Timestamp{2, 2});
    c.erase_rows(0, 1, 1, false);
    CHECK_EQUAL(c.size(), 2);

    index->destroy();
    c.destroy_search_index();
    c.destroy();
}


// Bug found by AFL during development of TimestampColumn
TEST(TimestampColumn_LargeNegativeTimestampSearchIndex)
{
    constexpr bool nullable = true;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable);
    TimestampColumn c(nullable, Allocator::get_default(), ref);

    c.add(Timestamp{-1934556340879361, 0});
    StringIndex* index = c.create_search_index();
    CHECK(index);
    c.set_null(0);

    c.erase_rows(0, 1, 1, false);
    CHECK_EQUAL(c.size(), 0);

    index->destroy();
    c.destroy_search_index();
    c.destroy();
}


TEST(TimestampColumn_LargeNegativeTimestampSearchIndexErase)
{
    constexpr bool nullable = true;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable);
    TimestampColumn c(nullable, Allocator::get_default(), ref);

    c.add(Timestamp{-1934556340879361, 0});
    StringIndex* index = c.create_search_index();
    CHECK(index);
    c.set_null(0);

    c.erase(0, true);
    CHECK_EQUAL(c.size(), 0);
    CHECK(index->is_empty());

    index->destroy();
    c.destroy_search_index();
    c.destroy();
}

namespace { // anonymous namespace

template <class T, class C>
bool compare(T&& a, T&& b, C&& condition)
{
    return condition(a, b, a.is_null(), b.is_null());
}

} // anonymous namespace

TEST(TimestampColumn_Operators)
{
    // Note that the Timestamp::operator==, operator>, operator<, operator>=, etc, do not work
    // if one of the Timestamps are null! Please use realm::Greater, realm::Equal, etc instead.

    // Test A. Note that Timestamp{} is null and Timestamp(0, 0) is non-null
    // -----------------------------------------------------------------------------------------
    CHECK(compare(Timestamp{}, Timestamp{}, realm::Equal()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::Equal()));
    CHECK(compare(Timestamp(1, 2), Timestamp(1, 2), realm::Equal()));
    CHECK(compare(Timestamp(-1, -2), Timestamp(-1, -2), realm::Equal()));

    // Test B
    // -----------------------------------------------------------------------------------------
    CHECK(!compare(Timestamp{}, Timestamp(0, 0), realm::Equal()));
    CHECK(!compare(Timestamp(0, 0), Timestamp{}, realm::Equal()));
    CHECK(!compare(Timestamp(0, 0), Timestamp(0, 1), realm::Equal()));
    CHECK(!compare(Timestamp(0, 1), Timestamp(0, 0), realm::Equal()));
    CHECK(!compare(Timestamp(1, 0), Timestamp(0, 0), realm::Equal()));
    CHECK(!compare(Timestamp(0, 0), Timestamp(1, 0), realm::Equal()));

    // Test C: !compare(..., Equal) == compare(..., NotEqual)
    // -----------------------------------------------------------------------------------------
    CHECK(compare(Timestamp{}, Timestamp(0, 0), realm::NotEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp{}, realm::NotEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 1), realm::NotEqual()));
    CHECK(compare(Timestamp(0, 1), Timestamp(0, 0), realm::NotEqual()));
    CHECK(compare(Timestamp(1, 0), Timestamp(0, 0), realm::NotEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(1, 0), realm::NotEqual()));

    // Test D: compare(..., Equal) == true implies that compare(..., GreaterEqual) == true
    // (but not vice versa). So we copy/pate tests from test B again:
    // -----------------------------------------------------------------------------------------
    CHECK(compare(Timestamp{}, Timestamp{}, realm::GreaterEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::GreaterEqual()));
    CHECK(compare(Timestamp(1, 2), Timestamp(1, 2), realm::GreaterEqual()));
    CHECK(compare(Timestamp(-1, -2), Timestamp(-1, -2), realm::GreaterEqual()));

    CHECK(compare(Timestamp{}, Timestamp{}, realm::LessEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::LessEqual()));
    CHECK(compare(Timestamp(1, 2), Timestamp(1, 2), realm::LessEqual()));
    CHECK(compare(Timestamp(-1, -2), Timestamp(-1, -2), realm::LessEqual()));

    // Test E: Sorting order of nulls vs. non-nulls should be the same for Timestamp as for other types
    // -----------------------------------------------------------------------------------------
    // All four data elements are null here (StringData{} means null)
    CHECK(compare(Timestamp{}, Timestamp{}, realm::Greater()) ==
          compare(StringData{}, StringData{}, realm::Greater()));

    // Compare null with non-nulls (Timestamp(0, 0) is non-null and StringData("") is non-null
    CHECK(compare(Timestamp(0, 0), Timestamp{}, realm::Greater()) ==
          compare(StringData(""), StringData{}, realm::Greater()));

    // All four elements are non-nulls
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::Greater()) ==
          compare(StringData(""), StringData(""), realm::Greater()));

    // Repeat with other operators than Greater
    CHECK(compare(Timestamp{}, Timestamp{}, realm::Less()) == compare(StringData{}, StringData{}, realm::Less()));
    CHECK(compare(Timestamp(0, 0), Timestamp{}, realm::Less()) ==
          compare(StringData(""), StringData{}, realm::Less()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::Less()) ==
          compare(StringData(""), StringData(""), realm::Less()));

    CHECK(compare(Timestamp{}, Timestamp{}, realm::Equal()) == compare(StringData{}, StringData{}, realm::Equal()));
    CHECK(compare(Timestamp(0, 0), Timestamp{}, realm::Equal()) ==
          compare(StringData(""), StringData{}, realm::Equal()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::Equal()) ==
          compare(StringData(""), StringData(""), realm::Equal()));

    CHECK(compare(Timestamp{}, Timestamp{}, realm::NotEqual()) ==
          compare(StringData{}, StringData{}, realm::NotEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp{}, realm::NotEqual()) ==
          compare(StringData(""), StringData{}, realm::NotEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::NotEqual()) ==
          compare(StringData(""), StringData(""), realm::NotEqual()));

    CHECK(compare(Timestamp{}, Timestamp{}, realm::GreaterEqual()) ==
          compare(StringData{}, StringData{}, realm::GreaterEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp{}, realm::GreaterEqual()) ==
          compare(StringData(""), StringData{}, realm::GreaterEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::GreaterEqual()) ==
          compare(StringData(""), StringData(""), realm::GreaterEqual()));

    CHECK(compare(Timestamp{}, Timestamp{}, realm::LessEqual()) ==
          compare(StringData{}, StringData{}, realm::LessEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp{}, realm::LessEqual()) ==
          compare(StringData(""), StringData{}, realm::LessEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::LessEqual()) ==
          compare(StringData(""), StringData(""), realm::LessEqual()));
}


TEST_TYPES(TimestampColumn_ForceReallocate, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable_toggle);
    TimestampColumn c(nullable_toggle, Allocator::get_default(), ref);

    int32_t items_count = REALM_MAX_BPNODE_SIZE * 5;
    for (int32_t i = 0; i < items_count; ++i) {
        c.add(Timestamp{i, i});
    }

    CHECK_EQUAL(c.size(), items_count);

    c.destroy();
}

TEST(TimestampColumn_FindFirst)
{
    constexpr bool nullable = true;
    constexpr bool non_nullable = false;

    Table t;
    t.add_column(type_Timestamp, "date", nullable);
    t.add_column(type_Timestamp, "date", non_nullable);

    t.add_empty_row(10);

    t.set_timestamp(0, 0, Timestamp{}); // null
    t.set_timestamp(0, 1, Timestamp(0, 0));
    t.set_timestamp(0, 2, Timestamp(1, 0));
    t.set_timestamp(0, 3, Timestamp(0, 1));
    t.set_timestamp(0, 4, Timestamp(1, 1));
    t.set_timestamp(0, 5, Timestamp(-1, 0));

    t.set_timestamp(1, 0, Timestamp(0, 0)); // null not possible, so insert (0, 0 instead)
    t.set_timestamp(1, 1, Timestamp(0, 0));
    t.set_timestamp(1, 2, Timestamp(1, 0));
    t.set_timestamp(1, 3, Timestamp(0, 1));
    t.set_timestamp(1, 4, Timestamp(1, 1));
    t.set_timestamp(1, 5, Timestamp(-1, 0));

    CHECK_EQUAL(t.find_first_timestamp(0, Timestamp{}), 0);
    CHECK_EQUAL(t.find_first_timestamp(0, Timestamp(0, 0)), 1);
    CHECK_EQUAL(t.find_first_timestamp(0, Timestamp(1, 0)), 2);
    CHECK_EQUAL(t.find_first_timestamp(0, Timestamp(0, 1)), 3);
    CHECK_EQUAL(t.find_first_timestamp(0, Timestamp(1, 1)), 4);
    CHECK_EQUAL(t.find_first_timestamp(0, Timestamp(-1, 0)), 5);

    CHECK_EQUAL(t.find_first_timestamp(1, Timestamp(0, 0)), 0);
    CHECK_EQUAL(t.find_first_timestamp(1, Timestamp(1, 0)), 2);
    CHECK_EQUAL(t.find_first_timestamp(1, Timestamp(0, 1)), 3);
    CHECK_EQUAL(t.find_first_timestamp(1, Timestamp(1, 1)), 4);
    CHECK_EQUAL(t.find_first_timestamp(1, Timestamp(-1, 0)), 5);
}

TEST(TimestampColumn_AddColumnAfterRows)
{
    constexpr bool nullable = true;
    constexpr bool non_nullable = false;

    Table t;
    t.add_column(type_Int, "1", non_nullable);
    t.add_empty_row(REALM_MAX_BPNODE_SIZE * 2 + 1);
    t.set_int(0, 0, 100);

    t.add_column(type_Timestamp, "2", non_nullable);
    t.add_column(type_Timestamp, "3", nullable);
    CHECK_EQUAL(t.get_timestamp(1, 0).get_seconds(), 0);
    CHECK_EQUAL(t.get_timestamp(1, 0).get_nanoseconds(), 0);
    CHECK(t.get_timestamp(2, 0).is_null());
    CHECK(t.is_null(2, 0));
}

// max/min on pure null timestamps must reuturn npos like for int, float and double
TEST(TimestampColumn_AggregateBug)
{
    size_t index;
    Table t;
    TableView tv;
    Timestamp ts;

    t.add_column(type_Timestamp, "ts", true);
    t.add_empty_row(4);
    tv = t.where().find_all();
    CHECK_EQUAL(4, tv.size());
    ts = tv.maximum_timestamp(0, &index);
    CHECK_EQUAL(npos, index);
    ts = tv.minimum_timestamp(0, &index);
    CHECK_EQUAL(npos, index);

    Query q;

    ts = t.where().maximum_timestamp(0, &index);
    CHECK_EQUAL(npos, index);

    ts = t.where().minimum_timestamp(0, &index);
    CHECK_EQUAL(npos, index);

    t.set_timestamp(0, 2, Timestamp(1, 0));

    ts = t.where().maximum_timestamp(0, &index);
    CHECK_EQUAL(2, index);
    CHECK_EQUAL(ts, Timestamp(1, 0));

    ts = t.where().minimum_timestamp(0, &index);
    CHECK_EQUAL(2, index);
    CHECK_EQUAL(ts, Timestamp(1, 0));

    t.set_timestamp(0, 3, Timestamp(1, 1));

    ts = t.where().maximum_timestamp(0, &index);
    CHECK_EQUAL(3, index);
    CHECK_EQUAL(ts, Timestamp(1, 1));

    ts = t.where().minimum_timestamp(0, &index);
    CHECK_EQUAL(2, index);
    CHECK_EQUAL(ts, Timestamp(1, 0));
}

TEST(Table_DistinctTimestamp)
{
    Table table;
    table.add_column(type_Timestamp, "first");
    table.add_empty_row(4);
    table.set_timestamp(0, 0, Timestamp(0, 0));
    table.set_timestamp(0, 1, Timestamp(1, 0));
    table.set_timestamp(0, 2, Timestamp(3, 0));
    table.set_timestamp(0, 3, Timestamp(3, 0));

    table.add_search_index(0);
    CHECK(table.has_search_index(0));

    TableView view = table.get_distinct_view(0);
    CHECK_EQUAL(3, view.size());
}

TEST_TYPES(Timestamp_Conversions, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    ref_type ref = TimestampColumn::create(Allocator::get_default(), 0, nullable_toggle);
    TimestampColumn c(nullable_toggle, Allocator::get_default(), ref);

    constexpr int64_t millis[] = {1, 0, -1, 1000, -1000, 1001, -1001, 203558400, 1461746402, -1000000000};
    constexpr size_t num_millis = sizeof(millis) / sizeof(millis[0]);

    for (size_t i = 0; i < num_millis; ++i) {
        const int64_t milliseconds = millis[i];
        const Timestamp ts = Timestamp::from_milliseconds(milliseconds);
        c.add(ts);
    }

    for (size_t i = 0; i < num_millis; ++i) {
        const Timestamp ts = c.get(i);
        const int64_t milliseconds = ts.to_milliseconds();
        CHECK_EQUAL(milliseconds, millis[i]);
    }

    c.destroy();
}

TEST_TYPES(Timestamp_factory_methods, std::true_type, std::false_type)
{

    auto epoch = Timestamp::epoch();
    CHECK(epoch.get_seconds() == 0);
    CHECK(epoch.get_nanoseconds() == 0);

    auto max = Timestamp::max();
    CHECK(max.get_seconds() == INT64_MAX);
    CHECK(max.get_nanoseconds() == Timestamp::nanoseconds_per_second - 1);

    auto min = Timestamp::min();
    CHECK(min.get_seconds() == INT64_MIN);
    CHECK(min.get_nanoseconds() == -Timestamp::nanoseconds_per_second + 1);

    // Fuzzy check
    auto now = std::chrono::system_clock::now();
    int64_t now_ms_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    int64_t ts_now = Timestamp::now().to_milliseconds();
    CHECK(now_ms_since_epoch <= ts_now);
    CHECK(ts_now <= now_ms_since_epoch + 5);

    CHECK(Timestamp::from_milliseconds(0) == Timestamp::epoch());

    auto ms_neg = Timestamp::from_milliseconds(-1100);
    CHECK(ms_neg.get_seconds() == -1);
    CHECK(ms_neg.get_nanoseconds() == -100000000);

    auto ms_pos = Timestamp::from_milliseconds(1100);
    CHECK(ms_pos.get_seconds() == 1);
    CHECK(ms_pos.get_nanoseconds() == 100000000);
}

TEST_TYPES(Timestamp_modifier_methods, std::true_type, std::false_type)
{
    // Seconds
    CHECK(Timestamp::max().add_seconds(1) == Timestamp::max());
    CHECK(Timestamp::min().add_seconds(-1) == Timestamp::min());
    CHECK(Timestamp::epoch().add_seconds(0) == Timestamp::epoch());
    CHECK(Timestamp::epoch().add_seconds(1) == Timestamp(1, 0));
    CHECK(Timestamp::epoch().add_seconds(-1) == Timestamp(-1, 0));

    // Milliseconds
    CHECK(Timestamp::max().add_milliseconds(1) == Timestamp::max());
    CHECK(Timestamp::min().add_milliseconds(-1) == Timestamp::min());
    CHECK(Timestamp::epoch().add_milliseconds(0) == Timestamp::epoch());
    CHECK(Timestamp::epoch().add_milliseconds(1100) == Timestamp(1, 100000000));
    CHECK(Timestamp::epoch().add_milliseconds(-1100) == Timestamp( -1, -100000000));

    // Nanoseconds
    CHECK(Timestamp::max().add_nanoseconds(1) == Timestamp::max());
    CHECK(Timestamp::min().add_nanoseconds(-1) == Timestamp::min());
    CHECK(Timestamp::epoch().add_nanoseconds(0) == Timestamp::epoch());
    CHECK(Timestamp::epoch().add_nanoseconds(1100000000) == Timestamp(1, 100000000));
    CHECK(Timestamp::epoch().add_nanoseconds(-1100000000) == Timestamp( -1, -100000000));
    CHECK(Timestamp(0, Timestamp::nanoseconds_per_second - 1).add_nanoseconds(1) == Timestamp(1, 0));
    CHECK(Timestamp(0, -Timestamp::nanoseconds_per_second + 1).add_nanoseconds(-1) == Timestamp(-1, 0));
    CHECK(Timestamp(INT64_MAX, Timestamp::nanoseconds_per_second - 1).add_nanoseconds(1) == Timestamp::max());
    CHECK(Timestamp(INT64_MIN, -Timestamp::nanoseconds_per_second + 1).add_nanoseconds(-1) == Timestamp::min());
}

TEST_TYPES(Timestamp_conversion_methods, std::true_type, std::false_type)
{
    CHECK(Timestamp::max().to_milliseconds() == INT64_MAX);
    CHECK(Timestamp::min().to_milliseconds() == INT64_MIN);
    CHECK(Timestamp::epoch().to_milliseconds() == 0);
    CHECK(Timestamp((INT64_MAX/1000) + 1, 0).to_milliseconds() == INT64_MAX);
    CHECK(Timestamp((INT64_MIN/1000) - 1, 0).to_milliseconds() == INT64_MIN);
    CHECK(Timestamp(1, 500000 /* 5 microsec.*/).to_milliseconds() == 1000);
    CHECK(Timestamp(1, 500001 /* ~5 microsec.*/).to_milliseconds() == 1000);
    CHECK(Timestamp(-1, -500000 /* 5 microsec.*/).to_milliseconds() == -1000);
    CHECK(Timestamp(-1, -500001 /* ~5 microsec.*/).to_milliseconds() == -1000);

}



#endif // TEST_COLUMN_TIMESTAMP
