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

#include <realm.hpp>
#include <realm/bplustree.hpp>
#include <realm/array_timestamp.hpp>
#include <realm/array_key.hpp>

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


using TimestampColumn = BPlusTree<Timestamp>;

TEST(TimestampColumn_Basic)
{
    TimestampColumn c(Allocator::get_default());
    c.create();
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
    auto col_non_nullable = t.add_column(type_Timestamp, "date", non_nullable);
    auto col_nullable = t.add_column(type_Timestamp, "date_null", nullable);

    Obj obj = t.create_object();
    CHECK(!obj.is_null(col_non_nullable));
    CHECK(obj.is_null(col_nullable));

    CHECK_THROW_ANY(obj.set_null(col_non_nullable));
    obj.set_null(col_nullable);

    CHECK_THROW_ANY(obj.set<Timestamp>(col_non_nullable, Timestamp{}));
}

TEST(TimestampColumn_Relocate)
{
    constexpr bool nullable = true;

    // Fill so much data in a column that it relocates, to check if relocation propagates up correctly
    Table t;
    auto col = t.add_column(type_Timestamp, "date", nullable);

    for (unsigned int i = 0; i < 10000; i++) {
        t.create_object().set<Timestamp>(col, Timestamp(i, i));
    }
}


TEST(TimestampColumn_SwapRows)
{
    BPlusTree<Timestamp> c(Allocator::get_default());
    c.create();

    Timestamp one{1, 1};
    Timestamp three{3, 3};
    c.add(one);
    c.add(Timestamp{2, 2});
    c.add(three);


    CHECK_EQUAL(c.get(0), one);
    CHECK_EQUAL(c.get(2), three);
    c.swap(0, 2);
    CHECK_EQUAL(c.get(2), one);
    CHECK_EQUAL(c.get(0), three);

    c.destroy();
}

TEST(TimestampColumn_LargeNegativeTimestampSearchIndexErase)
{
    Table t;
    auto col = t.add_column(type_Timestamp, "date", true);
    Obj obj = t.create_object();

    obj.set(col, Timestamp{-1934556340879361, 0});
    t.add_search_index(col);
    CHECK(t.has_search_index(col));
    obj.set_null(col);

    obj.remove();
    CHECK_EQUAL(t.size(), 0);
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


TEST(TimestampColumn_ForceReallocate)
{
    BPlusTree<Timestamp> c(Allocator::get_default());
    c.create();

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
    auto col_nullable = t.add_column(type_Timestamp, "date_null", nullable);
    auto col_non_nullable = t.add_column(type_Timestamp, "date", non_nullable);

    std::vector<ObjKey> keys;
    t.create_objects(10, keys);

    t.get_object(keys[0]).set_all(Timestamp{}, Timestamp(0, 0)); // null
    t.get_object(keys[1]).set_all(Timestamp(0, 0), Timestamp(0, 0));
    t.get_object(keys[2]).set_all(Timestamp(1, 0), Timestamp(1, 0));
    t.get_object(keys[3]).set_all(Timestamp(0, 1), Timestamp(0, 1));
    t.get_object(keys[4]).set_all(Timestamp(1, 1), Timestamp(1, 1));
    t.get_object(keys[5]).set_all(Timestamp(-1, 0), Timestamp(-1, 0));

    CHECK_EQUAL(t.find_first_timestamp(col_nullable, Timestamp{}), keys[0]);
    CHECK_EQUAL(t.find_first_timestamp(col_nullable, Timestamp(0, 0)), keys[1]);
    CHECK_EQUAL(t.find_first_timestamp(col_nullable, Timestamp(1, 0)), keys[2]);
    CHECK_EQUAL(t.find_first_timestamp(col_nullable, Timestamp(0, 1)), keys[3]);
    CHECK_EQUAL(t.find_first_timestamp(col_nullable, Timestamp(1, 1)), keys[4]);
    CHECK_EQUAL(t.find_first_timestamp(col_nullable, Timestamp(-1, 0)), keys[5]);

    CHECK_EQUAL(t.find_first_timestamp(col_non_nullable, Timestamp(0, 0)), keys[0]);
    CHECK_EQUAL(t.find_first_timestamp(col_non_nullable, Timestamp(1, 0)), keys[2]);
    CHECK_EQUAL(t.find_first_timestamp(col_non_nullable, Timestamp(0, 1)), keys[3]);
    CHECK_EQUAL(t.find_first_timestamp(col_non_nullable, Timestamp(1, 1)), keys[4]);
    CHECK_EQUAL(t.find_first_timestamp(col_non_nullable, Timestamp(-1, 0)), keys[5]);
}


TEST(TimestampColumn_AddColumnAfterRows)
{
    constexpr bool nullable = true;
    constexpr bool non_nullable = false;

    Table t;
    auto col_0 = t.add_column(type_Int, "1", non_nullable);
    std::vector<ObjKey> keys;
    t.create_objects(REALM_MAX_BPNODE_SIZE * 2 + 1, keys);
    t.get_object(keys[0]).set<Int>(col_0, 100);

    auto col_1 = t.add_column(type_Timestamp, "2", non_nullable);
    auto col_2 = t.add_column(type_Timestamp, "3", nullable);
    CHECK_EQUAL(t.get_object(keys[0]).get<Timestamp>(col_1).get_seconds(), 0);
    CHECK_EQUAL(t.get_object(keys[0]).get<Timestamp>(col_1).get_nanoseconds(), 0);
    CHECK(t.get_object(keys[0]).get<Timestamp>(col_2).is_null());
    CHECK(t.get_object(keys[0]).is_null(col_2));
}

// max/min on pure null timestamps must return npos like for int, float and double
TEST(TimestampColumn_AggregateBug)
{
    ObjKey index;
    Table t;
    TableView tv;
    Timestamp ts;

    auto col = t.add_column(type_Timestamp, "ts", true);
    std::vector<ObjKey> keys;
    t.create_objects(4, keys);
    tv = t.where().find_all();
    CHECK_EQUAL(4, tv.size());
    ts = tv.maximum_timestamp(col, &index);
    CHECK_EQUAL(null_key, index);
    ts = tv.minimum_timestamp(col, &index);
    CHECK_EQUAL(null_key, index);

    Query q;

    ts = t.where().maximum_timestamp(col, &index);
    CHECK_EQUAL(null_key, index);

    ts = t.where().minimum_timestamp(col, &index);
    CHECK_EQUAL(null_key, index);

    t.get_object(keys[2]).set(col, Timestamp(1, 0));

    ts = t.where().maximum_timestamp(col, &index);
    CHECK_EQUAL(keys[2], index);
    CHECK_EQUAL(ts, Timestamp(1, 0));

    ts = t.where().minimum_timestamp(col, &index);
    CHECK_EQUAL(keys[2], index);
    CHECK_EQUAL(ts, Timestamp(1, 0));

    t.get_object(keys[3]).set(col, Timestamp(1, 1));

    ts = t.where().maximum_timestamp(col, &index);
    CHECK_EQUAL(keys[3], index);
    CHECK_EQUAL(ts, Timestamp(1, 1));

    ts = t.where().minimum_timestamp(col, &index);
    CHECK_EQUAL(keys[2], index);
    CHECK_EQUAL(ts, Timestamp(1, 0));
}


namespace {
// Since C++11, modulo with negative operands is well-defined

// "Reference implementations" for conversions to and from milliseconds
Timestamp milliseconds_to_timestamp(int64_t milliseconds)
{
    int64_t seconds = milliseconds / 1000;
    int32_t nanoseconds = (milliseconds % 1000) * 1000000;
    return Timestamp(seconds, nanoseconds);
}

int64_t timestamp_to_milliseconds(const Timestamp& ts)
{
    const int64_t seconds = ts.get_seconds();
    const int32_t nanoseconds = ts.get_nanoseconds();
    const int64_t milliseconds = seconds * 1000 + nanoseconds / 1000000; // This may overflow
    return milliseconds;
}

} // unnamed namespace


TEST(Timestamp_Conversions)
{
    BPlusTree<Timestamp> c(Allocator::get_default());
    c.create();

    constexpr int64_t millis[] = {1, 0, -1, 1000, -1000, 1001, -1001, 203558400, 1461746402, -1000000000};
    constexpr size_t num_millis = sizeof(millis) / sizeof(millis[0]);

    for (size_t i = 0; i < num_millis; ++i) {
        const int64_t milliseconds = millis[i];
        const Timestamp ts = milliseconds_to_timestamp(milliseconds);
        c.add(ts);
    }

    for (size_t i = 0; i < num_millis; ++i) {
        const Timestamp ts = c.get(i);
        const int64_t milliseconds = timestamp_to_milliseconds(ts);
        CHECK_EQUAL(milliseconds, millis[i]);
    }

    c.destroy();
}

TEST(Timestamp_ChronoConvertions)
{
    Timestamp t(1, 0);
    auto tp = t.get_time_point();
    CHECK_EQUAL(std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count(), 1000);
    Timestamp t2(tp + std::chrono::milliseconds(500));
    CHECK_EQUAL(t2, Timestamp(1, 500 * 1000 * 1000));

    auto now = std::chrono::system_clock::now();
    Timestamp t3(now);
    tp = t3.get_time_point();
    CHECK_EQUAL(tp, now);
}

#endif // TEST_COLUMN_TIMESTAMP
