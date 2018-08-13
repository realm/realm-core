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
#ifdef TEST_TABLE_VIEW

#include <limits>
#include <string>
#include <sstream>
#include <ostream>
#include <cwchar>

#include <realm.hpp>

#include "util/misc.hpp"

#include "test.hpp"
#include "test_table_helper.hpp"

using namespace realm;
using namespace test_util;

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

TEST(TableView_TimestampMaxMinCount)
{
    Table t;
    auto col = t.add_column(type_Timestamp, "ts", true);
    auto max_key = t.create_object().set_all(Timestamp(300, 300)).get_key();
    auto min_key = t.create_object().set_all(Timestamp(100, 100)).get_key();
    t.create_object().set_all(Timestamp(200, 200));

    // Add object with null. For max(), any non-null is greater, and for min() any non-null is less
    t.create_object();

    TableView tv = t.where().find_all();
    Timestamp ts;

    ts = tv.maximum_timestamp(col, nullptr);
    CHECK_EQUAL(ts, Timestamp(300, 300));
    ts = tv.minimum_timestamp(col, nullptr);
    CHECK_EQUAL(ts, Timestamp(100, 100));

    ObjKey key;
    ts = tv.maximum_timestamp(col, &key);
    CHECK_EQUAL(key, max_key);
    ts = tv.minimum_timestamp(col, &key);
    CHECK_EQUAL(key, min_key);

    size_t cnt;
    cnt = tv.count_timestamp(col, Timestamp(100, 100));
    CHECK_EQUAL(cnt, 1);

    cnt = tv.count_timestamp(col, Timestamp{});
    CHECK_EQUAL(cnt, 1);
}


TEST(TableView_FloatsFindAndAggregations)
{
    Table table;
    auto col_float = table.add_column(type_Float, "1");
    auto col_double = table.add_column(type_Double, "2");
    auto col_int = table.add_column(type_Int, "3");

    float f_val[] = {1.2f, 2.1f, 3.1f, -1.1f, 2.1f, 0.0f};
    double d_val[] = {-1.2, 2.2, 3.2, -1.2, 2.3, 0.0};
    // v_some =       ^^^^            ^^^^
    double sum_f = 0.0;
    double sum_d = 0.0;
    std::vector<ObjKey> keys;
    table.create_objects(6, keys);
    for (int i = 0; i < 6; ++i) {
        table.get_object(keys[i]).set_all(f_val[i], d_val[i], 1);
        sum_d += d_val[i];
        sum_f += f_val[i];
    }

    // Test find_all()
    TableView v_all = table.find_all_int(col_int, 1);
    CHECK_EQUAL(6, v_all.size());

    TableView v_some = table.find_all_double(col_double, -1.2);
    CHECK_EQUAL(2, v_some.size());
    CHECK_EQUAL(ObjKey(0), v_some.get_key(0));
    CHECK_EQUAL(ObjKey(3), v_some.get_key(1));

    // Test find_first
    CHECK_EQUAL(keys[0], v_all.find_first<Double>(col_double, -1.2));
    CHECK_EQUAL(keys[5], v_all.find_first<Double>(col_double, 0.0));
    CHECK_EQUAL(keys[2], v_all.find_first<Double>(col_double, 3.2));

    CHECK_EQUAL(keys[1], v_all.find_first<float>(col_float, 2.1f));
    CHECK_EQUAL(keys[5], v_all.find_first<float>(col_float, 0.0f));
    CHECK_EQUAL(keys[2], v_all.find_first<float>(col_float, 3.1f));

    // TODO: add for float as well

    double epsilon = std::numeric_limits<double>::epsilon();

    // Test sum
    CHECK_APPROXIMATELY_EQUAL(sum_d, v_all.sum_double(col_double), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum_f, v_all.sum_float(col_float), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(-1.2 + -1.2, v_some.sum_double(col_double), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(double(1.2f) + double(-1.1f), v_some.sum_float(col_float), 10 * epsilon);

    ObjKey key;

    // Test max
    CHECK_EQUAL(3.2, v_all.maximum_double(col_double, &key));
    CHECK_EQUAL(ObjKey(2), key);

    CHECK_EQUAL(-1.2, v_some.maximum_double(col_double, &key));
    CHECK_EQUAL(ObjKey(0), key);

    CHECK_EQUAL(3.1f, v_all.maximum_float(col_float, &key));
    CHECK_EQUAL(ObjKey(2), key);

    CHECK_EQUAL(1.2f, v_some.maximum_float(col_float, &key));
    CHECK_EQUAL(ObjKey(0), key);

    // Max without ret_index
    CHECK_EQUAL(3.2, v_all.maximum_double(col_double));
    CHECK_EQUAL(-1.2, v_some.maximum_double(col_double));
    CHECK_EQUAL(3.1f, v_all.maximum_float(col_float));
    CHECK_EQUAL(1.2f, v_some.maximum_float(col_float));

    // Test min
    CHECK_EQUAL(-1.2, v_all.minimum_double(col_double));
    CHECK_EQUAL(-1.2, v_some.minimum_double(col_double));
    CHECK_EQUAL(-1.1f, v_all.minimum_float(col_float));
    CHECK_EQUAL(-1.1f, v_some.minimum_float(col_float));
    // min with ret_ndx
    CHECK_EQUAL(-1.2, v_all.minimum_double(col_double, &key));
    CHECK_EQUAL(ObjKey(0), key);

    CHECK_EQUAL(-1.2, v_some.minimum_double(col_double, &key));
    CHECK_EQUAL(ObjKey(0), key);

    CHECK_EQUAL(-1.1f, v_all.minimum_float(col_float, &key));
    CHECK_EQUAL(ObjKey(3), key);

    CHECK_EQUAL(-1.1f, v_some.minimum_float(col_float, &key));
    CHECK_EQUAL(ObjKey(3), key);

    // Test avg
    CHECK_APPROXIMATELY_EQUAL(sum_d / 6.0, v_all.average_double(col_double), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL((-1.2 + -1.2) / 2.0, v_some.average_double(col_double), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum_f / 6.0, v_all.average_float(col_float), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL((double(1.2f) + double(-1.1f)) / 2, v_some.average_float(col_float), 10 * epsilon);

    CHECK_EQUAL(1, v_some.count_float(col_float, 1.2f));
    CHECK_EQUAL(2, v_some.count_double(col_double, -1.2));
    CHECK_EQUAL(2, v_some.count_int(col_int, 1));

    CHECK_EQUAL(2, v_all.count_float(col_float, 2.1f));
    CHECK_EQUAL(2, v_all.count_double(col_double, -1.2));
    CHECK_EQUAL(6, v_all.count_int(col_int, 1));
}

TEST(TableView_Sum)
{
    Table table;
    auto c0 = table.add_column(type_Int, "1");

    table.create_object().set_all( 2);
    table.create_object().set_all( 2);
    table.create_object().set_all( 2);
    table.create_object().set_all( 2);
    table.create_object().set_all( 2);

    TableView v = table.find_all_int(c0, 2);
    CHECK_EQUAL(5, v.size());

    int64_t sum = v.sum_int(c0);
    CHECK_EQUAL(10, sum);
}

TEST(TableView_Average)
{
    Table table;
    auto c0 = table.add_column(type_Int, "1");

    table.create_object().set_all( 2);
    table.create_object().set_all( 2);
    table.create_object().set_all( 2);
    table.create_object().set_all( 2);
    table.create_object().set_all( 2);

    TableView v = table.find_all_int(c0, 2);
    CHECK_EQUAL(5, v.size());

    double sum = v.average_int(c0);
    CHECK_APPROXIMATELY_EQUAL(2., sum, 0.00001);
}

TEST(TableView_SumNegative)
{
    Table table;
    auto c0 = table.add_column(type_Int, "1");

    table.create_object().set_all( 0);
    table.create_object().set_all( 0);
    table.create_object().set_all( 0);

    TableView v = table.find_all_int(c0, 0);
    v[0].set<Int>(c0, 11);
    v[2].set<Int>(c0, -20);

    int64_t sum = v.sum_int(c0);
    CHECK_EQUAL(-9, sum);
}

TEST(TableView_IsAttached)
{
    Table table;
    auto c0 = table.add_column(type_Int, "1");

    table.create_object().set_all( 0);
    table.create_object().set_all( 0);
    table.create_object().set_all( 0);

    TableView v = table.find_all_int(c0, 0);
    TableView v2 = table.find_all_int(c0, 0);
    v[0].set<Int>(c0, 11);
    CHECK_EQUAL(true, v.is_attached());
    CHECK_EQUAL(true, v2.is_attached());
    v.remove_last();
    CHECK_EQUAL(true, v.is_attached());
    CHECK_EQUAL(true, v2.is_attached());
}

TEST(TableView_Max)
{
    Table table;
    auto c0 = table.add_column(type_Int, "1");

    table.create_object().set_all( 0);
    table.create_object().set_all( 0);
    table.create_object().set_all( 0);

    TableView v = table.find_all_int(c0, 0);
    v[0].set<Int>(c0, -1);
    v[1].set<Int>(c0, 2);
    v[2].set<Int>(c0, 1);

    int64_t max = v.maximum_int(c0);
    CHECK_EQUAL(2, max);
}

TEST(TableView_Max2)
{
    Table table;
    auto c0 = table.add_column(type_Int, "1");

    table.create_object().set_all( 0);
    table.create_object().set_all( 0);
    table.create_object().set_all( 0);

    TableView v = table.find_all_int(c0, 0);
    v[0].set<Int>(c0, -1);
    v[1].set<Int>(c0, -2);
    v[2].set<Int>(c0, -3);

    int64_t max = v.maximum_int(c0, 0);
    CHECK_EQUAL(-1, max);
}


TEST(TableView_Min)
{
    Table table;
    auto c0 = table.add_column(type_Int, "first");

    table.create_object().set_all( 0);
    table.create_object().set_all( 0);
    table.create_object().set_all( 0);

    TableView v = table.find_all_int(c0, 0);
    v[0].set<Int>(c0, -1);
    v[1].set<Int>(c0, 2);
    v[2].set<Int>(c0, 1);

    int64_t min = v.minimum_int(c0);
    CHECK_EQUAL(-1, min);

    ObjKey key;
    min = v.minimum_int(c0, &key);
    CHECK_EQUAL(-1, min);
    CHECK_EQUAL(v[0].get_key(), key);
}

TEST(TableView_Min2)
{
    Table table;
    auto c0 = table.add_column(type_Int, "first");

    table.create_object().set_all( 0);
    table.create_object().set_all( 0);
    table.create_object().set_all( 0);

    TableView v = table.find_all_int(c0, 0);
    v[0].set<Int>(c0, -1);
    v[1].set<Int>(c0, -2);
    v[2].set<Int>(c0, -3);

    int64_t min = v.minimum_int(c0);
    CHECK_EQUAL(-3, min);

    ObjKey key;
    min = v.minimum_int(c0 ,&key);
    CHECK_EQUAL(-3, min);
    CHECK_EQUAL(v[2].get_key(), key);
}


#ifdef LEGACY_TESTS
TEST(TableView_Find)
{
    Table table;
    table.add_column(type_Int, "int");
    table.add_column(type_Int, "int?", true);
    table.add_column(type_Bool, "bool");
    table.add_column(type_Bool, "bool?", true);
    table.add_column(type_Float, "float");
    table.add_column(type_Float, "float?", true);
    table.add_column(type_Double, "double");
    table.add_column(type_Double, "double?", true);
    table.add_column(type_Timestamp, "timestamp");
    table.add_column(type_Timestamp, "timestamp?", true);
    table.add_column(type_String, "string");
    table.add_column(type_String, "string?", true);
    table.add_column(type_Binary, "binary");
    table.add_column(type_Binary, "binary?", true);

    table.create_object().set_all( 0);
    table.create_object().set_all( 1, 1, false, false, 1.1f, 1.1f, 1.1, 1.1, Timestamp(1, 1), Timestamp(1, 1),
        "a", "a", BinaryData("a", 1), BinaryData("a", 1));
    table.create_object().set_all( 2, nullptr, true, nullptr, 2.2f, nullptr, 2.2, nullptr, Timestamp(2, 2), nullptr,
        "b", nullptr, BinaryData("b", 1), nullptr);
    table.create_object().set_all( -1);

    // TV where index in TV equals the index in the table
    TableView all = table.where().find_all();
    // TV where index in TV is offset by one from the index in the table
    TableView after_first = table.where().find_all(1);

    // Ensure the TVs have a detached ref to deal with
    table.remove(3);

    // Look for the values in the second row
    CHECK_EQUAL(1, all.find_first_int(0, 1));
    CHECK_EQUAL(1, all.find_first(1, util::Optional<int64_t>(1)));
    CHECK_EQUAL(0, all.find_first(2, false));
    CHECK_EQUAL(1, all.find_first(3, util::make_optional(false)));
    CHECK_EQUAL(1, all.find_first(4, 1.1f));
    CHECK_EQUAL(1, all.find_first(5, util::make_optional(1.1f)));
    CHECK_EQUAL(1, all.find_first(6, 1.1));
    CHECK_EQUAL(1, all.find_first(7, util::make_optional(1.1)));
    CHECK_EQUAL(1, all.find_first(8, Timestamp(1, 1)));
    CHECK_EQUAL(1, all.find_first(9, Timestamp(1, 1)));
    CHECK_EQUAL(1, all.find_first(10, StringData("a")));
    CHECK_EQUAL(1, all.find_first(11, StringData("a")));
    CHECK_EQUAL(1, all.find_first(12, BinaryData("a", 1)));
    CHECK_EQUAL(1, all.find_first(13, BinaryData("a", 1)));

    CHECK_EQUAL(0, after_first.find_first_int(0, 1));
    CHECK_EQUAL(0, after_first.find_first(1, util::Optional<int64_t>(1)));
    CHECK_EQUAL(0, after_first.find_first(2, false));
    CHECK_EQUAL(0, after_first.find_first(3, util::make_optional(false)));
    CHECK_EQUAL(0, after_first.find_first(4, 1.1f));
    CHECK_EQUAL(0, after_first.find_first(5, util::make_optional(1.1f)));
    CHECK_EQUAL(0, after_first.find_first(6, 1.1));
    CHECK_EQUAL(0, after_first.find_first(7, util::make_optional(1.1)));
    CHECK_EQUAL(0, after_first.find_first(8, Timestamp(1, 1)));
    CHECK_EQUAL(0, after_first.find_first(9, Timestamp(1, 1)));
    CHECK_EQUAL(0, after_first.find_first(10, StringData("a")));
    CHECK_EQUAL(0, after_first.find_first(11, StringData("a")));
    CHECK_EQUAL(0, after_first.find_first(12, BinaryData("a", 1)));
    CHECK_EQUAL(0, after_first.find_first(13, BinaryData("a", 1)));

    // Look for the values in the third row
    CHECK_EQUAL(2, all.find_first_int(0, 2));
    CHECK_EQUAL(0, all.find_first(1, util::Optional<int64_t>()));
    CHECK_EQUAL(2, all.find_first(2, true));
    CHECK_EQUAL(0, all.find_first(3, util::Optional<bool>()));
    CHECK_EQUAL(2, all.find_first(4, 2.2f));
    CHECK_EQUAL(0, all.find_first(5, util::Optional<float>()));
    CHECK_EQUAL(2, all.find_first(6, 2.2));
    CHECK_EQUAL(0, all.find_first(7, util::Optional<double>()));
    CHECK_EQUAL(2, all.find_first(8, Timestamp(2, 2)));
    CHECK_EQUAL(0, all.find_first(9, Timestamp()));
    CHECK_EQUAL(2, all.find_first(10, StringData("b")));
    CHECK_EQUAL(0, all.find_first(11, StringData()));
    CHECK_EQUAL(2, all.find_first(12, BinaryData("b", 1)));
    CHECK_EQUAL(0, all.find_first(13, BinaryData()));

    CHECK_EQUAL(1, after_first.find_first_int(0, 2));
    CHECK_EQUAL(1, after_first.find_first(1, util::Optional<int64_t>()));
    CHECK_EQUAL(1, after_first.find_first(2, true));
    CHECK_EQUAL(1, after_first.find_first(3, util::Optional<bool>()));
    CHECK_EQUAL(1, after_first.find_first(4, 2.2f));
    CHECK_EQUAL(1, after_first.find_first(5, util::Optional<float>()));
    CHECK_EQUAL(1, after_first.find_first(6, 2.2));
    CHECK_EQUAL(1, after_first.find_first(7, util::Optional<double>()));
    CHECK_EQUAL(1, after_first.find_first(8, Timestamp(2, 2)));
    CHECK_EQUAL(1, after_first.find_first(9, Timestamp()));
    CHECK_EQUAL(1, after_first.find_first(10, StringData("b")));
    CHECK_EQUAL(1, after_first.find_first(11, StringData()));
    CHECK_EQUAL(1, after_first.find_first(12, BinaryData("b", 1)));
    CHECK_EQUAL(1, after_first.find_first(13, BinaryData()));

    // Look for values that aren't present
    CHECK_EQUAL(npos, all.find_first_int(0, 5));
    CHECK_EQUAL(npos, all.find_first(1, util::Optional<int64_t>(5)));
    CHECK_EQUAL(npos, all.find_first(4, 3.3f));
    CHECK_EQUAL(npos, all.find_first(5, util::make_optional(3.3f)));
    CHECK_EQUAL(npos, all.find_first(6, 3.3));
    CHECK_EQUAL(npos, all.find_first(7, util::make_optional(3.3)));
    CHECK_EQUAL(npos, all.find_first(8, Timestamp(3, 3)));
    CHECK_EQUAL(npos, all.find_first(9, Timestamp(3, 3)));
    CHECK_EQUAL(npos, all.find_first(10, StringData("c")));
    CHECK_EQUAL(npos, all.find_first(11, StringData("c")));
    CHECK_EQUAL(npos, all.find_first(12, BinaryData("c", 1)));
    CHECK_EQUAL(npos, all.find_first(13, BinaryData("c", 1)));

    CHECK_EQUAL(npos, after_first.find_first_int(0, 5));
    CHECK_EQUAL(npos, after_first.find_first(1, util::Optional<int64_t>(5)));
    CHECK_EQUAL(npos, after_first.find_first(4, 3.3f));
    CHECK_EQUAL(npos, after_first.find_first(5, util::make_optional(3.3f)));
    CHECK_EQUAL(npos, after_first.find_first(6, 3.3));
    CHECK_EQUAL(npos, after_first.find_first(7, util::make_optional(3.3)));
    CHECK_EQUAL(npos, after_first.find_first(8, Timestamp(3, 3)));
    CHECK_EQUAL(npos, after_first.find_first(9, Timestamp(3, 3)));
    CHECK_EQUAL(npos, after_first.find_first(10, StringData("c")));
    CHECK_EQUAL(npos, after_first.find_first(11, StringData("c")));
    CHECK_EQUAL(npos, after_first.find_first(12, BinaryData("c", 1)));
    CHECK_EQUAL(npos, after_first.find_first(13, BinaryData("c", 1)));
}


TEST(TableView_Follows_Changes)
{
    Table table;
    table.add_column(type_Int, "first");
    table.add_empty_row();
    table.set_int(0, 0, 1);
    Query q = table.where().equal(0, 1);
    TableView v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_int(0, 0));

    // low level sanity check that we can copy a query and run the copy:
    Query q2 = q;
    TableView v2 = q2.find_all();

    // now the fun begins
    CHECK_EQUAL(1, v.size());
    table.add_empty_row();
    CHECK_EQUAL(1, v.size());
    table.set_int(0, 1, 1);
    v.sync_if_needed();
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(1, v.get_int(0, 0));
    CHECK_EQUAL(1, v.get_int(0, 1));
    table.set_int(0, 0, 7);
    v.sync_if_needed();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_int(0, 0));
    table.set_int(0, 1, 7);
    v.sync_if_needed();
    CHECK_EQUAL(0, v.size());
    table.set_int(0, 1, 1);
    v.sync_if_needed();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_int(0, 0));
}


TEST(TableView_Distinct_Follows_Changes)
{
    Table table;
    table.add_column(type_Int, "first");
    table.add_column(type_String, "second");
    table.add_search_index(0);

    table.add_empty_row(5);
    for (int i = 0; i < 5; ++i) {
        table.set_int(0, i, i);
        table.set_string(1, i, "Foo");
    }

    TableView distinct_ints = table.get_distinct_view(0);
    CHECK_EQUAL(5, distinct_ints.size());
    CHECK(distinct_ints.is_in_sync());

    // Check that adding a value that doesn't actually impact the
    // view still invalidates the view (which is inspected for now).
    table.add_empty_row();
    table.set_int(0, 5, 4);
    table.set_string(1, 5, "Foo");
    CHECK(!distinct_ints.is_in_sync());
    distinct_ints.sync_if_needed();
    CHECK(distinct_ints.is_in_sync());
    CHECK_EQUAL(5, distinct_ints.size());

    // Check that adding a value that impacts the view invalidates the view.
    distinct_ints.sync_if_needed();
    table.add_empty_row();
    table.set_int(0, 6, 10);
    table.set_string(1, 6, "Foo");
    CHECK(!distinct_ints.is_in_sync());
    distinct_ints.sync_if_needed();
    CHECK(distinct_ints.is_in_sync());
    CHECK_EQUAL(6, distinct_ints.size());
}


TEST(TableView_SyncAfterCopy)
{
    Table table;
    table.add_column(type_Int, "first");
    table.add_empty_row();
    table.set_int(0, 0, 1);

    // do initial query
    Query q = table.where().equal(0, 1);
    TableView v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_int(0, 0));

    // move the tableview
    TableView v2 = v;
    CHECK_EQUAL(1, v2.size());

    // make a change
    size_t ndx2 = table.add_empty_row();
    table.set_int(0, ndx2, 1);

    // verify that the copied view sees the change
    v2.sync_if_needed();
    CHECK_EQUAL(2, v2.size());
}

TEST(TableView_FindAll)
{
    Table table;
    table.add_column(type_Int, "first");

    table.create_object().set_all( 0);
    table.create_object().set_all( 0);
    table.create_object().set_all( 0);

    TableView v = table.find_all_int(0, 0);
    CHECK_EQUAL(3, v.size());
    v[0].set_int(0, 5);
    v[1].set_int(0, 4); // match
    v[2].set_int(0, 4); // match

    // todo, add creation to wrapper function in table.h
    TableView v2 = v.find_all_int(0, 4);
    CHECK_EQUAL(2, v2.size());
    CHECK_EQUAL(1, v2.get_source_ndx(0));
    CHECK_EQUAL(2, v2.get_source_ndx(1));
}


TEST(TableView_FindAllString)
{
    Table table;
    table.add_column(type_String, "1");

    table.create_object().set_all( "a");
    table.create_object().set_all( "a");
    table.create_object().set_all( "a");

    TableView v = table.find_all_string(0, "a");
    v[0].set_string(0, "foo");
    v[1].set_string(0, "bar"); // match
    v[2].set_string(0, "bar"); // match

    // todo, add creation to wrapper function in table.h
    TableView v2 = v.find_all_string(0, "bar");
    CHECK_EQUAL(1, v2.get_source_ndx(0));
    CHECK_EQUAL(2, v2.get_source_ndx(1));
}


NONCONCURRENT_TEST(TableView_StringSort)
{
    // WARNING: Do not use the C++11 method (set_string_compare_method(1)) on Windows 8.1 because it has a bug that
    // takes length in count when sorting ("b" comes before "aaaa"). Bug is not present in Windows 7.

    // Test of handling of unicode takes place in test_utf8.cpp
    Table table;
    table.add_column(type_String, "1");

    table.create_object().set_all( "alpha");
    table.create_object().set_all( "zebra");
    table.create_object().set_all( "ALPHA");
    table.create_object().set_all( "ZEBRA");

    // Core-only is default comparer
    TableView v = table.where().find_all();
    v.sort(0);
    CHECK_EQUAL("alpha", v[0].get_string(0));
    CHECK_EQUAL("ALPHA", v[1].get_string(0));
    CHECK_EQUAL("zebra", v[2].get_string(0));
    CHECK_EQUAL("ZEBRA", v[3].get_string(0));

    // Should be exactly the same as above because 0 was default already
    set_string_compare_method(STRING_COMPARE_CORE, nullptr);
    v = table.where().find_all();
    v.sort(0);
    CHECK_EQUAL("alpha", v[0].get_string(0));
    CHECK_EQUAL("ALPHA", v[1].get_string(0));
    CHECK_EQUAL("zebra", v[2].get_string(0));
    CHECK_EQUAL("ZEBRA", v[3].get_string(0));

    // Test descending mode
    v = table.where().find_all();
    v.sort(0, false);
    CHECK_EQUAL("alpha", v[3].get_string(0));
    CHECK_EQUAL("ALPHA", v[2].get_string(0));
    CHECK_EQUAL("zebra", v[1].get_string(0));
    CHECK_EQUAL("ZEBRA", v[0].get_string(0));

    // primitive C locale comparer. But that's OK since all we want to test is
    // if the callback is invoked
    bool got_called = false;
    auto comparer = [&](const char* s1, const char* s2) {
        got_called = true;
        return *s1 < *s2;
    };

    // Test if callback comparer works. Our callback is a primitive dummy-comparer
    set_string_compare_method(STRING_COMPARE_CALLBACK, comparer);
    v = table.where().find_all();
    v.sort(0);
    CHECK_EQUAL("ALPHA", v[0].get_string(0));
    CHECK_EQUAL("ZEBRA", v[1].get_string(0));
    CHECK_EQUAL("alpha", v[2].get_string(0));
    CHECK_EQUAL("zebra", v[3].get_string(0));
    CHECK_EQUAL(true, got_called);

#ifdef _MSC_VER
    // Try C++11 method which uses current locale of the operating system to give precise sorting. This C++11 feature
    // is currently (mid 2014) only supported by Visual Studio
    got_called = false;
    bool available = set_string_compare_method(STRING_COMPARE_CPP11, nullptr);
    if (available) {
        v = table.where().find_all();
        v.sort(0);
        CHECK_EQUAL("alpha", v[0].get_string(0));
        CHECK_EQUAL("ALPHA", v[1].get_string(0));
        CHECK_EQUAL("zebra", v[2].get_string(0));
        CHECK_EQUAL("ZEBRA", v[3].get_string(0));
        CHECK_EQUAL(false, got_called);
    }
#endif

    // Set back to default for use by other unit tests
    set_string_compare_method(STRING_COMPARE_CORE, nullptr);
}


TEST(TableView_FloatDoubleSort)
{
    Table t;
    t.add_column(type_Float, "1");
    t.add_column(type_Double, "2");

    add(t, 1.0f, 10.0);
    add(t, 3.0f, 30.0);
    add(t, 2.0f, 20.0);
    add(t, 0.0f, 5.0);

    TableView tv = t.where().find_all();
    tv.sort(0);

    CHECK_EQUAL(0.0f, tv[0].get_float(0));
    CHECK_EQUAL(1.0f, tv[1].get_float(0));
    CHECK_EQUAL(2.0f, tv[2].get_float(0));
    CHECK_EQUAL(3.0f, tv[3].get_float(0));

    tv.sort(1);
    CHECK_EQUAL(5.0f, tv[0].get_double(1));
    CHECK_EQUAL(10.0f, tv[1].get_double(1));
    CHECK_EQUAL(20.0f, tv[2].get_double(1));
    CHECK_EQUAL(30.0f, tv[3].get_double(1));
}

TEST(TableView_DoubleSortPrecision)
{
    // Detect if sorting algorithm accidentially casts doubles to float somewhere so that precision gets lost
    Table t;
    t.add_column(type_Float, "1");
    t.add_column(type_Double, "2");

    double d1 = 100000000000.0;
    double d2 = 100000000001.0;

    // When casted to float, they are equal
    float f1 = static_cast<float>(d1);
    float f2 = static_cast<float>(d2);

    // If this check fails, it's a bug in this unit test, not in Realm
    CHECK_EQUAL(f1, f2);

    // First verify that our unit is guaranteed to find such a bug; that is, test if such a cast is guaranteed to give
    // bad sorting order. This is not granted, because an unstable sorting algorithm could *by chance* give the
    // correct sorting order. Fortunatly we use std::stable_sort which must maintain order on draws.
    add(t, f2, d2);
    add(t, f1, d1);

    TableView tv = t.where().find_all();
    tv.sort(0);

    // Sort should be stable
    CHECK_EQUAL(f2, tv[0].get_float(0));
    CHECK_EQUAL(f1, tv[1].get_float(0));

    // If sort is stable, and compare makes a draw because the doubles are accidentially casted to float in Realm,
    // then
    // original order would be maintained. Check that it's not maintained:
    tv.sort(1);
    CHECK_EQUAL(d1, tv[0].get_double(1));
    CHECK_EQUAL(d2, tv[1].get_double(1));
}

TEST(TableView_SortNullString)
{
    Table t;
    t.add_column(type_String, "s", true);
    t.add_empty_row(4);
    t.set_string(0, 0, StringData("")); // empty string
    t.set_string(0, 1, realm::null());  // realm::null()
    t.set_string(0, 2, StringData("")); // empty string
    t.set_string(0, 3, realm::null());  // realm::null()

    TableView tv;

    tv = t.where().find_all();
    tv.sort(0);
    CHECK(tv.get_string(0, 0).is_null());
    CHECK(tv.get_string(0, 1).is_null());
    CHECK(!tv.get_string(0, 2).is_null());
    CHECK(!tv.get_string(0, 3).is_null());

    t.set_string(0, 0, StringData("medium medium medium medium"));

    tv = t.where().find_all();
    tv.sort(0);
    CHECK(tv.get_string(0, 0).is_null());
    CHECK(tv.get_string(0, 1).is_null());
    CHECK(!tv.get_string(0, 2).is_null());
    CHECK(!tv.get_string(0, 3).is_null());

    t.set_string(0, 0, StringData("long long long long long long long long long long long long long long"));

    tv = t.where().find_all();
    tv.sort(0);
    CHECK(tv.get_string(0, 0).is_null());
    CHECK(tv.get_string(0, 1).is_null());
    CHECK(!tv.get_string(0, 2).is_null());
    CHECK(!tv.get_string(0, 3).is_null());
}

TEST(TableView_Delete)
{
    Table table;
    table.add_column(type_Int, "first");

    table.create_object().set_all( 1);
    table.create_object().set_all( 2);
    table.create_object().set_all( 1);
    table.create_object().set_all( 3);
    table.create_object().set_all( 1);

    TableView v = table.find_all_int(0, 1);
    CHECK_EQUAL(3, v.size());

    v.remove(1);
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(0, v.get_source_ndx(0));
    CHECK_EQUAL(3, v.get_source_ndx(1));

    CHECK_EQUAL(4, table.size());
    CHECK_EQUAL(1, table[0].get_int(0));
    CHECK_EQUAL(2, table[1].get_int(0));
    CHECK_EQUAL(3, table[2].get_int(0));
    CHECK_EQUAL(1, table[3].get_int(0));

    v.remove(0);
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(2, v.get_source_ndx(0));

    CHECK_EQUAL(3, table.size());
    CHECK_EQUAL(2, table[0].get_int(0));
    CHECK_EQUAL(3, table[1].get_int(0));
    CHECK_EQUAL(1, table[2].get_int(0));

    v.remove(0);
    CHECK_EQUAL(0, v.size());

    CHECK_EQUAL(2, table.size());
    CHECK_EQUAL(2, table[0].get_int(0));
    CHECK_EQUAL(3, table[1].get_int(0));
}

TEST(TableView_Clear)
{
    Table table;
    table.add_column(type_Int, "first");

    table.create_object().set_all( 1);
    table.create_object().set_all( 2);
    table.create_object().set_all( 1);
    table.create_object().set_all( 3);
    table.create_object().set_all( 1);

    TableView v = table.find_all_int(0, 1);
    CHECK_EQUAL(3, v.size());

    v.clear();
    CHECK_EQUAL(0, v.size());

    CHECK_EQUAL(2, table.size());
    CHECK_EQUAL(2, table[0].get_int(0));
    CHECK_EQUAL(3, table[1].get_int(0));
}


// Verify that TableView::clear() can handle a detached ref,
// so that it can be used in an imperative setting
TEST(TableView_Imperative_Clear)
{
    Table t;
    t.add_column(type_Int, "i1");
    t.add_empty_row(3);
    t.set_int(0, 0, 7);
    t.set_int(0, 1, 13);
    t.set_int(0, 2, 29);

    TableView v = t.where().less(0, 20).find_all();
    CHECK_EQUAL(2, v.size());
    // remove the underlying entry in the table, introducing a detached ref
    t.move_last_over(v.get_source_ndx(0));
    // the detached ref still counts as an entry when calling size()
    CHECK_EQUAL(2, v.size());
    // but is does not count as attached anymore:
    CHECK_EQUAL(1, v.num_attached_rows());
    v.clear();
    CHECK_EQUAL(0, v.size());
    CHECK_EQUAL(1, t.size());
}

// exposes a bug in stacked tableview:
// view V1 selects a subset of rows from Table T1
// View V2 selects rows from  view V1
// Then, some rows in V2 can be found, that are not in V1
TEST(TableView_Stacked)
{
    Table t;
    t.add_column(type_Int, "i1");
    t.add_column(type_Int, "i2");
    t.add_column(type_String, "S1");
    t.add_empty_row(2);
    t.set_int(0, 0, 1);      // 1
    t.set_int(1, 0, 2);      // 2
    t.set_string(2, 0, "A"); // "A"
    t.set_int(0, 1, 2);      // 2
    t.set_int(1, 1, 2);      // 2
    t.set_string(2, 1, "B"); // "B"

    TableView tv = t.find_all_int(0, 2);
    TableView tv2 = tv.find_all_int(1, 2);
    CHECK_EQUAL(1, tv2.size());             // evaluates tv2.size to 1 which is expected
    CHECK_EQUAL("B", tv2.get_string(2, 0)); // evalates get_string(2,0) to "A" which is not expected
}


TEST(TableView_ClearNone)
{
    Table table;
    table.add_column(type_Int, "first");

    TableView v = table.find_all_int(0, 1);
    CHECK_EQUAL(0, v.size());

    v.clear();
}

TEST(TableView_FindAllStacked)
{
    Table table;
    table.add_column(type_Int, "1");
    table.add_column(type_Int, "2");	

    table.create_object().set_all( 0, 1);
    table.create_object().set_all( 0, 2);
    table.create_object().set_all( 0, 3);
    table.create_object().set_all( 1, 1);
    table.create_object().set_all( 1, 2);
    table.create_object().set_all( 1, 3);

    TableView v = table.find_all_int(0, 0);
    CHECK_EQUAL(3, v.size());

    TableView v2 = v.find_all_int(1, 2);
    CHECK_EQUAL(1, v2.size());
    CHECK_EQUAL(0, v2[0].get_int(0));
    CHECK_EQUAL(2, v2[0].get_int(1));
    CHECK_EQUAL(1, v2.get_source_ndx(0));
}




TEST(TableView_DynPivot)
{
    TableRef table = Table::create();
    size_t column_ndx_sex = table->add_column(type_String, "sex");
    size_t column_ndx_age = table->add_column(type_Int, "age");
    table->add_column(type_Bool, "hired");

    size_t count = 5000;
    for (size_t i = 0; i < count; ++i) {
        StringData sex = i % 2 ? "Male" : "Female";
        table->insert_empty_row(i);
        table->set_string(0, i, sex);
        table->set_int(1, i, 20 + (i % 20));
        table->set_bool(2, i, true);
    }

    TableView tv = table->where().find_all();

    Table result_count;
    tv.aggregate(0, 1, Table::aggr_count, result_count);
    int64_t half = count / 2;
    CHECK_EQUAL(2, result_count.get_column_count());
    CHECK_EQUAL(2, result_count.size());
    CHECK_EQUAL(half, result_count.get_int(1, 0));
    CHECK_EQUAL(half, result_count.get_int(1, 1));

    Table result_sum;
    tv.aggregate(column_ndx_sex, column_ndx_age, Table::aggr_sum, result_sum);

    Table result_avg;
    tv.aggregate(column_ndx_sex, column_ndx_age, Table::aggr_avg, result_avg);

    Table result_min;
    tv.aggregate(column_ndx_sex, column_ndx_age, Table::aggr_min, result_min);

    Table result_max;
    tv.aggregate(column_ndx_sex, column_ndx_age, Table::aggr_max, result_max);


    // Test with enumerated strings
    table->optimize();

    Table result_count2;
    tv.aggregate(column_ndx_sex, column_ndx_age, Table::aggr_count, result_count2);
    CHECK_EQUAL(2, result_count2.get_column_count());
    CHECK_EQUAL(2, result_count2.size());
    CHECK_EQUAL(half, result_count2.get_int(1, 0));
    CHECK_EQUAL(half, result_count2.get_int(1, 1));
}


TEST(TableView_RowAccessor)
{
    Table table;
    table.add_column(type_Int, "");
    table.add_empty_row();
    table.set_int(0, 0, 703);
    TableView tv = table.where().find_all();
    Row row = tv[0];
    CHECK_EQUAL(703, row.get_int(0));
    ConstRow crow = tv[0];
    CHECK_EQUAL(703, crow.get_int(0));
    ConstTableView ctv = table.where().find_all();
    ConstRow crow_2 = ctv[0];
    CHECK_EQUAL(703, crow_2.get_int(0));
}

TEST(TableView_FindBySourceNdx)
{
    Table table;
    table.add_column(type_Int, "");
    table.add_empty_row();
    table.add_empty_row();
    table.add_empty_row();
    table[0].set_int(0, 0);
    table[1].set_int(0, 1);
    table[2].set_int(0, 2);
    TableView tv = table.where().find_all();
    tv.sort(0, false);
    CHECK_EQUAL(0, tv.find_by_source_ndx(2));
    CHECK_EQUAL(1, tv.find_by_source_ndx(1));
    CHECK_EQUAL(2, tv.find_by_source_ndx(0));
}

TEST(TableView_MultiColSort)
{
    Table table;
    table.add_column(type_Int, "");
    table.add_column(type_Float, "");
    table.add_empty_row();
    table.add_empty_row();
    table.add_empty_row();
    table[0].set_int(0, 0);
    table[1].set_int(0, 1);
    table[2].set_int(0, 1);

    table[0].set_float(1, 0.f);
    table[1].set_float(1, 2.f);
    table[2].set_float(1, 1.f);

    TableView tv = table.where().find_all();

    std::vector<std::vector<size_t>> v = {{0}, {1}};
    std::vector<bool> a = {true, true};

    tv.sort(SortDescriptor{table, v, a});

    CHECK_EQUAL(tv.get_float(1, 0), 0.f);
    CHECK_EQUAL(tv.get_float(1, 1), 1.f);
    CHECK_EQUAL(tv.get_float(1, 2), 2.f);

    std::vector<bool> a_descending = {false, false};
    tv = table.where().find_all();
    tv.sort(SortDescriptor{table, v, a_descending});

    CHECK_EQUAL(tv.get_float(1, 0), 2.f);
    CHECK_EQUAL(tv.get_float(1, 1), 1.f);
    CHECK_EQUAL(tv.get_float(1, 2), 0.f);

    std::vector<bool> a_ascdesc = {true, false};
    tv = table.where().find_all();
    tv.sort(SortDescriptor{table, v, a_ascdesc});

    CHECK_EQUAL(tv.get_float(1, 0), 0.f);
    CHECK_EQUAL(tv.get_float(1, 1), 2.f);
    CHECK_EQUAL(tv.get_float(1, 2), 1.f);
}

TEST(TableView_QueryCopy)
{
    Table table;
    table.add_column(type_Int, "");
    table.add_empty_row();
    table.add_empty_row();
    table.add_empty_row();
    table[0].set_int(0, 0);
    table[1].set_int(0, 1);
    table[2].set_int(0, 2);

    // Test if copy-assign of Query in TableView works
    TableView tv = table.where().find_all();

    Query q = table.where();

    q.group();
    q.equal(0, 1);
    q.Or();
    q.equal(0, 2);
    q.end_group();

    q.count();

    Query q2;
    q2 = table.where().equal(0, 1234);

    q2 = q;
    size_t t = q2.count();

    CHECK_EQUAL(t, 2);
}

TEST(TableView_SortEnum)
{
    Table table;
    table.add_column(type_String, "str");
    table.add_empty_row(3);
    table[0].set_string(0, "foo");
    table[1].set_string(0, "foo");
    table[2].set_string(0, "foo");

    table.optimize();

    table.add_empty_row(3);
    table[3].set_string(0, "bbb");
    table[4].set_string(0, "aaa");
    table[5].set_string(0, "baz");

    TableView tv = table.where().find_all();
    tv.sort(0);

    CHECK_EQUAL(tv[0].get_string(0), "aaa");
    CHECK_EQUAL(tv[1].get_string(0), "baz");
    CHECK_EQUAL(tv[2].get_string(0), "bbb");
    CHECK_EQUAL(tv[3].get_string(0), "foo");
    CHECK_EQUAL(tv[4].get_string(0), "foo");
    CHECK_EQUAL(tv[5].get_string(0), "foo");
}

TEST(TableView_Backlinks)
{
    Group group;

    TableRef source = group.add_table("source");
    source->add_column(type_Int, "int");

    TableRef links = group.add_table("links");
    links->add_column_link(type_Link, "link", *source);
    links->add_column_link(type_LinkList, "link_list", *source);

    source->add_empty_row(3);

    {
        // Links
        TableView tv = source->get_backlink_view(2, links.get(), 0);

        CHECK_EQUAL(tv.size(), 0);

        links->add_empty_row();
        links->set_link(0, 0, 2);

        tv.sync_if_needed();
        CHECK_EQUAL(tv.size(), 1);
        CHECK_EQUAL(tv[0].get_index(), links->get(0).get_index());
    }
    {
        // LinkViews
        TableView tv = source->get_backlink_view(2, links.get(), 1);

        CHECK_EQUAL(tv.size(), 0);

        auto ll = links->get_linklist(1, 0);
        ll->add(2);
        ll->add(0);
        ll->add(2);

        tv.sync_if_needed();
        CHECK_EQUAL(tv.size(), 2);
        CHECK_EQUAL(tv[0].get_index(), links->get(0).get_index());
    }
}

// Verify that a TableView that represents backlinks to a row functions correctly
// after being move-assigned.
TEST(TableView_BacklinksAfterMoveAssign)
{
    Group group;

    TableRef source = group.add_table("source");
    source->add_column(type_Int, "int");

    TableRef links = group.add_table("links");
    links->add_column_link(type_Link, "link", *source);
    links->add_column_link(type_LinkList, "link_list", *source);

    source->add_empty_row(3);

    {
        // Links
        TableView tv_source = source->get_backlink_view(2, links.get(), 0);
        TableView tv;
        tv = std::move(tv_source);

        CHECK_EQUAL(tv.size(), 0);

        links->add_empty_row();
        links->set_link(0, 0, 2);

        tv.sync_if_needed();
        CHECK_EQUAL(tv.size(), 1);
        CHECK_EQUAL(tv[0].get_index(), links->get(0).get_index());
    }
    {
        // LinkViews
        TableView tv_source = source->get_backlink_view(2, links.get(), 1);
        TableView tv;
        tv = std::move(tv_source);

        CHECK_EQUAL(tv.size(), 0);

        auto ll = links->get_linklist(1, 0);
        ll->add(2);
        ll->add(0);
        ll->add(2);

        tv.sync_if_needed();
        CHECK_EQUAL(tv.size(), 2);
        CHECK_EQUAL(tv[0].get_index(), links->get(0).get_index());
    }
}

#endif

namespace {
struct DistinctDirect {
    Table& table;
    DistinctDirect(TableRef, TableRef t, ColKey)
        : table(*t)
    {
    }

    SortDescriptor get_sort(std::initializer_list<ColKey> columns, std::vector<bool> ascending = {}) const
    {
        std::vector<std::vector<ColKey>> column_indices;
        for (ColKey col : columns)
            column_indices.push_back({col});
        return SortDescriptor(column_indices, ascending);
    }

    DistinctDescriptor get_distinct(std::initializer_list<ColKey> columns) const
    {
        std::vector<std::vector<ColKey>> column_indices;
        for (ColKey col : columns)
            column_indices.push_back({col});
        return DistinctDescriptor(column_indices);
    }

    ObjKey get_key(const TableView& tv, size_t ndx) const
    {
        return tv.get_key(ndx);
    }

    StringData get_string(const TableView& tv, ColKey col, size_t row) const
    {
        return tv.ConstTableView::get_object(row).get<String>(col);
    }

    TableView find_all() const
    {
        return table.where().find_all();
    }
};

struct DistinctOverLink {
    Table& table;
    ColKey m_col_link;
    DistinctOverLink(TableRef t, TableRef, ColKey col_link)
        : table(*t)
        , m_col_link(col_link)
    {
    }

    SortDescriptor get_sort(std::initializer_list<ColKey> columns, std::vector<bool> ascending = {}) const
    {
        std::vector<std::vector<ColKey>> column_indices;
        for (ColKey col : columns)
            column_indices.push_back({m_col_link, col});
        return SortDescriptor(column_indices, ascending);
    }

    DistinctDescriptor get_distinct(std::initializer_list<ColKey> columns) const
    {
        std::vector<std::vector<ColKey>> column_indices;
        for (ColKey col : columns)
            column_indices.push_back({m_col_link, col});
        return DistinctDescriptor(column_indices);
    }

    ObjKey get_key(const TableView& tv, size_t ndx) const
    {
        return tv.ConstTableView::get_object(ndx).get<ObjKey>(m_col_link);
    }

    StringData get_string(const TableView& tv, ColKey col, size_t ndx) const
    {
        return tv.ConstTableView::get_object(ndx).get_linked_object(m_col_link).get<String>(col);
    }

    TableView find_all() const
    {
        return table.where().find_all();
    }
};
} // anonymous namespace

TEST_TYPES(TableView_Distinct, DistinctDirect, DistinctOverLink)
{
    // distinct() will preserve the original order of the row pointers, also if the order is a result of sort()
    // If multiple rows are identical for the given set of distinct-columns, then only the first is kept.
    // You can call sync_if_needed() to update the distinct view, just like you can for a sorted view.
    // Each time you call distinct() it will compound on the previous call.
    // Results of distinct are affected by a previously applied sort order.

    // distinct() is internally based on the existing sort() method which is well tested. Hence it's not required
    // to test distinct() with all possible Realm data types.


    Group g;
    TableRef target = g.add_table("target");
    TableRef origin = g.add_table("origin");
    auto col_link = origin->add_column_link(type_Link, "link", *target);

    Table& t = *target;
    auto col_str = t.add_column(type_String, "s", true);
    auto col_int = t.add_column(type_Int, "i", true);
    t.add_column(type_Float, "f", true);

    ObjKey k0 = t.create_object().set_all(StringData(""), 100, 100.f).get_key();
    ObjKey k1 = t.create_object().set_all(StringData(), 200, 200.f).get_key();
    t.create_object().set_all(StringData(""), 100, 100.f).get_key();
    t.create_object().set_all(StringData(), 200, 200.f).get_key();
    ObjKey k4 = t.create_object().set_all(StringData("foo"), 300, 300.f).get_key();
    ObjKey k5 = t.create_object().set_all(StringData("foo"), 400, 400.f).get_key();
    ObjKey k6 = t.create_object().set_all(StringData("bar"), 500, 500.f).get_key();

    for (auto it : t) {
        origin->create_object().set(col_link, it.get_key());
    }

    TEST_TYPE h(origin, target, col_link);

    TableView tv;
    tv = h.find_all();
    tv.distinct(h.get_distinct({col_str}));
    CHECK_EQUAL(tv.size(), 4);
    CHECK_EQUAL(h.get_key(tv, 0), k0);
    CHECK_EQUAL(h.get_key(tv, 1), k1);
    CHECK_EQUAL(h.get_key(tv, 2), k4);
    CHECK_EQUAL(h.get_key(tv, 3), k6);

    tv = h.find_all();
    tv.distinct(h.get_distinct({col_str}));
    tv.sort(h.get_sort({col_str}));
    CHECK_EQUAL(tv.size(), 4);
    CHECK_EQUAL(h.get_key(tv, 0), k1);
    CHECK_EQUAL(h.get_key(tv, 1), k0);
    CHECK_EQUAL(h.get_key(tv, 2), k6);
    CHECK_EQUAL(h.get_key(tv, 3), k4);

    tv = h.find_all();
    tv.distinct(h.get_distinct({col_str}));
    tv.sort(h.get_sort({col_str}, {false}));
    CHECK_EQUAL(h.get_key(tv, 0), k4);
    CHECK_EQUAL(h.get_key(tv, 1), k6);
    CHECK_EQUAL(h.get_key(tv, 2), k0);
    CHECK_EQUAL(h.get_key(tv, 3), k1);

    // Note here that our stable sort will sort the two "foo"s like row {4, 5}
    tv = h.find_all();
    tv.distinct(h.get_distinct({col_str, col_int}));
    tv.sort(h.get_sort({col_str}, {false}));
    CHECK_EQUAL(tv.size(), 5);
    CHECK_EQUAL(h.get_key(tv, 0), k4);
    CHECK_EQUAL(h.get_key(tv, 1), k5);
    CHECK_EQUAL(h.get_key(tv, 2), k6);
    CHECK_EQUAL(h.get_key(tv, 3), k0);
    CHECK_EQUAL(h.get_key(tv, 4), k1);


    // Now try distinct on string+float column. The float column has the same values as the int column
    // so the result should equal the test above
    tv = h.find_all();
    tv.distinct(h.get_distinct({col_str, col_int}));
    tv.sort(h.get_sort({col_str}, {false}));
    CHECK_EQUAL(tv.size(), 5);
    CHECK_EQUAL(h.get_key(tv, 0), k4);
    CHECK_EQUAL(h.get_key(tv, 1), k5);
    CHECK_EQUAL(h.get_key(tv, 2), k6);
    CHECK_EQUAL(h.get_key(tv, 3), k0);
    CHECK_EQUAL(h.get_key(tv, 4), k1);


    // Same as previous test, but with string column being Enum
    t.enumerate_string_column(col_str);
    tv = h.find_all();
    tv.distinct(h.get_distinct({col_str, col_int}));
    tv.sort(h.get_sort({col_str}, {false}));
    CHECK_EQUAL(tv.size(), 5);
    CHECK_EQUAL(h.get_key(tv, 0), k4);
    CHECK_EQUAL(h.get_key(tv, 1), k5);
    CHECK_EQUAL(h.get_key(tv, 2), k6);
    CHECK_EQUAL(h.get_key(tv, 3), k0);
    CHECK_EQUAL(h.get_key(tv, 4), k1);


    // Now test sync_if_needed()
    tv = h.find_all();
    // "", null, "", null, "foo", "foo", "bar"

    tv.distinct(h.get_distinct({col_str}));
    tv.sort(h.get_sort({col_str}, {false}));
    // "foo", "bar", "", null

    CHECK_EQUAL(tv.size(), 4);
    CHECK_EQUAL(h.get_string(tv, col_str, 0), "foo");
    CHECK_EQUAL(h.get_string(tv, col_str, 1), "bar");
    CHECK_EQUAL(h.get_string(tv, col_str, 2), "");
    CHECK(h.get_string(tv, col_str, 3).is_null());

    // remove "bar"
    target->remove_object(k6);
    // access to tv undefined; may crash

    tv.sync_if_needed();
    // "foo", "", null

    CHECK_EQUAL(tv.size(), 3);
    CHECK_EQUAL(h.get_string(tv, col_str, 0), "foo");
    CHECK_EQUAL(h.get_string(tv, col_str, 1), "");
    CHECK(h.get_string(tv, col_str, 2).is_null());
}

TEST(TableView_DistinctOverNullLink)
{
    Group g;
    TableRef target = g.add_table("target");
    auto col_int = target->add_column(type_Int, "value");

    ObjKey k0 = target->create_object().set(col_int, 0).get_key();
    ObjKey k1 = target->create_object().set(col_int, 1).get_key();

    TableRef origin = g.add_table("origin");
    auto col_link = origin->add_column_link(type_Link, "link", *target);

    origin->create_object().set(col_link, k0);
    origin->create_object().set(col_link, k1);
    origin->create_object().set(col_link, k0);
    origin->create_object().set(col_link, k1);
    origin->create_object(); // link is null

    auto tv = origin->where().find_all();
    tv.distinct(DistinctDescriptor({{col_link, col_int}}));
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get(0).get_linked_object(col_link).get<Int>(col_int), 0);
    CHECK_EQUAL(tv.get(1).get_linked_object(col_link).get<Int>(col_int), 1);
}

TEST(TableView_IsRowAttachedAfterClear)
{
    Table t;
    auto col_id = t.add_column(type_Int, "id");

    t.create_object().set(col_id, 0);
    t.create_object().set(col_id, 1);

    TableView tv = t.where().find_all();
    CHECK_EQUAL(2, tv.size());
    CHECK(tv.is_obj_valid(0));
    CHECK(tv.is_obj_valid(1));

    t.get_object(1).remove();
    CHECK_EQUAL(2, tv.size());
    CHECK(tv.is_obj_valid(0));
    CHECK(!tv.is_obj_valid(1));

    t.clear();
    CHECK_EQUAL(2, tv.size());
    CHECK(!tv.is_obj_valid(0));
    CHECK(!tv.is_obj_valid(1));
}

TEST(TableView_IsInTableOrder)
{
    Group g;

    TableRef source = g.add_table("source");
    TableRef target = g.add_table("target");

    auto col_link = source->add_column_link(type_LinkList, "link", *target);
    auto col_name = source->add_column(type_String, "name");
    auto col_id = target->add_column(type_Int, "id");
    // target->add_search_index(col_id);

    Obj obj7 = target->create_object(ObjKey(7));
    Obj src_obj = source->create_object();
    src_obj.get_list<ObjKey>(col_link).add(ObjKey(7));

    // Detached views are in table order.
    TableView tv;
    CHECK_EQUAL(false, tv.is_in_table_order());

    // Queries not restricted by views are in table order.
    tv = target->where().find_all();
    CHECK_EQUAL(true, tv.is_in_table_order());

    // Views that have a distinct filter remain in table order.
    tv.distinct(col_id);
    CHECK_EQUAL(true, tv.is_in_table_order());

    // Views that are sorted are not guaranteed to be in table order.
    tv.sort(col_id, true);
    CHECK_EQUAL(false, tv.is_in_table_order());

    // Queries restricted by views are not guaranteed to be in table order.
    TableView restricting_view = target->where().equal(col_id, 0).find_all();
    tv = target->where(&restricting_view).find_all();
    CHECK_EQUAL(false, tv.is_in_table_order());

    // Backlinks are not guaranteed to be in table order.
    tv = obj7.get_backlink_view(source, col_link);
    CHECK_EQUAL(false, tv.is_in_table_order());

    // Views derived from a LinkView are not guaranteed to be in table order.
    auto ll = src_obj.get_linklist_ptr(col_link);
    tv = ll->get_sorted_view(col_name);
    CHECK_EQUAL(false, tv.is_in_table_order());

#ifdef LEGACY_TESTS   // FIXME
    // Asserts:
    tv = target->get_distinct_view(col_id);
    CHECK_EQUAL(true, tv.is_in_table_order());
#endif

    // … unless sorted.
    tv = target->get_sorted_view(col_id);
    CHECK_EQUAL(false, tv.is_in_table_order());
}

NONCONCURRENT_TEST(TableView_SortOrder_Similiar)
{
    Table table;
    auto col = table.add_column(type_String, "1");

    // This tests the expected sorting order with STRING_COMPARE_CORE_SIMILAR. See utf8_compare() in unicode.cpp. Only
    // characters
    // that have a visual representation are tested (control characters such as line feed are omitted).
    //
    // NOTE: Your editor must assume that Core source code is in utf8, and it must save as utf8, else this unit
    // test will fail.

    /*
    // This code snippet can be used to produce a list of *all* unicode characters in sorted order.
    //
    std::vector<int> original(collation_order, collation_order + sizeof collation_order / sizeof collation_order[0]);
    std::vector<int> sorted = original;
    std::sort(sorted.begin(), sorted.end());
    size_t highest_rank = sorted[sorted.size() - 1];

    std::wstring ws;
    for (size_t rank = 0; rank <= highest_rank; rank++) {
        size_t unicode = std::find(original.begin(), original.end(), rank) - original.begin();
        if (unicode != original.size()) {
            std::wcout << wchar_t(unicode) << "\n";
            std::cout << unicode << ", ";
            ws += wchar_t(unicode);
        }
    }
    */

    set_string_compare_method(STRING_COMPARE_CORE_SIMILAR, nullptr);

    table.create_object().set_all( " ");
    table.create_object().set_all( "!");
    table.create_object().set_all( "\"");
    table.create_object().set_all( "#");
    table.create_object().set_all( "%");
    table.create_object().set_all( "&");
    table.create_object().set_all( "'");
    table.create_object().set_all( "(");
    table.create_object().set_all( ")");
    table.create_object().set_all( "*");
    table.create_object().set_all( "+");
    table.create_object().set_all( ",");
    table.create_object().set_all( "-");
    table.create_object().set_all( ".");
    table.create_object().set_all( "/");
    table.create_object().set_all( ":");
    table.create_object().set_all( ";");
    table.create_object().set_all( "<");
    table.create_object().set_all( "=");
    table.create_object().set_all( ">");
    table.create_object().set_all( "?");
    table.create_object().set_all( "@");
    table.create_object().set_all( "[");
    table.create_object().set_all( "\\");
    table.create_object().set_all( "]");
    table.create_object().set_all( "^");
    table.create_object().set_all( "_");
    table.create_object().set_all( "`");
    table.create_object().set_all( "{");
    table.create_object().set_all( "|");
    table.create_object().set_all( "}");
    table.create_object().set_all( "~");
    table.create_object().set_all( " ");
    table.create_object().set_all( "¡");
    table.create_object().set_all( "¦");
    table.create_object().set_all( "§");
    table.create_object().set_all( "¨");
    table.create_object().set_all( "©");
    table.create_object().set_all( "«");
    table.create_object().set_all( "¬");
    table.create_object().set_all( "®");
    table.create_object().set_all( "¯");
    table.create_object().set_all( "°");
    table.create_object().set_all( "±");
    table.create_object().set_all( "´");
    table.create_object().set_all( "¶");
    table.create_object().set_all( "·");
    table.create_object().set_all( "¸");
    table.create_object().set_all( "»");
    table.create_object().set_all( "¿");
    table.create_object().set_all( "×");
    table.create_object().set_all( "÷");
    table.create_object().set_all( "¤");
    table.create_object().set_all( "¢");
    table.create_object().set_all( "$");
    table.create_object().set_all( "£");
    table.create_object().set_all( "¥");
    table.create_object().set_all( "0");
    table.create_object().set_all( "1");
    table.create_object().set_all( "¹");
    table.create_object().set_all( "½");
    table.create_object().set_all( "¼");
    table.create_object().set_all( "2");
    table.create_object().set_all( "²");
    table.create_object().set_all( "3");
    table.create_object().set_all( "³");
    table.create_object().set_all( "¾");
    table.create_object().set_all( "4");
    table.create_object().set_all( "5");
    table.create_object().set_all( "6");
    table.create_object().set_all( "7");
    table.create_object().set_all( "8");
    table.create_object().set_all( "9");
    table.create_object().set_all( "a");
    table.create_object().set_all( "A");
    table.create_object().set_all( "ª");
    table.create_object().set_all( "á");
    table.create_object().set_all( "Á");
    table.create_object().set_all( "à");
    table.create_object().set_all( "À");
    table.create_object().set_all( "ă");
    table.create_object().set_all( "Ă");
    table.create_object().set_all( "â");
    table.create_object().set_all( "Â");
    table.create_object().set_all( "ǎ");
    table.create_object().set_all( "Ǎ");
    table.create_object().set_all( "å");
    table.create_object().set_all( "Å");
    table.create_object().set_all( "ǻ");
    table.create_object().set_all( "Ǻ");
    table.create_object().set_all( "ä");
    table.create_object().set_all( "Ä");
    table.create_object().set_all( "ǟ");
    table.create_object().set_all( "Ǟ");
    table.create_object().set_all( "ã");
    table.create_object().set_all( "Ã");
    table.create_object().set_all( "ȧ");
    table.create_object().set_all( "Ȧ");
    table.create_object().set_all( "ǡ");
    table.create_object().set_all( "Ǡ");
    table.create_object().set_all( "ą");
    table.create_object().set_all( "Ą");
    table.create_object().set_all( "ā");
    table.create_object().set_all( "Ā");
    table.create_object().set_all( "ȁ");
    table.create_object().set_all( "Ȁ");
    table.create_object().set_all( "ȃ");
    table.create_object().set_all( "Ȃ");
    table.create_object().set_all( "æ");
    table.create_object().set_all( "Æ");
    table.create_object().set_all( "ǽ");
    table.create_object().set_all( "Ǽ");
    table.create_object().set_all( "ǣ");
    table.create_object().set_all( "Ǣ");
    table.create_object().set_all( "Ⱥ");
    table.create_object().set_all( "b");
    table.create_object().set_all( "B");
    table.create_object().set_all( "ƀ");
    table.create_object().set_all( "Ƀ");
    table.create_object().set_all( "Ɓ");
    table.create_object().set_all( "ƃ");
    table.create_object().set_all( "Ƃ");
    table.create_object().set_all( "c");
    table.create_object().set_all( "C");
    table.create_object().set_all( "ć");
    table.create_object().set_all( "Ć");
    table.create_object().set_all( "ĉ");
    table.create_object().set_all( "Ĉ");
    table.create_object().set_all( "č");
    table.create_object().set_all( "Č");
    table.create_object().set_all( "ċ");
    table.create_object().set_all( "Ċ");
    table.create_object().set_all( "ç");
    table.create_object().set_all( "Ç");
    table.create_object().set_all( "ȼ");
    table.create_object().set_all( "Ȼ");
    table.create_object().set_all( "ƈ");
    table.create_object().set_all( "Ƈ");
    table.create_object().set_all( "d");
    table.create_object().set_all( "D");
    table.create_object().set_all( "ď");
    table.create_object().set_all( "Ď");
    table.create_object().set_all( "đ");
    table.create_object().set_all( "Đ");
    table.create_object().set_all( "ð");
    table.create_object().set_all( "Ð");
    table.create_object().set_all( "ȸ");
    table.create_object().set_all( "ǳ");
    table.create_object().set_all( "ǲ");
    table.create_object().set_all( "Ǳ");
    table.create_object().set_all( "ǆ");
    table.create_object().set_all( "ǅ");
    table.create_object().set_all( "Ǆ");
    table.create_object().set_all( "Ɖ");
    table.create_object().set_all( "Ɗ");
    table.create_object().set_all( "ƌ");
    table.create_object().set_all( "Ƌ");
    table.create_object().set_all( "ȡ");
    table.create_object().set_all( "e");
    table.create_object().set_all( "E");
    table.create_object().set_all( "é");
    table.create_object().set_all( "É");
    table.create_object().set_all( "è");
    table.create_object().set_all( "È");
    table.create_object().set_all( "ĕ");
    table.create_object().set_all( "Ĕ");
    table.create_object().set_all( "ê");
    table.create_object().set_all( "Ê");
    table.create_object().set_all( "ě");
    table.create_object().set_all( "Ě");
    table.create_object().set_all( "ë");
    table.create_object().set_all( "Ë");
    table.create_object().set_all( "ė");
    table.create_object().set_all( "Ė");
    table.create_object().set_all( "ȩ");
    table.create_object().set_all( "Ȩ");
    table.create_object().set_all( "ę");
    table.create_object().set_all( "Ę");
    table.create_object().set_all( "ē");
    table.create_object().set_all( "Ē");
    table.create_object().set_all( "ȅ");
    table.create_object().set_all( "Ȅ");
    table.create_object().set_all( "ȇ");
    table.create_object().set_all( "Ȇ");
    table.create_object().set_all( "ɇ");
    table.create_object().set_all( "Ɇ");
    table.create_object().set_all( "ǝ");
    table.create_object().set_all( "Ǝ");
    table.create_object().set_all( "Ə");
    table.create_object().set_all( "Ɛ");
    table.create_object().set_all( "f");
    table.create_object().set_all( "F");
    table.create_object().set_all( "ƒ");
    table.create_object().set_all( "Ƒ");
    table.create_object().set_all( "g");
    table.create_object().set_all( "G");
    table.create_object().set_all( "ǵ");
    table.create_object().set_all( "Ǵ");
    table.create_object().set_all( "ğ");
    table.create_object().set_all( "Ğ");
    table.create_object().set_all( "ĝ");
    table.create_object().set_all( "Ĝ");
    table.create_object().set_all( "ǧ");
    table.create_object().set_all( "Ǧ");
    table.create_object().set_all( "ġ");
    table.create_object().set_all( "Ġ");
    table.create_object().set_all( "ģ");
    table.create_object().set_all( "Ģ");
    table.create_object().set_all( "ǥ");
    table.create_object().set_all( "Ǥ");
    table.create_object().set_all( "Ɠ");
    table.create_object().set_all( "Ɣ");
    table.create_object().set_all( "ƣ");
    table.create_object().set_all( "Ƣ");
    table.create_object().set_all( "h");
    table.create_object().set_all( "H");
    table.create_object().set_all( "ĥ");
    table.create_object().set_all( "Ĥ");
    table.create_object().set_all( "ȟ");
    table.create_object().set_all( "Ȟ");
    table.create_object().set_all( "ħ");
    table.create_object().set_all( "Ħ");
    table.create_object().set_all( "ƕ");
    table.create_object().set_all( "Ƕ");
    table.create_object().set_all( "i");
    table.create_object().set_all( "I");
    table.create_object().set_all( "í");
    table.create_object().set_all( "Í");
    table.create_object().set_all( "ì");
    table.create_object().set_all( "Ì");
    table.create_object().set_all( "ĭ");
    table.create_object().set_all( "Ĭ");
    table.create_object().set_all( "î");
    table.create_object().set_all( "Î");
    table.create_object().set_all( "ǐ");
    table.create_object().set_all( "Ǐ");
    table.create_object().set_all( "ï");
    table.create_object().set_all( "Ï");
    table.create_object().set_all( "ĩ");
    table.create_object().set_all( "Ĩ");
    table.create_object().set_all( "İ");
    table.create_object().set_all( "į");
    table.create_object().set_all( "Į");
    table.create_object().set_all( "ī");
    table.create_object().set_all( "Ī");
    table.create_object().set_all( "ȉ");
    table.create_object().set_all( "Ȉ");
    table.create_object().set_all( "ȋ");
    table.create_object().set_all( "Ȋ");
    table.create_object().set_all( "ĳ");
    table.create_object().set_all( "Ĳ");
    table.create_object().set_all( "ı");
    table.create_object().set_all( "Ɨ");
    table.create_object().set_all( "Ɩ");
    table.create_object().set_all( "j");
    table.create_object().set_all( "J");
    table.create_object().set_all( "ĵ");
    table.create_object().set_all( "Ĵ");
    table.create_object().set_all( "ǰ");
    table.create_object().set_all( "ȷ");
    table.create_object().set_all( "ɉ");
    table.create_object().set_all( "Ɉ");
    table.create_object().set_all( "k");
    table.create_object().set_all( "K");
    table.create_object().set_all( "ǩ");
    table.create_object().set_all( "Ǩ");
    table.create_object().set_all( "ķ");
    table.create_object().set_all( "Ķ");
    table.create_object().set_all( "ƙ");
    table.create_object().set_all( "Ƙ");
    table.create_object().set_all( "ĺ");
    table.create_object().set_all( "Ĺ");
    table.create_object().set_all( "ľ");
    table.create_object().set_all( "Ľ");
    table.create_object().set_all( "ļ");
    table.create_object().set_all( "Ļ");
    table.create_object().set_all( "ł");
    table.create_object().set_all( "Ł");
    table.create_object().set_all( "ŀ");
    table.create_object().set_all( "l");
    table.create_object().set_all( "Ŀ");
    table.create_object().set_all( "L");
    table.create_object().set_all( "ǉ");
    table.create_object().set_all( "ǈ");
    table.create_object().set_all( "Ǉ");
    table.create_object().set_all( "ƚ");
    table.create_object().set_all( "Ƚ");
    table.create_object().set_all( "ȴ");
    table.create_object().set_all( "ƛ");
    table.create_object().set_all( "m");
    table.create_object().set_all( "M");
    table.create_object().set_all( "n");
    table.create_object().set_all( "N");
    table.create_object().set_all( "ń");
    table.create_object().set_all( "Ń");
    table.create_object().set_all( "ǹ");
    table.create_object().set_all( "Ǹ");
    table.create_object().set_all( "ň");
    table.create_object().set_all( "Ň");
    table.create_object().set_all( "ñ");
    table.create_object().set_all( "Ñ");
    table.create_object().set_all( "ņ");
    table.create_object().set_all( "Ņ");
    table.create_object().set_all( "ǌ");
    table.create_object().set_all( "ǋ");
    table.create_object().set_all( "Ǌ");
    table.create_object().set_all( "Ɲ");
    table.create_object().set_all( "ƞ");
    table.create_object().set_all( "Ƞ");
    table.create_object().set_all( "ȵ");
    table.create_object().set_all( "ŋ");
    table.create_object().set_all( "Ŋ");
    table.create_object().set_all( "o");
    table.create_object().set_all( "O");
    table.create_object().set_all( "º");
    table.create_object().set_all( "ó");
    table.create_object().set_all( "Ó");
    table.create_object().set_all( "ò");
    table.create_object().set_all( "Ò");
    table.create_object().set_all( "ŏ");
    table.create_object().set_all( "Ŏ");
    table.create_object().set_all( "ô");
    table.create_object().set_all( "Ô");
    table.create_object().set_all( "ǒ");
    table.create_object().set_all( "Ǒ");
    table.create_object().set_all( "ö");
    table.create_object().set_all( "Ö");
    table.create_object().set_all( "ȫ");
    table.create_object().set_all( "Ȫ");
    table.create_object().set_all( "ő");
    table.create_object().set_all( "Ő");
    table.create_object().set_all( "õ");
    table.create_object().set_all( "Õ");
    table.create_object().set_all( "ȭ");
    table.create_object().set_all( "Ȭ");
    table.create_object().set_all( "ȯ");
    table.create_object().set_all( "Ȯ");
    table.create_object().set_all( "ȱ");
    table.create_object().set_all( "Ȱ");
    table.create_object().set_all( "ø");
    table.create_object().set_all( "Ø");
    table.create_object().set_all( "ǿ");
    table.create_object().set_all( "Ǿ");
    table.create_object().set_all( "ǫ");
    table.create_object().set_all( "Ǫ");
    table.create_object().set_all( "ǭ");
    table.create_object().set_all( "Ǭ");
    table.create_object().set_all( "ō");
    table.create_object().set_all( "Ō");
    table.create_object().set_all( "ȍ");
    table.create_object().set_all( "Ȍ");
    table.create_object().set_all( "ȏ");
    table.create_object().set_all( "Ȏ");
    table.create_object().set_all( "ơ");
    table.create_object().set_all( "Ơ");
    table.create_object().set_all( "œ");
    table.create_object().set_all( "Œ");
    table.create_object().set_all( "Ɔ");
    table.create_object().set_all( "Ɵ");
    table.create_object().set_all( "ȣ");
    table.create_object().set_all( "Ȣ");
    table.create_object().set_all( "p");
    table.create_object().set_all( "P");
    table.create_object().set_all( "ƥ");
    table.create_object().set_all( "Ƥ");
    table.create_object().set_all( "q");
    table.create_object().set_all( "Q");
    table.create_object().set_all( "ȹ");
    table.create_object().set_all( "ɋ");
    table.create_object().set_all( "Ɋ");
    table.create_object().set_all( "ĸ");
    table.create_object().set_all( "r");
    table.create_object().set_all( "R");
    table.create_object().set_all( "ŕ");
    table.create_object().set_all( "Ŕ");
    table.create_object().set_all( "ř");
    table.create_object().set_all( "Ř");
    table.create_object().set_all( "ŗ");
    table.create_object().set_all( "Ŗ");
    table.create_object().set_all( "ȑ");
    table.create_object().set_all( "Ȑ");
    table.create_object().set_all( "ȓ");
    table.create_object().set_all( "Ȓ");
    table.create_object().set_all( "Ʀ");
    table.create_object().set_all( "ɍ");
    table.create_object().set_all( "Ɍ");
    table.create_object().set_all( "s");
    table.create_object().set_all( "S");
    table.create_object().set_all( "ś");
    table.create_object().set_all( "Ś");
    table.create_object().set_all( "ŝ");
    table.create_object().set_all( "Ŝ");
    table.create_object().set_all( "š");
    table.create_object().set_all( "Š");
    table.create_object().set_all( "ş");
    table.create_object().set_all( "Ş");
    table.create_object().set_all( "ș");
    table.create_object().set_all( "Ș");
    table.create_object().set_all( "ſ");
    table.create_object().set_all( "ß");
    table.create_object().set_all( "ȿ");
    table.create_object().set_all( "Ʃ");
    table.create_object().set_all( "ƪ");
    table.create_object().set_all( "t");
    table.create_object().set_all( "T");
    table.create_object().set_all( "ť");
    table.create_object().set_all( "Ť");
    table.create_object().set_all( "ţ");
    table.create_object().set_all( "Ţ");
    table.create_object().set_all( "ț");
    table.create_object().set_all( "Ț");
    table.create_object().set_all( "ƾ");
    table.create_object().set_all( "ŧ");
    table.create_object().set_all( "Ŧ");
    table.create_object().set_all( "Ⱦ");
    table.create_object().set_all( "ƫ");
    table.create_object().set_all( "ƭ");
    table.create_object().set_all( "Ƭ");
    table.create_object().set_all( "Ʈ");
    table.create_object().set_all( "ȶ");
    table.create_object().set_all( "u");
    table.create_object().set_all( "U");
    table.create_object().set_all( "ú");
    table.create_object().set_all( "Ú");
    table.create_object().set_all( "ù");
    table.create_object().set_all( "Ù");
    table.create_object().set_all( "ŭ");
    table.create_object().set_all( "Ŭ");
    table.create_object().set_all( "û");
    table.create_object().set_all( "Û");
    table.create_object().set_all( "ǔ");
    table.create_object().set_all( "Ǔ");
    table.create_object().set_all( "ů");
    table.create_object().set_all( "Ů");
    table.create_object().set_all( "ü");
    table.create_object().set_all( "Ü");
    table.create_object().set_all( "ǘ");
    table.create_object().set_all( "Ǘ");
    table.create_object().set_all( "ǜ");
    table.create_object().set_all( "Ǜ");
    table.create_object().set_all( "ǚ");
    table.create_object().set_all( "Ǚ");
    table.create_object().set_all( "ǖ");
    table.create_object().set_all( "Ǖ");
    table.create_object().set_all( "ű");
    table.create_object().set_all( "Ű");
    table.create_object().set_all( "ũ");
    table.create_object().set_all( "Ũ");
    table.create_object().set_all( "ų");
    table.create_object().set_all( "Ų");
    table.create_object().set_all( "ū");
    table.create_object().set_all( "Ū");
    table.create_object().set_all( "ȕ");
    table.create_object().set_all( "Ȕ");
    table.create_object().set_all( "ȗ");
    table.create_object().set_all( "Ȗ");
    table.create_object().set_all( "ư");
    table.create_object().set_all( "Ư");
    table.create_object().set_all( "Ʉ");
    table.create_object().set_all( "Ɯ");
    table.create_object().set_all( "Ʊ");
    table.create_object().set_all( "v");
    table.create_object().set_all( "V");
    table.create_object().set_all( "Ʋ");
    table.create_object().set_all( "Ʌ");
    table.create_object().set_all( "w");
    table.create_object().set_all( "W");
    table.create_object().set_all( "ŵ");
    table.create_object().set_all( "Ŵ");
    table.create_object().set_all( "x");
    table.create_object().set_all( "X");
    table.create_object().set_all( "y");
    table.create_object().set_all( "Y");
    table.create_object().set_all( "ý");
    table.create_object().set_all( "Ý");
    table.create_object().set_all( "ŷ");
    table.create_object().set_all( "Ŷ");
    table.create_object().set_all( "ÿ");
    table.create_object().set_all( "Ÿ");
    table.create_object().set_all( "ȳ");
    table.create_object().set_all( "Ȳ");
    table.create_object().set_all( "ɏ");
    table.create_object().set_all( "Ɏ");
    table.create_object().set_all( "ƴ");
    table.create_object().set_all( "Ƴ");
    table.create_object().set_all( "ȝ");
    table.create_object().set_all( "Ȝ");
    table.create_object().set_all( "z");
    table.create_object().set_all( "Z");
    table.create_object().set_all( "ź");
    table.create_object().set_all( "Ź");
    table.create_object().set_all( "ž");
    table.create_object().set_all( "Ž");
    table.create_object().set_all( "ż");
    table.create_object().set_all( "Ż");
    table.create_object().set_all( "ƍ");
    table.create_object().set_all( "ƶ");
    table.create_object().set_all( "Ƶ");
    table.create_object().set_all( "ȥ");
    table.create_object().set_all( "Ȥ");
    table.create_object().set_all( "ɀ");
    table.create_object().set_all( "Ʒ");
    table.create_object().set_all( "ǯ");
    table.create_object().set_all( "Ǯ");
    table.create_object().set_all( "ƹ");
    table.create_object().set_all( "Ƹ");
    table.create_object().set_all( "ƺ");
    table.create_object().set_all( "þ");
    table.create_object().set_all( "Þ");
    table.create_object().set_all( "ƿ");
    table.create_object().set_all( "Ƿ");
    table.create_object().set_all( "ƻ");
    table.create_object().set_all( "ƨ");
    table.create_object().set_all( "Ƨ");
    table.create_object().set_all( "ƽ");
    table.create_object().set_all( "Ƽ");
    table.create_object().set_all( "ƅ");
    table.create_object().set_all( "Ƅ");
    table.create_object().set_all( "ɂ");
    table.create_object().set_all( "Ɂ");
    table.create_object().set_all( "ŉ");
    table.create_object().set_all( "ǀ");
    table.create_object().set_all( "ǁ");
    table.create_object().set_all( "ǂ");
    table.create_object().set_all( "ǃ");
    table.create_object().set_all( "µ");

    // Core-only is default comparer
    TableView v1 = table.where().find_all();
    TableView v2 = table.where().find_all();

    v2.sort(col);
#ifdef LEGACY_TESTS   // FIXME
    // FIXME: The following checks fail
    for (size_t t = 0; t < v1.size(); t++) {
        CHECK_EQUAL(v1.get_object(t).get_key(), v2.get_object(t).get_key());
    }
#endif

    // Set back to default in case other tests rely on this
    set_string_compare_method(STRING_COMPARE_CORE, nullptr);
}


NONCONCURRENT_TEST(TableView_SortOrder_Core)
{
    Table table;
    auto col = table.add_column(type_String, "1");

    // This tests the expected sorting order with STRING_COMPARE_CORE. See utf8_compare() in unicode.cpp. Only
    // characters
    // that have a visual representation are tested (control characters such as line feed are omitted).
    //
    // NOTE: Your editor must assume that Core source code is in utf8, and it must save as utf8, else this unit
    // test will fail.

    set_string_compare_method(STRING_COMPARE_CORE, nullptr);

    table.create_object().set_all( "'");
    table.create_object().set_all( "-");
    table.create_object().set_all( " ");
    table.create_object().set_all( " ");
    table.create_object().set_all( "!");
    table.create_object().set_all( "\"");
    table.create_object().set_all( "#");
    table.create_object().set_all( "$");
    table.create_object().set_all( "%");
    table.create_object().set_all( "&");
    table.create_object().set_all( "(");
    table.create_object().set_all( ")");
    table.create_object().set_all( "*");
    table.create_object().set_all( ",");
    table.create_object().set_all( ".");
    table.create_object().set_all( "/");
    table.create_object().set_all( ":");
    table.create_object().set_all( ";");
    table.create_object().set_all( "?");
    table.create_object().set_all( "@");
    table.create_object().set_all( "[");
    table.create_object().set_all( "\\");
    table.create_object().set_all( "^");
    table.create_object().set_all( "_");
    table.create_object().set_all( "`");
    table.create_object().set_all( "{");
    table.create_object().set_all( "|");
    table.create_object().set_all( "}");
    table.create_object().set_all( "~");
    table.create_object().set_all( "¡");
    table.create_object().set_all( "¦");
    table.create_object().set_all( "¨");
    table.create_object().set_all( "¯");
    table.create_object().set_all( "´");
    table.create_object().set_all( "¸");
    table.create_object().set_all( "¿");
    table.create_object().set_all( "ǃ");
    table.create_object().set_all( "¢");
    table.create_object().set_all( "£");
    table.create_object().set_all( "¤");
    table.create_object().set_all( "¥");
    table.create_object().set_all( "+");
    table.create_object().set_all( "<");
    table.create_object().set_all( "=");
    table.create_object().set_all( ">");
    table.create_object().set_all( "±");
    table.create_object().set_all( "«");
    table.create_object().set_all( "»");
    table.create_object().set_all( "×");
    table.create_object().set_all( "÷");
    table.create_object().set_all( "ǀ");
    table.create_object().set_all( "ǁ");
    table.create_object().set_all( "ǂ");
    table.create_object().set_all( "§");
    table.create_object().set_all( "©");
    table.create_object().set_all( "¬");
    table.create_object().set_all( "®");
    table.create_object().set_all( "°");
    table.create_object().set_all( "µ");
    table.create_object().set_all( "¶");
    table.create_object().set_all( "·");
    table.create_object().set_all( "0");
    table.create_object().set_all( "¼");
    table.create_object().set_all( "½");
    table.create_object().set_all( "¾");
    table.create_object().set_all( "1");
    table.create_object().set_all( "¹");
    table.create_object().set_all( "2");
    table.create_object().set_all( "ƻ");
    table.create_object().set_all( "²");
    table.create_object().set_all( "3");
    table.create_object().set_all( "³");
    table.create_object().set_all( "4");
    table.create_object().set_all( "5");
    table.create_object().set_all( "ƽ");
    table.create_object().set_all( "Ƽ");
    table.create_object().set_all( "6");
    table.create_object().set_all( "7");
    table.create_object().set_all( "8");
    table.create_object().set_all( "9");
    table.create_object().set_all( "a");
    table.create_object().set_all( "A");
    table.create_object().set_all( "ª");
    table.create_object().set_all( "á");
    table.create_object().set_all( "Á");
    table.create_object().set_all( "à");
    table.create_object().set_all( "À");
    table.create_object().set_all( "ȧ");
    table.create_object().set_all( "Ȧ");
    table.create_object().set_all( "â");
    table.create_object().set_all( "Â");
    table.create_object().set_all( "ǎ");
    table.create_object().set_all( "Ǎ");
    table.create_object().set_all( "ă");
    table.create_object().set_all( "Ă");
    table.create_object().set_all( "ā");
    table.create_object().set_all( "Ā");
    table.create_object().set_all( "ã");
    table.create_object().set_all( "Ã");
    table.create_object().set_all( "ą");
    table.create_object().set_all( "Ą");
    table.create_object().set_all( "Ⱥ");
    table.create_object().set_all( "ǡ");
    table.create_object().set_all( "Ǡ");
    table.create_object().set_all( "ǻ");
    table.create_object().set_all( "Ǻ");
    table.create_object().set_all( "ǟ");
    table.create_object().set_all( "Ǟ");
    table.create_object().set_all( "ȁ");
    table.create_object().set_all( "Ȁ");
    table.create_object().set_all( "ȃ");
    table.create_object().set_all( "Ȃ");
    table.create_object().set_all( "ǽ");
    table.create_object().set_all( "Ǽ");
    table.create_object().set_all( "b");
    table.create_object().set_all( "B");
    table.create_object().set_all( "ƀ");
    table.create_object().set_all( "Ƀ");
    table.create_object().set_all( "Ɓ");
    table.create_object().set_all( "ƃ");
    table.create_object().set_all( "Ƃ");
    table.create_object().set_all( "ƅ");
    table.create_object().set_all( "Ƅ");
    table.create_object().set_all( "c");
    table.create_object().set_all( "C");
    table.create_object().set_all( "ć");
    table.create_object().set_all( "Ć");
    table.create_object().set_all( "ċ");
    table.create_object().set_all( "Ċ");
    table.create_object().set_all( "ĉ");
    table.create_object().set_all( "Ĉ");
    table.create_object().set_all( "č");
    table.create_object().set_all( "Č");
    table.create_object().set_all( "ç");
    table.create_object().set_all( "Ç");
    table.create_object().set_all( "ȼ");
    table.create_object().set_all( "Ȼ");
    table.create_object().set_all( "ƈ");
    table.create_object().set_all( "Ƈ");
    table.create_object().set_all( "Ɔ");
    table.create_object().set_all( "d");
    table.create_object().set_all( "D");
    table.create_object().set_all( "ď");
    table.create_object().set_all( "Ď");
    table.create_object().set_all( "đ");
    table.create_object().set_all( "Đ");
    table.create_object().set_all( "ƌ");
    table.create_object().set_all( "Ƌ");
    table.create_object().set_all( "Ɗ");
    table.create_object().set_all( "ð");
    table.create_object().set_all( "Ð");
    table.create_object().set_all( "ƍ");
    table.create_object().set_all( "ȸ");
    table.create_object().set_all( "ǳ");
    table.create_object().set_all( "ǲ");
    table.create_object().set_all( "Ǳ");
    table.create_object().set_all( "ǆ");
    table.create_object().set_all( "ǅ");
    table.create_object().set_all( "Ǆ");
    table.create_object().set_all( "Ɖ");
    table.create_object().set_all( "ȡ");
    table.create_object().set_all( "e");
    table.create_object().set_all( "E");
    table.create_object().set_all( "é");
    table.create_object().set_all( "É");
    table.create_object().set_all( "è");
    table.create_object().set_all( "È");
    table.create_object().set_all( "ė");
    table.create_object().set_all( "Ė");
    table.create_object().set_all( "ê");
    table.create_object().set_all( "Ê");
    table.create_object().set_all( "ë");
    table.create_object().set_all( "Ë");
    table.create_object().set_all( "ě");
    table.create_object().set_all( "Ě");
    table.create_object().set_all( "ĕ");
    table.create_object().set_all( "Ĕ");
    table.create_object().set_all( "ē");
    table.create_object().set_all( "Ē");
    table.create_object().set_all( "ę");
    table.create_object().set_all( "Ę");
    table.create_object().set_all( "ȩ");
    table.create_object().set_all( "Ȩ");
    table.create_object().set_all( "ɇ");
    table.create_object().set_all( "Ɇ");
    table.create_object().set_all( "ȅ");
    table.create_object().set_all( "Ȅ");
    table.create_object().set_all( "ȇ");
    table.create_object().set_all( "Ȇ");
    table.create_object().set_all( "ǝ");
    table.create_object().set_all( "Ǝ");
    table.create_object().set_all( "Ə");
    table.create_object().set_all( "Ɛ");
    table.create_object().set_all( "ȝ");
    table.create_object().set_all( "Ȝ");
    table.create_object().set_all( "f");
    table.create_object().set_all( "F");
    table.create_object().set_all( "ƒ");
    table.create_object().set_all( "Ƒ");
    table.create_object().set_all( "g");
    table.create_object().set_all( "G");
    table.create_object().set_all( "ǵ");
    table.create_object().set_all( "Ǵ");
    table.create_object().set_all( "ġ");
    table.create_object().set_all( "Ġ");
    table.create_object().set_all( "ĝ");
    table.create_object().set_all( "Ĝ");
    table.create_object().set_all( "ǧ");
    table.create_object().set_all( "Ǧ");
    table.create_object().set_all( "ğ");
    table.create_object().set_all( "Ğ");
    table.create_object().set_all( "ģ");
    table.create_object().set_all( "Ģ");
    table.create_object().set_all( "ǥ");
    table.create_object().set_all( "Ǥ");
    table.create_object().set_all( "Ɠ");
    table.create_object().set_all( "Ɣ");
    table.create_object().set_all( "h");
    table.create_object().set_all( "H");
    table.create_object().set_all( "ĥ");
    table.create_object().set_all( "Ĥ");
    table.create_object().set_all( "ȟ");
    table.create_object().set_all( "Ȟ");
    table.create_object().set_all( "ħ");
    table.create_object().set_all( "Ħ");
    table.create_object().set_all( "ƕ");
    table.create_object().set_all( "Ƕ");
    table.create_object().set_all( "i");
    table.create_object().set_all( "I");
    table.create_object().set_all( "ı");
    table.create_object().set_all( "í");
    table.create_object().set_all( "Í");
    table.create_object().set_all( "ì");
    table.create_object().set_all( "Ì");
    table.create_object().set_all( "İ");
    table.create_object().set_all( "î");
    table.create_object().set_all( "Î");
    table.create_object().set_all( "ï");
    table.create_object().set_all( "Ï");
    table.create_object().set_all( "ǐ");
    table.create_object().set_all( "Ǐ");
    table.create_object().set_all( "ĭ");
    table.create_object().set_all( "Ĭ");
    table.create_object().set_all( "ī");
    table.create_object().set_all( "Ī");
    table.create_object().set_all( "ĩ");
    table.create_object().set_all( "Ĩ");
    table.create_object().set_all( "į");
    table.create_object().set_all( "Į");
    table.create_object().set_all( "Ɨ");
    table.create_object().set_all( "ȉ");
    table.create_object().set_all( "Ȉ");
    table.create_object().set_all( "ȋ");
    table.create_object().set_all( "Ȋ");
    table.create_object().set_all( "Ɩ");
    table.create_object().set_all( "ĳ");
    table.create_object().set_all( "Ĳ");
    table.create_object().set_all( "j");
    table.create_object().set_all( "J");
    table.create_object().set_all( "ȷ");
    table.create_object().set_all( "ĵ");
    table.create_object().set_all( "Ĵ");
    table.create_object().set_all( "ǰ");
    table.create_object().set_all( "ɉ");
    table.create_object().set_all( "Ɉ");
    table.create_object().set_all( "k");
    table.create_object().set_all( "K");
    table.create_object().set_all( "ǩ");
    table.create_object().set_all( "Ǩ");
    table.create_object().set_all( "ķ");
    table.create_object().set_all( "Ķ");
    table.create_object().set_all( "ƙ");
    table.create_object().set_all( "Ƙ");
    table.create_object().set_all( "l");
    table.create_object().set_all( "L");
    table.create_object().set_all( "ĺ");
    table.create_object().set_all( "Ĺ");
    table.create_object().set_all( "ŀ");
    table.create_object().set_all( "Ŀ");
    table.create_object().set_all( "ľ");
    table.create_object().set_all( "Ľ");
    table.create_object().set_all( "ļ");
    table.create_object().set_all( "Ļ");
    table.create_object().set_all( "ƚ");
    table.create_object().set_all( "Ƚ");
    table.create_object().set_all( "ł");
    table.create_object().set_all( "Ł");
    table.create_object().set_all( "ƛ");
    table.create_object().set_all( "ǉ");
    table.create_object().set_all( "ǈ");
    table.create_object().set_all( "Ǉ");
    table.create_object().set_all( "ȴ");
    table.create_object().set_all( "m");
    table.create_object().set_all( "M");
    table.create_object().set_all( "Ɯ");
    table.create_object().set_all( "n");
    table.create_object().set_all( "N");
    table.create_object().set_all( "ń");
    table.create_object().set_all( "Ń");
    table.create_object().set_all( "ǹ");
    table.create_object().set_all( "Ǹ");
    table.create_object().set_all( "ň");
    table.create_object().set_all( "Ň");
    table.create_object().set_all( "ñ");
    table.create_object().set_all( "Ñ");
    table.create_object().set_all( "ņ");
    table.create_object().set_all( "Ņ");
    table.create_object().set_all( "Ɲ");
    table.create_object().set_all( "ŉ");
    table.create_object().set_all( "ƞ");
    table.create_object().set_all( "Ƞ");
    table.create_object().set_all( "ǌ");
    table.create_object().set_all( "ǋ");
    table.create_object().set_all( "Ǌ");
    table.create_object().set_all( "ȵ");
    table.create_object().set_all( "ŋ");
    table.create_object().set_all( "Ŋ");
    table.create_object().set_all( "o");
    table.create_object().set_all( "O");
    table.create_object().set_all( "º");
    table.create_object().set_all( "ó");
    table.create_object().set_all( "Ó");
    table.create_object().set_all( "ò");
    table.create_object().set_all( "Ò");
    table.create_object().set_all( "ȯ");
    table.create_object().set_all( "Ȯ");
    table.create_object().set_all( "ô");
    table.create_object().set_all( "Ô");
    table.create_object().set_all( "ǒ");
    table.create_object().set_all( "Ǒ");
    table.create_object().set_all( "ŏ");
    table.create_object().set_all( "Ŏ");
    table.create_object().set_all( "ō");
    table.create_object().set_all( "Ō");
    table.create_object().set_all( "õ");
    table.create_object().set_all( "Õ");
    table.create_object().set_all( "ǫ");
    table.create_object().set_all( "Ǫ");
    table.create_object().set_all( "Ɵ");
    table.create_object().set_all( "ȱ");
    table.create_object().set_all( "Ȱ");
    table.create_object().set_all( "ȫ");
    table.create_object().set_all( "Ȫ");
    table.create_object().set_all( "ǿ");
    table.create_object().set_all( "Ǿ");
    table.create_object().set_all( "ȭ");
    table.create_object().set_all( "Ȭ");
    table.create_object().set_all( "ǭ");
    table.create_object().set_all( "Ǭ");
    table.create_object().set_all( "ȍ");
    table.create_object().set_all( "Ȍ");
    table.create_object().set_all( "ȏ");
    table.create_object().set_all( "Ȏ");
    table.create_object().set_all( "ơ");
    table.create_object().set_all( "Ơ");
    table.create_object().set_all( "ƣ");
    table.create_object().set_all( "Ƣ");
    table.create_object().set_all( "œ");
    table.create_object().set_all( "Œ");
    table.create_object().set_all( "ȣ");
    table.create_object().set_all( "Ȣ");
    table.create_object().set_all( "p");
    table.create_object().set_all( "P");
    table.create_object().set_all( "ƥ");
    table.create_object().set_all( "Ƥ");
    table.create_object().set_all( "q");
    table.create_object().set_all( "Q");
    table.create_object().set_all( "ĸ");
    table.create_object().set_all( "ɋ");
    table.create_object().set_all( "Ɋ");
    table.create_object().set_all( "ȹ");
    table.create_object().set_all( "r");
    table.create_object().set_all( "R");
    table.create_object().set_all( "Ʀ");
    table.create_object().set_all( "ŕ");
    table.create_object().set_all( "Ŕ");
    table.create_object().set_all( "ř");
    table.create_object().set_all( "Ř");
    table.create_object().set_all( "ŗ");
    table.create_object().set_all( "Ŗ");
    table.create_object().set_all( "ɍ");
    table.create_object().set_all( "Ɍ");
    table.create_object().set_all( "ȑ");
    table.create_object().set_all( "Ȑ");
    table.create_object().set_all( "ȓ");
    table.create_object().set_all( "Ȓ");
    table.create_object().set_all( "s");
    table.create_object().set_all( "S");
    table.create_object().set_all( "ś");
    table.create_object().set_all( "Ś");
    table.create_object().set_all( "ŝ");
    table.create_object().set_all( "Ŝ");
    table.create_object().set_all( "š");
    table.create_object().set_all( "Š");
    table.create_object().set_all( "ş");
    table.create_object().set_all( "Ş");
    table.create_object().set_all( "ș");
    table.create_object().set_all( "Ș");
    table.create_object().set_all( "ȿ");
    table.create_object().set_all( "Ʃ");
    table.create_object().set_all( "ƨ");
    table.create_object().set_all( "Ƨ");
    table.create_object().set_all( "ƪ");
    table.create_object().set_all( "ß");
    table.create_object().set_all( "ſ");
    table.create_object().set_all( "t");
    table.create_object().set_all( "T");
    table.create_object().set_all( "ť");
    table.create_object().set_all( "Ť");
    table.create_object().set_all( "ţ");
    table.create_object().set_all( "Ţ");
    table.create_object().set_all( "ƭ");
    table.create_object().set_all( "Ƭ");
    table.create_object().set_all( "ƫ");
    table.create_object().set_all( "Ʈ");
    table.create_object().set_all( "ț");
    table.create_object().set_all( "Ț");
    table.create_object().set_all( "Ⱦ");
    table.create_object().set_all( "ȶ");
    table.create_object().set_all( "þ");
    table.create_object().set_all( "Þ");
    table.create_object().set_all( "ŧ");
    table.create_object().set_all( "Ŧ");
    table.create_object().set_all( "u");
    table.create_object().set_all( "U");
    table.create_object().set_all( "ú");
    table.create_object().set_all( "Ú");
    table.create_object().set_all( "ù");
    table.create_object().set_all( "Ù");
    table.create_object().set_all( "û");
    table.create_object().set_all( "Û");
    table.create_object().set_all( "ǔ");
    table.create_object().set_all( "Ǔ");
    table.create_object().set_all( "ŭ");
    table.create_object().set_all( "Ŭ");
    table.create_object().set_all( "ū");
    table.create_object().set_all( "Ū");
    table.create_object().set_all( "ũ");
    table.create_object().set_all( "Ũ");
    table.create_object().set_all( "ů");
    table.create_object().set_all( "Ů");
    table.create_object().set_all( "ų");
    table.create_object().set_all( "Ų");
    table.create_object().set_all( "Ʉ");
    table.create_object().set_all( "ǘ");
    table.create_object().set_all( "Ǘ");
    table.create_object().set_all( "ǜ");
    table.create_object().set_all( "Ǜ");
    table.create_object().set_all( "ǚ");
    table.create_object().set_all( "Ǚ");
    table.create_object().set_all( "ǖ");
    table.create_object().set_all( "Ǖ");
    table.create_object().set_all( "ȕ");
    table.create_object().set_all( "Ȕ");
    table.create_object().set_all( "ȗ");
    table.create_object().set_all( "Ȗ");
    table.create_object().set_all( "ư");
    table.create_object().set_all( "Ư");
    table.create_object().set_all( "Ʊ");
    table.create_object().set_all( "v");
    table.create_object().set_all( "V");
    table.create_object().set_all( "Ʋ");
    table.create_object().set_all( "Ʌ");
    table.create_object().set_all( "w");
    table.create_object().set_all( "W");
    table.create_object().set_all( "ŵ");
    table.create_object().set_all( "Ŵ");
    table.create_object().set_all( "ƿ");
    table.create_object().set_all( "Ƿ");
    table.create_object().set_all( "x");
    table.create_object().set_all( "X");
    table.create_object().set_all( "y");
    table.create_object().set_all( "Y");
    table.create_object().set_all( "ý");
    table.create_object().set_all( "Ý");
    table.create_object().set_all( "ŷ");
    table.create_object().set_all( "Ŷ");
    table.create_object().set_all( "ÿ");
    table.create_object().set_all( "Ÿ");
    table.create_object().set_all( "ȳ");
    table.create_object().set_all( "Ȳ");
    table.create_object().set_all( "ű");
    table.create_object().set_all( "Ű");
    table.create_object().set_all( "ɏ");
    table.create_object().set_all( "Ɏ");
    table.create_object().set_all( "ƴ");
    table.create_object().set_all( "Ƴ");
    table.create_object().set_all( "ü");
    table.create_object().set_all( "Ü");
    table.create_object().set_all( "z");
    table.create_object().set_all( "Z");
    table.create_object().set_all( "ź");
    table.create_object().set_all( "Ź");
    table.create_object().set_all( "ż");
    table.create_object().set_all( "Ż");
    table.create_object().set_all( "ž");
    table.create_object().set_all( "Ž");
    table.create_object().set_all( "ƶ");
    table.create_object().set_all( "Ƶ");
    table.create_object().set_all( "ȥ");
    table.create_object().set_all( "Ȥ");
    table.create_object().set_all( "ɀ");
    table.create_object().set_all( "æ");
    table.create_object().set_all( "Æ");
    table.create_object().set_all( "Ʒ");
    table.create_object().set_all( "ǣ");
    table.create_object().set_all( "Ǣ");
    table.create_object().set_all( "ä");
    table.create_object().set_all( "Ä");
    table.create_object().set_all( "ǯ");
    table.create_object().set_all( "Ǯ");
    table.create_object().set_all( "ƹ");
    table.create_object().set_all( "Ƹ");
    table.create_object().set_all( "ƺ");
    table.create_object().set_all( "ø");
    table.create_object().set_all( "Ø");
    table.create_object().set_all( "ö");
    table.create_object().set_all( "Ö");
    table.create_object().set_all( "ő");
    table.create_object().set_all( "Ő");
    table.create_object().set_all( "å");
    table.create_object().set_all( "Å");
    table.create_object().set_all( "ƾ");
    table.create_object().set_all( "ɂ");
    table.create_object().set_all( "Ɂ");

    // Core-only is default comparer
    TableView v1 = table.where().find_all();
    TableView v2 = table.where().find_all();

    v2.sort(col);

#ifdef LEGACY_TESTS   // FIXME
    // FIXME: The following checks fail
    for (size_t t = 0; t < v1.size(); t++) {
        CHECK_EQUAL(v1.get_object(t).get_key(), v2.get_object(t).get_key());
    }
#endif

    // Set back to default in case other tests rely on this
    set_string_compare_method(STRING_COMPARE_CORE, nullptr);
}


// Verify that copy-constructed and copy-assigned TableViews work normally.
TEST(TableView_Copy)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");

    table.create_object().set(col_id, -1);
    ObjKey k1 = table.create_object().set(col_id, 1).get_key();
    ObjKey k2 = table.create_object().set(col_id, 2).get_key();

    TableView tv = (table.column<Int>(col_id) > 0).find_all();
    CHECK_EQUAL(2, tv.size());

    TableView copy_1(tv);
    TableView copy_2;
    copy_2 = tv;

    CHECK_EQUAL(2, copy_1.size());
    CHECK_EQUAL(k1, copy_1.get_key(0));
    CHECK_EQUAL(k2, copy_1.get_key(1));

    CHECK_EQUAL(2, copy_2.size());
    CHECK_EQUAL(k1, copy_2.get_key(0));
    CHECK_EQUAL(k2, copy_2.get_key(1));

    table.remove_object(k1);

    CHECK(!copy_1.is_in_sync());
    CHECK(!copy_2.is_in_sync());

    copy_1.sync_if_needed();
    CHECK_EQUAL(1, copy_1.size());
    CHECK_EQUAL(k2, copy_1.get_key(0));

    copy_2.sync_if_needed();
    CHECK_EQUAL(1, copy_2.size());
    CHECK_EQUAL(k2, copy_2.get_key(0));
}

TEST(TableView_RemoveColumnsAfterSort)
{
    Table table;
    auto col_str0 = table.add_column(type_String, "0");
    auto col_str1 = table.add_column(type_String, "1");
    auto col_int = table.add_column(type_Int, "value");
    for (int i = 0; i < 10; ++i) {
        table.create_object().set(col_int, i);
    }

    SortDescriptor desc({{col_int}}, {false}); // sort by the one column in descending order

    table.remove_column(col_str0);
    auto tv = table.get_sorted_view(desc);
    CHECK_EQUAL(tv.get(0).get<Int>(col_int), 9);
    CHECK_EQUAL(tv.get(9).get<Int>(col_int), 0);

    table.remove_column(col_str1);
    table.create_object();
    tv.sync_if_needed();
    CHECK_EQUAL(tv.get(0).get<Int>(col_int), 9);
    CHECK_EQUAL(tv.get(10).get<Int>(col_int), 0);
}

TEST(TableView_TimestampMaxRemoveRow)
{
    Table table;
    auto col_date = table.add_column(type_Timestamp, "time");
    for (size_t i = 0; i < 10; ++i) {
        table.create_object().set(col_date, Timestamp(i, 0));
    }

    TableView tv = table.where().find_all();
    CHECK_EQUAL(tv.size(), 10);
    CHECK_EQUAL(tv.maximum_timestamp(col_date), Timestamp(9, 0));

    table.remove_object(ObjKey(9));
    CHECK_EQUAL(tv.size(), 10);                            // not changed since sync_if_needed hasn't been called
    CHECK_EQUAL(tv.maximum_timestamp(col_date), Timestamp(8, 0)); // but aggregate functions skip removed rows

    tv.sync_if_needed();
    CHECK_EQUAL(tv.size(), 9);
    CHECK_EQUAL(tv.maximum_timestamp(col_date), Timestamp(8, 0));
}

TEST(TableView_FindAll)
{
    Table t;
    auto col_str = t.add_column(type_String, "strings");
    auto col_int = t.add_column(type_Int, "integers");

    ObjKey k0 = t.create_object().set_all("hello", 1).get_key();
    ObjKey k1 = t.create_object().set_all("world", 2).get_key();
    ObjKey k2 = t.create_object().set_all("hello", 3).get_key();
    ObjKey k3 = t.create_object().set_all("world", 4).get_key();
    ObjKey k4 = t.create_object().set_all("hello", 5).get_key();

    ConstTableView tv = t.where().find_all();

    ObjKey j = tv.find_first<Int>(col_int, 4);
    CHECK_EQUAL(j, k3);
    ObjKey k = tv.find_first<String>(col_str, "world");
    CHECK_EQUAL(k, k1);
    auto tv1 = tv.find_all<String>(col_str, "hello");
    CHECK_EQUAL(tv1.size(), 3);
    CHECK_EQUAL(tv1.get_key(0), k0);
    CHECK_EQUAL(tv1.get_key(1), k2);
    CHECK_EQUAL(tv1.get_key(2), k4);
}

#endif // TEST_TABLE_VIEW
