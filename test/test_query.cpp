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
#ifdef TEST_QUERY

#include <cstdlib> // itoa()
#include <initializer_list>
#include <limits>
#include <vector>

#include <realm.hpp>
#include <realm/column_integer.hpp>
#include <realm/array_bool.hpp>
#include <realm/history.hpp>
#include <realm/query_expression.hpp>
#include <realm/index_string.hpp>

#include "test.hpp"
#include "test_table_helper.hpp"

using namespace realm;
using namespace realm::util;
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


TEST(Query_NoConditions)
{
    Table table;
    table.add_column(type_Int, "i");
    {
        Query query(table.where());
        CHECK_EQUAL(null_key, query.find());
    }
    {
        Query query = table.where();
        CHECK_EQUAL(null_key, query.find());
    }
    table.create_object(ObjKey(5));
    {
        Query query(table.where());
        CHECK_EQUAL(5, query.find().value);
    }
    {
        Query query = table.where();
        CHECK_EQUAL(5, query.find().value);
    }
}


TEST(Query_Count)
{
    // Intended to test QueryState::match<pattern = true>(); which is only triggered if:
    // * Table size is large enough to have SSE-aligned or bithack-aligned rows (this requires
    //   REALM_MAX_BPNODE_SIZE > [some large number]!)
    // * You're doing a 'count' which is currently the only operation that uses 'pattern', and
    // * There exists exactly 1 condition (if there is 0 conditions, it will fallback to column::count
    //   and if there exists > 1 conditions, 'pattern' is currently not supported - but could easily be
    //   extended to support it)

    Random random(random_int<unsigned long>()); // Seed from slow global generator
    for (int j = 0; j < 100; j++) {
        Table table;
        auto col_ndx = table.add_column(type_Int, "i");

        size_t matching = 0;
        size_t not_matching = 0;
        size_t rows = random.draw_int_mod(5 * REALM_MAX_BPNODE_SIZE); // to cross some leaf boundaries

        for (size_t i = 0; i < rows; ++i) {
            int64_t val = random.draw_int_mod(5);
            table.create_object().set(col_ndx, val);
            if (val == 2)
                matching++;
            else
                not_matching++;
        }

        CHECK_EQUAL(matching, table.where().equal(col_ndx, 2).count());
        CHECK_EQUAL(not_matching, table.where().not_equal(col_ndx, 2).count());
    }
}

TEST(Query_NextGenSyntaxTypedString)
{
    Table books;
    books.add_column(type_String, "1");
    auto c1 = books.add_column(type_String, "2");
    auto c2 = books.add_column(type_Int, "3");

    Obj obj1 = books.create_object().set_all("Computer Architecture and Organization", "B. Govindarajalu", 752);
    Obj obj2 = books.create_object().set_all("Introduction to Quantum Mechanics", "David Griffiths", 480);
    Obj obj3 = books.create_object().set_all("Biophysics: Searching for Principles", "William Bialek", 640);

    // Typed table:
    Query q = books.column<Int>(c2) >= 200 && books.column<String>(c1) == "David Griffiths";
    auto match = q.find();
    CHECK_EQUAL(obj2.get_key(), match);
    // You don't need to create a query object first:
    match = (books.column<Int>(c2) >= 200 && books.column<String>(c1) == "David Griffiths").find();
    CHECK_EQUAL(obj2.get_key(), match);

    // You can also create column objects and use them in expressions:
    Columns<Int> pages = books.column<Int>(c2);
    Columns<String> author = books.column<String>(c1);
    match = (pages >= 200 && author == "David Griffiths").find();
    CHECK_EQUAL(obj2.get_key(), match);
}


TEST(Query_NextGenSyntax)
{
    ObjKey match;

    // Setup untyped table
    Table untyped;
    auto c0 = untyped.add_column(type_Int, "firs1");
    auto c1 = untyped.add_column(type_Float, "second");
    auto c2 = untyped.add_column(type_Double, "third");
    auto c3 = untyped.add_column(type_Bool, "third2");
    auto c4 = untyped.add_column(type_String, "fourth");
    ObjKey k0 = untyped.create_object().set_all(20, 19.9f, 3.0, true, "hello").get_key();
    ObjKey k1 = untyped.create_object().set_all(20, 20.1f, 4.0, false, "world").get_key();

    match = (untyped.column<String>(c4) == "world").find();
    CHECK_EQUAL(match, k1);

    match = ("world" == untyped.column<String>(c4)).find();
    CHECK_EQUAL(match, k1);

    match = ("hello" != untyped.column<String>(c4)).find();
    CHECK_EQUAL(match, k1);

    match = (!("hello" == untyped.column<String>(c4))).find();
    CHECK_EQUAL(match, k1);

    match = (untyped.column<String>(c4) != StringData("hello")).find();
    CHECK_EQUAL(match, k1);

    match = (!(untyped.column<String>(c4) == StringData("hello"))).find();
    CHECK_EQUAL(match, k1);

    match = (!(!(untyped.column<String>(c4) != StringData("hello")))).find();
    CHECK_EQUAL(match, k1);


    // This is a demonstration of fallback to old query_engine for the specific cases where it's possible
    // because old engine is faster. This will return a ->less(...) query
    match = (untyped.column<int64_t>(c0) == untyped.column<int64_t>(c0)).find();
    CHECK_EQUAL(match, k0);


    match = (untyped.column<bool>(c3) == false).find();
    CHECK_EQUAL(match, k1);

    match = (20.3 > untyped.column<double>(c2) + 2).find();
    CHECK_EQUAL(match, k0);


    match = (untyped.column<int64_t>(c0) > untyped.column<int64_t>(c0)).find();
    CHECK_EQUAL(match, realm::null_key);

    // Left condition makes first row non-match
    match = (untyped.column<float>(c1) + 1 > 21 && untyped.column<double>(c2) > 2).find();
    CHECK_EQUAL(match, k1);

    // Right condition makes first row a non-match
    match = (untyped.column<float>(c1) > 10 && untyped.column<double>(c2) > 3.5).find();
    CHECK_EQUAL(match, k1);

    // Both make first row match
    match = (untyped.column<float>(c1) < 20 && untyped.column<double>(c2) > 2).find();
    CHECK_EQUAL(match, k0);

    // Both make first row non-match
    match = (untyped.column<float>(c1) > 20 && untyped.column<double>(c2) > 3.5).find();
    CHECK_EQUAL(match, k1);

    // Left cond match 0, right match 1
    match = (untyped.column<float>(c1) < 20 && untyped.column<double>(c2) > 3.5).find();
    CHECK_EQUAL(match, realm::null_key);

    // Left match 1, right match 0
    match = (untyped.column<float>(c1) > 20 && untyped.column<double>(c2) < 3.5).find();
    CHECK_EQUAL(match, realm::null_key);

    // Untyped ||

    // Left match 0
    match = (untyped.column<float>(c1) < 20 || untyped.column<double>(c2) < 3.5).find();
    CHECK_EQUAL(match, k0);

    // Right match 0
    match = (untyped.column<float>(c1) > 20 || untyped.column<double>(c2) < 3.5).find();
    CHECK_EQUAL(match, k0);

    // Left match 1

    match = (untyped.column<float>(c1) > 20 || untyped.column<double>(c2) > 9.5).find();

    CHECK_EQUAL(match, k1);

    Query q4 = untyped.column<float>(c1) + untyped.column<int64_t>(c0) > 40;


    Query q5 = 20 < untyped.column<float>(c1);

    match = q4.and_query(q5).find();
    CHECK_EQUAL(match, k1);


    // Untyped, direct column addressing
    Value<int64_t> uv1(1);

    Columns<float> uc1 = untyped.column<float>(c1);

    Query q2 = uv1 <= uc1;
    match = q2.find();
    CHECK_EQUAL(match, k0);


    Query q0 = uv1 <= uc1;
    match = q0.find();
    CHECK_EQUAL(match, k0);

    Query q99 = uv1 <= untyped.column<float>(c1);
    match = q99.find();
    CHECK_EQUAL(match, k0);


    Query q8 = 1 > untyped.column<float>(c1) + 5;
    match = q8.find();
    CHECK_EQUAL(match, null_key);

    Query q3 = untyped.column<float>(c1) + untyped.column<int64_t>(c0) > 10 + untyped.column<int64_t>(c0);
    match = q3.find();

    match = q2.find();
    CHECK_EQUAL(match, k0);

    match = (untyped.column<int64_t>(c0) + untyped.column<float>(c1) > 40).find();
    CHECK_EQUAL(match, k1);

    match = (untyped.column<int64_t>(c0) + untyped.column<float>(c1) < 40).find();
    CHECK_EQUAL(match, k0);

    match = (untyped.column<float>(c1) <= untyped.column<int64_t>(c0)).find();
    CHECK_EQUAL(match, k0);

    match = (untyped.column<int64_t>(c0) + untyped.column<float>(c1) >=
             untyped.column<int64_t>(c0) + untyped.column<float>(c1))
                .find();
    CHECK_EQUAL(match, k0);

    // Untyped, column objects
    Columns<int64_t> u0 = untyped.column<int64_t>(c0);
    Columns<float> u1 = untyped.column<float>(c1);

    match = (u0 + u1 > 40).find();
    CHECK_EQUAL(match, k1);
}

/*
This tests the new string conditions now available for the expression syntax.

Null behaviour (+ means concatenation):

If A + B == B, then A is a prefix of B, and B is a suffix of A. This is valid for any A and B, including null and
empty strings. Some examples:

1)    "" both begins with null and ends with null and contains null.
2)    "foobar" begins with null, ends with null and contains null.
3)    "foobar" begins with "", ends with "" and contains ""
4)    null does not contain, begin with, or end with ""
5)    null contains null, begins with null and ends with null

See TEST(StringData_Substrings) for more unit tests for null, isolated to using only StringData class with no
columns or queries involved
*/


TEST(Query_NextGen_StringConditions)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    auto col_str1 = table1->add_column(type_String, "str1");
    auto col_str2 = table1->add_column(type_String, "str2");

    // add some rows
    ObjKey key_1_0 = table1->create_object().set_all("foo", "F").get_key();
    table1->create_object().set_all("!", "x").get_key();
    ObjKey key_1_2 = table1->create_object().set_all("bar", "r").get_key();

    ObjKey m;
    // Equal
    m = table1->column<String>(col_str1).equal("bar", false).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).equal("bar", true).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).equal("Bar", true).find();
    CHECK_EQUAL(m, null_key);

    m = table1->column<String>(col_str1).equal("Bar", false).find();
    CHECK_EQUAL(m, key_1_2);

    // Contains
    m = table1->column<String>(col_str1).contains("a", false).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).contains("a", true).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).contains("A", true).find();
    CHECK_EQUAL(m, null_key);

    m = table1->column<String>(col_str1).contains("A", false).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).contains(table1->column<String>(col_str2), false).find();
    CHECK_EQUAL(m, key_1_0);

    m = table1->column<String>(col_str1).contains(table1->column<String>(col_str2), true).find();
    CHECK_EQUAL(m, key_1_2);

    // Begins with
    m = table1->column<String>(col_str1).begins_with("b", false).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).begins_with("b", true).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).begins_with("B", true).find();
    CHECK_EQUAL(m, null_key);

    m = table1->column<String>(col_str1).begins_with("B", false).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).begins_with(table1->column<String>(col_str2), false).find();
    CHECK_EQUAL(m, key_1_0);

    m = table1->column<String>(col_str1).begins_with(table1->column<String>(col_str2), true).find();
    CHECK_EQUAL(m, null_key);

    // Ends with
    m = table1->column<String>(col_str1).ends_with("r", false).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).ends_with("r", true).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).ends_with("R", true).find();
    CHECK_EQUAL(m, null_key);

    m = table1->column<String>(col_str1).ends_with("R", false).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).ends_with(table1->column<String>(col_str2), false).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).ends_with(table1->column<String>(col_str2), true).find();
    CHECK_EQUAL(m, key_1_2);

    // Like (wildcard matching)
    m = table1->column<String>(col_str1).like("b*", true).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).like("b*", false).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).like("*r", false).find();
    CHECK_EQUAL(m, key_1_2);

    m = table1->column<String>(col_str1).like("f?o", false).find();
    CHECK_EQUAL(m, key_1_0);

    m = (table1->column<String>(col_str1).like("f*", false) && table1->column<String>(col_str1) == "foo").find();
    CHECK_EQUAL(m, key_1_0);

    m = table1->column<String>(col_str1).like(table1->column<String>(col_str2), true).find();
    CHECK_EQUAL(m, null_key);

    // Test various compare operations with null
    TableRef table2 = group.add_table("table2");
    auto col_str3 = table2->add_column(type_String, "str3", true);

    ObjKey key_2_0 = table2->create_object().set(col_str3, "foo").get_key();
    ObjKey key_2_1 = table2->create_object().set(col_str3, "!").get_key();
    ObjKey key_2_2 = table2->create_object().get_key(); // null
    ObjKey key_2_3 = table2->create_object().set(col_str3, "bar").get_key();
    ObjKey key_2_4 = table2->create_object().set(col_str3, "").get_key();

    size_t cnt;
    cnt = table2->column<String>(col_str3).contains(StringData("")).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table2->column<String>(col_str3).begins_with(StringData("")).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table2->column<String>(col_str3).ends_with(StringData("")).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table2->column<String>(col_str3).equal(StringData("")).count();
    CHECK_EQUAL(cnt, 1);

    cnt = table2->column<String>(col_str3).not_equal(StringData("")).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table2->column<String>(col_str3).equal(realm::null()).count();
    CHECK_EQUAL(cnt, 1);

    cnt = table2->column<String>(col_str3).not_equal(realm::null()).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table2->column<String>(col_str3).contains(realm::null()).count();
    CHECK_EQUAL(cnt, 5);

    cnt = table2->column<String>(col_str3).like(realm::null()).count();
    CHECK_EQUAL(cnt, 1);

    cnt = table2->column<String>(col_str3).contains(StringData(""), false).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table2->column<String>(col_str3).like(StringData(""), false).count();
    CHECK_EQUAL(cnt, 1);

    cnt = table2->column<String>(col_str3).begins_with(StringData(""), false).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table2->column<String>(col_str3).ends_with(StringData(""), false).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table2->column<String>(col_str3).equal(StringData(""), false).count();
    CHECK_EQUAL(cnt, 1);

    cnt = table2->column<String>(col_str3).not_equal(StringData(""), false).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table2->column<String>(col_str3).equal(realm::null(), false).count();
    CHECK_EQUAL(cnt, 1);

    cnt = table2->column<String>(col_str3).not_equal(realm::null(), false).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table2->column<String>(col_str3).contains(realm::null(), false).count();
    CHECK_EQUAL(cnt, 5);

    cnt = table2->column<String>(col_str3).like(realm::null(), false).count();
    CHECK_EQUAL(cnt, 1);

    TableRef table3 = group.add_table(StringData("table3"));
    auto col_link1 = table3->add_column_link(type_Link, "link1", *table2);

    table3->create_object().set(col_link1, key_2_0);
    table3->create_object().set(col_link1, key_2_1);
    table3->create_object().set(col_link1, key_2_2);
    table3->create_object().set(col_link1, key_2_3);
    table3->create_object().set(col_link1, key_2_4);

    cnt = table3->link(col_link1).column<String>(col_str3).contains(StringData("")).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table3->link(col_link1).column<String>(col_str3).begins_with(StringData("")).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table3->link(col_link1).column<String>(col_str3).ends_with(StringData("")).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table3->link(col_link1).column<String>(col_str3).equal(StringData("")).count();
    CHECK_EQUAL(cnt, 1);

    cnt = table3->link(col_link1).column<String>(col_str3).not_equal(StringData("")).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table3->link(col_link1).column<String>(col_str3).equal(realm::null()).count();
    CHECK_EQUAL(cnt, 1);

    cnt = table3->link(col_link1).column<String>(col_str3).not_equal(realm::null()).count();
    CHECK_EQUAL(cnt, 4);


    cnt = table3->link(col_link1).column<String>(col_str3).contains(StringData(""), false).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table3->link(col_link1).column<String>(col_str3).like(StringData(""), false).count();
    CHECK_EQUAL(cnt, 1);

    cnt = table3->link(col_link1).column<String>(col_str3).begins_with(StringData(""), false).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table3->link(col_link1).column<String>(col_str3).ends_with(StringData(""), false).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table3->link(col_link1).column<String>(col_str3).equal(StringData(""), false).count();
    CHECK_EQUAL(cnt, 1);

    cnt = table3->link(col_link1).column<String>(col_str3).not_equal(StringData(""), false).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table3->link(col_link1).column<String>(col_str3).equal(realm::null(), false).count();
    CHECK_EQUAL(cnt, 1);

    cnt = table3->link(col_link1).column<String>(col_str3).not_equal(realm::null(), false).count();
    CHECK_EQUAL(cnt, 4);

    cnt = table3->link(col_link1).column<String>(col_str3).contains(realm::null(), false).count();
    CHECK_EQUAL(cnt, 4);

    // Test long string contains search (where needle is longer than 255 chars)
    const char long_string[] = "This is a long search string that does not contain the word being searched for!, "
                               "This is a long search string that does not contain the word being searched for!, "
                               "This is a long search string that does not contain the word being searched for!, "
                               "This is a long search string that does not contain the word being searched for!, "
                               "This is a long search string that does not contain the word being searched for!, "
                               "This is a long search string that does not contain the word being searched for!, "
                               "This is a long search string that does not contain the word being searched for!, "
                               "This is a long search string that does not contain the word being searched for!, "
                               "This is a long search string that does not contain the word being searched for!, "
                               "needle, "
                               "This is a long search string that does not contain the word being searched for!, "
                               "This is a long search string that does not contain the word being searched for!";
    const char search_1[] = "This is a long search string that does not contain the word being searched for!, "
                            "This is a long search string that does not contain the word being searched for!, "
                            "This is a long search string that does not contain the word being searched for!, "
                            "This is a long search string that does not contain the word being searched for!, "
                            "This is a long search string that does not contain the word being searched for!, "
                            "This is a long search string that does not contain the word being searched for!, "
                            "NEEDLE";
    const char search_2[] = "This is a long search string that does not contain the word being searched for!, "
                            "This is a long search string that does not contain the word being searched for!, "
                            "This is a long search string that does not contain the word being searched for!, "
                            "This is a long search string that does not contain the word being searched for!, "
                            "This is a long search string that does not contain the word being searched for!, "
                            "This is a long search string that does not contain the word being searched for!, "
                            "needle";
    table2->create_object().set(col_str3, long_string).get_key();

    cnt = table2->column<String>(col_str3).contains(search_1, false).count();
    CHECK_EQUAL(cnt, 1);

    cnt = table2->column<String>(col_str3).contains(search_2, true).count();
    CHECK_EQUAL(cnt, 1);

    cnt = table3->link(col_link1).column<String>(col_str3).like(realm::null(), false).count();
    CHECK_EQUAL(cnt, 1);
}

TEST(Query_NextGenSyntaxMonkey0)
{
    // Intended to test eval() for columns in query_expression.hpp which fetch 8 values at a time. This test varies
    // table size to test out-of-bounds bugs.

    Random random(random_int<unsigned long>()); // Seed from slow global generator
    for (int iter = 1; iter < 10 + TEST_DURATION * 1000; iter++) {
        const size_t rows = 1 + random.draw_int_mod(2 * REALM_MAX_BPNODE_SIZE);
        Table table;

        // Two different row types prevents fallback to query_engine (good because we want to test query_expression)
        auto col_int = table.add_column(type_Int, "first");
        auto col_float = table.add_column(type_Float, "second");
        auto col_str = table.add_column(type_String, "third");

        for (size_t r = 0; r < rows; r++) {
            Obj obj = table.create_object();
            // using '% iter' tests different bitwidths
            obj.set(col_int, random.draw_int_mod(iter));
            obj.set(col_float, float(random.draw_int_mod(iter)));
            if (random.draw_bool())
                obj.set(col_str, "a");
            else
                obj.set(col_str, "b");
        }

        size_t tvpos;

        realm::Query q =
            table.column<Int>(col_int) > table.column<Float>(col_float) && table.column<String>(col_str) == "a";

        // without start or limit
        realm::TableView tv = q.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int) > o.get<Float>(col_float) && o.get<String>(col_str) == "a") {
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv.size());

        tvpos = 0;

        // with start and limit
        size_t start = random.draw_int_mod(rows);
        size_t limit = random.draw_int_mod(rows);
        tv = q.find_all(start, size_t(-1), limit);
        tvpos = 0;
        size_t r = 0;
        for (Obj o : table) {
            if (r >= start && tvpos < limit && o.get<Int>(col_int) > o.get<Float>(col_float) &&
                o.get<String>(col_str) == "a") {
                tvpos++;
            }
            r++;
        }
        CHECK_EQUAL(tvpos, tv.size());
    }
}

TEST(Query_NextGenSyntaxMonkey)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    for (int iter = 1; iter < 5 * (TEST_DURATION * TEST_DURATION * TEST_DURATION + 1); iter++) {
        // Set 'rows' to at least '* 20' else some tests will give 0 matches and bad coverage
        const size_t rows = 1 +
                            random.draw_int_mod<size_t>(REALM_MAX_BPNODE_SIZE * 20 *
                                                        (TEST_DURATION * TEST_DURATION * TEST_DURATION + 1));
        Table table;
        auto col_int0 = table.add_column(type_Int, "first");
        auto col_int1 = table.add_column(type_Int, "second");
        auto col_int2 = table.add_column(type_Int, "third");

        for (size_t r = 0; r < rows; r++) {
            Obj obj = table.create_object();
            // using '% iter' tests different bitwidths
            obj.set(col_int0, random.draw_int_mod(iter));
            obj.set(col_int1, random.draw_int_mod(iter));
            obj.set(col_int2, random.draw_int_mod(iter));
        }

        size_t tvpos;

        // second == 1
        realm::Query q1_0 = table.where().equal(col_int1, 1);
        realm::Query q2_0 = table.column<int64_t>(col_int1) == 1;
        realm::TableView tv_0 = q2_0.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int1) == 1) {
                CHECK_EQUAL(o.get_key(), tv_0.get_key(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv_0.size());

        // (first == 0 || first == 1) && second == 1
        realm::Query q2_1 = (table.column<int64_t>(col_int0) == 0 || table.column<int64_t>(col_int0) == 1) &&
                            table.column<int64_t>(col_int1) == 1;
        realm::TableView tv_1 = q2_1.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if ((o.get<Int>(col_int0) == 0 || o.get<Int>(col_int0) == 1) && o.get<Int>(col_int1) == 1) {
                CHECK_EQUAL(o.get_key(), tv_1.get_key(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv_1.size());

        // first == 0 || (first == 1 && second == 1)
        realm::Query q2_2 = table.column<int64_t>(col_int0) == 0 ||
                            (table.column<int64_t>(col_int0) == 1 && table.column<int64_t>(col_int1) == 1);
        realm::TableView tv_2 = q2_2.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int0) == 0 || (o.get<Int>(col_int0) == 1 && o.get<Int>(col_int1) == 1)) {
                CHECK_EQUAL(o.get_key(), tv_2.get_key(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv_2.size());


        // second == 0 && (first == 0 || first == 2)
        realm::Query q4_8 = table.column<int64_t>(col_int1) == 0 &&
                            (table.column<int64_t>(col_int0) == 0 || table.column<int64_t>(col_int0) == 2);
        realm::TableView tv_8 = q4_8.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int1) == 0 && ((o.get<Int>(col_int0) == 0) || o.get<Int>(col_int0) == 2)) {
                CHECK_EQUAL(o.get_key(), tv_8.get_key(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv_8.size());


        // (first == 0 || first == 2) && (first == 1 || second == 1)
        realm::Query q3_7 = (table.column<int64_t>(col_int0) == 0 || table.column<int64_t>(col_int0) == 2) &&
                            (table.column<int64_t>(col_int0) == 1 || table.column<int64_t>(col_int1) == 1);
        realm::TableView tv_7 = q3_7.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if ((o.get<Int>(col_int0) == 0 || o.get<Int>(col_int0) == 2) &&
                (o.get<Int>(col_int0) == 1 || o.get<Int>(col_int1) == 1)) {
                CHECK_EQUAL(o.get_key(), tv_7.get_key(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv_7.size());


        // (first == 0 || first == 2) || (first == 1 || second == 1)
        realm::Query q4_7 = (table.column<int64_t>(col_int0) == 0 || table.column<int64_t>(col_int0) == 2) ||
                            (table.column<int64_t>(col_int0) == 1 || table.column<int64_t>(col_int1) == 1);
        realm::TableView tv_10 = q4_7.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if ((o.get<Int>(col_int0) == 0 || o.get<Int>(col_int0) == 2) ||
                (o.get<Int>(col_int0) == 1 || o.get<Int>(col_int1) == 1)) {
                CHECK_EQUAL(o.get_key(), tv_10.get_key(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv_10.size());


        TableView tv;

        // first == 0 || first == 2 || first == 1 || second == 1
        realm::Query q20 = table.column<int64_t>(col_int0) == 0 || table.column<int64_t>(col_int0) == 2 ||
                           table.column<int64_t>(col_int0) == 1 || table.column<int64_t>(col_int1) == 1;
        tv = q20.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int0) == 0 || o.get<Int>(col_int0) == 2 || o.get<Int>(col_int0) == 1 ||
                o.get<Int>(col_int1) == 1) {
                CHECK_EQUAL(o.get_key(), tv.get_key(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv.size());


        // first * 2 > second / 2 + third + 1
        realm::Query q21 = table.column<int64_t>(col_int0) * 2 >
                           table.column<int64_t>(col_int1) / 2 + table.column<int64_t>(col_int2) + 1;
        tv = q21.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int0) * 2 > o.get<Int>(col_int1) / 2 + o.get<Int>(col_int2) + 1) {
                CHECK_EQUAL(o.get_key(), tv.get_key(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv.size());

        // first * 2 > second / 2 + third + 1 + third - third + third - third + third - third + third - third + third
        // - third
        realm::Query q22 = table.column<int64_t>(col_int0) * 2 >
                           table.column<int64_t>(col_int1) / 2 + table.column<int64_t>(col_int2) + 1 +
                               table.column<int64_t>(col_int2) - table.column<int64_t>(col_int2) +
                               table.column<int64_t>(col_int2) - table.column<int64_t>(col_int2) +
                               table.column<int64_t>(col_int2) - table.column<int64_t>(col_int2) +
                               table.column<int64_t>(col_int2) - table.column<int64_t>(col_int2) +
                               table.column<int64_t>(col_int2) - table.column<int64_t>(col_int2);
        tv = q22.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int0) * 2 > o.get<Int>(col_int1) / 2 + o.get<Int>(col_int2) + 1) {
                CHECK_EQUAL(o.get_key(), tv.get_key(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv.size());
    }
}


TEST(Query_MergeQueriesOverloads)
{
    // Tests && and || overloads of Query class
    Table table;
    auto col_int0 = table.add_column(type_Int, "first");
    auto col_int1 = table.add_column(type_Int, "second");

    table.create_object().set_all(20, 20);
    table.create_object().set_all(20, 30);
    table.create_object().set_all(30, 30);

    size_t c;

    // q1_0 && q2_0
    realm::Query q1_110 = table.where().equal(col_int0, 20);
    realm::Query q2_110 = table.where().equal(col_int1, 30);
    realm::Query q3_110 = q1_110.and_query(q2_110);
    c = q1_110.count();
    c = q2_110.count();
    c = q3_110.count();


    // The overloads must behave such as if each side of the operator is inside parentheses, that is,
    // (first == 1 || first == 20) operator&& (second == 30), regardless of order of operands

    // q1_0 && q2_0
    realm::Query q1_0 = table.where().equal(col_int0, 10).Or().equal(col_int0, 20);
    realm::Query q2_0 = table.where().equal(col_int1, 30);
    realm::Query q3_0 = q1_0 && q2_0;
    c = q3_0.count();
    CHECK_EQUAL(1, c);

    // q2_0 && q1_0 (reversed operand order)
    realm::Query q1_1 = table.where().equal(col_int0, 10).Or().equal(col_int0, 20);
    realm::Query q2_1 = table.where().equal(col_int1, 30);
    c = q1_1.count();

    realm::Query q3_1 = q2_1 && q1_1;
    c = q3_1.count();
    CHECK_EQUAL(1, c);

    // Short test for ||
    realm::Query q1_2 = table.where().equal(col_int0, 10);
    realm::Query q2_2 = table.where().equal(col_int1, 30);
    realm::Query q3_2 = q2_2 || q1_2;
    c = q3_2.count();
    CHECK_EQUAL(2, c);
}


TEST(Query_MergeQueries)
{
    // test OR vs AND precedence
    Table table;
    auto col_int0 = table.add_column(type_Int, "first");
    auto col_int1 = table.add_column(type_Int, "second");

    table.create_object().set_all(10, 20);
    table.create_object().set_all(20, 30);
    table.create_object().set_all(30, 20);

    // Must evaluate as if and_query is inside paranthesis, that is, (first == 10 || first == 20) && second == 30
    realm::Query q1_0 = table.where().equal(col_int0, 10).Or().equal(col_int0, 20);
    realm::Query q2_0 = table.where().and_query(q1_0).equal(col_int1, 30);

    size_t c = q2_0.count();
    CHECK_EQUAL(1, c);
}

TEST(Query_Not)
{
    // test Not vs And, Or, Groups.
    Table table;
    auto col_int0 = table.add_column(type_Int, "first");

    table.create_object().set(col_int0, 10);
    table.create_object().set(col_int0, 20);
    table.create_object().set(col_int0, 30);

    // should apply not to single term, leading to query "not A" with two matching entries:
    realm::Query q0 = table.where().Not().equal(col_int0, 10);
    CHECK_EQUAL(2, q0.count());

    // grouping, after not
    realm::Query q0b = table.where().Not().group().equal(col_int0, 10).end_group();
    CHECK_EQUAL(2, q0b.count());

    // grouping, surrounding not
    realm::Query q0c = table.where().group().Not().equal(col_int0, 10).end_group();
    CHECK_EQUAL(2, q0c.count());

    // nested nots (implicit grouping)
    realm::Query q0d = table.where().Not().Not().equal(col_int0, 10);
    CHECK_EQUAL(1, q0d.count()); // FAILS

    realm::Query q0e = table.where().Not().Not().Not().equal(col_int0, 10);
    CHECK_EQUAL(2, q0e.count()); // FAILS

    // just checking the above
    realm::Query q0f = table.where().Not().not_equal(col_int0, 10);
    CHECK_EQUAL(1, q0f.count());

    realm::Query q0g = table.where().Not().Not().not_equal(col_int0, 10);
    CHECK_EQUAL(2, q0g.count()); // FAILS

    realm::Query q0h = table.where().not_equal(col_int0, 10);
    CHECK_EQUAL(2, q0h.count());

    // should apply not to first term, leading to query "not A and A", which is obviously empty:
    realm::Query q1 = table.where().Not().equal(col_int0, 10).equal(col_int0, 10);
    CHECK_EQUAL(0, q1.count());

    // should apply not to first term, leading to query "not A and A", which is obviously empty:
    realm::Query q1b = table.where().group().Not().equal(col_int0, 10).end_group().equal(col_int0, 10);
    CHECK_EQUAL(0, q1b.count());

    // should apply not to first term, leading to query "not A and A", which is obviously empty:
    realm::Query q1c = table.where().Not().group().equal(col_int0, 10).end_group().equal(col_int0, 10);
    CHECK_EQUAL(0, q1c.count());


    // should apply not to second term, leading to query "A and not A", which is obviously empty:
    realm::Query q2 = table.where().equal(col_int0, 10).Not().equal(col_int0, 10);
    CHECK_EQUAL(0, q2.count()); // FAILS

    // should apply not to second term, leading to query "A and not A", which is obviously empty:
    realm::Query q2b = table.where().equal(col_int0, 10).group().Not().equal(col_int0, 10).end_group();
    CHECK_EQUAL(0, q2b.count());

    // should apply not to second term, leading to query "A and not A", which is obviously empty:
    realm::Query q2c = table.where().equal(col_int0, 10).Not().group().equal(col_int0, 10).end_group();
    CHECK_EQUAL(0, q2c.count()); // FAILS


    // should apply not to both terms, leading to query "not A and not A", which has 2 members
    realm::Query q3 = table.where().Not().equal(col_int0, 10).Not().equal(col_int0, 10);
    CHECK_EQUAL(2, q3.count()); // FAILS

    // applying not to an empty query is forbidden
    realm::Query q4 = table.where();
    CHECK_THROW(!q4, std::runtime_error);
}


TEST(Query_MergeQueriesMonkey)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    for (int iter = 0; iter < 5; iter++) {
        const size_t rows = REALM_MAX_BPNODE_SIZE * 4;
        Table table;
        auto col_int0 = table.add_column(type_Int, "first");
        auto col_int1 = table.add_column(type_Int, "second");
        auto col_int2 = table.add_column(type_Int, "third");

        for (size_t r = 0; r < rows; r++) {
            Obj obj = table.create_object();
            obj.set(col_int0, random.draw_int_mod(3));
            obj.set(col_int1, random.draw_int_mod(3));
            obj.set(col_int2, random.draw_int_mod(3));
        }

        size_t tvpos;

        // and_query(second == 1)
        realm::Query q1_0 = table.where().equal(col_int1, 1);
        realm::Query q2_0 = table.where().and_query(q1_0);
        realm::TableView tv_0 = q2_0.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int1) == 1) {
                CHECK_EQUAL(o.get_key(), tv_0.get_key(tvpos));
                tvpos++;
            }
        }

        // (first == 0 || first == 1) && and_query(second == 1)
        realm::Query q1_1 = table.where().equal(col_int1, 1);
        realm::Query q2_1 =
            table.where().group().equal(col_int0, 0).Or().equal(col_int0, 1).end_group().and_query(q1_1);
        realm::TableView tv_1 = q2_1.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if ((o.get<Int>(col_int0) == 0 || o.get<Int>(col_int0) == 1) && o.get<Int>(col_int1) == 1) {
                CHECK_EQUAL(o.get_key(), tv_1.get_key(tvpos));
                tvpos++;
            }
        }

        // first == 0 || (first == 1 && and_query(second == 1))
        realm::Query q1_2 = table.where().equal(col_int1, 1);
        realm::Query q2_2 = table.where().equal(col_int0, 0).Or().equal(col_int0, 1).and_query(q1_2);
        realm::TableView tv_2 = q2_2.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int0) == 0 || (o.get<Int>(col_int0) == 1 && o.get<Int>(col_int1) == 1)) {
                CHECK_EQUAL(o.get_key(), tv_2.get_key(tvpos));
                tvpos++;
            }
        }

        // and_query(first == 0) || (first == 1 && second == 1)
        realm::Query q1_3 = table.where().equal(col_int0, 0);
        realm::Query q2_3 = table.where().and_query(q1_3).Or().equal(col_int0, 1).equal(col_int1, 1);
        realm::TableView tv_3 = q2_3.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int0) == 0 || (o.get<Int>(col_int0) == 1 && o.get<Int>(col_int1) == 1)) {
                CHECK_EQUAL(o.get_key(), tv_3.get_key(tvpos));
                tvpos++;
            }
        }


        // first == 0 || and_query(first == 1 && second == 1)
        realm::Query q2_4 = table.where().equal(col_int0, 1).equal(col_int1, 1);
        realm::Query q1_4 = table.where().equal(col_int0, 0).Or().and_query(q2_4);
        realm::TableView tv_4 = q1_4.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int0) == 0 || (o.get<Int>(col_int0) == 1 && o.get<Int>(col_int1) == 1)) {
                CHECK_EQUAL(o.get_key(), tv_4.get_key(tvpos));
                tvpos++;
            }
        }


        // and_query(first == 0 || first == 2) || and_query(first == 1 && second == 1)
        realm::Query q2_5 = table.where().equal(col_int0, 0).Or().equal(col_int0, 2);
        realm::Query q1_5 = table.where().equal(col_int0, 1).equal(col_int1, 1);
        realm::Query q3_5 = table.where().and_query(q2_5).Or().and_query(q1_5);
        realm::TableView tv_5 = q3_5.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if ((o.get<Int>(col_int0) == 0 || o.get<Int>(col_int0) == 2) ||
                (o.get<Int>(col_int0) == 1 && o.get<Int>(col_int1) == 1)) {
                CHECK_EQUAL(o.get_key(), tv_5.get_key(tvpos));
                tvpos++;
            }
        }


        // and_query(first == 0) && and_query(second == 1)
        realm::Query q1_6 = table.where().equal(col_int0, 0);
        realm::Query q2_6 = table.where().equal(col_int1, 1);
        realm::Query q3_6 = table.where().and_query(q1_6).and_query(q2_6);
        realm::TableView tv_6 = q3_6.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int0) == 0 && o.get<Int>(col_int1) == 1) {
                CHECK_EQUAL(o.get_key(), tv_6.get_key(tvpos));
                tvpos++;
            }
        }

        // and_query(first == 0 || first == 2) && and_query(first == 1 || second == 1)
        realm::Query q2_7 = table.where().equal(col_int0, 0).Or().equal(col_int0, 2);
        realm::Query q1_7 = table.where().equal(col_int0, 1).equal(col_int0, 1).Or().equal(col_int1, 1);
        realm::Query q3_7 = table.where().and_query(q2_7).and_query(q1_7);
        realm::TableView tv_7 = q3_7.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if ((o.get<Int>(col_int0) == 0 || o.get<Int>(col_int0) == 2) &&
                (o.get<Int>(col_int0) == 1 || o.get<Int>(col_int1) == 1)) {
                CHECK_EQUAL(o.get_key(), tv_7.get_key(tvpos));
                tvpos++;
            }
        }

        // Nested and_query

        // second == 0 && and_query(first == 0 || and_query(first == 2))
        realm::Query q2_8 = table.where().equal(col_int0, 2);
        realm::Query q3_8 = table.where().equal(col_int0, 0).Or().and_query(q2_8);
        realm::Query q4_8 = table.where().equal(col_int1, 0).and_query(q3_8);
        realm::TableView tv_8 = q4_8.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int1) == 0 && ((o.get<Int>(col_int0) == 0) || o.get<Int>(col_int0) == 2)) {
                CHECK_EQUAL(o.get_key(), tv_8.get_key(tvpos));
                tvpos++;
            }
        }


        // Nested as above but constructed differently

        // second == 0 && and_query(first == 0 || and_query(first == 2))
        realm::Query q2_9 = table.where().equal(col_int0, 2);
        realm::Query q5_9 = table.where().equal(col_int0, 0);
        realm::Query q3_9 = table.where().and_query(q5_9).Or().and_query(q2_9);
        realm::Query q4_9 = table.where().equal(col_int1, 0).and_query(q3_9);
        realm::TableView tv_9 = q4_9.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int1) == 0 && ((o.get<Int>(col_int0) == 0) || o.get<Int>(col_int0) == 2)) {
                CHECK_EQUAL(o.get_key(), tv_9.get_key(tvpos));
                tvpos++;
            }
        }


        // Nested

        // and_query(and_query(and_query(first == 0)))
        realm::Query q2_10 = table.where().equal(col_int0, 0);
        realm::Query q5_10 = table.where().and_query(q2_10);
        realm::Query q3_10 = table.where().and_query(q5_10);
        realm::Query q4_10 = table.where().and_query(q3_10);
        realm::TableView tv_10 = q4_10.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int0) == 0) {
                CHECK_EQUAL(o.get_key(), tv_10.get_key(tvpos));
                tvpos++;
            }
        }
    }
}

TEST(Query_MergeQueriesMonkeyOverloads)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    for (int iter = 0; iter < 5; iter++) {
        const size_t rows = REALM_MAX_BPNODE_SIZE * 4;
        Table table;
        auto col_int0 = table.add_column(type_Int, "first");
        auto col_int1 = table.add_column(type_Int, "second");
        auto col_int2 = table.add_column(type_Int, "third");

        for (size_t r = 0; r < rows; r++) {
            Obj obj = table.create_object();
            obj.set(col_int0, random.draw_int_mod(3));
            obj.set(col_int1, random.draw_int_mod(3));
            obj.set(col_int2, random.draw_int_mod(3));
        }

        size_t tvpos;

        // Left side of operator&& is empty query
        // and_query(second == 1)
        realm::Query q1_0 = table.where().equal(col_int1, 1);
        realm::Query q2_0 = table.where() && q1_0;
        realm::TableView tv_0 = q2_0.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int1) == 1) {
                CHECK_EQUAL(o.get_key(), tv_0.get_key(tvpos));
                tvpos++;
            }
        }

        // Right side of operator&& is empty query
        // and_query(second == 1)
        realm::Query q1_10 = table.where().equal(col_int1, 1);
        realm::Query q2_10 = q1_10 && table.where();
        realm::TableView tv_10 = q2_10.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int1) == 1) {
                CHECK_EQUAL(o.get_key(), tv_10.get_key(tvpos));
                tvpos++;
            }
        }

        // (first == 0 || first == 1) && and_query(second == 1)
        realm::Query q1_1 = table.where().equal(col_int0, 0);
        realm::Query q2_1 = table.where().equal(col_int0, 1);
        realm::Query q3_1 = q1_1 || q2_1;
        realm::Query q4_1 = table.where().equal(col_int1, 1);
        realm::Query q5_1 = q3_1 && q4_1;

        realm::TableView tv_1 = q5_1.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if ((o.get<Int>(col_int0) == 0 || o.get<Int>(col_int0) == 1) && o.get<Int>(col_int1) == 1) {
                CHECK_EQUAL(o.get_key(), tv_1.get_key(tvpos));
                tvpos++;
            }
        }

        // (first == 0 || first == 1) && and_query(second == 1) as above, written in another way
        realm::Query q1_20 =
            table.where().equal(col_int0, 0).Or().equal(col_int0, 1) && table.where().equal(col_int1, 1);
        realm::TableView tv_20 = q1_20.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if ((o.get<Int>(col_int0) == 0 || o.get<Int>(col_int0) == 1) && o.get<Int>(col_int1) == 1) {
                CHECK_EQUAL(o.get_key(), tv_20.get_key(tvpos));
                tvpos++;
            }
        }

        // and_query(first == 0) || (first == 1 && second == 1)
        realm::Query q1_3 = table.where().equal(col_int0, 0);
        realm::Query q2_3 = table.where().equal(col_int0, 1);
        realm::Query q3_3 = table.where().equal(col_int1, 1);
        realm::Query q4_3 = q1_3 || (q2_3 && q3_3);
        realm::TableView tv_3 = q4_3.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int0) == 0 || (o.get<Int>(col_int0) == 1 && o.get<Int>(col_int1) == 1)) {
                CHECK_EQUAL(o.get_key(), tv_3.get_key(tvpos));
                tvpos++;
            }
        }


        // and_query(first == 0) || (first == 1 && second == 1) written in another way
        realm::Query q1_30 = table.where().equal(col_int0, 0);
        realm::Query q3_30 = table.where().equal(col_int1, 1);
        realm::Query q4_30 = table.where().equal(col_int0, 0) || (table.where().equal(col_int0, 1) && q3_30);
        realm::TableView tv_30 = q4_30.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int0) == 0 || (o.get<Int>(col_int0) == 1 && o.get<Int>(col_int1) == 1)) {
                CHECK_EQUAL(o.get_key(), tv_30.get_key(tvpos));
                tvpos++;
            }
        }
    }
}

TEST(Query_Expressions0)
{
    /*
    We have following variables to vary in the tests:

    left        right
    +           -           *           /          pow
    Subexpr    Column       Value
    >           <           ==          !=          >=          <=
    float       int         double      int64_t

    Many of them are combined and tested together in equality classes below
    */
    Table table;
    auto col_int = table.add_column(type_Int, "first1");
    auto col_float = table.add_column(type_Float, "second1");
    auto col_double = table.add_column(type_Double, "third");


    ObjKey match;

    Columns<int64_t> first = table.column<int64_t>(col_int);
    Columns<float> second = table.column<float>(col_float);
    Columns<double> third = table.column<double>(col_double);

    ObjKey key0 = table.create_object().set_all(20, 19.9f, 3.0).get_key();
    ObjKey key1 = table.create_object().set_all(20, 20.1f, 4.0).get_key();

    /**
    Conversion / promotion
    **/

    // 20 must convert to float
    match = (second + 0.2f > 20).find();
    CHECK_EQUAL(match, key0);

    match = (first >= 20.0f).find();
    CHECK_EQUAL(match, key0);

    // 20.1f must remain float
    match = (first >= 20.1f).find();
    CHECK_EQUAL(match, null_key);

    // first must convert to float
    match = (second >= first).find();
    CHECK_EQUAL(match, key1);

    // 20 and 40 must convert to float
    match = (second + 20 > 40).find();
    CHECK_EQUAL(match, key1);

    // first and 40 must convert to float
    match = (second + first >= 40).find();
    CHECK_EQUAL(match, key1);

    // 20 must convert to float
    match = (0.2f + second > 20).find();
    CHECK_EQUAL(match, key0);

    /**
    Permutations of types (Subexpr, Value, Column) of left/right side
    **/

    // Compare, left = Subexpr, right = Value
    match = (second + first >= 40).find();
    CHECK_EQUAL(match, key1);

    match = (second + first > 40).find();
    CHECK_EQUAL(match, key1);

    match = (first - second < 0).find();
    CHECK_EQUAL(match, key1);

    match = (second - second == 0).find();
    CHECK_EQUAL(match, key0);

    match = (first - second <= 0).find();
    CHECK_EQUAL(match, key1);

    match = (first * first != 400).find();
    CHECK_EQUAL(match, null_key);

    // Compare, left = Column, right = Value
    match = (second >= 20).find();
    CHECK_EQUAL(match, key1);

    match = (second > 20).find();
    CHECK_EQUAL(match, key1);

    match = (second < 20).find();
    CHECK_EQUAL(match, key0);

    match = (second == 20.1f).find();
    CHECK_EQUAL(match, key1);

    match = (second != 19.9f).find();
    CHECK_EQUAL(match, key1);

    match = (second <= 21).find();
    CHECK_EQUAL(match, key0);

    // Compare, left = Column, right = Value
    match = (20 <= second).find();
    CHECK_EQUAL(match, key1);

    match = (20 < second).find();
    CHECK_EQUAL(match, key1);

    match = (20 > second).find();
    CHECK_EQUAL(match, key0);

    match = (20.1f == second).find();
    CHECK_EQUAL(match, key1);

    match = (19.9f != second).find();
    CHECK_EQUAL(match, key1);

    match = (21 >= second).find();
    CHECK_EQUAL(match, key0);

    // Compare, left = Subexpr, right = Value
    match = (40 <= second + first).find();
    CHECK_EQUAL(match, key1);

    match = (40 < second + first).find();
    CHECK_EQUAL(match, key1);

    match = (0 > first - second).find();
    CHECK_EQUAL(match, key1);

    match = (0 == second - second).find();
    CHECK_EQUAL(match, key0);

    match = (0 >= first - second).find();
    CHECK_EQUAL(match, key1);

    match = (400 != first * first).find();
    CHECK_EQUAL(match, null_key);

    // Col compare Col
    match = (second > first).find();
    CHECK_EQUAL(match, key1);

    match = (second >= first).find();
    CHECK_EQUAL(match, key1);

    match = (second == first).find();
    CHECK_EQUAL(match, null_key);

    match = (second != second).find();
    CHECK_EQUAL(match, null_key);

    match = (first < second).find();
    CHECK_EQUAL(match, key1);

    match = (first <= second).find();
    CHECK_EQUAL(match, key1);

    // Subexpr compare Subexpr
    match = (second + 0 > first + 0).find();
    CHECK_EQUAL(match, key1);

    match = (second + 0 >= first + 0).find();
    CHECK_EQUAL(match, key1);

    match = (second + 0 == first + 0).find();
    CHECK_EQUAL(match, null_key);

    match = (second + 0 != second + 0).find();
    CHECK_EQUAL(match, null_key);

    match = (first + 0 < second + 0).find();
    CHECK_EQUAL(match, key1);

    match = (first + 0 <= second + 0).find();
    CHECK_EQUAL(match, key1);

    // Conversions, again
    table.clear();
    key0 = table.create_object().set_all(20, 3.0f, 3.0).get_key();

    match = (1 / second == 1 / second).find();
    CHECK_EQUAL(match, key0);

    match = (1 / third == 1 / third).find();
    CHECK_EQUAL(match, key0);

    // Nifty test: Compare operator must preserve precision of each side, hence NO match; if double accidentially
    // was truncated to float, or float was rounded to nearest double, then this test would fail.
    match = (1 / second == 1 / third).find();
    CHECK_EQUAL(match, null_key);

    // power operator (power(x) = x^2)
    match = (power(first) == 400).find();
    CHECK_EQUAL(key0, match);

    match = (power(first) == 401).find();
    CHECK_EQUAL(null_key, match);

    Query qq = (power(first) == 401);

    // power of floats. Using a range check because of float arithmetic imprecisions
    match = (power(second) < 9.001 && power(second) > 8.999).find();
    CHECK_EQUAL(key0, match);

    // For `float < int_column` we had a bug where the float truncated to int, and the int_column remained int
    // (correct behaviour would be that the float remained float and int_column converted to float). This test
    // exposes such a bug because 1000000001 should convert to the nearest float value which is `1000000000.`
    // (gap between floats is bigger than 1 and cannot represent 1000000001).
    table.clear();
    table.create_object().set(col_int, 1000000001);

    match = (1000000000.f < first).find();
    CHECK_EQUAL(match, null_key);

    match = (first > 1000000000.f).find();
    CHECK_EQUAL(match, null_key);
}

TEST(Query_StrIndexCrash)
{
    // Rasmus "8" index crash
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    for (int iter = 0; iter < 5; ++iter) {
        Group group;
        TableRef table = group.add_table("test");
        auto col = table->add_column(type_String, "first");

        size_t eights = 0;

        for (int i = 0; i < REALM_MAX_BPNODE_SIZE * 2; ++i) {
            int v = random.draw_int_mod(10);
            if (v == 8) {
                eights++;
            }
            char dst[100];
            memset(dst, 0, sizeof(dst));
            sprintf(dst, "%d", v);
            table->create_object().set(col, dst);
        }

        table->add_search_index(col);
        TableView v = table->where().equal(col, StringData("8")).find_all();
        CHECK_EQUAL(eights, v.size());

        v = table->where().equal(col, StringData("10")).find_all();

        v = table->where().equal(col, StringData("8")).find_all();
        CHECK_EQUAL(eights, v.size());
    }
}

TEST(Query_Links)
{
    Group g;

    TableRef origin = g.add_table("origin");
    TableRef target1 = g.add_table("target1");
    TableRef target2 = g.add_table("target2");

    auto int_col = target2->add_column(type_Int, "integers");
    auto str_col = target1->add_column(type_String, "strings");
    auto linklist_col = target1->add_column_link(type_LinkList, "linklist", *target2);
    auto link_col = origin->add_column_link(type_Link, "link", *target1);
    auto double_col = origin->add_column(type_Double, "doubles");

    std::vector<ObjKey> origin_keys;
    origin->create_objects(10, origin_keys);
    std::vector<ObjKey> target1_keys;
    target1->create_objects(10, target1_keys);
    std::vector<ObjKey> target2_keys;
    target2->create_objects(10, target2_keys);

    for (int i = 0; i < 10; i++) {
        target2->get_object(target2_keys[i]).set(int_col, i);
    }

    for (unsigned i = 0; i < 10; i++) {
        Obj obj = target1->get_object(target1_keys[i]);
        std::string str = "Str";
        str += util::to_string(i);
        obj.set(str_col, StringData(str));
        auto lv = obj.get_linklist(linklist_col);
        for (unsigned j = 0; j < i % 5; j++) {
            lv.add(target2_keys[j]);
        }
    }

    for (unsigned i = 0; i < 10; i++) {
        Obj obj = origin->get_object(origin_keys[i]);
        obj.set(link_col, target1_keys[i]);
        obj.set(double_col, 1.5 * i);
    }

    Query q = target1->link(linklist_col).column<Int>(int_col) == 2;
    auto tv = q.find_all();
    CHECK_EQUAL(tv.size(), 4);

    q = origin->link(link_col).link(linklist_col).column<Int>(int_col) == 2;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 4);

    q = target2->backlink(*target1, linklist_col).column<String>(str_col) == StringData("Str3");
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 3);

    q = target2->backlink(*target1, linklist_col).backlink(*origin, link_col).column<double>(double_col) > 12.0;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 4);
}

TEST(Query_size)
{
    Group g;

    TableRef table1 = g.add_table("primary");
    TableRef table2 = g.add_table("secondary");
    TableRef table3 = g.add_table("top");

    auto string_col = table1->add_column(type_String, "strings");
    auto bin_col = table1->add_column(type_Binary, "binaries", true);
    auto int_list_col = table1->add_column_list(type_Int, "intlist");
    auto linklist_col = table1->add_column_link(type_LinkList, "linklist", *table2);

    auto int_col = table2->add_column(type_Int, "integers");

    auto link_col = table3->add_column_link(type_Link, "link", *table1);
    auto linklist_col1 = table3->add_column_link(type_LinkList, "linklist", *table1);

    std::vector<ObjKey> table1_keys;
    table1->create_objects(10, table1_keys);
    std::vector<ObjKey> table2_keys;
    table2->create_objects(10, table2_keys);
    std::vector<ObjKey> table3_keys;
    table3->create_objects(10, table3_keys);

    auto strings = table1->column<String>(string_col);
    auto binaries = table1->column<Binary>(bin_col);
    auto intlist = table1->column<Lst<Int>>(int_list_col);
    auto linklist = table1->column<Lst<ObjKey>>(linklist_col);

    for (int i = 0; i < 10; i++) {
        table2->get_object(table2_keys[i]).set(int_col, i);
    }

    // Leave the last one null
    for (unsigned i = 0; i < 9; i++) {
        table3->get_object(table3_keys[i]).set(link_col, table1_keys[i % 4]);
    }

    for (unsigned i = 0; i < 10; i++) {
        auto lv = table3->get_object(table3_keys[i]).get_linklist(linklist_col1);
        for (unsigned j = 0; j < i % 5; j++) {
            lv.add(table1_keys[j]);
        }
    }

    std::string bin1(100, 'a');
    std::string bin2(500, '5');
    table1->get_object(table1_keys[0]).set(string_col, "Hi").set(bin_col, BinaryData(bin1));
    table1->get_object(table1_keys[1]).set(string_col, "world").set(bin_col, BinaryData(bin2));

    auto set_list = [](LstPtr<Int> list, const std::vector<int64_t>& value_list) {
        size_t sz = value_list.size();
        list->clear();
        for (size_t i = 0; i < sz; i++) {
            list->add(value_list[i]);
        }
    };
    set_list(table1->get_object(table1_keys[0]).get_list_ptr<Int>(int_list_col),
             std::vector<Int>({100, 200, 300, 400, 500}));
    set_list(table1->get_object(table1_keys[1]).get_list_ptr<Int>(int_list_col), std::vector<Int>({1, 2, 3}));
    set_list(table1->get_object(table1_keys[2]).get_list_ptr<Int>(int_list_col), std::vector<Int>({1, 2, 3, 4, 5}));
    set_list(table1->get_object(table1_keys[3]).get_list_ptr<Int>(int_list_col),
             std::vector<Int>({1, 2, 3, 4, 5, 6, 7, 8, 9}));

    auto set_links = [&table2_keys](LnkLstPtr lv, const std::vector<int>& value_list) {
        for (auto v : value_list) {
            lv->add(table2_keys[v]);
        }
    };
    set_links(table1->get_object(table1_keys[0]).get_linklist_ptr(linklist_col),
              std::vector<int>({0, 1, 2, 3, 4, 5}));
    set_links(table1->get_object(table1_keys[1]).get_linklist_ptr(linklist_col), std::vector<int>({6, 7, 8, 9}));

    Query q;
    Query q1;
    ObjKey match;
    TableView tv;

    q = strings.size() == 5;
    q1 = table1->where().size_equal(string_col, 5);
    match = q.find();
    CHECK_EQUAL(table1_keys[1], match);
    match = q1.find();
    CHECK_EQUAL(table1_keys[1], match);

    // Check that the null values are handled correctly
    q = binaries.size() == realm::null();
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 8);
    CHECK_EQUAL(tv.get_key(0), table1_keys[2]);

    // Here the null values should not be included in the search
    q = binaries.size() < 500;
    q1 = table1->where().size_less(bin_col, 500);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    tv = q1.find_all();
    CHECK_EQUAL(tv.size(), 1);

    q = intlist.size() > 3;
    q1 = table1->where().size_greater(int_list_col, 3);
    tv = q.find_all();
    CHECK_EQUAL(3, tv.size());
    tv = q1.find_all();
    CHECK_EQUAL(3, tv.size());
    q1 = table1->where().size_between(int_list_col, 3, 7);
    tv = q1.find_all();
    CHECK_EQUAL(3, tv.size());

    q = intlist.size() == 3;
    match = q.find();
    CHECK_EQUAL(table1_keys[1], match);

    q1 = table1->where().size_not_equal(linklist_col, 6);
    match = q1.find();
    CHECK_EQUAL(table1_keys[1], match);

    q = intlist.size() > strings.size();
    tv = q.find_all();
    CHECK_EQUAL(3, tv.size());
    CHECK_EQUAL(table1_keys[0], tv.get_key(0));

    // Single links
    q = table3->link(link_col).column<Lst<Int>>(int_list_col).size() == 5;
    tv = q.find_all();
    CHECK_EQUAL(5, tv.size());

    // Multiple links
    q = table3->link(linklist_col1).column<Lst<Int>>(int_list_col).size() == 3;
    tv = q.find_all();
    CHECK_EQUAL(6, tv.size());
}

TEST(Query_ListOfPrimitives)
{
    Group g;

    TableRef table = g.add_table("foo");

    auto col_int_list = table->add_column_list(type_Int, "integers");
    auto col_string_list = table->add_column_list(type_String, "strings");
    auto col_string = table->add_column(type_String, "other");
    std::vector<ObjKey> keys;

    table->create_objects(4, keys);

    {
        auto set_string_list = [](Lst<String> list, const std::vector<int64_t>& value_list) {
            size_t sz = value_list.size();
            list.clear();
            for (size_t i = 0; i < sz; i++) {
                if (value_list[i] < 100) {
                    std::string str("Str_");
                    str += util::to_string(value_list[i]);
                    list.add(str);
                }
            }
        };

        set_string_list(table->get_object(keys[0]).get_list<String>(col_string_list), std::vector<Int>({0, 1}));
        set_string_list(table->get_object(keys[1]).get_list<String>(col_string_list), std::vector<Int>({2, 3, 4, 5}));
        set_string_list(table->get_object(keys[2]).get_list<String>(col_string_list),
                        std::vector<Int>({6, 7, 100, 8, 9}));
    }

    table->get_object(keys[0]).set_list_values(col_int_list, std::vector<Int>({0, 1}));
    table->get_object(keys[1]).set_list_values(col_int_list, std::vector<Int>({2, 3, 4, 5}));
    table->get_object(keys[2]).set_list_values(col_int_list, std::vector<Int>({6, 7, 8, 9}));
    table->get_object(keys[3]).set_list_values(col_int_list, std::vector<Int>({}));

    table->get_object(keys[0]).set<String>(col_string, StringData("foo"));
    table->get_object(keys[1]).set<String>(col_string, StringData("str"));
    table->get_object(keys[2]).set<String>(col_string, StringData("str_9_baa"));

    Query q;
    TableView tv;
    q = table->column<Lst<Int>>(col_int_list) == 5;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_key(0), keys[1]);
    q = table->column<Lst<String>>(col_string_list) == "Str_5";
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_key(0), keys[1]);

    q = table->column<Lst<String>>(col_string_list).begins_with("Str");
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 3);
    q = table->column<Lst<String>>(col_string_list).ends_with("_8");
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_key(0), keys[2]);
    q = table->column<Lst<String>>(col_string_list).begins_with(table->column<String>(col_string), false);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_key(0), keys[1]);
    q = table->column<String>(col_string).begins_with(table->column<Lst<String>>(col_string_list), false);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_key(0), keys[2]);

    q = table->column<Lst<Int>>(col_int_list).min() >= 2;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_key(0), keys[1]);
    CHECK_EQUAL(tv.get_key(1), keys[2]);
    q = table->column<Lst<Int>>(col_int_list).max() > 6;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_key(0), keys[2]);
    q = table->column<Lst<Int>>(col_int_list).sum() == 14;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_key(0), keys[1]);
    q = table->column<Lst<Int>>(col_int_list).average() < 4;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_key(0), keys[0]);
    CHECK_EQUAL(tv.get_key(1), keys[1]);

    TableRef baa = g.add_table("baa");
    auto col_link = baa->add_column_link(type_Link, "link", *table);
    auto col_linklist = baa->add_column_link(type_LinkList, "linklist", *table);
    Obj obj0 = baa->create_object().set(col_link, keys[1]);
    Obj obj1 = baa->create_object().set(col_link, keys[0]);

    auto lv = obj0.get_linklist_ptr(col_linklist);
    lv->add(keys[0]);
    lv->add(keys[1]);
    lv = obj1.get_linklist_ptr(col_linklist);
    lv->add(keys[1]);
    lv->add(keys[2]);
    lv->add(keys[3]);

    q = baa->link(col_link).column<Lst<Int>>(col_int_list) == 5;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_key(0), keys[0]);

    q = baa->link(col_linklist).column<Lst<String>>(col_string_list) == "Str_5";
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);

    q = baa->link(col_linklist).column<Lst<Int>>(col_int_list).average() >= 3.0;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    table->get_object(keys[1]).get_list<Int>(col_int_list).set(3, -10); // {2, 3, 4, -10}
    // Now, one less object will have average bigger than 3
    tv.sync_if_needed();
    CHECK_EQUAL(tv.size(), 1);
}

TEST_TYPES(Query_StringIndexCommonPrefix, std::true_type, std::false_type)
{
    Group group;
    TableRef table = group.add_table("test");
    auto col_str = table->add_column(type_String, "first");
    table->add_search_index(col_str);
    if (TEST_TYPE::value == true) {
        table->enumerate_string_column(col_str);
    }

    auto test_prefix_find = [&](std::string prefix) {
        std::string prefix_b = prefix + "b";
        std::string prefix_c = prefix + "c";
        std::string prefix_d = prefix + "d";
        std::string prefix_e = prefix + "e";
        StringData spb(prefix_b);
        StringData spc(prefix_c);
        StringData spd(prefix_d);
        StringData spe(prefix_e);

        std::vector<ObjKey> keys;
        table->create_objects(6, keys);
        table->get_object(keys[0]).set(col_str, spb);
        table->get_object(keys[1]).set(col_str, spc);
        table->get_object(keys[2]).set(col_str, spc);
        table->get_object(keys[3]).set(col_str, spe);
        table->get_object(keys[4]).set(col_str, spe);
        table->get_object(keys[5]).set(col_str, spe);

        TableView v = table->where().equal(col_str, spb).find_all();
        CHECK_EQUAL(v.size(), 1);
        CHECK_EQUAL(v.get(0).get_key(), keys[0]);

        v = table->where().equal(col_str, spc).find_all();
        CHECK_EQUAL(v.size(), 2);
        CHECK_EQUAL(v.get(0).get_key(), keys[1]);
        CHECK_EQUAL(v.get(1).get_key(), keys[2]);

        v = table->where().equal(col_str, spd).find_all();
        CHECK_EQUAL(v.size(), 0);

        v = table->where().equal(col_str, spe).find_all();
        CHECK_EQUAL(v.size(), 3);
        CHECK_EQUAL(v.get(0).get_key(), keys[3]);
        CHECK_EQUAL(v.get(1).get_key(), keys[4]);
        CHECK_EQUAL(v.get(2).get_key(), keys[5]);
    };

    std::string std_max(StringIndex::s_max_offset, 'a');
    std::string std_over_max = std_max + "a";
    std::string std_under_max(StringIndex::s_max_offset >> 1, 'a');

    test_prefix_find(std_max);
    test_prefix_find(std_over_max);
    test_prefix_find(std_under_max);
}


TEST(Query_TwoColsEqualVaryWidthAndValues)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    std::vector<ObjKey> ints1;
    std::vector<ObjKey> ints2;
    std::vector<ObjKey> ints3;

    std::vector<ObjKey> floats;
    std::vector<ObjKey> doubles;

    Table table;
    auto col_int0 = table.add_column(type_Int, "first1");
    auto col_int1 = table.add_column(type_Int, "second1");

    auto col_int2 = table.add_column(type_Int, "first2");
    auto col_int3 = table.add_column(type_Int, "second2");

    auto col_int4 = table.add_column(type_Int, "first3");
    auto col_int5 = table.add_column(type_Int, "second3");

    auto col_float6 = table.add_column(type_Float, "third");
    auto col_float7 = table.add_column(type_Float, "fourth");
    auto col_double8 = table.add_column(type_Double, "fifth");
    auto col_double9 = table.add_column(type_Double, "sixth");

#ifdef REALM_DEBUG
    for (int i = 0; i < REALM_MAX_BPNODE_SIZE * 5; i++) {
#else
    for (int i = 0; i < 50000; i++) {
#endif
        Obj obj = table.create_object();
        ObjKey key = obj.get_key();

        // Important thing to test is different bitwidths because we might use SSE and/or bithacks on 64-bit blocks

        // Both are bytes
        obj.set(col_int0, random.draw_int_mod(100));
        obj.set(col_int1, random.draw_int_mod(100));

        // Second column widest
        obj.set(col_int2, random.draw_int_mod(10));
        obj.set(col_int3, random.draw_int_mod(100));

        // First column widest
        obj.set(col_int4, random.draw_int_mod(100));
        obj.set(col_int5, random.draw_int_mod(10));

        obj.set(col_float6, float(random.draw_int_mod(10)));
        obj.set(col_float7, float(random.draw_int_mod(10)));

        obj.set(col_double8, double(random.draw_int_mod(10)));
        obj.set(col_double9, double(random.draw_int_mod(10)));

        if (obj.get<Int>(col_int0) == obj.get<Int>(col_int1))
            ints1.push_back(key);

        if (obj.get<Int>(col_int2) == obj.get<Int>(col_int3))
            ints2.push_back(key);

        if (obj.get<Int>(col_int4) == obj.get<Int>(col_int5))
            ints3.push_back(key);

        if (obj.get<Float>(col_float6) == obj.get<Float>(col_float7))
            floats.push_back(key);

        if (obj.get<Double>(col_double8) == obj.get<Double>(col_double9))
            doubles.push_back(key);
    }

    realm::TableView t1 = table.where().equal_int(col_int0, col_int1).find_all();
    realm::TableView t2 = table.where().equal_int(col_int2, col_int3).find_all();
    realm::TableView t3 = table.where().equal_int(col_int4, col_int5).find_all();

    realm::TableView t4 = table.where().equal_float(col_float6, col_float7).find_all();
    realm::TableView t5 = table.where().equal_double(col_double8, col_double9).find_all();


    CHECK_EQUAL(ints1.size(), t1.size());
    for (size_t t = 0; t < ints1.size(); t++)
        CHECK_EQUAL(ints1[t], t1.get_key(t));

    CHECK_EQUAL(ints2.size(), t2.size());
    for (size_t t = 0; t < ints2.size(); t++)
        CHECK_EQUAL(ints2[t], t2.get_key(t));

    CHECK_EQUAL(ints3.size(), t3.size());
    for (size_t t = 0; t < ints3.size(); t++)
        CHECK_EQUAL(ints3[t], t3.get_key(t));

    CHECK_EQUAL(floats.size(), t4.size());
    for (size_t t = 0; t < floats.size(); t++)
        CHECK_EQUAL(floats[t], t4.get_key(t));

    CHECK_EQUAL(doubles.size(), t5.size());
    for (size_t t = 0; t < doubles.size(); t++)
        CHECK_EQUAL(doubles[t], t5.get_key(t));
}

TEST(Query_TwoColsVaryOperators)
{
    std::vector<size_t> ints1;
    std::vector<size_t> floats;
    std::vector<size_t> doubles;

    Table table;
    auto col_int0 = table.add_column(type_Int, "first1");
    auto col_int1 = table.add_column(type_Int, "second1");

    auto col_float2 = table.add_column(type_Float, "third");
    auto col_float3 = table.add_column(type_Float, "fourth");
    auto col_double4 = table.add_column(type_Double, "fifth");
    auto col_double5 = table.add_column(type_Double, "sixth");

    Obj obj0 = table.create_object().set_all(5, 10, 5.0f, 10.0f, 5.0, 10.0);
    Obj obj1 = table.create_object().set_all(10, 5, 10.0f, 5.0f, 10.0, 5.0);
    Obj obj2 = table.create_object().set_all(-10, -5, -10.0f, -5.0f, -10.0, -5.0);

    CHECK_EQUAL(null_key, table.where().equal_int(col_int0, col_int1).find());
    CHECK_EQUAL(obj0.get_key(), table.where().not_equal_int(col_int0, col_int1).find());
    CHECK_EQUAL(obj0.get_key(), table.where().less_int(col_int0, col_int1).find());
    CHECK_EQUAL(obj1.get_key(), table.where().greater_int(col_int0, col_int1).find());
    CHECK_EQUAL(obj1.get_key(), table.where().greater_equal_int(col_int0, col_int1).find());
    CHECK_EQUAL(obj0.get_key(), table.where().less_equal_int(col_int0, col_int1).find());

    CHECK_EQUAL(null_key, table.where().equal_float(col_float2, col_float3).find());
    CHECK_EQUAL(obj0.get_key(), table.where().not_equal_float(col_float2, col_float3).find());
    CHECK_EQUAL(obj0.get_key(), table.where().less_float(col_float2, col_float3).find());
    CHECK_EQUAL(obj1.get_key(), table.where().greater_float(col_float2, col_float3).find());
    CHECK_EQUAL(obj1.get_key(), table.where().greater_equal_float(col_float2, col_float3).find());
    CHECK_EQUAL(obj0.get_key(), table.where().less_equal_float(col_float2, col_float3).find());

    CHECK_EQUAL(null_key, table.where().equal_double(col_double4, col_double5).find());
    CHECK_EQUAL(obj0.get_key(), table.where().not_equal_double(col_double4, col_double5).find());
    CHECK_EQUAL(obj0.get_key(), table.where().less_double(col_double4, col_double5).find());
    CHECK_EQUAL(obj1.get_key(), table.where().greater_double(col_double4, col_double5).find());
    CHECK_EQUAL(obj1.get_key(), table.where().greater_equal_double(col_double4, col_double5).find());
    CHECK_EQUAL(obj0.get_key(), table.where().less_equal_double(col_double4, col_double5).find());
}


TEST(Query_TwoCols0)
{
    Table table;
    auto col0 = table.add_column(type_Int, "first1");
    auto col1 = table.add_column(type_Int, "second1");


    for (int i = 0; i < 50; i++) {
        table.create_object();
    }

    realm::TableView t1 = table.where().equal_int(col0, col1).find_all();
    CHECK_EQUAL(50, t1.size());

    realm::TableView t2 = table.where().less_int(col0, col1).find_all();
    CHECK_EQUAL(0, t2.size());
}


TEST(Query_TwoSameCols)
{
    Table table;
    auto col_bool0 = table.add_column(type_Bool, "first1");
    auto col_bool1 = table.add_column(type_Bool, "first2");
    auto col_date2 = table.add_column(type_Timestamp, "second1");
    auto col_date3 = table.add_column(type_Timestamp, "second2");
    auto col_str4 = table.add_column(type_String, "third1");
    auto col_str5 = table.add_column(type_String, "third2");

    Timestamp d1(200, 0);
    Timestamp d2(300, 0);
    ObjKey key0 = table.create_object().set_all(false, true, d1, d2, "a", "b").get_key();
    ObjKey key1 = table.create_object().set_all(true, true, d2, d2, "b", "b").get_key();
    table.create_object().set_all(false, true, d1, d2, "a", "b").get_key();

    Query q1 = table.column<Bool>(col_bool0) == table.column<Bool>(col_bool1);
    Query q2 = table.column<Timestamp>(col_date2) == table.column<Timestamp>(col_date3);
    Query q3 = table.column<String>(col_str4) == table.column<String>(col_str5);

    CHECK_EQUAL(key1, q1.find());
    CHECK_EQUAL(key1, q2.find());
    CHECK_EQUAL(key1, q3.find());
    CHECK_EQUAL(1, q1.count());
    CHECK_EQUAL(1, q2.count());
    CHECK_EQUAL(1, q3.count());

    Query q4 = table.column<Bool>(col_bool0) != table.column<Bool>(col_bool1);
    Query q5 = table.column<Timestamp>(col_date2) != table.column<Timestamp>(col_date3);
    Query q6 = table.column<String>(col_str4) != table.column<String>(col_str5);

    CHECK_EQUAL(key0, q5.find());
    CHECK_EQUAL(key0, q5.find());
    CHECK_EQUAL(key0, q6.find());
    CHECK_EQUAL(2, q5.count());
    CHECK_EQUAL(2, q5.count());
    CHECK_EQUAL(2, q6.count());
}


TEST(Query_DateTest)
{
    Table table;
    auto col_date = table.add_column(type_Timestamp, "second1");

    for (int i = 0; i < 9; i++) {
        table.create_object().set(col_date, Timestamp(i * 1000, i));
    }

    Query q = table.where().equal(col_date, Timestamp(5000, 5));
    CHECK_EQUAL(1, q.count());
    TableView tv = q.find_all();
    CHECK_EQUAL(1, tv.size());
}

TEST(Query_TwoColsNoRows)
{
    Table table;
    auto col0 = table.add_column(type_Int, "first1");
    auto col1 = table.add_column(type_Int, "second1");

    CHECK_EQUAL(null_key, table.where().equal_int(col0, col1).find());
    CHECK_EQUAL(null_key, table.where().not_equal_int(col0, col1).find());
}


TEST(Query_Huge)
{
    Random random;

#if TEST_DURATION == 0
    for (int N = 0; N < 1; N++) {
#elif TEST_DURATION == 1
    for (int N = 0; N < 100; N++) {
#elif TEST_DURATION == 2
    for (int N = 0; N < 1000; N++) {
#elif TEST_DURATION == 3
    for (int N = 0; N < 10000; N++) {
#endif

        // Makes you reproduce a bug in a certain run, without having
        // to run all successive runs
        random.seed(N + 123);

        Table tt;
        auto col_str0 = tt.add_column(type_String, "1");
        auto col_str1 = tt.add_column(type_String, "2");
        auto col_int2 = tt.add_column(type_Int, "3");

        TableView v;
        bool long1 = false;
        bool long2 = false;

        size_t mdist1 = 1;
        size_t mdist2 = 1;
        size_t mdist3 = 1;

        std::string first;
        std::string second;
        int64_t third;

        size_t res1 = 0;
        size_t res2 = 0;
        size_t res3 = 0;
        size_t res4 = 0;
        size_t res5 = 0;
        size_t res6 = 0;
        size_t res7 = 0;
        size_t res8 = 0;

        size_t start = random.draw_int_mod(3000);
        size_t end = start + random.draw_int_mod(3000 - start);
        size_t limit;
        if (random.draw_bool())
            limit = random.draw_int_mod(5000);
        else
            limit = size_t(-1);


        size_t blocksize = random.draw_int_mod(800) + 1;

        for (size_t row = 0; row < 3000; row++) {

            if (row % blocksize == 0) {
                long1 = random.draw_bool();
                long2 = random.draw_bool();

                if (random.draw_bool()) {
                    mdist1 = random.draw_int(1, 500);
                    mdist2 = random.draw_int(1, 500);
                    mdist3 = random.draw_int(1, 500);
                }
                else {
                    mdist1 = random.draw_int(1, 5);
                    mdist2 = random.draw_int(1, 5);
                    mdist3 = random.draw_int(1, 5);
                }
            }

            Obj obj = tt.create_object();

            if (long1) {
                if (random.draw_int_mod(mdist1) == 0)
                    first = "longlonglonglonglonglonglong A";
                else
                    first = "longlonglonglonglonglonglong B";
            }
            else {
                if (random.draw_int_mod(mdist1) == 0)
                    first = "A";
                else
                    first = "B";
            }

            if (long2) {
                if (random.draw_int_mod(mdist2) == 0)
                    second = "longlonglonglonglonglonglong A";
                else
                    second = "longlonglonglonglonglonglong B";
            }
            else {
                if (random.draw_int_mod(mdist2) == 0)
                    second = "A";
                else
                    second = "B";
            }

            if (random.draw_int_mod(mdist3) == 0)
                third = 1;
            else
                third = 2;

            obj.set(col_str0, StringData(first));
            obj.set(col_str1, StringData(second));
            obj.set(col_int2, third);

            if ((row >= start && row < end && limit > res1) && (first == "A" && second == "A" && third == 1))
                res1++;

            if ((row >= start && row < end && limit > res2) && ((first == "A" || second == "A") && third == 1))
                res2++;

            if ((row >= start && row < end && limit > res3) && (first == "A" && (second == "A" || third == 1)))
                res3++;

            if ((row >= start && row < end && limit > res4) && (second == "A" && (first == "A" || third == 1)))
                res4++;

            if ((row >= start && row < end && limit > res5) && (first == "A" || second == "A" || third == 1))
                res5++;

            if ((row >= start && row < end && limit > res6) && (first != "A" && second == "A" && third == 1))
                res6++;

            if ((row >= start && row < end && limit > res7) &&
                (first != "longlonglonglonglonglonglong A" && second == "A" && third == 1))
                res7++;

            if ((row >= start && row < end && limit > res8) &&
                (first != "longlonglonglonglonglonglong A" && second == "A" && third == 2))
                res8++;
        }

        for (size_t t = 0; t < 4; t++) {

            if (t == 1) {
                tt.enumerate_string_column(col_str0);
                tt.enumerate_string_column(col_str1);
            }
            else if (t == 2) {
                tt.add_search_index(col_str0);
            }
            else if (t == 3) {
                tt.add_search_index(col_str1);
            }

            v = tt.where().equal(col_str0, "A").equal(col_str1, "A").equal(col_int2, 1).find_all(start, end, limit);
            CHECK_EQUAL(res1, v.size());

            v = tt.where().equal(col_str1, "A").equal(col_str0, "A").equal(col_int2, 1).find_all(start, end, limit);
            CHECK_EQUAL(res1, v.size());

            v = tt.where().equal(col_int2, 1).equal(col_str1, "A").equal(col_str0, "A").find_all(start, end, limit);
            CHECK_EQUAL(res1, v.size());

            v = tt.where()
                    .group()
                    .equal(col_str0, "A")
                    .Or()
                    .equal(col_str1, "A")
                    .end_group()
                    .equal(col_int2, 1)
                    .find_all(start, end, limit);
            CHECK_EQUAL(res2, v.size());

            v = tt.where()
                    .equal(col_str0, "A")
                    .group()
                    .equal(col_str1, "A")
                    .Or()
                    .equal(col_int2, 1)
                    .end_group()
                    .find_all(start, end, limit);
            CHECK_EQUAL(res3, v.size());

            Query q =
                tt.where().group().equal(col_str0, "A").Or().equal(col_int2, 1).end_group().equal(col_str1, "A");
            v = q.find_all(start, end, limit);
            CHECK_EQUAL(res4, v.size());

            v = tt.where()
                    .group()
                    .equal(col_str0, "A")
                    .Or()
                    .equal(col_int2, 1)
                    .end_group()
                    .equal(col_str1, "A")
                    .find_all(start, end, limit);
            CHECK_EQUAL(res4, v.size());

            v = tt.where()
                    .equal(col_str0, "A")
                    .Or()
                    .equal(col_str1, "A")
                    .Or()
                    .equal(col_int2, 1)
                    .find_all(start, end, limit);
            CHECK_EQUAL(res5, v.size());

            v = tt.where()
                    .not_equal(col_str0, "A")
                    .equal(col_str1, "A")
                    .equal(col_int2, 1)
                    .find_all(start, end, limit);
            CHECK_EQUAL(res6, v.size());

            v = tt.where()
                    .not_equal(col_str0, "longlonglonglonglonglonglong A")
                    .equal(col_str1, "A")
                    .equal(col_int2, 1)
                    .find_all(start, end, limit);
            CHECK_EQUAL(res7, v.size());

            v = tt.where()
                    .not_equal(col_str0, "longlonglonglonglonglonglong A")
                    .equal(col_str1, "A")
                    .equal(col_int2, 2)
                    .find_all(start, end, limit);
            CHECK_EQUAL(res8, v.size());
        }
    }
}


TEST(Query_OnTableView_where)
{
    Random random;

    for (int iter = 0; iter < 50 * (1 + TEST_DURATION * TEST_DURATION); iter++) {
        random.seed(164);
        Table oti;
        auto col = oti.add_column(type_Int, "1");

        size_t cnt1 = 0;
        size_t cnt0 = 0;
        size_t limit = random.draw_int_max(REALM_MAX_BPNODE_SIZE * 10);

        size_t lbound = random.draw_int_mod(REALM_MAX_BPNODE_SIZE * 10);
        size_t ubound = lbound + random.draw_int_mod(REALM_MAX_BPNODE_SIZE * 10 - lbound);

        for (size_t i = 0; i < REALM_MAX_BPNODE_SIZE * 10; i++) {
            int v = random.draw_int_mod(3);

            if (v == 1 && i >= lbound && i < ubound && cnt0 < limit)
                cnt1++;

            if (v != 0 && i >= lbound && i < ubound)
                cnt0++;

            oti.create_object().set(col, v);
        }

        TableView v = oti.where().not_equal(col, 0).find_all(lbound, ubound, limit);
        size_t cnt2 = oti.where(&v).equal(col, 1).count();

        CHECK_EQUAL(cnt1, cnt2);
    }
}

TEST(Query_StrIndex3)
{
    // Create two columns where query match-density varies alot throughout the rows. This forces the query engine to
    // jump back and forth between the two conditions and test edge cases in these transitions. Tests combinations of
    // linear scan, enum and index

    Random random(random_int<unsigned long>()); // Seed from slow global generator

#ifdef REALM_DEBUG
    for (int N = 0; N < 4; N++) {
#else
    for (int N = 0; N < 20; N++) {
#endif
        Table ttt;
        auto col_int = ttt.add_column(type_Int, "1");
        auto col_str = ttt.add_column(type_String, "2");

        std::vector<ObjKey> vec;

        size_t n = 0;
#ifdef REALM_DEBUG
        for (int i = 0; i < 4; i++) {
#else
        for (int i = 0; i < 20; i++) {
#endif
            // 1/500 match probability because we want possibility for a 1000 sized leaf to contain 0 matches
            // (important
            // edge case)
            int f1 = random.draw_int_mod(REALM_MAX_BPNODE_SIZE) / 2 + 1;
            int f2 = random.draw_int_mod(REALM_MAX_BPNODE_SIZE) / 2 + 1;
            bool longstrings = random.chance(1, 5);

            // 2200 entries with that probability to fill out two concecutive 1000 sized leaves with above
            // probability,
            // plus a remainder (edge case)
            for (int j = 0; j < REALM_MAX_BPNODE_SIZE * 2 + REALM_MAX_BPNODE_SIZE / 5; j++) {
                if (random.chance(1, f1)) {
                    if (random.chance(1, f2)) {
                        ObjKey key =
                            ttt.create_object().set_all(0, longstrings ? "AAAAAAAAAAAAAAAAAAAAAAAA" : "AA").get_key();
                        if (!longstrings) {
                            n++;
                            vec.push_back(key);
                        }
                    }
                    else {
                        ttt.create_object().set_all(0, "BB");
                    }
                }
                else {
                    if (random.chance(1, f2)) {
                        ttt.create_object().set_all(1, "AA");
                    }
                    else {
                        ttt.create_object().set_all(1, "BB");
                    }
                }
            }
        }

        TableView v;

        // Both linear scans
        v = ttt.where().equal(col_str, "AA").equal(col_int, 0).find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_key(t));

        v = ttt.where().equal(col_int, 0).equal(col_str, "AA").find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_key(t));

        ttt.enumerate_string_column(col_str);

        // Linear scan over enum, plus linear integer column scan
        v = ttt.where().equal(col_str, "AA").equal(col_int, 0).find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_key(t));

        v = ttt.where().equal(col_int, 0).equal(col_str, "AA").find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_key(t));

        ttt.add_search_index(col_str);

        // Index lookup, plus linear integer column scan
        v = ttt.where().equal(col_str, "AA").equal(col_int, 0).find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_key(t));

        v = ttt.where().equal(col_int, 0).equal(col_str, "AA").find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_key(t));
    }
}

TEST(Query_StrIndex2)
{
    Table ttt;
    ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    int64_t s;

    for (int i = 0; i < 100; ++i) {
        ttt.create_object().set_all(1, "AA");
    }
    ttt.create_object().set_all(1, "BB");
    ttt.add_search_index(col_str);

    s = ttt.where().equal(col_str, "AA").count();
    CHECK_EQUAL(100, s);

    s = ttt.where().equal(col_str, "BB").count();
    CHECK_EQUAL(1, s);

    s = ttt.where().equal(col_str, "CC").count();
    CHECK_EQUAL(0, s);
}

TEST(Query_StrEnum)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    Table ttt;
    ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    int aa;
    int64_t s;

    for (int i = 0; i < 100; ++i) {
        ttt.clear();
        aa = 0;
        for (size_t t = 0; t < REALM_MAX_BPNODE_SIZE * 2; ++t) {
            if (random.chance(1, 3)) {
                ttt.create_object().set_all(1, "AA");
                ++aa;
            }
            else {
                ttt.create_object().set_all(1, "BB");
            }
        }
        ttt.enumerate_string_column(col_str);
        s = ttt.where().equal(col_str, "AA").count();
        CHECK_EQUAL(aa, s);
    }
}

TEST(Query_StrIndex)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

#ifdef REALM_DEBUG
    size_t itera = 4;
    size_t iterb = 100;
#else
    size_t itera = 100;
    size_t iterb = 2000;
#endif

    int aa;
    int64_t s;

    for (size_t i = 0; i < itera; i++) {
        Table ttt;
        ttt.add_column(type_Int, "1");
        auto str_col = ttt.add_column(type_String, "2");

        aa = 0;
        for (size_t t = 0; t < iterb; t++) {
            if (random.chance(1, 3)) {
                ttt.create_object().set_all(1, "AA");
                aa++;
            }
            else {
                ttt.create_object().set_all(1, "BB");
            }
        }

        s = ttt.where().equal(str_col, "AA").count();
        CHECK_EQUAL(aa, s);

        ttt.enumerate_string_column(str_col);
        s = ttt.where().equal(str_col, "AA").count();
        CHECK_EQUAL(aa, s);

        ttt.add_search_index(str_col);
        s = ttt.where().equal(str_col, "AA").count();
        CHECK_EQUAL(aa, s);
    }
}

TEST(Query_GA_Crash)
{
    GROUP_TEST_PATH(path);
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    {
        Group g;
        TableRef t = g.add_table("firstevents");
        auto col_str0 = t->add_column(type_String, "1");
        auto col_str1 = t->add_column(type_String, "2");
        auto col_str2 = t->add_column(type_String, "3");
        t->add_column(type_Int, "4");
        t->add_column(type_Int, "5");

        for (size_t i = 0; i < 100; ++i) {
            int64_t r1 = random.draw_int_mod(100);
            int64_t r2 = random.draw_int_mod(100);

            t->create_object().set_all("10", "US", "1.0", r1, r2);
        }
        t->enumerate_string_column(col_str0);
        t->enumerate_string_column(col_str1);
        t->enumerate_string_column(col_str2);
        g.write(path);
    }

    Group g(path);
    TableRef t = g.get_table("firstevents");
    auto col_str1 = t->get_column_key("2");

    Query q = t->where().equal(col_str1, "US");

    size_t c1 = 0;
    for (size_t i = 0; i < 100; ++i)
        c1 += t->count_string(col_str1, "US");

    size_t c2 = 0;
    for (size_t i = 0; i < 100; ++i)
        c2 += q.count();

    CHECK_EQUAL(c1, t->size() * 100);
    CHECK_EQUAL(c1, c2);
}

TEST(Query_Float3)
{
    Table t;
    auto col_float = t.add_column(type_Float, "1");
    auto col_double = t.add_column(type_Double, "2");
    auto col_int = t.add_column(type_Int, "3");

    t.create_object().set_all(float(1.1), double(2.1), 1);
    t.create_object().set_all(float(1.2), double(2.2), 2);
    t.create_object().set_all(float(1.3), double(2.3), 3);
    t.create_object().set_all(float(1.4), double(2.4), 4); // match
    t.create_object().set_all(float(1.5), double(2.5), 5); // match
    t.create_object().set_all(float(1.6), double(2.6), 6); // match
    t.create_object().set_all(float(1.7), double(2.7), 7);
    t.create_object().set_all(float(1.8), double(2.8), 8);
    t.create_object().set_all(float(1.9), double(2.9), 9);

    Query q1 = t.where().greater(col_float, 1.35f).less(col_double, 2.65);
    int64_t a1 = q1.sum_int(col_int);
    CHECK_EQUAL(15, a1);

    Query q2 = t.where().less(col_double, 2.65).greater(col_float, 1.35f);
    int64_t a2 = q2.sum_int(col_int);
    CHECK_EQUAL(15, a2);

    Query q3 = t.where().less(col_double, 2.65).greater(col_float, 1.35f);
    double a3 = q3.sum_float(col_float);
    double sum3 = double(1.4f) + double(1.5f) + double(1.6f);
    CHECK_EQUAL(sum3, a3);

    Query q4 = t.where().greater(col_float, 1.35f).less(col_double, 2.65);
    double a4 = q4.sum_float(col_float);
    CHECK_EQUAL(sum3, a4);

    Query q5 = t.where().greater_equal(col_int, 4).less(col_double, 2.65);
    double a5 = q5.sum_float(col_float);
    CHECK_EQUAL(sum3, a5);

    Query q6 = t.where().less(col_double, 2.65).greater_equal(col_int, 4);
    double a6 = q6.sum_float(col_float);
    CHECK_EQUAL(sum3, a6);

    Query q7 = t.where().greater(col_int, 3).less(col_int, 7);
    int64_t a7 = q7.sum_int(col_int);
    CHECK_EQUAL(15, a7);
    Query q8 = t.where().greater(col_int, 3).less(col_int, 7);
    int64_t a8 = q8.sum_int(col_int);
    CHECK_EQUAL(15, a8);
}


TEST(Query_Float3_where)
{
    // Sum on query on tableview
    Table t;
    auto col_float = t.add_column(type_Float, "1");
    auto col_double = t.add_column(type_Double, "2");
    auto col_int = t.add_column(type_Int, "3");

    t.create_object().set_all(float(1.1), double(2.1), 1);
    t.create_object().set_all(float(1.2), double(2.2), 2);
    t.create_object().set_all(float(1.3), double(2.3), 3);
    t.create_object().set_all(float(1.4), double(2.4), 4); // match
    t.create_object().set_all(float(1.5), double(2.5), 5); // match
    t.create_object().set_all(float(1.6), double(2.6), 6); // match
    t.create_object().set_all(float(1.7), double(2.7), 7);
    t.create_object().set_all(float(1.8), double(2.8), 8);
    t.create_object().set_all(float(1.9), double(2.9), 9);

    TableView v = t.where().find_all();

    Query q1 = t.where(&v).greater(col_float, 1.35f).less(col_double, 2.65);
    int64_t a1 = q1.sum_int(col_int);
    CHECK_EQUAL(15, a1);

    Query q2 = t.where(&v).less(col_double, 2.65).greater(col_float, 1.35f);
    int64_t a2 = q2.sum_int(col_int);
    CHECK_EQUAL(15, a2);

    Query q3 = t.where(&v).less(col_double, 2.65).greater(col_float, 1.35f);
    double a3 = q3.sum_float(col_float);
    double sum3 = double(1.4f) + double(1.5f) + double(1.6f);
    CHECK_EQUAL(sum3, a3);

    Query q4 = t.where(&v).greater(col_float, 1.35f).less(col_double, 2.65);
    double a4 = q4.sum_float(col_float);
    CHECK_EQUAL(sum3, a4);

    Query q5 = t.where(&v).greater_equal(col_int, 4).less(col_double, 2.65);
    double a5 = q5.sum_float(col_float);
    CHECK_EQUAL(sum3, a5);

    Query q6 = t.where(&v).less(col_double, 2.65).greater_equal(col_int, 4);
    double a6 = q6.sum_float(col_float);
    CHECK_EQUAL(sum3, a6);

    Query q7 = t.where(&v).greater(col_int, 3).less(col_int, 7);
    int64_t a7 = q7.sum_int(col_int);
    CHECK_EQUAL(15, a7);
    Query q8 = t.where(&v).greater(col_int, 3).less(col_int, 7);
    int64_t a8 = q8.sum_int(col_int);
    CHECK_EQUAL(15, a8);
}

TEST(Query_TableViewSum)
{
    Table t;

    auto col_int = t.add_column(type_Int, "3");

    for (int i = 0; i < 10; i++) {
        t.create_object().set(col_int, i + 1);
    }

    Query q1 = t.where().between(col_int, 5, 9);
    TableView tv1 = q1.find_all();
    int64_t s = tv1.sum_int(col_int);
    CHECK_EQUAL(5 + 6 + 7 + 8 + 9, s);
}

TEST(Query_JavaMinimumCrash)
{
    // Test that triggers a bug that was discovered through Java intnerface and has been fixed
    Table ttt;

    auto col_str = ttt.add_column(type_String, "1");
    ttt.add_column(type_String, "2");
    auto col_int = ttt.add_column(type_Int, "3");

    ttt.create_object().set_all("Joe", "John", 1);
    ttt.create_object().set_all("Jane", "Doe", 2);
    ttt.create_object().set_all("Bob", "Hanson", 3);

    Query q1 = ttt.where().equal(col_str, "Joe").Or().equal(col_str, "Bob");
    int64_t m = q1.minimum_int(col_int);
    CHECK_EQUAL(1, m);
}


TEST(Query_Float4)
{
    Table t;

    auto col_float = t.add_column(type_Float, "1");
    auto col_double = t.add_column(type_Double, "2");
    t.add_column(type_Int, "3");

    t.create_object().set_all(std::numeric_limits<float>::max(), std::numeric_limits<double>::max(), 11111);
    t.create_object().set_all(std::numeric_limits<float>::infinity(), std::numeric_limits<double>::infinity(), 11111);
    t.create_object().set_all(12345.0f, 12345.0, 11111);

    Query q1 = t.where();
    float a1 = q1.maximum_float(col_float);
    double a2 = q1.maximum_double(col_double);
    CHECK_EQUAL(std::numeric_limits<float>::infinity(), a1);
    CHECK_EQUAL(std::numeric_limits<double>::infinity(), a2);


    Query q2 = t.where();
    float a3 = q1.minimum_float(col_float);
    double a4 = q1.minimum_double(col_double);
    CHECK_EQUAL(12345.0, a3);
    CHECK_EQUAL(12345.0, a4);
}


TEST(Query_Float)
{
    Table t;
    auto col_float = t.add_column(type_Float, "1");
    auto col_double = t.add_column(type_Double, "2");

    ObjKey k0 = t.create_object().set_all(1.10f, 2.20).get_key();
    ObjKey k1 = t.create_object().set_all(1.13f, 2.21).get_key();
    t.create_object().set_all(1.13f, 2.22);
    t.create_object().set_all(1.10f, 2.20).get_key();
    ObjKey k4 = t.create_object().set_all(1.20f, 3.20).get_key();

    // Test find_all()
    TableView v = t.where().equal(col_float, 1.13f).find_all();
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(1.13f, v[0].get<float>(col_float));
    CHECK_EQUAL(1.13f, v[1].get<float>(col_float));

    TableView v2 = t.where().equal(col_double, 3.2).find_all();
    CHECK_EQUAL(1, v2.size());
    CHECK_EQUAL(3.2, v2[0].get<double>(col_double));

    // Test operators (and count)
    CHECK_EQUAL(2, t.where().equal(col_float, 1.13f).count());
    CHECK_EQUAL(3, t.where().not_equal(col_float, 1.13f).count());
    CHECK_EQUAL(3, t.where().greater(col_float, 1.1f).count());
    CHECK_EQUAL(3, t.where().greater_equal(col_float, 1.13f).count());
    CHECK_EQUAL(4, t.where().less_equal(col_float, 1.13f).count());
    CHECK_EQUAL(2, t.where().less(col_float, 1.13f).count());
    CHECK_EQUAL(3, t.where().between(col_float, 1.13f, 1.2f).count());

    CHECK_EQUAL(2, t.where().equal(col_double, 2.20).count());
    CHECK_EQUAL(3, t.where().not_equal(col_double, 2.20).count());
    CHECK_EQUAL(2, t.where().greater(col_double, 2.21).count());
    CHECK_EQUAL(3, t.where().greater_equal(col_double, 2.21).count());
    CHECK_EQUAL(4, t.where().less_equal(col_double, 2.22).count());
    CHECK_EQUAL(3, t.where().less(col_double, 2.22).count());
    CHECK_EQUAL(4, t.where().between(col_double, 2.20, 2.22).count());

    double epsilon = std::numeric_limits<double>::epsilon();

    // ------ Test sum()
    // ... NO conditions
    double sum1_d = 2.20 + 2.21 + 2.22 + 2.20 + 3.20;
    CHECK_APPROXIMATELY_EQUAL(sum1_d, t.where().sum_double(col_double), 10 * epsilon);

    // Note: sum of float is calculated by having a double aggregate to where each float is added
    // (thereby getting casted to double).
    double sum1_f = double(1.10f) + double(1.13f) + double(1.13f) + double(1.10f) + double(1.20f);
    double res = t.where().sum_float(col_float);
    CHECK_APPROXIMATELY_EQUAL(sum1_f, res, 10 * epsilon);

    // ... with conditions
    double sum2_f = double(1.13f) + double(1.20f);
    double sum2_d = 2.21 + 3.20;
    Query q2 = t.where().between(col_float, 1.13f, 1.20f).not_equal(col_double, 2.22);
    CHECK_APPROXIMATELY_EQUAL(sum2_f, q2.sum_float(col_float), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum2_d, q2.sum_double(col_double), 10 * epsilon);

    // ------ Test average()

    // ... NO conditions
    CHECK_APPROXIMATELY_EQUAL(sum1_f / 5, t.where().average_float(col_float), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum1_d / 5, t.where().average_double(col_double), 10 * epsilon);
    // ... with conditions
    CHECK_APPROXIMATELY_EQUAL(sum2_f / 2, q2.average_float(col_float), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum2_d / 2, q2.average_double(col_double), 10 * epsilon);

    // -------- Test minimum(), maximum()

    ObjKey ndx;

    // ... NO conditions
    CHECK_EQUAL(1.20f, t.where().maximum_float(col_float));
    t.where().maximum_float(col_float, &ndx);
    CHECK_EQUAL(k4, ndx);

    CHECK_EQUAL(1.10f, t.where().minimum_float(col_float));
    t.where().minimum_float(col_float, &ndx);
    CHECK_EQUAL(k0, ndx);

    CHECK_EQUAL(3.20, t.where().maximum_double(col_double));
    CHECK_EQUAL(3.20, t.where().maximum_double(col_double, &ndx));

    CHECK_EQUAL(2.20, t.where().minimum_double(col_double));
    t.where().minimum_double(col_double, &ndx);

    // ... with conditions
    CHECK_EQUAL(1.20f, q2.maximum_float(col_float));
    q2.maximum_float(col_float, &ndx);
    CHECK_EQUAL(k4, ndx);

    CHECK_EQUAL(1.13f, q2.minimum_float(col_float));
    q2.minimum_float(col_float, &ndx);
    CHECK_EQUAL(k1, ndx);

    CHECK_EQUAL(3.20, q2.maximum_double(col_double));
    q2.maximum_double(col_double, &ndx);
    CHECK_EQUAL(k4, ndx);

    CHECK_EQUAL(2.21, q2.minimum_double(col_double));
    q2.minimum_double(col_double, &ndx);
    CHECK_EQUAL(k1, ndx);
}


TEST(Query_DoubleCoordinates)
{
    Group group;
    TableRef table = group.add_table("test");

    auto col0 = table->add_column(type_Double, "name");
    auto col1 = table->add_column(type_Double, "age");

    size_t expected = 0;

    for (size_t t = 0; t < 100000; t++) {
        Obj obj = table->create_object().set_all(double((t * 12345) % 1000), double((t * 12345) % 1000));

        if (obj.get<double>(col0) >= 100. && obj.get<double>(col0) <= 110. && obj.get<double>(col1) >= 100. &&
            obj.get<double>(col1) <= 110.) {
            expected++;
        }
    }

    // This unit test can be used as benchmark. Just enable this for loop
    //    for (size_t t = 0; t < 1000; t++) {
    Query q = table->column<double>(col0) >= 100. && table->column<double>(col0) <= 110. &&
              table->column<double>(col1) >= 100. && table->column<double>(col1) <= 110.;

    size_t c = q.count();
    CHECK_EQUAL(c, expected);
}


TEST_TYPES(Query_StrIndexed, std::true_type, std::false_type)
{
    Table ttt;
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    for (size_t t = 0; t < 10; t++) {
        ttt.create_object().set_all(1, "a");
        ttt.create_object().set_all(4, "b");
        ttt.create_object().set_all(7, "c");
        ttt.create_object().set_all(10, "a");
        ttt.create_object().set_all(1, "b");
        ttt.create_object().set_all(4, "c");
    }

    if (TEST_TYPE::value == true) {
        ttt.enumerate_string_column(col_str);
    }

    ttt.add_search_index(col_str);

    int64_t s = ttt.where().equal(col_str, "a").sum_int(col_int);
    CHECK_EQUAL(10 * 11, s);

    s = ttt.where().equal(col_str, "a").equal(col_int, 10).sum_int(col_int);
    CHECK_EQUAL(100, s);

    s = ttt.where().equal(col_int, 10).equal(col_str, "a").sum_int(col_int);
    CHECK_EQUAL(100, s);

    TableView tv = ttt.where().equal(col_str, "a").find_all();
    CHECK_EQUAL(10 * 2, tv.size());
}

TEST(Query_FindAllContains2_2)
{
    Table ttt;
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, "foo");
    ttt.create_object().set_all(1, "foobar");
    ttt.create_object().set_all(2, "hellofoobar");
    ttt.create_object().set_all(3, "foO");
    ttt.create_object().set_all(4, "foObar");
    ttt.create_object().set_all(5, "hellofoObar");
    ttt.create_object().set_all(6, "hellofo");
    ttt.create_object().set_all(7, "fobar");
    ttt.create_object().set_all(8, "oobar");

    // FIXME: UTF-8 case handling is only implemented on msw for now
    Query q1 = ttt.where().contains(col_str, "foO", false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(6, tv1.size());
    CHECK_EQUAL(0, tv1.get(0).get<Int>(col_int));
    CHECK_EQUAL(1, tv1.get(1).get<Int>(col_int));
    CHECK_EQUAL(2, tv1.get(2).get<Int>(col_int));
    CHECK_EQUAL(3, tv1.get(3).get<Int>(col_int));
    CHECK_EQUAL(4, tv1.get(4).get<Int>(col_int));
    CHECK_EQUAL(5, tv1.get(5).get<Int>(col_int));
    Query q2 = ttt.where().contains(col_str, "foO", true);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(3, tv2.size());
    CHECK_EQUAL(3, tv2.get(0).get<Int>(col_int));
    CHECK_EQUAL(4, tv2.get(1).get<Int>(col_int));
    CHECK_EQUAL(5, tv2.get(2).get<Int>(col_int));
}

TEST(Query_SumNewAggregates)
{
    // test the new ACTION_FIND_PATTERN() method in array
    Table t;
    auto col_int = t.add_column(type_Int, "1");
    for (size_t i = 0; i < 1000; i++) {
        t.create_object().set(col_int, 1);
        t.create_object().set(col_int, 2);
        t.create_object().set(col_int, 4);
        t.create_object().set(col_int, 6);
    }
    size_t c = t.where().equal(col_int, 2).count();
    CHECK_EQUAL(1000, c);

    c = t.where().greater(col_int, 2).count();
    CHECK_EQUAL(2000, c);
}


TEST(Query_SumMinMaxAvgForeignCol)
{
    Table t;
    auto col_int0 = t.add_column(type_Int, "1");
    auto col_int1 = t.add_column(type_Int, "2");

    t.create_object().set_all(1, 10);
    t.create_object().set_all(2, 20);
    t.create_object().set_all(2, 30);
    t.create_object().set_all(4, 40);

    CHECK_EQUAL(50, t.where().equal(col_int0, 2).sum_int(col_int1));
}

TEST(Query_AggregateSingleCond)
{
    Table t;
    auto col_int = t.add_column(type_Int, "1");

    t.create_object().set(col_int, 1);
    t.create_object().set(col_int, 2);
    t.create_object().set(col_int, 2);
    t.create_object().set(col_int, 3);
    t.create_object().set(col_int, 3);
    t.create_object().set(col_int, 4);

    int64_t s = t.where().equal(col_int, 2).sum_int(col_int);
    CHECK_EQUAL(4, s);

    s = t.where().greater(col_int, 2).sum_int(col_int);
    CHECK_EQUAL(10, s);

    s = t.where().less(col_int, 3).sum_int(col_int);
    CHECK_EQUAL(5, s);

    s = t.where().not_equal(col_int, 3).sum_int(col_int);
    CHECK_EQUAL(9, s);
}

TEST(Query_FindAllRange1)
{
    Table ttt;
    ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(1, "a");
    ttt.create_object().set_all(4, "a");
    ttt.create_object().set_all(7, "a");
    ttt.create_object().set_all(10, "a");
    ttt.create_object().set_all(1, "a");
    ttt.create_object().set_all(4, "a");
    ttt.create_object().set_all(7, "a");
    ttt.create_object().set_all(10, "a");
    ttt.create_object().set_all(1, "a");
    ttt.create_object().set_all(4, "a");
    ttt.create_object().set_all(7, "a");
    ttt.create_object().set_all(10, "a");

    Query q1 = ttt.where().equal(col_str, "a");
    TableView tv1 = q1.find_all(4, 10);
    CHECK_EQUAL(6, tv1.size());
}


TEST(Query_FindAllRangeOrMonkey2)
{
    const size_t ROWS = 20;
    const size_t ITER = 100;

    Random random(random_int<unsigned long>()); // Seed from slow global generator

    for (size_t u = 0; u < ITER; u++) {
        Table tit;
        auto col_int0 = tit.add_column(type_Int, "1");
        auto col_int1 = tit.add_column(type_Int, "2");

        std::vector<ObjKey> a;
        std::vector<ObjKey> keys;
        size_t start = random.draw_int_max(ROWS);
        size_t end = start + random.draw_int_max(ROWS - start);

        if (end > ROWS)
            end = ROWS;

        for (size_t t = 0; t < ROWS; t++) {
            int64_t r1 = random.draw_int_mod(10);
            int64_t r2 = random.draw_int_mod(10);
            tit.create_object().set_all(r1, r2).get_key();
        }

        Query q1 = tit.where().group().equal(col_int0, 3).Or().equal(col_int0, 7).end_group().greater(col_int1, 5);
        TableView tv1 = q1.find_all(start, end);

        auto it = tit.begin() + start;
        auto e = tit.begin() + end;
        while (it != e) {
            if ((it->get<Int>(col_int0) == 3 || it->get<Int>(col_int0) == 7) && it->get<Int>(col_int1) > 5)
                a.push_back(it->get_key());
            ++it;
        }
        size_t s1 = a.size();
        size_t s2 = tv1.size();
        if (s1 != s2) {
            printf("Error\n");
        }

        CHECK_EQUAL(s1, s2);
        for (size_t t = 0; t < a.size(); t++) {
            CHECK_EQUAL(tv1.get_key(t), a[t]);
        }
    }
}

TEST(Query_FindAllRangeOr)
{
    Table ttt;
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(1, "b");
    ttt.create_object().set_all(2, "a"); //// match
    ttt.create_object().set_all(3, "b"); //
    ttt.create_object().set_all(1, "a"); //// match
    ttt.create_object().set_all(2, "b"); //// match
    ttt.create_object().set_all(3, "a");
    ttt.create_object().set_all(1, "b");
    ttt.create_object().set_all(2, "a"); //// match
    ttt.create_object().set_all(3, "b"); //

    Query q1 = ttt.where().group().greater(col_int, 1).Or().equal(col_str, "a").end_group().less(col_int, 3);
    TableView tv1 = q1.find_all(1, 8);
    CHECK_EQUAL(4, tv1.size());

    TableView  tv2 = q1.find_all(2, 8);
    CHECK_EQUAL(3, tv2.size());

    TableView  tv3 = q1.find_all(1, 7);
    CHECK_EQUAL(3, tv3.size());
}


TEST(Query_SimpleStr)
{
    Table ttt;
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(1, "X");
    ttt.create_object().set_all(2, "a");
    ttt.create_object().set_all(3, "X");
    ttt.create_object().set_all(4, "a");
    ttt.create_object().set_all(5, "X");
    ttt.create_object().set_all(6, "X");

    Query q = ttt.where().equal(col_str, "X");
    size_t c = q.count();
    CHECK_EQUAL(4, c);

    size_t r = q.remove();
    CHECK_EQUAL(4, r);
    CHECK_EQUAL(2, ttt.size());
    CHECK_EQUAL(2, ttt.get_object(0).get<Int>(col_int));
    CHECK_EQUAL(4, ttt.get_object(1).get<Int>(col_int));

    // test remove of all
    Query q2 = ttt.where().greater(col_int, 0);
    r = q2.remove();
    CHECK_EQUAL(2, r);
    CHECK_EQUAL(0, ttt.size());
}


TEST(Query_Simple)
{
    Table ttt;
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ObjKey k0 = ttt.create_object().set_all(1, "a").get_key();
    ObjKey k1 = ttt.create_object().set_all(2, "a").get_key();
    ObjKey k2 = ttt.create_object().set_all(3, "X").get_key();

    Query q0 = ttt.where();

    TableView tv0 = q0.find_all();
    CHECK_EQUAL(3, tv0.size());
    CHECK_EQUAL(k0, tv0.get_key(0));

    Query q1 = ttt.where().equal(col_int, 2);

    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(k1, tv1.get_key(0));

    Query q2 = ttt.where().Not().equal(col_str, "a");

    TableView tv2 = q2.find_all();
    CHECK_EQUAL(1, tv2.size());
    CHECK_EQUAL(k2, tv2.get_key(0));
}

TEST(Query_Sort1)
{
    Table ttt;
    auto col_int = ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    ttt.create_object().set_all(1, "a"); // 0
    ttt.create_object().set_all(2, "a"); // 1
    ttt.create_object().set_all(3, "X"); // 2
    ttt.create_object().set_all(1, "a"); // 3
    ttt.create_object().set_all(2, "a"); // 4
    ttt.create_object().set_all(3, "X"); // 5
    ttt.create_object().set_all(9, "a"); // 6
    ttt.create_object().set_all(8, "a"); // 7
    ttt.create_object().set_all(7, "X"); // 8

    // tv.get_key()  = 0, 2, 3, 5, 6, 7, 8
    // Vals         = 1, 3, 1, 3, 9, 8, 7
    // result       = 3, 0, 5, 2, 8, 7, 6

    Query q = ttt.where().not_equal(col_int, 2);
    TableView tv = q.find_all();
    tv.sort(col_int);

    CHECK_EQUAL(tv.size(), 7);
    CHECK_EQUAL(tv[0].get<Int>(col_int), 1);
    CHECK_EQUAL(tv[1].get<Int>(col_int), 1);
    CHECK_EQUAL(tv[2].get<Int>(col_int), 3);
    CHECK_EQUAL(tv[3].get<Int>(col_int), 3);
    CHECK_EQUAL(tv[4].get<Int>(col_int), 7);
    CHECK_EQUAL(tv[5].get<Int>(col_int), 8);
    CHECK_EQUAL(tv[6].get<Int>(col_int), 9);

    CHECK_EQUAL(tv[0].get_key(), ObjKey(0));
    CHECK_EQUAL(tv[1].get_key(), ObjKey(3));
    CHECK_EQUAL(tv[2].get_key(), ObjKey(2));
    CHECK_EQUAL(tv[3].get_key(), ObjKey(5));
    CHECK_EQUAL(tv[4].get_key(), ObjKey(8));
    CHECK_EQUAL(tv[5].get_key(), ObjKey(7));
    CHECK_EQUAL(tv[6].get_key(), ObjKey(6));

    tv = q.find_all();
    tv.sort(col_int, false);

    CHECK_EQUAL(tv.size(), 7);
    CHECK_EQUAL(tv[0].get_key(), ObjKey(6));
    CHECK_EQUAL(tv[1].get_key(), ObjKey(7));
    CHECK_EQUAL(tv[2].get_key(), ObjKey(8));
    CHECK_EQUAL(tv[3].get_key(), ObjKey(2));
    CHECK_EQUAL(tv[4].get_key(), ObjKey(5));
    CHECK_EQUAL(tv[5].get_key(), ObjKey(0));
    CHECK_EQUAL(tv[6].get_key(), ObjKey(3));
}

TEST(Query_SortAscending)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    Table ttt;
    auto col_int = ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    for (size_t t = 0; t < 1000; t++)
        ttt.create_object().set_all(random.draw_int_mod(1100), "a"); // 0

    Query q = ttt.where();
    TableView tv = q.find_all();
    tv.sort(col_int);

    CHECK(tv.size() == 1000);
    for (size_t t = 1; t < tv.size(); t++) {
        CHECK(tv[t].get<Int>(col_int) >= tv[t - 1].get<Int>(col_int));
    }
}

TEST(Query_SortDescending)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    Table ttt;
    auto col_int = ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    for (size_t t = 0; t < 1000; t++)
        ttt.create_object().set_all(random.draw_int_mod(1100), "a"); // 0

    Query q = ttt.where();
    TableView tv = q.find_all();
    tv.sort(col_int, false);

    CHECK(tv.size() == 1000);
    for (size_t t = 1; t < tv.size(); t++) {
        CHECK(tv[t].get<Int>(col_int) <= tv[t - 1].get<Int>(col_int));
    }
}


TEST(Query_SortDates)
{
    Table table;
    auto col_date = table.add_column(type_Timestamp, "first");

    table.create_object().set(col_date, Timestamp(1000, 0));
    table.create_object().set(col_date, Timestamp(3000, 0));
    table.create_object().set(col_date, Timestamp(2000, 0));

    TableView tv = table.where().find_all();
    CHECK(tv.size() == 3);

    tv.sort(col_date);
    CHECK_EQUAL(tv[0].get<Timestamp>(col_date), Timestamp(1000, 0));
    CHECK_EQUAL(tv[1].get<Timestamp>(col_date), Timestamp(2000, 0));
    CHECK_EQUAL(tv[2].get<Timestamp>(col_date), Timestamp(3000, 0));
}


TEST(Query_SortBools)
{
    Table table;
    auto col = table.add_column(type_Bool, "first");

    table.create_object().set(col, true);
    table.create_object().set(col, false);
    table.create_object().set(col, true);

    TableView tv = table.where().find_all();
    CHECK(tv.size() == 3);

    tv.sort(col);
    CHECK_EQUAL(tv[0].get<Bool>(col), false);
    CHECK_EQUAL(tv[1].get<Bool>(col), true);
    CHECK_EQUAL(tv[2].get<Bool>(col), true);
}

TEST(Query_SortLinks)
{
    const size_t num_rows = 10;
    Group g;
    TableRef t1 = g.add_table("t1");
    TableRef t2 = g.add_table("t2");

    auto t1_int_col = t1->add_column(type_Int, "t1_int");
    auto t1_str_col = t1->add_column(type_String, "t1_string");
    auto t1_link_t2_col = t1->add_column_link(type_Link, "t1_link_to_t2", *t2);
    t2->add_column(type_Int, "t2_int");
    t2->add_column(type_String, "t2_string");
    t2->add_column_link(type_Link, "t2_link_to_t1", *t1);

    std::vector<ObjKey> t1_keys;
    std::vector<ObjKey> t2_keys;
    t1->create_objects(num_rows, t1_keys);
    t2->create_objects(num_rows, t2_keys);
    std::vector<std::string> ordered_strings;

    for (size_t i = 0; i < num_rows; ++i) {
        ordered_strings.push_back(std::string("a string") + util::to_string(i));
        t1->get_object(t1_keys[i]).set_all(int64_t(i), StringData(ordered_strings[i]), t2_keys[num_rows - i - 1]);
        t2->get_object(t1_keys[i]).set_all(int64_t(i), StringData(ordered_strings[i]), t1_keys[i]);
    }

    TableView tv = t1->where().find_all();

    // Check natural order
    CHECK_EQUAL(tv.size(), num_rows);
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), i);
        CHECK_EQUAL(tv[i].get<String>(t1_str_col), ordered_strings[i]);
    }

    // Check sorted order by ints
    tv.sort(t1_int_col);
    CHECK_EQUAL(tv.size(), num_rows);
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), i);
        CHECK_EQUAL(tv[i].get<String>(t1_str_col), ordered_strings[i]);
    }

    // Check that you can sort on a regular link column
    tv = t1->where().find_all();
    tv.sort(t1_link_t2_col);
    CHECK_EQUAL(tv.size(), num_rows);
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), num_rows - i - 1);
        CHECK_EQUAL(tv[i].get<String>(t1_str_col), ordered_strings[num_rows - i - 1]);
    }
}

TEST(Query_SortLinkChains)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    TableRef t2 = g.add_table("t2");
    TableRef t3 = g.add_table("t3");

    auto t1_int_col = t1->add_column(type_Int, "t1_int");
    auto t1_link_col = t1->add_column_link(type_Link, "t1_link_t2", *t2);

    auto t2_int_col = t2->add_column(type_Int, "t2_int");
    auto t2_link_col = t2->add_column_link(type_Link, "t2_link_t3", *t3);

    auto t3_int_col = t3->add_column(type_Int, "t3_int", true);
    auto t3_str_col = t3->add_column(type_String, "t3_str");

    ObjKeyVector t1_keys({0, 1, 2, 3, 4, 5, 6});
    ObjKeyVector t2_keys({10, 11, 12, 13, 14, 15});
    ObjKeyVector t3_keys({20, 21, 22, 23});
    t1->create_objects(t1_keys);
    t2->create_objects(t2_keys);
    t3->create_objects(t3_keys);

    t1->get_object(t1_keys[0]).set(t1_int_col, 99);
    for (size_t i = 0; i < t2->size(); i++) {
        t1->get_object(t1_keys[i + 1]).set(t1_int_col, int64_t(i));
        t2->get_object(t2_keys[i]).set(t2_int_col, int64_t(t1->size() - i));
    }

    t1->get_object(t1_keys[0]).set(t1_link_col, t2_keys[1]);
    t1->get_object(t1_keys[1]).set(t1_link_col, t2_keys[0]);
    t1->get_object(t1_keys[2]).set(t1_link_col, t2_keys[2]);
    t1->get_object(t1_keys[3]).set(t1_link_col, t2_keys[3]);
    t1->get_object(t1_keys[4]).set(t1_link_col, t2_keys[5]);
    t1->get_object(t1_keys[5]).set(t1_link_col, t2_keys[4]);
    t1->get_object(t1_keys[6]).set(t1_link_col, t2_keys[1]);

    t2->get_object(t2_keys[0]).set(t2_link_col, t3_keys[3]);
    t2->get_object(t2_keys[1]).set(t2_link_col, t3_keys[2]);
    t2->get_object(t2_keys[2]).set(t2_link_col, t3_keys[0]);
    t2->get_object(t2_keys[3]).set(t2_link_col, t3_keys[1]);

    t3->get_object(t3_keys[1]).set(t3_int_col, 4);
    t3->get_object(t3_keys[2]).set(t3_int_col, 7);
    t3->get_object(t3_keys[3]).set(t3_int_col, 3);
    t3->get_object(t3_keys[0]).set(t3_str_col, "b");
    t3->get_object(t3_keys[1]).set(t3_str_col, "a");
    t3->get_object(t3_keys[2]).set(t3_str_col, "c");
    t3->get_object(t3_keys[3]).set(t3_str_col, "k");

    //  T1                       T2                     T3
    //  t1_int   t1_link_t2  |   t2_int  t2_link_t3 |   t3_int  t3_str
    //  ==============================================================
    //  99       11          |   5       23         |   null    "b"
    //  0        10          |   4       22         |   4       "a"
    //  1        12          |   3       20         |   7       "c"
    //  2        13          |   2       21         |   3       "k"
    //  3        15          |   1       null       |
    //  4        14          |   0       null       |
    //  5        11          |                      |

    TableView tv = t1->where().less(t1_int_col, 6).find_all();

    // Test original functionality through chain class
    std::vector<size_t> results1 = {0, 1, 2, 3, 4, 5};
    tv.sort(SortDescriptor(*t1, {{t1_int_col}}, {true}));
    CHECK_EQUAL(tv.size(), results1.size());
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results1[i]);
    }
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor(*t1, {{t1_int_col}}, {false}));
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results1[results1.size() - 1 - i]);
    }

    // Test basic one link chain
    std::vector<size_t> results2 = {3, 4, 2, 1, 5, 0};
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_int_col}}, {true}));
    CHECK_EQUAL(tv.size(), results2.size());
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results2[i]);
    }
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_int_col}}, {false}));
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results2[results2.size() - 1 - i]);
    }

    // Test link chain through two links with nulls
    std::vector<size_t> results3 = {1, 0, 2, 5};
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}}, {true}));
    // No guarantees about nullified links except they are at the end.
    CHECK(tv.size() >= results3.size());
    util::Optional<int64_t> last;
    for (size_t i = 0; i < results3.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results3[i]);
        util::Optional<int64_t> current = tv[i]
                                              .get_linked_object(t1_link_col)
                                              .get_linked_object(t2_link_col)
                                              .get<util::Optional<int64_t>>(t3_int_col);
        CHECK(!last || current.value() >= last.value());
        last = current;
    }
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}}, {false}));
    // No guarantees about nullified links except they are at the beginning.
    size_t num_nulls = tv.size() - results3.size();
    for (size_t i = num_nulls; i < results3.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results3[results2.size() - 1 - i]);
    }

    // Test link chain with nulls and a single local column
    std::vector<size_t> results4 = {1, 0, 2, 5, 3, 4};
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}, {t1_int_col}}));
    CHECK_EQUAL(tv.size(), results4.size());
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results4[i]);
    }
    std::vector<size_t> results4_rev = {1, 0, 2, 5, 4, 3};
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}, {t1_int_col}}, {true, false}));
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results4_rev[i]);
    }
    std::vector<size_t> results4_rev2 = {3, 4, 5, 2, 0, 1};
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}, {t1_int_col}}, {false, true}));
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results4_rev2[i]);
    }
    std::vector<size_t> results4_rev3 = {4, 3, 5, 2, 0, 1};
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}, {t1_int_col}}, {false, false}));
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results4_rev3[i]);
    }
}

TEST(Query_LinkChainSortErrors)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    TableRef t2 = g.add_table("t2");

    auto t1_int_col = t1->add_column(type_Int, "t1_int");
    auto t1_linklist_col = t1->add_column_link(type_LinkList, "t1_linklist", *t2);
    auto t2_string_col = t2->add_column(type_String, "t2_string");
    t2->add_column_link(type_Link, "t2_link_t1", *t1); // add a backlink to t1

    // Disallow invalid column ids, linklists, other non-link column types.
    ColKey backlink_ndx(2);
    CHECK_LOGIC_ERROR(SortDescriptor(*t1, {{t1_linklist_col, t2_string_col}}), LogicError::type_mismatch);
    CHECK_LOGIC_ERROR(SortDescriptor(*t1, {{backlink_ndx, t2_string_col}}), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(SortDescriptor(*t1, {{t1_int_col, t2_string_col}}), LogicError::type_mismatch);
}


TEST(Query_EmptyDescriptors)
{
    Group g;
    TableRef t1 = g.add_table("t1");

    auto t1_int_col = t1->add_column(type_Int, "t1_int");

    t1->create_object().set(t1_int_col, 4);
    t1->create_object().set(t1_int_col, 3);
    t1->create_object().set(t1_int_col, 2);
    t1->create_object().set(t1_int_col, 3);

    std::vector<size_t> results = {4, 3, 2, 3}; // original order

    {   // Sorting with an empty sort descriptor is a no-op
        TableView tv = t1->where().find_all();
        tv.sort(SortDescriptor(*t1, {}));
        for (size_t i = 0; i < results.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i]);
        }
    }
    {   // Distinct with an empty descriptor is a no-op
        TableView tv = t1->where().find_all();
        tv.distinct(DistinctDescriptor(*t1, {}));
        for (size_t i = 0; i < results.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i]);
        }
    }
    {   // Empty sort, empty distinct is still a no-op
        TableView tv = t1->where().find_all();
        tv.sort(SortDescriptor(*t1, {}));
        tv.distinct(DistinctDescriptor(*t1, {}));
        for (size_t i = 0; i < results.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i]);
        }
    }
    {   // Arbitrary compounded empty sort and distinct is still a no-op
        TableView tv = t1->where().find_all();
        tv.sort(SortDescriptor(*t1, {}));
        tv.sort(SortDescriptor(*t1, {}));
        tv.distinct(DistinctDescriptor(*t1, {}));
        tv.sort(SortDescriptor(*t1, {}));
        tv.distinct(DistinctDescriptor(*t1, {}));
        tv.distinct(DistinctDescriptor(*t1, {}));
        tv.distinct(DistinctDescriptor(*t1, {}));
        for (size_t i = 0; i < results.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i]);
        }
    }
    {   // Empty distinct compounded on a valid distinct is a no-op
        TableView tv = t1->where().find_all();
        tv.distinct(DistinctDescriptor(*t1, {}));
        tv.distinct(DistinctDescriptor(*t1, {{t1_int_col}}));
        tv.distinct(DistinctDescriptor(*t1, {}));
        results = {4, 3, 2};
        for (size_t i = 0; i < results.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i]);
        }
    }
    {   // Empty sort compounded on a valid sort is a no-op
        TableView tv = t1->where().find_all();
        tv.sort(SortDescriptor(*t1, {}));
        tv.sort(SortDescriptor(*t1, {{t1_int_col}}));
        tv.sort(SortDescriptor(*t1, {}));
        results = {2, 3, 3, 4};
        for (size_t i = 0; i < results.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i]);
        }
    }
}

TEST(Query_DescriptorsWillApply)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    auto t1_int_col = t1->add_column(type_Int, "t1_int");
    auto t1_str_col = t1->add_column(type_String, "t1_str");

    t1->create_object();

    DescriptorOrdering ordering;

    CHECK(!ordering.will_apply_sort());
    CHECK(!ordering.will_apply_distinct());

    ordering.append_sort(SortDescriptor());
    CHECK(!ordering.will_apply_sort());
    CHECK(!ordering.will_apply_distinct());

    ordering.append_distinct(DistinctDescriptor());
    CHECK(!ordering.will_apply_sort());
    CHECK(!ordering.will_apply_distinct());

    ordering.append_sort(SortDescriptor(*t1, {{t1_int_col}}));
    CHECK(ordering.will_apply_sort());
    CHECK(!ordering.will_apply_distinct());

    ordering.append_distinct(DistinctDescriptor(*t1, {{t1_int_col}}));
    CHECK(ordering.will_apply_sort());
    CHECK(ordering.will_apply_distinct());

    ordering.append_distinct(DistinctDescriptor(*t1, {{t1_str_col}}));
    CHECK(ordering.will_apply_sort());
    CHECK(ordering.will_apply_distinct());

    ordering.append_sort(SortDescriptor(*t1, {{t1_str_col}}));
    CHECK(ordering.will_apply_sort());
    CHECK(ordering.will_apply_distinct());

    DescriptorOrdering ordering_copy = ordering;
    CHECK(ordering.will_apply_sort());
    CHECK(ordering.will_apply_distinct());
    CHECK(ordering_copy.will_apply_sort());
    CHECK(ordering_copy.will_apply_distinct());
}

TEST(Query_DistinctAndSort)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    TableRef t2 = g.add_table("t2");
    auto t1_int_col = t1->add_column(type_Int, "t1_int");
    auto t1_str_col = t1->add_column(type_String, "t1_str");
    auto t1_link_col = t1->add_column_link(type_Link, "t1_link_t2", *t2);
    auto t2_int_col = t2->add_column(type_Int, "t2_int");

    ObjKeyVector t1_keys({0, 1, 2, 3, 4, 5});
    ObjKeyVector t2_keys({10, 11, 12, 13, 14, 15});
    t1->create_objects(t1_keys);
    t2->create_objects(t2_keys);

    t1->get_object(t1_keys[0]).set_all(1, "A", t2_keys[1]);
    t1->get_object(t1_keys[1]).set_all(1, "A", t2_keys[0]);
    t1->get_object(t1_keys[2]).set_all(1, "B", t2_keys[2]);
    t1->get_object(t1_keys[3]).set_all(2, "B", t2_keys[3]);
    t1->get_object(t1_keys[4]).set_all(2, "A", t2_keys[5]);
    t1->get_object(t1_keys[5]).set_all(2, "A", t2_keys[4]);

    t2->get_object(t2_keys[0]).set(t2_int_col, 0);
    t2->get_object(t2_keys[1]).set(t2_int_col, 0);
    t2->get_object(t2_keys[2]).set(t2_int_col, 1);
    t2->get_object(t2_keys[3]).set(t2_int_col, 1);
    t2->get_object(t2_keys[4]).set(t2_int_col, 2);
    t2->get_object(t2_keys[5]).set(t2_int_col, 2);

    //     T1                              T2
    //   | t1_int   t1_str   t1_link_t2  | t2_int  |
    //   ===========================================
    // 0 | 1        "A"      1           | 0       |
    // 1 | 1        "A"      0           | 0       |
    // 2 | 1        "B"      2           | 1       |
    // 3 | 2        "B"      3           | 1       |
    // 4 | 2        "A"      5           | 2       |
    // 5 | 2        "A"      4           | 2       |

    using ResultList = std::vector<std::pair<size_t, ObjKey>>; // value, key
    {   // distinct with no sort keeps original order
        TableView tv = t1->where().find_all();
        ResultList expected = {{1, t1_keys[0]}, {2, t1_keys[3]}};
        tv.distinct(t1_int_col);
        CHECK_EQUAL(tv.size(), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    {   // distinct on a sorted view retains sorted order
        TableView tv = t1->where().find_all();
        ResultList expected = {{1, t1_keys[0]}, {2, t1_keys[4]}};
        tv.sort(SortDescriptor(*t1, {{t1_str_col}, {t1_int_col}}));
        tv.distinct(t1_int_col);
        CHECK_EQUAL(tv.size(), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    {   // distinct on a view sorted descending retains sorted order
        TableView tv = t1->where().find_all();
        ResultList expected = {{2, t1_keys[3]}, {1, t1_keys[2]}};
        tv.sort(SortDescriptor(*t1, {{t1_str_col}, {t1_int_col}},
                            {false /* descending */, false /* descending */}));
        tv.distinct(t1_int_col);
        CHECK_EQUAL(tv.size(), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    {   // distinct on a sorted view (different from table order) retains sorted order
        TableView tv = t1->where().find_all();
        ResultList expected = {{2, t1_keys[3]}, {1, t1_keys[0]}};
        tv.sort(t1_int_col, false /* descending */);
        tv.distinct(t1_int_col);
        CHECK_EQUAL(tv.size(), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    {   // distinct across links on an unsorted view retains original order
        TableView tv = t1->where().find_all();
        ResultList expected = {{1, t1_keys[0]}, {1, t1_keys[2]}, {2, t1_keys[4]}};
        tv.distinct(DistinctDescriptor(*t1, {{t1_link_col, t2_int_col}}));
        CHECK_EQUAL(tv.size(), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    {   // distinct on a view sorted across links retains sorted order
        TableView tv = t1->where().find_all();
        ResultList expected = {{1, t1_keys[0]}, {2, t1_keys[3]}};
        tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_int_col}}));
        tv.distinct(t1_int_col);
        CHECK_EQUAL(tv.size(), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    {   // distinct across links and sort across links
        TableView tv = t1->where().find_all();
        ResultList expected = {{1, t1_keys[0]}, {1, t1_keys[2]}, {2, t1_keys[4]}};
        tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_int_col}}));
        tv.distinct(DistinctDescriptor(*t1, {{t1_link_col, t2_int_col}}));
        CHECK_EQUAL(tv.size(), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
}

#ifdef LEGACY_TESTS
TEST(Query_SortDistinctOrderThroughHandover) {
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DB sg_w(*hist_w, SharedGroupOptions(crypt_key()));
    Group& g = sg_w.begin_write();

    TableRef t1 = g.add_table("t1");
    auto t1_int_col = t1->add_column(type_Int, "t1_int");
    auto t1_str_col = t1->add_column(type_String, "t1_str");

    ObjKey k0 = t1->create_object().set_all(100, "A").get_key();
    ObjKey k1 = t1->create_object().set_all(200, "A").get_key();
    ObjKey k2 = t1->create_object().set_all(300, "A").get_key();
    t1->create_object().set_all(300, "A");
    ObjKey k4 = t1->create_object().set_all(400, "A").get_key();

    LangBindHelper::commit_and_continue_as_read(sg_w);
    DB::VersionID version_id = sg_w.get_version_of_current_transaction();
    using HandoverPtr = std::unique_ptr<DB::Handover<TableView>>;
    using ResultList = std::vector<std::pair<std::string, ObjKey>>;

    auto check_across_handover = [&](ResultList results, HandoverPtr handover) {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DB sg(*hist, SharedGroupOptions(crypt_key()));
        sg.begin_read();
        LangBindHelper::advance_read(sg, version_id);
        auto tv = sg.import_from_handover(std::move(handover));
        tv->sync_if_needed();
        CHECK(tv->is_in_sync());
        CHECK_EQUAL(tv->size(), results.size());
        for (size_t i = 0; i < tv->size(); ++i) {
            CHECK_EQUAL(tv->get(i).get<String>(t1_str_col), results[i].first);
            CHECK_EQUAL(tv->get_key(i), results[i].second);
        }
        sg.end_read();
    };

    //     T1
    //   | t1_int     t1_str   |
    //   =======================
    // 0 | 100        "A"      |
    // 1 | 200        "A"      |
    // 2 | 300        "A"      |
    // 3 | 300        "A"      |
    // 4 | 400        "A"      |

    {   // sort descending then distinct
        TableView tv = t1->where().find_all();
        ResultList results = {{"A", k4}};
        tv.sort(SortDescriptor(*t1, {{t1_int_col}}, {false}));
        tv.distinct(DistinctDescriptor(*t1, {{t1_str_col}}));

        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get(i).get<String>(t1_str_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        HandoverPtr hp = sg_w.export_for_handover(tv, ConstSourcePayload::Stay);
        check_across_handover(results, std::move(hp));
    }
    {   // distinct then sort descending
        TableView tv = t1->where().find_all();
        std::vector<std::pair<std::string, ObjKey>> results = {{"A", k0}};
        tv.distinct(DistinctDescriptor(*t1, {{t1_str_col}}));
        tv.sort(SortDescriptor(*t1, {{t1_int_col}}, {false}));
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get(i).get<String>(t1_str_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        HandoverPtr hp = sg_w.export_for_handover(tv, ConstSourcePayload::Stay);
        check_across_handover(results, std::move(hp));
    }
    {   // sort descending then multicolumn distinct
        TableView tv = t1->where().find_all();
        std::vector<std::pair<std::string, ObjKey>> results = {{"A", k4}, {"A", k2}, {"A", k1}, {"A", k0}};
        tv.sort(SortDescriptor(*t1, {{t1_int_col}}, {false}));
        tv.distinct(DistinctDescriptor(*t1, {{t1_str_col}, {t1_int_col}}));
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get(i).get<String>(t1_str_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        HandoverPtr hp = sg_w.export_for_handover(tv, ConstSourcePayload::Stay);
        check_across_handover(results, std::move(hp));
    }
    {   // multicolumn distinct then sort descending
        TableView tv = t1->where().find_all();
        std::vector<std::pair<std::string, ObjKey>> results = {{"A", k4}, {"A", k2}, {"A", k1}, {"A", k0}};
        tv.distinct(DistinctDescriptor(*t1, {{t1_str_col}, {t1_int_col}}));
        tv.sort(SortDescriptor(*t1, {{t1_int_col}}, {false}));
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get(i).get<String>(t1_str_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        HandoverPtr hp = sg_w.export_for_handover(tv, ConstSourcePayload::Stay);
        check_across_handover(results, std::move(hp));
    }
}


TEST(Query_CompoundDescriptors) {
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DB sg_w(*hist_w, SharedGroupOptions(crypt_key()));
    Group& g = sg_w.begin_write();

    TableRef t1 = g.add_table("t1");
    ColKey t1_int_col = t1->add_column(type_Int, "t1_int");
    ColKey t1_str_col = t1->add_column(type_String, "t1_str");

    ObjKey k0 = t1->create_object().set_all(1, "A").get_key();
    ObjKey k1 = t1->create_object().set_all(1, "A").get_key();
    ObjKey k2 = t1->create_object().set_all(1, "B").get_key();
    ObjKey k3 = t1->create_object().set_all(2, "B").get_key();
    ObjKey k4 = t1->create_object().set_all(2, "A").get_key();
    ObjKey k5 = t1->create_object().set_all(2, "A").get_key();

    LangBindHelper::commit_and_continue_as_read(sg_w);
    DB::VersionID version_id = sg_w.get_version_of_current_transaction();
    using HandoverPtr = std::unique_ptr<DB::Handover<TableView>>;
    using ResultList = std::vector<std::pair<size_t, ObjKey>>; // value, index

    auto check_across_handover = [&](ResultList results, HandoverPtr handover) {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DB sg(*hist, SharedGroupOptions(crypt_key()));
        sg.begin_read();
        LangBindHelper::advance_read(sg, version_id);
        auto tv = sg.import_from_handover(std::move(handover));
        tv->sync_if_needed();
        CHECK(tv->is_in_sync());
        CHECK_EQUAL(tv->size(), results.size());
        for (size_t i = 0; i < tv->size(); ++i) {
            CHECK_EQUAL(tv->get(i).get<Int>(t1_int_col), results[i].first);
            CHECK_EQUAL(tv->get_key(i), results[i].second);
        }
        sg.end_read();
    };

    //     T1
    //   | t1_int   t1_str  |
    //   ====================
    // 0 | 1        "A"     |
    // 1 | 1        "A"     |
    // 2 | 1        "B"     |
    // 3 | 2        "B"     |
    // 4 | 2        "A"     |
    // 5 | 2        "A"     |

    {   // sorting twice should the same as a single sort with both criteria
        // but reversed: sort(a).sort(b) == sort(b, a)
        ResultList results = {{2, k3}, {1, k2}, {2, k4}, {2, k5}, {1, k0}, {1, k1}};
        TableView tv = t1->where().find_all();
        tv.sort(SortDescriptor(*t1, {{t1_int_col}}, {false}));
        tv.sort(SortDescriptor(*t1, {{t1_str_col}}, {false}));
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        HandoverPtr hp = sg_w.export_for_handover(tv, ConstSourcePayload::Stay);
        check_across_handover(results, std::move(hp));

        tv = t1->where().find_all();
        tv.sort(SortDescriptor(*t1, {{t1_str_col}, {t1_int_col}}, {false, false}));
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        hp = sg_w.export_for_handover(tv, ConstSourcePayload::Stay);
        check_across_handover(results, std::move(hp));
    }

    {   // two distincts are not the same as a single distinct with both criteria
        ResultList results = {{1, k0}, {2, k3}};
        TableView tv = t1->where().find_all();
        tv.distinct(DistinctDescriptor(*t1, {{t1_int_col}}));
        tv.distinct(DistinctDescriptor(*t1, {{t1_str_col}}));
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        HandoverPtr hp = sg_w.export_for_handover(tv, ConstSourcePayload::Stay);
        check_across_handover(results, std::move(hp));

        results = {{1, k0}, {1, k2}, {2, k3}, {2, k4}};
        tv = t1->where().find_all();
        tv.distinct(DistinctDescriptor(*t1, {{t1_int_col}, {t1_str_col}}));
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        hp = sg_w.export_for_handover(tv, ConstSourcePayload::Stay);
        check_across_handover(results, std::move(hp));
    }

    {   // check results of sort-distinct-sort-distinct
        TableView tv = t1->where().find_all();
        tv.sort(SortDescriptor(*t1, {{t1_str_col}, {t1_int_col}}, {true, true}));
        tv.distinct(DistinctDescriptor(*t1, {{t1_int_col}}));
        ResultList results = {{1, k0}, {2, k4}};
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        HandoverPtr hp = sg_w.export_for_handover(tv, ConstSourcePayload::Stay);
        check_across_handover(results, std::move(hp));

        tv.sort(SortDescriptor(*t1, {{t1_int_col}}, {false})); // = {{2, 4}, {1, 0}}
        tv.distinct(DistinctDescriptor(*t1, {{t1_str_col}}));  // = {{2, 4}}
        results = {{2, k4}};
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        hp = sg_w.export_for_handover(tv, ConstSourcePayload::Stay);
        check_across_handover(results, std::move(hp));
    }
}
#endif

TEST(Query_DistinctThroughLinks)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    TableRef t2 = g.add_table("t2");
    TableRef t3 = g.add_table("t3");

    auto t1_int_col = t1->add_column(type_Int, "t1_int");
    auto t1_link_col = t1->add_column_link(type_Link, "t1_link_t2", *t2);

    auto t2_int_col = t2->add_column(type_Int, "t2_int");
    auto t2_link_col = t2->add_column_link(type_Link, "t2_link_t3", *t3);

    auto t3_int_col = t3->add_column(type_Int, "t3_int", true);
    auto t3_str_col = t3->add_column(type_String, "t3_str");

    ObjKeyVector t1_keys({0, 1, 2, 3, 4, 5, 6});
    ObjKeyVector t2_keys({10, 11, 12, 13, 14, 15});
    ObjKeyVector t3_keys({20, 21, 22, 23});
    t1->create_objects(t1_keys);
    t2->create_objects(t2_keys);
    t3->create_objects(t3_keys);

    t1->get_object(t1_keys[0]).set(t1_int_col, 99);
    for (size_t i = 0; i < t2->size(); i++) {
        t1->get_object(t1_keys[i + 1]).set(t1_int_col, int64_t(i));
        t2->get_object(t2_keys[i]).set(t2_int_col, int64_t(t2->size() - i - 1));
    }
    t2->get_object(t2_keys[0]).set(t2_int_col, 0);
    t2->get_object(t2_keys[1]).set(t2_int_col, 0);

    t1->get_object(t1_keys[0]).set(t1_link_col, t2_keys[1]);
    t1->get_object(t1_keys[1]).set(t1_link_col, t2_keys[0]);
    t1->get_object(t1_keys[2]).set(t1_link_col, t2_keys[2]);
    t1->get_object(t1_keys[3]).set(t1_link_col, t2_keys[3]);
    t1->get_object(t1_keys[4]).set(t1_link_col, t2_keys[5]);
    t1->get_object(t1_keys[5]).set(t1_link_col, t2_keys[4]);
    t1->get_object(t1_keys[6]).set(t1_link_col, t2_keys[1]);

    t2->get_object(t2_keys[0]).set(t2_link_col, t3_keys[3]);
    t2->get_object(t2_keys[1]).set(t2_link_col, t3_keys[2]);
    t2->get_object(t2_keys[2]).set(t2_link_col, t3_keys[0]);
    t2->get_object(t2_keys[3]).set(t2_link_col, t3_keys[1]);

    t3->get_object(t3_keys[1]).set(t3_int_col, 4);
    t3->get_object(t3_keys[2]).set(t3_int_col, 7);
    t3->get_object(t3_keys[3]).set(t3_int_col, 3);
    t3->get_object(t3_keys[0]).set(t3_str_col, "b");
    t3->get_object(t3_keys[1]).set(t3_str_col, "a");
    t3->get_object(t3_keys[2]).set(t3_str_col, "c");
    t3->get_object(t3_keys[3]).set(t3_str_col, "k");

    //  T1                       T2                     T3
    //  t1_int   t1_link_t2  |   t2_int  t2_link_t3 |   t3_int  t3_str
    //  ==============================================================
    //  99       1           |   0       3          |   null    "b"
    //  0        0           |   0       2          |   4       "a"
    //  1        2           |   3       0          |   7       "c"
    //  2        3           |   2       1          |   3       "k"
    //  3        5           |   1       null       |
    //  4        4           |   0       null       |
    //  5        1           |                      |

    {
        TableView tv = t1->where().less(t1_int_col, 6).find_all();

        // Test original funcionality through chain class
        std::vector<size_t> results1 = {0, 1, 2, 3, 4, 5};
        tv.distinct(DistinctDescriptor(*t1, {{t1_int_col}}));
        CHECK_EQUAL(tv.size(), results1.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results1[i]);
        }
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.distinct(DistinctDescriptor(*t1, {{t1_int_col}}));
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results1[i]); // results haven't been sorted
        }
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.sort(SortDescriptor(*t1, {{t1_int_col}}, {true}));
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results1[i]); // still same order here by conincidence
        }
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.sort(SortDescriptor(*t1, {{t1_int_col}}, {false}));
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results1[results1.size() - 1 - i]); // now its reversed
        }
    }

    {
        TableView tv = t1->where().less(t1_int_col, 6).find_all();

        // Test basic one link chain
        std::vector<size_t> results2 = {0, 1, 2, 4};
        tv.distinct(DistinctDescriptor(*t1, {{t1_link_col, t2_int_col}}));
        CHECK_EQUAL(tv.size(), results2.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results2[i]);
        }

        std::vector<size_t> results2_sorted_link = {0, 4, 2, 1};
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.distinct(DistinctDescriptor(*t1, {{t1_link_col, t2_int_col}}));
        tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_int_col}}, {true}));
        CHECK_EQUAL(tv.size(), results2_sorted_link.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results2_sorted_link[i]);
        }
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.distinct(DistinctDescriptor(*t1, {{t1_link_col, t2_int_col}}));
        tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_int_col}}, {false}));
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results2_sorted_link[results2_sorted_link.size() - 1 - i]);
        }
    }

    {
        TableView tv = t1->where().less(t1_int_col, 6).find_all();

        // Test link chain through two links with nulls
        std::vector<size_t> results3 = {0, 1, 2, 5};
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.distinct(DistinctDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}}));
        // Nullified links are excluded from distinct.
        CHECK_EQUAL(tv.size(), results3.size());
        for (size_t i = 0; i < results3.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results3[i]);
        }

        results3 = {1, 0, 2, 5}; // sorted order on t3_col_int { null, 3, 4, 7 }
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.distinct(DistinctDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}}));
        tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}}));
        CHECK_EQUAL(tv.size(), results3.size());
        for (size_t i = 0; i < results3.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results3[i]);
        }
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.distinct(DistinctDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}}));
        tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}}, {false}));
        CHECK_EQUAL(tv.size(), results3.size());
        for (size_t i = 0; i < results3.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results3[results3.size() - 1 - i]);
        }
    }
}

TEST(Query_Sort_And_Requery_Typed1)
{
    Table ttt;
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(1, "a"); // 0 *
    ttt.create_object().set_all(2, "a"); // 1
    ttt.create_object().set_all(3, "X"); // 2
    ttt.create_object().set_all(1, "a"); // 3 *
    ttt.create_object().set_all(2, "a"); // 4
    ttt.create_object().set_all(3, "X"); // 5
    ttt.create_object().set_all(9, "a"); // 6 *
    ttt.create_object().set_all(8, "a"); // 7 *
    ttt.create_object().set_all(7, "X"); // 8

    // tv.get_key()  = 0, 2, 3, 5, 6, 7, 8
    // Vals         = 1, 3, 1, 3, 9, 8, 7
    // result       = 3, 0, 5, 2, 8, 7, 6

    Query q = ttt.where().not_equal(col_int, 2);
    TableView tv = q.find_all();

    ObjKey match = ttt.where(&tv).equal(col_int, 7).find();
    CHECK_EQUAL(match, ObjKey(8));

    tv.sort(col_int);

    CHECK(tv.size() == 7);
    CHECK(tv[0].get<Int>(col_int) == 1);
    CHECK(tv[1].get<Int>(col_int) == 1);
    CHECK(tv[2].get<Int>(col_int) == 3);
    CHECK(tv[3].get<Int>(col_int) == 3);
    CHECK(tv[4].get<Int>(col_int) == 7);
    CHECK(tv[5].get<Int>(col_int) == 8);
    CHECK(tv[6].get<Int>(col_int) == 9);

    Query q2 = ttt.where(&tv).not_equal(col_str, "X");
    TableView tv2 = q2.find_all();

    CHECK_EQUAL(4, tv2.size());
    CHECK_EQUAL(1, tv2[0].get<Int>(col_int));
    CHECK_EQUAL(1, tv2[1].get<Int>(col_int));
    CHECK_EQUAL(8, tv2[2].get<Int>(col_int)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv2[3].get<Int>(col_int));

    match = ttt.where(&tv).not_equal(col_str, "X").find();
    CHECK_EQUAL(match, ObjKey(0));

    match = ttt.where(&tv).not_equal(col_str, "a").find();
    CHECK_EQUAL(match, ObjKey(2));
}


TEST(Query_Sort_And_Requery_FindFirst)
{
    Table ttt;
    auto col_int0 = ttt.add_column(type_Int, "1");
    auto col_int1 = ttt.add_column(type_Int, "2");

    ttt.create_object().set_all(1, 60);
    ttt.create_object().set_all(2, 50); // **
    ttt.create_object().set_all(3, 40); // *
    ttt.create_object().set_all(1, 30);
    ttt.create_object().set_all(2, 20); // **
    ttt.create_object().set_all(3, 10); // **

    Query q = ttt.where().greater(col_int0, 1);
    TableView tv = q.find_all();
    CHECK_EQUAL(tv.size(), 4);
    tv.sort(col_int1);

    // 3, 2, 3, 2
    ObjKey k = ttt.where(&tv).equal(col_int0, 3).find();
    int64_t s = ttt.where(&tv).not_equal(col_int1, 40).sum_int(col_int0);
    CHECK_EQUAL(k, ObjKey(5));
    CHECK_EQUAL(s, 7);
}


TEST(Query_Sort_And_Requery)
{
    // New where(tableview) method
    Table table;
    auto col_int = table.add_column(type_Int, "first1");
    auto col_str = table.add_column(type_String, "second1");

    table.create_object().set_all(1, "a");
    table.create_object().set_all(2, "a");
    table.create_object().set_all(3, "X");
    table.create_object().set_all(1, "a");
    table.create_object().set_all(2, "a");
    table.create_object().set_all(3, "X");
    table.create_object().set_all(9, "a");
    table.create_object().set_all(8, "a");
    table.create_object().set_all(7, "X");

    Query q = table.where().not_equal(col_int, 2);
    TableView tv = q.find_all();
    tv.sort(col_int);

    CHECK(tv.size() == 7);

    CHECK(tv[0].get<Int>(col_int) == 1);
    CHECK(tv[1].get<Int>(col_int) == 1);
    CHECK(tv[2].get<Int>(col_int) == 3);
    CHECK(tv[3].get<Int>(col_int) == 3);
    CHECK(tv[4].get<Int>(col_int) == 7);
    CHECK(tv[5].get<Int>(col_int) == 8);
    CHECK(tv[6].get<Int>(col_int) == 9);

    Query q2 = table.where(&tv).not_equal(col_str, "X");
    TableView tv2 = q2.find_all();

    CHECK_EQUAL(4, tv2.size());
    CHECK_EQUAL(1, tv2[0].get<Int>(col_int));
    CHECK_EQUAL(1, tv2[1].get<Int>(col_int));
    CHECK_EQUAL(8, tv2[2].get<Int>(col_int)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv2[3].get<Int>(col_int));

    Query q3 = table.where(&tv2).not_equal(col_str, "X");
    TableView tv3 = q3.find_all();

    CHECK_EQUAL(4, tv3.size());
    CHECK_EQUAL(1, tv3[0].get<Int>(col_int));
    CHECK_EQUAL(1, tv3[1].get<Int>(col_int));
    CHECK_EQUAL(8, tv3[2].get<Int>(col_int)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv3[3].get<Int>(col_int));

    // Test that remove() maintains order
    tv3.remove(0);
    // q5 and q3 should behave the same.
    Query q5 = table.where(&tv2).not_equal(col_str, "X");
    TableView tv5 = q5.find_all();
    tv5.sync_if_needed(); // you may think tv5 is in sync, BUT it was generated from tv2 which wasn't
    // Note the side effect - as tv5 depends on ... on tv2 etc, all views are synchronized.
    CHECK_EQUAL(3, tv5.size());
    CHECK_EQUAL(1, tv5[0].get<Int>(col_int));
    CHECK_EQUAL(8, tv5[1].get<Int>(col_int)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv5[2].get<Int>(col_int));

    CHECK_EQUAL(6, tv.size());
    CHECK_EQUAL(3, tv3.size());
    CHECK_EQUAL(1, tv3[0].get<Int>(col_int));
    CHECK_EQUAL(8, tv3[1].get<Int>(col_int)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv3[2].get<Int>(col_int));

    Query q4 = table.where(&tv3).not_equal(col_str, "X");
    TableView tv4 = q4.find_all();

    CHECK_EQUAL(3, tv4.size());
    CHECK_EQUAL(1, tv4[0].get<Int>(col_int));
    CHECK_EQUAL(8, tv4[1].get<Int>(col_int)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv4[2].get<Int>(col_int));
}


TEST(Query_Sort_And_Requery_Untyped_Monkey2)
{
    // New where(tableview) method
    for (int iter = 0; iter < 1; iter++) {
        size_t b;
        Table table;
        auto col_int0 = table.add_column(type_Int, "first1");
        auto col_int1 = table.add_column(type_Int, "second1");

        // Add random data to table
        for (size_t t = 0; t < 2 * REALM_MAX_BPNODE_SIZE; t++) {
            int64_t val1 = rand() % 5;
            int64_t val2 = rand() % 5;
            table.create_object().set_all(val1, val2);
        }

        // Query and sort
        Query q = table.where().equal(col_int1, 2);
        TableView tv = q.find_all();
        tv.sort(col_int0);

        // Requery and keep original sort order
        Query q2 = table.where(&tv).not_equal(col_int0, 3);
        TableView tv2 = q2.find_all();

        b = 0;
        // Test if sort order is the same as original
        for (size_t t = 0; t < tv2.size(); t++) {
            ObjKey a = tv2.get_key(t);
            REALM_ASSERT_EX(b < tv.size(), b, tv.size());
            while (a != tv.get_key(b)) {
                b++;
            }
        }

        // We cannot test remove() if query resulted in 0 items
        if (tv2.size() == 0)
            continue;

        size_t remove = rand() % tv2.size();
        static_cast<void>(remove);

        Query q3 = table.where(&tv2).not_equal(col_int0, 2);
        TableView tv3 = q3.find_all();

        b = 0;
        // Test if sort order is the same as original
        for (size_t t = 0; t < tv3.size(); t++) {
            ObjKey a = tv3.get_key(t);
            REALM_ASSERT_EX(b < tv2.size(), b, tv2.size());
            while (a != tv2.get_key(b)) {
                b++;
                CHECK(b < tv2.size());
            }
        }

        // Now test combinations of sorted and non-sorted tableviews
        Query q4 = table.where().not_equal(col_int0, 1);
        TableView tv4 = q4.find_all();

        Query q5 = table.where(&tv4).not_equal(col_int0, 2);
        TableView tv5 = q5.find_all();

        for (size_t t = 1; t < tv5.size(); t++) {
            CHECK(tv5.get_key(t - 1) < tv5.get_key(t));
        }

        // Test that tv5 is ordered the same way as tv4 (tv5 is subset of tv4)
        size_t foreignindex = 0;
        for (size_t t = 0; t < tv5.size(); t++) {
            size_t foreignindex2 = 0;
            while (tv4.get_key(foreignindex2) != tv5.get_key(t))
                foreignindex2++;

            CHECK(foreignindex2 >= foreignindex);
            foreignindex = foreignindex2;
        }

        // New test where both tableviews are sorted according to a column, and both sets are equal
        Query q6 = table.where().not_equal(col_int0, 2);
        TableView tv6 = q6.find_all();

        Query q7 = table.where(&tv6).not_equal(col_int0, 2);
        TableView tv7 = q7.find_all();

        // Test that tv7 is ordered the same way as tv6
        foreignindex = 0;
        for (size_t t = 0; t < tv5.size(); t++) {
            size_t foreignindex2 = 0;
            while (tv4.get_key(foreignindex2) != tv5.get_key(t))
                foreignindex2++;

            CHECK(foreignindex2 >= foreignindex);
            foreignindex = foreignindex2;
        }

        tv7.sort(col_int1);
        tv6.sort(col_int1);

        // Test that tv7 is ordered the same way as tv6
        foreignindex = 0;
        for (size_t t = 0; t < tv5.size(); t++) {
            size_t foreignindex2 = 0;
            while (tv4.get_key(foreignindex2) != tv5.get_key(t))
                foreignindex2++;

            CHECK(foreignindex2 >= foreignindex);
            foreignindex = foreignindex2;
        }
    }
}


TEST(Query_BigString)
{
    Table ttt;
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(1, "a");
    ObjKey res1 = ttt.where().equal(col_str, "a").find();
    CHECK_EQUAL(ttt.get_object(res1).get<Int>(col_int), 1);

    const char* medium_string = "40 chars  40 chars  40 chars  40 chars  ";
    ttt.create_object().set_all(2, medium_string);
    ObjKey res2 = ttt.where().equal(col_str, medium_string).find();
    CHECK_EQUAL(ttt.get_object(res2).get<Int>(col_int), 2);

    const char* long_string = "70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ";
    ttt.create_object().set_all(3, long_string);
    ObjKey res3 = ttt.where().equal(col_str, long_string).find();
    CHECK_EQUAL(ttt.get_object(res3).get<Int>(col_int), 3);
}


TEST(Query_Limit)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a"); //
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 1, "a");
    ttt.create_object().set_all(4, 2, "a"); //
    ttt.create_object().set_all(5, 3, "X");
    ttt.create_object().set_all(6, 1, "a");
    ttt.create_object().set_all(7, 2, "a"); //
    ttt.create_object().set_all(8, 3, "X");
    ttt.create_object().set_all(9, 1, "a");
    ttt.create_object().set_all(10, 2, "a"); //
    ttt.create_object().set_all(11, 3, "X");
    ttt.create_object().set_all(12, 1, "a");
    ttt.create_object().set_all(13, 2, "a"); //
    ttt.create_object().set_all(14, 3, "X");

    Query q1 = ttt.where().equal(col_int, 2);

    TableView tv1 = q1.find_all(0, size_t(-1), 2);
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(4, tv1[1].get<Int>(col_id));

    TableView tv2 = q1.find_all(5, size_t(-1), 2);
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(7, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(10, tv2[1].get<Int>(col_id));

    TableView tv3 = q1.find_all(11, size_t(-1), 2);
    CHECK_EQUAL(1, tv3.size());
    CHECK_EQUAL(13, tv3[0].get<Int>(col_id));


    Query q2 = ttt.where();
    TableView tv4 = q2.find_all(0, 5, 3);
    CHECK_EQUAL(3, tv4.size());

    Query q3 = ttt.where();
    TableView tv5 = q3.find_all(0, 3, 5);
    CHECK_EQUAL(3, tv5.size());
}


TEST(Query_FindAll1)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 4, "a");
    ttt.create_object().set_all(4, 5, "a");
    ttt.create_object().set_all(5, 6, "X");
    ttt.create_object().set_all(6, 7, "X");

    Query q1 = ttt.where().equal(col_str, "a").greater(col_int, 2).not_equal(col_int, 4);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1[0].get<Int>(col_id));

    Query q2 = ttt.where().equal(col_str, "X").greater(col_int, 4);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(tv2.size(), 2);
    CHECK_EQUAL(5, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(6, tv2[1].get<Int>(col_id));
}

TEST(Query_FindAll2)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 4, "a");
    ttt.create_object().set_all(4, 5, "a");
    ttt.create_object().set_all(5, 11, "X");
    ttt.create_object().set_all(6, 0, "X");

    Query q2 = ttt.where().not_equal(col_str, "a").less(col_int, 3);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(tv2.size(), 1);
    CHECK_EQUAL(6, tv2[0].get<Int>(col_id));
}

TEST(Query_FindAllBetween)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 4, "a");
    ttt.create_object().set_all(4, 5, "a");
    ttt.create_object().set_all(5, 11, "X");
    ttt.create_object().set_all(6, 3, "X");

    Query q2 = ttt.where().between(col_int, 3, 5);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(tv2.size(), 4);
    CHECK_EQUAL(2, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(3, tv2[1].get<Int>(col_id));
    CHECK_EQUAL(4, tv2[2].get<Int>(col_id));
    CHECK_EQUAL(6, tv2[3].get<Int>(col_id));
}


TEST(Query_FindAllOr)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 4, "a");
    ttt.create_object().set_all(4, 5, "a");
    ttt.create_object().set_all(5, 6, "a");
    ttt.create_object().set_all(6, 7, "X");

    // first == 5 || second == X
    Query q1 = ttt.where().equal(col_int, 5).Or().equal(col_str, "X");
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(2, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(4, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(6, tv1[2].get<Int>(col_id));
}


TEST(Query_FindAllParens1)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 3, "X");
    ttt.create_object().set_all(4, 4, "a");
    ttt.create_object().set_all(5, 5, "a");
    ttt.create_object().set_all(6, 11, "X");

    // first > 3 && (second == X)
    Query q1 = ttt.where().greater(col_int, 3).group().equal(col_str, "X").end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(6, tv1[0].get<Int>(col_id));
}


TEST(Query_FindAllOrParan)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X"); //
    ttt.create_object().set_all(3, 4, "a");
    ttt.create_object().set_all(4, 5, "a"); //
    ttt.create_object().set_all(5, 6, "a");
    ttt.create_object().set_all(6, 7, "X"); //
    ttt.create_object().set_all(7, 2, "X");

    // (first == 5 || second == X && first > 2)
    Query q1 = ttt.where().group().equal(col_int, 5).Or().equal(col_str, "X").greater(col_int, 2).end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(2, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(4, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(6, tv1[2].get<Int>(col_id));
}


TEST(Query_FindAllOrNested0)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 3, "X");
    ttt.create_object().set_all(4, 4, "a");
    ttt.create_object().set_all(5, 5, "a");
    ttt.create_object().set_all(6, 11, "X");
    ttt.create_object().set_all(7, 8, "Y");

    // first > 3 && (first == 5 || second == X)
    Query q1 = ttt.where().greater(col_int, 3).group().equal(col_int, 5).Or().equal(col_str, "X").end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(5, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(6, tv1[1].get<Int>(col_id));
}

TEST(Query_FindAllOrNested)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 3, "X");
    ttt.create_object().set_all(4, 4, "a");
    ttt.create_object().set_all(5, 5, "a");
    ttt.create_object().set_all(6, 11, "X");
    ttt.create_object().set_all(7, 8, "Y");

    // first > 3 && (first == 5 || second == X || second == Y)
    Query q1 = ttt.where()
                   .greater(col_int, 3)
                   .group()
                   .equal(col_int, 5)
                   .Or()
                   .equal(col_str, "X")
                   .Or()
                   .equal(col_str, "Y")
                   .end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(5, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(6, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(7, tv1[2].get<Int>(col_id));
}

TEST(Query_FindAllOrNestedInnerGroup)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 3, "X");
    ttt.create_object().set_all(4, 4, "a");
    ttt.create_object().set_all(5, 5, "a");
    ttt.create_object().set_all(6, 11, "X");
    ttt.create_object().set_all(7, 8, "Y");

    // first > 3 && (first == 5 || (second == X || second == Y))
    Query q1 = ttt.where()
                   .greater(col_int, 3)
                   .group()
                   .equal(col_int, 5)
                   .Or()
                   .group()
                   .equal(col_str, "X")
                   .Or()
                   .equal(col_str, "Y")
                   .end_group()
                   .end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(5, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(6, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(7, tv1[2].get<Int>(col_id));
}

TEST(Query_FindAllOrPHP)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "Joe");
    ttt.create_object().set_all(1, 2, "Sara");
    ttt.create_object().set_all(2, 3, "Jim");

    // (second == Jim || second == Joe) && first = 1
    Query q1 = ttt.where().group().equal(col_str, "Jim").Or().equal(col_str, "Joe").end_group().equal(col_int, 1);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));

    q1 = ttt.where().group().equal(col_str, "Jim").Or().equal(col_str, "Joe").end_group().equal(col_int, 3);
    tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1[0].get<Int>(col_id));
}

TEST(Query_FindAllParens2)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");

    ttt.create_object().set_all(0, 1);
    ttt.create_object().set_all(1, 2);
    ttt.create_object().set_all(2, 3);
    ttt.create_object().set_all(3, 3);
    ttt.create_object().set_all(4, 4);
    ttt.create_object().set_all(5, 5);
    ttt.create_object().set_all(6, 11);

    // ()
    Query q1 = ttt.where().group().end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(7, tv1.size());

    // ()((first > 3()) && (()))
    q1 = ttt.where()
             .group()
             .end_group()
             .group()
             .group()
             .greater(col_int, 3)
             .group()
             .end_group()
             .end_group()
             .group()
             .group()
             .end_group()
             .end_group()
             .end_group();
    tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(4, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(5, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(6, tv1[2].get<Int>(col_id));
}


TEST(Query_FindAllBool)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_bool = table.add_column(type_Bool, "2");

    table.create_object().set_all(0, true);
    table.create_object().set_all(1, false);
    table.create_object().set_all(2, true);
    table.create_object().set_all(3, false);

    Query q1 = table.where().equal(col_bool, true);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[1].get<Int>(col_id));

    Query q2 = table.where().equal(col_bool, false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(1, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(3, tv2[1].get<Int>(col_id));
}

TEST(Query_FindAllBegins)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, "fo");
    table.create_object().set_all(1, "foo");
    table.create_object().set_all(2, "foobar");

    Query q1 = table.where().begins_with(col_str, "foo");
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[1].get<Int>(col_id));
}

TEST(Query_FindAllEnds)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, "barfo");
    table.create_object().set_all(1, "barfoo");
    table.create_object().set_all(2, "barfoobar");

    Query q1 = table.where().ends_with(col_str, "foo");
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(1, tv1[0].get<Int>(col_id));
}


TEST(Query_FindAllContains)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, "foo");
    table.create_object().set_all(1, "foobar");
    table.create_object().set_all(2, "barfoo");
    table.create_object().set_all(3, "barfoobaz");
    table.create_object().set_all(4, "fo");
    table.create_object().set_all(5, "fobar");
    table.create_object().set_all(6, "barfo");

    Query q1 = table.where().contains(col_str, "foo");
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(1, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[2].get<Int>(col_id));
    CHECK_EQUAL(3, tv1[3].get<Int>(col_id));

    q1 = table.where().like(col_str, "*foo*");
    tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(1, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[2].get<Int>(col_id));
    CHECK_EQUAL(3, tv1[3].get<Int>(col_id));
}

TEST(Query_FindAllLikeStackOverflow)
{
    std::string str(100000, 'x');
    StringData sd(str);

    Table table;
    auto col = table.add_column(type_String, "strings");
    ObjKey k = table.create_object().set(col, sd).get_key();

    auto res = table.where().like(col, sd).find();
    CHECK_EQUAL(res, k);
}

TEST(Query_FindAllLikeCaseInsensitive)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, "Foo");
    table.create_object().set_all(1, "FOOBAR");
    table.create_object().set_all(2, "BaRfOo");
    table.create_object().set_all(3, "barFOObaz");
    table.create_object().set_all(4, "Fo");
    table.create_object().set_all(5, "Fobar");
    table.create_object().set_all(6, "baRFo");

    Query q1 = table.where().like(col_str, "*foo*", false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(1, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[2].get<Int>(col_id));
    CHECK_EQUAL(3, tv1[3].get<Int>(col_id));
}

TEST(Query_Binary)
{
    Table t;
    t.add_column(type_Int, "1");
    auto c1 = t.add_column(type_Binary, "2");

    const char bin[64] = {6, 3, 9, 5, 9, 7, 6, 3, 2, 6, 0, 0, 5, 4, 2, 4, 5, 7, 9, 5, 7, 1,
                          1, 2, 0, 8, 3, 8, 0, 9, 6, 8, 4, 7, 3, 4, 9, 5, 2, 3, 6, 2, 7, 4,
                          0, 3, 7, 6, 2, 3, 5, 9, 3, 1, 2, 1, 0, 5, 5, 2, 9, 4, 5, 9};

    const char bin_2[4] = {6, 6, 6, 6}; // Not occuring above

    std::vector<ObjKey> keys;
    t.create_objects(9, keys);

    t.get_object(keys[0]).set_all(0, BinaryData(bin + 0, 16));
    t.get_object(keys[1]).set_all(0, BinaryData(bin + 0, 32));
    t.get_object(keys[2]).set_all(0, BinaryData(bin + 0, 48));
    t.get_object(keys[3]).set_all(0, BinaryData(bin + 0, 64));
    t.get_object(keys[4]).set_all(0, BinaryData(bin + 16, 48));
    t.get_object(keys[5]).set_all(0, BinaryData(bin + 32, 32));
    t.get_object(keys[6]).set_all(0, BinaryData(bin + 48, 16));
    t.get_object(keys[7]).set_all(0, BinaryData(bin + 24, 16)); // The "odd ball"
    t.get_object(keys[8]).set_all(0, BinaryData(bin + 0, 32));  // Repeat an entry

    CHECK_EQUAL(0, t.where().equal(c1, BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(1, t.where().equal(c1, BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(1, t.where().equal(c1, BinaryData(bin + 48, 16)).count());
    CHECK_EQUAL(2, t.where().equal(c1, BinaryData(bin + 0, 32)).count());

    CHECK_EQUAL(9, t.where().not_equal(c1, BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(8, t.where().not_equal(c1, BinaryData(bin + 0, 16)).count());

    CHECK_EQUAL(0, t.where().begins_with(c1, BinaryData(bin + 8, 16)).count());
    CHECK_EQUAL(1, t.where().begins_with(c1, BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(4, t.where().begins_with(c1, BinaryData(bin + 0, 32)).count());
    CHECK_EQUAL(5, t.where().begins_with(c1, BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(1, t.where().begins_with(c1, BinaryData(bin + 48, 16)).count());
    CHECK_EQUAL(9, t.where().begins_with(c1, BinaryData(bin + 0, 0)).count());

    CHECK_EQUAL(0, t.where().ends_with(c1, BinaryData(bin + 40, 16)).count());
    CHECK_EQUAL(1, t.where().ends_with(c1, BinaryData(bin + 32, 16)).count());
    CHECK_EQUAL(3, t.where().ends_with(c1, BinaryData(bin + 32, 32)).count());
    CHECK_EQUAL(4, t.where().ends_with(c1, BinaryData(bin + 48, 16)).count());
    CHECK_EQUAL(1, t.where().ends_with(c1, BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(9, t.where().ends_with(c1, BinaryData(bin + 64, 0)).count());

    CHECK_EQUAL(0, t.where().contains(c1, BinaryData(bin_2)).count());
    CHECK_EQUAL(5, t.where().contains(c1, BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(5, t.where().contains(c1, BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(4, t.where().contains(c1, BinaryData(bin + 24, 16)).count());
    CHECK_EQUAL(4, t.where().contains(c1, BinaryData(bin + 32, 16)).count());
    CHECK_EQUAL(9, t.where().contains(c1, BinaryData(bin + 0, 0)).count());

    {
        TableView tv = t.where().equal(c1, BinaryData(bin + 0, 32)).find_all();
        if (tv.size() == 2) {
            CHECK_EQUAL(keys[1], tv.get_key(0));
            CHECK_EQUAL(keys[8], tv.get_key(1));
        }
        else
            CHECK(false);
    }

    {
        TableView tv = t.where().contains(c1, BinaryData(bin + 24, 16)).find_all();
        if (tv.size() == 4) {
            CHECK_EQUAL(keys[2], tv.get_key(0));
            CHECK_EQUAL(keys[3], tv.get_key(1));
            CHECK_EQUAL(keys[4], tv.get_key(2));
            CHECK_EQUAL(keys[7], tv.get_key(3));
        }
        else
            CHECK(false);
    }
}

TEST(Query_Enums)
{
    Table table;
    auto col_int = table.add_column(type_Int, "1");
    auto col_str = table.add_column(type_String, "2");


    for (size_t i = 0; i < 5; ++i) {
        table.create_object().set_all(1, "abd");
        table.create_object().set_all(2, "eftg");
        table.create_object().set_all(5, "hijkl");
        table.create_object().set_all(8, "mnopqr");
        table.create_object().set_all(9, "stuvxyz");
    }

    table.enumerate_string_column(col_str);

    Query q1 = table.where().equal(col_str, "eftg");
    TableView tv1 = q1.find_all();

    CHECK_EQUAL(5, tv1.size());
    CHECK_EQUAL(2, tv1[0].get<Int>(col_int));
    CHECK_EQUAL(2, tv1[1].get<Int>(col_int));
    CHECK_EQUAL(2, tv1[2].get<Int>(col_int));
    CHECK_EQUAL(2, tv1[3].get<Int>(col_int));
    CHECK_EQUAL(2, tv1[4].get<Int>(col_int));
}


TEST_TYPES(Query_CaseSensitivity, std::true_type, std::false_type)
{
    constexpr bool nullable = TEST_TYPE::value;

    Table ttt;
    auto col = ttt.add_column(type_String, "2", nullable);

    ObjKey k = ttt.create_object().set(col, "BLAAbaergroed").get_key();
    ttt.create_object().set(col, "BLAAbaergroedandMORE");
    ttt.create_object().set(col, "BLAAbaergroedZ");
    ttt.create_object().set(col, "BLAAbaergroedZ");
    ttt.create_object().set(col, "BLAAbaergroedZ");

    Query q1 = ttt.where().equal(col, "blaabaerGROED", false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(k, tv1.get_key(0));

    Query q2 = ttt.where().equal(col, "blaabaerGROEDz", false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(3, tv2.size());

    ttt.add_search_index(col);

    Query q3 = ttt.where().equal(col, "blaabaerGROEDz", false);
    TableView tv3 = q3.find_all();
    CHECK_EQUAL(3, tv3.size());
}

#define uY "\x0CE\x0AB"            // greek capital letter upsilon with dialytika (U+03AB)
#define uYd "\x0CE\x0A5\x0CC\x088" // decomposed form (Y followed by two dots)
#define uy "\x0CF\x08B"            // greek small letter upsilon with dialytika (U+03AB)
#define uyd "\x0cf\x085\x0CC\x088" // decomposed form (Y followed by two dots)

#define uA "\x0c3\x085"       // danish capital A with ring above (as in BLAABAERGROED)
#define uAd "\x041\x0cc\x08a" // decomposed form (A (41) followed by ring)
#define ua "\x0c3\x0a5"       // danish lower case a with ring above (as in blaabaergroed)
#define uad "\x061\x0cc\x08a" // decomposed form (a (41) followed by ring)

#if (defined(_WIN32) || defined(__WIN32__) || defined(_WIN64))

TEST(Query_Unicode2)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, uY);
    table.create_object().set_all(1, uYd);
    table.create_object().set_all(2, uy);
    table.create_object().set_all(3, uyd);

    Query q1 = table.where().equal(col_str, uY, false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[1].get<Int>(col_id));

    Query q2 = table.where().equal(col_str, uYd, false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(1, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(3, tv2[1].get<Int>(col_id));

    Query q3 = table.where().equal(col_str, uYd, true);
    TableView tv3 = q3.find_all();
    CHECK_EQUAL(1, tv3.size());
    CHECK_EQUAL(1, tv3[0].get<Int>(col_id));
}


TEST(Query_Unicode3)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, uA);
    table.create_object().set_all(1, uAd);
    table.create_object().set_all(2, ua);
    table.create_object().set_all(3, uad);

    Query q1 = table.where().equal(col_str, uA, false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[1].get<Int>(col_id));

    Query q2 = table.where().equal(col_str, ua, false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(0, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(2, tv2[1].get<Int>(col_id));

    Query q3 = table.where().equal(col_str, uad, false);
    TableView tv3 = q3.find_all();
    CHECK_EQUAL(2, tv3.size());
    CHECK_EQUAL(1, tv3[0].get<Int>(col_id));
    CHECK_EQUAL(3, tv3[1].get<Int>(col_id));

    Query q4 = table.where().equal(col_str, uad, true);
    TableView tv4 = q4.find_all();
    CHECK_EQUAL(1, tv4.size());
    CHECK_EQUAL(3, tv4[0].get<Int>(col_id));
}

#endif

TEST(Query_FindAllBeginsUnicode)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, uad "fo");
    table.create_object().set_all(1, uad "foo");
    table.create_object().set_all(2, uad "foobar");

    Query q1 = table.where().begins_with(col_str, uad "foo");
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[1].get<Int>(col_id));
}


TEST(Query_FindAllEndsUnicode)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, "barfo");
    table.create_object().set_all(1, "barfoo" uad);
    table.create_object().set_all(2, "barfoobar");

    Query q1 = table.where().ends_with(col_str, "foo" uad);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(1, tv1[0].get<Int>(col_id));

    Query q2 = table.where().ends_with(col_str, "foo" uAd, false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(1, tv2.size());
    CHECK_EQUAL(1, tv2[0].get<Int>(col_id));
}


TEST(Query_FindAllContainsUnicode)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, uad "foo");
    table.create_object().set_all(1, uad "foobar");
    table.create_object().set_all(2, "bar" uad "foo");
    table.create_object().set_all(3, uad "bar" uad "foobaz");
    table.create_object().set_all(4, uad "fo");
    table.create_object().set_all(5, uad "fobar");
    table.create_object().set_all(6, uad "barfo");

    Query q1 = table.where().contains(col_str, uad "foo");
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(1, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[2].get<Int>(col_id));
    CHECK_EQUAL(3, tv1[3].get<Int>(col_id));

    Query q2 = table.where().contains(col_str, uAd "foo", false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(4, tv2.size());
    CHECK_EQUAL(0, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(1, tv2[1].get<Int>(col_id));
    CHECK_EQUAL(2, tv2[2].get<Int>(col_id));
    CHECK_EQUAL(3, tv2[3].get<Int>(col_id));
}

TEST(Query_SyntaxCheck)
{
    Table table;
    auto col_int = table.add_column(type_Int, "1");
    table.add_column(type_String, "2");

    std::string s;

    table.create_object().set_all(1, "a");
    table.create_object().set_all(2, "a");
    table.create_object().set_all(3, "X");

    Query q1 = table.where().equal(col_int, 2).end_group();
    s = q1.validate();
    CHECK(s != "");

    Query q2 = table.where().group().group().equal(col_int, 2).end_group();
    s = q2.validate();
    CHECK(s != "");

    Query q3 = table.where().equal(col_int, 2).Or();
    s = q3.validate();
    CHECK(s != "");

    Query q4 = table.where().Or().equal(col_int, 2);
    s = q4.validate();
    CHECK(s != "");

    Query q5 = table.where().equal(col_int, 2);
    s = q5.validate();
    CHECK(s == "");

    Query q6 = table.where().group().equal(col_int, 2);
    s = q6.validate();
    CHECK(s != "");

    // FIXME: Work is currently underway to fully support locale
    // independent case folding as defined by Unicode. Reenable this test
    // when is becomes available.
    /*
    Query q7 = ttt.where().equal(1, "\xa0", false);
#ifdef REALM_DEBUG
    s = q7.verify();
    CHECK(s != "");
#endif
    */
}

TEST(Query_TestTV_where)
{
    // When using .where(&tv), tv can have any order, and the resulting view will retain its order
    Table table;
    auto col_int = table.add_column(type_Int, "1");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(1, "a");
    table.create_object().set_all(2, "a");
    table.create_object().set_all(3, "c");

    TableView v = table.where().greater(col_int, 1).find_all();

    Query q1 = table.where(&v);
    CHECK_EQUAL(2, q1.count());

    Query q3 = table.where(&v).equal(col_str, "a");
    CHECK_EQUAL(1, q3.count());

    Query q4 = table.where(&v).between(col_int, 3, 6);
    CHECK_EQUAL(1, q4.count());
}


TEST(Query_SumMinMaxAvg)
{
    Table t;

    auto int_col = t.add_column(type_Int, "1");
    auto date_col = t.add_column(type_Timestamp, "3");
    auto float_col = t.add_column(type_Float, "4");
    auto double_col = t.add_column(type_Double, "5");

    std::vector<ObjKey> keys;
    t.create_objects(9, keys);
    t.get_object(keys[0]).set_all(1, Timestamp{200, 0}, 1.0f, 2.0);
    t.get_object(keys[1]).set_all(1, Timestamp{100, 0}, 1.0f, 1.0);
    t.get_object(keys[2]).set_all(1, Timestamp{100, 0}, 1.0f, 1.0);
    t.get_object(keys[3]).set_all(1, Timestamp{100, 0}, 1.0f, 1.0);
    t.get_object(keys[4]).set_all(2, Timestamp{300, 0}, 3.0f, 3.0);
    t.get_object(keys[5]).set_all(3, Timestamp{50, 0}, 5.0f, 5.0);
    t.get_object(keys[6]).set_all(0, Timestamp{100, 0}, 1.0f, 1.0);
    t.get_object(keys[7]).set_all(0, Timestamp{3000, 0}, 30.0f, 30.0);
    t.get_object(keys[8]).set_all(0, Timestamp{5, 0}, 0.5f, 0.5);

    CHECK_EQUAL(9, t.where().sum_int(int_col));

    CHECK_EQUAL(0, t.where().minimum_int(int_col));
    CHECK_EQUAL(3, t.where().maximum_int(int_col));

    ObjKey resindex;

    t.where().maximum_int(int_col, &resindex);
    CHECK_EQUAL(keys[5], resindex);

    t.where().minimum_int(int_col, &resindex);
    CHECK_EQUAL(keys[6], resindex);

    t.where().maximum_float(float_col, &resindex);
    CHECK_EQUAL(keys[7], resindex);

    t.where().minimum_float(float_col, &resindex);
    CHECK_EQUAL(keys[8], resindex);

    t.where().maximum_double(double_col, &resindex);
    CHECK_EQUAL(keys[7], resindex);

    t.where().minimum_double(double_col, &resindex);
    CHECK_EQUAL(keys[8], resindex);

    t.where().maximum_timestamp(date_col, &resindex);
    CHECK_EQUAL(keys[7], resindex);

    t.where().minimum_timestamp(date_col, &resindex);
    CHECK_EQUAL(keys[8], resindex);

    // Now with condition (tests another code path in Array::minmax())
    t.where().not_equal(int_col, 0).minimum_double(double_col, &resindex);
    CHECK_EQUAL(keys[1], resindex);

    t.where().not_equal(int_col, 0).minimum_float(float_col, &resindex);
    CHECK_EQUAL(keys[0], resindex);

    t.where().not_equal(int_col, 0).minimum_timestamp(date_col, &resindex);
    CHECK_EQUAL(keys[5], resindex);

    t.where().not_equal(int_col, 0).maximum_timestamp(date_col, &resindex);
    CHECK_EQUAL(keys[4], resindex);

    CHECK_APPROXIMATELY_EQUAL(1, t.where().average_int(int_col), 0.001);

    CHECK_EQUAL(t.where().maximum_timestamp(date_col), Timestamp(3000, 0));
    CHECK_EQUAL(t.where().minimum_timestamp(date_col), Timestamp(5, 0));
}


TEST(Query_Avg)
{
    Table t;
    auto col = t.add_column(type_Int, "1");

    t.create_object().set(col, 10);
    CHECK_EQUAL(10, t.where().average_int(col));
    t.create_object().set(col, 30);
    CHECK_EQUAL(20, t.where().average_int(col));
}

TEST(Query_Avg2)
{
    Table t;
    auto col_int = t.add_column(type_Int, "1");
    auto col_str = t.add_column(type_String, "2");

    size_t cnt;

    t.create_object().set_all(10, "a");
    t.create_object().set_all(100, "b");
    t.create_object().set_all(20, "a");
    t.create_object().set_all(100, "b");
    t.create_object().set_all(100, "b");
    t.create_object().set_all(30, "a");

    CHECK_EQUAL(60, t.where().equal(col_str, "a").sum_int(col_int));

    CHECK_EQUAL(20, t.where().equal(col_str, "a").average_int(col_int, &cnt));
    CHECK_EQUAL(3, cnt);
    CHECK_EQUAL(100, t.where().equal(col_str, "b").average_int(col_int, &cnt));
    CHECK_EQUAL(3, cnt);
}

#ifdef LEGACY_TESTS
TEST(Query_OfByOne)
{
    TestTable t;
    t.add_column(type_Int, "1");
    t.add_column(type_String, "2");

    for (size_t i = 0; i < REALM_MAX_BPNODE_SIZE * 2; ++i) {
        add(t, 1, "a");
    }

    // Top
    t[0].set_int(0, 0);
    size_t res = t.where().equal(0, 0).find();
    CHECK_EQUAL(0, res);
    t[0].set_int(0, 1); // reset

    // Before split
    t[REALM_MAX_BPNODE_SIZE - 1].set_int(0, 0);
    res = t.where().equal(0, 0).find();
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE - 1, res);
    t[REALM_MAX_BPNODE_SIZE - 1].set_int(0, 1); // reset

    // After split
    t[REALM_MAX_BPNODE_SIZE].set_int(0, 0);
    res = t.where().equal(0, 0).find();
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE, res);
    t[REALM_MAX_BPNODE_SIZE].set_int(0, 1); // reset

    // Before end
    const size_t last_pos = (REALM_MAX_BPNODE_SIZE * 2) - 1;
    t[last_pos].set_int(0, 0);
    res = t.where().equal(0, 0).find();
    CHECK_EQUAL(last_pos, res);
}

TEST(Query_Const)
{
    TestTable t;
    t.add_column(type_Int, "1");
    t.add_column(type_String, "2");

    add(t, 10, "a");
    add(t, 100, "b");
    add(t, 20, "a");

    const Table& const_table = t;

    const size_t count = const_table.where().equal(1, "a").count();
    CHECK_EQUAL(2, count);

    // TODO: Should not be possible
    const_table.where().equal(1, "a").remove();
}

TEST(Query_AllTypesDynamicallyTyped)
{
    for (int nullable = 0; nullable < 2; nullable++) {
        bool n = (nullable == 1);

        Table table;
        DescriptorRef sub1;
        table.add_column(type_Bool, "boo", n);
        table.add_column(type_Int, "int", n);
        table.add_column(type_Float, "flt", n);
        table.add_column(type_Double, "dbl", n);
        table.add_column(type_String, "str", n);
        table.add_column(type_Binary, "bin", n);
        table.add_column(type_OldDateTime, "dat", n);
        table.add_column(type_Table, "tab", &sub1);
        table.add_column(type_Mixed, "mix");
        sub1->add_column(type_Int, "sub_int");
        sub1.reset();

        const char bin[4] = {0, 1, 2, 3};
        BinaryData bin1(bin, sizeof bin / 2);
        BinaryData bin2(bin, sizeof bin);
        int_fast64_t time_now = time(nullptr);
        Mixed mix_int(int64_t(1));
        Mixed mix_subtab((Mixed::subtable_tag()));

        table.add_empty_row();
        table.set_bool(0, 0, false);
        table.set_int(1, 0, 54);
        table.set_float(2, 0, 0.7f);
        table.set_double(3, 0, 0.8);
        table.set_string(4, 0, "foo");
        table.set_binary(5, 0, bin1);
        table.set_olddatetime(6, 0, 0);
        table.set_mixed(8, 0, mix_int);

        table.add_empty_row();
        table.set_bool(0, 1, true);
        table.set_int(1, 1, 506);
        table.set_float(2, 1, 7.7f);
        table.set_double(3, 1, 8.8);
        table.set_string(4, 1, "banach");
        table.set_binary(5, 1, bin2);
        table.set_olddatetime(6, 1, time_now);
        TableRef subtab = table.get_subtable(7, 1);
        subtab->add_empty_row();
        subtab->set_int(0, 0, 100);
        table.set_mixed(8, 1, mix_subtab);

        CHECK_EQUAL(1, table.where().equal(0, false).count());
        CHECK_EQUAL(1, table.where().equal(1, int64_t(54)).count());
        CHECK_EQUAL(1, table.where().equal(2, 0.7f).count());
        CHECK_EQUAL(1, table.where().equal(3, 0.8).count());
        CHECK_EQUAL(1, table.where().equal(4, "foo").count());
        CHECK_EQUAL(1, table.where().equal(5, bin1).count());
        CHECK_EQUAL(1, table.where().equal_olddatetime(6, 0).count());
        //    CHECK_EQUAL(1, table.where().equal(7, subtab).count());
        //    CHECK_EQUAL(1, table.where().equal(8, mix_int).count());

        Query query = table.where().equal(0, false);

        size_t ndx = not_found;

        CHECK_EQUAL(54, query.minimum_int(1));
        query.minimum_int(1, nullptr, 0, not_found, not_found, &ndx);
        CHECK_EQUAL(0, ndx);

        CHECK_EQUAL(54, query.maximum_int(1));
        query.maximum_int(1, nullptr, 0, not_found, not_found, &ndx);
        CHECK_EQUAL(0, ndx);

        CHECK_EQUAL(54, query.sum_int(1));
        CHECK_EQUAL(54, query.average_int(1));

        CHECK_EQUAL(0.7f, query.minimum_float(2));
        query.minimum_float(2, nullptr, 0, not_found, not_found, &ndx);
        CHECK_EQUAL(0, ndx);

        CHECK_EQUAL(0.7f, query.maximum_float(2));
        query.maximum_float(2, nullptr, 0, not_found, not_found, &ndx);
        CHECK_EQUAL(0, ndx);

        CHECK_EQUAL(0.7f, query.sum_float(2));
        CHECK_EQUAL(0.7f, query.average_float(2));

        CHECK_EQUAL(0.8, query.minimum_double(3));
        query.minimum_double(3, nullptr, 0, not_found, not_found, &ndx);
        CHECK_EQUAL(0, ndx);

        CHECK_EQUAL(0.8, query.maximum_double(3));
        query.maximum_double(3, nullptr, 0, not_found, not_found, &ndx);
        CHECK_EQUAL(0, ndx);

        CHECK_EQUAL(0.8, query.sum_double(3));
        CHECK_EQUAL(0.8, query.average_double(3));
    }
}

TEST(Query_AggregateSortedView)
{
    Table table;
    table.add_column(type_Double, "col");

    const int count = REALM_MAX_BPNODE_SIZE * 2;
    table.add_empty_row(count);
    for (int i = 0; i < count; ++i)
        table.set_double(0, i, i + 1); // no 0s to reduce chance of passing by coincidence

    TableView tv = table.where().greater(0, 1.0).find_all();
    tv.sort(0, false);

    CHECK_EQUAL(2.0, tv.minimum_double(0));
    CHECK_EQUAL(count, tv.maximum_double(0));
    CHECK_APPROXIMATELY_EQUAL((count + 1) * count / 2, tv.sum_double(0), .1);
}


TEST(Query_RefCounting)
{
    Table* t = LangBindHelper::new_table();
    t->add_column(type_Int, "myint");
    t->insert_empty_row(0);
    t->set_int(0, 0, 12);

    Query q = t->where();

    LangBindHelper::unbind_table_ptr(t);

    // Now try to access Query and see that the Table is still alive
    TableView tv = q.find_all();
    CHECK_EQUAL(1, tv.size());
}


TEST(Query_DeepCopy)
{
    // NOTE: You can only create a copy of a fully constructed; i.e. you cannot copy a query which is missing an
    // end_group(). Run Query::validate() to see if it's fully constructed.

    TestTable t;
    t.add_column(type_Int, "1");
    t.add_column(type_String, "2");
    t.add_column(type_Double, "3");

    add(t, 1, "1", 1.1);
    add(t, 2, "2", 2.2);
    add(t, 3, "3", 3.3);
    add(t, 4, "4", 4.4);

    Query q = t.column<Int>(0) >
              Value<Int>(2); // Explicit use of Value<>() makes query_expression node instead of query_engine.


    // Test if we can execute a copy
    Query q2(q);

    CHECK_EQUAL(2, q2.find());


    // See if we can execute a copy of a deleted query. The copy should not contain references to the original.
    Query* q3 = new Query(q);
    Query* q4 = new Query(*q3);
    delete q3;


    // Attempt to overwrite memory of the deleted q3 by allocating various sized objects so that a spurious execution
    // of methods on q3 can be detected (by making unit test crash).
    char* tmp[1000];
    for (size_t i = 0; i < sizeof(tmp) / sizeof(tmp[0]); i++) {
        tmp[i] = new char[i];
        memset(tmp[i], 0, i);
    }
    for (size_t i = 0; i < sizeof(tmp) / sizeof(tmp[0]); i++) {
        delete[] tmp[i];
    }

    CHECK_EQUAL(2, q4->find());
    delete q4;

    // See if we can append a criteria to a query
    Query q5 = t.column<Int>(0) >
               Value<Int>(2); // Explicit use of Value<>() makes query_expression node instead of query_engine
    q5.greater(2, 4.0);
    CHECK_EQUAL(3, q5.find());

    // See if we can append a criteria to a copy without modifying the original (copy should not contain references
    // to original). Tests query_expression integer node.
    Query q6 = t.column<Int>(0) >
               Value<Int>(2); // Explicit use of Value<>() makes query_expression node instead of query_engine
    Query q7(q6);

    q7.greater(2, 4.0);
    CHECK_EQUAL(3, q7.find());
    CHECK_EQUAL(2, q6.find());


    // See if we can append a criteria to a copy without modifying the original (copy should not contain references
    // to original). Tests query_engine integer node.
    Query q8 = t.column<Int>(0) > 2;
    Query q9(q8);

    q9.greater(2, 4.0);
    CHECK_EQUAL(3, q9.find());
    CHECK_EQUAL(2, q8.find());


    // See if we can append a criteria to a copy without modifying the original (copy should not contain references
    // to original). Tests query_engine string node.
    Query q10 = t.column<String>(1) != "2";
    Query q11(q10);

    q11.greater(2, 4.0);
    CHECK_EQUAL(3, q11.find());
    CHECK_EQUAL(0, q10.find());

    // Test and_query() on a copy
    Query q12 = t.column<Int>(0) > 2;
    Query q13(q12);

    q13.and_query(t.column<String>(1) != "3");
    CHECK_EQUAL(3, q13.find());
    CHECK_EQUAL(2, q12.find());
}

TEST(Query_TableViewMoveAssign1)
{
    TestTable t;
    t.add_column(type_Int, "1");
    t.add_column(type_String, "2");
    t.add_column(type_Double, "3");

    add(t, 1, "1", 1.1);
    add(t, 2, "2", 2.2);
    add(t, 3, "3", 3.3);
    add(t, 4, "4", 4.4);

    // temporary query is created, then q makes and stores a deep copy and then temporary is destructed
    Query q = t.column<Int>(0) >
              Value<Int>(2); // Explicit use of Value<>() makes query_expression node instead of query_engine

    // now deep copy should be destructed and replaced by new temporary
    TableView tv = q.find_all();

    // the original should still work; destruction of temporaries and deep copies should have no references
    // to original
    tv = q.find_all();
}

TEST(Query_TableViewMoveAssignLeak2)
{
    Table t;
    t.add_column(type_Int, "1");
    t.add_column(type_String, "2");
    t.add_column(type_Double, "3");

    Query q = t.column<Int>(0) < t.column<double>(2) && t.column<String>(1) == "4";
    TableView tv = q.find_all();

    // Upon each find_all() call, tv copies the query 'q' into itself. See if this copying works
    tv = q.find_all();
    tv = q.find_all();
    tv = q.find_all();
    tv = q.find_all();
    tv = q.find_all();

    tv.sort(0, true);

    tv = q.find_all();

    Query q2 = t.column<Int>(0) <= t.column<double>(2);
    tv = q2.find_all();
    q.and_query(q2);
    tv = q.find_all();

    tv.sync_if_needed();

    size_t t2 = q.find();
    static_cast<void>(t2);
    tv = q.find_all();
    tv.sync_if_needed();
    t2 = q.find();
    tv.sync_if_needed();
    tv = q.find_all();
    tv.sync_if_needed();
    t2 = q.find();
    tv.sync_if_needed();
    tv = q.find_all();
    tv.sync_if_needed();
    tv = q.find_all();
    tv.sync_if_needed();

    Query q3;

    q2 = t.column<Int>(0) <= t.column<double>(2);
    q3 = q2;

    q3.find();
    q2.find();
}


TEST(Query_DeepCopyLeak1)
{
    // NOTE: You can only create a copy of a fully constructed; i.e. you cannot copy a query which is missing an
    // end_group(). Run Query::validate() to see if it's fully constructed.

    TestTable t;
    t.add_column(type_Int, "1");
    t.add_column(type_String, "2");
    t.add_column(type_Double, "3");

    add(t, 1, "1", 1.1);
    add(t, 2, "2", 2.2);
    add(t, 3, "3", 3.3);
    add(t, 4, "4", 4.4);

    // See if copying of a mix of query_expression and query_engine nodes will leak
    Query q = !(t.column<Int>(0) > Value<Int>(2) && t.column<Int>(0) > 2 && t.column<double>(2) > 2.2) ||
              t.column<Int>(0) == 4 || t.column<Int>(0) == Value<Int>(4);
    Query q2(q);
    Query q3(q2);
}

TEST(Query_DeepCopyTest)
{
    // If Query::first vector was relocated because of push_back, then Query would crash, because referenced
    // pointers were pointing into it.
    Table table;
    table.add_column(type_Int, "first");

    Query q1 = table.where();

    Query q2(q1);

    q2.group();
    q2.end_group();
}

TEST(Query_StringIndexCrash)
{
    // Test for a crash which occured when a query testing for equality on a
    // string index was deep-copied after being run
    Table table;
    table.add_column(type_String, "s", true);
    table.add_search_index(0);

    Query q = table.where().equal(0, StringData(""));
    q.count();
    Query q2(q);
}

TEST(Query_NullStrings)
{
    Table table;
    table.add_column(type_String, "s", true);
    table.add_empty_row(3);

    Query q;
    TableView v;

    // Short strings
    table.set_string(0, 0, "Albertslund"); // Normal non-empty string
    table.set_string(0, 1, realm::null()); // NULL string
    table.set_string(0, 2, "");            // Empty string

    q = table.column<StringData>(0) == realm::null();
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_key(0));

    q = table.column<StringData>(0) != realm::null();
    v = q.find_all();
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(0, v.get_key(0));
    CHECK_EQUAL(2, v.get_key(1));

    // contrary to SQL, comparisons with realm::null() can be true in Realm (todo, discuss if we want this behaviour)
    q = table.column<StringData>(0) != StringData("Albertslund");
    v = q.find_all();
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(1, v.get_key(0));
    CHECK_EQUAL(2, v.get_key(1));

    q = table.column<StringData>(0) == "";
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(2, v.get_key(0));

    // Medium strings (16+)
    table.set_string(0, 0, "AlbertslundAlbertslundAlbert");

    q = table.column<StringData>(0) == realm::null();
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_key(0));

    q = table.column<StringData>(0) == "";
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(2, v.get_key(0));

    // Long strings (64+)
    table.set_string(0, 0, "AlbertslundAlbertslundAlbertslundAlbertslundAlbertslundAlbertslundAlbertslund");
    q = table.column<StringData>(0) == realm::null();
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_key(0));

    q = table.column<StringData>(0) == "";
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(2, v.get_key(0));
}

TEST(Query_Nulls_Fuzzy)
{
    for (int attributes = 1; attributes < 5; attributes++) {
        Random random(random_int<unsigned long>());

        for (size_t t = 0; t < 10; t++) {
            Table table;
            table.add_column(type_String, "string", true);

            if (attributes == 0) {
            }
            if (attributes == 1) {
                table.add_search_index(0);
            }
            else if (attributes == 2) {
                table.optimize(true);
            }
            else if (attributes == 3) {
                table.add_search_index(0);
                table.optimize(true);
            }
            else if (attributes == 4) {
                table.optimize(true);
                table.add_search_index(0);
            }

            // vector that is kept in sync with the column so that we can compare with it
            std::vector<std::string> v;

            // ArrayString capacity starts at 128 bytes, so we need lots of elements
            // to test if relocation works
            for (size_t i = 0; i < 100; i++) {
                unsigned char action = static_cast<unsigned char>(random.draw_int_max<unsigned int>(100));

                if (action > 48 && table.size() < 10) {
                    // Generate string with equal probability of being empty, null, short, medium and long, and with
                    // their contents having equal proability of being either random or a duplicate of a previous
                    // string. When it's random, each char must have equal probability of being 0 or non-0
                    char buf[] =
                        "This string is around 90 bytes long, which falls in the long-string type of Realm strings";
                    char* buf1 = static_cast<char*>(malloc(sizeof(buf)));
                    memcpy(buf1, buf, sizeof(buf));
                    char buf2[] =
                        "                                                                                         ";

                    StringData sd;
                    std::string st;

                    if (fastrand(1) == 0) {
                        // null string
                        sd = realm::null();
                        st = "null";
                    }
                    else {
                        // non-null string
                        size_t len = static_cast<size_t>(fastrand(3));
                        if (len == 0)
                            len = 0;
                        else if (len == 1)
                            len = 7;
                        else if (len == 2)
                            len = 27;
                        else
                            len = 73;

                        if (fastrand(1) == 0) {
                            // duplicate string
                            sd = StringData(buf1, len);
                            st = std::string(buf1, len);
                        }
                        else {
                            // random string
                            for (size_t s = 0; s < len; s++) {
                                if (fastrand(100) > 20)
                                    buf2[s] = 0; // zero byte
                                else
                                    buf2[s] = static_cast<char>(fastrand(255)); // random byte
                            }
                            // no generated string can equal "null" (our vector magic value for null) because
                            // len == 4 is not possible
                            sd = StringData(buf2, len);
                            st = std::string(buf2, len);
                        }
                    }

                    size_t pos = random.draw_int_max<size_t>(table.size());
                    table.insert_empty_row(pos);
                    table.set_string(0, pos, sd);

                    v.insert(v.begin() + pos, st);
                    free(buf1);
                }
                else if (table.size() > 0) {
                    // delete
                    size_t row = random.draw_int_max<size_t>(table.size() - 1);
                    table.remove(row);
                    v.erase(v.begin() + row);
                }


                CHECK_EQUAL(table.size(), v.size());
                for (size_t j = 0; j < table.size(); j++) {
                    if (v[j] == "null") {
                        CHECK(table.get_string(0, j).is_null());
                    }
                    else {
                        CHECK(table.get_string(0, j) == v[j]);
                    }
                }
            }
        }
    }
}


TEST(Query_BinaryNull)
{
    Table table;
    table.add_column(type_Binary, "first", true);
    table.add_empty_row(3);
    table.set_binary(0, 0, BinaryData());
    table.set_binary(0, 1, BinaryData("", 0)); // NOTE: Specify size = 0, else size turns into 1!
    table.set_binary(0, 2, BinaryData("foo"));

    TableView t;

    // Next gen syntax
    t = (table.column<BinaryData>(0) == BinaryData()).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(0, t.get_key(0));

    t = (BinaryData() == table.column<BinaryData>(0)).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(0, t.get_key(0));

    t = (table.column<BinaryData>(0) == BinaryData("", 0)).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(1, t.get_key(0));

    t = (BinaryData("", 0) == table.column<BinaryData>(0)).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(1, t.get_key(0));

    t = (table.column<BinaryData>(0) != BinaryData("", 0)).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(0, t.get_key(0));
    CHECK_EQUAL(2, t.get_key(1));

    t = (BinaryData("", 0) != table.column<BinaryData>(0)).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(0, t.get_key(0));
    CHECK_EQUAL(2, t.get_key(1));


    // Old syntax
    t = table.where().equal(0, BinaryData()).find_all();
    CHECK_EQUAL(0, t.get_key(0));
    CHECK_EQUAL(1, t.size());

    t = table.where().equal(0, BinaryData("", 0)).find_all();
    CHECK_EQUAL(1, t.get_key(0));
    CHECK_EQUAL(1, t.size());

    t = table.where().equal(0, BinaryData("foo")).find_all();
    CHECK_EQUAL(2, t.get_key(0));
    CHECK_EQUAL(1, t.size());

    t = table.where().not_equal(0, BinaryData()).find_all();
    CHECK_EQUAL(1, t.get_key(0));
    CHECK_EQUAL(2, t.get_key(1));
    CHECK_EQUAL(2, t.size());

    t = table.where().not_equal(0, BinaryData("", 0)).find_all();
    CHECK_EQUAL(0, t.get_key(0));
    CHECK_EQUAL(2, t.get_key(1));
    CHECK_EQUAL(2, t.size());

    t = table.where().begins_with(0, BinaryData()).find_all();
    CHECK_EQUAL(3, t.size());

    t = table.where().begins_with(0, BinaryData("", 0)).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(1, t.get_key(0));
    CHECK_EQUAL(2, t.get_key(1));

    t = table.where().begins_with(0, BinaryData("foo")).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(2, t.get_key(0));

    t = table.where().ends_with(0, BinaryData()).find_all();
    CHECK_EQUAL(3, t.size());

    t = table.where().ends_with(0, BinaryData("", 0)).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(1, t.get_key(0));
    CHECK_EQUAL(2, t.get_key(1));

    t = table.where().ends_with(0, BinaryData("foo")).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(2, t.get_key(0));
}

TEST(Query_IntegerNullOldQueryEngine)
{
    /*
        first   second  third
         null      100      1
            0     null      2
          123      200      3
          null    null      4
    */
    Table table;
    table.add_column(type_Int, "first", true);
    table.add_column(type_Int, "second", true);
    table.add_column(type_Int, "third", false);
    table.add_empty_row(4);

    table.set_int(0, 1, 0);
    table.set_int(0, 2, 123);

    table.set_int(1, 0, 100);
    table.set_int(1, 2, 200);

    table.set_int(2, 0, 1);
    table.set_int(2, 1, 2);
    table.set_int(2, 2, 3);
    table.set_int(2, 3, 4);

    TableView t;

    t = table.where().equal(0, null{}).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(0, t.get_key(0));
    CHECK_EQUAL(3, t.get_key(1));

    t = table.where().equal(0, 0).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(1, t.get_key(0));

    t = table.where().equal(0, 123).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(2, t.get_key(0));

    t = table.where().not_equal(0, null{}).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(1, t.get_key(0));
    CHECK_EQUAL(2, t.get_key(1));

    t = table.where().not_equal(0, 0).find_all();
    CHECK_EQUAL(3, t.size());
    CHECK_EQUAL(0, t.get_key(0));
    CHECK_EQUAL(2, t.get_key(1));
    CHECK_EQUAL(3, t.get_key(2));

    t = table.where().greater(0, 0).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(2, t.get_key(0));
}

TEST(Query_IntegerNonNull)
{
    Table table;
    table.add_column(type_Int, "first", false);
    table.add_empty_row(3);
    table.set_int(0, 1, 123);
    table.set_int(0, 2, 456);

    TableView t;

    // Fixme, should you be able to query a non-nullable column against null?
    //    t = table.where().equal(0, null{}).find_all();
    //    CHECK_EQUAL(0, t.size());
}


TEST(Query_64BitValues)
{
    Group g;
    size_t m;
    TableRef table = g.add_table("table");
    table->insert_column(0, type_Int, "key");
    table->insert_column(1, type_Int, "16bit");

    const int64_t start = 4485019129LL;
    const int64_t count = 20; // First 16 SSE-searched, four fallback
    const int64_t min = std::numeric_limits<int64_t>::min();
    const int64_t max = std::numeric_limits<int64_t>::max();
    table->add_empty_row(count);
    for (size_t i = 0; i < count; ++i) {
        table->set_int(0, i, start + i);
    }

    for (size_t i = 0; i < 5; i++) {
        // Insert values 5, 4, 3, 2, 1
        table->set_int(1, i, 5 - i);
    }

    m = table->where().less(1, 4).find();
    CHECK_EQUAL(2, m);

    m = table->where().less(1, 5).find();
    CHECK_EQUAL(1, m);

    CHECK_EQUAL(0, table->where().less(0, min).count());
    CHECK_EQUAL(0, table->where().less(0, start).count());
    CHECK_EQUAL(1, table->where().less(0, start + 1).count());
    CHECK_EQUAL(count, table->where().less(0, start + count).count());
    CHECK_EQUAL(count, table->where().less(0, max).count());

    CHECK_EQUAL(0, table->where().less_equal(0, min).count());
    CHECK_EQUAL(1, table->where().less_equal(0, start).count());
    CHECK_EQUAL(count, table->where().less_equal(0, start + count).count());
    CHECK_EQUAL(count, table->where().less_equal(0, max).count());

    CHECK_EQUAL(count, table->where().greater(0, min).count());
    CHECK_EQUAL(count - 1, table->where().greater(0, start).count());
    CHECK_EQUAL(1, table->where().greater(0, start + count - 2).count());
    CHECK_EQUAL(0, table->where().greater(0, start + count - 1).count());
    CHECK_EQUAL(0, table->where().greater(0, max).count());

    CHECK_EQUAL(count, table->where().greater_equal(0, min).count());
    CHECK_EQUAL(count, table->where().greater_equal(0, start).count());
    CHECK_EQUAL(count - 1, table->where().greater_equal(0, start + 1).count());
    CHECK_EQUAL(1, table->where().greater_equal(0, start + count - 1).count());
    CHECK_EQUAL(0, table->where().greater_equal(0, start + count).count());
    CHECK_EQUAL(0, table->where().greater_equal(0, max).count());
}

namespace {

void create_columns(TableRef table, bool nullable = true)
{
    table->insert_column(0, type_Int, "Price", nullable);
    table->insert_column(1, type_Float, "Shipping", nullable);
    table->insert_column(2, type_String, "Description", nullable);
    table->insert_column(3, type_Double, "Rating", nullable);
    table->insert_column(4, type_Bool, "Stock", nullable);
    table->insert_column(5, type_OldDateTime, "Delivery date", nullable);
    table->insert_column(6, type_Binary, "Photo", nullable);
    table->insert_column(7, type_Timestamp, "ts", nullable);
}

bool equals(TableView& tv, std::vector<size_t> indexes)
{
    if (static_cast<int>(tv.size()) != indexes.end() - indexes.begin()) {
        return false;
    }

    for (auto it = indexes.begin(); it != indexes.end(); ++it) {
        if (tv.get_key(it - indexes.begin()) != *it) {
            return false;
        }
    }

    return true;
}

void fill_data(TableRef table)
{
    table->add_empty_row(3);

    table->set_int(0, 0, 1);
    table->set_null(0, 1);
    table->set_int(0, 2, 3);

    table->set_null(1, 0);
    table->set_null(1, 1);
    table->set_float(1, 2, 30.f);

    table->set_string(2, 0, null());
    table->set_string(2, 1, "foo");
    table->set_string(2, 2, "bar");

    table->set_double(3, 0, 1.1);
    table->set_double(3, 1, 2.2);
    table->set_null(3, 2);

    table->set_bool(4, 0, true);
    table->set_null(4, 1);
    table->set_bool(4, 2, false);

    table->set_olddatetime(5, 0, OldDateTime(2016, 2, 2));
    table->set_null(5, 1);
    table->set_olddatetime(5, 2, OldDateTime(2016, 6, 6));
}

} // unnamed namespace

TEST(Query_NullShowcase)
{
    /*
    Here we show how comparisons and arithmetic with null works in queries. Basic rules:

    null    +, -, *, /          value   ==   null
    null    +, -, *, /          null    ==   null

    null    ==, >=, <=]         null    ==   true
    null    !=, >, <            null    ==   false

    null    ==, >=, <=, >, <    value   ==   false
    null    !=                  value   ==   true

    This does NOT follow SQL! In particular, (null == null) == true and
    (null != value) == true.

    NOTE NOTE: There is currently only very little syntax checking.

    NOTE NOTE: For BinaryData, use BinaryData() instead of null().

        Price<int>      Shipping<float>     Description<String>     Rating<double>      Stock<bool>
    Delivery<OldDateTime>   Photo<BinaryData>
        -------------------------------------------------------------------------------------------------------------------------------------
    0   null            null                null                    1.1                 true          2016-2-2 "foo"
    1   10              null                "foo"                   2.2                 null          null
    zero-lenght non-null
    2   20              30.0                "bar"                   3.3                 false         2016-6-6 null
    */

    Group g;
    TableRef table = g.add_table("Inventory");
    create_columns(table);

    table->add_empty_row(3);

    // Default values for all nullable columns
    CHECK(table->is_null(0, 0));
    CHECK(table->is_null(1, 0));
    CHECK(table->is_null(2, 0));
    CHECK(table->is_null(3, 0));
    CHECK(table->is_null(4, 0));
    CHECK(table->is_null(5, 0));
    CHECK(table->is_null(6, 0));

    table->set_null(0, 0);
    table->set_int(0, 1, 10);
    table->set_int(0, 2, 20);

    table->set_null(1, 0);
    table->set_null(1, 1);
    table->set_float(1, 2, 30.f);

    table->set_string(2, 0, null());
    table->set_string(2, 1, "foo");
    table->set_string(2, 2, "bar");

    table->set_double(3, 0, 1.1);
    table->set_double(3, 1, 2.2);
    table->set_double(3, 2, 3.3);

    table->set_bool(4, 0, true);
    table->set_null(4, 1);
    table->set_bool(4, 2, false);

    table->set_olddatetime(5, 0, OldDateTime(2016, 2, 2));
    table->set_null(5, 1);
    table->set_olddatetime(5, 2, OldDateTime(2016, 6, 6));

    table->set_binary(6, 0, BinaryData("foo"));
    table->set_binary(6, 1,
                      BinaryData("", 0)); // remember 0, else it will have length of 1 due to 0 termination of c++
    table->set_null(6, 2);

    Columns<Int> price = table->column<Int>(0);
    Columns<Float> shipping = table->column<Float>(1);
    Columns<Double> rating = table->column<Double>(3);
    Columns<Bool> stock = table->column<Bool>(4);
    Columns<OldDateTime> delivery = table->column<OldDateTime>(5);
    Columns<BinaryData> photo = table->column<BinaryData>(6);

    // check int/double type mismatch error handling
    CHECK_THROW_ANY(table->column<Int>(3));

    TableView tv;

    tv = (price == null()).find_all();
    CHECK(equals(tv, {0}));

    tv = (price != null()).find_all();
    CHECK(equals(tv, {1, 2}));

    // Note that this returns rows with null, which differs from SQL!
    tv = (price == shipping).find_all();
    CHECK(equals(tv, {0})); // null == null

    // If you add a != null criteria, you would probably get what most users intended, like in SQL
    tv = (price == shipping && price != null()).find_all();
    CHECK(equals(tv, {}));

    tv = (price != shipping).find_all();
    CHECK(equals(tv, {1, 2})); // 10 != null

    tv = (price < 0 || price > 0).find_all();
    CHECK(equals(tv, {1, 2}));

    // Shows that null + null == null, and 10 + null == null, and null < 100 == false
    tv = (price + shipping < 100).find_all();
    CHECK(equals(tv, {2}));

    //  null < 0 == false
    tv = (price < 0).find_all();
    CHECK(equals(tv, {}));

    //  null > 0 == false
    tv = (price == 0).find_all();
    CHECK(equals(tv, {}));

    // (null == 0) == false
    tv = (price > 0).find_all();
    CHECK(equals(tv, {1, 2}));

    // Show that power(null) == null
    tv = (power(price) == null()).find_all();
    CHECK(equals(tv, {0}));

    // Doubles
    // (null > double) == false
    tv = (price > rating).find_all();
    CHECK(equals(tv, {1, 2}));

    tv = (price + rating == null()).find_all();
    CHECK(equals(tv, {0}));

    tv = (price + rating != null()).find_all();
    CHECK(equals(tv, {1, 2}));


    // Booleans
    tv = (stock == true).find_all();
    CHECK(equals(tv, {0}));

    tv = (stock == false).find_all();
    CHECK(equals(tv, {2}));

    tv = (stock == null()).find_all();
    CHECK(equals(tv, {1}));

    tv = (stock != null()).find_all();
    CHECK(equals(tv, {0, 2}));

    // Dates
    tv = (delivery == OldDateTime(2016, 6, 6)).find_all();
    CHECK(equals(tv, {2}));

    tv = (delivery != OldDateTime(2016, 6, 6)).find_all();
    CHECK(equals(tv, {0, 1}));

    tv = (delivery == null()).find_all();
    CHECK(equals(tv, {1}));

    tv = (delivery != null()).find_all();
    CHECK(equals(tv, {0, 2}));

    // BinaryData
    //
    // BinaryData only supports == and !=, and you cannot compare two columns - only a column and a constant
    tv = (photo == BinaryData("foo")).find_all();
    CHECK(equals(tv, {0}));

    tv = (photo == BinaryData("", 0)).find_all();
    CHECK(equals(tv, {1}));

    tv = (photo == BinaryData()).find_all();
    CHECK(equals(tv, {2}));

    tv = (photo != BinaryData("foo")).find_all();
    CHECK(equals(tv, {1, 2}));

    // Old query syntax
    tv = table->where().equal(0, null()).find_all();
    CHECK(equals(tv, {0}));

    tv = table->where().not_equal(0, null()).find_all();
    CHECK(equals(tv, {1, 2}));

    // You can also compare against user-given null with > and <, but only in the expression syntax!
    tv = (price > null()).find_all();
    CHECK(equals(tv, {}));
    tv = (price + rating > null()).find_all();
    CHECK(equals(tv, {}));

    // As stated above, if you want to use `> null()`, you cannot do it in the old syntax. This is for source
    // code simplicity (would need tons of new method overloads that also need unit test testing, etc). So
    // following is not possible and will not compile
    // (tv = table->where().greater(0, null()).find_all());

    // Nullable floats in old syntax
    tv = table->where().equal(1, null()).find_all();
    CHECK(equals(tv, {0, 1}));

    tv = table->where().not_equal(1, null()).find_all();
    CHECK(equals(tv, {2}));

    tv = table->where().greater(1, 0.0f).find_all();
    CHECK(equals(tv, {2}));

    tv = table->where().less(1, 20.0f).find_all();
    CHECK(equals(tv, {}));

    // TableView
    size_t count;
    int64_t i;
    double d;
    OldDateTime dt;
    tv = table->where().find_all();

    // Integer column
    i = tv.maximum_int(0);
    CHECK_EQUAL(i, 20);

    i = tv.minimum_int(0);
    CHECK_EQUAL(i, 10);

    count = 123;
    d = tv.average_int(0, &count);
    CHECK_APPROXIMATELY_EQUAL(d, 15., 0.001);
    CHECK_EQUAL(count, 2);

    i = tv.sum_int(0);
    CHECK_EQUAL(i, 30);


    // Float column
    d = tv.maximum_float(1);
    CHECK_EQUAL(d, 30.);

    d = tv.minimum_float(1);
    CHECK_EQUAL(d, 30.);

    count = 123;
    d = tv.average_float(1, &count);
    CHECK_APPROXIMATELY_EQUAL(d, 30., 0.001);
    CHECK_EQUAL(count, 1);

    d = tv.sum_float(1);
    CHECK_APPROXIMATELY_EQUAL(d, 30., 0.001);

    // Double column
    d = tv.maximum_double(3);
    CHECK_EQUAL(d, 3.3);
    d = tv.minimum_double(3);
    CHECK_EQUAL(d, 1.1);
    d = tv.average_double(3);
    CHECK_APPROXIMATELY_EQUAL(d, (1.1 + 2.2 + 3.3) / 3, 0.001);
    d = tv.sum_double(3);
    CHECK_APPROXIMATELY_EQUAL(d, 1.1 + 2.2 + 3.3, 0.001);

    // OldDateTime column
    dt = tv.maximum_olddatetime(5);
    CHECK_EQUAL(dt, OldDateTime(2016, 6, 6));
    dt = tv.minimum_olddatetime(5);
    CHECK_EQUAL(dt, OldDateTime(2016, 2, 2));

    // NaN
    // null converts to 0 when calling get_float() on it. We intentionally do not return the bit pattern
    // for internal Realm representation, because that's a NaN, hence making it harder for the end user
    // to distinguish between his own NaNs and null
    CHECK_EQUAL(table->get_float(1, 0), 0);

    table->set_float(1, 0, std::numeric_limits<float>::signaling_NaN());
    table->set_float(1, 1, std::numeric_limits<float>::quiet_NaN());

    // Realm may return a signalling/quiet NaN that is different from the signalling/quiet NaN you stored
    // (the IEEE standard defines a sequence of bits in the NaN that can have custom contents). Realm does
    // not preserve these bits.
    CHECK(std::isnan(table->get_float(1, 0)));
    CHECK(std::isnan(table->get_float(1, 1)));


// FIXME: std::numeric_limits<float>::signaling_NaN() seems broken in VS2015 in that it returns a non-
// signaling NaN. A bug report has been filed to Microsoft. Update: It turns out that on 32-bit Intel
// Architecture (at least on my Core i7 in 32 bit code), if you push a float-NaN (fld instruction) that
// has bit 22 clear (indicates it's signaling), and pop it back (fst instruction), the FPU will toggle
// that bit into being set. All this needs further investigation, so a P2 has been created. Note that
// IEEE just began specifying signaling vs. non-signaling NaNs in 2008. Also note that all this seems
// to work fine on ARM in both 32 and 64 bit mode.

#if !defined(_WIN32) && !REALM_ARCHITECTURE_X86_32
    CHECK(null::is_signaling(table->get_float(1, 0)));
#endif

#ifndef _WIN32 // signaling_NaN() may be broken in VS2015 (see long comment above)
    CHECK(!null::is_signaling(table->get_float(1, 1)));
#endif

    CHECK(!table->is_null(1, 0));
    CHECK(!table->is_null(1, 1));

    table->set_double(3, 0, std::numeric_limits<double>::signaling_NaN());
    table->set_double(3, 1, std::numeric_limits<double>::quiet_NaN());
    CHECK(std::isnan(table->get_double(3, 0)));
    CHECK(std::isnan(table->get_double(3, 1)));

// signaling_NaN() broken in VS2015, and broken in 32bit intel
#if !defined(_WIN32) && !REALM_ARCHITECTURE_X86_32
    CHECK(null::is_signaling(table->get_double(3, 0)));
    CHECK(!null::is_signaling(table->get_double(3, 1)));
#endif

    CHECK(!table->is_null(3, 0));
    CHECK(!table->is_null(3, 1));

    // NOTE NOTE Queries on float/double columns that contain user-given NaNs are undefined.
}


// Test error handling and default values (user gives bad column type, is_null() returns false,
// get_float() must return 0.9 for null entries, etc, etc)
TEST(Query_Null_DefaultsAndErrorhandling)
{
    // Non-nullable columns: Tests is_nullable() and set_null()
    {
        Group g;
        TableRef table = g.add_table("Inventory");
        create_columns(table, false /* nullability */);

        table->add_empty_row(1);

        CHECK(!table->is_nullable(0));
        CHECK(!table->is_nullable(1));
        CHECK(!table->is_nullable(2));
        CHECK(!table->is_nullable(3));
        CHECK(!table->is_nullable(4));
        CHECK(!table->is_nullable(5));

        // is_null() on non-nullable column returns false. If you want it to throw, then do so
        // in the language binding
        CHECK(!table->is_null(0, 0));
        CHECK(!table->is_null(1, 0));
        CHECK(!table->is_null(2, 0));
        CHECK(!table->is_null(3, 0));
        CHECK(!table->is_null(4, 0));
        CHECK(!table->is_null(5, 0));

        CHECK_THROW_ANY(table->set_null(0, 0));
        CHECK_THROW_ANY(table->set_null(1, 0));
        CHECK_THROW_ANY(table->set_null(2, 0));
        CHECK_THROW_ANY(table->set_null(3, 0));
        CHECK_THROW_ANY(table->set_null(4, 0));
        CHECK_THROW_ANY(table->set_null(5, 0));

        // verify that set_null() did not have any side effects
        CHECK(!table->is_null(0, 0));
        CHECK(!table->is_null(1, 0));
        CHECK(!table->is_null(2, 0));
        CHECK(!table->is_null(3, 0));
        CHECK(!table->is_null(4, 0));
        CHECK(!table->is_null(5, 0));
    }

    // Nullable columns: Tests that default value is null, and tests is_nullable() and set_null()
    {
        Group g;
        TableRef table = g.add_table("Inventory");
        create_columns(table);
        table->add_empty_row(1);

        CHECK(table->is_nullable(0));
        CHECK(table->is_nullable(1));
        CHECK(table->is_nullable(2));
        CHECK(table->is_nullable(3));
        CHECK(table->is_nullable(4));
        CHECK(table->is_nullable(5));

        // default values should be null
        CHECK(table->is_null(0, 0));
        CHECK(table->is_null(1, 0));
        CHECK(table->is_null(2, 0));
        CHECK(table->is_null(3, 0));
        CHECK(table->is_null(4, 0));
        CHECK(table->is_null(5, 0));

        // calling get() on a numeric column must return following:
        CHECK_EQUAL(table->get_int(0, 0), 0);
        CHECK_EQUAL(table->get_float(1, 0), 0.0f);
        CHECK_EQUAL(table->get_double(3, 0), 0.0);
        CHECK_EQUAL(table->get_bool(4, 0), false);
        CHECK_EQUAL(table->get_olddatetime(5, 0), OldDateTime(0));

        // Set everything to non-null values
        table->set_int(0, 0, 0);
        table->set_float(1, 0, 0.f);
        table->set_string(2, 0, StringData("", 0));
        table->set_double(3, 0, 0.);
        table->set_bool(4, 0, false);
        table->set_olddatetime(5, 0, OldDateTime(0));

        CHECK(!table->is_null(0, 0));
        CHECK(!table->is_null(1, 0));
        CHECK(!table->is_null(2, 0));
        CHECK(!table->is_null(3, 0));
        CHECK(!table->is_null(4, 0));
        CHECK(!table->is_null(5, 0));

        table->set_null(0, 0);
        table->set_null(1, 0);
        table->set_null(2, 0);
        table->set_null(3, 0);
        table->set_null(4, 0);
        table->set_null(5, 0);

        CHECK(table->is_null(0, 0));
        CHECK(table->is_null(1, 0));
        CHECK(table->is_null(2, 0));
        CHECK(table->is_null(3, 0));
        CHECK(table->is_null(4, 0));
        CHECK(table->is_null(5, 0));
    }
}

// Tests queries that compare two columns with eachother in various ways. The columns have different
// integral types
TEST(Query_Null_Two_Columns)
{
    Group g;
    TableRef table = g.add_table("Inventory");
    create_columns(table);
    fill_data(table);

    Columns<Int> price = table->column<Int>(0);
    Columns<Float> shipping = table->column<Float>(1);
    Columns<String> description = table->column<String>(2);
    Columns<Double> rating = table->column<Double>(3);
    Columns<Bool> stock = table->column<Bool>(4);
    Columns<OldDateTime> delivery = table->column<OldDateTime>(5);

    TableView tv;

    /*
    Price<int>      Shipping<float>     Description<String>     Rating<double>      Stock<bool> Delivery<OldDateTime>
    ----------------------------------------------------------------------------------------------------------------
    0   1           null                null                    1.1                 true          2016-2-2
    1   null        null                "foo"                   2.2                 null          null
    2   3           30.0                "bar"                   null                false         2016-6-6
    */

    tv = (shipping > rating).find_all();
    CHECK(equals(tv, {}));

    tv = (shipping < rating).find_all();
    CHECK(equals(tv, {}));

    tv = (price == rating).find_all();
    CHECK(equals(tv, {}));

    tv = (price != rating).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (shipping == rating).find_all();
    CHECK(equals(tv, {}));

    tv = (shipping != rating).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    // Comparison column with itself
    tv = (shipping == shipping).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (shipping > shipping).find_all();
    CHECK(equals(tv, {}));

    tv = (shipping < shipping).find_all();
    CHECK(equals(tv, {}));

    tv = (shipping <= shipping).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (shipping >= shipping).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (rating == rating).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (rating != rating).find_all();
    CHECK(equals(tv, {}));

    tv = (rating > rating).find_all();
    CHECK(equals(tv, {}));

    tv = (rating < rating).find_all();
    CHECK(equals(tv, {}));

    tv = (rating >= rating).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (rating <= rating).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (stock == stock).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (stock != stock).find_all();
    CHECK(equals(tv, {}));

    tv = (price == price).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (price != price).find_all();
    CHECK(equals(tv, {}));

    tv = (price > price).find_all();
    CHECK(equals(tv, {}));

    tv = (price < price).find_all();
    CHECK(equals(tv, {}));

    tv = (price >= price).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (price <= price).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (delivery == delivery).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (delivery != delivery).find_all();
    CHECK(equals(tv, {}));

    tv = (delivery > delivery).find_all();
    CHECK(equals(tv, {}));

    tv = (delivery < delivery).find_all();
    CHECK(equals(tv, {}));

    tv = (delivery >= delivery).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (delivery <= delivery).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (description == description).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (description != description).find_all();
    CHECK(equals(tv, {}));

    // integer + null == null
    // note: booleans can convert to 0 and 1 when compared agaist numeric values, like in c++
    tv = (price + shipping == stock).find_all();
    CHECK(equals(tv, {1}));

    // Test a few untested things
    tv = table->where().equal(3, null()).find_all();
    CHECK(equals(tv, {2}));

    tv = table->where().equal(0, null()).find_all();
    CHECK(equals(tv, {1}));

    tv = table->where().not_equal(3, null()).find_all();
    CHECK(equals(tv, {0, 1}));

    tv = table->where().between(0, 2, 4).find_all();
    CHECK(equals(tv, {2}));

    // between for floats
    tv = table->where().between(1, 10.f, 40.f).find_all();
    CHECK(equals(tv, {2}));

    tv = table->where().between(1, 0.f, 20.f).find_all();
    CHECK(equals(tv, {}));

    tv = table->where().between(1, 40.f, 100.f).find_all();
    CHECK(equals(tv, {}));

    // between for doubles
    tv = table->where().between(3, 0., 100.).find_all();
    CHECK(equals(tv, {0, 1}));

    tv = table->where().between(3, 1., 2.).find_all();
    CHECK(equals(tv, {0}));

    tv = table->where().between(3, 2., 3.).find_all();
    CHECK(equals(tv, {1}));

    tv = table->where().between(3, 3., 100.).find_all();
    CHECK(equals(tv, {}));
}

// Between, count, min and max
TEST(Query_Null_BetweenMinMax_Nullable)
{
    Group g;
    TableRef table = g.add_table("Inventory");
    create_columns(table);
    table->add_empty_row();

    /*
    Price<int>      Shipping<float>     Description<String>     Rating<double>      Stock<bool>
    Delivery<OldDateTime>     ts<Timestamp>
    --------------------------------------------------------------------------------------------------------------------------------------
    null            null                null                    null                null            null null
    */

    TableView tv;
    size_t match;
    size_t count;

    // Here we test max/min/average with 0 rows used to compute the value, either becuase all inputs are null or
    // becuase 0 rows exist.
    auto test_tv = [&]() {
        // int
        match = 123;
        tv.maximum_int(0, &match);
        CHECK_EQUAL(match, npos);

        match = 123;
        tv.minimum_int(0, &match);
        CHECK_EQUAL(match, npos);

        CHECK_EQUAL(tv.sum_int(0), 0);
        count = 123;
        CHECK_EQUAL(tv.average_int(0, &count), 0.);
        CHECK_EQUAL(count, 0);

        // float
        match = 123;
        tv.maximum_float(1, &match);
        CHECK_EQUAL(match, npos);

        match = 123;
        tv.minimum_float(1, &match);
        CHECK_EQUAL(match, npos);

        CHECK_EQUAL(tv.sum_float(1), 0.);
        count = 123;
        CHECK_EQUAL(tv.average_float(1, &count), 0.);
        CHECK_EQUAL(count, 0);

        // double
        match = 123;
        tv.maximum_double(3, &match);
        CHECK_EQUAL(match, npos);

        match = 123;
        tv.minimum_double(3, &match);
        CHECK_EQUAL(match, npos);

        CHECK_EQUAL(tv.sum_double(3), 0.);
        count = 123;
        CHECK_EQUAL(tv.average_double(3, &count), 0.);
        CHECK_EQUAL(count, 0);

        // date
        match = 123;
        tv.maximum_olddatetime(5, &match);
        CHECK_EQUAL(match, npos);

        match = 123;
        tv.minimum_olddatetime(5, &match);
        CHECK_EQUAL(match, npos);

        // timestamp
        match = 123;
        tv.maximum_timestamp(7, &match);
        CHECK_EQUAL(match, npos);

        match = 123;
        tv.minimum_timestamp(7, &match);
        CHECK_EQUAL(match, npos);

    };

    // There are rows in TableView but they all point to null
    tv = table->where().find_all();
    test_tv();

    // There are 0 rows in TableView
    tv = table->where().equal(0, 123).find_all();
    test_tv();

    // Now we test that average does not include nulls in row count:
    /*
    Price<int>      Shipping<float>     Description<String>     Rating<double>      Stock<bool> Delivery<OldDateTime>
    ----------------------------------------------------------------------------------------------------------------
    null            null                null                    null                null            null
    10              10.f                null                    10.                 null            null
    */

    table->add_empty_row();
    table->set_int(0, 1, 10);
    table->set_float(1, 1, 10.f);
    table->set_double(3, 1, 10.);

    tv = table->where().find_all();
    count = 123;
    CHECK_EQUAL(tv.average_int(0, &count), 10);
    CHECK_EQUAL(count, 1);
    count = 123;
    CHECK_EQUAL(tv.average_float(1, &count), 10.);
    CHECK_EQUAL(count, 1);
    count = 123;
    CHECK_EQUAL(tv.average_double(3, &count), 10.);
    CHECK_EQUAL(count, 1);
}


// If number of rows is larger than 8, they can be loaded in chunks by the query system. Test if this works by
// creating a large table with nulls in arbitrary places and query for nulls. Verify the search result manually.
// Do that for all Realm types.
TEST(Query_Null_ManyRows)
{
    Group g;
    TableRef table = g.add_table("Inventory");
    create_columns(table);

    Columns<Int> price = table->column<Int>(0);
    Columns<Float> shipping = table->column<Float>(1);
    Columns<String> description = table->column<String>(2);
    Columns<Double> rating = table->column<Double>(3);
    Columns<Bool> stock = table->column<Bool>(4);
    Columns<OldDateTime> delivery = table->column<OldDateTime>(5);

    // Create lots of non-null rows
    for (size_t t = 0; t < 2000; t++) {
        table->add_empty_row(1);
        table->set_int(0, t, 123);
        table->set_float(1, t, 30.f);
        table->set_string(2, t, "foo");
        table->set_double(3, t, 12.3);
        table->set_bool(4, t, true);
        table->set_olddatetime(5, t, OldDateTime(2016, 2, 2));
    }

    // Reference lists used to verify query results
    std::vector<size_t> nulls;     // List of rows that have all fields set to null
    std::vector<size_t> non_nulls; // List of non-null rows

    // Fill in nulls in random rows, at each 10'th row on average
    for (size_t t = 0; t < table->size() / 10; t++) {
        // Bad but fast random generator
        size_t prime = 883;
        size_t random = ((t + prime) * prime + t) % table->size();

        // Test if already null (simplest way to avoid dublicates in our nulls vector)
        if (!table->is_null(0, random)) {
            table->set_null(0, random);
            table->set_null(1, random);
            table->set_null(2, random);
            table->set_null(3, random);
            table->set_null(4, random);
            table->set_null(5, random);
            nulls.push_back(random);
        }
    }

    // Fill out non_nulls vector
    for (size_t t = 0; t < table->size(); t++) {
        if (!table->is_null(0, t))
            non_nulls.push_back(t);
    }

    std::sort(nulls.begin(), nulls.end(), [](size_t a, size_t b) { return b > a; });
    TableView tv;

    // Search for nulls and non-nulls and verify matches against our manually created `nulls` and non_nulls vectors.
    // Do that for all Realm data types
    tv = (price == null()).find_all();
    CHECK(equals(tv, nulls));

    tv = (price != null()).find_all();
    CHECK(equals(tv, non_nulls));

    tv = (shipping == null()).find_all();
    CHECK(equals(tv, nulls));

    tv = (shipping != null()).find_all();
    CHECK(equals(tv, non_nulls));

    tv = (description == null()).find_all();
    CHECK(equals(tv, nulls));

    tv = (description != null()).find_all();
    CHECK(equals(tv, non_nulls));

    tv = (rating == null()).find_all();
    CHECK(equals(tv, nulls));

    tv = (rating != null()).find_all();
    CHECK(equals(tv, non_nulls));

    tv = (stock == null()).find_all();
    CHECK(equals(tv, nulls));

    tv = (stock != null()).find_all();
    CHECK(equals(tv, non_nulls));

    tv = (delivery == null()).find_all();
    CHECK(equals(tv, nulls));

    tv = (delivery != null()).find_all();
    CHECK(equals(tv, non_nulls));
}

TEST(Query_Null_Sort)
{
    Group g;
    TableRef table = g.add_table("Inventory");
    create_columns(table);

    table->add_empty_row(3);

    table->set_int(0, 0, 0);
    table->set_float(1, 0, 0.f);
    table->set_string(2, 0, "0");
    table->set_double(3, 0, 0.0);
    table->set_bool(4, 0, false);
    table->set_olddatetime(5, 0, OldDateTime(0));

    table->set_int(0, 2, 2);
    table->set_float(1, 2, 2.f);
    table->set_string(2, 2, "2");
    table->set_double(3, 2, 2.0);
    table->set_bool(4, 2, true);
    table->set_olddatetime(5, 2, OldDateTime(2000));

    for (int i = 0; i <= 5; i++) {
        TableView tv = table->where().find_all();
        CHECK(tv.size() == 3);

        tv.sort(i, true);
        CHECK_EQUAL(tv.get_key(0), 1);
        CHECK_EQUAL(tv.get_key(1), 0);
        CHECK_EQUAL(tv.get_key(2), 2);

        tv = table->where().find_all();
        tv.sort(i, false);
        CHECK_EQUAL(tv.get_key(0), 2);
        CHECK_EQUAL(tv.get_key(1), 0);
        CHECK_EQUAL(tv.get_key(2), 1);
    }
}

TEST(Query_LinkCounts)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    table1->add_column(type_String, "str");

    table1->add_empty_row();
    table1->set_string(0, 0, "abc");
    table1->add_empty_row();
    table1->set_string(0, 1, "def");
    table1->add_empty_row();
    table1->set_string(0, 2, "ghi");

    TableRef table2 = group.add_table("table2");
    size_t col_int = table2->add_column(type_Int, "int");
    size_t col_link = table2->add_column_link(type_Link, "link", *table1);
    size_t col_linklist = table2->add_column_link(type_LinkList, "linklist", *table1);

    table2->add_empty_row();
    table2->set_int(col_int, 0, 0);

    table2->add_empty_row();
    table2->set_int(col_int, 1, 1);
    table2->set_link(col_link, 1, 1);
    LinkViewRef links = table2->get_linklist(col_linklist, 1);
    links->add(1);

    table2->add_empty_row();
    table2->set_int(col_int, 2, 2);
    table2->set_link(col_link, 2, 2);
    links = table2->get_linklist(col_linklist, 2);
    links->add(1);
    links->add(2);

    Query q;
    size_t match;

    // Verify that queries against the count of a LinkList column work.
    q = table2->column<LnkLst>(col_linklist).count() == 0;
    match = q.find();
    CHECK_EQUAL(0, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).count() == 1;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).count() >= 1;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    // Verify that queries against the count of a Link column work.
    q = table2->column<Link>(col_link).count() == 0;
    match = q.find();
    CHECK_EQUAL(0, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<Link>(col_link).count() == 1;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    // Verify that reusing the count expression works.
    auto link_count = table2->column<LnkLst>(col_linklist).count();
    size_t match_count = (link_count == 0).count();
    CHECK_EQUAL(1, match_count);

    match_count = (link_count >= 1).count();
    CHECK_EQUAL(2, match_count);


    // Verify that combining the count expression with other queries on the same table works.
    q = table2->column<LnkLst>(col_linklist).count() == 1 && table2->column<Int>(col_int) == 1;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);
}

TEST(Query_Link_Minimum)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    table1->add_column(type_Int, "int", /* nullable */ true);
    table1->add_column(type_Float, "float", /* nullable */ true);
    table1->add_column(type_Double, "double", /* nullable */ true);

    // table1
    // 0: 789 789.0f 789.0
    // 1: 456 456.0f 456.0
    // 2: 123 123.0f 123.0
    // 3: null null null

    table1->add_empty_row();
    table1->set_int(0, 0, 789);
    table1->set_float(1, 0, 789.0f);
    table1->set_double(2, 0, 789.0);
    table1->add_empty_row();
    table1->set_int(0, 1, 456);
    table1->set_float(1, 1, 456.0f);
    table1->set_double(2, 1, 456.0);
    table1->add_empty_row();
    table1->set_int(0, 2, 123);
    table1->set_float(1, 2, 123.0f);
    table1->set_double(2, 2, 123.0);
    table1->add_empty_row();
    table1->set_null(0, 3);
    table1->set_null(1, 3);
    table1->set_null(2, 3);

    TableRef table2 = group.add_table("table2");
    size_t col_linklist = table2->add_column_link(type_LinkList, "linklist", *table1);

    // table2
    // 0: { }
    // 1: { 1 }
    // 2: { 1, 2 }
    // 3: { 1, 2, 3 }

    table2->add_empty_row();

    table2->add_empty_row();
    LinkViewRef links = table2->get_linklist(col_linklist, 1);
    links->add(1);

    table2->add_empty_row();
    links = table2->get_linklist(col_linklist, 2);
    links->add(1);
    links->add(2);

    table2->add_empty_row();
    links = table2->get_linklist(col_linklist, 3);
    links->add(1);
    links->add(2);
    links->add(3);

    Query q;
    size_t match;

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).min() == 123;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).min() == 456;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).min() == null();
    match = q.find();
    CHECK_EQUAL(0, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LnkLst>(col_linklist).column<Float>(1).min() == 123.0f;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Float>(1).min() == 456.0f;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LnkLst>(col_linklist).column<Double>(2).min() == 123.0;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Double>(2).min() == 456.0;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);
}

TEST(Query_Link_MaximumSumAverage)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    table1->add_column(type_Int, "int", /* nullable */ true);
    table1->add_column(type_Float, "float", /* nullable */ true);
    table1->add_column(type_Double, "double", /* nullable */ true);

    // table1
    // 0: 123 123.0f 123.0
    // 1: 456 456.0f 456.0
    // 2: 789 789.0f 789.0
    // 3: null null null

    table1->add_empty_row();
    table1->set_int(0, 0, 123);
    table1->set_float(1, 0, 123.0f);
    table1->set_double(2, 0, 123.0);
    table1->add_empty_row();
    table1->set_int(0, 1, 456);
    table1->set_float(1, 1, 456.0f);
    table1->set_double(2, 1, 456.0);
    table1->add_empty_row();
    table1->set_int(0, 2, 789);
    table1->set_float(1, 2, 789.0f);
    table1->set_double(2, 2, 789.0);
    table1->add_empty_row();
    table1->set_null(0, 3);
    table1->set_null(1, 3);
    table1->set_null(2, 3);

    TableRef table2 = group.add_table("table2");
    size_t col_double = table2->add_column(type_Double, "double");
    size_t col_link = table2->add_column_link(type_Link, "link", *table1);
    size_t col_linklist = table2->add_column_link(type_LinkList, "linklist", *table1);

    // table2
    // 0: 456.0 ->0 { }
    // 1: 456.0 ->1 { 1 }
    // 2: 456.0 ->2 { 1, 2 }
    // 3: 456.0 ->3 { 1, 2, 3 }

    table2->add_empty_row();
    table2->set_double(col_double, 0, 456.0);
    table2->set_link(col_link, 0, 0);

    table2->add_empty_row();
    table2->set_double(col_double, 1, 456.0);
    table2->set_link(col_link, 1, 1);
    LinkViewRef links = table2->get_linklist(col_linklist, 1);
    links->add(1);

    table2->add_empty_row();
    table2->set_double(col_double, 2, 456.0);
    table2->set_link(col_link, 2, 2);
    links = table2->get_linklist(col_linklist, 2);
    links->add(1);
    links->add(2);

    table2->add_empty_row();
    table2->set_double(col_double, 3, 456.0);
    table2->set_link(col_link, 3, 3);
    links = table2->get_linklist(col_linklist, 3);
    links->add(1);
    links->add(2);
    links->add(3);

    Query q;
    size_t match;

    // Maximum.

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).max() == 789;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).max() == 456;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).max() == null();
    match = q.find();
    CHECK_EQUAL(0, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).max() == table2->link(col_link).column<Int>(0);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).max() == table2->column<Double>(col_double);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LnkLst>(col_linklist).column<Float>(1).max() == 789.0f;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Float>(1).max() == 456.0f;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LnkLst>(col_linklist).column<Double>(2).max() == 789.0;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Double>(2).max() == 456.0;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    // Sum.
    // Floating point results below may be inexact for some combination of architectures, compilers, and compiler
    // flags.

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).sum() == 1245;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).sum() == 456;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).sum() == table2->link(col_link).column<Int>(0);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).sum() == table2->column<Double>(col_double);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LnkLst>(col_linklist).column<Float>(1).sum() == 1245.0f;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Float>(1).sum() == 456.0f;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LnkLst>(col_linklist).column<Double>(2).sum() == 1245.0;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Double>(2).sum() == 456.0;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    // Average.
    // Floating point results below may be inexact for some combination of architectures, compilers, and compiler
    // flags.

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).average() == 622.5;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).average() == 456;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).average() == null();
    match = q.find();
    CHECK_EQUAL(0, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).average() < table2->link(col_link).column<Int>(0);
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Int>(0).average() == table2->column<Double>(col_double);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LnkLst>(col_linklist).column<Float>(1).average() == 622.5;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Float>(1).average() == 456.0f;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LnkLst>(col_linklist).column<Double>(2).average() == 622.5;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LnkLst>(col_linklist).column<Double>(2).average() == 456.0;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);
}

TEST(Query_OperatorsOverLink)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    table1->add_column(type_Int, "int");
    table1->add_column(type_Double, "double");

    // table1
    // 0: 2 2.0
    // 1: 3 3.0

    table1->add_empty_row();
    table1->set_int(0, 0, 2);
    table1->set_double(1, 0, 2.0);
    table1->add_empty_row();
    table1->set_int(0, 1, 3);
    table1->set_double(1, 1, 3.0);

    TableRef table2 = group.add_table("table2");
    table2->add_column(type_Int, "int");
    size_t col_linklist = table2->add_column_link(type_LinkList, "linklist", *table1);

    // table2
    // 0:  0 { }
    // 1:  4 { 0 }
    // 2:  4 { 1, 0 }

    table2->add_empty_row();
    table2->set_int(0, 0, 0);

    table2->add_empty_row();
    table2->set_int(0, 1, 4);
    LinkViewRef links = table2->get_linklist(col_linklist, 1);
    links->add(0);

    table2->add_empty_row();
    table2->set_int(0, 2, 4);
    links = table2->get_linklist(col_linklist, 2);
    links->add(1);
    links->add(0);

    Query q;
    size_t match;

    // Unary operators.

    // Rows 1 and 2 should match this query as 2 * 2 == 4.
    // Row 0 should not as the power subexpression will not produce any results.
    q = power(table2->link(col_linklist).column<Int>(0)) == table2->column<Int>(0);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    // Rows 1 and 2 should match this query as 2 * 2 == 4.
    // Row 0 should not as the power subexpression will not produce any results.
    q = table2->column<Int>(0) == power(table2->link(col_linklist).column<Int>(0));
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    // Rows 1 and 2 should match this query as 2.0 * 2.0 == 4.0.
    // Row 0 should not as the power subexpression will not produce any results.
    q = power(table2->link(col_linklist).column<Double>(1)) == table2->column<Int>(0);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    // Rows 1 and 2 should match this query as 2.0 * 2.0 == 4.0.
    // Row 0 should not as the power subexpression will not produce any results.
    q = table2->column<Int>(0) == power(table2->link(col_linklist).column<Double>(1));
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    // Binary operators.

    // Rows 1 and 2 should match this query as 2 * 2 == 4.
    // Row 0 should not as the multiplication will not produce any results.
    q = table2->link(col_linklist).column<Int>(0) * 2 == table2->column<Int>(0);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    // Rows 1 and 2 should match this query as 2 * 2 == 4.
    // Row 0 should not as the multiplication will not produce any results.
    q = table2->column<Int>(0) == 2 * table2->link(col_linklist).column<Int>(0);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    // Rows 1 and 2 should match this query as 2.0 * 2.0 == 4.0.
    // Row 0 should not as the multiplication will not produce any results.
    q = table2->link(col_linklist).column<Double>(1) * 2 == table2->column<Int>(0);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    // Rows 1 and 2 should match this query as 2.0 * 2.0 == 4.0.
    // Row 0 should not as the multiplication will not produce any results.
    q = table2->column<Int>(0) == 2 * table2->link(col_linklist).column<Double>(1);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);
}

TEST(Query_CompareLinkedColumnVsColumn)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    table1->add_column(type_Int, "int");
    table1->add_column(type_Double, "double");

    // table1
    // 0: 2 2.0
    // 1: 3 3.0

    table1->add_empty_row();
    table1->set_int(0, 0, 2);
    table1->set_double(1, 0, 2.0);
    table1->add_empty_row();
    table1->set_int(0, 1, 3);
    table1->set_double(1, 1, 3.0);

    TableRef table2 = group.add_table("table2");
    table2->add_column(type_Int, "int");
    size_t col_link1 = table2->add_column_link(type_Link, "link1", *table1);
    size_t col_link2 = table2->add_column_link(type_Link, "link2", *table1);

    // table2
    // 0: 0 {   } { 0 }
    // 1: 4 { 0 } { 1 }
    // 2: 4 { 1 } {   }

    table2->add_empty_row();
    table2->set_int(0, 0, 0);
    table2->set_link(col_link2, 0, 0);

    table2->add_empty_row();
    table2->set_int(0, 1, 4);
    table2->set_link(col_link1, 1, 0);
    table2->set_link(col_link2, 1, 1);

    table2->add_empty_row();
    table2->set_int(0, 2, 4);
    table2->set_link(col_link1, 2, 1);

    Query q;
    size_t match;

    q = table2->link(col_link1).column<Int>(0) < table2->column<Int>(0);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->link(col_link1).column<Double>(1) < table2->column<Int>(0);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);
}

TEST(Query_CompareThroughUnaryLinks)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    table1->add_column(type_Int, "int");
    table1->add_column(type_Double, "double");
    table1->add_column(type_String, "string");

    // table1
    // 0: 2 2.0 "abc"
    // 1: 3 3.0 "def"
    // 2: 8 8.0 "def"

    table1->add_empty_row();
    table1->set_int(0, 0, 2);
    table1->set_double(1, 0, 2.0);
    table1->set_string(2, 0, "abc");
    table1->add_empty_row();
    table1->set_int(0, 1, 3);
    table1->set_double(1, 1, 3.0);
    table1->set_string(2, 1, "def");
    table1->add_empty_row();
    table1->set_int(0, 2, 8);
    table1->set_double(1, 2, 8.0);
    table1->set_string(2, 2, "def");

    TableRef table2 = group.add_table("table2");
    size_t col_link1 = table2->add_column_link(type_Link, "link1", *table1);
    size_t col_link2 = table2->add_column_link(type_Link, "link2", *table1);

    // table2
    // 0: {   } { 0 }
    // 1: { 0 } { 1 }
    // 2: { 1 } { 2 }
    // 3: { 2 } {   }

    table2->add_empty_row();
    table2->set_link(col_link2, 0, 0);

    table2->add_empty_row();
    table2->set_link(col_link1, 1, 0);
    table2->set_link(col_link2, 1, 1);

    table2->add_empty_row();
    table2->set_link(col_link1, 2, 1);
    table2->set_link(col_link2, 2, 2);

    table2->add_empty_row();
    table2->set_link(col_link1, 3, 2);

    Query q;
    size_t match;

    q = table2->link(col_link1).column<Int>(0) < table2->link(col_link2).column<Int>(0);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->link(col_link1).column<Double>(1) < table2->link(col_link2).column<Double>(1);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->link(col_link1).column<String>(2) == table2->link(col_link2).column<String>(2);
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);
}

TEST(Query_DeepLink)
{

    //
    // +---------+--------+------------+
    // | int     | bool   | list       |
    // +---------+--------+------------+
    // |       0 | true   | null       |
    // |       1 | false  | 0          |
    // |       2 | true   | 0, 1       |
    // |       N | even(N)| 0, .., N-1 |
    // +---------+--------+-------------+

    const size_t N = 10;

    Group group;
    TableRef table = group.add_table("test");
    size_t col_int = table->add_column(type_Int, "int");
    size_t col_bool = table->add_column(type_Bool, "bool");
    size_t col_linklist = table->add_column_link(type_LinkList, "list", *table);

    for (size_t j = 0; j < N; ++j) {
        table->add_empty_row();
        table->set_int(col_int, j, j);
        table->set_bool(col_bool, j, (j % 2) == 0);

        LinkViewRef links = table->get_linklist(col_linklist, j);
        TableView view = table->where().find_all();
        for (size_t i = 0; i < view.size(); ++i) {
            links->add(i);
        }
    }

    Query query = table->link(col_linklist).column<Bool>(col_bool) == true;
    TableView view = query.find_all();
    CHECK_EQUAL(N, view.size());
}

TEST(Query_LinksToDeletedOrMovedRow)
{
    Group group;

    TableRef source = group.add_table("source");
    TableRef target = group.add_table("target");

    size_t col_link = source->add_column_link(type_Link, "link", *target);
    size_t col_name = target->add_column(type_String, "name");

    target->add_empty_row(3);
    target->set_string(col_name, 0, "A");
    target->set_string(col_name, 1, "B");
    target->set_string(col_name, 2, "C");

    source->add_empty_row(3);
    source->set_link(col_link, 0, 0);
    source->set_link(col_link, 1, 1);
    source->set_link(col_link, 2, 2);

    Query qA = source->column<Link>(col_link) == target->get(0);
    Query qB = source->column<Link>(col_link) == target->get(1);
    Query qC = source->column<Link>(col_link) == target->get(2);

    // Move row C over row A. Row C is now at position 0, and row A has been removed.
    target->move_last_over(0);

    // Row A should not be found as it has been removed.
    TableView tvA = qA.find_all();
    CHECK_EQUAL(0, tvA.size());

    // Row B should be found as it was not changed.
    TableView tvB = qB.find_all();
    CHECK_EQUAL(1, tvB.size());
    CHECK_EQUAL(1, tvB[0].get_link(col_link));
    CHECK_EQUAL("B", target->get_string(col_name, tvB[0].get_link(col_link)));

    // Row C should still be found, despite having been moved.
    TableView tvC = qC.find_all();
    CHECK_EQUAL(1, tvC.size());
    CHECK_EQUAL(0, tvC[0].get_link(col_link));
    CHECK_EQUAL("C", target->get_string(col_name, tvC[0].get_link(col_link)));
}

// Triggers bug in compare_relation()
TEST(Query_BrokenFindGT)
{
    Group group;
    TableRef table = group.add_table("test");
    size_t col = table->add_column(type_Int, "int");

    const size_t rows = 12;
    for (size_t i = 0; i < rows; ++i) {
        table->add_empty_row();
        table->set_int(col, i, i + 2);
    }

    table->add_empty_row();
    table->set_int(col, rows + 0, 1);

    table->add_empty_row();
    table->set_int(col, rows + 1, 1);

    table->add_empty_row();
    table->set_int(col, rows + 2, 1);

    for (size_t i = 0; i < 3; ++i) {
        table->add_empty_row();
        table->set_int(col, rows + 3 + i, i + 2);
    }

    CHECK_EQUAL(18, table->size());

    Query q = table->where().greater(col, 1);
    TableView tv = q.find_all();
    CHECK_EQUAL(15, tv.size());

    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_NOT_EQUAL(1, tv.get_int(col, i));
    }
}

// Small fuzzy test also to trigger bugs such as the compare_relation() bug above
TEST(Query_FuzzyFind)
{
    // TEST_DURATION is normally 0.
    for (size_t iter = 0; iter < 50 + TEST_DURATION * 2000; iter++) {
        Group group;
        TableRef table = group.add_table("test");
        size_t col = table->add_column(type_Int, "int");

        // The bug happened when values were stored in 4 bits or less. So create a table full of such random values
        const size_t rows = 18;
        for (size_t i = 0; i < rows; ++i) {
            table->add_empty_row();

            // Produce numbers -3 ... 17. Just to test edge cases around 4-bit values also
            int64_t t = (fastrand() % 21) - 3;
            table->set_int(col, i, t);
        }

        for (int64_t s = -2; s < 18; s++) {
            Query q_g = table->where().greater(col, s);
            TableView tv_g = q_g.find_all();
            for (size_t i = 0; i < tv_g.size(); ++i) {
                CHECK(tv_g.get_int(col, i) > s);
            }

            Query q_l = table->where().less(col, s);
            TableView tv_l = q_l.find_all();
            for (size_t i = 0; i < tv_l.size(); ++i) {
                CHECK(tv_l.get_int(col, i) < s);
            }

            Query q_le = table->where().less_equal(col, s);
            TableView tv_le = q_le.find_all();
            for (size_t i = 0; i < tv_le.size(); ++i) {
                CHECK(tv_le.get_int(col, i) <= s);
            }

            // Sum of values greater + less-or-equal should be total number of rows. This ensures that both
            // 1) no search results are *omitted* from find_all(), and no 2) results are *false* positives
            CHECK(tv_g.size() + tv_le.size() == rows);
        }
    }
}

TEST(Query_AverageNullableColumns)
{
    Table table;
    size_t col_int = table.add_column(type_Int, "int", true);
    size_t col_float = table.add_column(type_Float, "float", true);
    size_t col_double = table.add_column(type_Double, "double", true);

    CHECK_EQUAL(0, table.where().average_int(col_int));
    CHECK_EQUAL(0, table.where().average_float(col_float));
    CHECK_EQUAL(0, table.where().average_double(col_double));

    //
    // +-----+-------+--------+
    // | int | float | double |
    // +-----+-------+--------+
    // |   2 |     2 |      2 |
    // |   4 |     4 |      4 |
    // +-----+-------+--------+

    table.add_empty_row(2);

    table.set_int(col_int, 0, 2);
    table.set_int(col_int, 1, 4);

    table.set_float(col_float, 0, 2.0f);
    table.set_float(col_float, 1, 4.0f);

    table.set_double(col_double, 1, 4.0);
    table.set_double(col_double, 0, 2.0);

    CHECK_EQUAL(3, table.where().average_int(col_int));
    CHECK_EQUAL(3, table.where().average_float(col_float));
    CHECK_EQUAL(3, table.where().average_double(col_double));

    // Add a row with nulls in each column. These nulls must be treated as not existing, that is,
    // it must be such that the average of 2 + 2 + null == 2.
    table.add_empty_row();

    CHECK_EQUAL(3, table.where().average_int(col_int));
    CHECK_EQUAL(3, table.where().average_float(col_float));
    CHECK_EQUAL(3, table.where().average_double(col_double));
}

TEST(Query_NegativeNumbers)
{
    for (size_t nullable = 0; nullable < 2; ++nullable) {
        Group group;
        TableRef table = group.add_table("test");
        table->add_column(type_Int, "int", nullable == 0);

        int64_t id = -1;
        for (size_t i = 0; i < 10; ++i) {
            table->add_empty_row();
            table->set_int(0, i, id--);
        }

        CHECK_EQUAL(10, table->where().between(0, -10, -1).find_all().size());
        CHECK_EQUAL(10, (table->column<Int>(0) > -11).find_all().size());
        CHECK_EQUAL(10, table->where().greater(0, -11).find_all().size());
        CHECK_EQUAL(10, (table->column<Int>(0) >= -10).find_all().size());
        CHECK_EQUAL(10, table->where().greater_equal(0, -10).find_all().size());
        CHECK_EQUAL(10, (table->column<Int>(0) < 128).find_all().size());
        CHECK_EQUAL(10, table->where().less(0, 128).find_all().size());
        CHECK_EQUAL(10, (table->column<Int>(0) < 127).find_all().size());
        CHECK_EQUAL(10, table->where().less(0, 127).find_all().size());
        CHECK_EQUAL(10, (table->column<Int>(0) <= -1).find_all().size());
        CHECK_EQUAL(10, table->where().less_equal(0, -1).find_all().size());
        CHECK_EQUAL(10, (table->column<Int>(0) < 0).find_all().size());
        TableView view = table->where().less(0, 0).find_all();
        CHECK_EQUAL(10, view.size());
        id = -1;
        for (size_t i = 0; i < view.size(); ++i) {
            CHECK_EQUAL(id, view.get_int(0, i));
            id--;
        }
    }
}

// Exposes bug that would lead to nulls being included as 0 value in average when performed
// on Query. When performed on TableView or Table, it worked OK.
TEST(Query_MaximumSumAverage)
{
    for (int nullable = 0; nullable < 2; nullable++) {
        bool n = (nullable == 1);
        Group group;
        TableRef table1 = group.add_table("table1");
        table1->add_column(type_Int, "int1", /* nullable */ n);
        table1->add_column(type_Int, "int2", /* nullable */ n);
        table1->add_column(type_Double, "d", /* nullable */ n);

        // Create three identical columns with values: For the nullable case:
        //      3, 4, null
        // For non-nullable iteration:
        //      3, 4

        table1->add_empty_row(n ? 3 : 2);
        table1->set_int(0, 0, 3);
        table1->set_int(0, 1, 4);
        table1->set_int(1, 0, 3);
        table1->set_int(1, 1, 4);
        table1->set_double(2, 0, 3.);
        table1->set_double(2, 1, 4.);

        // Average
        {
            double d;

            // Those that have criterias include all rows, also those with null
            d = table1->where().average_int(0);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            d = table1->where().average_int(1);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            // Criteria on same column as average
            d = table1->where().not_equal(0, 1234).average_int(0);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            // Criteria on other column than average (triggers different code paths)
            d = table1->where().not_equal(0, 1234).average_int(1);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            // Average of double, criteria on integer
            d = table1->where().not_equal(0, 1234).average_double(2);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            d = table1->where().not_equal(2, 1234.).average_double(2);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);


            // Those with criteria now only include some rows, whereof none are null
            d = table1->where().average_int(0);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            d = table1->where().average_int(1);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            // Criteria on same column as average
            d = table1->where().equal(0, 3).average_int(0);
            CHECK_APPROXIMATELY_EQUAL(d, 3., 0.001);

            // Criteria on other column than average (triggers different code paths)
            d = table1->where().equal(0, 3).average_int(1);
            CHECK_APPROXIMATELY_EQUAL(d, 3., 0.001);

            // Average of double, criteria on integer
            d = table1->where().not_equal(0, 3).average_double(2);
            CHECK_APPROXIMATELY_EQUAL(d, 4., 0.001);

            d = table1->where().equal(2, 3.).average_double(2);
            CHECK_APPROXIMATELY_EQUAL(d, 3., 0.001);

            // Now using null as criteria
            d = (table1->column<Int>(0) != null()).average_double(2);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            d = (table1->column<Double>(2) != null()).average_double(2);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            d = (table1->column<Int>(0) != null()).average_int(0);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            d = (table1->column<Int>(1) != null()).average_int(0);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);
        }


        // Maximum
        {
            int64_t d;
            double dbl;
            // Those that have criterias include all rows, also those with null
            d = table1->where().maximum_int(0);
            CHECK_EQUAL(d, 4);

            d = table1->where().maximum_int(1);
            CHECK_EQUAL(d, 4);

            // Criteria on same column as maximum
            d = table1->where().not_equal(0, 1234).maximum_int(0);
            CHECK_EQUAL(d, 4);

            // Criteria on other column than maximum (triggers different code paths)
            d = table1->where().not_equal(0, 1234).maximum_int(1);
            CHECK_EQUAL(d, 4);

            // Average of double, criteria on integer
            dbl = table1->where().not_equal(0, 1234).maximum_double(2);
            CHECK_EQUAL(d, 4);

            dbl = table1->where().not_equal(2, 1234.).maximum_double(2);
            CHECK_EQUAL(d, 4.);


            // Those with criteria now only include some rows, whereof none are null
            d = table1->where().maximum_int(0);
            CHECK_EQUAL(d, 4);

            d = table1->where().maximum_int(1);
            CHECK_EQUAL(d, 4);

            // Criteria on same column as maximum
            d = table1->where().equal(0, 4).maximum_int(0);
            CHECK_EQUAL(d, 4);

            // Criteria on other column than maximum (triggers different code paths)
            d = table1->where().equal(0, 4).maximum_int(1);
            CHECK_EQUAL(d, 4);

            // Average of double, criteria on integer
            dbl = table1->where().not_equal(0, 3).maximum_double(2);
            CHECK_EQUAL(dbl, 4.);

            dbl = table1->where().equal(2, 3.).maximum_double(2);
            CHECK_EQUAL(dbl, 3.);

            // Now using null as criteria
            dbl = (table1->column<Int>(0) != null()).maximum_double(2);
            CHECK_EQUAL(dbl, 4.);

            dbl = (table1->column<Double>(2) != null()).maximum_double(2);
            CHECK_EQUAL(dbl, 4.);

            d = (table1->column<Int>(0) != null()).maximum_int(0);
            CHECK_EQUAL(dbl, 4);

            d = (table1->column<Int>(1) != null()).maximum_int(0);
            CHECK_EQUAL(dbl, 4);
        }


        // Minimum
        {
            int64_t d;
            double dbl;
            // Those that have criterias include all rows, also those with null
            d = table1->where().minimum_int(0);
            CHECK_EQUAL(d, 3);

            d = table1->where().minimum_int(1);
            CHECK_EQUAL(d, 3);

            // Criteria on same column as minimum
            d = table1->where().not_equal(0, 1234).minimum_int(0);
            CHECK_EQUAL(d, 3);

            // Criteria on other column than minimum (triggers different code paths)
            d = table1->where().not_equal(0, 1234).minimum_int(1);
            CHECK_EQUAL(d, 3);

            // Average of double, criteria on integer
            dbl = table1->where().not_equal(0, 1234).minimum_double(2);
            CHECK_EQUAL(d, 3);

            dbl = table1->where().not_equal(2, 1234.).minimum_double(2);
            CHECK_EQUAL(d, 3.);


            // Those with criteria now only include some rows, whereof none are null
            d = table1->where().minimum_int(0);
            CHECK_EQUAL(d, 3);

            d = table1->where().minimum_int(1);
            CHECK_EQUAL(d, 3);

            // Criteria on same column as minimum
            d = table1->where().equal(0, 4).minimum_int(0);
            CHECK_EQUAL(d, 4);

            // Criteria on other column than minimum (triggers different code paths)
            d = table1->where().equal(0, 4).minimum_int(1);
            CHECK_EQUAL(d, 4);

            // Average of double, criteria on integer
            dbl = table1->where().not_equal(0, 3).minimum_double(2);
            CHECK_EQUAL(dbl, 4.);

            dbl = table1->where().equal(2, 3.).minimum_double(2);
            CHECK_EQUAL(dbl, 3.);

            // Now using null as criteria
            dbl = (table1->column<Int>(0) != null()).minimum_double(2);
            CHECK_EQUAL(dbl, 3.);

            dbl = (table1->column<Double>(2) != null()).minimum_double(2);
            CHECK_EQUAL(dbl, 3.);

            d = (table1->column<Int>(0) != null()).minimum_int(0);
            CHECK_EQUAL(dbl, 3);

            d = (table1->column<Int>(1) != null()).minimum_int(0);
            CHECK_EQUAL(dbl, 3);
        }

        // Sum
        {
            int64_t d;
            double dbl;
            // Those that have criterias include all rows, also those with null
            d = table1->where().sum_int(0);
            CHECK_EQUAL(d, 7);

            // Criteria on same column as maximum
            d = table1->where().not_equal(0, 1234).sum_int(0);
            CHECK_EQUAL(d, 7);

            // Criteria on other column than maximum (triggers different code paths)
            d = table1->where().not_equal(0, 1234).sum_int(1);
            CHECK_EQUAL(d, 7);

            // Average of double, criteria on integer
            dbl = table1->where().not_equal(0, 1234).sum_double(2);
            CHECK_EQUAL(d, 7.);

            dbl = table1->where().not_equal(2, 1234.).sum_double(2);
            CHECK_APPROXIMATELY_EQUAL(dbl, 7., 0.001);


            // Those with criteria now only include some rows, whereof none are null
            d = table1->where().sum_int(0);
            CHECK_EQUAL(d, 7);

            d = table1->where().sum_int(1);
            CHECK_EQUAL(d, 7);

            // Criteria on same column as maximum
            d = table1->where().equal(0, 4).sum_int(0);
            CHECK_EQUAL(d, 4);

            // Criteria on other column than maximum (triggers different code paths)
            d = table1->where().equal(0, 4).sum_int(1);
            CHECK_EQUAL(d, 4);

            // Average of double, criteria on integer
            dbl = table1->where().not_equal(0, 3).sum_double(2);
            CHECK_APPROXIMATELY_EQUAL(dbl, 4., 0.001);

            dbl = table1->where().equal(2, 3.).sum_double(2);
            CHECK_APPROXIMATELY_EQUAL(dbl, 3., 0.001);

            // Now using null as criteria
            dbl = (table1->column<Int>(0) != null()).sum_double(2);
            CHECK_APPROXIMATELY_EQUAL(dbl, 7., 0.001);

            dbl = (table1->column<Double>(2) != null()).sum_double(2);
            CHECK_APPROXIMATELY_EQUAL(dbl, 7., 0.001);

            d = (table1->column<Int>(0) != null()).sum_int(0);
            CHECK_EQUAL(dbl, 7);

            d = (table1->column<Int>(1) != null()).sum_int(0);
            CHECK_EQUAL(dbl, 7);
        }


        // Count
        {
            int64_t d;
            d = table1->where().count();
            CHECK_EQUAL(d, n ? 3 : 2);

            d = table1->where().not_equal(0, 1234).count();
            CHECK_EQUAL(d, n ? 3 : 2);

            d = table1->where().equal(0, 4).count();
            CHECK_EQUAL(d, 1);

            d = table1->where().not_equal(0, 3).count();
            CHECK_EQUAL(d, n ? 2 : 1);

            d = table1->where().equal(2, 3.).count();
            CHECK_EQUAL(d, 1);

            // Now using null as criteria
            d = (table1->column<Int>(0) != null()).count();
            CHECK_EQUAL(d, 2);

            d = (table1->column<Double>(2) != null()).count();
            CHECK_EQUAL(d, 2);

            d = (table1->column<Int>(0) != null()).count();
            CHECK_EQUAL(d, 2);

            d = (table1->column<Int>(1) != null()).count();
            CHECK_EQUAL(d, 2);
        }
    }
}
#endif

TEST(Query_ReferDeletedLinkView)
{
    // Queries and TableViews that depend on a deleted LinkList will now produce valid empty-like results
    // (find() returns npos, find_all() returns empty TableView, sum() returns 0, etc.).
    // They will no longer throw exceptions or crash.
    Group group;
    TableRef table = group.add_table("table");
    auto col_link = table->add_column_link(type_LinkList, "children", *table);
    auto col_int = table->add_column(type_Int, "age");
    auto links = table->create_object().set(col_int, 123).get_linklist(col_link);
    Query q = table->where(links);
    TableView tv = q.find_all();

    // TableView that depends on LinkView soon to be deleted
    TableView tv_sorted = links.get_sorted_view(col_int);

    // First test depends_on_deleted_object()
    CHECK(!tv_sorted.depends_on_deleted_object());
    TableView tv2 = table->where(&tv).find_all();
    CHECK(!tv2.depends_on_deleted_object());

    // Delete LinkList so LinkView gets detached
    table->remove_object(table->begin());
    CHECK(!links.is_attached());
    CHECK(tv_sorted.depends_on_deleted_object());

    // See if "Query that depends on LinkView" returns sane "empty"-like values
    CHECK_EQUAL(q.find_all().size(), 0);
    CHECK_EQUAL(q.find(), null_key);
    CHECK_EQUAL(q.sum_int(col_int), 0);
    CHECK_EQUAL(q.count(), 0);
    size_t rows;
    q.average_int(col_int, &rows);
    CHECK_EQUAL(rows, 0);

    tv_sorted.sync_if_needed();
    // See if "TableView that depends on LinkView" returns sane "empty"-like values
    tv_sorted.average_int(col_int, &rows);
    CHECK_EQUAL(rows, 0);

    // Now check a "Query that depends on (TableView that depends on LinkView)"
    Query q2 = table->where(&tv_sorted);
    CHECK_EQUAL(q2.count(), 0);
    CHECK_EQUAL(q2.find(), null_key);

    CHECK(!links.is_attached());
    tv.sync_if_needed();

    // PLEASE NOTE that 'tv' will still return true in this case! Even though it indirectly depends on
    // the LinkView through multiple levels!
    CHECK(tv.is_attached());

    // Before executing any methods on a LinkList, you must still always check is_attached(). If you
    // call links->add() on a deleted LinkViewRef (where is_attached() == false), it will assert
    CHECK(!links.is_attached());
}

TEST(Query_SubQueries)
{
    Group group;

    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");

    // add some more columns to origin and target
    auto col_int_t = target->add_column(type_Int, "integers");
    auto col_string_t = target->add_column(type_String, "strings");
    // in order to use set_all, columns involved in set_all must be inserted first.
    auto col_link_o = origin->add_column_link(type_LinkList, "link", *target);


    // add some rows
    origin->create_object(ObjKey(0));
    origin->create_object(ObjKey(1));
    origin->create_object(ObjKey(2));

    target->create_object(ObjKey(0)).set_all(400, "hello");
    target->create_object(ObjKey(1)).set_all(500, "world");
    target->create_object(ObjKey(2)).set_all(600, "!");
    target->create_object(ObjKey(3)).set_all(600, "world");

    // set some links
    auto links0 = origin->get_object(ObjKey(0)).get_linklist(col_link_o);
    links0.add(ObjKey(1));

    auto links1 = origin->get_object(ObjKey(1)).get_linklist(col_link_o);
    links1.add(ObjKey(1));
    links1.add(ObjKey(2));

    ObjKey match;
    TableView tv;
    Query q;
    Query sub_query;

    // The linked rows for rows 0 and 2 all match ("world", 500). Row 2 does by virtue of having no rows.
    sub_query = target->column<String>(col_string_t) == "world" && target->column<Int>(col_int_t) == 500;
    q = origin->column<Link>(col_link_o, sub_query).count() == origin->column<Link>(col_link_o).count();
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(ObjKey(0), tv.get_key(0));
    CHECK_EQUAL(ObjKey(2), tv.get_key(1));

    // No linked rows match ("world, 600).
    sub_query = target->column<String>(col_string_t) == "world" && target->column<Int>(col_int_t) == 600;
    q = origin->column<Link>(col_link_o, sub_query).count() >= 1;
    match = q.find();
    CHECK_EQUAL(match, null_key);

    // Rows 0 and 1 both have at least one linked row that matches ("world", 500).
    sub_query = target->column<String>(col_string_t) == "world" && target->column<Int>(col_int_t) == 500;
    q = origin->column<Link>(col_link_o, sub_query).count() >= 1;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(ObjKey(0), tv.get_key(0));
    CHECK_EQUAL(ObjKey(1), tv.get_key(1));

    // Row 1 has at least one linked row that matches ("!", 600).
    sub_query = target->column<String>(col_string_t) == "!" && target->column<Int>(col_int_t) == 600;
    q = origin->column<Link>(col_link_o, sub_query).count() >= 1;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(ObjKey(1), tv.get_key(0));

    // Row 1 has two linked rows that contain either "world" or 600.
    sub_query = target->column<String>(col_string_t) == "world" || target->column<Int>(col_int_t) == 600;
    q = origin->column<Link>(col_link_o, sub_query).count() == 2;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(ObjKey(1), tv.get_key(0));

    // Rows 0 and 2 have at most one linked row that contains either "world" or 600. Row 2 does by virtue of having no
    // rows.
    sub_query = target->column<String>(col_string_t) == "world" || target->column<Int>(col_int_t) == 600;
    q = origin->column<Link>(col_link_o, sub_query).count() <= 1;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(ObjKey(0), tv.get_key(0));
    CHECK_EQUAL(ObjKey(2), tv.get_key(1));
}

// Ensure that Query's move constructor and move assignment operator don't result in
// a TableView owned by the query being double-deleted when the queries are destroyed.
TEST(Query_MoveDoesntDoubleDelete)
{
    Table table;

    {
        Query q1(table, std::unique_ptr<ConstTableView>(new TableView()));
        Query q2 = std::move(q1);
    }

    {
        Query q1(table, std::unique_ptr<ConstTableView>(new TableView()));
        Query q2;
        q2 = std::move(q1);
    }
}

TEST(Query_Timestamp)
{
    ObjKey match;
    size_t cnt;
    Table table;
    auto col_first = table.add_column(type_Timestamp, "first", true);
    auto col_second = table.add_column(type_Timestamp, "second", true);
    Columns<Timestamp> first = table.column<Timestamp>(col_first);
    Columns<Timestamp> second = table.column<Timestamp>(col_second);

    std::vector<ObjKey> keys;
    table.create_objects(6, keys);
    table.get_object(keys[0]).set(col_first, Timestamp(111, 222));
    table.get_object(keys[1]).set(col_first, Timestamp(111, 333));
    table.get_object(keys[2]).set(col_first, Timestamp(333, 444)).set(col_second, Timestamp(222, 222));
    table.get_object(keys[3]).set(col_first, Timestamp{});
    table.get_object(keys[4]).set(col_first, Timestamp(0, 0));
    table.get_object(keys[5]).set(col_first, Timestamp(-1000, 0));


    CHECK(table.get_object(keys[0]).get<Timestamp>(col_first) == Timestamp(111, 222));

    match = (first == Timestamp(111, 222)).find();
    CHECK_EQUAL(match, keys[0]);

    match = (first != Timestamp(111, 222)).find();
    CHECK_EQUAL(match, keys[1]);

    match = (first > Timestamp(111, 222)).find();
    CHECK_EQUAL(match, keys[1]);

    match = (first < Timestamp(111, 333)).find();
    CHECK_EQUAL(match, keys[0]);

    match = (first == Timestamp(0, 0)).find();
    CHECK_EQUAL(match, keys[4]);

    match = (first < Timestamp(111, 333)).find();
    CHECK_EQUAL(match, keys[0]);

    match = (first < Timestamp(0, 0)).find();
    CHECK_EQUAL(match, keys[5]);

    // Note: .count(), not find()
    cnt = (first < Timestamp(0, 0)).count();
    CHECK_EQUAL(cnt, 1);

    cnt = (first != Timestamp{}).count();
    CHECK_EQUAL(cnt, 5);

    cnt = (first != null{}).count();
    CHECK_EQUAL(cnt, 5);

    cnt = (first != Timestamp(0, 0)).count();
    CHECK_EQUAL(cnt, 5);

    match = (first < Timestamp(-100, 0)).find();
    CHECK_EQUAL(match, keys[5]);

    // Left-hand-side being Timestamp() constant, right being column
    match = (Timestamp(111, 222) == first).find();
    CHECK_EQUAL(match, keys[0]);

    match = (Timestamp{} == first).find();
    CHECK_EQUAL(match, keys[3]);

    match = (Timestamp(111, 222) > first).find();
    CHECK_EQUAL(match, keys[4]);

    match = (Timestamp(111, 333) < first).find();
    CHECK_EQUAL(match, keys[2]);

    match = (Timestamp(111, 222) >= first).find();
    CHECK_EQUAL(match, keys[0]);

    match = (Timestamp(111, 111) >= first).find();
    CHECK_EQUAL(match, keys[4]);

    match = (Timestamp(333, 444) <= first).find();
    CHECK_EQUAL(match, keys[2]);

    match = (Timestamp(111, 300) <= first).find();
    CHECK_EQUAL(match, keys[1]);

    match = (Timestamp(111, 222) != first).find();
    CHECK_EQUAL(match, keys[1]);

    // Compare column with self
    match = (first == first).find();
    CHECK_EQUAL(match, keys[0]);

    match = (first != first).find();
    CHECK_EQUAL(match, null_key);

    match = (first > first).find();
    CHECK_EQUAL(match, null_key);

    match = (first < first).find();
    CHECK_EQUAL(match, null_key);

    match = (first >= first).find();
    CHECK_EQUAL(match, keys[0]);

    match = (first <= first).find();
    CHECK_EQUAL(match, keys[0]);

    // Two different columns
    match = (first == second).find();
    CHECK_EQUAL(match, keys[3]); // null == null

    match = (first > second).find();
    CHECK_EQUAL(match, keys[2]); // Timestamp(333, 444) > Timestamp(111, 222)

    match = (first < second).find();
    CHECK_EQUAL(match, null_key); // Note that (null < null) == false
}

TEST(Query_Timestamp_Null)
{
    // Test that querying for null on non-nullable column (with default value being non-null value) is
    // possible (i.e. does not throw or fail) and also gives no search matches.
    Table table;
    ObjKey match;

    auto col0 = table.add_column(type_Timestamp, "first", false);
    auto col1 = table.add_column(type_Timestamp, "second", true);
    ObjKey k0 = table.create_object().get_key();

    Columns<Timestamp> first = table.column<Timestamp>(col0);
    Columns<Timestamp> second = table.column<Timestamp>(col1);

    match = (first == Timestamp{}).find();
    CHECK_EQUAL(match, null_key);

    match = (second == Timestamp{}).find();
    CHECK_EQUAL(match, k0);
}

// Ensure that coyping a Query copies a restricting TableView if the query owns the view.
TEST(Query_CopyRestrictingTableViewWhenOwned)
{
    Table table;

    {
        Query q1(table, std::unique_ptr<ConstTableView>(new TableView()));
        Query q2(q1);

        // Reset the source query, destroying the original TableView.
        q1 = {};

        // Operations on the copied query that touch the restricting view should not crash.
        CHECK_EQUAL(0, q2.count());
    }

    {
        Query q1(table, std::unique_ptr<ConstTableView>(new TableView()));
        Query q2;
        q2 = q1;

        // Reset the source query, destroying the original TableView.
        q1 = {};

        // Operations on the copied query that touch the restricting view should not crash.
        CHECK_EQUAL(0, q2.count());
    }
}

TEST(Query_SyncViewIfNeeded)
{
    Group group;
    TableRef source = group.add_table("source");
    TableRef target = group.add_table("target");

    auto col_links = source->add_column_link(type_LinkList, "link", *target);
    auto col_id = target->add_column(type_Int, "id");

    auto reset_table_contents = [&] {
        source->clear();
        target->clear();

        for (int64_t i = 0; i < 15; ++i) {
            target->create_object(ObjKey(i)).set(col_id, i);
        }

        LnkLst ll = source->create_object().get_linklist(col_links);
        for (size_t i = 6; i < 15; ++i) {
            ll.add(ObjKey(i));
        }
    };

    // Restricting TableView. Query::sync_view_if_needed() syncs the TableView if needed.
    {
        reset_table_contents();
        TableView restricting_view = target->where().greater(col_id, 5).find_all();
        Query q = target->where(&restricting_view).less(col_id, 10);

        // Bring the view out of sync with the table.
        target->get_object(ObjKey(7)).set(col_id, -7);
        target->get_object(ObjKey(8)).set(col_id, -8);

        // Verify that the query uses the view as-is.
        CHECK_EQUAL(4, q.count());
        CHECK_EQUAL(false, restricting_view.is_in_sync());

        // And that syncing the query brings the view back into sync.
        auto version = q.sync_view_if_needed();
        CHECK_EQUAL(true, restricting_view.is_in_sync());
        CHECK_EQUAL(2, q.count());
        CHECK_EQUAL(version[0].first, target->get_key());
        CHECK_EQUAL(version[0].second, target->get_content_version());
    }

    // Restricting LinkView.
    {
        reset_table_contents();
        LnkLst restricting_view = source->begin()->get_linklist(col_links);
        Query q = target->where(restricting_view).less(col_id, 10);
        CHECK_EQUAL(restricting_view.size(), 9);

        // Modify the underlying table to remove rows from the LinkView.
        target->remove_object(ObjKey(7));
        target->remove_object(ObjKey(8));

        // The view is out of sync.
        CHECK_EQUAL(false, restricting_view.is_in_sync());
        // Running the query will update embedded query
        CHECK_EQUAL(2, q.count());
        // The view is still out of sync.
        CHECK_EQUAL(false, restricting_view.is_in_sync());
        // Accessing it will bring it up to date
        CHECK_EQUAL(restricting_view.size(), 7);
        CHECK_EQUAL(true, restricting_view.is_in_sync());

        // And that syncing the query does nothing.
        auto version = q.sync_view_if_needed();
        CHECK_EQUAL(true, restricting_view.is_in_sync());
        CHECK_EQUAL(version[0].first, target->get_key());
        CHECK_EQUAL(version[0].second, target->get_content_version());
        CHECK_EQUAL(2, q.count());
    }

    // No restricting view. Query::sync_view_if_needed() does nothing.
    {
        reset_table_contents();
        Query q = target->where().greater(col_id, 5).less(col_id, 10);

        target->get_object(ObjKey(7)).set(col_id, -7);
        target->get_object(ObjKey(8)).set(col_id, -8);

        CHECK_EQUAL(2, q.count());

        auto version = q.sync_view_if_needed();
        CHECK_EQUAL(version[0].first, target->get_key());
        CHECK_EQUAL(version[0].second, target->get_content_version());
        CHECK_EQUAL(2, q.count());
    }

    // Query that is not associated with a Table. Query::sync_view_if_needed() does nothing.
    {
        reset_table_contents();
        Query q;

        auto version = q.sync_view_if_needed();
        CHECK(version.empty());
    }
}

// Ensure that two queries can be combined via Query::and_query, &&, and || even if one of them has no conditions.
TEST(Query_CombineWithEmptyQueryDoesntCrash)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");

    table.create_object().set(col_id, 0);
    table.create_object().set(col_id, 1);
    table.create_object().set(col_id, 2);

    {
        Query q = table.where().equal(col_id, 1);
        q.and_query(table.where());
        CHECK_EQUAL(1, q.find_all().size());
    }

    {
        Query q1 = table.where().equal(col_id, 1);
        Query q2 = table.where();
        q1.and_query(q2);
        CHECK_EQUAL(1, q1.count());
    }

    {
        Query q1 = table.where().equal(col_id, 1);
        Query q2 = table.where();
        q2.and_query(q1);
        CHECK_EQUAL(1, q2.count());
    }

    {
        Query q = table.where();
        q.and_query(table.where().equal(col_id, 1));
        CHECK_EQUAL(1, q.count());
    }

    {
        Query q1 = table.where().equal(col_id, 1);
        Query q2 = q1 && table.where();
        CHECK_EQUAL(1, q2.count());

        Query q3 = table.where() && q1;
        CHECK_EQUAL(1, q3.count());
    }

    {
        Query q1 = table.where().equal(col_id, 1);
        Query q2 = q1 || table.where();
        CHECK_EQUAL(1, q2.count());

        Query q3 = table.where() || q1;
        CHECK_EQUAL(1, q3.count());
    }
}

// Check that queries take into account restricting views, but still
// return row index into the underlying table
TEST(Query_AccountForRestrictingViews)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");

    table.create_object().set(col_id, 42);
    table.create_object().set(col_id, 43);
    table.create_object().set(col_id, 44);

    {
        // Create initial table view
        TableView results = table.where().equal(col_id, 44).find_all();
        CHECK_EQUAL(1, results.size());
        CHECK_EQUAL(44, results[0].get<Int>(col_id));

        // Create query based on restricting view
        Query q = Query(results.get_parent().where(&results));
        ObjKey obj_key = q.find();
        CHECK_EQUAL(obj_key, results.get_key(0));
    }
}

/*

// These tests fail on Windows due to lack of tolerance for invalid UTF-8 in the case mapping methods

TEST(Query_UTF8_Contains)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    table1->add_column(type_String, "str1");
    table1->add_empty_row();
    table1->set_string(0, 0, StringData("\x0ff\x000", 2));
    size_t m = table1->column<String>(0).contains(StringData("\x0ff\x000", 2), false).count();
    CHECK_EQUAL(1, m);
}


TEST(Query_UTF8_Contains_Fuzzy)
{
    Table table;
    table.add_column(type_String, "str1");
    table.add_empty_row();

    for (size_t t = 0; t < 10000; t++) {
        char haystack[10];
        char needle[7];

        for (size_t c = 0; c < 10; c++)
            haystack[c] = char(fastrand());

        for (size_t c = 0; c < 7; c++)
            needle[c] = char(fastrand());

        table.set_string(0, 0, StringData(haystack, 10));

        table.column<String>(0).contains(StringData(needle, fastrand(7)), false).count();
        table.column<String>(0).contains(StringData(needle, fastrand(7)), true).count();
    }
}
*/

TEST(Query_ArrayLeafRelocate)
{
    for (size_t iter = 0; iter < 10; iter++) {
        // Tests crash where a query node would have a SequentialGetter that pointed to an old array leaf
        // that was relocated. https://github.com/realm/realm-core/issues/2269
        // The above description does not apply to the cluster based implementation.
        Group group;

        TableRef contact = group.add_table("contact");
        TableRef contact_type = group.add_table("contact_type");

        auto col_int = contact_type->add_column(type_Int, "id");
        auto col_str = contact_type->add_column(type_String, "str");
        auto col_link = contact->add_column_link(type_LinkList, "link", *contact_type);

        std::vector<ObjKey> contact_type_keys;
        std::vector<ObjKey> contact_keys;
        contact_type->create_objects(10, contact_type_keys);
        contact->create_objects(10, contact_keys);

        Query q1 = (contact->link(col_link).column<Int>(col_int) == 0);
        Query q2 = contact_type->where().equal(col_int, 0);
        Query q3 = (contact_type->column<Int>(col_int) + contact_type->column<Int>(col_int) == 0);
        Query q4 = (contact_type->column<Int>(col_int) == 0);
        Query q5 = (contact_type->column<String>(col_str) == "hejsa");

        TableView tv = q1.find_all();
        TableView tv2 = q2.find_all();
        TableView tv3 = q3.find_all();
        TableView tv4 = q4.find_all();
        TableView tv5 = q5.find_all();

        contact->add_column(type_Float, "extra");
        contact_type->add_column(type_Float, "extra");

        for (size_t t = 0; t < REALM_MAX_BPNODE_SIZE + 1; t++) {
            Obj contact_obj = contact->create_object();
            Obj contact_type_obj = contact_type->create_object();
            //  contact_type.get()->set_string(1, t, "hejsa");

            auto ll = contact_obj.get_linklist(col_link);
            ll.add(contact_type_obj.get_key());

            if (t == 0 || t == REALM_MAX_BPNODE_SIZE) {
                tv.sync_if_needed();
                tv2.sync_if_needed();
                tv3.sync_if_needed();
                tv4.sync_if_needed();
                tv5.sync_if_needed();
            }
        }
    }
}


TEST(Query_ColumnDeletionSimple)
{
    Table foo;
    auto col_int0 = foo.add_column(type_Int, "a");
    auto col_int1 = foo.add_column(type_Int, "b");

    std::vector<ObjKey> keys;
    foo.create_objects(10, keys);

    foo.get_object(keys[3]).set(col_int0, 123);
    foo.get_object(keys[4]).set(col_int0, 123);
    foo.get_object(keys[7]).set(col_int0, 123);
    foo.get_object(keys[2]).set(col_int1, 456);
    foo.get_object(keys[4]).set(col_int1, 456);

    auto q1 = foo.column<Int>(col_int0) == 123;
    auto q2 = foo.column<Int>(col_int1) == 456;
    auto q3 = q1 || q2;
    TableView tv1 = q1.find_all();
    TableView tv2 = q2.find_all();
    TableView tv3 = q3.find_all();
    CHECK_EQUAL(tv1.size(), 3);
    CHECK_EQUAL(tv2.size(), 2);
    CHECK_EQUAL(tv3.size(), 4);

    foo.remove_column(col_int0);

    size_t x = 0;
    CHECK_LOGIC_ERROR(x = q1.count(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv1.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_EQUAL(x, 0);
    CHECK_EQUAL(tv1.size(), 0);

    // This one should succeed in spite the column index is 1 and we
    x = q2.count();
    tv2.sync_if_needed();
    CHECK_EQUAL(x, 2);
    CHECK_EQUAL(tv2.size(), 2);

    x = 0;
    CHECK_LOGIC_ERROR(x = q3.count(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv3.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_EQUAL(x, 0);
    CHECK_EQUAL(tv3.size(), 0);
}


TEST(Query_ColumnDeletionExpression)
{
    Table foo;
    auto col_int0 = foo.add_column(type_Int, "a");
    auto col_int1 = foo.add_column(type_Int, "b");
    auto col_date2 = foo.add_column(type_Timestamp, "c");
    auto col_date3 = foo.add_column(type_Timestamp, "d");
    auto col_str4 = foo.add_column(type_String, "e");
    auto col_float5 = foo.add_column(type_Float, "f");
    auto col_bin6 = foo.add_column(type_Binary, "g");

    Obj obj0 = foo.create_object();
    Obj obj1 = foo.create_object();
    Obj obj2 = foo.create_object();
    Obj obj3 = foo.create_object();
    Obj obj4 = foo.create_object();
    obj0.set(col_int0, 0);
    obj1.set(col_int0, 1);
    obj2.set(col_int0, 2);
    obj3.set(col_int0, 3);
    obj4.set(col_int0, 4);
    obj0.set(col_int1, 0);
    obj1.set(col_int1, 0);
    obj2.set(col_int1, 3);
    obj3.set(col_int1, 5);
    obj4.set(col_int1, 3);
    obj0.set(col_date2, Timestamp(100, 100));
    obj0.set(col_date3, Timestamp(200, 100));
    obj0.set(col_str4, StringData("Hello, world"));
    obj0.set(col_float5, 3.141592f);
    obj1.set(col_float5, 1.0f);
    obj0.set(col_bin6, BinaryData("Binary", 6));

    // Expression
    auto q = foo.column<Int>(col_int0) == foo.column<Int>(col_int1) + 1;
    // TwoColumnsNode
    auto q1 = foo.column<Int>(col_int0) == foo.column<Int>(col_int1);
    TableView tv = q.find_all();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv1.size(), 1);

    foo.remove_column(col_int0);
    size_t x = 0;
    CHECK_LOGIC_ERROR(x = q.count(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv1.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_EQUAL(x, 0);
    CHECK_EQUAL(tv.size(), 0);

    q = foo.column<Timestamp>(col_date2) < foo.column<Timestamp>(col_date3);
    // TimestampNode
    q1 = foo.column<Timestamp>(col_date3) == Timestamp(200, 100);
    tv = q.find_all();
    tv1 = q1.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv1.size(), 1);
    foo.remove_column(col_date3);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv1.sync_if_needed(), LogicError::column_does_not_exist);

    // StringNodeBase
    q = foo.column<String>(col_str4) == StringData("Hello, world");
    q1 = !(foo.column<String>(col_str4) == StringData("Hello, world"));
    tv = q.find_all();
    tv1 = q1.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv1.size(), 4);
    foo.remove_column(col_str4);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv1.sync_if_needed(), LogicError::column_does_not_exist);

    // FloatDoubleNode
    q = foo.column<Float>(col_float5) > 0.0f;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    foo.remove_column(col_float5);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);

    // BinaryNode
    q = foo.column<Binary>(col_bin6) != BinaryData("Binary", 6);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 4);
    foo.remove_column(col_bin6);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
}


TEST(Query_ColumnDeletionLinks)
{
    Group g;
    TableRef foo = g.add_table("foo");
    TableRef bar = g.add_table("bar");
    TableRef foobar = g.add_table("foobar");

    auto col_int0 = foobar->add_column(type_Int, "int");

    auto col_int1 = bar->add_column(type_Int, "int");
    auto col_link0 = bar->add_column_link(type_Link, "link", *foobar);

    auto col_link1 = foo->add_column_link(type_Link, "link", *bar);

    std::vector<ObjKey> foobar_keys;
    std::vector<ObjKey> bar_keys;
    std::vector<ObjKey> foo_keys;
    foobar->create_objects(5, foobar_keys);
    bar->create_objects(5, bar_keys);
    foo->create_objects(10, foo_keys);

    for (int i = 0; i < 5; i++) {
        foobar->get_object(foobar_keys[i]).set(col_int0, i);
        bar->get_object(bar_keys[i]).set(col_int1, i);
        bar->get_object(bar_keys[i]).set(col_link0, foobar_keys[i]);
        foo->get_object(foo_keys[i]).set(col_link1, bar_keys[i]);
    }
    auto q = foo->link(col_link1).link(col_link0).column<Int>(col_int0) == 2;
    auto q1 = foo->column<Link>(col_link1).is_null();
    auto q2 = foo->column<Link>(col_link1) == bar->get_object(bar_keys[2]);

    auto tv = q.find_all();
    auto cnt = q1.count();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(cnt, 5);
    cnt = q2.count();
    CHECK_EQUAL(cnt, 1);

    // remove integer column, should not affect query
    bar->remove_column(col_int1);
    tv.sync_if_needed();
    CHECK_EQUAL(tv.size(), 1);
    // remove link column, disaster
    bar->remove_column(col_link0);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
    foo->remove_column(col_link1);
    CHECK_LOGIC_ERROR(q1.count(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(q2.count(), LogicError::column_does_not_exist);
}


TEST(Query_CaseInsensitiveIndexEquality_CommonNumericPrefix)
{
    Table table;
    auto col_ndx = table.add_column(type_String, "id");
    table.add_search_index(col_ndx);

    ObjKey key0 = table.create_object().set(col_ndx, "111111111111111111111111").get_key();
    table.create_object().set(col_ndx, "111111111111111111111112");

    Query q = table.where().equal(col_ndx, "111111111111111111111111", false);
    CHECK_EQUAL(q.count(), 1);
    TableView tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv[0].get_key(), key0);
}


TEST_TYPES(Query_CaseInsensitiveNullable, std::true_type, std::false_type)
{
    Table table;
    bool nullable = true;
    constexpr bool with_index = TEST_TYPE::value;
    auto col_ndx = table.add_column(type_String, "id", nullable);
    if (with_index) {
        table.add_search_index(col_ndx);
    }

    table.create_object().set(col_ndx, "test");
    table.create_object().set(col_ndx, "words");
    ObjKey key2 = table.create_object().get_key();
    ObjKey key3 = table.create_object().get_key();
    table.create_object().set(col_ndx, "");
    table.create_object().set(col_ndx, "");

    bool case_sensitive = true;
    StringData null_string;
    Query q = table.where().equal(col_ndx, null_string, case_sensitive);
    CHECK_EQUAL(q.count(), 2);
    TableView tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_key(0), key2);
    CHECK_EQUAL(tv.get_key(1), key3);
    Query q2 = table.where().contains(col_ndx, null_string, case_sensitive);
    CHECK_EQUAL(q2.count(), 6);
    tv = q2.find_all();
    CHECK_EQUAL(tv.size(), 6);

    case_sensitive = false;
    q = table.where().equal(col_ndx, null_string, case_sensitive);
    CHECK_EQUAL(q.count(), 2);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_key(0), key2);
    CHECK_EQUAL(tv.get_key(1), key3);
    q2 = table.where().contains(col_ndx, null_string, case_sensitive);
    CHECK_EQUAL(q2.count(), 6);
    tv = q2.find_all();
    CHECK_EQUAL(tv.size(), 6);
}


TEST_TYPES(Query_Rover, std::true_type, std::false_type)
{
    constexpr bool nullable = TEST_TYPE::value;

    Table table;
    auto col = table.add_column(type_String, "name", nullable);
    table.add_search_index(col);

    table.create_object().set(col, "ROVER");
    table.create_object().set(col, "Rover");

    Query q = table.where().equal(col, "rover", false);
    CHECK_EQUAL(q.count(), 2);
    TableView tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
}


TEST(Query_IntOnly)
{
    Table table;
    auto c0 = table.add_column(type_Int, "i1");
    auto c1 = table.add_column(type_Int, "i2");

    table.create_object(ObjKey(7)).set_all(7, 6);
    table.create_object(ObjKey(19)).set_all(19, 9);
    table.create_object(ObjKey(5)).set_all(19, 22);
    table.create_object(ObjKey(21)).set_all(2, 6);

    auto q = table.column<Int>(c1) == 6;
    ObjKey key = q.find();
    CHECK_EQUAL(key, ObjKey(7));

    TableView tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get(0).get_key(), ObjKey(7));
    CHECK_EQUAL(tv.get(1).get_key(), ObjKey(21));

    auto q1 = table.where(&tv).equal(c0, 2);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(tv1.size(), 1);
    CHECK_EQUAL(tv1.get(0).get_key(), ObjKey(21));

    q1 = table.where(&tv).greater(c0, 5);
    tv1 = q1.find_all();
    CHECK_EQUAL(tv1.size(), 1);
    CHECK_EQUAL(tv1.get(0).get_key(), ObjKey(7));

    q = table.column<Int>(c0) == 19 && table.column<Int>(c1) == 9;
    key = q.find();
    CHECK_EQUAL(key.value, 19);

    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get(0).get_key(), ObjKey(19));

    // Two column expression
    q = table.column<Int>(c0) < table.column<Int>(c1);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get(0).get_key(), ObjKey(5));
    CHECK_EQUAL(tv.get(1).get_key(), ObjKey(21));
}

TEST(Query_LinksTo)
{
    Query q;
    ObjKey found_key;
    Group group;

    TableRef source = group.add_table("source");
    TableRef target = group.add_table("target");

    auto col_link = source->add_column_link(type_Link, "link", *target);
    auto col_linklist = source->add_column_link(type_LinkList, "linklist", *target);

    std::vector<ObjKey> target_keys;
    target->create_objects(10, target_keys);

    std::vector<ObjKey> source_keys;
    source->create_objects(10, source_keys);

    source->get_object(source_keys[2]).set(col_link, target_keys[2]);
    source->get_object(source_keys[5]).set(col_link, target_keys[5]);
    source->get_object(source_keys[9]).set(col_link, target_keys[9]);

    q = source->column<Link>(col_link) == target->get_object(target_keys[2]);
    found_key = q.find();
    CHECK_EQUAL(found_key, source_keys[2]);

    q = source->column<Link>(col_link) == target->get_object(target_keys[5]);
    found_key = q.find();
    CHECK_EQUAL(found_key, source_keys[5]);

    q = source->column<Link>(col_link) == target->get_object(target_keys[9]);
    found_key = q.find();
    CHECK_EQUAL(found_key, source_keys[9]);

    q = source->column<Link>(col_link) == target->get_object(target_keys[0]);
    found_key = q.find();
    CHECK_EQUAL(found_key, null_key);

    q = source->column<Link>(col_link).is_null();
    auto tv = q.find_all();
    CHECK_EQUAL(tv.size(), 7);

    q = source->column<Link>(col_link) != null();
    found_key = q.find();
    CHECK_EQUAL(found_key, source_keys[2]);

    auto linklist = source->get_object(source_keys[1]).get_linklist_ptr(col_linklist);
    linklist->add(target_keys[7]);
    linklist = source->get_object(source_keys[2]).get_linklist_ptr(col_linklist);
    linklist->add(target_keys[0]);
    linklist->add(target_keys[1]);
    linklist->add(target_keys[2]);
    linklist = source->get_object(source_keys[8]).get_linklist_ptr(col_linklist);
    linklist->add(target_keys[0]);
    linklist->add(target_keys[5]);
    linklist->add(target_keys[6]);

    q = source->column<Link>(col_linklist) == target->get_object(target_keys[5]);
    found_key = q.find();
    CHECK_EQUAL(found_key, source_keys[8]);

    q = source->column<Link>(col_linklist) != target->get_object(target_keys[7]);
    found_key = q.find();
    CHECK_EQUAL(found_key, source_keys[2]);
}

TEST(Query_Group_bug)
{
    // Tests for a bug in queries with OR nodes at different nesting levels

    Group g;
    TableRef service_table = g.add_table("service");
    TableRef profile_table = g.add_table("profile");
    TableRef person_table = g.add_table("person");

    auto col_service_id = service_table->add_column(type_String, "id");
    auto col_service_link = service_table->add_column_link(type_LinkList, "profiles", *profile_table);

    auto col_profile_id = profile_table->add_column(type_String, "role");
    auto col_profile_link = profile_table->add_column_link(type_Link, "services", *service_table);

    auto col_person_id = person_table->add_column(type_String, "id");
    auto col_person_link = person_table->add_column_link(type_LinkList, "services", *service_table);

    auto sk0 = service_table->create_object().set(col_service_id, "service_1").get_key();
    auto sk1 = service_table->create_object().set(col_service_id, "service_2").get_key();

    auto pk0 = profile_table->create_object().set(col_profile_id, "profile_1").get_key();
    auto pk1 = profile_table->create_object().set(col_profile_id, "profile_2").get_key();
    auto pk2 = profile_table->create_object().set(col_profile_id, "profile_3").get_key();
    auto pk3 = profile_table->create_object().set(col_profile_id, "profile_4").get_key();
    auto pk4 = profile_table->create_object().set(col_profile_id, "profile_5").get_key();

    {
        auto ll0 = service_table->get_object(sk0).get_linklist(col_service_link);
        auto ll1 = service_table->get_object(sk1).get_linklist(col_service_link);
        ll0.add(pk0);
        ll0.add(pk1);
        ll1.add(pk2);
        ll0.add(pk3);
        ll0.add(pk4);
    }

    profile_table->get_object(pk0).set(col_profile_link, sk0);
    profile_table->get_object(pk1).set(col_profile_link, sk0);
    profile_table->get_object(pk2).set(col_profile_link, sk1);
    profile_table->get_object(pk3).set(col_profile_link, sk0);
    profile_table->get_object(pk4).set(col_profile_link, sk0);

    person_table->create_object().set(col_person_id, "person_1").get_linklist(col_person_link).add(sk0);
    person_table->create_object().set(col_person_id, "person_2").get_linklist(col_person_link).add(sk0);
    person_table->create_object().set(col_person_id, "person_3").get_linklist(col_person_link).add(sk1);
    person_table->create_object().set(col_person_id, "person_4").get_linklist(col_person_link).add(sk0);
    person_table->create_object().set(col_person_id, "person_5").get_linklist(col_person_link).add(sk0);

    realm::Query q0 =
        person_table->where()
            .group()

            .group()
            .and_query(person_table->link(col_person_link)
                           .link(col_service_link)
                           .column<String>(col_profile_id)
                           .equal("profile_1"))
            .Or()
            .and_query(person_table->link(col_person_link)
                           .link(col_service_link)
                           .column<String>(col_profile_id)
                           .equal("profile_2"))
            .end_group()

            .group()
            .and_query(person_table->link(col_person_link).column<String>(col_service_id).equal("service_1"))
            .end_group()

            .end_group()

            .Or()

            .group()
            .equal(col_person_id, "person_3")
            .end_group();

    CHECK_EQUAL(5, q0.count());
}

#endif // TEST_QUERY
