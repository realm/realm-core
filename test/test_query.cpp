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
#include <limits>
#include <vector>
#include <chrono>

using namespace std::chrono;

#include <realm.hpp>
#include <realm/column_integer.hpp>
#include <realm/array_bool.hpp>
#include <realm/history.hpp>
#include <realm/query_expression.hpp>
#include <realm/index_string.hpp>
#include <realm/query_expression.hpp>
#include "test.hpp"
#include "test_table_helper.hpp"
#include "test_types_helper.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

// #include <valgrind/callgrind.h>

#ifndef CALLGRIND_START_INSTRUMENTATION
#define CALLGRIND_START_INSTRUMENTATION
#endif

#ifndef CALLGRIND_STOP_INSTRUMENTATION
#define CALLGRIND_STOP_INSTRUMENTATION
#endif

// valgrind --tool=callgrind --instr-atstart=no realm-tests

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

TEST(Query_Parser)
{
    Table books;
    books.add_column(type_String, "title");
    books.add_column(type_String, "author");
    books.add_column(type_Int, "pages");

    Obj obj1 = books.create_object().set_all("Computer Architecture and Organization", "B. Govindarajalu", 752);
    Obj obj2 = books.create_object().set_all("Introduction to Quantum Mechanics", "David Griffiths", 480);
    Obj obj3 = books.create_object().set_all("Biophysics: Searching for Principles", "William Bialek", 640);

    // Typed table:
    Query q = books.query("pages >= $0 && author == $1", {{200, "David Griffiths"}});
    auto match = q.find();
    CHECK_EQUAL(obj2.get_key(), match);
    // You don't need to create a query object first:
    match = books.query("pages >= 200 && author == \"David Griffiths\"").find();
    CHECK_EQUAL(obj2.get_key(), match);
}


TEST(Query_NextGenSyntax)
{
    ObjKey match;

    // Setup untyped table
    Table untyped;
    auto c0 = untyped.add_column(type_Int, "firs1");
    auto c1 = untyped.add_column(type_Float, "second");
    untyped.add_column(type_Double, "third");
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
    match = (false == untyped.column<bool>(c3)).find();
    CHECK_EQUAL(match, k1);

    match = untyped.query("10 + 10.3 > third + 2").find();
    CHECK_EQUAL(match, k0);


    match = (untyped.column<int64_t>(c0) > untyped.column<int64_t>(c0)).find();
    CHECK_EQUAL(match, realm::null_key);

    // Left condition makes first row non-match
    match = untyped.query("second + 1 > 21 && third > 1 + 1").find();
    CHECK_EQUAL(match, k1);

    // Right condition makes first row a non-match
    match = untyped.query("second > 10 && third > 1.5 + 1 * 2").find();
    CHECK_EQUAL(match, k1);

    // Both make first row match
    match = untyped.query("second < 20 && third > 2").find();
    CHECK_EQUAL(match, k0);

    // Both make first row non-match
    match = untyped.query("second > 20 && third > 3.5").find();
    CHECK_EQUAL(match, k1);

    // Left cond match 0, right match 1
    match = untyped.query("second < 20 && third > 3.5").find();
    CHECK_EQUAL(match, realm::null_key);

    // Left match 1, right match 0
    match = untyped.query("second > 20 && third < 3.5").find();
    CHECK_EQUAL(match, realm::null_key);

    // Untyped ||

    // Left match 0
    match = untyped.query("second < 20 || third < 3.5").find();
    CHECK_EQUAL(match, k0);

    // Right match 0
    match = untyped.query("second > 20 || third < 3.5").find();
    CHECK_EQUAL(match, k0);

    // Left match 1

    match = untyped.query("second > 20 || third > 9.5").find();

    CHECK_EQUAL(match, k1);

    Query q4 = untyped.query("second + firs1 > 40");


    Query q5 = untyped.query("20 < second");

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

    Query q8 = untyped.query("1 > second + 5");
    match = q8.find();
    CHECK_EQUAL(match, null_key);

    Query q3 = untyped.query("second + firs1 > 10 + firs1");
    match = q3.find();

    match = q2.find();
    CHECK_EQUAL(match, k0);

    match = untyped.query("firs1 + second > 40").find();
    CHECK_EQUAL(match, k1);

    match = untyped.query("firs1 + second < 40").find();
    CHECK_EQUAL(match, k0);

    match = untyped.query("second <= firs1").find();
    CHECK_EQUAL(match, k0);

    match = untyped.query("firs1 + second >= firs1 + second").find();
    CHECK_EQUAL(match, k0);

    match = untyped.query("firs1 + second > 40").find();
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
    auto col_link1 = table3->add_column(*table2, "link1");

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


        realm::Query q21 = table.query("first * 2 > second / 2 + third + 1");
        tv = q21.find_all();
        tvpos = 0;
        for (Obj o : table) {
            if (o.get<Int>(col_int0) * 2 > o.get<Int>(col_int1) / 2 + o.get<Int>(col_int2) + 1) {
                CHECK_EQUAL(o.get_key(), tv.get_key(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv.size());

        realm::Query q22 = table.query("first * 2 > second / 2 + third + 1 + third - third + third - third + third - "
                                       "third + third - third + third - third");
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
    auto col_int = table.add_column(type_Int, "first");
    auto col_float = table.add_column(type_Float, "second");
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
    match = table.query("second + 0.2 > 20").find();
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
    match = table.query("second + 20 > 40").find();
    CHECK_EQUAL(match, key1);

    // first and 40 must convert to float
    match = table.query("second + first >= 40").find();
    CHECK_EQUAL(match, key1);

    // 20 must convert to float
    match = table.query("0.2f + second > 20").find();
    CHECK_EQUAL(match, key0);

    /**
    Permutations of types (Subexpr, Value, Column) of left/right side
    **/

    // Compare, left = Subexpr, right = Value
    match = table.query("second + first >= 40").find();
    CHECK_EQUAL(match, key1);

    match = table.query("second + first > 40").find();
    CHECK_EQUAL(match, key1);

    match = table.query("first - second < 0").find();
    CHECK_EQUAL(match, key1);

    match = table.query("second - second == 0").find();
    CHECK_EQUAL(match, key0);

    match = table.query("first - second <= 0").find();
    CHECK_EQUAL(match, key1);

    match = table.query("first * first != 400").find();
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
    match = table.query("40 <= second + first").find();
    CHECK_EQUAL(match, key1);

    match = table.query("40 < second + first").find();
    CHECK_EQUAL(match, key1);

    match = table.query("0 > first - second").find();
    CHECK_EQUAL(match, key1);

    match = table.query("0 == second - second").find();
    CHECK_EQUAL(match, key0);

    match = table.query("0 >= first - second").find();
    CHECK_EQUAL(match, key1);

    match = table.query("400 != first * first").find();
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
    match = table.query("second + 0 > first + 0").find();
    CHECK_EQUAL(match, key1);

    match = table.query("second + 0 >= first + 0").find();
    CHECK_EQUAL(match, key1);

    match = table.query("second + 0 == first + 0").find();
    CHECK_EQUAL(match, null_key);

    match = table.query("second + 0 != second + 0").find();
    CHECK_EQUAL(match, null_key);

    match = table.query("first + 0 < second + 0").find();
    CHECK_EQUAL(match, key1);

    match = table.query("first + 0 <= second + 0").find();
    CHECK_EQUAL(match, key1);

    // Conversions, again
    table.clear();
    key0 = table.create_object().set_all(17, 3.0f, 3.0).get_key();

    match = table.query("1 / second == 1 / second").find();
    CHECK_EQUAL(match, key0);

    match = table.query("1 / third == 1 / third").find();
    CHECK_EQUAL(match, key0);

    // Nifty test: Compare operator must preserve precision of each side, hence NO match; if double accidentially
    // was truncated to float, or float was rounded to nearest double, then this test would fail.
    match = table.query("1 / second == 1 / third").find();
    CHECK_EQUAL(match, null_key);

    match = table.query("first / 2 == 8").find();
    CHECK_EQUAL(match, key0);

    // For `float < int_column` we had a bug where int was promoted to float instead of double.
    table.clear();
    auto k = table.create_object().set(col_int, 1000000001).get_key();

    match = (1000000000.f < first).find();
    CHECK_EQUAL(match, k);

    match = (first > 1000000000.f).find();
    CHECK_EQUAL(match, k);
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

TEST(Query_IntIndex)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    Group group;
    TableRef table = group.add_table("test");
    auto col = table->add_column(type_Int, "first", true);
    table->add_search_index(col);

    size_t eights = 0;
    size_t nulls = 0;

    for (int i = 0; i < REALM_MAX_BPNODE_SIZE * 2; ++i) {
        int v = random.draw_int_mod(10);
        if (v == 8) {
            eights++;
        }
        auto obj = table->create_object();
        if (v == 5) {
            nulls++;
        }
        else {
            obj.set(col, v);
        }
    }

    // This will use IntegerNode
    auto q = table->column<Int>(col) == 8;
    auto cnt = q.count();
    CHECK_EQUAL(cnt, eights);

    // Uses Compare expression
    q = table->column<Int>(col) == 8.0;
    cnt = q.count();
    CHECK_EQUAL(cnt, eights);

    TableRef origin = group.add_table("origin");
    auto col_link = origin->add_column(*table, "link");
    for (auto&& o : *table) {
        origin->create_object().set(col_link, o.get_key());
    }
    // Querying over links makes sure we will not use IntegerNode
    q = origin->link(col_link).column<Int>(col) == 8;
    cnt = q.count();
    CHECK_EQUAL(cnt, eights);

    q = origin->link(col_link).column<Int>(col) == realm::null();
    cnt = q.count();
    CHECK_EQUAL(cnt, nulls);
}

TEST(Query_StringIndexNull)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    Group group;
    TableRef table = group.add_table("test");
    auto col = table->add_column(type_String, "first", true);
    table->add_search_index(col);

    size_t nulls = 0;

    for (int i = 0; i < REALM_MAX_BPNODE_SIZE * 2; ++i) {
        int v = random.draw_int_mod(10);
        auto obj = table->create_object();
        if (v == 8) {
            nulls++;
        }
        else {
            obj.set(col, util::to_string(v));
        }
    }

    TableRef origin = group.add_table("origin");
    auto col_link = origin->add_column(*table, "link");
    for (auto&& o : *table) {
        origin->create_object().set(col_link, o.get_key());
    }

    auto q = origin->link(col_link).column<String>(col) == realm::null();
    auto cnt = q.count();
    CHECK_EQUAL(cnt, nulls);
}

TEST(Query_Links)
{
    Group g;

    TableRef origin = g.add_table("origin");
    TableRef target1 = g.add_table("target1");
    TableRef target2 = g.add_table("target2");

    auto int_col = target2->add_column(type_Int, "integers");
    auto str_col = target1->add_column(type_String, "strings");
    auto linklist_col = target1->add_column_list(*target2, "linklist");
    auto link_col = origin->add_column(*target1, "link");
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
    auto linklist_col = table1->add_column_list(*table2, "linklist");

    auto int_col = table2->add_column(type_Int, "integers");

    auto link_col = table3->add_column(*table1, "link");
    auto linklist_col1 = table3->add_column_list(*table1, "linklist");

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
    auto col_link = baa->add_column(*table, "link");
    auto col_linklist = baa->add_column_list(*table, "linklist");
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

TEST(Query_SetOfPrimitives)
{
    Group g;

    TableRef table = g.add_table("foo");

    auto col_int_set = table->add_column_set(type_Int, "integers");
    std::vector<ObjKey> keys;

    table->create_objects(4, keys);

    auto set_values = [](Set<Int> set, const std::vector<Int>& value_list) {
        for (auto val : value_list)
            set.insert(val);
    };

    set_values(table->get_object(keys[0]).get_set<Int>(col_int_set), {0, 1});
    set_values(table->get_object(keys[1]).get_set<Int>(col_int_set), {2, 3, 4, 5});
    set_values(table->get_object(keys[2]).get_set<Int>(col_int_set), {6, 7, 100, 8, 9});
    set_values(table->get_object(keys[3]).get_set<Int>(col_int_set), {3, 11, 7});

    Query q = table->column<Set<Int>>(col_int_set) == 3;
    auto tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_key(0), keys[1]);
    CHECK_EQUAL(tv.get_key(1), keys[3]);

    q = table->column<Set<Int>>(col_int_set).size() == 4;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_key(0), keys[1]);

    q = table->column<Set<Int>>(col_int_set).max() == 100;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_key(0), keys[2]);
}

TEST(Query_SetOfObjects)
{
    Group g;

    TableRef table = g.add_table("foo");
    TableRef table_bar = g.add_table("bar");

    std::vector<ObjKey> bar_keys;
    auto col_string = table_bar->add_column(type_String, "name");
    table_bar->create_objects(3, bar_keys);
    table_bar->get_object(bar_keys[0]).set(col_string, "zero");
    table_bar->get_object(bar_keys[1]).set(col_string, "one");
    table_bar->get_object(bar_keys[2]).set(col_string, "two");

    auto col_obj_set = table->add_column_set(*table_bar, "objects");
    std::vector<ObjKey> keys;

    table->create_objects(4, keys);

    auto set_values = [](Set<ObjKey> set, const std::vector<ObjKey>& value_list) {
        for (auto val : value_list)
            set.insert(val);
    };

    set_values(table->get_object(keys[0]).get_set<ObjKey>(col_obj_set), {bar_keys[0], bar_keys[1]});
    set_values(table->get_object(keys[1]).get_set<ObjKey>(col_obj_set), {bar_keys[2]});
    set_values(table->get_object(keys[2]).get_set<ObjKey>(col_obj_set), {bar_keys[0], bar_keys[1], bar_keys[2]});

    Query q = table->where().links_to(col_obj_set, bar_keys[0]);
    auto tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_key(0), keys[0]);
    CHECK_EQUAL(tv.get_key(1), keys[2]);

    q = table->where().links_to(col_obj_set, {bar_keys[0], bar_keys[2]});
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 3);
    CHECK_EQUAL(tv.get_key(0), keys[0]);
    CHECK_EQUAL(tv.get_key(1), keys[1]);
    CHECK_EQUAL(tv.get_key(2), keys[2]);

    q = table->column<Set<ObjKey>>(col_obj_set).size() == 3;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_key(0), keys[2]);
}

template <typename T>
struct AggregateValues {
    static std::vector<T> values()
    {
        std::vector<T> values = {std::numeric_limits<T>::lowest(), T{-1}, T{0}, T{1}, std::numeric_limits<T>::max()};
        if (std::numeric_limits<T>::has_quiet_NaN) {
            values.push_back(std::numeric_limits<T>::quiet_NaN());
        }
        return values;
    }
    using OptionalT = util::Optional<T>;
    static const constexpr util::None null = util::none;
};

template <>
struct AggregateValues<Decimal128> {
    static std::vector<Decimal128> values()
    {
        return {std::numeric_limits<Decimal128>::lowest(), Decimal128{-1}, Decimal128{0}, Decimal128{1},
                std::numeric_limits<Decimal128>::max()};
    }
    using OptionalT = Decimal128;
    static const constexpr realm::null null = realm::null();
};

template <>
struct AggregateValues<Timestamp> {
    static std::vector<Timestamp> values()
    {
        return {std::numeric_limits<Timestamp>::lowest(), Timestamp{-1, 0}, Timestamp{0, 0}, Timestamp{1, 0},
                std::numeric_limits<Timestamp>::max()};
    }
    using OptionalT = Timestamp;
    static const constexpr realm::null null = realm::null();
};

template <typename T>
ColKey generate_all_combinations(Table& table)
{
    using OptionalT = typename AggregateValues<T>::OptionalT;
    auto values = AggregateValues<T>::values();
    size_t n = values.size() + 1;
    auto col = table.add_column_list(ColumnTypeTraits<T>::id, "col", true);

    // Add a row for each permutation of k=1..n values
    for (size_t k = 1; k <= n; ++k) {
        // Loop over each possible selection of k different values
        std::vector<bool> selector(n);
        std::fill(selector.begin(), selector.begin() + k, true);
        do {
            std::vector<OptionalT> selected_values;
            for (size_t i = 0; i < n; i++) {
                if (selector[i]) {
                    if (i == 0)
                        selected_values.push_back(AggregateValues<T>::null);
                    else
                        selected_values.push_back(values[i - 1]);
                }
            }

            // Loop over each permutation of the selected values
            REALM_ASSERT(std::is_sorted(selected_values.begin(), selected_values.end()));
            do {
                auto list = table.create_object().get_list<OptionalT>(col);
                for (auto value : selected_values)
                    list.add(value);
            } while (std::next_permutation(selected_values.begin(), selected_values.end()));
        } while (std::prev_permutation(selector.begin(), selector.end()));
    }
    return col;
}

template <typename T, typename Value, typename Getter>
void validate_aggregate_results(unit_test::TestContext& test_context, Table& table, ColKey col, Value value,
                                Getter getter)
{
    auto tv = (getter(table.column<Lst<T>>(col)) == value).find_all();
    auto not_tv = (getter(table.column<Lst<T>>(col)) != value).find_all();

    // Verify that all rows are present in one of the TVs and that each row in
    // the TV should have matched the query
    using OptionalT = typename AggregateValues<T>::OptionalT;
    CHECK_EQUAL(tv.size() + not_tv.size(), table.size());
    for (size_t i = 0; i < tv.size(); ++i) {
        auto result = getter(tv.get_object(i).template get_list<OptionalT>(col));
        CHECK(result);
        CHECK_EQUAL(*result, Mixed(value));
    }
    for (size_t i = 0; i < not_tv.size(); ++i) {
        auto result = getter(not_tv.get_object(i).template get_list<OptionalT>(col));
        CHECK(result);
        CHECK_NOT_EQUAL(*result, Mixed(value));
    }
}

TEST_TYPES(Query_ListOfPrimitives_MinMax, int64_t, float, double, Decimal128, Timestamp)
{
    using T = TEST_TYPE;
    auto values = AggregateValues<T>::values();
    Table table;
    auto col = generate_all_combinations<T>(table);

    auto min = [](auto&& list) {
        return list.min();
    };
    validate_aggregate_results<T>(test_context, table, col, null(), min);
    for (auto value : values)
        validate_aggregate_results<T>(test_context, table, col, value, min);

    auto max = [](auto&& list) {
        return list.max();
    };
    validate_aggregate_results<T>(test_context, table, col, null(), max);
    for (auto value : values)
        validate_aggregate_results<T>(test_context, table, col, value, max);
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

    realm::TableView t1 = table.where().equal(col_int0, col_int1).find_all();
    realm::TableView t2 = table.where().equal(col_int2, col_int3).find_all();
    realm::TableView t3 = table.where().equal(col_int4, col_int5).find_all();

    realm::TableView t4 = table.where().equal(col_float6, col_float7).find_all();
    realm::TableView t5 = table.where().equal(col_double8, col_double9).find_all();


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

    CHECK_EQUAL(null_key, table.where().equal(col_int0, col_int1).find());
    CHECK_EQUAL(obj0.get_key(), table.where().not_equal(col_int0, col_int1).find());
    CHECK_EQUAL(obj0.get_key(), table.where().less(col_int0, col_int1).find());
    CHECK_EQUAL(obj1.get_key(), table.where().greater(col_int0, col_int1).find());
    CHECK_EQUAL(obj1.get_key(), table.where().greater_equal(col_int0, col_int1).find());
    CHECK_EQUAL(obj0.get_key(), table.where().less_equal(col_int0, col_int1).find());

    CHECK_EQUAL(null_key, table.where().equal(col_float2, col_float3).find());
    CHECK_EQUAL(obj0.get_key(), table.where().not_equal(col_float2, col_float3).find());
    CHECK_EQUAL(obj0.get_key(), table.where().less(col_float2, col_float3).find());
    CHECK_EQUAL(obj1.get_key(), table.where().greater(col_float2, col_float3).find());
    CHECK_EQUAL(obj1.get_key(), table.where().greater_equal(col_float2, col_float3).find());
    CHECK_EQUAL(obj0.get_key(), table.where().less_equal(col_float2, col_float3).find());

    CHECK_EQUAL(null_key, table.where().equal(col_double4, col_double5).find());
    CHECK_EQUAL(obj0.get_key(), table.where().not_equal(col_double4, col_double5).find());
    CHECK_EQUAL(obj0.get_key(), table.where().less(col_double4, col_double5).find());
    CHECK_EQUAL(obj1.get_key(), table.where().greater(col_double4, col_double5).find());
    CHECK_EQUAL(obj1.get_key(), table.where().greater_equal(col_double4, col_double5).find());
    CHECK_EQUAL(obj0.get_key(), table.where().less_equal(col_double4, col_double5).find());
}


TEST(Query_TwoCols0)
{
    Table table;
    auto col0 = table.add_column(type_Int, "first1");
    auto col1 = table.add_column(type_Int, "second1");


    for (int i = 0; i < 50; i++) {
        table.create_object();
    }

    realm::TableView t1 = table.where().equal(col0, col1).find_all();
    CHECK_EQUAL(50, t1.size());

    realm::TableView t2 = table.where().less(col0, col1).find_all();
    CHECK_EQUAL(0, t2.size());
}


TEST(Query_TwoSameCols)
{
    Group g;
    Table& table = *g.add_table("table");
    auto col_bool0 = table.add_column(type_Bool, "first1");
    auto col_bool1 = table.add_column(type_Bool, "first2");
    auto col_date2 = table.add_column(type_Timestamp, "second1");
    auto col_date3 = table.add_column(type_Timestamp, "second2");
    auto col_str4 = table.add_column(type_String, "third1");
    auto col_str5 = table.add_column(type_String, "third2");
    auto col_obj1 = table.add_column(table, "obj1");
    auto col_obj2 = table.add_column(table, "obj2");

    Timestamp d1(200, 0);
    Timestamp d2(300, 0);
    Obj obj0 = table.create_object();
    ObjKey key0 = obj0.get_key();
    obj0.set_all(false, true, d1, d2, "a", "b", key0);
    ObjKey key1 = table.create_object().set_all(true, true, d2, d2, "b", "b", key0, key0).get_key();
    table.create_object().set_all(false, true, d1, d2, "a", "b", key0, key1).get_key();

    Query q1 = table.column<Bool>(col_bool0) == table.column<Bool>(col_bool1);
    Query q2 = table.column<Timestamp>(col_date2) == table.column<Timestamp>(col_date3);
    Query q3 = table.column<String>(col_str4) == table.column<String>(col_str5);
    Query q4 = table.column<Link>(col_obj1) == table.column<Link>(col_obj2);

    CHECK_EQUAL(key1, q1.find());
    CHECK_EQUAL(key1, q2.find());
    CHECK_EQUAL(key1, q3.find());
    CHECK_EQUAL(key1, q4.find());
    CHECK_EQUAL(1, q1.count());
    CHECK_EQUAL(1, q2.count());
    CHECK_EQUAL(1, q3.count());
    CHECK_EQUAL(1, q4.count());

    Query q5 = table.column<Bool>(col_bool0) != table.column<Bool>(col_bool1);
    Query q6 = table.column<Timestamp>(col_date2) != table.column<Timestamp>(col_date3);
    Query q7 = table.column<String>(col_str4) != table.column<String>(col_str5);
    Query q8 = table.column<Link>(col_obj1) != table.column<Link>(col_obj2);

    CHECK_EQUAL(key0, q5.find());
    CHECK_EQUAL(key0, q6.find());
    CHECK_EQUAL(key0, q7.find());
    CHECK_EQUAL(key0, q8.find());
    CHECK_EQUAL(2, q5.count());
    CHECK_EQUAL(2, q6.count());
    CHECK_EQUAL(2, q7.count());
    CHECK_EQUAL(2, q8.count());
}

void construct_all_types_table(Table& table)
{
    table.add_column(type_Int, "int");
    table.add_column(type_Float, "float");
    table.add_column(type_Double, "double");
    table.add_column(type_Decimal, "decimal128");
    table.add_column(type_Mixed, "mixed");
    table.add_column(type_String, "string");

    table.add_column(type_Int, "int?", true);
    table.add_column(type_Float, "float?", true);
    table.add_column(type_Double, "double?", true);
    table.add_column(type_Decimal, "decimal128?", true);
    table.add_column(type_Mixed, "mixed?", true);
    table.add_column(type_String, "string?", true);
}

TEST(Query_TwoColumnsNumeric)
{
    Table table;
    construct_all_types_table(table);
    TestValueGenerator gen;

    std::vector<int64_t> ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    std::vector<float> floats = gen.values_from_int<float>(ints);
    std::vector<double> doubles = gen.values_from_int<double>(ints);
    std::vector<Decimal128> decimals = gen.values_from_int<Decimal128>(ints);
    std::vector<Mixed> mixeds = gen.values_from_int<Mixed>(ints);
    std::vector<StringData> strings = gen.values_from_int<StringData>(ints);
    size_t num_rows = ints.size();
    for (size_t i = 0; i < num_rows; ++i) {
        table.create_object().set_all(ints[i], floats[i], doubles[i], decimals[i], mixeds[i], strings[i], ints[i],
                                      floats[i], doubles[i], decimals[i], mixeds[i], strings[i]);
    }

    ColKeys columns = table.get_column_keys();
    for (size_t i = 0; i < columns.size(); ++i) {
        for (size_t j = 0; j < columns.size(); ++j) {
            ColKey lhs = columns[i];
            ColKey rhs = columns[j];
            DataType lhs_type = DataType(lhs.get_type());
            DataType rhs_type = DataType(rhs.get_type());
            size_t num_expected_matches = num_rows;
            if ((lhs_type == type_Mixed) != (rhs_type == type_Mixed)) {
                // Only one prop is mixed
                num_expected_matches = 6;
            }
            if ((lhs_type == type_String) != (rhs_type == type_String)) {
                // Only one prop is string
                num_expected_matches = 0;
                if ((lhs_type == type_Mixed) || (rhs_type == type_Mixed)) {
                    num_expected_matches = 2;
                }
            }
            {
                size_t actual_matches = table.where().equal(lhs, rhs).count();
                CHECK_EQUAL(num_expected_matches, actual_matches);
                if (actual_matches != num_expected_matches) {
                    std::cout << "failure comparing columns: " << table.get_column_name(lhs)
                              << " == " << table.get_column_name(rhs) << std::endl;
                }
            }
            // select some typed query expressions to test as well
            if (lhs_type == type_Int && rhs_type == type_Double) {
                size_t actual_matches = (table.column<Int>(lhs) == table.column<Double>(rhs)).count();
                CHECK_EQUAL(num_expected_matches, actual_matches);
            }
            {
                size_t actual_matches = table.where().not_equal(lhs, rhs).count();
                CHECK_EQUAL(num_rows - num_expected_matches, actual_matches);
                if (actual_matches != num_rows - num_expected_matches) {
                    std::cout << "failure comparing columns: " << table.get_column_name(lhs)
                              << " != " << table.get_column_name(rhs) << std::endl;
                }
            }
            {
                size_t actual_matches = table.where().greater_equal(lhs, rhs).count();
                CHECK_EQUAL(num_expected_matches, actual_matches);
                if (actual_matches != num_expected_matches) {
                    std::cout << "failure comparing columns: " << table.get_column_name(lhs)
                              << " >= " << table.get_column_name(rhs) << std::endl;
                }
            }
            {
                size_t actual_matches = table.where().less_equal(lhs, rhs).count();
                CHECK_EQUAL(num_expected_matches, actual_matches);
                if (actual_matches != num_expected_matches) {
                    std::cout << "failure comparing columns: " << table.get_column_name(lhs)
                              << " <= " << table.get_column_name(rhs) << std::endl;
                }
            }
            {
                num_expected_matches = 0;
                size_t actual_matches = table.where().greater(lhs, rhs).count();
                CHECK_EQUAL(num_expected_matches, actual_matches);
                if (actual_matches != num_expected_matches) {
                    std::cout << "failure comparing columns: " << table.get_column_name(lhs) << " > "
                              << table.get_column_name(rhs) << std::endl;
                }
            }
            {
                num_expected_matches = 0;
                size_t actual_matches = table.where().less(lhs, rhs).count();
                CHECK_EQUAL(num_expected_matches, actual_matches);
                if (actual_matches != num_expected_matches) {
                    std::cout << "failure comparing columns: " << table.get_column_name(lhs) << " < "
                              << table.get_column_name(rhs) << std::endl;
                }
            }
        }
    }
}

TEST(Query_TwoColumnsCrossTypesNullability)
{
    Table table;
    construct_all_types_table(table);
    TestValueGenerator gen;

    constexpr size_t num_rows = 1;
    table.create_object(); // add one row of default values, null or zero

    ColKeys columns = table.get_column_keys();
    for (size_t i = 0; i < columns.size(); ++i) {
        for (size_t j = 0; j < columns.size(); ++j) {
            ColKey lhs = columns[i];
            ColKey rhs = columns[j];
            DataType lhs_type = DataType(lhs.get_type());
            DataType rhs_type = DataType(rhs.get_type());
            bool both_non_nullable = !lhs.is_nullable() && !rhs.is_nullable();
            bool are_comparable = Mixed::data_types_are_comparable(lhs_type, rhs_type);
            size_t num_expected_matches = 0;
            if (lhs.is_nullable() && rhs.is_nullable()) {
                num_expected_matches = 1; // both default to null
            }
            else if (!lhs.is_nullable() && !rhs.is_nullable()) {
                if (are_comparable) {
                    num_expected_matches = 1; // numerics are 0
                }
            }
            {
                size_t actual_matches = table.where().equal(lhs, rhs).count();
                CHECK_EQUAL(num_expected_matches, actual_matches);
                if (actual_matches != num_expected_matches) {
                    std::cout << "failure comparing columns: " << table.get_column_name(lhs)
                              << " == " << table.get_column_name(rhs) << std::endl;
                }
            }
            // select some typed query expressions to test as well
            if (lhs_type == type_Int && rhs_type == type_Double) {
                size_t actual_matches = (table.column<Int>(lhs) == table.column<Double>(rhs)).count();
                CHECK_EQUAL(num_expected_matches, actual_matches);
            }
            if (lhs_type == type_String && rhs_type == type_Binary) {
                size_t actual_matches = (table.column<String>(lhs) == table.column<Binary>(rhs)).count();
                CHECK_EQUAL(num_expected_matches, actual_matches);
            }
            {
                size_t actual_matches = table.where().not_equal(lhs, rhs).count();
                CHECK_EQUAL(num_rows - num_expected_matches, actual_matches);
                if (actual_matches != num_rows - num_expected_matches) {
                    std::cout << "failure comparing columns: " << table.get_column_name(lhs)
                              << " != " << table.get_column_name(rhs) << std::endl;
                }
            }
            {
                size_t expected_gte = num_expected_matches;
                if (both_non_nullable && lhs_type == type_String && rhs_type == type_Binary) {
                    expected_gte = num_rows;
                }
                size_t actual_matches = table.where().greater_equal(lhs, rhs).count();
                CHECK_EQUAL(expected_gte, actual_matches);
                if (actual_matches != expected_gte) {
                    std::cout << "failure comparing columns: " << table.get_column_name(lhs)
                              << " >= " << table.get_column_name(rhs) << std::endl;
                }
            }
            {
                size_t expected_le = num_expected_matches;
                if (both_non_nullable && lhs_type == type_Binary && rhs_type == type_String) {
                    expected_le = num_rows;
                }
                size_t actual_matches = table.where().less_equal(lhs, rhs).count();
                CHECK_EQUAL(expected_le, actual_matches);
                if (actual_matches != expected_le) {
                    std::cout << "failure comparing columns: " << table.get_column_name(lhs)
                              << " <= " << table.get_column_name(rhs) << std::endl;
                }
            }
            {
                size_t expected_greater = 0;
                size_t actual_matches = table.where().greater(lhs, rhs).count();
                CHECK_EQUAL(expected_greater, actual_matches);
                if (actual_matches != expected_greater) {
                    std::cout << "failure comparing columns: " << table.get_column_name(lhs) << " > "
                              << table.get_column_name(rhs) << std::endl;
                }
            }
            {
                size_t expected_less = 0;
                size_t actual_matches = table.where().less(lhs, rhs).count();
                CHECK_EQUAL(expected_less, actual_matches);
                if (actual_matches != expected_less) {
                    std::cout << "failure comparing columns: " << table.get_column_name(lhs) << " < "
                              << table.get_column_name(rhs) << std::endl;
                }
            }
        }
    }
}

TEST(Query_TwoColumnsCrossTypesNaN)
{
    // across double/float nullable/non-nullable combinations
    // verify query comparisons for: NaN == NaN, null == null, NaN != null
    Table table;
    table.add_column(type_Float, "float");
    table.add_column(type_Double, "double");
    table.add_column(type_Float, "float?", true);
    table.add_column(type_Double, "double?", true);

    CHECK(std::numeric_limits<double>::has_quiet_NaN);
    CHECK(std::numeric_limits<float>::has_quiet_NaN);
    double nan_d = std::numeric_limits<double>::quiet_NaN();
    float nan_f = std::numeric_limits<float>::quiet_NaN();
    util::Optional<double> null_d;
    util::Optional<float> null_f;
    table.create_object().set_all(nan_f, nan_d, null_f, null_d);
    table.create_object().set_all(nan_f, nan_d, nan_f, nan_d);
    ColKeys columns = table.get_column_keys();
    for (size_t i = 0; i < columns.size(); ++i) {
        for (size_t j = 0; j < columns.size(); ++j) {
            ColKey lhs = columns[i];
            ColKey rhs = columns[j];
            bool same_nullablity = lhs.is_nullable() == rhs.is_nullable();
            size_t num_expected_matches = same_nullablity ? 2 : 1;
            {
                size_t actual_matches = table.where().equal(lhs, rhs).count();
                CHECK_EQUAL(num_expected_matches, actual_matches);
                if (actual_matches != num_expected_matches) {
                    std::cout << "failure comparing columns: " << table.get_column_name(lhs)
                              << " == " << table.get_column_name(rhs) << std::endl;
                }
            }
        }
    }
}

TEST(Query_TwoColumnsDifferentTables)
{
    Group g;
    auto table_a = g.add_table("table a");
    auto table_b = g.add_table("table b");
    ColKey col_a = table_a->add_column(type_Float, "float");
    ColKey col_b = table_b->add_column(type_Float, "float");
    ColKey col_c = table_b->add_column(type_Float, "another float");
    table_a->create_object();
    table_a->create_object();
    table_b->create_object();

    CHECK_THROW_ANY(table_a->where().equal(col_a, col_b).count());
    CHECK_THROW_ANY(table_a->where().equal(col_b, col_c).count());
    CHECK_THROW_ANY((table_a->column<Float>(col_a) == table_b->column<Float>(col_b)).count());
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

    CHECK_EQUAL(null_key, table.where().equal(col0, col1).find());
    CHECK_EQUAL(null_key, table.where().not_equal(col0, col1).find());
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

TEST_IF(Query_StrIndex3, TEST_DURATION > 0)
{
    // Create two columns where query match-density varies alot throughout the rows. This forces the query engine to
    // jump back and forth between the two conditions and test edge cases in these transitions. Tests combinations of
    // linear scan, enum and index

    Random random(random_int<unsigned long>()); // Seed from slow global generator

#if REALM_MAX_BPNODE_SIZE > 256
    constexpr int node_size = 256;
#else
    constexpr int node_size = 4;
#endif

#if defined REALM_DEBUG || REALM_ANDROID
    for (int N = 0; N < 4; N++) {
#else
    for (int N = 0; N < 20; N++) {
#endif
        Table ttt;
        auto col_int = ttt.add_column(type_Int, "1");
        auto col_str = ttt.add_column(type_String, "2");

        std::vector<ObjKey> vec;

        size_t n = 0;
#if defined REALM_DEBUG || REALM_ANDROID
        for (int i = 0; i < 4; i++) {
#else
        for (int i = 0; i < 20; i++) {
#endif
            // 1/128 match probability because we want possibility for a 256 sized leaf to contain 0 matches
            // (important edge case)
            int f1 = random.draw_int_mod(node_size) / 2 + 1;
            int f2 = random.draw_int_mod(node_size) / 2 + 1;
            bool longstrings = random.chance(1, 5);

            // 576 entries with that probability to fill out two concecutive 256 sized leaves with above
            // probability, plus a remainder (edge case)
            for (int j = 0; j < node_size * 2 + node_size / 4; j++) {
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

TEST(Query_StrIndexUpdating)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history());
    auto sg = DB::create(*hist, path, DBOptions(crypt_key()));
    auto group = sg->start_write();

    auto t = group->add_table("table");
    auto col = t->add_column(type_String, "value");
    t->add_search_index(col);
    TableView tv = t->where().equal(col, "").find_all();
    TableView tv_ins = t->where().equal(col, "", false).find_all();
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv_ins.size(), 0);
    group->commit_and_continue_as_read();

    // Queries on indexes have different codepaths for 0, 1, and multiple results,
    // so check each of the 6 possible transitions. The write transactions are
    // required here because otherwise the Query will be using the StringIndex
    // being mutated and will happen to give correct results even if it fails
    // to update all of its internal state.

    // 0 -> 1 result
    group->promote_to_write();
    t->create_object();
    group->commit_and_continue_as_read();
    tv.sync_if_needed();
    tv_ins.sync_if_needed();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv_ins.size(), 1);

    // 1 -> multiple results
    group->promote_to_write();
    t->create_object();
    t->create_object();
    group->commit_and_continue_as_read();
    tv.sync_if_needed();
    tv_ins.sync_if_needed();
    CHECK_EQUAL(tv.size(), 3);
    CHECK_EQUAL(tv_ins.size(), 3);

    // multiple -> 1
    group->promote_to_write();
    t->remove_object(tv.get_key(0));
    t->remove_object(tv.get_key(1));
    group->commit_and_continue_as_read();
    tv.sync_if_needed();
    tv_ins.sync_if_needed();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv_ins.size(), 1);

    // 1 -> 0
    group->promote_to_write();
    t->remove_object(tv.get_key(0));
    group->commit_and_continue_as_read();
    tv.sync_if_needed();
    tv_ins.sync_if_needed();
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv_ins.size(), 0);

    // 0 -> multiple
    group->promote_to_write();
    t->create_object();
    t->create_object();
    group->commit_and_continue_as_read();
    tv.sync_if_needed();
    tv_ins.sync_if_needed();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv_ins.size(), 2);

    // multiple -> 0
    group->promote_to_write();
    t->remove_object(tv.get_key(0));
    t->remove_object(tv.get_key(1));
    group->commit_and_continue_as_read();
    tv.sync_if_needed();
    tv_ins.sync_if_needed();
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv_ins.size(), 0);
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
    auto col_float = t.add_column(type_Float, "1", true);
    auto col_double = t.add_column(type_Double, "2");
    auto col_int = t.add_column(type_Int, "3");

    t.create_object().set_all(float(1.1), double(2.1), 1);
    t.create_object().set_all(float(1.2), double(2.2), 2);
    t.create_object().set_all(float(1.3), double(2.3), 3);
    t.create_object().set_all(float(1.4), double(2.4), 4); // match
    t.create_object().set_all(float(1.5), double(2.5), 5); // match
    t.create_object().set_all(float(1.6), double(2.6), 6); // match
    t.create_object().set_all(float(1.7), double(2.7), 7);
    t.create_object().set_all(nanf("7"), double(2.8), 8);
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

    q8 = t.where().greater(col_int, 3);
    float f = float(q8.sum_float(col_float));
    CHECK_EQUAL(8.1f, f);
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
    t.create_object(ObjKey(0xc001ede1b0)).set_all(float(1.6), double(2.6), 6); // match
    t.create_object().set_all(float(1.7), double(2.7), 7);
    t.create_object().set_all(float(1.8), double(2.8), 8);
    t.create_object().set_all(float(1.9), double(2.9), 9);

    TableView v = t.where().find_all();

    Query q1 = t.where(&v).greater(col_float, 1.35f).less(col_double, 2.65);
    int64_t a1 = q1.sum_int(col_int);
    CHECK_EQUAL(15, a1);

    ObjKey k;
    a1 = q1.maximum_int(col_int, &k);
    CHECK_EQUAL(k.value, 0xc001ede1b0);
    CHECK_EQUAL(a1, 6);

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
    Query q1 = ttt.where().contains(col_str, StringData("foO"), false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(6, tv1.size());
    CHECK_EQUAL(0, tv1.get(0).get<Int>(col_int));
    CHECK_EQUAL(1, tv1.get(1).get<Int>(col_int));
    CHECK_EQUAL(2, tv1.get(2).get<Int>(col_int));
    CHECK_EQUAL(3, tv1.get(3).get<Int>(col_int));
    CHECK_EQUAL(4, tv1.get(4).get<Int>(col_int));
    CHECK_EQUAL(5, tv1.get(5).get<Int>(col_int));
    Query q2 = ttt.where().contains(col_str, StringData("foO"), true);
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

        CHECK_EQUAL(a.size(), tv1.size());
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
    auto t1_link_t2_col = t1->add_column(*t2, "t1_link_to_t2");
    t2->add_column(type_Int, "t2_int");
    t2->add_column(type_String, "t2_string");
    t2->add_column(*t1, "t2_link_to_t1");

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
    auto t1_link_col = t1->add_column(*t2, "t1_link_t2");

    auto t2_int_col = t2->add_column(type_Int, "t2_int");
    auto t2_link_col = t2->add_column(*t3, "t2_link_t3");

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
    tv.sort(SortDescriptor({{t1_int_col}}, {true}));
    CHECK_EQUAL(tv.size(), results1.size());
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results1[i]);
    }
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor({{t1_int_col}}, {false}));
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results1[results1.size() - 1 - i]);
    }

    // Test basic one link chain
    std::vector<size_t> results2 = {3, 4, 2, 1, 5, 0};
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor({{t1_link_col, t2_int_col}}, {true}));
    CHECK_EQUAL(tv.size(), results2.size());
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results2[i]);
    }
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor({{t1_link_col, t2_int_col}}, {false}));
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results2[results2.size() - 1 - i]);
    }

    // Test link chain through two links with nulls
    std::vector<size_t> results3 = {1, 0, 2, 5};
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor({{t1_link_col, t2_link_col, t3_int_col}}, {true}));
    // No guarantees about nullified links except they are at the end.
    CHECK(tv.size() >= results3.size());
    util::Optional<int64_t> last;
    for (size_t i = 0; i < results3.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results3[i]);
        util::Optional<int64_t> current = tv[i]
                                              .get_linked_object(t1_link_col)
                                              .get_linked_object(t2_link_col)
                                              .get<util::Optional<int64_t>>(t3_int_col);
        if (last) {
            CHECK(current);
            CHECK(current.value() >= last.value());
        }
        if (current) {
            last = current.value();
        }
    }
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor({{t1_link_col, t2_link_col, t3_int_col}}, {false}));
    // No guarantees about nullified links except they are at the beginning.
    size_t num_nulls = tv.size() - results3.size();
    for (size_t i = num_nulls; i < results3.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results3[results2.size() - 1 - i]);
    }

    // Test link chain with nulls and a single local column
    std::vector<size_t> results4 = {1, 0, 2, 5, 3, 4};
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor({{t1_link_col, t2_link_col, t3_int_col}, {t1_int_col}}));
    CHECK_EQUAL(tv.size(), results4.size());
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results4[i]);
    }
    std::vector<size_t> results4_rev = {1, 0, 2, 5, 4, 3};
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor({{t1_link_col, t2_link_col, t3_int_col}, {t1_int_col}}, {true, false}));
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results4_rev[i]);
    }
    std::vector<size_t> results4_rev2 = {3, 4, 5, 2, 0, 1};
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor({{t1_link_col, t2_link_col, t3_int_col}, {t1_int_col}}, {false, true}));
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results4_rev2[i]);
    }
    std::vector<size_t> results4_rev3 = {4, 3, 5, 2, 0, 1};
    tv = t1->where().less(t1_int_col, 6).find_all();
    tv.sort(SortDescriptor({{t1_link_col, t2_link_col, t3_int_col}, {t1_int_col}}, {false, false}));
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
    auto t1_linklist_col = t1->add_column_list(*t2, "t1_linklist");
    auto t2_string_col = t2->add_column(type_String, "t2_string");
    t2->add_column(*t1, "t2_link_t1"); // add a backlink to t1

    t1->create_object();

    // Disallow invalid column ids, linklists, other non-link column types.
    ColKey backlink_ndx(ColKey::Idx{2}, col_type_Link, ColumnAttrMask{}, 0);
    CHECK_LOGIC_ERROR(t1->get_sorted_view(SortDescriptor({{t1_linklist_col, t2_string_col}})),
                      LogicError::type_mismatch);
    CHECK_LOGIC_ERROR(t1->get_sorted_view(SortDescriptor({{backlink_ndx, t2_string_col}})),
                      LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(t1->get_sorted_view(SortDescriptor({{t1_int_col, t2_string_col}})), LogicError::type_mismatch);
    CHECK_LOGIC_ERROR(t1->get_sorted_view(SortDescriptor({{t1_linklist_col}})), LogicError::type_mismatch);
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
        tv.sort(SortDescriptor());
        for (size_t i = 0; i < results.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i]);
        }
    }
    {   // Distinct with an empty descriptor is a no-op
        TableView tv = t1->where().find_all();
        tv.distinct(DistinctDescriptor());
        for (size_t i = 0; i < results.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i]);
        }
    }
    {   // Empty sort, empty distinct is still a no-op
        TableView tv = t1->where().find_all();
        tv.sort(SortDescriptor());
        tv.distinct(DistinctDescriptor());
        for (size_t i = 0; i < results.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i]);
        }
    }
    {   // Arbitrary compounded empty sort and distinct is still a no-op
        TableView tv = t1->where().find_all();
        tv.sort(SortDescriptor());
        tv.sort(SortDescriptor());
        tv.distinct(DistinctDescriptor());
        tv.sort(SortDescriptor());
        tv.distinct(DistinctDescriptor());
        tv.distinct(DistinctDescriptor());
        tv.distinct(DistinctDescriptor());
        for (size_t i = 0; i < results.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i]);
        }
    }
    {   // Empty distinct compounded on a valid distinct is a no-op
        TableView tv = t1->where().find_all();
        tv.distinct(DistinctDescriptor());
        tv.distinct(DistinctDescriptor({{t1_int_col}}));
        tv.distinct(DistinctDescriptor());
        results = {4, 3, 2};
        for (size_t i = 0; i < results.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i]);
        }
    }
    {   // Empty sort compounded on a valid sort is a no-op
        TableView tv = t1->where().find_all();
        tv.sort(SortDescriptor());
        tv.sort(SortDescriptor({{t1_int_col}}));
        tv.sort(SortDescriptor());
        results = {2, 3, 3, 4};
        for (size_t i = 0; i < results.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i]);
        }
    }
}

TEST(Query_AllowEmptyDescriptors)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    t1->add_column(type_Int, "t1_int");
    t1->add_column(type_String, "t1_str");
    t1->create_object();

    DescriptorOrdering ordering;

    CHECK(!ordering.will_apply_sort());
    CHECK(!ordering.will_apply_distinct());
    CHECK(!ordering.will_apply_limit());
    CHECK(!ordering.will_limit_to_zero());
    CHECK_EQUAL(ordering.size(), 0);

    ordering.append_sort(SortDescriptor());
    ordering.append_distinct(DistinctDescriptor());
    CHECK(!ordering.will_apply_sort());
    CHECK(!ordering.will_apply_distinct());
    CHECK(!ordering.will_apply_limit());
    CHECK(!ordering.will_limit_to_zero());
    CHECK_EQUAL(ordering.size(), 0);
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
    CHECK(!ordering.will_apply_limit());
    CHECK(!ordering.will_limit_to_zero());
    CHECK_EQUAL(ordering.size(), 0);

    ordering.append_sort(SortDescriptor());
    CHECK(!ordering.will_apply_sort());
    CHECK(!ordering.will_apply_distinct());
    CHECK(!ordering.will_apply_limit());
    CHECK(!ordering.will_limit_to_zero());
    CHECK_EQUAL(ordering.size(), 0);

    ordering.append_distinct(DistinctDescriptor());
    CHECK(!ordering.will_apply_sort());
    CHECK(!ordering.will_apply_distinct());
    CHECK(!ordering.will_apply_limit());
    CHECK(!ordering.will_limit_to_zero());
    CHECK_EQUAL(ordering.size(), 0);

    ordering.append_limit(LimitDescriptor());
    CHECK(!ordering.will_apply_sort());
    CHECK(!ordering.will_apply_distinct());
    CHECK(!ordering.will_apply_limit());
    CHECK(!ordering.will_limit_to_zero());
    CHECK_EQUAL(ordering.size(), 0);

    ordering.append_sort(SortDescriptor({{t1_int_col}}));
    CHECK(ordering.will_apply_sort());
    CHECK(!ordering.will_apply_distinct());
    CHECK(!ordering.will_apply_limit());
    CHECK(!ordering.will_limit_to_zero());

    ordering.append_distinct(DistinctDescriptor({{t1_int_col}}));
    CHECK(ordering.will_apply_sort());
    CHECK(ordering.will_apply_distinct());
    CHECK(!ordering.will_apply_limit());
    CHECK(!ordering.will_limit_to_zero());

    ordering.append_distinct(DistinctDescriptor({{t1_str_col}}));
    CHECK(ordering.will_apply_sort());
    CHECK(ordering.will_apply_distinct());
    CHECK(!ordering.will_apply_limit());
    CHECK(!ordering.will_limit_to_zero());

    ordering.append_sort(SortDescriptor({{t1_str_col}}));
    CHECK(ordering.will_apply_sort());
    CHECK(ordering.will_apply_distinct());
    CHECK(!ordering.will_apply_limit());
    CHECK(!ordering.will_limit_to_zero());

    ordering.append_limit(LimitDescriptor(1));
    CHECK(ordering.will_apply_sort());
    CHECK(ordering.will_apply_distinct());
    CHECK(ordering.will_apply_limit());
    CHECK(!ordering.will_limit_to_zero());

    CHECK_EQUAL(ordering.size(), 5);
    CHECK(ordering.get_type(0) == DescriptorType::Sort);
    CHECK(ordering.get_type(1) == DescriptorType::Distinct);
    CHECK(ordering.get_type(2) == DescriptorType::Distinct);
    CHECK(ordering.get_type(3) == DescriptorType::Sort);
    CHECK(ordering.get_type(4) == DescriptorType::Limit);

    DescriptorOrdering ordering_copy = ordering;
    CHECK(ordering.will_apply_sort());
    CHECK(ordering.will_apply_distinct());
    CHECK(ordering.will_apply_limit());
    CHECK(!ordering.will_limit_to_zero());
    CHECK(ordering_copy.will_apply_sort());
    CHECK(ordering_copy.will_apply_distinct());
    CHECK(ordering_copy.will_apply_limit());
    CHECK(!ordering_copy.will_limit_to_zero());

    ordering_copy.append_limit({10});
    ordering_copy.append_limit({0});
    CHECK(ordering_copy.will_limit_to_zero());
}

TEST(Query_FindWithDescriptorOrdering)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    auto t1_int_col = t1->add_column(type_Int, "t1_int");
    auto t1_str_col = t1->add_column(type_String, "t1_str");

    auto k0 = t1->create_object().set_all(1, "A").get_key();
    auto k1 = t1->create_object().set_all(1, "A").get_key();
    auto k2 = t1->create_object().set_all(1, "B").get_key();
    auto k3 = t1->create_object().set_all(2, "B").get_key();
    auto k4 = t1->create_object().set_all(2, "A").get_key();
    auto k5 = t1->create_object().set_all(2, "A").get_key();

    //     T1
    //   | t1_int   t1_str  |
    //   ====================
    // 0 | 1        "A"     |
    // 1 | 1        "A"     |
    // 2 | 1        "B"     |
    // 3 | 2        "B"     |
    // 4 | 2        "A"     |
    // 5 | 2        "A"     |

    using ResultList = std::vector<std::pair<int64_t, ObjKey>>; // value, key
    {
        // applying only limit
        DescriptorOrdering ordering;
        TableView tv = t1->where().find_all(ordering);
        CHECK_EQUAL(tv.size(), 6);
        CHECK_EQUAL(t1->where().count(ordering), 6);
        ordering.append_limit({2});
        ResultList expected = {{1, k0}, {1, k1}};
        tv = t1->where().find_all(ordering);
        CHECK_EQUAL(tv.size(), expected.size());
        CHECK_EQUAL(t1->where().count(ordering), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
        ordering.append_limit({1}); // two limits should apply the minimum limit
        expected = {{1, k0}};
        tv = t1->where().find_all(ordering);
        CHECK_EQUAL(tv.size(), expected.size());
        CHECK_EQUAL(t1->where().count(ordering), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    {
        // applying sort and sort with prepend
        DescriptorOrdering ordering;
        ordering.append_sort(SortDescriptor({{t1_str_col}}, {true}), SortDescriptor::MergeMode::prepend);
        ordering.append_sort(SortDescriptor({{t1_int_col}}, {true}), SortDescriptor::MergeMode::prepend);
        TableView tv = t1->where().find_all(ordering);
        ResultList expected = {{1, k0}, {1, k1}, {1, k2}, {2, k4}, {2, k5}, {2, k3}};
        CHECK_EQUAL(tv.size(), expected.size());
        CHECK_EQUAL(t1->where().count(ordering), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    {
        // applying sort and sort with append
        DescriptorOrdering ordering;
        ordering.append_sort(SortDescriptor({{t1_str_col}}, {true}), SortDescriptor::MergeMode::append);
        ordering.append_sort(SortDescriptor({{t1_int_col}}, {true}), SortDescriptor::MergeMode::append);
        TableView tv = t1->where().find_all(ordering);
        ResultList expected = {{1, k0}, {1, k1}, {2, k4}, {2, k5}, {1, k2}, {2, k3}};
        CHECK_EQUAL(tv.size(), expected.size());
        CHECK_EQUAL(t1->where().count(ordering), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    {
        // applying string sort, then a limit, and then a descending integer sort, then replace the last sort with
        // an ascending integer sort - the end result should reflect the limit and the first and last sort descriptors
        DescriptorOrdering ordering;
        ordering.append_sort(SortDescriptor({{t1_str_col}}, {false}));
        ordering.append_limit(LimitDescriptor(4));
        ordering.append_sort(SortDescriptor({{t1_int_col}}, {false}));
        ordering.append_sort(SortDescriptor({{t1_int_col}}, {true}), SortDescriptor::MergeMode::replace);
        TableView tv = t1->where().find_all(ordering);
        ResultList expected = {{1, k2}, {1, k0}, {1, k1}, {2, k3}};
        CHECK_EQUAL(tv.size(), expected.size());
        CHECK_EQUAL(t1->where().count(ordering), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    {
        // applying sort and limit
        DescriptorOrdering ordering;
        ordering.append_sort(SortDescriptor({{t1_str_col}}, {false}));
        ordering.append_limit({2});
        TableView tv = t1->where().find_all(ordering);
        ResultList expected = {{1, k2}, {2, k3}};
        CHECK_EQUAL(tv.size(), expected.size());
        CHECK_EQUAL(t1->where().count(ordering), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    { // sort limit distinct
        DescriptorOrdering ordering;
        ordering.append_sort(SortDescriptor({{t1_str_col}}, {false}));
        ordering.append_limit({3});
        ordering.append_distinct(DistinctDescriptor({{t1_int_col}}));
        TableView tv = t1->where().find_all(ordering);
        ResultList expected = {{1, k2}, {2, k3}};
        CHECK_EQUAL(tv.size(), expected.size());
        CHECK_EQUAL(t1->where().count(ordering), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    { // sort distinct limit
        DescriptorOrdering ordering;
        ordering.append_sort(SortDescriptor({{t1_str_col}}, {false}));
        ordering.append_distinct(DistinctDescriptor({{t1_int_col}}));
        ordering.append_limit({1});
        TableView tv = t1->where().find_all(ordering);
        ResultList expected = {{1, k2}};
        CHECK_EQUAL(tv.size(), expected.size());
        CHECK_EQUAL(t1->where().count(ordering), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    { // limit sort distinct
        DescriptorOrdering ordering;
        ordering.append_limit({2});
        ordering.append_sort(SortDescriptor({{t1_str_col}}, {false}));
        ordering.append_distinct(DistinctDescriptor({{t1_int_col}}));
        TableView tv = t1->where().find_all(ordering);
        ResultList expected = {{1, k0}};
        CHECK_EQUAL(tv.size(), expected.size());
        CHECK_EQUAL(t1->where().count(ordering), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    { // sort limit sort limit
        DescriptorOrdering ordering;
        ordering.append_sort(SortDescriptor({{t1_str_col}}, {true}));
        ordering.append_limit({5});
        ordering.append_sort(SortDescriptor({{t1_int_col}}, {false}));
        ordering.append_limit({3});
        TableView tv = t1->where().find_all(ordering);
        ResultList expected = {{2, k4}, {2, k5}, {1, k0}};
        CHECK_EQUAL(tv.size(), expected.size());
        CHECK_EQUAL(t1->where().count(ordering), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
}


TEST(Query_FindWithDescriptorOrderingOverTableviewSync)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    auto t1_int_col = t1->add_column(type_Int, "t1_int");
    auto t1_str_col = t1->add_column(type_String, "t1_str");

    auto init_table = [&]() {
        t1->clear();
        t1->create_object().set_all(0, "A");
        t1->create_object().set_all(1, "A");
        t1->create_object().set_all(2, "B");
        t1->create_object().set_all(3, "B");
        t1->create_object().set_all(4, "A");
        t1->create_object().set_all(5, "A");
    };

    //     T1
    //   | t1_int   t1_str  |
    //   ====================
    // 0 | 0        "A"     |
    // 1 | 1        "A"     |
    // 2 | 2        "B"     |
    // 3 | 3        "B"     |
    // 4 | 4        "A"     |
    // 5 | 5        "A"     |

    using ResultList = std::vector<std::pair<size_t, std::string>>; // t1_int, t1_str
    {
        // applying only limit
        init_table();
        DescriptorOrdering ordering;
        Query base = t1->where().greater(t1_int_col, 2);
        TableView tv = base.find_all(ordering);
        CHECK_EQUAL(tv.size(), 3);
        CHECK_EQUAL(base.count(ordering), 3);
        ordering.append_limit({2});
        ResultList expected = {{3, "B"}, {4, "A"}};
        tv = base.find_all(ordering);
        CHECK_EQUAL(tv.size(), expected.size());
        CHECK_EQUAL(base.count(ordering), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv[i].get<String>(t1_str_col), expected[i].second);
        }
        t1->create_object().set_all(6, "C");
        t1->get_object(4).remove();
        expected = {{3, "B"}, {5, "A"}};
        CHECK(!tv.is_in_sync());
        tv.sync_if_needed();
        CHECK_EQUAL(tv.size(), expected.size());
        CHECK_EQUAL(base.count(ordering), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv[i].get<String>(t1_str_col), expected[i].second);
        }
    }
    { // applying sort and limit
        init_table();
        DescriptorOrdering ordering;
        Query base = t1->where().greater(t1_int_col, 2);
        TableView tv = base.find_all(ordering);
        CHECK_EQUAL(tv.size(), 3);
        CHECK_EQUAL(base.count(ordering), 3);
        ordering.append_sort(SortDescriptor({{t1_str_col}}, {true}));
        ordering.append_limit({2});
        ResultList expected = {{4, "A"}, {5, "A"}};
        tv = base.find_all(ordering);
        CHECK_EQUAL(tv.size(), expected.size());
        CHECK_EQUAL(base.count(ordering), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv[i].get<String>(t1_str_col), expected[i].second);
        }
        t1->create_object().set_all(6, "C");
        t1->get_object(4).remove();
        expected = {{5, "A"}, {3, "B"}};
        CHECK(!tv.is_in_sync());
        tv.sync_if_needed();
        CHECK_EQUAL(tv.size(), expected.size());
        CHECK_EQUAL(base.count(ordering), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv[i].get<String>(t1_str_col), expected[i].second);
        }
    }
}

TEST(Query_DistinctAndSort)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    TableRef t2 = g.add_table("t2");
    auto t1_int_col = t1->add_column(type_Int, "t1_int");
    auto t1_str_col = t1->add_column(type_String, "t1_str");
    auto t1_link_col = t1->add_column(*t2, "t1_link_t2");
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
        tv.sort(SortDescriptor({{t1_str_col}, {t1_int_col}}));
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
        tv.sort(SortDescriptor({{t1_str_col}, {t1_int_col}}, {false /* descending */, false /* descending */}));
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
        tv.distinct(DistinctDescriptor({{t1_link_col, t2_int_col}}));
        CHECK_EQUAL(tv.size(), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
    {   // distinct on a view sorted across links retains sorted order
        TableView tv = t1->where().find_all();
        ResultList expected = {{1, t1_keys[0]}, {2, t1_keys[3]}};
        tv.sort(SortDescriptor({{t1_link_col, t2_int_col}}));
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
        tv.sort(SortDescriptor({{t1_link_col, t2_int_col}}));
        tv.distinct(DistinctDescriptor({{t1_link_col, t2_int_col}}));
        CHECK_EQUAL(tv.size(), expected.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), expected[i].first);
            CHECK_EQUAL(tv.get_key(i), expected[i].second);
        }
    }
}

TEST(Query_SortDistinctOrderThroughHandover)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history());
    DBRef sg_w = DB::create(*hist_w, path, DBOptions(crypt_key()));
    auto g = sg_w->start_write();

    TableRef t1 = g->add_table("t1");
    auto t1_int_col = t1->add_column(type_Int, "t1_int");
    auto t1_str_col = t1->add_column(type_String, "t1_str");

    ObjKey k0 = t1->create_object().set_all(100, "A").get_key();
    ObjKey k1 = t1->create_object().set_all(200, "A").get_key();
    ObjKey k2 = t1->create_object().set_all(300, "A").get_key();
    t1->create_object().set_all(300, "A");
    ObjKey k4 = t1->create_object().set_all(400, "A").get_key();

    g->commit_and_continue_as_read();
    using ResultList = std::vector<std::pair<std::string, ObjKey>>;

    auto check_across_handover = [&](ResultList results, std::unique_ptr<TableView> tv) {
        tv->sync_if_needed();
        CHECK(tv->is_in_sync());
        CHECK_EQUAL(tv->size(), results.size());
        for (size_t i = 0; i < tv->size(); ++i) {
            CHECK_EQUAL(tv->get(i).get<String>(t1_str_col), results[i].first);
            CHECK_EQUAL(tv->get_key(i), results[i].second);
        }
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
        tv.sort(SortDescriptor({{t1_int_col}}, {false}));
        tv.distinct(DistinctDescriptor({{t1_str_col}}));

        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get(i).get<String>(t1_str_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        auto tr = g->duplicate();
        auto tv2 = tr->import_copy_of(tv, PayloadPolicy::Stay);
        check_across_handover(results, std::move(tv2));
    }
    { // sort descending then distinct then limit
        TableView tv = t1->where().find_all();
        ResultList results = {};
        tv.sort(SortDescriptor({{t1_int_col}}, {false}));
        tv.distinct(DistinctDescriptor({{t1_str_col}}));
        tv.limit(LimitDescriptor(0));
        CHECK_EQUAL(tv.size(), results.size());
        auto tr = g->duplicate();
        auto tv2 = tr->import_copy_of(tv, PayloadPolicy::Stay);
        check_across_handover(results, std::move(tv2));
    }
    { // sort descending then distinct then limit and include
        TableView tv = t1->where().find_all();
        ResultList results = {};
        tv.sort(SortDescriptor({{t1_int_col}}, {false}));
        tv.distinct(DistinctDescriptor({{t1_str_col}}));
        tv.limit(LimitDescriptor(0));
        CHECK_EQUAL(tv.size(), results.size());
        auto tr = g->duplicate();
        auto tv2 = tr->import_copy_of(tv, PayloadPolicy::Stay);
        check_across_handover(results, std::move(tv2));
    }
    {   // distinct then sort descending
        TableView tv = t1->where().find_all();
        std::vector<std::pair<std::string, ObjKey>> results = {{"A", k0}};
        tv.distinct(DistinctDescriptor({{t1_str_col}}));
        tv.sort(SortDescriptor({{t1_int_col}}, {false}));
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get(i).get<String>(t1_str_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        auto tr = g->duplicate();
        auto tv2 = tr->import_copy_of(tv, PayloadPolicy::Stay);
        check_across_handover(results, std::move(tv2));
    }
    {   // sort descending then multicolumn distinct
        TableView tv = t1->where().find_all();
        std::vector<std::pair<std::string, ObjKey>> results = {{"A", k4}, {"A", k2}, {"A", k1}, {"A", k0}};
        tv.sort(SortDescriptor({{t1_int_col}}, {false}));
        tv.distinct(DistinctDescriptor({{t1_str_col}, {t1_int_col}}));
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get(i).get<String>(t1_str_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        auto tr = g->duplicate();
        auto tv2 = tr->import_copy_of(tv, PayloadPolicy::Stay);
        check_across_handover(results, std::move(tv2));
    }
    {   // multicolumn distinct then sort descending
        TableView tv = t1->where().find_all();
        std::vector<std::pair<std::string, ObjKey>> results = {{"A", k4}, {"A", k2}, {"A", k1}, {"A", k0}};
        tv.distinct(DistinctDescriptor({{t1_str_col}, {t1_int_col}}));
        tv.sort(SortDescriptor({{t1_int_col}}, {false}));
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get(i).get<String>(t1_str_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        auto tr = g->duplicate();
        auto tv2 = tr->import_copy_of(tv, PayloadPolicy::Stay);
        check_across_handover(results, std::move(tv2));
    }
}

TEST(Query_CompoundDescriptors) {
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history());
    DBRef sg_w = DB::create(*hist_w, path, DBOptions(crypt_key()));
    auto g = sg_w->start_write();

    TableRef t1 = g->add_table("t1");
    ColKey t1_int_col = t1->add_column(type_Int, "t1_int");
    ColKey t1_str_col = t1->add_column(type_String, "t1_str");

    ObjKey k0 = t1->create_object().set_all(1, "A").get_key();
    ObjKey k1 = t1->create_object().set_all(1, "A").get_key();
    ObjKey k2 = t1->create_object().set_all(1, "B").get_key();
    ObjKey k3 = t1->create_object().set_all(2, "B").get_key();
    ObjKey k4 = t1->create_object().set_all(2, "A").get_key();
    ObjKey k5 = t1->create_object().set_all(2, "A").get_key();

    g->commit_and_continue_as_read();
    using ResultList = std::vector<std::pair<size_t, ObjKey>>;

    auto check_across_handover = [&](ResultList results, std::unique_ptr<TableView> tv) {
        tv->sync_if_needed();
        CHECK(tv->is_in_sync());
        CHECK_EQUAL(tv->size(), results.size());
        for (size_t i = 0; i < tv->size(); ++i) {
            CHECK_EQUAL(tv->get(i).get<Int>(t1_int_col), results[i].first);
            CHECK_EQUAL(tv->get_key(i), results[i].second);
        }
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
        tv.sort(SortDescriptor({{t1_int_col}}, {false}));
        tv.sort(SortDescriptor({{t1_str_col}}, {false}));
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        auto tr = g->duplicate();
        auto tv2 = tr->import_copy_of(tv, PayloadPolicy::Stay);
        check_across_handover(results, std::move(tv2));

        tv = t1->where().find_all();
        tv.sort(SortDescriptor({{t1_str_col}, {t1_int_col}}, {false, false}));
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        auto hp = tr->import_copy_of(tv, PayloadPolicy::Stay);
        check_across_handover(results, std::move(hp));
    }

    {   // two distincts are not the same as a single distinct with both criteria
        ResultList results = {{1, k0}, {2, k3}};
        TableView tv = t1->where().find_all();
        tv.distinct(DistinctDescriptor({{t1_int_col}}));
        tv.distinct(DistinctDescriptor({{t1_str_col}}));
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        auto tr = g->duplicate();
        auto tv2 = tr->import_copy_of(tv, PayloadPolicy::Stay);
        check_across_handover(results, std::move(tv2));

        results = {{1, k0}, {1, k2}, {2, k3}, {2, k4}};
        tv = t1->where().find_all();
        tv.distinct(DistinctDescriptor({{t1_int_col}, {t1_str_col}}));
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        auto hp = tr->import_copy_of(tv, PayloadPolicy::Stay);
        check_across_handover(results, std::move(hp));
    }

    {   // check results of sort-distinct-sort-distinct
        TableView tv = t1->where().find_all();
        tv.sort(SortDescriptor({{t1_str_col}, {t1_int_col}}, {true, true}));
        tv.distinct(DistinctDescriptor({{t1_int_col}}));
        ResultList results = {{1, k0}, {2, k4}};
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        auto tr = g->duplicate();
        auto tv2 = tr->import_copy_of(tv, PayloadPolicy::Stay);
        check_across_handover(results, std::move(tv2));

        tv.sort(SortDescriptor({{t1_int_col}}, {false})); // = {{2, 4}, {1, 0}}
        tv.distinct(DistinctDescriptor({{t1_str_col}}));  // = {{2, 4}}
        results = {{2, k4}};
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i].first);
            CHECK_EQUAL(tv.get_key(i), results[i].second);
        }
        auto hp = tr->import_copy_of(tv, PayloadPolicy::Stay);
        check_across_handover(results, std::move(hp));
    }
}

TEST(Query_DistinctThroughLinks)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    TableRef t2 = g.add_table("t2");
    TableRef t3 = g.add_table("t3");

    auto t1_int_col = t1->add_column(type_Int, "t1_int");
    auto t1_link_col = t1->add_column(*t2, "t1_link_t2");

    auto t2_int_col = t2->add_column(type_Int, "t2_int");
    auto t2_link_col = t2->add_column(*t3, "t2_link_t3");

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
        tv.distinct(DistinctDescriptor({{t1_int_col}}));
        CHECK_EQUAL(tv.size(), results1.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results1[i]);
        }
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.distinct(DistinctDescriptor({{t1_int_col}}));
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results1[i]); // results haven't been sorted
        }
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.sort(SortDescriptor({{t1_int_col}}, {true}));
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results1[i]); // still same order here by conincidence
        }
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.sort(SortDescriptor({{t1_int_col}}, {false}));
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results1[results1.size() - 1 - i]); // now its reversed
        }
    }

    {
        TableView tv = t1->where().less(t1_int_col, 6).find_all();

        // Test basic one link chain
        std::vector<size_t> results2 = {0, 1, 2, 4};
        tv.distinct(DistinctDescriptor({{t1_link_col, t2_int_col}}));
        CHECK_EQUAL(tv.size(), results2.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results2[i]);
        }

        std::vector<size_t> results2_sorted_link = {0, 4, 2, 1};
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.distinct(DistinctDescriptor({{t1_link_col, t2_int_col}}));
        tv.sort(SortDescriptor({{t1_link_col, t2_int_col}}, {true}));
        CHECK_EQUAL(tv.size(), results2_sorted_link.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results2_sorted_link[i]);
        }
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.distinct(DistinctDescriptor({{t1_link_col, t2_int_col}}));
        tv.sort(SortDescriptor({{t1_link_col, t2_int_col}}, {false}));
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results2_sorted_link[results2_sorted_link.size() - 1 - i]);
        }
    }

    {
        TableView tv = t1->where().less(t1_int_col, 6).find_all();

        // Test link chain through two links with nulls
        std::vector<size_t> results3 = {0, 1, 2, 5};
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.distinct(DistinctDescriptor({{t1_link_col, t2_link_col, t3_int_col}}));
        // Nullified links are excluded from distinct.
        CHECK_EQUAL(tv.size(), results3.size());
        for (size_t i = 0; i < results3.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results3[i]);
        }

        results3 = {1, 0, 2, 5}; // sorted order on t3_col_int { null, 3, 4, 7 }
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.distinct(DistinctDescriptor({{t1_link_col, t2_link_col, t3_int_col}}));
        tv.sort(SortDescriptor({{t1_link_col, t2_link_col, t3_int_col}}));
        CHECK_EQUAL(tv.size(), results3.size());
        for (size_t i = 0; i < results3.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results3[i]);
        }
        tv = t1->where().less(t1_int_col, 6).find_all();
        tv.distinct(DistinctDescriptor({{t1_link_col, t2_link_col, t3_int_col}}));
        tv.sort(SortDescriptor({{t1_link_col, t2_link_col, t3_int_col}}, {false}));
        CHECK_EQUAL(tv.size(), results3.size());
        for (size_t i = 0; i < results3.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results3[results3.size() - 1 - i]);
        }
    }

    {
        TableView tv = t1->where().less(t1_int_col, 6).find_all();

        // Test distinct after sort
        tv.sort(SortDescriptor({{t1_link_col, t2_int_col}}, {true}));
        //  t1_int   link.t2_int
        //  ====================
        //  0        0
        //  3        0
        //  5        0
        //  4        1
        //  2        2
        //  1        3

        tv.distinct(DistinctDescriptor({{t1_link_col, t2_int_col}}));
        //  t1_int   link.t2_int
        //  ====================
        //  0        0
        //  4        1
        //  2        2
        //  1        3

        std::vector<size_t> results = {0, 4, 2, 1};
        CHECK_EQUAL(tv.size(), results.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv[i].get<Int>(t1_int_col), results[i]);
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
    tv3.get(0).remove();
    tv3.sync_if_needed();
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

TEST(Query_Performance)
{
    Group g;
    auto foo = g.add_table("Foo");
    auto bar = g.add_table("Bar");

    auto col_double = foo->add_column(type_Double, "doubles");
    auto col_int = foo->add_column(type_Int, "ints");
    auto col_link = bar->add_column(*foo, "links");
    auto col_linklist = bar->add_column_list(*foo, "linklists");

    for (int i = 0; i < 10000; i++) {
        auto obj = foo->create_object();
        obj.set(col_double, double(i % 19));
        obj.set(col_int, 30 - (i % 19));
    }
    auto it = foo->begin();
    for (int i = 0; i < 1000; i++) {
        auto obj = bar->create_object();
        obj.set(col_link, it->get_key());
        auto ll = obj.get_linklist(col_linklist);
        for (size_t j = 0; j < 10; j++) {
            ll.add(it->get_key());
            ++it;
        }
    }

    auto t1 = steady_clock::now();

    size_t cnt = (foo->column<double>(col_double) > 10).count();
    CHECK_EQUAL(cnt, 4208);

    auto t2 = steady_clock::now();
    CHECK(t2 > t1);

    cnt = (bar->link(col_link).column<double>(col_double) > 10).count();
    CHECK_EQUAL(cnt, 421);

    auto t3 = steady_clock::now();
    CHECK(t3 > t2);
    CALLGRIND_START_INSTRUMENTATION;

    cnt = (bar->link(col_linklist).column<double>(col_double) > 15).count();
    CHECK_EQUAL(cnt, 630);

    CALLGRIND_STOP_INSTRUMENTATION;
    auto t4 = steady_clock::now();
    CHECK(t4 > t3);

    cnt = (foo->column<double>(col_double) > foo->column<Int>(col_int)).count();
    CHECK_EQUAL(cnt, 1578);

    auto t5 = steady_clock::now();
    CHECK(t5 > t4);

    /*
    std::cout << "Row against constant: " << duration_cast<microseconds>(t2 - t1).count() << " us" << std::endl;
    std::cout << "Linked row against constant: " << duration_cast<microseconds>(t3 - t2).count() << " us"
              << std::endl;
    std::cout << "List row against constant: " << duration_cast<microseconds>(t4 - t3).count() << " us" << std::endl;
    std::cout << "Row against row: " << duration_cast<microseconds>(t5 - t4).count() << " us" << std::endl;
    */
}

TEST(Query_AllocatorBug_DestOlderThanSource)
{
    // At some point this test failed when cluster node size was 4.
    Group g;
    auto foo = g.add_table("Foo");
    auto bar = g.add_table("Bar");

    auto col_double = foo->add_column(type_Double, "doubles");
    auto col_int = foo->add_column(type_Int, "ints");
    auto col_link = bar->add_column(*foo, "links");
    auto col_linklist = bar->add_column_list(*foo, "linklists");

    for (int i = 0; i < 10000; i++) {
        auto obj = foo->create_object();
        obj.set(col_double, double(i % 19));
        obj.set(col_int, 30 - (i % 19));
    }

    // At this point the WrappedAllocator in "foo" points to a translation table with 6 elements

    auto it = foo->begin();
    for (int i = 0; i < 1000; i++) {
        // During this a new slab is needed, so the translation table in "bar" contains 7 elements.
        // Some clusters if "bar" will use this last element for translation
        auto obj = bar->create_object();
        obj.set(col_link, it->get_key());
        auto ll = obj.get_linklist(col_linklist);
        for (size_t j = 0; j < 10; j++) {
            ll.add(it->get_key());
            ++it;
        }
    }

    // When traversion clusters in "bar" wee should use the "bar" wrapped allocator and not the
    // one in "foo" (that was the error)
    auto cnt = (bar->link(col_link).column<double>(col_double) > 10).count();
    CHECK_EQUAL(cnt, 421);
}

TEST(Query_AllocatorBug_SourceOlderThanDest)
{
    Group g;
    auto foo = g.add_table("Foo");
    auto bar = g.add_table("Bar");

    auto col_double = foo->add_column_list(type_Double, "doubles");
    auto col_link = bar->add_column(*foo, "links");
    auto col_linklist = bar->add_column_list(*foo, "linklists");

    for (int i = 0; i < 10000; i++)
        foo->create_object();

    // foo's WrappedAllocator now points to a translation table with 2 elements

    auto it = foo->begin();
    for (int i = 0; i < 1000; i++) {
        auto obj = bar->create_object();
        obj.set(col_link, it->get_key());
        auto ll = obj.get_linklist(col_linklist);
        for (size_t j = 0; j < 10; j++) {
            ll.add(it->get_key());
            ++it;
        }
    }

    // bar's WrappedAllocator now points to a translation table with 3 elements

    int i = 0;
    for (auto& obj : *foo) {
        obj.get_list<double>(col_double).add(double(i % 19));
        ++i;
    }

    // foo's WrappedAllocator now points to a translation table with 6 elements

    // If this query uses bar's allocator to access foo it'll perform an out-of-bounds read
    // for any of the values in slabs 3-5
    auto cnt = (bar->link(col_link).column<Lst<double>>(col_double) > 10).count();
    CHECK_EQUAL(cnt, 421);
    cnt = (bar->link(col_link).column<Lst<double>>(col_double).size() == 1).count();
    cnt = (bar->link(col_link).column<Lst<double>>(col_double).min() == 1).count();
}

TEST(Query_StringNodeEqualBaseBug)
{
    Group g;
    TableRef table = g.add_table("table");
    auto col_type = table->add_column(type_String, "type");
    auto col_tags = table->add_column(type_String, "tags");
    table->add_search_index(col_type);

    // Create 2 clusters
    for (int i = 0; i < 500; i++) {
        table->create_object().set(col_type, "project").set(col_tags, "tag001");
    }

    Query q = table->where()
                  .equal(col_type, StringData("test"), false)
                  .Or()
                  .contains(col_tags, StringData("tag005"), false);
    auto tv = q.find_all();
    CHECK_EQUAL(tv.size(), 0);
    table->begin()->set(col_type, "task");
    tv.sync_if_needed();
    CHECK_EQUAL(tv.size(), 0);
}

// Disabled because it is timing-dependent and frequently causes spurious failures on CI.
TEST_IF(Query_OptimalNode, false)
{
    const char* types[9] = {"todo", "task", "issue", "report", "test", "item", "epic", "story", "flow"};
    Group g;
    TableRef table = g.add_table("table");
    auto col_type = table->add_column(type_String, "type");
    auto col_tags = table->add_column(type_String, "tags");
    table->add_search_index(col_type);

    for (int i = 0; i < 10000; i++) {
        auto obj = table->create_object();
        std::string type = types[i % 9];
        std::string val = type + util::to_string(i % 10);
        obj.set(col_type, val);
        std::string tags;
        for (int j = 0; j < 7; j++) {
            tags += " TAG" + util::to_string(i % 500);
        }
        obj.set(col_tags, tags);
    }

    auto q1 = table->where().equal(col_type, StringData("todo0"), false);
    q1.count(); // Warm up
    auto t1 = steady_clock::now();
    auto cnt = q1.count();
    auto t2 = steady_clock::now();
    CHECK_EQUAL(cnt, 112);
    auto dur1 = duration_cast<microseconds>(t2 - t1).count();
    // std::cout << "cnt: " << cnt << " dur1: " << dur1 << " us" << std::endl;

    auto q2 = table->where().contains(col_tags, StringData("tag0"), false);
    q2.count(); // Warm up
    t1 = steady_clock::now();
    cnt = q2.count();
    t2 = steady_clock::now();
    CHECK_EQUAL(cnt, 20);
    auto dur2 = duration_cast<microseconds>(t2 - t1).count();
    // std::cout << "cnt: " << cnt << " dur2: " << dur2 << " us" << std::endl;

    Query q = q1.and_query(q2);
    q.count(); // Warm up
    t1 = steady_clock::now();
    cnt = q.count();
    t2 = steady_clock::now();
    CHECK_EQUAL(cnt, 3);
    auto dur3 = duration_cast<microseconds>(t2 - t1).count();

    // The duration of the combined query should be closer to the duration for
    // the query using the index.
    CHECK_GREATER(dur3, dur1);
    CHECK_LESS(dur3, dur2 / 5);
    // std::cout << "cnt: " << cnt << " dur3: " << dur3 << " us" << std::endl;
}

TEST(Query_IntPerformance)
{
    Table table;
    auto col_1 = table.add_column(type_Int, "1");
    auto col_2 = table.add_column(type_Int, "2");

    for (int i = 0; i < 1000; i++) {
        Obj o = table.create_object().set(col_1, i).set(col_2, i == 500 ? 500 : 2);
    }

    Query q1 = table.where().equal(col_2, 2);
    Query q2 = table.where().not_equal(col_1, 500);

    auto t1 = steady_clock::now();

    CALLGRIND_START_INSTRUMENTATION;

    size_t nb_reps = 1000;
    for (size_t t = 0; t < nb_reps; t++) {
        TableView tv = q1.find_all();
        CHECK_EQUAL(tv.size(), 999);
    }

    auto t2 = steady_clock::now();

    for (size_t t = 0; t < nb_reps; t++) {
        TableView tv = q2.find_all();
        CHECK_EQUAL(tv.size(), 999);
    }

    auto t3 = steady_clock::now();

    for (size_t t = 0; t < nb_reps; t++) {
        auto sum = q2.sum_int(col_2);
        CHECK_EQUAL(sum, 1998);
    }

    CALLGRIND_STOP_INSTRUMENTATION;

    auto t4 = steady_clock::now();

    std::cout << nb_reps << " repetitions in Query_IntPerformance" << std::endl;
    std::cout << "    time equal: " << duration_cast<nanoseconds>(t2 - t1).count() / nb_reps << " ns/rep"
              << std::endl;
    std::cout << "    time not_equal: " << duration_cast<nanoseconds>(t3 - t2).count() / nb_reps << " ns/rep"
              << std::endl;
    std::cout << "    time sum: " << duration_cast<nanoseconds>(t4 - t3).count() / nb_reps << " ns/rep" << std::endl;
}

TEST(Query_NotWithEmptyGroup)
{
    Group g;
    TableRef table = g.add_table("table");
    auto col = table->add_column(type_String, "type");
    table->create_object().set(col, "hello");
    auto q = table->where().equal(col, "hello").Or().Not().group().end_group();
    CHECK_EQUAL(q.count(), 1);
    q = table->where().Not().group().end_group().Or().equal(col, "hello");
    CHECK_EQUAL(q.count(), 1);
}

#endif // TEST_QUERY
