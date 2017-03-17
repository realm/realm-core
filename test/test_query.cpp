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
#include <realm/lang_bind_helper.hpp>
#include <realm/column.hpp>
#include <realm/history.hpp>
#include <realm/query_engine.hpp>

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
        CHECK_EQUAL(not_found, query.find());
    }
    {
        Query query = table.where();
        CHECK_EQUAL(not_found, query.find());
    }
    table.add_empty_row();
    {
        Query query(table.where());
        CHECK_EQUAL(0, query.find());
    }
    {
        Query query = table.where();
        CHECK_EQUAL(0, query.find());
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
        table.add_column(type_Int, "i");

        size_t matching = 0;
        size_t not_matching = 0;
        size_t rows = random.draw_int_mod(5 * REALM_MAX_BPNODE_SIZE); // to cross some leaf boundaries

        for (size_t i = 0; i < rows; ++i) {
            table.add_empty_row();
            int64_t val = random.draw_int_mod(5);
            table.set_int(0, i, val);
            if (val == 2)
                matching++;
            else
                not_matching++;
        }

        CHECK_EQUAL(matching, table.where().equal(0, 2).count());
        CHECK_EQUAL(not_matching, table.where().not_equal(0, 2).count());
    }
}


TEST(Query_NextGenSyntaxTypedString)
{
    TestTable books;
    books.add_column(type_String, "1");
    books.add_column(type_String, "2");
    books.add_column(type_Int, "3");

    add(books, "Computer Architecture and Organization", "B. Govindarajalu", 752);
    add(books, "Introduction to Quantum Mechanics", "David Griffiths", 480);
    add(books, "Biophysics: Searching for Principles", "William Bialek", 640);

    // Typed table:
    Query q = books.column<Int>(2) >= 200 && books.column<String>(1) == "David Griffiths";
    size_t match = q.find();
    CHECK_EQUAL(1, match);
    // You don't need to create a query object first:
    match = (books.column<Int>(2) >= 200 && books.column<String>(1) == "David Griffiths").find();
    CHECK_EQUAL(1, match);

    // You can also create column objects and use them in expressions:
    Columns<Int> pages = books.column<Int>(2);
    Columns<String> author = books.column<String>(1);
    match = (pages >= 200 && author == "David Griffiths").find();
    CHECK_EQUAL(1, match);
}

TEST(Query_NextGenSyntax)
{
    size_t match;

    // Setup untyped table
    Table untyped;
    untyped.add_column(type_Int, "firs1");
    untyped.add_column(type_Float, "second");
    untyped.add_column(type_Double, "third");
    untyped.add_column(type_Bool, "third2");
    untyped.add_column(type_String, "fourth");
    untyped.add_empty_row(2);
    untyped.set_int(0, 0, 20);
    untyped.set_float(1, 0, 19.9f);
    untyped.set_double(2, 0, 3.0);
    untyped.set_bool(3, 0, true);
    untyped.set_string(4, 0, "hello");

    untyped.set_int(0, 1, 20);
    untyped.set_float(1, 1, 20.1f);
    untyped.set_double(2, 1, 4.0);
    untyped.set_bool(3, 1, false);
    untyped.set_string(4, 1, "world");

    TestTable typed;
    typed.add_column(type_Int, "1");
    typed.add_column(type_Float, "2");
    typed.add_column(type_Double, "3");
    typed.add_column(type_Bool, "4");
    typed.add_column(type_String, "5");

    add(typed, 20, 19.9f, 3.0, true, "hello");
    add(typed, 20, 20.1f, 4.0, false, "world");

    match = (untyped.column<String>(4) == "world").find();
    CHECK_EQUAL(match, 1);

    match = ("world" == untyped.column<String>(4)).find();
    CHECK_EQUAL(match, 1);

    match = ("hello" != untyped.column<String>(4)).find();
    CHECK_EQUAL(match, 1);

    match = (!("hello" == untyped.column<String>(4))).find();
    CHECK_EQUAL(match, 1);

    match = (untyped.column<String>(4) != StringData("hello")).find();
    CHECK_EQUAL(match, 1);

    match = (!(untyped.column<String>(4) == StringData("hello"))).find();
    CHECK_EQUAL(match, 1);

    match = (!(!(untyped.column<String>(4) != StringData("hello")))).find();
    CHECK_EQUAL(match, 1);


    // This is a demonstration of fallback to old query_engine for the specific cases where it's possible
    // because old engine is faster. This will return a ->less(...) query
    match = (untyped.column<int64_t>(0) == untyped.column<int64_t>(0)).find();
    CHECK_EQUAL(match, 0);


    match = (untyped.column<bool>(3) == false).find();
    CHECK_EQUAL(match, 1);

    match = (20.3 > untyped.column<double>(2) + 2).find();
    CHECK_EQUAL(match, 0);


    match = (untyped.column<int64_t>(0) > untyped.column<int64_t>(0)).find();
    CHECK_EQUAL(match, not_found);


    // Small typed table test:
    match = (typed.column<float>(1) + 100 > 120 && typed.column<int64_t>(0) > 2).find();
    CHECK_EQUAL(match, 1);

    // internal negation (rewrite of test above):
    match = (!(!(typed.column<float>(1) + 100 > 120) || !(typed.column<int64_t>(0) > 2))).find();
    CHECK_EQUAL(match, 1);


    // Untyped &&

    // Left condition makes first row non-match
    match = (untyped.column<float>(1) + 1 > 21 && untyped.column<double>(2) > 2).find();
    CHECK_EQUAL(match, 1);

    // Right condition makes first row a non-match
    match = (untyped.column<float>(1) > 10 && untyped.column<double>(2) > 3.5).find();
    CHECK_EQUAL(match, 1);

    // Both make first row match
    match = (untyped.column<float>(1) < 20 && untyped.column<double>(2) > 2).find();
    CHECK_EQUAL(match, 0);

    // Both make first row non-match
    match = (untyped.column<float>(1) > 20 && untyped.column<double>(2) > 3.5).find();
    CHECK_EQUAL(match, 1);

    // Left cond match 0, right match 1
    match = (untyped.column<float>(1) < 20 && untyped.column<double>(2) > 3.5).find();
    CHECK_EQUAL(match, not_found);

    // Left match 1, right match 0
    match = (untyped.column<float>(1) > 20 && untyped.column<double>(2) < 3.5).find();
    CHECK_EQUAL(match, not_found);

    // Untyped ||

    // Left match 0
    match = (untyped.column<float>(1) < 20 || untyped.column<double>(2) < 3.5).find();
    CHECK_EQUAL(match, 0);

    // Right match 0
    match = (untyped.column<float>(1) > 20 || untyped.column<double>(2) < 3.5).find();
    CHECK_EQUAL(match, 0);

    // Left match 1

    match = (untyped.column<float>(1) > 20 || untyped.column<double>(2) > 9.5).find();

    CHECK_EQUAL(match, 1);

    Query q4 = untyped.column<float>(1) + untyped.column<int64_t>(0) > 40;


    Query q5 = 20 < untyped.column<float>(1);

    match = q4.and_query(q5).find();
    CHECK_EQUAL(match, 1);


    // Untyped, direct column addressing
    Value<int64_t> uv1(1);

    Columns<float> uc1 = untyped.column<float>(1);

    Query q2 = uv1 <= uc1;
    match = q2.find();
    CHECK_EQUAL(match, 0);


    Query q0 = uv1 <= uc1;
    match = q0.find();
    CHECK_EQUAL(match, 0);

    Query q99 = uv1 <= untyped.column<float>(1);
    match = q99.find();
    CHECK_EQUAL(match, 0);


    Query q8 = 1 > untyped.column<float>(1) + 5;
    match = q8.find();
    CHECK_EQUAL(match, not_found);

    Query q3 = untyped.column<float>(1) + untyped.column<int64_t>(0) > 10 + untyped.column<int64_t>(0);
    match = q3.find();

    match = q2.find();
    CHECK_EQUAL(match, 0);


    // Typed, direct column addressing
    Query q1 = typed.column<float>(1) + typed.column<Int>(0) > 40;
    match = q1.find();
    CHECK_EQUAL(match, 1);


    match = (typed.column<Int>(0) + typed.column<float>(1) > 40).find();
    CHECK_EQUAL(match, 1);


    Query tq1 = typed.column<Int>(0) + typed.column<float>(1) >= typed.column<Int>(0) + typed.column<float>(1);
    match = tq1.find();
    CHECK_EQUAL(match, 0);


    // Typed, column objects
    Columns<int64_t> t0 = typed.column<Int>(0);
    Columns<float> t1 = typed.column<float>(1);

    match = (t0 + t1 > 40).find();
    CHECK_EQUAL(match, 1);

    match = q1.find();
    CHECK_EQUAL(match, 1);

    match = (untyped.column<int64_t>(0) + untyped.column<float>(1) > 40).find();
    CHECK_EQUAL(match, 1);

    match = (untyped.column<int64_t>(0) + untyped.column<float>(1) < 40).find();
    CHECK_EQUAL(match, 0);

    match = (untyped.column<float>(1) <= untyped.column<int64_t>(0)).find();
    CHECK_EQUAL(match, 0);

    match = (untyped.column<int64_t>(0) + untyped.column<float>(1) >=
             untyped.column<int64_t>(0) + untyped.column<float>(1))
                .find();
    CHECK_EQUAL(match, 0);

    // Untyped, column objects
    Columns<int64_t> u0 = untyped.column<int64_t>(0);
    Columns<float> u1 = untyped.column<float>(1);

    match = (u0 + u1 > 40).find();
    CHECK_EQUAL(match, 1);
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
    table1->add_column(type_String, "str1");
    table1->add_column(type_String, "str2");

    // add some rows
    table1->add_empty_row();
    table1->set_string(0, 0, "foo");
    table1->set_string(1, 0, "F");
    table1->add_empty_row();
    table1->set_string(0, 1, "!");
    table1->set_string(1, 1, "x");
    table1->add_empty_row();
    table1->set_string(0, 2, "bar");
    table1->set_string(1, 2, "r");

    size_t m;
    // Equal
    m = table1->column<String>(0).equal("bar", false).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).equal("bar", true).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).equal("Bar", true).find();
    CHECK_EQUAL(m, not_found);

    m = table1->column<String>(0).equal("Bar", false).find();
    CHECK_EQUAL(m, 2);

    // Contains
    m = table1->column<String>(0).contains("a", false).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).contains("a", true).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).contains("A", true).find();
    CHECK_EQUAL(m, not_found);

    m = table1->column<String>(0).contains("A", false).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).contains(table1->column<String>(1), false).find();
    CHECK_EQUAL(m, 0);

    m = table1->column<String>(0).contains(table1->column<String>(1), true).find();
    CHECK_EQUAL(m, 2);

    // Begins with
    m = table1->column<String>(0).begins_with("b", false).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).begins_with("b", true).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).begins_with("B", true).find();
    CHECK_EQUAL(m, not_found);

    m = table1->column<String>(0).begins_with("B", false).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).begins_with(table1->column<String>(1), false).find();
    CHECK_EQUAL(m, 0);

    m = table1->column<String>(0).begins_with(table1->column<String>(1), true).find();
    CHECK_EQUAL(m, not_found);

    // Ends with
    m = table1->column<String>(0).ends_with("r", false).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).ends_with("r", true).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).ends_with("R", true).find();
    CHECK_EQUAL(m, not_found);

    m = table1->column<String>(0).ends_with("R", false).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).ends_with(table1->column<String>(1), false).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).ends_with(table1->column<String>(1), true).find();
    CHECK_EQUAL(m, 2);

    // Like (wildcard matching)
    m = table1->column<String>(0).like("b*", true).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).like("b*", false).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).like("*r", false).find();
    CHECK_EQUAL(m, 2);

    m = table1->column<String>(0).like("f?o", false).find();
    CHECK_EQUAL(m, 0);

    m = (table1->column<String>(0).like("f*", false) && table1->column<String>(0) == "foo").find();
    CHECK_EQUAL(m, 0);

    m = table1->column<String>(0).like(table1->column<String>(1), true).find();
    CHECK_EQUAL(m, not_found);

    // Test various compare operations with null
    TableRef table2 = group.add_table("table2");
    table2->add_column(type_String, "str1", true);

    table2->add_empty_row();
    table2->set_string(0, 0, "foo");
    table2->add_empty_row();
    table2->set_string(0, 1, "!");
    table2->add_empty_row();
    table2->set_string(0, 2, realm::null());
    table2->add_empty_row();
    table2->set_string(0, 3, "bar");
    table2->add_empty_row();
    table2->set_string(0, 4, "");

    m = table2->column<String>(0).contains(StringData("")).count();
    CHECK_EQUAL(m, 4);

    m = table2->column<String>(0).begins_with(StringData("")).count();
    CHECK_EQUAL(m, 4);

    m = table2->column<String>(0).ends_with(StringData("")).count();
    CHECK_EQUAL(m, 4);

    m = table2->column<String>(0).equal(StringData("")).count();
    CHECK_EQUAL(m, 1);

    m = table2->column<String>(0).not_equal(StringData("")).count();
    CHECK_EQUAL(m, 4);

    m = table2->column<String>(0).equal(realm::null()).count();
    CHECK_EQUAL(m, 1);

    m = table2->column<String>(0).not_equal(realm::null()).count();
    CHECK_EQUAL(m, 4);


    m = table2->column<String>(0).contains(StringData(""), false).count();
    CHECK_EQUAL(m, 4);

    m = table2->column<String>(0).like(StringData(""), false).count();
    CHECK_EQUAL(m, 1);

    m = table2->column<String>(0).begins_with(StringData(""), false).count();
    CHECK_EQUAL(m, 4);

    m = table2->column<String>(0).ends_with(StringData(""), false).count();
    CHECK_EQUAL(m, 4);

    m = table2->column<String>(0).equal(StringData(""), false).count();
    CHECK_EQUAL(m, 1);

    m = table2->column<String>(0).not_equal(StringData(""), false).count();
    CHECK_EQUAL(m, 4);

    m = table2->column<String>(0).equal(realm::null(), false).count();
    CHECK_EQUAL(m, 1);

    m = table2->column<String>(0).not_equal(realm::null(), false).count();
    CHECK_EQUAL(m, 4);

    m = table2->column<String>(0).contains(realm::null(), false).count();
    CHECK_EQUAL(m, 4);

    m = table2->column<String>(0).like(realm::null(), false).count();
    CHECK_EQUAL(m, 1);

    TableRef table3 = group.add_table(StringData("table3"));
    table3->add_column_link(type_Link, "link1", *table2);

    table3->add_empty_row();
    table3->set_link(0, 0, 0);
    table3->add_empty_row();
    table3->set_link(0, 1, 1);
    table3->add_empty_row();
    table3->set_link(0, 2, 2);
    table3->add_empty_row();
    table3->set_link(0, 3, 3);
    table3->add_empty_row();
    table3->set_link(0, 4, 4);

    m = table3->link(0).column<String>(0).contains(StringData("")).count();
    CHECK_EQUAL(m, 4);

    m = table3->link(0).column<String>(0).begins_with(StringData("")).count();
    CHECK_EQUAL(m, 4);

    m = table3->link(0).column<String>(0).ends_with(StringData("")).count();
    CHECK_EQUAL(m, 4);

    m = table3->link(0).column<String>(0).equal(StringData("")).count();
    CHECK_EQUAL(m, 1);

    m = table3->link(0).column<String>(0).not_equal(StringData("")).count();
    CHECK_EQUAL(m, 4);

    m = table3->link(0).column<String>(0).equal(realm::null()).count();
    CHECK_EQUAL(m, 1);

    m = table3->link(0).column<String>(0).not_equal(realm::null()).count();
    CHECK_EQUAL(m, 4);


    m = table3->link(0).column<String>(0).contains(StringData(""), false).count();
    CHECK_EQUAL(m, 4);

    m = table3->link(0).column<String>(0).like(StringData(""), false).count();
    CHECK_EQUAL(m, 1);

    m = table3->link(0).column<String>(0).begins_with(StringData(""), false).count();
    CHECK_EQUAL(m, 4);

    m = table3->link(0).column<String>(0).ends_with(StringData(""), false).count();
    CHECK_EQUAL(m, 4);

    m = table3->link(0).column<String>(0).equal(StringData(""), false).count();
    CHECK_EQUAL(m, 1);

    m = table3->link(0).column<String>(0).not_equal(StringData(""), false).count();
    CHECK_EQUAL(m, 4);

    m = table3->link(0).column<String>(0).equal(realm::null(), false).count();
    CHECK_EQUAL(m, 1);

    m = table3->link(0).column<String>(0).not_equal(realm::null(), false).count();
    CHECK_EQUAL(m, 4);

    m = table3->link(0).column<String>(0).contains(realm::null(), false).count();
    CHECK_EQUAL(m, 4);
    
    // Test long string contains search (where needle is longer than 255 chars)
    table2->add_empty_row();
    table2->set_string(0, 0, "This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, needle, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!");
    
    m = table2->column<String>(0).contains("This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, needle", false).count();
    CHECK_EQUAL(m, 1);
    
    m = table2->column<String>(0).contains("This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, This is a long search string that does not contain the word being searched for!, needle", true).count();
    CHECK_EQUAL(m, 1);
    
    m = table3->link(0).column<String>(0).like(realm::null(), false).count();
    CHECK_EQUAL(m, 1);
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
        table.add_column(type_Int, "first");
        table.add_column(type_Float, "second");
        table.add_column(type_String, "third");

        for (size_t r = 0; r < rows; r++) {
            table.add_empty_row();
            // using '% iter' tests different bitwidths
            table.set_int(0, r, random.draw_int_mod(iter));
            table.set_float(1, r, float(random.draw_int_mod(iter)));
            if (random.draw_bool())
                table.set_string(2, r, "a");
            else
                table.set_string(2, r, "b");
        }

        size_t tvpos;

        realm::Query q = table.column<Int>(0) > table.column<Float>(1) && table.column<String>(2) == "a";

        // without start or limit
        realm::TableView tv = q.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(0, r) > table.get_float(1, r) && table.get_string(2, r) == "a") {
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
        for (size_t r = 0; r < rows; r++) {
            if (r >= start && tvpos < limit && table.get_int(0, r) > table.get_float(1, r) &&
                table.get_string(2, r) == "a") {
                tvpos++;
            }
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
        table.add_column(type_Int, "first");
        table.add_column(type_Int, "second");
        table.add_column(type_Int, "third");

        for (size_t r = 0; r < rows; r++) {
            table.add_empty_row();
            // using '% iter' tests different bitwidths
            table.set_int(0, r, random.draw_int_mod(iter));
            table.set_int(1, r, random.draw_int_mod(iter));
            table.set_int(2, r, random.draw_int_mod(iter));
        }

        size_t tvpos;

        // second == 1
        realm::Query q1_0 = table.where().equal(1, 1);
        realm::Query q2_0 = table.column<int64_t>(1) == 1;
        realm::TableView tv_0 = q2_0.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(1, r) == 1) {
                CHECK_EQUAL(r, tv_0.get_source_ndx(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv_0.size());

        // (first == 0 || first == 1) && second == 1
        realm::Query q2_1 =
            (table.column<int64_t>(0) == 0 || table.column<int64_t>(0) == 1) && table.column<int64_t>(1) == 1;
        realm::TableView tv_1 = q2_1.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if ((table.get_int(0, r) == 0 || table.get_int(0, r) == 1) && table.get_int(1, r) == 1) {
                CHECK_EQUAL(r, tv_1.get_source_ndx(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv_1.size());


        // first == 0 || (first == 1 && second == 1)
        realm::Query q2_2 =
            table.column<int64_t>(0) == 0 || (table.column<int64_t>(0) == 1 && table.column<int64_t>(1) == 1);
        realm::TableView tv_2 = q2_2.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(0, r) == 0 || (table.get_int(0, r) == 1 && table.get_int(1, r) == 1)) {
                CHECK_EQUAL(r, tv_2.get_source_ndx(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv_2.size());


        // second == 0 && (first == 0 || first == 2)
        realm::Query q4_8 =
            table.column<int64_t>(1) == 0 && (table.column<int64_t>(0) == 0 || table.column<int64_t>(0) == 2);
        realm::TableView tv_8 = q4_8.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(1, r) == 0 && ((table.get_int(0, r) == 0) || table.get_int(0, r) == 2)) {
                CHECK_EQUAL(r, tv_8.get_source_ndx(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv_8.size());


        // (first == 0 || first == 2) && (first == 1 || second == 1)
        realm::Query q3_7 = (table.column<int64_t>(0) == 0 || table.column<int64_t>(0) == 2) &&
                            (table.column<int64_t>(0) == 1 || table.column<int64_t>(1) == 1);
        realm::TableView tv_7 = q3_7.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if ((table.get_int(0, r) == 0 || table.get_int(0, r) == 2) &&
                (table.get_int(0, r) == 1 || table.get_int(1, r) == 1)) {
                CHECK_EQUAL(r, tv_7.get_source_ndx(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv_7.size());


        // (first == 0 || first == 2) || (first == 1 || second == 1)
        realm::Query q4_7 = (table.column<int64_t>(0) == 0 || table.column<int64_t>(0) == 2) ||
                            (table.column<int64_t>(0) == 1 || table.column<int64_t>(1) == 1);
        realm::TableView tv_10 = q4_7.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if ((table.get_int(0, r) == 0 || table.get_int(0, r) == 2) ||
                (table.get_int(0, r) == 1 || table.get_int(1, r) == 1)) {
                CHECK_EQUAL(r, tv_10.get_source_ndx(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv_10.size());


        TableView tv;

        // first == 0 || first == 2 || first == 1 || second == 1
        realm::Query q20 = table.column<int64_t>(0) == 0 || table.column<int64_t>(0) == 2 ||
                           table.column<int64_t>(0) == 1 || table.column<int64_t>(1) == 1;
        tv = q20.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(0, r) == 0 || table.get_int(0, r) == 2 || table.get_int(0, r) == 1 ||
                table.get_int(1, r) == 1) {
                CHECK_EQUAL(r, tv.get_source_ndx(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv.size());


        // first * 2 > second / 2 + third + 1
        realm::Query q21 = table.column<int64_t>(0) * 2 > table.column<int64_t>(1) / 2 + table.column<int64_t>(2) + 1;
        tv = q21.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(0, r) * 2 > table.get_int(1, r) / 2 + table.get_int(2, r) + 1) {
                CHECK_EQUAL(r, tv.get_source_ndx(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv.size());

        // first * 2 > second / 2 + third + 1 + third - third + third - third + third - third + third - third + third
        // - third
        realm::Query q22 = table.column<int64_t>(0) * 2 >
                           table.column<int64_t>(1) / 2 + table.column<int64_t>(2) + 1 + table.column<int64_t>(2) -
                               table.column<int64_t>(2) + table.column<int64_t>(2) - table.column<int64_t>(2) +
                               table.column<int64_t>(2) - table.column<int64_t>(2) + table.column<int64_t>(2) -
                               table.column<int64_t>(2) + table.column<int64_t>(2) - table.column<int64_t>(2);
        tv = q22.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(0, r) * 2 > table.get_int(1, r) / 2 + table.get_int(2, r) + 1) {
                CHECK_EQUAL(r, tv.get_source_ndx(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv.size());
    }
}


TEST(Query_LimitUntyped)
{
    Table table;
    table.add_column(type_Int, "first1");
    table.add_column(type_Int, "second1");

    table.add_empty_row(3);
    table.set_int(0, 0, 10000);
    table.set_int(0, 1, 30000);
    table.set_int(0, 2, 10000);

    Query q = table.where();
    int64_t sum;

    sum = q.sum_int(0, nullptr, 0, -1, 1);
    CHECK_EQUAL(10000, sum);

    sum = q.sum_int(0, nullptr, 0, -1, 2);
    CHECK_EQUAL(40000, sum);

    sum = q.sum_int(0, nullptr, 0, -1, 3);
    CHECK_EQUAL(50000, sum);
}


TEST(Query_MergeQueriesOverloads)
{
    // Tests && and || overloads of Query class
    Table table;
    table.add_column(type_Int, "first");
    table.add_column(type_Int, "second");

    table.add_empty_row(3);
    table.set_int(0, 0, 20);
    table.set_int(1, 0, 20);

    table.set_int(0, 1, 20);
    table.set_int(1, 1, 30);

    table.set_int(0, 2, 30);
    table.set_int(1, 2, 30);

    size_t c;


    // q1_0 && q2_0
    realm::Query q1_110 = table.where().equal(0, 20);
    realm::Query q2_110 = table.where().equal(1, 30);
    realm::Query q3_110 = q1_110.and_query(q2_110);
    c = q1_110.count();
    c = q2_110.count();
    c = q3_110.count();


    // The overloads must behave such as if each side of the operator is inside parentheses, that is,
    // (first == 1 || first == 20) operator&& (second == 30), regardless of order of operands

    // q1_0 && q2_0
    realm::Query q1_0 = table.where().equal(0, 10).Or().equal(0, 20);
    realm::Query q2_0 = table.where().equal(1, 30);
    realm::Query q3_0 = q1_0 && q2_0;
    c = q3_0.count();
    CHECK_EQUAL(1, c);

    // q2_0 && q1_0 (reversed operand order)
    realm::Query q1_1 = table.where().equal(0, 10).Or().equal(0, 20);
    realm::Query q2_1 = table.where().equal(1, 30);
    c = q1_1.count();

    realm::Query q3_1 = q2_1 && q1_1;
    c = q3_1.count();
    CHECK_EQUAL(1, c);

    // Short test for ||
    realm::Query q1_2 = table.where().equal(0, 10);
    realm::Query q2_2 = table.where().equal(1, 30);
    realm::Query q3_2 = q2_2 || q1_2;
    c = q3_2.count();
    CHECK_EQUAL(2, c);
}


TEST(Query_MergeQueries)
{
    // test OR vs AND precedence
    Table table;
    table.add_column(type_Int, "first");
    table.add_column(type_Int, "second");

    table.add_empty_row(3);
    table.set_int(0, 0, 10);
    table.set_int(1, 0, 20);

    table.set_int(0, 1, 20);
    table.set_int(1, 1, 30);

    table.set_int(0, 2, 30);
    table.set_int(1, 2, 20);

    // Must evaluate as if and_query is inside paranthesis, that is, (first == 10 || first == 20) && second == 30
    realm::Query q1_0 = table.where().equal(0, 10).Or().equal(0, 20);
    realm::Query q2_0 = table.where().and_query(q1_0).equal(1, 30);

    size_t c = q2_0.count();
    CHECK_EQUAL(1, c);
}

TEST(Query_Not)
{
    // test Not vs And, Or, Groups.
    Table table;
    table.add_column(type_Int, "first");
    table.add_column(type_Int, "second");

    table.add_empty_row(3);
    table.set_int(0, 0, 10);
    table.set_int(1, 0, 20);

    table.set_int(0, 1, 20);
    table.set_int(1, 1, 30);

    table.set_int(0, 2, 30);
    table.set_int(1, 2, 20);

    // should apply not to single term, leading to query "not A" with two matching entries:
    realm::Query q0 = table.where().Not().equal(0, 10);
    CHECK_EQUAL(2, q0.count());

    // grouping, after not
    realm::Query q0b = table.where().Not().group().equal(0, 10).end_group();
    CHECK_EQUAL(2, q0b.count());

    // grouping, surrounding not
    realm::Query q0c = table.where().group().Not().equal(0, 10).end_group();
    CHECK_EQUAL(2, q0c.count());

    // nested nots (implicit grouping)
    realm::Query q0d = table.where().Not().Not().equal(0, 10);
    CHECK_EQUAL(1, q0d.count()); // FAILS

    realm::Query q0e = table.where().Not().Not().Not().equal(0, 10);
    CHECK_EQUAL(2, q0e.count()); // FAILS

    // just checking the above
    realm::Query q0f = table.where().Not().not_equal(0, 10);
    CHECK_EQUAL(1, q0f.count());

    realm::Query q0g = table.where().Not().Not().not_equal(0, 10);
    CHECK_EQUAL(2, q0g.count()); // FAILS

    realm::Query q0h = table.where().not_equal(0, 10);
    CHECK_EQUAL(2, q0h.count());

    // should apply not to first term, leading to query "not A and A", which is obviously empty:
    realm::Query q1 = table.where().Not().equal(0, 10).equal(0, 10);
    CHECK_EQUAL(0, q1.count());

    // should apply not to first term, leading to query "not A and A", which is obviously empty:
    realm::Query q1b = table.where().group().Not().equal(0, 10).end_group().equal(0, 10);
    CHECK_EQUAL(0, q1b.count());

    // should apply not to first term, leading to query "not A and A", which is obviously empty:
    realm::Query q1c = table.where().Not().group().equal(0, 10).end_group().equal(0, 10);
    CHECK_EQUAL(0, q1c.count());


    // should apply not to second term, leading to query "A and not A", which is obviously empty:
    realm::Query q2 = table.where().equal(0, 10).Not().equal(0, 10);
    CHECK_EQUAL(0, q2.count()); // FAILS

    // should apply not to second term, leading to query "A and not A", which is obviously empty:
    realm::Query q2b = table.where().equal(0, 10).group().Not().equal(0, 10).end_group();
    CHECK_EQUAL(0, q2b.count());

    // should apply not to second term, leading to query "A and not A", which is obviously empty:
    realm::Query q2c = table.where().equal(0, 10).Not().group().equal(0, 10).end_group();
    CHECK_EQUAL(0, q2c.count()); // FAILS


    // should apply not to both terms, leading to query "not A and not A", which has 2 members
    realm::Query q3 = table.where().Not().equal(0, 10).Not().equal(0, 10);
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
        table.add_column(type_Int, "first");
        table.add_column(type_Int, "second");
        table.add_column(type_Int, "third");

        for (size_t r = 0; r < rows; r++) {
            table.add_empty_row();
            table.set_int(0, r, random.draw_int_mod(3));
            table.set_int(1, r, random.draw_int_mod(3));
            table.set_int(2, r, random.draw_int_mod(3));
        }

        size_t tvpos;

        // and_query(second == 1)
        realm::Query q1_0 = table.where().equal(1, 1);
        realm::Query q2_0 = table.where().and_query(q1_0);
        realm::TableView tv_0 = q2_0.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(1, r) == 1) {
                CHECK_EQUAL(r, tv_0.get_source_ndx(tvpos));
                tvpos++;
            }
        }

        // (first == 0 || first == 1) && and_query(second == 1)
        realm::Query q1_1 = table.where().equal(1, 1);
        realm::Query q2_1 = table.where().group().equal(0, 0).Or().equal(0, 1).end_group().and_query(q1_1);
        realm::TableView tv_1 = q2_1.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if ((table.get_int(0, r) == 0 || table.get_int(0, r) == 1) && table.get_int(1, r) == 1) {
                CHECK_EQUAL(r, tv_1.get_source_ndx(tvpos));
                tvpos++;
            }
        }

        // first == 0 || (first == 1 && and_query(second == 1))
        realm::Query q1_2 = table.where().equal(1, 1);
        realm::Query q2_2 = table.where().equal(0, 0).Or().equal(0, 1).and_query(q1_2);
        realm::TableView tv_2 = q2_2.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(0, r) == 0 || (table.get_int(0, r) == 1 && table.get_int(1, r) == 1)) {
                CHECK_EQUAL(r, tv_2.get_source_ndx(tvpos));
                tvpos++;
            }
        }

        // and_query(first == 0) || (first == 1 && second == 1)
        realm::Query q1_3 = table.where().equal(0, 0);
        realm::Query q2_3 = table.where().and_query(q1_3).Or().equal(0, 1).equal(1, 1);
        realm::TableView tv_3 = q2_3.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(0, r) == 0 || (table.get_int(0, r) == 1 && table.get_int(1, r) == 1)) {
                CHECK_EQUAL(r, tv_3.get_source_ndx(tvpos));
                tvpos++;
            }
        }


        // first == 0 || and_query(first == 1 && second == 1)
        realm::Query q2_4 = table.where().equal(0, 1).equal(1, 1);
        realm::Query q1_4 = table.where().equal(0, 0).Or().and_query(q2_4);
        realm::TableView tv_4 = q1_4.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(0, r) == 0 || (table.get_int(0, r) == 1 && table.get_int(1, r) == 1)) {
                CHECK_EQUAL(r, tv_4.get_source_ndx(tvpos));
                tvpos++;
            }
        }


        // and_query(first == 0 || first == 2) || and_query(first == 1 && second == 1)
        realm::Query q2_5 = table.where().equal(0, 0).Or().equal(0, 2);
        realm::Query q1_5 = table.where().equal(0, 1).equal(1, 1);
        realm::Query q3_5 = table.where().and_query(q2_5).Or().and_query(q1_5);
        realm::TableView tv_5 = q3_5.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if ((table.get_int(0, r) == 0 || table.get_int(0, r) == 2) ||
                (table.get_int(0, r) == 1 && table.get_int(1, r) == 1)) {
                CHECK_EQUAL(r, tv_5.get_source_ndx(tvpos));
                tvpos++;
            }
        }


        // and_query(first == 0) && and_query(second == 1)
        realm::Query q1_6 = table.where().equal(0, 0);
        realm::Query q2_6 = table.where().equal(1, 1);
        realm::Query q3_6 = table.where().and_query(q1_6).and_query(q2_6);
        realm::TableView tv_6 = q3_6.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(0, r) == 0 && table.get_int(1, r) == 1) {
                CHECK_EQUAL(r, tv_6.get_source_ndx(tvpos));
                tvpos++;
            }
        }

        // and_query(first == 0 || first == 2) && and_query(first == 1 || second == 1)
        realm::Query q2_7 = table.where().equal(0, 0).Or().equal(0, 2);
        realm::Query q1_7 = table.where().equal(0, 1).equal(0, 1).Or().equal(1, 1);
        realm::Query q3_7 = table.where().and_query(q2_7).and_query(q1_7);
        realm::TableView tv_7 = q3_7.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if ((table.get_int(0, r) == 0 || table.get_int(0, r) == 2) &&
                (table.get_int(0, r) == 1 || table.get_int(1, r) == 1)) {
                CHECK_EQUAL(r, tv_7.get_source_ndx(tvpos));
                tvpos++;
            }
        }

        // Nested and_query

        // second == 0 && and_query(first == 0 || and_query(first == 2))
        realm::Query q2_8 = table.where().equal(0, 2);
        realm::Query q3_8 = table.where().equal(0, 0).Or().and_query(q2_8);
        realm::Query q4_8 = table.where().equal(1, 0).and_query(q3_8);
        realm::TableView tv_8 = q4_8.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(1, r) == 0 && ((table.get_int(0, r) == 0) || table.get_int(0, r) == 2)) {
                CHECK_EQUAL(r, tv_8.get_source_ndx(tvpos));
                tvpos++;
            }
        }


        // Nested as above but constructed differently

        // second == 0 && and_query(first == 0 || and_query(first == 2))
        realm::Query q2_9 = table.where().equal(0, 2);
        realm::Query q5_9 = table.where().equal(0, 0);
        realm::Query q3_9 = table.where().and_query(q5_9).Or().and_query(q2_9);
        realm::Query q4_9 = table.where().equal(1, 0).and_query(q3_9);
        realm::TableView tv_9 = q4_9.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(1, r) == 0 && ((table.get_int(0, r) == 0) || table.get_int(0, r) == 2)) {
                CHECK_EQUAL(r, tv_9.get_source_ndx(tvpos));
                tvpos++;
            }
        }


        // Nested

        // and_query(and_query(and_query(first == 0)))
        realm::Query q2_10 = table.where().equal(0, 0);
        realm::Query q5_10 = table.where().and_query(q2_10);
        realm::Query q3_10 = table.where().and_query(q5_10);
        realm::Query q4_10 = table.where().and_query(q3_10);
        realm::TableView tv_10 = q4_10.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(0, r) == 0) {
                CHECK_EQUAL(r, tv_10.get_source_ndx(tvpos));
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
        table.add_column(type_Int, "first");
        table.add_column(type_Int, "second");
        table.add_column(type_Int, "third");


        for (size_t r = 0; r < rows; r++) {
            table.add_empty_row();
            table.set_int(0, r, random.draw_int_mod(3));
            table.set_int(1, r, random.draw_int_mod(3));
            table.set_int(2, r, random.draw_int_mod(3));
        }

        size_t tvpos;

        // Left side of operator&& is empty query
        // and_query(second == 1)
        realm::Query q1_0 = table.where().equal(1, 1);
        realm::Query q2_0 = table.where() && q1_0;
        realm::TableView tv_0 = q2_0.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(1, r) == 1) {
                CHECK_EQUAL(r, tv_0.get_source_ndx(tvpos));
                tvpos++;
            }
        }

        // Right side of operator&& is empty query
        // and_query(second == 1)
        realm::Query q1_10 = table.where().equal(1, 1);
        realm::Query q2_10 = q1_10 && table.where();
        realm::TableView tv_10 = q2_10.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(1, r) == 1) {
                CHECK_EQUAL(r, tv_10.get_source_ndx(tvpos));
                tvpos++;
            }
        }

        // (first == 0 || first == 1) && and_query(second == 1)
        realm::Query q1_1 = table.where().equal(0, 0);
        realm::Query q2_1 = table.where().equal(0, 1);
        realm::Query q3_1 = q1_1 || q2_1;
        realm::Query q4_1 = table.where().equal(1, 1);
        realm::Query q5_1 = q3_1 && q4_1;

        realm::TableView tv_1 = q5_1.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if ((table.get_int(0, r) == 0 || table.get_int(0, r) == 1) && table.get_int(1, r) == 1) {
                CHECK_EQUAL(r, tv_1.get_source_ndx(tvpos));
                tvpos++;
            }
        }

        // (first == 0 || first == 1) && and_query(second == 1) as above, written in another way
        realm::Query q1_20 = table.where().equal(0, 0).Or().equal(0, 1) && table.where().equal(1, 1);
        realm::TableView tv_20 = q1_20.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if ((table.get_int(0, r) == 0 || table.get_int(0, r) == 1) && table.get_int(1, r) == 1) {
                CHECK_EQUAL(r, tv_20.get_source_ndx(tvpos));
                tvpos++;
            }
        }

        // and_query(first == 0) || (first == 1 && second == 1)
        realm::Query q1_3 = table.where().equal(0, 0);
        realm::Query q2_3 = table.where().equal(0, 1);
        realm::Query q3_3 = table.where().equal(1, 1);
        realm::Query q4_3 = q1_3 || (q2_3 && q3_3);
        realm::TableView tv_3 = q4_3.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(0, r) == 0 || (table.get_int(0, r) == 1 && table.get_int(1, r) == 1)) {
                CHECK_EQUAL(r, tv_3.get_source_ndx(tvpos));
                tvpos++;
            }
        }


        // and_query(first == 0) || (first == 1 && second == 1) written in another way
        realm::Query q1_30 = table.where().equal(0, 0);
        realm::Query q3_30 = table.where().equal(1, 1);
        realm::Query q4_30 = table.where().equal(0, 0) || (table.where().equal(0, 1) && q3_30);
        realm::TableView tv_30 = q4_30.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(0, r) == 0 || (table.get_int(0, r) == 1 && table.get_int(1, r) == 1)) {
                CHECK_EQUAL(r, tv_30.get_source_ndx(tvpos));
                tvpos++;
            }
        }
    }
}


TEST(Query_CountLimit)
{
    TestTable table;
    table.add_column(type_String, "1");
    table.add_column(type_Int, "2");

    add(table, "Mary", 14);
    add(table, "Joe", 17);
    add(table, "Alice", 42);
    add(table, "Jack", 22);
    add(table, "Bob", 50);
    add(table, "Frank", 12);

    // Select rows where age < 18
    Query query = table.where().less(1, 18);

    // Count all matching rows of entire table
    size_t count1 = query.count();
    CHECK_EQUAL(3, count1);

    // Very fast way to test if there are at least 2 matches in the table
    size_t count2 = query.count(0, size_t(-1), 2);
    CHECK_EQUAL(2, count2);

    // Count matches in latest 3 rows
    size_t count3 = query.count(table.size() - 3, table.size());
    CHECK_EQUAL(1, count3);
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
    table.add_column(type_Int, "first1");
    table.add_column(type_Float, "second1");
    table.add_column(type_Double, "third");


    size_t match;

    Columns<int64_t> first = table.column<int64_t>(0);
    Columns<float> second = table.column<float>(1);
    Columns<double> third = table.column<double>(2);

    table.add_empty_row(2);

    table.set_int(0, 0, 20);
    table.set_float(1, 0, 19.9f);
    table.set_double(2, 0, 3.0);

    table.set_int(0, 1, 20);
    table.set_float(1, 1, 20.1f);
    table.set_double(2, 1, 4.0);

    /**
    Conversion / promotion
    **/

    // 20 must convert to float
    match = (second + 0.2f > 20).find();
    CHECK_EQUAL(match, 0);

    match = (first >= 20.0f).find();
    CHECK_EQUAL(match, 0);

    // 20.1f must remain float
    match = (first >= 20.1f).find();
    CHECK_EQUAL(match, not_found);

    // first must convert to float
    match = (second >= first).find();
    CHECK_EQUAL(match, 1);

    // 20 and 40 must convert to float
    match = (second + 20 > 40).find();
    CHECK_EQUAL(match, 1);

    // first and 40 must convert to float
    match = (second + first >= 40).find();
    CHECK_EQUAL(match, 1);

    // 20 must convert to float
    match = (0.2f + second > 20).find();
    CHECK_EQUAL(match, 0);

    /**
    Permutations of types (Subexpr, Value, Column) of left/right side
    **/

    // Compare, left = Subexpr, right = Value
    match = (second + first >= 40).find();
    CHECK_EQUAL(match, 1);

    match = (second + first > 40).find();
    CHECK_EQUAL(match, 1);

    match = (first - second < 0).find();
    CHECK_EQUAL(match, 1);

    match = (second - second == 0).find();
    CHECK_EQUAL(match, 0);

    match = (first - second <= 0).find();
    CHECK_EQUAL(match, 1);

    match = (first * first != 400).find();
    CHECK_EQUAL(match, size_t(-1));

    // Compare, left = Column, right = Value
    match = (second >= 20).find();
    CHECK_EQUAL(match, 1);

    match = (second > 20).find();
    CHECK_EQUAL(match, 1);

    match = (second < 20).find();
    CHECK_EQUAL(match, 0);

    match = (second == 20.1f).find();
    CHECK_EQUAL(match, 1);

    match = (second != 19.9f).find();
    CHECK_EQUAL(match, 1);

    match = (second <= 21).find();
    CHECK_EQUAL(match, 0);

    // Compare, left = Column, right = Value
    match = (20 <= second).find();
    CHECK_EQUAL(match, 1);

    match = (20 < second).find();
    CHECK_EQUAL(match, 1);

    match = (20 > second).find();
    CHECK_EQUAL(match, 0);

    match = (20.1f == second).find();
    CHECK_EQUAL(match, 1);

    match = (19.9f != second).find();
    CHECK_EQUAL(match, 1);

    match = (21 >= second).find();
    CHECK_EQUAL(match, 0);

    // Compare, left = Subexpr, right = Value
    match = (40 <= second + first).find();
    CHECK_EQUAL(match, 1);

    match = (40 < second + first).find();
    CHECK_EQUAL(match, 1);

    match = (0 > first - second).find();
    CHECK_EQUAL(match, 1);

    match = (0 == second - second).find();
    CHECK_EQUAL(match, 0);

    match = (0 >= first - second).find();
    CHECK_EQUAL(match, 1);

    match = (400 != first * first).find();
    CHECK_EQUAL(match, size_t(-1));

    // Col compare Col
    match = (second > first).find();
    CHECK_EQUAL(match, 1);

    match = (second >= first).find();
    CHECK_EQUAL(match, 1);

    match = (second == first).find();
    CHECK_EQUAL(match, not_found);

    match = (second != second).find();
    CHECK_EQUAL(match, not_found);

    match = (first < second).find();
    CHECK_EQUAL(match, 1);

    match = (first <= second).find();
    CHECK_EQUAL(match, 1);

    // Subexpr compare Subexpr
    match = (second + 0 > first + 0).find();
    CHECK_EQUAL(match, 1);

    match = (second + 0 >= first + 0).find();
    CHECK_EQUAL(match, 1);

    match = (second + 0 == first + 0).find();
    CHECK_EQUAL(match, not_found);

    match = (second + 0 != second + 0).find();
    CHECK_EQUAL(match, not_found);

    match = (first + 0 < second + 0).find();
    CHECK_EQUAL(match, 1);

    match = (first + 0 <= second + 0).find();
    CHECK_EQUAL(match, 1);

    // Conversions, again
    table.clear();
    table.add_empty_row(1);

    table.set_int(0, 0, 20);
    table.set_float(1, 0, 3.0f);
    table.set_double(2, 0, 3.0);

    match = (1 / second == 1 / second).find();
    CHECK_EQUAL(match, 0);

    match = (1 / third == 1 / third).find();
    CHECK_EQUAL(match, 0);

    // Nifty test: Compare operator must preserve precision of each side, hence NO match; if double accidentially
    // was truncated to float, or float was rounded to nearest double, then this test would fail.
    match = (1 / second == 1 / third).find();
    CHECK_EQUAL(match, not_found);

    // power operator (power(x) = x^2)
    match = (power(first) == 400).find();
    CHECK_EQUAL(0, match);

    match = (power(first) == 401).find();
    CHECK_EQUAL(not_found, match);

    Query qq = (power(first) == 401);

    // power of floats. Using a range check because of float arithmetic imprecisions
    match = (power(second) < 9.001 && power(second) > 8.999).find();
    CHECK_EQUAL(0, match);

    // For `float < int_column` we had a bug where the float truncated to int, and the int_column remained int
    // (correct behaviour would be that the float remained float and int_column converted to float). This test
    // exposes such a bug because 1000000001 should convert to the nearest float value which is `1000000000.`
    // (gap between floats is bigger than 1 and cannot represent 1000000001).
    table.clear();
    table.add_empty_row(1);
    table.set_int(0, 0, 1000000001);

    match = (1000000000.f < first).find();
    CHECK_EQUAL(match, not_found);

    match = (first > 1000000000.f).find();
    CHECK_EQUAL(match, not_found);
}

TEST(Query_LimitUntyped2)
{
    Table table;
    table.add_column(type_Int, "first1");
    table.add_column(type_Float, "second1");
    table.add_column(type_Double, "second1");
    table.add_column(type_Timestamp, "date");

    table.add_empty_row(3);
    table.set_int(0, 0, 10000);
    table.set_int(0, 1, 30000);
    table.set_int(0, 2, 40000);

    table.set_float(1, 0, 10000.f);
    table.set_float(1, 1, 30000.f);
    table.set_float(1, 2, 40000.f);

    table.set_double(2, 0, 10000.);
    table.set_double(2, 1, 30000.);
    table.set_double(2, 2, 40000.);

    table.set_timestamp(3, 0, Timestamp(10000, 10000));
    table.set_timestamp(3, 1, Timestamp(30000, 30000));
    table.set_timestamp(3, 2, Timestamp(40000, 40000));

    Query q = table.where();
    int64_t sum;
    float sumf;
    double sumd;
    Timestamp ts;

    // sum, limited by 'limit'
    sum = q.sum_int(0, nullptr, 0, -1, 1);
    CHECK_EQUAL(10000, sum);
    sum = q.sum_int(0, nullptr, 0, -1, 2);
    CHECK_EQUAL(40000, sum);
    sum = q.sum_int(0, nullptr, 0, -1);
    CHECK_EQUAL(80000, sum);

    sumd = q.sum_float(1, nullptr, 0, -1, 1);
    CHECK_EQUAL(10000., sumd);
    sumd = q.sum_float(1, nullptr, 0, -1, 2);
    CHECK_EQUAL(40000., sumd);
    sumd = q.sum_float(1, nullptr, 0, -1);
    CHECK_EQUAL(80000., sumd);

    sumd = q.sum_double(2, nullptr, 0, -1, 1);
    CHECK_EQUAL(10000., sumd);
    sumd = q.sum_double(2, nullptr, 0, -1, 2);
    CHECK_EQUAL(40000., sumd);
    sumd = q.sum_double(2, nullptr, 0, -1);
    CHECK_EQUAL(80000., sumd);

    // sum, limited by 'end', but still having 'limit' specified
    sum = q.sum_int(0, nullptr, 0, 1, 3);
    CHECK_EQUAL(10000, sum);
    sum = q.sum_int(0, nullptr, 0, 2, 3);
    CHECK_EQUAL(40000, sum);

    sumd = q.sum_float(1, nullptr, 0, 1, 3);
    CHECK_EQUAL(10000., sumd);
    sumd = q.sum_float(1, nullptr, 0, 2, 3);
    CHECK_EQUAL(40000., sumd);

    sumd = q.sum_double(2, nullptr, 0, 1, 3);
    CHECK_EQUAL(10000., sumd);
    sumd = q.sum_double(2, nullptr, 0, 2, 3);
    CHECK_EQUAL(40000., sumd);

    size_t ndx = not_found;

    // max, limited by 'limit'

    // int
    sum = q.maximum_int(0, nullptr, 0, -1, 1);
    CHECK_EQUAL(10000, sum);
    q.maximum_int(0, nullptr, 0, -1, 1, &ndx);
    CHECK_EQUAL(0, ndx);

    sum = q.maximum_int(0, nullptr, 0, -1, 2);
    CHECK_EQUAL(30000, sum);
    q.maximum_int(0, nullptr, 0, -1, 2, &ndx);
    CHECK_EQUAL(1, ndx);

    sum = q.maximum_int(0, nullptr, 0, -1);
    CHECK_EQUAL(40000, sum);
    q.maximum_int(0, nullptr, 0, -1, -1, &ndx);
    CHECK_EQUAL(2, ndx);

    // Timestamp
    /*
    ts = q.maximum_timestamp(3, nullptr, 0, -1, 1);
    CHECK_EQUAL(Timestamp(10000, 10000), ts);
    q.maximum_int(0, nullptr, 0, -1, 1, &ndx);
    CHECK_EQUAL(0, ndx);

    ts = q.maximum_timestamp(3, nullptr, 0, -1, 2);
    CHECK_EQUAL(Timestamp(30000, 30000), ts);
    q.maximum_int(0, nullptr, 0, -1, 2, &ndx);
    CHECK_EQUAL(1, ndx);

    ts = q.maximum_timestamp(3, nullptr, 0, -1);
    CHECK_EQUAL(Timestamp(40000, 40000), ts);
    q.maximum_int(0, nullptr, 0, -1, -1, &ndx);
    CHECK_EQUAL(2, ndx);
    */
    // float
    sumf = q.maximum_float(1, nullptr, 0, -1, 1);
    CHECK_EQUAL(10000., sumf);
    q.maximum_float(1, nullptr, 0, -1, 1, &ndx);
    CHECK_EQUAL(0, ndx);

    sumf = q.maximum_float(1, nullptr, 0, -1, 2);
    CHECK_EQUAL(30000., sumf);
    q.maximum_float(1, nullptr, 0, -1, 2, &ndx);
    CHECK_EQUAL(1, ndx);

    sumf = q.maximum_float(1, nullptr, 0, -1);
    CHECK_EQUAL(40000., sumf);
    q.maximum_float(1, nullptr, 0, -1, -1, &ndx);
    CHECK_EQUAL(2, ndx);

    sumd = q.maximum_double(2, nullptr, 0, -1, 1);
    CHECK_EQUAL(10000., sumd);
    q.maximum_double(2, nullptr, 0, -1, 1, &ndx);
    CHECK_EQUAL(0, ndx);

    sumd = q.maximum_double(2, nullptr, 0, -1, 2);
    CHECK_EQUAL(30000., sumd);
    q.maximum_double(2, nullptr, 0, -1, 2, &ndx);
    CHECK_EQUAL(1, ndx);

    sumd = q.maximum_double(2, nullptr, 0, -1);
    CHECK_EQUAL(40000., sumd);
    q.maximum_double(2, nullptr, 0, -1, -1, &ndx);
    CHECK_EQUAL(2, ndx);

    // max, limited by 'end', but still having 'limit' specified
    sum = q.maximum_int(0, nullptr, 0, 1, 3);
    CHECK_EQUAL(10000, sum);
    q.maximum_int(0, nullptr, 0, 1, 3, &ndx);
    CHECK_EQUAL(0, ndx);

    sum = q.maximum_int(0, nullptr, 0, 2, 3);
    CHECK_EQUAL(30000, sum);
    q.maximum_int(0, nullptr, 0, 2, 3, &ndx);
    CHECK_EQUAL(1, ndx);

    sumf = q.maximum_float(1, nullptr, 0, 1, 3);
    CHECK_EQUAL(10000., sumf);
    q.maximum_float(1, nullptr, 0, 1, 3, &ndx);
    CHECK_EQUAL(0, ndx);

    sumf = q.maximum_float(1, nullptr, 0, 2, 3);
    CHECK_EQUAL(30000., sumf);
    q.maximum_float(1, nullptr, 0, 2, 3, &ndx);
    CHECK_EQUAL(1, ndx);

    sumd = q.maximum_double(2, nullptr, 0, 1, 3);
    CHECK_EQUAL(10000., sumd);
    q.maximum_double(2, nullptr, 0, 1, 3, &ndx);
    CHECK_EQUAL(0, ndx);

    sumd = q.maximum_double(2, nullptr, 0, 2, 3);
    CHECK_EQUAL(30000., sumd);
    q.maximum_double(2, nullptr, 0, 2, 3, &ndx);
    CHECK_EQUAL(1, ndx);


    // avg
    sumd = q.average_int(0, nullptr, 0, -1, 1);
    CHECK_EQUAL(10000, sumd);
    sumd = q.average_int(0, nullptr, 0, -1, 2);
    CHECK_EQUAL((10000 + 30000) / 2, sumd);

    sumd = q.average_float(1, nullptr, 0, -1, 1);
    CHECK_EQUAL(10000., sumd);
    sumd = q.average_float(1, nullptr, 0, -1, 2);
    CHECK_EQUAL((10000. + 30000.) / 2., sumd);


    // avg, limited by 'end', but still having 'limit' specified
    sumd = q.average_int(0, nullptr, 0, 1, 3);
    CHECK_EQUAL(10000, sumd);
    sumd = q.average_int(0, nullptr, 0, 2, 3);
    CHECK_EQUAL((10000 + 30000) / 2, sumd);

    sumd = q.average_float(1, nullptr, 0, 1, 3);
    CHECK_EQUAL(10000., sumd);
    sumd = q.average_float(1, nullptr, 0, 2, 3);
    CHECK_EQUAL((10000. + 30000.) / 2., sumd);

    // count
    size_t cnt = q.count(0, -1, 1);
    CHECK_EQUAL(1, cnt);
    cnt = q.count(0, -1, 2);
    CHECK_EQUAL(2, cnt);

    // count, limited by 'end', but still having 'limit' specified
    cnt = q.count(0, 1, 3);
    CHECK_EQUAL(1, cnt);
}


TEST(Query_StrIndexCrash)
{
    // Rasmus "8" index crash
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    for (int iter = 0; iter < 5; ++iter) {
        Group group;
        TableRef table = group.add_table("test");
        table->add_column(type_String, "first");

        size_t eights = 0;

        for (int i = 0; i < REALM_MAX_BPNODE_SIZE * 2; ++i) {
            int v = random.draw_int_mod(10);
            if (v == 8) {
                eights++;
            }
            char dst[100];
            memset(dst, 0, sizeof(dst));
            sprintf(dst, "%d", v);
            table->insert_empty_row(i);
            table->set_string(0, i, dst);
        }

        table->add_search_index(0);
        TableView v = table->where().equal(0, StringData("8")).find_all();
        CHECK_EQUAL(eights, v.size());

        v = table->where().equal(0, StringData("10")).find_all();

        v = table->where().equal(0, StringData("8")).find_all();
        CHECK_EQUAL(eights, v.size());
    }
}

TEST(Query_size)
{
    Group g;

    TableRef table1 = g.add_table("primary");
    TableRef table2 = g.add_table("secondary");
    TableRef table3 = g.add_table("top");

    table1->add_column(type_String, "strings");
    table1->add_column(type_Binary, "binaries", true);
    DescriptorRef subdesc;
    table1->add_column(type_Table, "intlist", false, &subdesc);
    subdesc->add_column(type_Int, "list", nullptr, true);
    table1->add_column_link(type_LinkList, "linklist", *table2);

    table2->add_column(type_Int, "integers");

    table3->add_column_link(type_Link, "link", *table1);
    table3->add_column_link(type_LinkList, "linklist", *table1);
    table3->add_empty_row(10);

    Columns<String> strings = table1->column<String>(0);
    Columns<Binary> binaries = table1->column<Binary>(1);
    Columns<SubTable> intlist = table1->column<SubTable>(2);
    Columns<LinkList> linklist = table1->column<LinkList>(3);

    table1->add_empty_row(10);
    table2->add_empty_row(10);

    for (size_t i = 0; i < 10; i++) {
        table2->set_int(0, i, i);
    }

    // Leave the last one null
    for (unsigned i = 0; i < 9; i++) {
        table3->set_link(0, i, i % 4);
    }

    for (unsigned i = 0; i < 10; i++) {
        auto lv = table3->get_linklist(1, i);
        for (unsigned j = 0; j < i % 5; j++) {
            lv->add(j);
        }
    }

    table1->set_string(0, 0, StringData("Hi"));
    table1->set_string(0, 1, StringData("world"));

    std::string bin1(100, 'a');
    std::string bin2(500, '5');
    table1->set_binary(1, 0, BinaryData(bin1));
    table1->set_binary(1, 1, BinaryData(bin2));

    auto set_list = [](TableRef subtable, const std::vector<int64_t>& value_list) {
        size_t sz = value_list.size();
        subtable->clear();
        subtable->add_empty_row(sz);
        for (size_t i = 0; i < sz; i++) {
            subtable->set_int(0, i, value_list[i]);
        }
    };
    set_list(table1->get_subtable(2, 0), std::vector<Int>({100, 200, 300, 400, 500}));
    set_list(table1->get_subtable(2, 1), std::vector<Int>({1, 2, 3}));
    set_list(table1->get_subtable(2, 2), std::vector<Int>({1, 2, 3, 4, 5}));
    set_list(table1->get_subtable(2, 3), std::vector<Int>({1, 2, 3, 4, 5, 6, 7, 8, 9}));

    auto set_links = [](LinkViewRef lv, const std::vector<int64_t>& value_list) {
        for (auto v : value_list) {
            lv->add(v);
        }
    };
    set_links(table1->get_linklist(3, 0), std::vector<Int>({0, 1, 2, 3, 4, 5}));
    set_links(table1->get_linklist(3, 1), std::vector<Int>({6, 7, 8, 9}));

    Query q;
    Query q1;
    size_t match;
    TableView tv;

    q = strings.size() == 5;
    q1 = table1->where().size_equal(0, 5);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q1.find();
    CHECK_EQUAL(1, match);

    // Check that the null values are handled correctly
    q = binaries.size() == realm::null();
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 8);
    CHECK_EQUAL(tv.get_source_ndx(0), 2);

    // Here the null values should not be included in the search
    q = binaries.size() < 500;
    q1 = table1->where().size_less(1, 500);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    tv = q1.find_all();
    CHECK_EQUAL(tv.size(), 1);

    q = intlist.size() > 3;
    q1 = table1->where().size_greater(2, 3);
    tv = q.find_all();
    CHECK_EQUAL(3, tv.size());
    tv = q1.find_all();
    CHECK_EQUAL(3, tv.size());
    q1 = table1->where().size_between(2, 3, 7);
    tv = q1.find_all();
    CHECK_EQUAL(3, tv.size());

    q = intlist.size() == 3;
    match = q.find();
    CHECK_EQUAL(1, match);

    q = linklist.size() != 6;
    q1 = table1->where().size_not_equal(3, 6);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q1.find();
    CHECK_EQUAL(1, match);
    q = linklist.size() == 4;
    match = q.find();
    CHECK_EQUAL(1, match);

    q = linklist.size() > strings.size();
    tv = q.find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    // Single links
    q = table3->link(0).column<SubTable>(2).size() == 5;
    tv = q.find_all();
    CHECK_EQUAL(5, tv.size());

    // Multiple links
    q = table3->link(1).column<SubTable>(2).size() == 3;
    tv = q.find_all();
    CHECK_EQUAL(6, tv.size());
}

TEST_TYPES(Query_StringIndexCommonPrefix, std::true_type, std::false_type)
{
    Group group;
    TableRef table = group.add_table("test");
    table->add_column(type_String, "first");
    table->add_search_index(0);
    if (TEST_TYPE::value == true) {
        bool force = true;
        table->optimize(force); // Make it a StringEnum column
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

        size_t start_row = table->size();
        size_t ins_pos = start_row;
        table->add_empty_row(6);
        table->set_string(0, ins_pos++, spb);
        table->set_string(0, ins_pos++, spc);
        table->set_string(0, ins_pos++, spc);
        table->set_string(0, ins_pos++, spe);
        table->set_string(0, ins_pos++, spe);
        table->set_string(0, ins_pos++, spe);

        TableView v = table->where().equal(0, spb).find_all();
        CHECK_EQUAL(v.size(), 1);
        CHECK_EQUAL(v.get(0).get_index(), start_row);

        v = table->where().equal(0, spc).find_all();
        CHECK_EQUAL(v.size(), 2);
        CHECK_EQUAL(v.get(0).get_index(), start_row + 1);
        CHECK_EQUAL(v.get(1).get_index(), start_row + 2);

        v = table->where().equal(0, spd).find_all();
        CHECK_EQUAL(v.size(), 0);

        v = table->where().equal(0, spe).find_all();
        CHECK_EQUAL(v.size(), 3);
        CHECK_EQUAL(v.get(0).get_index(), start_row + 3);
        CHECK_EQUAL(v.get(1).get_index(), start_row + 4);
        CHECK_EQUAL(v.get(2).get_index(), start_row + 5);
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

    std::vector<size_t> ints1;
    std::vector<size_t> ints2;
    std::vector<size_t> ints3;

    std::vector<size_t> floats;
    std::vector<size_t> doubles;

    Table table;
    table.add_column(type_Int, "first1");
    table.add_column(type_Int, "second1");

    table.add_column(type_Int, "first2");
    table.add_column(type_Int, "second2");

    table.add_column(type_Int, "first3");
    table.add_column(type_Int, "second3");

    table.add_column(type_Float, "third");
    table.add_column(type_Float, "fourth");
    table.add_column(type_Double, "fifth");
    table.add_column(type_Double, "sixth");

#ifdef REALM_DEBUG
    for (int i = 0; i < REALM_MAX_BPNODE_SIZE * 5; i++) {
#else
    for (int i = 0; i < 50000; i++) {
#endif
        table.add_empty_row();

        // Important thing to test is different bitwidths because we might use SSE and/or bithacks on 64-bit blocks

        // Both are bytes
        table.set_int(0, i, random.draw_int_mod(100));
        table.set_int(1, i, random.draw_int_mod(100));

        // Second column widest
        table.set_int(2, i, random.draw_int_mod(10));
        table.set_int(3, i, random.draw_int_mod(100));

        // First column widest
        table.set_int(4, i, random.draw_int_mod(100));
        table.set_int(5, i, random.draw_int_mod(10));

        table.set_float(6, i, float(random.draw_int_mod(10)));
        table.set_float(7, i, float(random.draw_int_mod(10)));

        table.set_double(8, i, double(random.draw_int_mod(10)));
        table.set_double(9, i, double(random.draw_int_mod(10)));

        if (table.get_int(0, i) == table.get_int(1, i))
            ints1.push_back(i);

        if (table.get_int(2, i) == table.get_int(3, i))
            ints2.push_back(i);

        if (table.get_int(4, i) == table.get_int(5, i))
            ints3.push_back(i);

        if (table.get_float(6, i) == table.get_float(7, i))
            floats.push_back(i);

        if (table.get_double(8, i) == table.get_double(9, i))
            doubles.push_back(i);
    }

    realm::TableView t1 = table.where().equal_int(size_t(0), size_t(1)).find_all();
    realm::TableView t2 = table.where().equal_int(size_t(2), size_t(3)).find_all();
    realm::TableView t3 = table.where().equal_int(size_t(4), size_t(5)).find_all();

    realm::TableView t4 = table.where().equal_float(size_t(6), size_t(7)).find_all();
    realm::TableView t5 = table.where().equal_double(size_t(8), size_t(9)).find_all();


    CHECK_EQUAL(ints1.size(), t1.size());
    for (size_t t = 0; t < ints1.size(); t++)
        CHECK_EQUAL(ints1[t], t1.get_source_ndx(t));

    CHECK_EQUAL(ints2.size(), t2.size());
    for (size_t t = 0; t < ints2.size(); t++)
        CHECK_EQUAL(ints2[t], t2.get_source_ndx(t));

    CHECK_EQUAL(ints3.size(), t3.size());
    for (size_t t = 0; t < ints3.size(); t++)
        CHECK_EQUAL(ints3[t], t3.get_source_ndx(t));

    CHECK_EQUAL(floats.size(), t4.size());
    for (size_t t = 0; t < floats.size(); t++)
        CHECK_EQUAL(floats[t], t4.get_source_ndx(t));

    CHECK_EQUAL(doubles.size(), t5.size());
    for (size_t t = 0; t < doubles.size(); t++)
        CHECK_EQUAL(doubles[t], t5.get_source_ndx(t));
}

TEST(Query_TwoColsVaryOperators)
{
    std::vector<size_t> ints1;
    std::vector<size_t> floats;
    std::vector<size_t> doubles;

    Table table;
    table.add_column(type_Int, "first1");
    table.add_column(type_Int, "second1");

    table.add_column(type_Float, "third");
    table.add_column(type_Float, "fourth");
    table.add_column(type_Double, "fifth");
    table.add_column(type_Double, "sixth");

    // row 0
    table.add_empty_row();
    table.set_int(0, 0, 5);
    table.set_int(1, 0, 10);
    table.set_float(2, 0, 5.0f);
    table.set_float(3, 0, 10.0f);
    table.set_double(4, 0, 5.0);
    table.set_double(5, 0, 10.0);

    // row 1
    table.add_empty_row();
    table.set_int(0, 1, 10);
    table.set_int(1, 1, 5);
    table.set_float(2, 1, 10.0f);
    table.set_float(3, 1, 5.0f);
    table.set_double(4, 1, 10.0);
    table.set_double(5, 1, 5.0);

    // row 2
    table.add_empty_row();
    table.set_int(0, 2, -10);
    table.set_int(1, 2, -5);
    table.set_float(2, 2, -10.0f);
    table.set_float(3, 2, -5.0f);
    table.set_double(4, 2, -10.0);
    table.set_double(5, 2, -5.0);


    CHECK_EQUAL(not_found, table.where().equal_int(size_t(0), size_t(1)).find());
    CHECK_EQUAL(0, table.where().not_equal_int(size_t(0), size_t(1)).find());
    CHECK_EQUAL(0, table.where().less_int(size_t(0), size_t(1)).find());
    CHECK_EQUAL(1, table.where().greater_int(size_t(0), size_t(1)).find());
    CHECK_EQUAL(1, table.where().greater_equal_int(size_t(0), size_t(1)).find());
    CHECK_EQUAL(0, table.where().less_equal_int(size_t(0), size_t(1)).find());

    CHECK_EQUAL(not_found, table.where().equal_float(size_t(2), size_t(3)).find());
    CHECK_EQUAL(0, table.where().not_equal_float(size_t(2), size_t(3)).find());
    CHECK_EQUAL(0, table.where().less_float(size_t(2), size_t(3)).find());
    CHECK_EQUAL(1, table.where().greater_float(size_t(2), size_t(3)).find());
    CHECK_EQUAL(1, table.where().greater_equal_float(size_t(2), size_t(3)).find());
    CHECK_EQUAL(0, table.where().less_equal_float(size_t(2), size_t(3)).find());

    CHECK_EQUAL(not_found, table.where().equal_double(size_t(4), size_t(5)).find());
    CHECK_EQUAL(0, table.where().not_equal_double(size_t(4), size_t(5)).find());
    CHECK_EQUAL(0, table.where().less_double(size_t(4), size_t(5)).find());
    CHECK_EQUAL(1, table.where().greater_double(size_t(4), size_t(5)).find());
    CHECK_EQUAL(1, table.where().greater_equal_double(size_t(4), size_t(5)).find());
    CHECK_EQUAL(0, table.where().less_equal_double(size_t(4), size_t(5)).find());
}


TEST(Query_TwoCols0)
{
    Table table;
    table.add_column(type_Int, "first1");
    table.add_column(type_Int, "second1");


    for (int i = 0; i < 50; i++) {
        table.add_empty_row();
        table.set_int(0, i, 0);
        table.set_int(1, i, 0);
    }

    realm::TableView t1 = table.where().equal_int(size_t(0), size_t(1)).find_all();
    CHECK_EQUAL(50, t1.size());

    realm::TableView t2 = table.where().less_int(size_t(0), size_t(1)).find_all();
    CHECK_EQUAL(0, t2.size());
}

TEST(Query_TwoSameCols)
{
    Table table;
    table.add_column(type_Bool, "first1");
    table.add_column(type_Bool, "first2");
    table.add_column(type_OldDateTime, "second1");
    table.add_column(type_OldDateTime, "second2");
    table.add_column(type_String, "third1");
    table.add_column(type_String, "third2");

    table.add_empty_row();
    table.set_bool(0, 0, false);
    table.set_bool(1, 0, true);
    table.set_olddatetime(2, 0, OldDateTime(0));
    table.set_olddatetime(3, 0, OldDateTime(1));
    table.set_string(4, 0, StringData("a"));
    table.set_string(5, 0, StringData("b"));

    table.add_empty_row();
    table.set_bool(0, 1, true);
    table.set_bool(1, 1, true);
    table.set_olddatetime(2, 1, OldDateTime(1));
    table.set_olddatetime(3, 1, OldDateTime(1));
    table.set_string(4, 1, StringData("b"));
    table.set_string(5, 1, StringData("b"));

    table.add_empty_row();
    table.set_bool(0, 2, false);
    table.set_bool(1, 2, true);
    table.set_olddatetime(2, 2, OldDateTime(0));
    table.set_olddatetime(3, 2, OldDateTime(1));
    table.set_string(4, 2, StringData("a"));
    table.set_string(5, 2, StringData("b"));

    Query q1 = table.column<Bool>(0) == table.column<Bool>(1);
    Query q2 = table.column<OldDateTime>(2) == table.column<OldDateTime>(3);
    Query q3 = table.column<String>(4) == table.column<String>(5);

    CHECK_EQUAL(1, q1.find());
    CHECK_EQUAL(1, q2.find());
    CHECK_EQUAL(1, q3.find());
    CHECK_EQUAL(1, q1.count());
    CHECK_EQUAL(1, q2.count());
    CHECK_EQUAL(1, q3.count());

    Query q4 = table.column<Bool>(0) != table.column<Bool>(1);
    Query q5 = table.column<OldDateTime>(2) != table.column<OldDateTime>(3);
    Query q6 = table.column<String>(4) != table.column<String>(5);

    CHECK_EQUAL(0, q5.find());
    CHECK_EQUAL(0, q5.find());
    CHECK_EQUAL(0, q6.find());
    CHECK_EQUAL(2, q5.count());
    CHECK_EQUAL(2, q5.count());
    CHECK_EQUAL(2, q6.count());
}

TEST(Query_DateTest)
{
    Table table;
    table.add_column(type_OldDateTime, "second1");

    for (int i = 1; i < 10; i++) {
        table.add_empty_row();
        table.set_olddatetime(0, i - 1, OldDateTime(i * 1000));
    }

    Query q = table.where().equal_olddatetime(0, OldDateTime(5000));
    CHECK_EQUAL(1, q.count());
    TableView tv = q.find_all();
    CHECK_EQUAL(1, tv.size());
}

TEST(Query_TwoColsNoRows)
{
    Table table;
    table.add_column(type_Int, "first1");
    table.add_column(type_Int, "second1");

    CHECK_EQUAL(not_found, table.where().equal_int(size_t(0), size_t(1)).find());
    CHECK_EQUAL(not_found, table.where().not_equal_int(size_t(0), size_t(1)).find());
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
        tt.add_column(type_String, "1");
        tt.add_column(type_String, "2");
        tt.add_column(type_Int, "3");

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

            tt.add_empty_row();

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

            tt[row].set_string(0, first);
            tt[row].set_string(1, second);
            tt[row].set_int(2, third);

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

            if (t == 1)
                tt.optimize();
            else if (t == 2)
                tt.add_search_index(0);
            else if (t == 3)
                tt.add_search_index(1);


            v = tt.where().equal(0, "A").equal(1, "A").equal(2, 1).find_all(start, end, limit);
            CHECK_EQUAL(res1, v.size());

            v = tt.where().equal(1, "A").equal(0, "A").equal(2, 1).find_all(start, end, limit);
            CHECK_EQUAL(res1, v.size());

            v = tt.where().equal(2, 1).equal(1, "A").equal(0, "A").find_all(start, end, limit);
            CHECK_EQUAL(res1, v.size());

            v = tt.where().group().equal(0, "A").Or().equal(1, "A").end_group().equal(2, 1).find_all(
                start, end, limit);
            CHECK_EQUAL(res2, v.size());

            v = tt.where().equal(0, "A").group().equal(1, "A").Or().equal(2, 1).end_group().find_all(
                start, end, limit);
            CHECK_EQUAL(res3, v.size());

            Query q = tt.where().group().equal(0, "A").Or().equal(2, 1).end_group().equal(1, "A");
            v = q.find_all(start, end, limit);
            CHECK_EQUAL(res4, v.size());

            v = tt.where().group().equal(0, "A").Or().equal(2, 1).end_group().equal(1, "A").find_all(
                start, end, limit);
            CHECK_EQUAL(res4, v.size());

            v = tt.where().equal(0, "A").Or().equal(1, "A").Or().equal(2, 1).find_all(start, end, limit);
            CHECK_EQUAL(res5, v.size());

            v = tt.where().not_equal(0, "A").equal(1, "A").equal(2, 1).find_all(start, end, limit);
            CHECK_EQUAL(res6, v.size());

            v = tt.where()
                    .not_equal(0, "longlonglonglonglonglonglong A")
                    .equal(1, "A")
                    .equal(2, 1)
                    .find_all(start, end, limit);
            CHECK_EQUAL(res7, v.size());

            v = tt.where()
                    .not_equal(0, "longlonglonglonglonglonglong A")
                    .equal(1, "A")
                    .equal(2, 2)
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
        TestTable oti;
        oti.add_column(type_Int, "1");

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

            add(oti, v);
        }

        TableView v = oti.where().not_equal(0, 0).find_all(lbound, ubound, limit);
        size_t cnt2 = oti.where(&v).equal(0, 1).count();

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
        TestTable ttt;
        ttt.add_column(type_Int, "1");
        ttt.add_column(type_String, "2");

        std::vector<size_t> vec;
        size_t row = 0;

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
                        add(ttt, 0, longstrings ? "AAAAAAAAAAAAAAAAAAAAAAAA" : "AA");
                        if (!longstrings) {
                            n++;
                            vec.push_back(row);
                        }
                    }
                    else {
                        add(ttt, 0, "BB");
                    }
                }
                else {
                    if (random.chance(1, f2)) {
                        add(ttt, 1, "AA");
                    }
                    else {
                        add(ttt, 1, "BB");
                    }
                }
                ++row;
            }
        }

        TableView v;

        // Both linear scans
        v = ttt.where().equal(1, "AA").equal(0, 0).find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_source_ndx(t));
        v.clear();
        vec.clear();

        v = ttt.where().equal(0, 0).equal(1, "AA").find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_source_ndx(t));
        v.clear();
        vec.clear();

        ttt.optimize();

        // Linear scan over enum, plus linear integer column scan
        v = ttt.where().equal(1, "AA").equal(0, 0).find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_source_ndx(t));
        v.clear();
        vec.clear();

        v = ttt.where().equal(0, 0).equal(1, "AA").find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_source_ndx(t));
        v.clear();
        vec.clear();

        ttt.add_search_index(1);

        // Index lookup, plus linear integer column scan
        v = ttt.where().equal(1, "AA").equal(0, 0).find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_source_ndx(t));
        v.clear();
        vec.clear();

        v = ttt.where().equal(0, 0).equal(1, "AA").find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_source_ndx(t));
        v.clear();
        vec.clear();
    }
}


TEST(Query_StrIndex2)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    int64_t s;

    for (int i = 0; i < 100; ++i) {
        add(ttt, 1, "AA");
    }
    add(ttt, 1, "BB");
    ttt.add_search_index(1);

    s = ttt.where().equal(1, "AA").count();
    CHECK_EQUAL(100, s);

    s = ttt.where().equal(1, "BB").count();
    CHECK_EQUAL(1, s);

    s = ttt.where().equal(1, "CC").count();
    CHECK_EQUAL(0, s);
}


TEST(Query_StrEnum)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    int aa;
    int64_t s;

    for (int i = 0; i < 100; ++i) {
        ttt.clear();
        aa = 0;
        for (size_t t = 0; t < REALM_MAX_BPNODE_SIZE * 2; ++t) {
            if (random.chance(1, 3)) {
                add(ttt, 1, "AA");
                ++aa;
            }
            else {
                add(ttt, 1, "BB");
            }
        }
        ttt.optimize();
        s = ttt.where().equal(1, "AA").count();
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
        TestTable ttt;
        ttt.add_column(type_Int, "1");
        ttt.add_column(type_String, "2");

        aa = 0;
        for (size_t t = 0; t < iterb; t++) {
            if (random.chance(1, 3)) {
                add(ttt, 1, "AA");
                aa++;
            }
            else {
                add(ttt, 1, "BB");
            }
        }

        s = ttt.where().equal(1, "AA").count();
        CHECK_EQUAL(aa, s);

        ttt.optimize();
        s = ttt.where().equal(1, "AA").count();
        CHECK_EQUAL(aa, s);

        ttt.add_search_index(1);
        s = ttt.where().equal(1, "AA").count();
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
        t->add_column(type_String, "1");
    	t->add_column(type_String, "2");
	    t->add_column(type_String, "3");
	    t->add_column(type_Int, "4");
	    t->add_column(type_Int, "5");

        for (size_t i = 0; i < 100; ++i) {
            int64_t r1 = random.draw_int_mod(100);
            int64_t r2 = random.draw_int_mod(100);

            add(t, "10", "US", "1.0", r1, r2);
        }
        t->optimize();
        g.write(path);
    }

    Group g(path);
    TableRef t = g.get_table("firstevents");

    Query q = t->where().equal(1, "US");

    size_t c1 = 0;
    for (size_t i = 0; i < 100; ++i)
        c1 += t.get()->count_string(1, "US");

    size_t c2 = 0;
    for (size_t i = 0; i < 100; ++i)
        c2 += q.count();

    CHECK_EQUAL(c1, t->size() * 100);
    CHECK_EQUAL(c1, c2);
}


TEST(Query_Float3)
{
    TestTable t;
    t.add_column(type_Float, "1");
    t.add_column(type_Double, "2");
    t.add_column(type_Int, "3");

    add(t, float(1.1), double(2.1), 1);
    add(t, float(1.2), double(2.2), 2);
    add(t, float(1.3), double(2.3), 3);
    add(t, float(1.4), double(2.4), 4); // match
    add(t, float(1.5), double(2.5), 5); // match
    add(t, float(1.6), double(2.6), 6); // match
    add(t, float(1.7), double(2.7), 7);
    add(t, float(1.8), double(2.8), 8);
    add(t, float(1.9), double(2.9), 9);

    Query q1 = t.where().greater(0, 1.35f).less(1, 2.65);
    int64_t a1 = q1.sum_int(2);
    CHECK_EQUAL(15, a1);

    Query q2 = t.where().less(1, 2.65).greater(0, 1.35f);
    int64_t a2 = q2.sum_int(2);
    CHECK_EQUAL(15, a2);

    Query q3 = t.where().less(1, 2.65).greater(0, 1.35f);
    double a3 = q3.sum_float(0);
    double sum3 = double(1.4f) + double(1.5f) + double(1.6f);
    CHECK_EQUAL(sum3, a3);

    Query q4 = t.where().greater(0, 1.35f).less(1, 2.65);
    double a4 = q4.sum_float(0);
    CHECK_EQUAL(sum3, a4);

    Query q5 = t.where().greater_equal(2, 4).less(1, 2.65);
    double a5 = q5.sum_float(0);
    CHECK_EQUAL(sum3, a5);

    Query q6 = t.where().less(1, 2.65).greater_equal(2, 4);
    double a6 = q6.sum_float(0);
    CHECK_EQUAL(sum3, a6);

    Query q7 = t.where().greater(2, 3).less(2, 7);
    int64_t a7 = q7.sum_int(2);
    CHECK_EQUAL(15, a7);
    Query q8 = t.where().greater(2, 3).less(2, 7);
    int64_t a8 = q8.sum_int(2);
    CHECK_EQUAL(15, a8);
}

TEST(Query_Float3_where)
{
    // Sum on query on tableview
    TestTable t;
    t.add_column(type_Float, "1");
    t.add_column(type_Double, "2");
    t.add_column(type_Int, "3");

    add(t, float(1.1), double(2.1), 1);
    add(t, float(1.2), double(2.2), 2);
    add(t, float(1.3), double(2.3), 3);
    add(t, float(1.4), double(2.4), 4); // match
    add(t, float(1.5), double(2.5), 5); // match
    add(t, float(1.6), double(2.6), 6); // match
    add(t, float(1.7), double(2.7), 7);
    add(t, float(1.8), double(2.8), 8);
    add(t, float(1.9), double(2.9), 9);

    TableView v = t.where().find_all();

    Query q1 = t.where(&v).greater(0, 1.35f).less(1, 2.65);
    int64_t a1 = q1.sum_int(2);
    CHECK_EQUAL(15, a1);

    Query q2 = t.where(&v).less(1, 2.65).greater(0, 1.35f);
    int64_t a2 = q2.sum_int(2);
    CHECK_EQUAL(15, a2);

    Query q3 = t.where(&v).less(1, 2.65).greater(0, 1.35f);
    double a3 = q3.sum_float(0);
    double sum3 = double(1.4f) + double(1.5f) + double(1.6f);
    CHECK_EQUAL(sum3, a3);

    Query q4 = t.where(&v).greater(0, 1.35f).less(1, 2.65);
    double a4 = q4.sum_float(0);
    CHECK_EQUAL(sum3, a4);

    Query q5 = t.where(&v).greater_equal(2, 4).less(1, 2.65);
    double a5 = q5.sum_float(0);
    CHECK_EQUAL(sum3, a5);

    Query q6 = t.where(&v).less(1, 2.65).greater_equal(2, 4);
    double a6 = q6.sum_float(0);
    CHECK_EQUAL(sum3, a6);

    Query q7 = t.where(&v).greater(2, 3).less(2, 7);
    int64_t a7 = q7.sum_int(2);
    CHECK_EQUAL(15, a7);
    Query q8 = t.where(&v).greater(2, 3).less(2, 7);
    int64_t a8 = q8.sum_int(2);
    CHECK_EQUAL(15, a8);
}

TEST(Query_TableViewSum)
{
    TestTable ttt;

    ttt.add_column(type_Float, "1");
    ttt.add_column(type_Double, "2");
    ttt.add_column(type_Int, "3");

    add(ttt, 1.0f, 1.0, 1);
    add(ttt, 2.0f, 2.0, 2);
    add(ttt, 3.0f, 3.0, 3);
    add(ttt, 4.0f, 4.0, 4);
    add(ttt, 5.0f, 5.0, 5);
    add(ttt, 6.0f, 6.0, 6);
    add(ttt, 7.0f, 7.0, 7);
    add(ttt, 8.0f, 8.0, 8);
    add(ttt, 9.0f, 9.0, 9);
    add(ttt, 10.0f, 10.0, 10);

    Query q1 = ttt.where().between(2, 5, 9);
    TableView tv1 = q1.find_all();
    int64_t s = tv1.sum_int(2);
    CHECK_EQUAL(5 + 6 + 7 + 8 + 9, s);
}


TEST(Query_JavaMinimumCrash)
{
    // Test that triggers a bug that was discovered through Java intnerface and has been fixed
    TestTable ttt;

    ttt.add_column(type_String, "1");
	ttt.add_column(type_String, "2");
	ttt.add_column(type_Int, "3");

    add(ttt, "Joe", "John", 1);
    add(ttt, "Jane", "Doe", 2);
    add(ttt, "Bob", "Hanson", 3);

    Query q1 = ttt.where().equal(0, "Joe").Or().equal(0, "Bob");
    int64_t m = q1.minimum_int(2);
    CHECK_EQUAL(1, m);
}


TEST(Query_Float4)
{
    TestTable t;

    t.add_column(type_Float, "1");
    t.add_column(type_Double, "2");
    t.add_column(type_Int, "3");

    add(t, std::numeric_limits<float>::max(), std::numeric_limits<double>::max(), 11111);
    add(t, std::numeric_limits<float>::infinity(), std::numeric_limits<double>::infinity(), 11111);
    add(t, 12345.0f, 12345.0, 11111);

    Query q1 = t.where();
    float a1 = q1.maximum_float(0);
    double a2 = q1.maximum_double(1);
    CHECK_EQUAL(std::numeric_limits<float>::infinity(), a1);
    CHECK_EQUAL(std::numeric_limits<double>::infinity(), a2);


    Query q2 = t.where();
    float a3 = q1.minimum_float(0);
    double a4 = q1.minimum_double(1);
    CHECK_EQUAL(12345.0, a3);
    CHECK_EQUAL(12345.0, a4);
}

TEST(Query_Float)
{
    TestTable t;
    t.add_column(type_Float, "1");
    t.add_column(type_Double, "2");    

    add(t, 1.10f, 2.20);
    add(t, 1.13f, 2.21);
    add(t, 1.13f, 2.22);
    add(t, 1.10f, 2.20);
    add(t, 1.20f, 3.20);

    // Test find_all()
    TableView v = t.where().equal(0, 1.13f).find_all();
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(1.13f, v[0].get_float(0));
    CHECK_EQUAL(1.13f, v[1].get_float(0));

    TableView v2 = t.where().equal(1, 3.2).find_all();
    CHECK_EQUAL(1, v2.size());
    CHECK_EQUAL(3.2, v2[0].get_double(1));

    // Test operators (and count)
    CHECK_EQUAL(2, t.where().equal(0, 1.13f).count());
    CHECK_EQUAL(3, t.where().not_equal(0, 1.13f).count());
    CHECK_EQUAL(3, t.where().greater(0, 1.1f).count());
    CHECK_EQUAL(3, t.where().greater_equal(0, 1.13f).count());
    CHECK_EQUAL(4, t.where().less_equal(0, 1.13f).count());
    CHECK_EQUAL(2, t.where().less(0, 1.13f).count());
    CHECK_EQUAL(3, t.where().between(0, 1.13f, 1.2f).count());

    CHECK_EQUAL(2, t.where().equal(1, 2.20).count());
    CHECK_EQUAL(3, t.where().not_equal(1, 2.20).count());
    CHECK_EQUAL(2, t.where().greater(1, 2.21).count());
    CHECK_EQUAL(3, t.where().greater_equal(1, 2.21).count());
    CHECK_EQUAL(4, t.where().less_equal(1, 2.22).count());
    CHECK_EQUAL(3, t.where().less(1, 2.22).count());
    CHECK_EQUAL(4, t.where().between(1, 2.20, 2.22).count());

    double epsilon = std::numeric_limits<double>::epsilon();

    // ------ Test sum()
    // ... NO conditions
    double sum1_d = 2.20 + 2.21 + 2.22 + 2.20 + 3.20;
    CHECK_APPROXIMATELY_EQUAL(sum1_d, t.where().sum_double(1), 10 * epsilon);

    // Note: sum of float is calculated by having a double aggregate to where each float is added
    // (thereby getting casted to double).
    double sum1_f = double(1.10f) + double(1.13f) + double(1.13f) + double(1.10f) + double(1.20f);
    double res = t.where().sum_float(0);
    CHECK_APPROXIMATELY_EQUAL(sum1_f, res, 10 * epsilon);

    // ... with conditions
    double sum2_f = double(1.13f) + double(1.20f);
    double sum2_d = 2.21 + 3.20;
    Query q2 = t.where().between(0, 1.13f, 1.20f).not_equal(1, 2.22);
    CHECK_APPROXIMATELY_EQUAL(sum2_f, q2.sum_float(0), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum2_d, q2.sum_double(1), 10 * epsilon);

    // ------ Test average()

    // ... NO conditions
    CHECK_APPROXIMATELY_EQUAL(sum1_f / 5, t.where().average_float(0), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum1_d / 5, t.where().average_double(1), 10 * epsilon);
    // ... with conditions
    CHECK_APPROXIMATELY_EQUAL(sum2_f / 2, q2.average_float(0), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum2_d / 2, q2.average_double(1), 10 * epsilon);

    // -------- Test minimum(), maximum()

    size_t ndx = not_found;

    // ... NO conditions
    CHECK_EQUAL(1.20f, t.where().maximum_float(0));
    t.where().maximum_float(0, nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(4, ndx);

    CHECK_EQUAL(1.10f, t.where().minimum_float(0));
    t.where().minimum_float(0, nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(3.20, t.where().maximum_double(1));
    CHECK_EQUAL(3.20, t.where().maximum_double(1, nullptr, 0, not_found, not_found, &ndx));

    CHECK_EQUAL(2.20, t.where().minimum_double(1));
    t.where().minimum_double(1, nullptr, 0, not_found, not_found, &ndx);

    // ... with conditions
    CHECK_EQUAL(1.20f, q2.maximum_float(0));
    q2.maximum_float(0, nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(4, ndx);

    CHECK_EQUAL(1.13f, q2.minimum_float(0));
    q2.minimum_float(0, nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(1, ndx);

    CHECK_EQUAL(3.20, q2.maximum_double(1));
    q2.maximum_double(1, nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(4, ndx);

    CHECK_EQUAL(2.21, q2.minimum_double(1));
    q2.minimum_double(1, nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(1, ndx);

    size_t count = 0;
    // ... NO conditions
    CHECK_EQUAL(1.20f, t.where().maximum_float(0, &count));
    CHECK_EQUAL(5, count);
    t.where().maximum_float(0, &count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(4, ndx);

    CHECK_EQUAL(1.10f, t.where().minimum_float(0, &count));
    CHECK_EQUAL(5, count);
    t.where().minimum_float(0, &count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(3.20, t.where().maximum_double(1, &count));
    CHECK_EQUAL(5, count);
    t.where().maximum_double(1, &count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(4, ndx);

    CHECK_EQUAL(2.20, t.where().minimum_double(1, &count));
    CHECK_EQUAL(5, count);
    t.where().minimum_double(1, &count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(0, ndx);

    // ... with conditions
    CHECK_EQUAL(1.20f, q2.maximum_float(0, &count));
    CHECK_EQUAL(2, count);
    q2.maximum_float(0, &count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(4, ndx);

    CHECK_EQUAL(1.13f, q2.minimum_float(0, &count));
    CHECK_EQUAL(2, count);
    q2.minimum_float(0, &count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(1, ndx);

    CHECK_EQUAL(3.20, q2.maximum_double(1, &count));
    CHECK_EQUAL(2, count);
    q2.maximum_double(1, &count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(4, ndx);

    CHECK_EQUAL(2.21, q2.minimum_double(1, &count));
    CHECK_EQUAL(2, count);
    q2.minimum_double(1, &count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(1, ndx);
}


TEST(Query_DoubleCoordinates)
{
    Group group;
    TableRef table = group.add_table("test");

    table->add_column(type_Double, "name");
    table->add_column(type_Double, "age");

    size_t expected = 0;

    for (size_t t = 0; t < 100000; t++) {
        table->add_empty_row(1);
        table->set_double(0, t, double((t * 12345) % 1000));
        table->set_double(1, t, double((t * 12345) % 1000));

        if (table->get_double(0, t) >= 100. && table->get_double(0, t) <= 110. && table->get_double(1, t) >= 100. &&
            table->get_double(1, t) <= 110.) {
            expected++;
        }
    }

    // This unit test can be used as benchmark. Just enable this for loop
    //    for (size_t t = 0; t < 1000; t++) {
    Query q = table->column<double>(0) >= 100. && table->column<double>(0) <= 110. &&
              table->column<double>(1) >= 100. && table->column<double>(1) <= 110.;

    size_t c = q.count();
    REALM_ASSERT(c == expected);
    static_cast<void>(c);

    //    }
}


TEST(Query_StrIndexedEnum)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    for (size_t t = 0; t < 10; t++) {
        add(ttt, 1, "a");
        add(ttt, 4, "b");
        add(ttt, 7, "c");
        add(ttt, 10, "a");
        add(ttt, 1, "b");
        add(ttt, 4, "c");
    }

    ttt.optimize();

    ttt.add_search_index(1);

    int64_t s = ttt.where().equal(1, "a").sum_int(0);
    CHECK_EQUAL(10 * 11, s);

    s = ttt.where().equal(1, "a").equal(0, 10).sum_int(0);
    CHECK_EQUAL(100, s);

    s = ttt.where().equal(0, 10).equal(1, "a").sum_int(0);
    CHECK_EQUAL(100, s);

    TableView tv = ttt.where().equal(1, "a").find_all();
    CHECK_EQUAL(10 * 2, tv.size());
}


TEST(Query_StrIndexedNonEnum)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    for (size_t t = 0; t < 10; t++) {
        add(ttt, 1, "a");
        add(ttt, 4, "b");
        add(ttt, 7, "c");
        add(ttt, 10, "a");
        add(ttt, 1, "b");
        add(ttt, 4, "c");
    }

    ttt.add_search_index(1);

    int64_t s = ttt.where().equal(1, "a").sum_int(0);
    CHECK_EQUAL(10 * 11, s);

    s = ttt.where().equal(1, "a").equal(0, 10).sum_int(0);
    CHECK_EQUAL(100, s);

    s = ttt.where().equal(0, 10).equal(1, "a").sum_int(0);
    CHECK_EQUAL(100, s);

    TableView tv = ttt.where().equal(1, "a").find_all();
    CHECK_EQUAL(10 * 2, tv.size());
}

TEST(Query_FindAllContains2_2)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 0, "foo");
    add(ttt, 1, "foobar");
    add(ttt, 2, "hellofoobar");
    add(ttt, 3, "foO");
    add(ttt, 4, "foObar");
    add(ttt, 5, "hellofoObar");
    add(ttt, 6, "hellofo");
    add(ttt, 7, "fobar");
    add(ttt, 8, "oobar");

    // FIXME: UTF-8 case handling is only implemented on msw for now
    Query q1 = ttt.where().contains(1, "foO", false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(6, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(1, tv1.get_source_ndx(1));
    CHECK_EQUAL(2, tv1.get_source_ndx(2));
    CHECK_EQUAL(3, tv1.get_source_ndx(3));
    CHECK_EQUAL(4, tv1.get_source_ndx(4));
    CHECK_EQUAL(5, tv1.get_source_ndx(5));
    Query q2 = ttt.where().contains(1, "foO", true);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(3, tv2.size());
    CHECK_EQUAL(3, tv2.get_source_ndx(0));
    CHECK_EQUAL(4, tv2.get_source_ndx(1));
    CHECK_EQUAL(5, tv2.get_source_ndx(2));
}

TEST(Query_SumNewAggregates)
{
    // test the new ACTION_FIND_PATTERN() method in array
	Table t;
	t.add_column(type_Int, "1");
    for (size_t i = 0; i < 1000; i++) {
		t.add_empty_row();
        t.set_int(0, t.size() - 1, 1);

		t.add_empty_row();
        t.set_int(0, t.size() - 1, 2);

		t.add_empty_row();
        t.set_int(0, t.size() - 1, 4);

		t.add_empty_row();
        t.set_int(0, t.size() - 1, 6);
    }
    size_t c = t.where().equal(0, 2).count();
    CHECK_EQUAL(1000, c);

    c = t.where().greater(0, 2).count();
    CHECK_EQUAL(2000, c);
}


TEST(Query_SumMinMaxAvgForeignCol)
{
	Table t;
	t.add_column(type_Int, "1");
	t.add_column(type_Int, "2");
	t.add_empty_row(4);
	t.set_int(0, 0, 1);
	t.set_int(1, 0, 10);

	t.set_int(0, 1, 2);
	t.set_int(1, 1, 20);

	t.set_int(0, 2, 2);
	t.set_int(1, 2, 30);

	t.set_int(0, 3, 4);
	t.set_int(1, 3, 40);

    CHECK_EQUAL(50, t.where().equal(0, 2).sum_int(1));
}


TEST(Query_AggregateSingleCond)
{
	Table t;
	t.add_column(type_Int, "1");

	t.add_empty_row();
    t.set_int(0, t.size() - 1, 1);

	t.add_empty_row();
    t.set_int(0, t.size() - 1, 2);

	t.add_empty_row();
    t.set_int(0, t.size() - 1, 2);

	t.add_empty_row();
    t.set_int(0, t.size() - 1, 3);

	t.add_empty_row();
    t.set_int(0, t.size() - 1, 3);

	t.add_empty_row();
    t.set_int(0, t.size() - 1, 4);

    int64_t s = t.where().equal(0, 2).sum_int(0);
    CHECK_EQUAL(4, s);

    s = t.where().greater(0, 2).sum_int(0);
    CHECK_EQUAL(10, s);

    s = t.where().less(0, 3).sum_int(0);
    CHECK_EQUAL(5, s);

    s = t.where().not_equal(0, 3).sum_int(0);
    CHECK_EQUAL(9, s);
}


TEST(Query_FindAllRange1)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 4, "a");
    add(ttt, 7, "a");
    add(ttt, 10, "a");
    add(ttt, 1, "a");
    add(ttt, 4, "a");
    add(ttt, 7, "a");
    add(ttt, 10, "a");
    add(ttt, 1, "a");
    add(ttt, 4, "a");
    add(ttt, 7, "a");
    add(ttt, 10, "a");

    Query q1 = ttt.where().equal(1, "a");
    TableView tv1 = q1.find_all(4, 10);
    CHECK_EQUAL(6, tv1.size());
}


TEST(Query_FindAllRangeOrMonkey2)
{
    const size_t ROWS = 20;
    const size_t ITER = 100;

    Random random(random_int<unsigned long>()); // Seed from slow global generator

    for (size_t u = 0; u < ITER; u++) {
        TestTable tit;
        tit.add_column(type_Int, "1");
        tit.add_column(type_Int, "2");

        ArrayInteger a(Allocator::get_default());
        a.create(Array::type_Normal);
        size_t start = random.draw_int_max(ROWS);
        size_t end = start + random.draw_int_max(ROWS);

        if (end > ROWS)
            end = ROWS;

        for (size_t t = 0; t < ROWS; t++) {
            int64_t r1 = random.draw_int_mod(10);
            int64_t r2 = random.draw_int_mod(10);
            add(tit, r1, r2);
        }

        Query q1 = tit.where().group().equal(0, 3).Or().equal(0, 7).end_group().greater(1, 5);
        TableView tv1 = q1.find_all(start, end);

        for (size_t t = start; t < end; t++) {
            if ((tit[t].get_int(0) == 3 || tit[t].get_int(0) == 7) && tit[t].get_int(1) > 5)
                a.add(t);
        }
        size_t s1 = a.size();
        size_t s2 = tv1.size();

        CHECK_EQUAL(s1, s2);
        for (size_t t = 0; t < a.size(); t++) {
            size_t i1 = to_size_t(a.get(t));
            size_t i2 = tv1.get_source_ndx(t);
            CHECK_EQUAL(i1, i2);
        }
        a.destroy();
    }
}


TEST(Query_FindAllRangeOr)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "b");
    add(ttt, 2, "a"); //// match
    add(ttt, 3, "b"); //
    add(ttt, 1, "a"); //// match
    add(ttt, 2, "b"); //// match
    add(ttt, 3, "a");
    add(ttt, 1, "b");
    add(ttt, 2, "a"); //// match
    add(ttt, 3, "b"); //

    Query q1 = ttt.where().group().greater(0, 1).Or().equal(1, "a").end_group().less(0, 3);
    TableView tv1 = q1.find_all(1, 8);
    CHECK_EQUAL(4, tv1.size());

    TableView  tv2 = q1.find_all(2, 8);
    CHECK_EQUAL(3, tv2.size());

    TableView  tv3 = q1.find_all(1, 7);
    CHECK_EQUAL(3, tv3.size());
}


TEST(Query_SimpleStr)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "X");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 4, "a");
    add(ttt, 5, "X");
    add(ttt, 6, "X");
    Query q = ttt.where().equal(1, "X");
    size_t c = q.count();

    CHECK_EQUAL(4, c);
}


TEST(Query_Delete)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "X");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 4, "a");
    add(ttt, 5, "X");
    add(ttt, 6, "X");

    Query q = ttt.where().equal(1, "X");
    size_t r = q.remove();

    CHECK_EQUAL(4, r);
    CHECK_EQUAL(2, ttt.size());
    CHECK_EQUAL(2, ttt[0].get_int(0));
    CHECK_EQUAL(4, ttt[1].get_int(0));

    // test remove of all
    ttt.clear();
    add(ttt, 1, "X");
    add(ttt, 2, "X");
    add(ttt, 3, "X");
    Query q2 = ttt.where().equal(1, "X");
    r = q2.remove();
    CHECK_EQUAL(3, r);
    CHECK_EQUAL(0, ttt.size());
}


TEST(Query_Simple)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");

    Query q1 = ttt.where().equal(0, 2);

    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
}

TEST(Query_Not2)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");

    Query q1 = ttt.where().Not().equal(1, "a");

    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(2, tv1.get_source_ndx(0));
}

TEST(Query_SimpleBugDetect)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");

    Query q1 = ttt.where();

    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));

    TableView resView = tv1.find_all_string(1, "Foo");

    // This previously crashed:
    // TableView resView = TableView(tv1);
    // tv1.find_all(resView, 1, "Foo");
}


TEST(Query_Subtable)
{
    Group group;
    TableRef table = group.add_table("test");

    // Create specification with sub-table
    DescriptorRef sub_1;
    table->add_column(type_Int, "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table, "third", &sub_1);
    sub_1->add_column(type_Int, "sub_first");
    sub_1->add_column(type_String, "sub_second");
    sub_1.reset();

    CHECK_EQUAL(3, table->get_column_count());

    // Main table
    table->insert_empty_row(0);
    table->set_int(0, 0, 111);
    table->set_string(1, 0, "this");

    table->insert_empty_row(1);
    table->set_int(0, 1, 222);
    table->set_string(1, 1, "is");

    table->insert_empty_row(2);
    table->set_int(0, 2, 333);
    table->set_string(1, 2, "a test");

    table->insert_empty_row(3);
    table->set_int(0, 3, 444);
    table->set_string(1, 3, "of queries");


    // Sub tables
    TableRef subtable = table->get_subtable(2, 0);
    subtable->insert_empty_row(0);
    subtable->set_int(0, 0, 11);
    subtable->set_string(1, 0, "a");

    subtable = table->get_subtable(2, 1);
    subtable->insert_empty_row(0);
    subtable->set_int(0, 0, 22);
    subtable->set_string(1, 0, "b");
    subtable->insert_empty_row(1);
    subtable->set_int(0, 1, 33);
    subtable->set_string(1, 1, "c");

    // Intentionally have empty (degenerate) subtable at 2,2

    subtable = table->get_subtable(2, 3);
    subtable->insert_empty_row(0);
    subtable->set_int(0, 0, 55);
    subtable->set_string(1, 0, "e");


    int64_t val50 = 50;
    int64_t val200 = 200;
    int64_t val20 = 20;
    int64_t val300 = 300;

    Query q1 = table->where();
    q1.greater(0, val200);
    q1.subtable(2);
    q1.less(0, val50);
    q1.end_subtable();
    TableView t1 = q1.find_all(0, size_t(-1));
    CHECK_EQUAL(1, t1.size());
    CHECK_EQUAL(1, t1.get_source_ndx(0));

    Query q2 = table->where();
    q2.subtable(2);
    q2.greater(0, val50);
    q2.Or();
    q2.less(0, val20);
    q2.end_subtable();
    TableView t2 = q2.find_all(0, size_t(-1));
    CHECK_EQUAL(1, t2.size());
    CHECK_EQUAL(0, t2.get_source_ndx(0));

    Query q3 = table->where();
    q3.subtable(2);
    q3.greater(0, val50);
    q3.Or();
    q3.less(0, val20);
    q3.end_subtable();
    q3.less(0, val300);
    TableView t3 = q3.find_all(0, size_t(-1));
    CHECK_EQUAL(1, t3.size());
    CHECK_EQUAL(0, t3.get_source_ndx(0));


    Query q4 = table->where();
    q4.equal(0, int64_t(333));
    q4.Or();
    q4.subtable(2);
    q4.greater(0, val50);
    q4.Or();
    q4.less(0, val20);
    q4.end_subtable();
    TableView t4 = q4.find_all(0, size_t(-1));
    CHECK_EQUAL(2, t4.size());
    CHECK_EQUAL(0, t4.get_source_ndx(0));
    CHECK_EQUAL(2, t4.get_source_ndx(1));
}

TEST(Query_SubtableBug)
{
    Group group;
    TableRef table = group.add_table("test");

    // Create specification with sub-table
    table->add_column(type_Int, "col 0");
    DescriptorRef sub;
    table->add_column(type_Table, "col 1", &sub);
    sub->add_column(type_Int, "sub 0");
    sub->add_column(type_String, "sub 1");
    sub->add_column(type_Bool, "sub 2");
    CHECK_EQUAL(2, table->get_column_count());

    for (int i = 0; i < 5; i++) {
        table->insert_empty_row(i);
        table->set_int(0, i, 100);
    }
    TableRef subtable = table->get_subtable(1, 0);
    subtable->insert_empty_row(0);
    subtable->set_int(0, 0, 11);
    subtable->set_string(1, 0, "a");
    subtable->set_bool(2, 0, true);

    Query q1 = table->where();
    q1.subtable(1);
    q1.equal(2, true);
    q1.end_subtable();
    std::string s = q1.validate();

    TableView t1 = q1.find_all(0, size_t(-1));
    CHECK_EQUAL(1, t1.size());
}


TEST(Query_Sort1)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a"); // 0
    add(ttt, 2, "a"); // 1
    add(ttt, 3, "X"); // 2
    add(ttt, 1, "a"); // 3
    add(ttt, 2, "a"); // 4
    add(ttt, 3, "X"); // 5
    add(ttt, 9, "a"); // 6
    add(ttt, 8, "a"); // 7
    add(ttt, 7, "X"); // 8

    // tv.get_source_ndx()  = 0, 2, 3, 5, 6, 7, 8
    // Vals         = 1, 3, 1, 3, 9, 8, 7
    // result       = 3, 0, 5, 2, 8, 7, 6

    Query q = ttt.where().not_equal(0, 2);
    TableView tv = q.find_all();
    tv.sort(0);

    CHECK(tv.size() == 7);
    CHECK(tv[0].get_int(0) == 1);
    CHECK(tv[1].get_int(0) == 1);
    CHECK(tv[2].get_int(0) == 3);
    CHECK(tv[3].get_int(0) == 3);
    CHECK(tv[4].get_int(0) == 7);
    CHECK(tv[5].get_int(0) == 8);
    CHECK(tv[6].get_int(0) == 9);
}


TEST(Query_QuickSort)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    // Triggers QuickSort because range > len
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    for (size_t t = 0; t < 1000; t++)
        add(ttt, random.draw_int_mod(1100), "a"); // 0

    Query q = ttt.where();
    TableView tv = q.find_all();
    tv.sort(0);

    CHECK(tv.size() == 1000);
    for (size_t t = 1; t < tv.size(); t++) {
        CHECK(tv[t].get_int(0) >= tv[t - 1].get_int(0));
    }
}

TEST(Query_CountSort)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    // Triggers CountSort because range <= len
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    for (size_t t = 0; t < 1000; t++)
        add(ttt, random.draw_int_mod(900), "a"); // 0

    Query q = ttt.where();
    TableView tv = q.find_all();
    tv.sort(0);

    CHECK(tv.size() == 1000);
    for (size_t t = 1; t < tv.size(); t++) {
        CHECK(tv[t].get_int(0) >= tv[t - 1].get_int(0));
    }
}


TEST(Query_SortDescending)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    for (size_t t = 0; t < 1000; t++)
        add(ttt, random.draw_int_mod(1100), "a"); // 0

    Query q = ttt.where();
    TableView tv = q.find_all();
    tv.sort(0, false);

    CHECK(tv.size() == 1000);
    for (size_t t = 1; t < tv.size(); t++) {
        CHECK(tv[t].get_int(0) <= tv[t - 1].get_int(0));
    }
}


TEST(Query_SortDates)
{
    Table table;
    table.add_column(type_OldDateTime, "first");

    table.insert_empty_row(0);
    table.set_olddatetime(0, 0, 1000);
    table.insert_empty_row(1);
    table.set_olddatetime(0, 1, 3000);
    table.insert_empty_row(2);
    table.set_olddatetime(0, 2, 2000);

    TableView tv = table.where().find_all();
    CHECK(tv.size() == 3);
    CHECK(tv.get_source_ndx(0) == 0);
    CHECK(tv.get_source_ndx(1) == 1);
    CHECK(tv.get_source_ndx(2) == 2);

    tv.sort(0);

    CHECK(tv.size() == 3);
    CHECK(tv.get_olddatetime(0, 0) == OldDateTime(1000));
    CHECK(tv.get_olddatetime(0, 1) == OldDateTime(2000));
    CHECK(tv.get_olddatetime(0, 2) == OldDateTime(3000));
}


TEST(Query_SortBools)
{
    Table table;
    table.add_column(type_Bool, "first");

    table.insert_empty_row(0);
    table.set_bool(0, 0, true);
    table.insert_empty_row(0);
    table.set_bool(0, 0, false);
    table.insert_empty_row(0);
    table.set_bool(0, 0, true);

    TableView tv = table.where().find_all();
    tv.sort(0);

    CHECK(tv.size() == 3);
    CHECK(tv.get_bool(0, 0) == false);
    CHECK(tv.get_bool(0, 1) == true);
    CHECK(tv.get_bool(0, 2) == true);
}

TEST(Query_SortLinks)
{
    const size_t num_rows = 10;
    Group g;
    TableRef t1 = g.add_table("t1");
    TableRef t2 = g.add_table("t2");

    size_t t1_int_col = t1->add_column(type_Int, "t1_int");
    size_t t1_str_col = t1->add_column(type_String, "t1_string");
    size_t t1_link_t2_col = t1->add_column_link(type_Link, "t1_link_to_t2", *t2);
    size_t t2_int_col = t2->add_column(type_Int, "t2_int");
    size_t t2_str_col = t2->add_column(type_String, "t2_string");
    size_t t2_link_t1_col = t2->add_column_link(type_Link, "t2_link_to_t1", *t1);

    t1->add_empty_row(num_rows);
    t2->add_empty_row(num_rows);
    std::vector<std::string> ordered_strings;

    for (size_t i = 0; i < num_rows; ++i) {
        ordered_strings.push_back(std::string("a string") + util::to_string(i));
        t1->set_int(t1_int_col, i, i);
        t1->set_string(t1_str_col, i, ordered_strings[i]);
        t1->set_link(t1_link_t2_col, i, num_rows - i - 1);

        t2->set_int(t2_int_col, i, i);
        t2->set_string(t2_str_col, i, ordered_strings[i]);
        t2->set_link(t2_link_t1_col, i, i);
    }

    TableView tv = t1->where().find_all();

    // Check natural order
    CHECK_EQUAL(tv.size(), num_rows);
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv.get_int(t1_int_col, i), i);
        CHECK_EQUAL(tv.get_string(t1_str_col, i), ordered_strings[i]);
    }

    // Check sorted order by ints
    tv.sort(t1_int_col);
    CHECK_EQUAL(tv.size(), num_rows);
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv.get_int(t1_int_col, i), i);
        CHECK_EQUAL(tv.get_string(t1_str_col, i), ordered_strings[i]);
    }

    // Check that you can sort on a regular link column
    tv.sort(t1_link_t2_col);
    CHECK_EQUAL(tv.size(), num_rows);
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv.get_int(t1_int_col, i), num_rows - i - 1);
        CHECK_EQUAL(tv.get_string(t1_str_col, i), ordered_strings[num_rows - i - 1]);
    }
}


TEST(Query_SortLinkChains)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    TableRef t2 = g.add_table("t2");
    TableRef t3 = g.add_table("t3");

    size_t t1_int_col = t1->add_column(type_Int, "t1_int");
    size_t t1_link_col = t1->add_column_link(type_Link, "t1_link_t2", *t2);
    size_t t2_int_col = t2->add_column(type_Int, "t2_int");
    size_t t2_link_col = t2->add_column_link(type_Link, "t2_link_t3", *t3);
    size_t t3_int_col = t3->add_column(type_Int, "t3_int", true);
    size_t t3_str_col = t3->add_column(type_String, "t3_str");

    t1->add_empty_row(7);
    t2->add_empty_row(6);
    t3->add_empty_row(4);

    t1->set_int(t1_int_col, 0, 99);
    for (size_t i = 0; i < t2->size(); i++) {
        t1->set_int(t1_int_col, i + 1, i);
        t2->set_int(t2_int_col, i, t1->size() - i);
    }

    t1->set_link(t1_link_col, 0, 1);
    t1->set_link(t1_link_col, 1, 0);
    t1->set_link(t1_link_col, 2, 2);
    t1->set_link(t1_link_col, 3, 3);
    t1->set_link(t1_link_col, 4, 5);
    t1->set_link(t1_link_col, 5, 4);
    t1->set_link(t1_link_col, 6, 1);

    t2->set_link(t2_link_col, 0, 3);
    t2->set_link(t2_link_col, 1, 2);
    t2->set_link(t2_link_col, 2, 0);
    t2->set_link(t2_link_col, 3, 1);
    t2->nullify_link(t2_link_col, 4);
    t2->nullify_link(t2_link_col, 5);

    t3->set_null(t3_int_col, 0);
    t3->set_int(t3_int_col, 1, 4);
    t3->set_int(t3_int_col, 2, 7);
    t3->set_int(t3_int_col, 3, 3);
    t3->set_string(t3_str_col, 0, "b");
    t3->set_string(t3_str_col, 1, "a");
    t3->set_string(t3_str_col, 2, "c");
    t3->set_string(t3_str_col, 3, "k");

    //  T1                       T2                     T3
    //  t1_int   t1_link_t2  |   t2_int  t2_link_t3 |   t3_int  t3_str
    //  ==============================================================
    //  99       1           |   5       3          |   null    "b"
    //  0        0           |   4       2          |   4       "a"
    //  1        2           |   3       0          |   7       "c"
    //  2        3           |   2       1          |   3       "k"
    //  3        5           |   1       null       |
    //  4        4           |   0       null       |
    //  5        1           |                      |

    TableView tv = t1->where().less(t1_int_col, 6).find_all();

    // Test original funcionality through chain class
    std::vector<size_t> results1 = {0, 1, 2, 3, 4, 5};
    tv.sort(SortDescriptor(*t1, {{t1_int_col}}, {true}));
    CHECK_EQUAL(tv.size(), results1.size());
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv.get_int(t1_int_col, i), results1[i]);
    }
    tv.sort(SortDescriptor(*t1, {{t1_int_col}}, {false}));
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv.get_int(t1_int_col, i), results1[results1.size() - 1 - i]);
    }

    // Test basic one link chain
    std::vector<size_t> results2 = {3, 4, 2, 1, 5, 0};
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_int_col}}, {true}));
    CHECK_EQUAL(tv.size(), results2.size());
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv.get_int(t1_int_col, i), results2[i]);
    }
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_int_col}}, {false}));
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv.get_int(t1_int_col, i), results2[results2.size() - 1 - i]);
    }

    // Test link chain through two links with nulls
    std::vector<size_t> results3 = {1, 0, 2, 5};
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}}, {true}));
    // No guarantees about nullified links except they are at the end.
    CHECK(tv.size() >= results3.size());
    for (size_t i = 0; i < results3.size(); ++i) {
        CHECK_EQUAL(tv.get_int(t1_int_col, i), results3[i]);
    }
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}}, {false}));
    // No guarantees about nullified links except they are at the beginning.
    size_t num_nulls = tv.size() - results3.size();
    for (size_t i = num_nulls; i < results3.size(); ++i) {
        CHECK_EQUAL(tv.get_int(t1_int_col, i), results3[results2.size() - 1 - i]);
    }

    // Test link chain with nulls and a single local column
    std::vector<size_t> results4 = {1, 0, 2, 5, 3, 4};
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}, {t1_int_col}}));
    CHECK_EQUAL(tv.size(), results4.size());
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv.get_int(t1_int_col, i), results4[i]);
    }
    std::vector<size_t> results4_rev = {1, 0, 2, 5, 4, 3};
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}, {t1_int_col}}, {true, false}));
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv.get_int(t1_int_col, i), results4_rev[i]);
    }
    std::vector<size_t> results4_rev2 = {3, 4, 5, 2, 0, 1};
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}, {t1_int_col}}, {false, true}));
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv.get_int(t1_int_col, i), results4_rev2[i]);
    }
    std::vector<size_t> results4_rev3 = {4, 3, 5, 2, 0, 1};
    tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}, {t1_int_col}}, {false, false}));
    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_EQUAL(tv.get_int(t1_int_col, i), results4_rev3[i]);
    }
}


TEST(Query_LinkChainSortErrors)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    TableRef t2 = g.add_table("t2");

    size_t t1_int_col = t1->add_column(type_Int, "t1_int");
    size_t t1_linklist_col = t1->add_column_link(type_LinkList, "t1_linklist", *t2);
    size_t t2_string_col = t2->add_column(type_String, "t2_string");
    t2->add_column_link(type_Link, "t2_link_t1", *t1); // add a backlink to t1

    // Disallow backlinks, linklists, other non-link column types.
    size_t backlink_ndx = 2;
    CHECK_LOGIC_ERROR(SortDescriptor(*t1, {{t1_linklist_col, t2_string_col}}), LogicError::type_mismatch);
    CHECK_LOGIC_ERROR(SortDescriptor(*t1, {{backlink_ndx, t2_string_col}}), LogicError::type_mismatch);
    CHECK_LOGIC_ERROR(SortDescriptor(*t1, {{t1_int_col, t2_string_col}}), LogicError::type_mismatch);
}


TEST(Query_DistinctThroughLinks)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    TableRef t2 = g.add_table("t2");
    TableRef t3 = g.add_table("t3");

    size_t t1_int_col = t1->add_column(type_Int, "t1_int");
    size_t t1_link_col = t1->add_column_link(type_Link, "t1_link_t2", *t2);
    size_t t2_int_col = t2->add_column(type_Int, "t2_int");
    size_t t2_link_col = t2->add_column_link(type_Link, "t2_link_t3", *t3);
    size_t t3_int_col = t3->add_column(type_Int, "t3_int", true);
    size_t t3_str_col = t3->add_column(type_String, "t3_str");

    t1->add_empty_row(7);
    t2->add_empty_row(6);
    t3->add_empty_row(4);

    t1->set_int(t1_int_col, 0, 99);
    for (size_t i = 0; i < t2->size(); i++) {
        t1->set_int(t1_int_col, i + 1, i);
        t2->set_int(t2_int_col, i, t2->size() - i - 1);
    }
    t2->set_int(t2_int_col, 0, 0);
    t2->set_int(t2_int_col, 1, 0);

    t1->set_link(t1_link_col, 0, 1);
    t1->set_link(t1_link_col, 1, 0);
    t1->set_link(t1_link_col, 2, 2);
    t1->set_link(t1_link_col, 3, 3);
    t1->set_link(t1_link_col, 4, 5);
    t1->set_link(t1_link_col, 5, 4);
    t1->set_link(t1_link_col, 6, 1);

    t2->set_link(t2_link_col, 0, 3);
    t2->set_link(t2_link_col, 1, 2);
    t2->set_link(t2_link_col, 2, 0);
    t2->set_link(t2_link_col, 3, 1);
    t2->nullify_link(t2_link_col, 4);
    t2->nullify_link(t2_link_col, 5);

    t3->set_null(t3_int_col, 0);
    t3->set_int(t3_int_col, 1, 4);
    t3->set_int(t3_int_col, 2, 7);
    t3->set_int(t3_int_col, 3, 3);
    t3->set_string(t3_str_col, 0, "b");
    t3->set_string(t3_str_col, 1, "a");
    t3->set_string(t3_str_col, 2, "c");
    t3->set_string(t3_str_col, 3, "k");

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
        tv.distinct(SortDescriptor(*t1, {{t1_int_col}}, {true}));
        CHECK_EQUAL(tv.size(), results1.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get_int(t1_int_col, i), results1[i]);
        }
        tv.distinct(SortDescriptor(*t1, {{t1_int_col}}, {false}));
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get_int(t1_int_col, i), results1[i]); // results haven't been sorted
        }
        tv.sort(SortDescriptor(*t1, {{t1_int_col}}, {true}));
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get_int(t1_int_col, i), results1[i]); // still same order here by conincidence
        }
        tv.sort(SortDescriptor(*t1, {{t1_int_col}}, {false}));
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get_int(t1_int_col, i), results1[results1.size() - 1 - i]); // now its reversed
        }
    }

    {
        TableView tv = t1->where().less(t1_int_col, 6).find_all(); // fresh unsorted view

        // Test basic one link chain
        std::vector<size_t> results2 = {0, 1, 2, 4};
        tv.distinct(SortDescriptor(*t1, {{t1_link_col, t2_int_col}}));
        CHECK_EQUAL(tv.size(), results2.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get_int(t1_int_col, i), results2[i]);
        }
        tv.distinct(SortDescriptor(*t1, {{t1_link_col, t2_int_col}}, {false}));
        CHECK_EQUAL(tv.size(), results2.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            // no difference even though false on distinct was specified
            CHECK_EQUAL(tv.get_int(t1_int_col, i), results2[i]);
        }

        std::vector<size_t> results2_sorted_link = {0, 4, 2, 1};
        tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_int_col}}, {true}));
        CHECK_EQUAL(tv.size(), results2_sorted_link.size());
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get_int(t1_int_col, i), results2_sorted_link[i]);
        }
        tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_int_col}}, {false}));
        for (size_t i = 0; i < tv.size(); ++i) {
            CHECK_EQUAL(tv.get_int(t1_int_col, i), results2_sorted_link[results2_sorted_link.size() - 1 - i]);
        }
    }

    {
        TableView tv = t1->where().less(t1_int_col, 6).find_all(); // fresh unsorted view

        // Test link chain through two links with nulls
        std::vector<size_t> results3 = {0, 1, 2, 5};
        tv.distinct(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}}));
        // Nullified links are excluded from distinct.
        CHECK_EQUAL(tv.size(), results3.size());
        for (size_t i = 0; i < results3.size(); ++i) {
            CHECK_EQUAL(tv.get_int(t1_int_col, i), results3[i]);
        }
        tv.distinct(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}}, {false}));
        CHECK_EQUAL(tv.size(), results3.size());
        for (size_t i = 0; i < results3.size(); ++i) {
            // same order as before
            CHECK_EQUAL(tv.get_int(t1_int_col, i), results3[i]);
        }

        results3 = {1, 0, 2, 5}; // sorted order on t3_col_int { null, 3, 4, 7 }
        tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}}));
        CHECK_EQUAL(tv.size(), results3.size());
        for (size_t i = 0; i < results3.size(); ++i) {
            CHECK_EQUAL(tv.get_int(t1_int_col, i), results3[i]);
        }
        tv.sort(SortDescriptor(*t1, {{t1_link_col, t2_link_col, t3_int_col}}, {false}));
        CHECK_EQUAL(tv.size(), results3.size());
        for (size_t i = 0; i < results3.size(); ++i) {
            CHECK_EQUAL(tv.get_int(t1_int_col, i), results3[results3.size() - 1 - i]);
        }
    }
}


TEST(Query_Sort_And_Requery_Typed1)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a"); // 0 *
    add(ttt, 2, "a"); // 1
    add(ttt, 3, "X"); // 2
    add(ttt, 1, "a"); // 3 *
    add(ttt, 2, "a"); // 4
    add(ttt, 3, "X"); // 5
    add(ttt, 9, "a"); // 6 *
    add(ttt, 8, "a"); // 7 *
    add(ttt, 7, "X"); // 8

    // tv.get_source_ndx()  = 0, 2, 3, 5, 6, 7, 8
    // Vals         = 1, 3, 1, 3, 9, 8, 7
    // result       = 3, 0, 5, 2, 8, 7, 6

    Query q = ttt.where().not_equal(0, 2);
    TableView tv = q.find_all();

    size_t match = ttt.where(&tv).equal(0, 7).find();
    CHECK_EQUAL(match, 8);

    tv.sort(0);

    CHECK(tv.size() == 7);
    CHECK(tv[0].get_int(0) == 1);
    CHECK(tv[1].get_int(0) == 1);
    CHECK(tv[2].get_int(0) == 3);
    CHECK(tv[3].get_int(0) == 3);
    CHECK(tv[4].get_int(0) == 7);
    CHECK(tv[5].get_int(0) == 8);
    CHECK(tv[6].get_int(0) == 9);

    Query q2 = ttt.where(&tv).not_equal(1, "X");
    TableView tv2 = q2.find_all();

    CHECK_EQUAL(4, tv2.size());
    CHECK_EQUAL(1, tv2[0].get_int(0));
    CHECK_EQUAL(1, tv2[1].get_int(0));
    CHECK_EQUAL(8, tv2[2].get_int(0)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv2[3].get_int(0));

    match = ttt.where(&tv).not_equal(1, "X").find();
    CHECK_EQUAL(match, 0);

    match = ttt.where(&tv).not_equal(1, "X").find(1);
    CHECK_EQUAL(match, 3);

    match = ttt.where(&tv).not_equal(1, "X").find(2);
    CHECK_EQUAL(match, 3);

    match = ttt.where(&tv).not_equal(1, "X").find(6);
    CHECK_EQUAL(match, 7);
}


TEST(Query_Sort_And_Requery_FindFirst)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_Int, "2");

    add(ttt, 1, 60);
    add(ttt, 2, 50); // **
    add(ttt, 3, 40); // *
    add(ttt, 1, 30);
    add(ttt, 2, 20); // **
    add(ttt, 3, 10); // **

    Query q = ttt.where().greater(0, 1);
    TableView tv = q.find_all();
    tv.sort(1);

    // 3, 2, 1, 3, 2, 1
    size_t t = ttt.where(&tv).equal(0, 3).find();
    int64_t s = ttt.where(&tv).not_equal(1, 40).sum_int(0);

    static_cast<void>(s);
    static_cast<void>(t);
}


TEST(Query_Sort_And_Requery_Untyped2)
{
    // New where(tableview) method
    Table table;
    table.add_column(type_Int, "first1");
    table.add_column(type_String, "second1");

    table.add_empty_row();
    table.set_int(0, 0, 1);
    table.set_string(1, 0, "a");

    table.add_empty_row();
    table.set_int(0, 1, 2);
    table.set_string(1, 1, "a");

    table.add_empty_row();
    table.set_int(0, 2, 3);
    table.set_string(1, 2, "X");

    table.add_empty_row();
    table.set_int(0, 3, 1);
    table.set_string(1, 3, "a");

    table.add_empty_row();
    table.set_int(0, 4, 2);
    table.set_string(1, 4, "a");

    table.add_empty_row();
    table.set_int(0, 5, 3);
    table.set_string(1, 5, "X");

    table.add_empty_row();
    table.set_int(0, 6, 9);
    table.set_string(1, 6, "a");

    table.add_empty_row();
    table.set_int(0, 7, 8);
    table.set_string(1, 7, "a");

    table.add_empty_row();
    table.set_int(0, 8, 7);
    table.set_string(1, 8, "X");

    // tv.get_source_ndx()  = 0, 2, 3, 5, 6, 7, 8
    // Vals         = 1, 3, 1, 3, 9, 8, 7
    // result       = 3, 0, 5, 2, 8, 7, 6

    Query q = table.where().not_equal(0, 2);
    TableView tv = q.find_all();
    tv.sort(0);

    CHECK(tv.size() == 7);

    CHECK(tv.get_int(0, 0) == 1);
    CHECK(tv.get_int(0, 1) == 1);
    CHECK(tv.get_int(0, 2) == 3);
    CHECK(tv.get_int(0, 3) == 3);
    CHECK(tv.get_int(0, 4) == 7);
    CHECK(tv.get_int(0, 5) == 8);
    CHECK(tv.get_int(0, 6) == 9);

    Query q2 = table.where(&tv).not_equal(1, "X");
    TableView tv2 = q2.find_all();

    CHECK_EQUAL(4, tv2.size());
    CHECK_EQUAL(1, tv2.get_int(0, 0));
    CHECK_EQUAL(1, tv2.get_int(0, 1));
    CHECK_EQUAL(8, tv2.get_int(0, 2)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv2.get_int(0, 3));

    Query q3 = table.where(&tv2).not_equal(1, "X");
    TableView tv3 = q3.find_all();

    CHECK_EQUAL(4, tv3.size());
    CHECK_EQUAL(1, tv3.get_int(0, 0));
    CHECK_EQUAL(1, tv3.get_int(0, 1));
    CHECK_EQUAL(8, tv3.get_int(0, 2)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv3.get_int(0, 3));

    // Test that remove() maintains order
    tv3.remove(0);
    // q5 and q3 should behave the same.
    Query q5 = table.where(&tv2).not_equal(1, "X");
    TableView tv5 = q5.find_all();
    tv5.sync_if_needed(); // you may think tv5 is in sync, BUT it was generated from tv2 which wasn't
    // Note the side effect - as tv5 depends on ... on tv2 etc, all views are synchronized.
    CHECK_EQUAL(3, tv5.size());
    CHECK_EQUAL(1, tv5.get_int(0, 0));
    CHECK_EQUAL(8, tv5.get_int(0, 1)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv5.get_int(0, 2));

    CHECK_EQUAL(6, tv.size());
    CHECK_EQUAL(3, tv3.size());
    CHECK_EQUAL(1, tv3.get_int(0, 0));
    CHECK_EQUAL(8, tv3.get_int(0, 1)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv3.get_int(0, 2));

    Query q4 = table.where(&tv3).not_equal(1, "X");
    TableView tv4 = q4.find_all();

    CHECK_EQUAL(3, tv4.size());
    CHECK_EQUAL(1, tv4.get_int(0, 0));
    CHECK_EQUAL(8, tv4.get_int(0, 1)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv4.get_int(0, 2));
}


TEST(Query_Sort_And_Requery_Untyped1)
{
    // More tests on new where(tv) query on tableviews
    Table table;
    table.add_column(type_Int, "first1");
    table.add_column(type_String, "second1");

    table.add_empty_row();
    table.set_int(0, 0, 1);
    table.set_string(1, 0, "a");

    table.add_empty_row();
    table.set_int(0, 1, 2);
    table.set_string(1, 1, "a");

    table.add_empty_row();
    table.set_int(0, 2, 3);
    table.set_string(1, 2, "X");

    table.add_empty_row();
    table.set_int(0, 3, 1);
    table.set_string(1, 3, "a");

    table.add_empty_row();
    table.set_int(0, 4, 2);
    table.set_string(1, 4, "a");

    table.add_empty_row();
    table.set_int(0, 5, 3);
    table.set_string(1, 5, "X");

    table.add_empty_row();
    table.set_int(0, 6, 9);
    table.set_string(1, 6, "a");

    table.add_empty_row();
    table.set_int(0, 7, 8);
    table.set_string(1, 7, "a");

    table.add_empty_row();
    table.set_int(0, 8, 7);
    table.set_string(1, 8, "X");


    // tv.get_source_ndx()  = 0, 2, 3, 5, 6, 7, 8
    // Vals         = 1, 3, 1, 3, 9, 8, 7
    // result       = 3, 0, 5, 2, 8, 7, 6

    Query q = table.where().not_equal(0, 2);
    TableView tv = q.find_all();
    tv.sort(0);

    CHECK(tv.size() == 7);

    CHECK(tv.get_int(0, 0) == 1);
    CHECK(tv.get_int(0, 1) == 1);
    CHECK(tv.get_int(0, 2) == 3);
    CHECK(tv.get_int(0, 3) == 3);
    CHECK(tv.get_int(0, 4) == 7);
    CHECK(tv.get_int(0, 5) == 8);
    CHECK(tv.get_int(0, 6) == 9);

    Query q2 = table.where(&tv).not_equal(1, "X");
    TableView tv2 = q2.find_all();

    CHECK_EQUAL(4, tv2.size());
    CHECK_EQUAL(1, tv2.get_int(0, 0));
    CHECK_EQUAL(1, tv2.get_int(0, 1));
    CHECK_EQUAL(8, tv2.get_int(0, 2)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv2.get_int(0, 3));

    Query q3 = table.where(&tv2).not_equal(1, "X");
    TableView tv3 = q3.find_all();

    CHECK_EQUAL(4, tv3.size());

    CHECK_EQUAL(4, tv3.size());
    CHECK_EQUAL(1, tv3.get_int(0, 0));
    CHECK_EQUAL(1, tv3.get_int(0, 1));
    CHECK_EQUAL(8, tv3.get_int(0, 2)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv3.get_int(0, 3));

    // Test remove()
    tv3.remove(0);
    Query q4 = table.where(&tv3).not_equal(1, "X");
    TableView tv4 = q4.find_all();

    CHECK_EQUAL(3, tv4.size());
    CHECK_EQUAL(1, tv4.get_int(0, 0));
    CHECK_EQUAL(8, tv4.get_int(0, 1)); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv4.get_int(0, 2));
}


TEST(Query_Sort_And_Requery_Untyped_Monkey2)
{
    // New where(tableview) method
    for (int iter = 0; iter < 1; iter++) {
        size_t b;
        Table table;
        table.add_column(type_Int, "first1");
        table.add_column(type_Int, "second1");

        // Add random data to table
        for (size_t t = 0; t < 2 * REALM_MAX_BPNODE_SIZE; t++) {
            table.add_empty_row();
            int64_t val1 = rand() % 5;
            table.set_int(0, t, val1);
            int64_t val2 = rand() % 5;
            table.set_int(1, t, val2);
        }

        // Query and sort
        Query q = table.where().equal(1, 2);
        TableView tv = q.find_all();
        tv.sort(0);

        // Requery and keep original sort order
        Query q2 = table.where(&tv).not_equal(0, 3);
        TableView tv2 = q2.find_all();

        b = 0;
        // Test if sort order is the same as original
        for (size_t t = 0; t < tv2.size(); t++) {
            size_t a = tv2.get_source_ndx(t);
            REALM_ASSERT_EX(b < tv.size(), b, tv.size());
            while (a != tv.get_source_ndx(b)) {
                b++;
            }
        }

        // We cannot test remove() if query resulted in 0 items
        if (tv2.size() == 0)
            continue;

        size_t remove = rand() % tv2.size();
        static_cast<void>(remove);

        Query q3 = table.where(&tv2).not_equal(0, 2);
        TableView tv3 = q3.find_all();

        b = 0;
        // Test if sort order is the same as original
        for (size_t t = 0; t < tv3.size(); t++) {
            size_t a = tv3.get_source_ndx(t);
            REALM_ASSERT_EX(b < tv2.size(), b, tv2.size());
            while (a != tv2.get_source_ndx(b)) {
                b++;
                CHECK(b < tv2.size());
            }
        }

        // Now test combinations of sorted and non-sorted tableviews
        Query q4 = table.where().not_equal(0, 1);
        TableView tv4 = q4.find_all();

        Query q5 = table.where(&tv4).not_equal(0, 2);
        TableView tv5 = q5.find_all();

        for (size_t t = 1; t < tv5.size(); t++) {
            CHECK(tv5.get_source_ndx(t - 1) < tv5.get_source_ndx(t));
        }

        // Test that tv5 is ordered the same way as tv4 (tv5 is subset of tv4)
        size_t foreignindex = 0;
        for (size_t t = 0; t < tv5.size(); t++) {
            size_t foreignindex2 = 0;
            while (tv4.get_source_ndx(foreignindex2) != tv5.get_source_ndx(t))
                foreignindex2++;

            CHECK(foreignindex2 >= foreignindex);
            foreignindex = foreignindex2;
        }

        // New test where both tableviews are sorted according to a column, and both sets are equal
        Query q6 = table.where().not_equal(0, 2);
        TableView tv6 = q6.find_all();

        Query q7 = table.where(&tv6).not_equal(0, 2);
        TableView tv7 = q7.find_all();

        // Test that tv7 is ordered the same way as tv6
        foreignindex = 0;
        for (size_t t = 0; t < tv5.size(); t++) {
            size_t foreignindex2 = 0;
            while (tv4.get_source_ndx(foreignindex2) != tv5.get_source_ndx(t))
                foreignindex2++;

            CHECK(foreignindex2 >= foreignindex);
            foreignindex = foreignindex2;
        }

        tv7.sort(1);
        tv6.sort(1);

        // Test that tv7 is ordered the same way as tv6
        foreignindex = 0;
        for (size_t t = 0; t < tv5.size(); t++) {
            size_t foreignindex2 = 0;
            while (tv4.get_source_ndx(foreignindex2) != tv5.get_source_ndx(t))
                foreignindex2++;

            CHECK(foreignindex2 >= foreignindex);
            foreignindex = foreignindex2;
        }
    }
}


TEST(Query_Threads)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    // Spread query search hits in an odd way to test more edge cases
    // (thread job size is THREAD_CHUNK_SIZE = 10)
    for (int i = 0; i < 30; i++) {
        for (int j = 0; j < 10; j++) {
            add(ttt, 5, "a");
            add(ttt, j, "b");
            add(ttt, 6, "c");
            add(ttt, 6, "a");
            add(ttt, 6, "b");
            add(ttt, 6, "c");
            add(ttt, 6, "a");
        }
    }
    Query q1 = ttt.where().equal(0, 2).equal(1, "b");

    // Note, set THREAD_CHUNK_SIZE to 1.000.000 or more for performance
    // q1.set_threads(5);
    TableView tv = q1.find_all();

    CHECK_EQUAL(30, tv.size());
    for (int i = 0; i < 30; i++) {
        const size_t expected = i * 7 * 10 + 14 + 1;
        const size_t actual = tv.get_source_ndx(i);
        CHECK_EQUAL(expected, actual);
    }
}


TEST(Query_LongString)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    // Spread query search hits in an odd way to test more edge cases
    // (thread job size is THREAD_CHUNK_SIZE = 10)
    for (int i = 0; i < 30; i++) {
        for (int j = 0; j < 10; j++) {
            add(ttt, 5, "aaaaaaaaaaaaaaaaaa");
            add(ttt, j, "bbbbbbbbbbbbbbbbbb");
            add(ttt, 6, "cccccccccccccccccc");
            add(ttt, 6, "aaaaaaaaaaaaaaaaaa");
            add(ttt, 6, "bbbbbbbbbbbbbbbbbb");
            add(ttt, 6, "cccccccccccccccccc");
            add(ttt, 6, "aaaaaaaaaaaaaaaaaa");
        }
    }
    Query q1 = ttt.where().equal(0, 2).equal(1, "bbbbbbbbbbbbbbbbbb");

    // Note, set THREAD_CHUNK_SIZE to 1.000.000 or more for performance
    // q1.set_threads(5);
    TableView tv = q1.find_all();

    CHECK_EQUAL(30, tv.size());
    for (int i = 0; i < 30; i++) {
        const size_t expected = i * 7 * 10 + 14 + 1;
        const size_t actual = tv.get_source_ndx(i);
        CHECK_EQUAL(expected, actual);
    }
}


TEST(Query_LongEnum)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    // Spread query search hits in an odd way to test more edge cases
    // (thread job size is THREAD_CHUNK_SIZE = 10)
    for (int i = 0; i < 30; i++) {
        for (int j = 0; j < 10; j++) {
            add(ttt, 5, "aaaaaaaaaaaaaaaaaa");
            add(ttt, j, "bbbbbbbbbbbbbbbbbb");
            add(ttt, 6, "cccccccccccccccccc");
            add(ttt, 6, "aaaaaaaaaaaaaaaaaa");
            add(ttt, 6, "bbbbbbbbbbbbbbbbbb");
            add(ttt, 6, "cccccccccccccccccc");
            add(ttt, 6, "aaaaaaaaaaaaaaaaaa");
        }
    }
    ttt.optimize();
    Query q1 = ttt.where().equal(0, 2).not_equal(1, "aaaaaaaaaaaaaaaaaa");

    // Note, set THREAD_CHUNK_SIZE to 1.000.000 or more for performance
    // q1.set_threads(5);
    TableView tv = q1.find_all();

    CHECK_EQUAL(30, tv.size());
    for (int i = 0; i < 30; i++) {
        const size_t expected = i * 7 * 10 + 14 + 1;
        const size_t actual = tv.get_source_ndx(i);
        CHECK_EQUAL(expected, actual);
    }
}

TEST(Query_BigString)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    size_t res1 = ttt.where().equal(1, "a").find();
    CHECK_EQUAL(0, res1);

    add(ttt, 2, "40 chars  40 chars  40 chars  40 chars  ");
    size_t res2 = ttt.where().equal(1, "40 chars  40 chars  40 chars  40 chars  ").find();
    CHECK_EQUAL(1, res2);

    add(ttt, 1, "70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
    size_t res3 =
        ttt.where().equal(1, "70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ").find();
    CHECK_EQUAL(2, res3);
}

TEST(Query_Simple2)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");

    Query q1 = ttt.where().equal(0, 2);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(4, tv1.get_source_ndx(1));
    CHECK_EQUAL(7, tv1.get_source_ndx(2));
}


TEST(Query_Limit)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a"); //
    add(ttt, 3, "X");
    add(ttt, 1, "a");
    add(ttt, 2, "a"); //
    add(ttt, 3, "X");
    add(ttt, 1, "a");
    add(ttt, 2, "a"); //
    add(ttt, 3, "X");
    add(ttt, 1, "a");
    add(ttt, 2, "a"); //
    add(ttt, 3, "X");
    add(ttt, 1, "a");
    add(ttt, 2, "a"); //
    add(ttt, 3, "X");

    Query q1 = ttt.where().equal(0, 2);

    TableView tv1 = q1.find_all(0, size_t(-1), 2);
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(4, tv1.get_source_ndx(1));

    TableView tv2 = q1.find_all(tv1.get_source_ndx(tv1.size() - 1) + 1, size_t(-1), 2);
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(7, tv2.get_source_ndx(0));
    CHECK_EQUAL(10, tv2.get_source_ndx(1));

    TableView tv3 = q1.find_all(tv2.get_source_ndx(tv2.size() - 1) + 1, size_t(-1), 2);
    CHECK_EQUAL(1, tv3.size());
    CHECK_EQUAL(13, tv3.get_source_ndx(0));


    Query q2 = ttt.where();
    TableView tv4 = q2.find_all(0, 5, 3);
    CHECK_EQUAL(3, tv4.size());

    Query q3 = ttt.where();
    TableView tv5 = q3.find_all(0, 3, 5);
    CHECK_EQUAL(3, tv5.size());
}


TEST(Query_FindNext)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 4, "a");
    add(ttt, 5, "a");
    add(ttt, 6, "X");
    add(ttt, 7, "X");

    Query q1 = ttt.where().equal(1, "X").greater(0, 4);

    const size_t res1 = q1.find();
    const size_t res2 = q1.find(res1 + 1);
    const size_t res3 = q1.find(res2 + 1);

    CHECK_EQUAL(5, res1);
    CHECK_EQUAL(6, res2);
    CHECK_EQUAL(not_found, res3); // no more matches

    // Do same searches with new query every time
    const size_t res4 = ttt.where().equal(1, "X").greater(0, 4).find();
    const size_t res5 = ttt.where().equal(1, "X").greater(0, 4).find(res1 + 1);
    const size_t res6 = ttt.where().equal(1, "X").greater(0, 4).find(res2 + 1);

    CHECK_EQUAL(5, res4);
    CHECK_EQUAL(6, res5);
    CHECK_EQUAL(not_found, res6); // no more matches
}


TEST(Query_FindNextBackwards)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    // Create multiple leaves
    for (size_t i = 0; i < REALM_MAX_BPNODE_SIZE * 4; i++) {
        add(ttt, 6, "X");
        add(ttt, 7, "X");
    }

    Query q = ttt.where().greater(0, 4);

    // Check if leaf caching works correctly in the case you go backwards. 'res' result is not so important
    // in this test; this test tests if we assert errorneously. Next test (TestQueryFindRandom) is more exhaustive
    size_t res = q.find(REALM_MAX_BPNODE_SIZE * 2);
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE * 2, res);
    res = q.find(0);
    CHECK_EQUAL(0, res);
}


// Begin search at arbitrary positions for *same* query object (other tests in this test_query file test same thing,
// but for independent query objects) to test if leaf cacher works correctly (can go backwards, etc).
TEST(Query_FindRandom)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    int64_t search = REALM_MAX_BPNODE_SIZE / 2;
    size_t rows = REALM_MAX_BPNODE_SIZE * 20;

    // Create multiple leaves
    for (size_t i = 0; i < rows; i++) {
        // This value distribution makes us sometimes cross a leaf boundary, and sometimes not, with both having
        // a fair probability of happening
        add(ttt, random.draw_int_mod(REALM_MAX_BPNODE_SIZE), "X");
    }

    Query q = ttt.where().equal(0, search);

    for (size_t t = 0; t < 100; t++) {
        size_t begin = random.draw_int_mod(rows);
        size_t res = q.find(begin);

        // Find correct match position manually in a for-loop
        size_t expected = not_found;
        for (size_t u = begin; u < rows; u++) {
            if (ttt.get_int(0, u) == search) {
                expected = u;
                break;
            }
        }

        // Compare .find() with manual for-loop-result
        CHECK_EQUAL(expected, res);
    }
}

TEST(Query_FindNext2)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 4, "a");
    add(ttt, 5, "a");
    add(ttt, 6, "X");
    add(ttt, 7, "X"); // match

    Query q1 = ttt.where().equal(1, "X").greater(0, 4);

    const size_t res1 = q1.find(6);
    CHECK_EQUAL(6, res1);
}

TEST(Query_FindAll1)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 4, "a");
    add(ttt, 5, "a");
    add(ttt, 6, "X");
    add(ttt, 7, "X");

    Query q1 = ttt.where().equal(1, "a").greater(0, 2).not_equal(0, 4);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.get_source_ndx(0));

    Query q2 = ttt.where().equal(1, "X").greater(0, 4);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(5, tv2.get_source_ndx(0));
    CHECK_EQUAL(6, tv2.get_source_ndx(1));
}

TEST(Query_FindAll2)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 4, "a");
    add(ttt, 5, "a");
    add(ttt, 11, "X");
    add(ttt, 0, "X");

    Query q2 = ttt.where().not_equal(1, "a").less(0, 3);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(6, tv2.get_source_ndx(0));
}

TEST(Query_FindAllBetween)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 4, "a");
    add(ttt, 5, "a");
    add(ttt, 11, "X");
    add(ttt, 3, "X");

    Query q2 = ttt.where().between(0, 3, 5);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(2, tv2.get_source_ndx(0));
    CHECK_EQUAL(3, tv2.get_source_ndx(1));
    CHECK_EQUAL(4, tv2.get_source_ndx(2));
    CHECK_EQUAL(6, tv2.get_source_ndx(3));
}


TEST(Query_FindAllRange)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 5, "a");
    add(ttt, 5, "a");
    add(ttt, 5, "a");

    Query q1 = ttt.where().equal(1, "a").greater(0, 2).not_equal(0, 4);
    TableView tv1 = q1.find_all(1, 2);
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
}


TEST(Query_FindAllOr)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 4, "a");
    add(ttt, 5, "a");
    add(ttt, 6, "a");
    add(ttt, 7, "X");

    // first == 5 || second == X
    Query q1 = ttt.where().equal(0, 5).Or().equal(1, "X");
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(2, tv1.get_source_ndx(0));
    CHECK_EQUAL(4, tv1.get_source_ndx(1));
    CHECK_EQUAL(6, tv1.get_source_ndx(2));
}


TEST(Query_FindAllParens1)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 3, "X");
    add(ttt, 4, "a");
    add(ttt, 5, "a");
    add(ttt, 11, "X");

    // first > 3 && (second == X)
    Query q1 = ttt.where().greater(0, 3).group().equal(1, "X").end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(6, tv1.get_source_ndx(0));
}


TEST(Query_FindAllOrParan)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X"); //
    add(ttt, 4, "a");
    add(ttt, 5, "a"); //
    add(ttt, 6, "a");
    add(ttt, 7, "X"); //
    add(ttt, 2, "X");

    // (first == 5 || second == X && first > 2)
    Query q1 = ttt.where().group().equal(0, 5).Or().equal(1, "X").greater(0, 2).end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(2, tv1.get_source_ndx(0));
    CHECK_EQUAL(4, tv1.get_source_ndx(1));
    CHECK_EQUAL(6, tv1.get_source_ndx(2));
}


TEST(Query_FindAllOrNested0)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 3, "X");
    add(ttt, 4, "a");
    add(ttt, 5, "a");
    add(ttt, 11, "X");
    add(ttt, 8, "Y");

    // first > 3 && (first == 5 || second == X)
    Query q1 = ttt.where().greater(0, 3).group().equal(0, 5).Or().equal(1, "X").end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(5, tv1.get_source_ndx(0));
    CHECK_EQUAL(6, tv1.get_source_ndx(1));
}

TEST(Query_FindAllOrNested)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 3, "X");
    add(ttt, 4, "a");
    add(ttt, 5, "a");
    add(ttt, 11, "X");
    add(ttt, 8, "Y");

    // first > 3 && (first == 5 || second == X || second == Y)
    Query q1 =
        ttt.where().greater(0, 3).group().equal(0, 5).Or().equal(1, "X").Or().equal(1, "Y").end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(5, tv1.get_source_ndx(0));
    CHECK_EQUAL(6, tv1.get_source_ndx(1));
    CHECK_EQUAL(7, tv1.get_source_ndx(2));
}

TEST(Query_FindAllOrNestedInnerGroup)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 3, "X");
    add(ttt, 4, "a");
    add(ttt, 5, "a");
    add(ttt, 11, "X");
    add(ttt, 8, "Y");

    // first > 3 && (first == 5 || (second == X || second == Y))
    Query q1 = ttt.where()
                                   .greater(0, 3)
                                   .group()
                                   .equal(0, 5)
                                   .Or()
                                   .group()
                                   .equal(1, "X")
                                   .Or()
                                   .equal(1, "Y")
                                   .end_group()
                                   .end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(5, tv1.get_source_ndx(0));
    CHECK_EQUAL(6, tv1.get_source_ndx(1));
    CHECK_EQUAL(7, tv1.get_source_ndx(2));
}

TEST(Query_FindAllOrPHP)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "Joe");
    add(ttt, 2, "Sara");
    add(ttt, 3, "Jim");

    // (second == Jim || second == Joe) && first = 1
    Query q1 =
        ttt.where().group().equal(1, "Jim").Or().equal(1, "Joe").end_group().equal(0, 1);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
}

TEST(Query_FindAllOr2)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "Joe");
    add(ttt, 2, "Sara");
    add(ttt, 3, "Jim");

    // (second == Jim || second == Joe) && first = 1
    Query q1 =
        ttt.where().group().equal(1, "Jim").Or().equal(1, "Joe").end_group().equal(0, 3);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.get_source_ndx(0));
}


TEST(Query_FindAllParens2)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 3, "X");
    add(ttt, 4, "a");
    add(ttt, 5, "a");
    add(ttt, 11, "X");

    // ()((first > 3()) && (()))
    Query q1 = ttt.where()
                                   .group()
                                   .end_group()
                                   .group()
                                   .group()
                                   .greater(0, 3)
                                   .group()
                                   .end_group()
                                   .end_group()
                                   .group()
                                   .group()
                                   .end_group()
                                   .end_group()
                                   .end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(4, tv1.get_source_ndx(0));
    CHECK_EQUAL(5, tv1.get_source_ndx(1));
    CHECK_EQUAL(6, tv1.get_source_ndx(2));
}

TEST(Query_FindAllParens4)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");
    add(ttt, 3, "X");
    add(ttt, 4, "a");
    add(ttt, 5, "a");
    add(ttt, 11, "X");

    // ()
    Query q1 = ttt.where().group().end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(7, tv1.size());
}


TEST(Query_FindAllBool)
{
    TestTable btt;
    btt.add_column(type_Int, "1");
    btt.add_column(type_Bool, "2");

    add(btt, 1, true);
    add(btt, 2, false);
    add(btt, 3, true);
    add(btt, 3, false);

    Query q1 = btt.where().equal(1, true);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));

    Query q2 = btt.where().equal(1, false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(1, tv2.get_source_ndx(0));
    CHECK_EQUAL(3, tv2.get_source_ndx(1));
}

TEST(Query_FindAllBegins)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 0, "fo");
    add(ttt, 0, "foo");
    add(ttt, 0, "foobar");

    Query q1 = ttt.where().begins_with(1, "foo");
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));
}

TEST(Query_FindAllEnds)
{

    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 0, "barfo");
    add(ttt, 0, "barfoo");
    add(ttt, 0, "barfoobar");

    Query q1 = ttt.where().ends_with(1, "foo");
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
}


TEST(Query_FindAllContains)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 0, "foo");
    add(ttt, 0, "foobar");
    add(ttt, 0, "barfoo");
    add(ttt, 0, "barfoobaz");
    add(ttt, 0, "fo");
    add(ttt, 0, "fobar");
    add(ttt, 0, "barfo");

    Query q1 = ttt.where().contains(1, "foo");
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(1, tv1.get_source_ndx(1));
    CHECK_EQUAL(2, tv1.get_source_ndx(2));
    CHECK_EQUAL(3, tv1.get_source_ndx(3));
}

TEST(Query_FindAllLike)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 0, "foo");
    add(ttt, 0, "foobar");
    add(ttt, 0, "barfoo");
    add(ttt, 0, "barfoobaz");
    add(ttt, 0, "fo");
    add(ttt, 0, "fobar");
    add(ttt, 0, "barfo");

    Query q1 = ttt.where().like(1, "*foo*");
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(1, tv1.get_source_ndx(1));
    CHECK_EQUAL(2, tv1.get_source_ndx(2));
    CHECK_EQUAL(3, tv1.get_source_ndx(3));
}

TEST(Query_FindAllLikeStackOverflow)
{
    std::string str(100000, 'x');
    StringData sd(str);

    Table table;
    table.add_column(type_String, "strings");
    table.add_empty_row();
    table.set_string(0, 0, sd);

    table.where().like(0, sd).find();
}

TEST(Query_FindAllLikeCaseInsensitive)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 0, "Foo");
    add(ttt, 0, "FOOBAR");
    add(ttt, 0, "BaRfOo");
    add(ttt, 0, "barFOObaz");
    add(ttt, 0, "Fo");
    add(ttt, 0, "Fobar");
    add(ttt, 0, "baRFo");

    Query q1 = ttt.where().like(1, "*foo*", false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(1, tv1.get_source_ndx(1));
    CHECK_EQUAL(2, tv1.get_source_ndx(2));
    CHECK_EQUAL(3, tv1.get_source_ndx(3));
}

TEST(Query_Binary)
{
    TestTable t;
    t.add_column(type_Int, "1");
    t.add_column(type_Binary, "2");

    const char bin[64] = {6, 3, 9, 5, 9, 7, 6, 3, 2, 6, 0, 0, 5, 4, 2, 4, 5, 7, 9, 5, 7, 1,
                          1, 2, 0, 8, 3, 8, 0, 9, 6, 8, 4, 7, 3, 4, 9, 5, 2, 3, 6, 2, 7, 4,
                          0, 3, 7, 6, 2, 3, 5, 9, 3, 1, 2, 1, 0, 5, 5, 2, 9, 4, 5, 9};

    const char bin_2[4] = {6, 6, 6, 6}; // Not occuring above

    add(t, 0, BinaryData(bin + 0, 16));
    add(t, 0, BinaryData(bin + 0, 32));
    add(t, 0, BinaryData(bin + 0, 48));
    add(t, 0, BinaryData(bin + 0, 64));
    add(t, 0, BinaryData(bin + 16, 48));
    add(t, 0, BinaryData(bin + 32, 32));
    add(t, 0, BinaryData(bin + 48, 16));
    add(t, 0, BinaryData(bin + 24, 16)); // The "odd ball"
    add(t, 0, BinaryData(bin + 0, 32));  // Repeat an entry

    CHECK_EQUAL(0, t.where().equal(1, BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(1, t.where().equal(1, BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(1, t.where().equal(1, BinaryData(bin + 48, 16)).count());
    CHECK_EQUAL(2, t.where().equal(1, BinaryData(bin + 0, 32)).count());

    CHECK_EQUAL(9, t.where().not_equal(1, BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(8, t.where().not_equal(1, BinaryData(bin + 0, 16)).count());

    CHECK_EQUAL(0, t.where().begins_with(1, BinaryData(bin + 8, 16)).count());
    CHECK_EQUAL(1, t.where().begins_with(1, BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(4, t.where().begins_with(1, BinaryData(bin + 0, 32)).count());
    CHECK_EQUAL(5, t.where().begins_with(1, BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(1, t.where().begins_with(1, BinaryData(bin + 48, 16)).count());
    CHECK_EQUAL(9, t.where().begins_with(1, BinaryData(bin + 0, 0)).count());

    CHECK_EQUAL(0, t.where().ends_with(1, BinaryData(bin + 40, 16)).count());
    CHECK_EQUAL(1, t.where().ends_with(1, BinaryData(bin + 32, 16)).count());
    CHECK_EQUAL(3, t.where().ends_with(1, BinaryData(bin + 32, 32)).count());
    CHECK_EQUAL(4, t.where().ends_with(1, BinaryData(bin + 48, 16)).count());
    CHECK_EQUAL(1, t.where().ends_with(1, BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(9, t.where().ends_with(1, BinaryData(bin + 64, 0)).count());

    CHECK_EQUAL(0, t.where().contains(1, BinaryData(bin_2)).count());
    CHECK_EQUAL(5, t.where().contains(1, BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(5, t.where().contains(1, BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(4, t.where().contains(1, BinaryData(bin + 24, 16)).count());
    CHECK_EQUAL(4, t.where().contains(1, BinaryData(bin + 32, 16)).count());
    CHECK_EQUAL(9, t.where().contains(1, BinaryData(bin + 0, 0)).count());

    {
        TableView tv = t.where().equal(1, BinaryData(bin + 0, 32)).find_all();
        if (tv.size() == 2) {
            CHECK_EQUAL(1, tv.get_source_ndx(0));
            CHECK_EQUAL(8, tv.get_source_ndx(1));
        }
        else
            CHECK(false);
    }

    {
        TableView tv = t.where().contains(1, BinaryData(bin + 24, 16)).find_all();
        if (tv.size() == 4) {
            CHECK_EQUAL(2, tv.get_source_ndx(0));
            CHECK_EQUAL(3, tv.get_source_ndx(1));
            CHECK_EQUAL(4, tv.get_source_ndx(2));
            CHECK_EQUAL(7, tv.get_source_ndx(3));
        }
        else
            CHECK(false);
    }
}


TEST(Query_Enums)
{
    TestTable t;
    t.add_column(type_Int, "1");
    t.add_column(type_String, "2");

    for (size_t i = 0; i < 5; ++i) {
        add(t, 1, "abd");
        add(t, 2, "eftg");
        add(t, 5, "hijkl");
        add(t, 8, "mnopqr");
        add(t, 9, "stuvxyz");
    }

    t.optimize();

    Query q1 = t.where().equal(1, "eftg");
    TableView tv1 = q1.find_all();

    CHECK_EQUAL(5, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(6, tv1.get_source_ndx(1));
    CHECK_EQUAL(11, tv1.get_source_ndx(2));
    CHECK_EQUAL(16, tv1.get_source_ndx(3));
    CHECK_EQUAL(21, tv1.get_source_ndx(4));
}


#define uY "\x0CE\x0AB"            // greek capital letter upsilon with dialytika (U+03AB)
#define uYd "\x0CE\x0A5\x0CC\x088" // decomposed form (Y followed by two dots)
#define uy "\x0CF\x08B"            // greek small letter upsilon with dialytika (U+03AB)
#define uyd "\x0cf\x085\x0CC\x088" // decomposed form (Y followed by two dots)

#define uA "\x0c3\x085"       // danish capital A with ring above (as in BLAABAERGROED)
#define uAd "\x041\x0cc\x08a" // decomposed form (A (41) followed by ring)
#define ua "\x0c3\x0a5"       // danish lower case a with ring above (as in blaabaergroed)
#define uad "\x061\x0cc\x08a" // decomposed form (a (41) followed by ring)

TEST(Query_CaseSensitivity)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, "BLAAbaergroed");
    add(ttt, 1, "BLAAbaergroedandMORE");
    add(ttt, 1, "BLAAbaergroedZ");
    add(ttt, 1, "BLAAbaergroedZ");
    add(ttt, 1, "BLAAbaergroedZ");

    Query q1 = ttt.where().equal(1, "blaabaerGROED", false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));

    Query q2 = ttt.where().equal(1, "blaabaerGROEDz", false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(3, tv2.size());

    ttt.add_search_index(1);

    Query q3 = ttt.where().equal(1, "blaabaerGROEDz", false);
    TableView tv3 = q3.find_all();
    CHECK_EQUAL(3, tv3.size());
}

#if (defined(_WIN32) || defined(__WIN32__) || defined(_WIN64))

TEST(Query_Unicode2)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, uY);
    add(ttt, 1, uYd);
    add(ttt, 1, uy);
    add(ttt, 1, uyd);

    Query q1 = ttt.where().equal(1, uY, false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));

    Query q2 = ttt.where().equal(1, uYd, false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(1, tv2.get_source_ndx(0));
    CHECK_EQUAL(3, tv2.get_source_ndx(1));

    Query q3 = ttt.where().equal(1, uYd, true);
    TableView tv3 = q3.find_all();
    CHECK_EQUAL(1, tv3.size());
    CHECK_EQUAL(1, tv3.get_source_ndx(0));
}


TEST(Query_Unicode3)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 1, uA);
    add(ttt, 1, uAd);
    add(ttt, 1, ua);
    add(ttt, 1, uad);

    Query q1 = ttt.where().equal(1, uA, false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));

    Query q2 = ttt.where().equal(1, ua, false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(0, tv2.get_source_ndx(0));
    CHECK_EQUAL(2, tv2.get_source_ndx(1));


    Query q3 = ttt.where().equal(1, uad, false);
    TableView tv3 = q3.find_all();
    CHECK_EQUAL(2, tv3.size());
    CHECK_EQUAL(1, tv3.get_source_ndx(0));
    CHECK_EQUAL(3, tv3.get_source_ndx(1));

    Query q4 = ttt.where().equal(1, uad, true);
    TableView tv4 = q4.find_all();
    CHECK_EQUAL(1, tv4.size());
    CHECK_EQUAL(3, tv4.get_source_ndx(0));
}

#endif

TEST(Query_FindAllBeginsUnicode)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 0, uad "fo");
    add(ttt, 0, uad "foo");
    add(ttt, 0, uad "foobar");

    Query q1 = ttt.where().begins_with(1, uad "foo");
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));
}


TEST(Query_FindAllEndsUnicode)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 0, "barfo");
    add(ttt, 0, "barfoo" uad);
    add(ttt, 0, "barfoobar");

    Query q1 = ttt.where().ends_with(1, "foo" uad);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));

    Query q2 = ttt.where().ends_with(1, "foo" uAd, false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(1, tv2.size());
    CHECK_EQUAL(1, tv2.get_source_ndx(0));
}


TEST(Query_FindAllContainsUnicode)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    add(ttt, 0, uad "foo");
    add(ttt, 0, uad "foobar");
    add(ttt, 0, "bar" uad "foo");
    add(ttt, 0, uad "bar" uad "foobaz");
    add(ttt, 0, uad "fo");
    add(ttt, 0, uad "fobar");
    add(ttt, 0, uad "barfo");

    Query q1 = ttt.where().contains(1, uad "foo");
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(1, tv1.get_source_ndx(1));
    CHECK_EQUAL(2, tv1.get_source_ndx(2));
    CHECK_EQUAL(3, tv1.get_source_ndx(3));

    Query q2 = ttt.where().contains(1, uAd "foo", false);
    TableView tv2 = q1.find_all();
    CHECK_EQUAL(4, tv2.size());
    CHECK_EQUAL(0, tv2.get_source_ndx(0));
    CHECK_EQUAL(1, tv2.get_source_ndx(1));
    CHECK_EQUAL(2, tv2.get_source_ndx(2));
    CHECK_EQUAL(3, tv2.get_source_ndx(3));
}

TEST(Query_SyntaxCheck)
{
    TestTable ttt;
    ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    std::string s;

    add(ttt, 1, "a");
    add(ttt, 2, "a");
    add(ttt, 3, "X");

    Query q1 = ttt.where().equal(0, 2).end_group();
    s = q1.validate();
    CHECK(s != "");

    Query q2 = ttt.where().group().group().equal(0, 2).end_group();
    s = q2.validate();
    CHECK(s != "");

    Query q3 = ttt.where().equal(0, 2).Or();
    s = q3.validate();
    CHECK(s != "");

    Query q4 = ttt.where().Or().equal(0, 2);
    s = q4.validate();
    CHECK(s != "");

    Query q5 = ttt.where().equal(0, 2);
    s = q5.validate();
    CHECK(s == "");

    Query q6 = ttt.where().group().equal(0, 2);
    s = q6.validate();
    CHECK(s != "");

    // FIXME: Work is currently underway to fully support locale
    // indenepdent case folding as defined by Unicode. Reenable this test
    // when is becomes available.
    /*
    Query q7 = ttt.where().equal(1, "\xa0", false);
#ifdef REALM_DEBUG
    s = q7.verify();
    CHECK(s != "");
#endif
    */
}

TEST(Query_SubtableSyntaxCheck)
{
    Group group;
    TableRef table = group.add_table("test");
    std::string s;

    // Create specification with sub-table
    DescriptorRef subdesc;
    table->add_column(type_Int, "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table, "third", &subdesc);
    subdesc->add_column(type_Int, "sub_first");
    subdesc->add_column(type_String, "sub_second");

    // Main table
    table->insert_empty_row(0);
    table->set_int(0, 0, 111);
    table->set_string(1, 0, "this");

    table->insert_empty_row(1);
    table->set_int(0, 1, 222);
    table->set_string(1, 1, "is");

    table->insert_empty_row(2);
    table->set_int(0, 2, 333);
    table->set_string(1, 2, "a test");

    table->insert_empty_row(3);
    table->set_int(0, 3, 444);
    table->set_string(1, 3, "of queries");


    // Sub tables
    TableRef subtable = table->get_subtable(2, 0);
    subtable->insert_empty_row(0);
    subtable->set_int(0, 0, 11);
    subtable->set_string(1, 0, "a");

    subtable = table->get_subtable(2, 1);
    subtable->insert_empty_row(0);
    subtable->set_int(0, 0, 22);
    subtable->set_string(1, 0, "b");
    subtable->insert_empty_row(1);
    subtable->set_int(0, 1, 33);
    subtable->set_string(1, 1, "c");

    subtable = table->get_subtable(2, 2);
    subtable->insert_empty_row(0);
    subtable->set_int(0, 0, 44);
    subtable->set_string(1, 0, "d");

    subtable = table->get_subtable(2, 3);
    subtable->insert_empty_row(0);
    subtable->set_int(0, 0, 55);
    subtable->set_string(1, 0, "e");

    Query q1 = table->where();
    q1.subtable(2);
    q1.greater(0, 50);
    s = q1.validate();
    CHECK(s != "");

    Query q2 = table->where();
    q2.subtable(2);
    q2.greater(0, 50);
    q2.end_subtable();
    s = q2.validate();
    CHECK(s == "");

    Query q3 = table->where();
    q3.greater(0, 50);
    q3.end_subtable();
    s = q3.validate();
    CHECK(s != "");
}


TEST(Query_TestTV_where)
{
    // When using .where(&tv), tv can have any order, and the resulting view will retain its order
    TestTable t;
    t.add_column(type_Int, "1");
    t.add_column(type_String, "2");

    add(t, 1, "a");
    add(t, 2, "a");
    add(t, 3, "c");

    TableView v = t.where().greater(0, 1).find_all();

    Query q1 = t.where(&v);
    CHECK_EQUAL(2, q1.count());

    Query q3 = t.where(&v).equal(1, "a");
    CHECK_EQUAL(1, q3.count());

    Query q4 = t.where(&v).between(0, 3, 6);
    CHECK_EQUAL(1, q4.count());
}

TEST(Query_SumMinMaxAvg)
{
    TestTable t;

    t.add_column(type_Int, "1");
    t.add_column(type_String, "2");
    t.add_column(type_OldDateTime, "3");
    t.add_column(type_Float, "4");
    t.add_column(type_Double, "5");

    add(t, 1, "a", OldDateTime(100), 1.0f, 1.0);
    add(t, 1, "a", OldDateTime(100), 1.0f, 1.0);
    add(t, 1, "a", OldDateTime(100), 1.0f, 1.0);
    add(t, 1, "a", OldDateTime(100), 1.0f, 1.0);
    add(t, 2, "b", OldDateTime(300), 3.0f, 3.0);
    add(t, 3, "c", OldDateTime(50), 5.0f, 5.0);
    add(t, 0, "a", OldDateTime(100), 1.0f, 1.0);
    add(t, 0, "b", OldDateTime(3000), 30.0f, 30.0);
    add(t, 0, "c", OldDateTime(5), 0.5f, 0.5);

    CHECK_EQUAL(9, t.where().sum_int(0));

    CHECK_EQUAL(0, t.where().minimum_int(0));
    CHECK_EQUAL(3, t.where().maximum_int(0));

    size_t resindex = not_found;

    t.where().maximum_int(0, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(5, resindex);

    t.where().minimum_int(0, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(6, resindex);

    t.where().maximum_float(3, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(7, resindex);

    t.where().minimum_float(3, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(8, resindex);

    t.where().maximum_double(4, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(7, resindex);

    t.where().minimum_double(4, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(8, resindex);

    // Now with condition (tests another code path in Array::minmax())
    t.where().not_equal(0, 0).minimum_double(4, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(0, resindex);

    t.where().not_equal(0, 0).minimum_float(3, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(0, resindex);

    t.where().not_equal(0, 0).minimum_olddatetime(2, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(5, resindex);

    t.where().not_equal(0, 0).maximum_olddatetime(2, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(4, resindex);

    CHECK_APPROXIMATELY_EQUAL(1, t.where().average_int(0), 0.001);

    CHECK_EQUAL(OldDateTime(3000), t.where().maximum_olddatetime(2));
    CHECK_EQUAL(OldDateTime(5), t.where().minimum_olddatetime(2));

    size_t cnt;
    CHECK_EQUAL(0, t.where().sum_int(0, &cnt, 0, 0));
    CHECK_EQUAL(0, cnt);
    CHECK_EQUAL(0, t.where().sum_int(0, &cnt, 1, 1));
    CHECK_EQUAL(0, cnt);
    CHECK_EQUAL(0, t.where().sum_int(0, &cnt, 2, 2));
    CHECK_EQUAL(0, cnt);

    CHECK_EQUAL(1, t.where().sum_int(0, &cnt, 0, 1));
    CHECK_EQUAL(1, cnt);
    CHECK_EQUAL(2, t.where().sum_int(0, &cnt, 4, 5));
    CHECK_EQUAL(1, cnt);
    CHECK_EQUAL(3, t.where().sum_int(0, &cnt, 5, 6));
    CHECK_EQUAL(1, cnt);

    CHECK_EQUAL(2, t.where().sum_int(0, &cnt, 0, 2));
    CHECK_EQUAL(2, cnt);
    CHECK_EQUAL(5, t.where().sum_int(0, &cnt, 1, 5));
    CHECK_EQUAL(4, cnt);

    CHECK_EQUAL(3, t.where().sum_int(0, &cnt, 0, 3));
    CHECK_EQUAL(3, cnt);
    CHECK_EQUAL(9, t.where().sum_int(0, &cnt, 0, size_t(-1)));
    CHECK_EQUAL(9, cnt);
}

TEST(Query_SumMinMaxAvg_where)
{
    TestTable t;

    t.add_column(type_Int, "1");
    t.add_column(type_String, "2");
    t.add_column(type_OldDateTime, "3");
    t.add_column(type_Float, "4");
    t.add_column(type_Double, "5");

    add(t, 1, "a", OldDateTime(100), 1.0f, 1.0);
    add(t, 1, "a", OldDateTime(100), 1.0f, 1.0);
    add(t, 1, "a", OldDateTime(100), 1.0f, 1.0);
    add(t, 1, "a", OldDateTime(100), 1.0f, 1.0);
    add(t, 2, "b", OldDateTime(300), 3.0f, 3.0);
    add(t, 3, "c", OldDateTime(50), 5.0f, 5.0);
    add(t, 0, "a", OldDateTime(100), 1.0f, 1.0);
    add(t, 0, "b", OldDateTime(3000), 30.0f, 30.0);
    add(t, 0, "c", OldDateTime(5), 0.5f, 0.5);

    TableView v = t.where().find_all();

    CHECK_EQUAL(9, t.where(&v).sum_int(0));

    CHECK_EQUAL(0, t.where(&v).minimum_int(0));
    CHECK_EQUAL(3, t.where(&v).maximum_int(0));

    size_t resindex = not_found;

    t.where(&v).maximum_int(0, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(5, resindex);

    t.where(&v).minimum_int(0, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(6, resindex);

    t.where(&v).maximum_float(3, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(7, resindex);

    t.where(&v).minimum_float(3, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(8, resindex);

    t.where(&v).maximum_double(4, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(7, resindex);

    t.where(&v).minimum_double(4, nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(8, resindex);

    CHECK_APPROXIMATELY_EQUAL(1, t.where(&v).average_int(0), 0.001);

    size_t cnt;
    CHECK_EQUAL(0, t.where(&v).sum_int(0, &cnt, 0, 0));
    CHECK_EQUAL(0, cnt);
    CHECK_EQUAL(0, t.where(&v).sum_int(0, &cnt, 1, 1));
    CHECK_EQUAL(0, cnt);
    CHECK_EQUAL(0, t.where(&v).sum_int(0, &cnt, 2, 2));
    CHECK_EQUAL(0, cnt);

    CHECK_EQUAL(1, t.where(&v).sum_int(0, &cnt, 0, 1));
    CHECK_EQUAL(1, cnt);
    CHECK_EQUAL(2, t.where(&v).sum_int(0, &cnt, 4, 5));
    CHECK_EQUAL(1, cnt);
    CHECK_EQUAL(3, t.where(&v).sum_int(0, &cnt, 5, 6));
    CHECK_EQUAL(1, cnt);

    CHECK_EQUAL(2, t.where(&v).sum_int(0, &cnt, 0, 2));
    CHECK_EQUAL(2, cnt);
    CHECK_EQUAL(5, t.where(&v).sum_int(0, &cnt, 1, 5));
    CHECK_EQUAL(4, cnt);

    CHECK_EQUAL(3, t.where(&v).sum_int(0, &cnt, 0, 3));
    CHECK_EQUAL(3, cnt);
    CHECK_EQUAL(9, t.where(&v).sum_int(0, &cnt, 0, size_t(-1)));
    CHECK_EQUAL(9, cnt);
}

TEST(Query_Avg)
{
    TestTable t;
    t.add_column(type_Int, "1");
    t.add_column(type_String, "2");

    size_t cnt;
    add(t, 10, "a");
    CHECK_EQUAL(10, t.where().average_int(0));
    add(t, 30, "b");
    CHECK_EQUAL(20, t.where().average_int(0));

    CHECK_EQUAL(0, t.where().average_int(0, nullptr, 0, 0));   // none
    CHECK_EQUAL(0, t.where().average_int(0, nullptr, 1, 1));   // none
    CHECK_EQUAL(20, t.where().average_int(0, nullptr, 0, 2));  // both
    CHECK_EQUAL(20, t.where().average_int(0, nullptr, 0, -1)); // both

    CHECK_EQUAL(10, t.where().average_int(0, &cnt, 0, 1)); // first

    CHECK_EQUAL(30, t.where().sum_int(0, nullptr, 1, 2));     // second
    CHECK_EQUAL(30, t.where().average_int(0, nullptr, 1, 2)); // second
}

TEST(Query_Avg2)
{
    TestTable t;
    t.add_column(type_Int, "1");
    t.add_column(type_String, "2");

    size_t cnt;

    add(t, 10, "a");
    add(t, 100, "b");
    add(t, 20, "a");
    add(t, 100, "b");
    add(t, 100, "b");
    add(t, 30, "a");
    Query q = t.where().equal(1, "a");
    CHECK_EQUAL(3, q.count());
    q.sum_int(0);

    CHECK_EQUAL(60, t.where().equal(1, "a").sum_int(0));

    CHECK_EQUAL(0, t.where().equal(1, "a").average_int(0, &cnt, 0, 0));
    CHECK_EQUAL(0, t.where().equal(1, "a").average_int(0, &cnt, 1, 1));
    CHECK_EQUAL(0, t.where().equal(1, "a").average_int(0, &cnt, 2, 2));
    CHECK_EQUAL(0, cnt);

    CHECK_EQUAL(10, t.where().equal(1, "a").average_int(0, &cnt, 0, 1));
    CHECK_EQUAL(20, t.where().equal(1, "a").average_int(0, &cnt, 1, 5));
    CHECK_EQUAL(30, t.where().equal(1, "a").average_int(0, &cnt, 5, 6));
    CHECK_EQUAL(1, cnt);

    CHECK_EQUAL(15, t.where().equal(1, "a").average_int(0, &cnt, 0, 3));
    CHECK_EQUAL(20, t.where().equal(1, "a").average_int(0, &cnt, 2, 5));
    CHECK_EQUAL(1, cnt);

    CHECK_EQUAL(20, t.where().equal(1, "a").average_int(0, &cnt));
    CHECK_EQUAL(3, cnt);
    CHECK_EQUAL(15, t.where().equal(1, "a").average_int(0, &cnt, 0, 3));
    CHECK_EQUAL(2, cnt);
    CHECK_EQUAL(20, t.where().equal(1, "a").average_int(0, &cnt, 0, size_t(-1)));
    CHECK_EQUAL(3, cnt);
}


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
    CHECK_EQUAL(1, v.get_source_ndx(0));

    q = table.column<StringData>(0) != realm::null();
    v = q.find_all();
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(0, v.get_source_ndx(0));
    CHECK_EQUAL(2, v.get_source_ndx(1));

    // contrary to SQL, comparisons with realm::null() can be true in Realm (todo, discuss if we want this behaviour)
    q = table.column<StringData>(0) != StringData("Albertslund");
    v = q.find_all();
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(1, v.get_source_ndx(0));
    CHECK_EQUAL(2, v.get_source_ndx(1));

    q = table.column<StringData>(0) == "";
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(2, v.get_source_ndx(0));

    // Medium strings (16+)
    table.set_string(0, 0, "AlbertslundAlbertslundAlbert");

    q = table.column<StringData>(0) == realm::null();
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_source_ndx(0));

    q = table.column<StringData>(0) == "";
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(2, v.get_source_ndx(0));

    // Long strings (64+)
    table.set_string(0, 0, "AlbertslundAlbertslundAlbertslundAlbertslundAlbertslundAlbertslundAlbertslund");
    q = table.column<StringData>(0) == realm::null();
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_source_ndx(0));

    q = table.column<StringData>(0) == "";
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(2, v.get_source_ndx(0));
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
    CHECK_EQUAL(0, t.get_source_ndx(0));

    t = (BinaryData() == table.column<BinaryData>(0)).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(0, t.get_source_ndx(0));

    t = (table.column<BinaryData>(0) == BinaryData("", 0)).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(1, t.get_source_ndx(0));

    t = (BinaryData("", 0) == table.column<BinaryData>(0)).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(1, t.get_source_ndx(0));

    t = (table.column<BinaryData>(0) != BinaryData("", 0)).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(0, t.get_source_ndx(0));
    CHECK_EQUAL(2, t.get_source_ndx(1));

    t = (BinaryData("", 0) != table.column<BinaryData>(0)).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(0, t.get_source_ndx(0));
    CHECK_EQUAL(2, t.get_source_ndx(1));


    // Old syntax
    t = table.where().equal(0, BinaryData()).find_all();
    CHECK_EQUAL(0, t.get_source_ndx(0));
    CHECK_EQUAL(1, t.size());

    t = table.where().equal(0, BinaryData("", 0)).find_all();
    CHECK_EQUAL(1, t.get_source_ndx(0));
    CHECK_EQUAL(1, t.size());

    t = table.where().equal(0, BinaryData("foo")).find_all();
    CHECK_EQUAL(2, t.get_source_ndx(0));
    CHECK_EQUAL(1, t.size());

    t = table.where().not_equal(0, BinaryData()).find_all();
    CHECK_EQUAL(1, t.get_source_ndx(0));
    CHECK_EQUAL(2, t.get_source_ndx(1));
    CHECK_EQUAL(2, t.size());

    t = table.where().not_equal(0, BinaryData("", 0)).find_all();
    CHECK_EQUAL(0, t.get_source_ndx(0));
    CHECK_EQUAL(2, t.get_source_ndx(1));
    CHECK_EQUAL(2, t.size());

    t = table.where().begins_with(0, BinaryData()).find_all();
    CHECK_EQUAL(3, t.size());

    t = table.where().begins_with(0, BinaryData("", 0)).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(1, t.get_source_ndx(0));
    CHECK_EQUAL(2, t.get_source_ndx(1));

    t = table.where().begins_with(0, BinaryData("foo")).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(2, t.get_source_ndx(0));

    t = table.where().ends_with(0, BinaryData()).find_all();
    CHECK_EQUAL(3, t.size());

    t = table.where().ends_with(0, BinaryData("", 0)).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(1, t.get_source_ndx(0));
    CHECK_EQUAL(2, t.get_source_ndx(1));

    t = table.where().ends_with(0, BinaryData("foo")).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(2, t.get_source_ndx(0));
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
    CHECK_EQUAL(0, t.get_source_ndx(0));
    CHECK_EQUAL(3, t.get_source_ndx(1));

    t = table.where().equal(0, 0).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(1, t.get_source_ndx(0));

    t = table.where().equal(0, 123).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(2, t.get_source_ndx(0));

    t = table.where().not_equal(0, null{}).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(1, t.get_source_ndx(0));
    CHECK_EQUAL(2, t.get_source_ndx(1));

    t = table.where().not_equal(0, 0).find_all();
    CHECK_EQUAL(3, t.size());
    CHECK_EQUAL(0, t.get_source_ndx(0));
    CHECK_EQUAL(2, t.get_source_ndx(1));
    CHECK_EQUAL(3, t.get_source_ndx(2));

    t = table.where().greater(0, 0).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(2, t.get_source_ndx(0));
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
        if (tv.get_source_ndx(it - indexes.begin()) != *it) {
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
        CHECK_EQUAL(tv.get_source_ndx(0), 1);
        CHECK_EQUAL(tv.get_source_ndx(1), 0);
        CHECK_EQUAL(tv.get_source_ndx(2), 2);

        tv.sort(i, false);
        CHECK_EQUAL(tv.get_source_ndx(0), 2);
        CHECK_EQUAL(tv.get_source_ndx(1), 0);
        CHECK_EQUAL(tv.get_source_ndx(2), 1);
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
    q = table2->column<LinkList>(col_linklist).count() == 0;
    match = q.find();
    CHECK_EQUAL(0, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).count() == 1;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).count() >= 1;
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
    auto link_count = table2->column<LinkList>(col_linklist).count();
    size_t match_count = (link_count == 0).count();
    CHECK_EQUAL(1, match_count);

    match_count = (link_count >= 1).count();
    CHECK_EQUAL(2, match_count);


    // Verify that combining the count expression with other queries on the same table works.
    q = table2->column<LinkList>(col_linklist).count() == 1 && table2->column<Int>(col_int) == 1;
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

    q = table2->column<LinkList>(col_linklist).column<Int>(0).min() == 123;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Int>(0).min() == 456;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Int>(0).min() == null();
    match = q.find();
    CHECK_EQUAL(0, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LinkList>(col_linklist).column<Float>(1).min() == 123.0f;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Float>(1).min() == 456.0f;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LinkList>(col_linklist).column<Double>(2).min() == 123.0;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Double>(2).min() == 456.0;
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

    q = table2->column<LinkList>(col_linklist).column<Int>(0).max() == 789;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Int>(0).max() == 456;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Int>(0).max() == null();
    match = q.find();
    CHECK_EQUAL(0, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Int>(0).max() == table2->link(col_link).column<Int>(0);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Int>(0).max() == table2->column<Double>(col_double);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LinkList>(col_linklist).column<Float>(1).max() == 789.0f;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Float>(1).max() == 456.0f;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LinkList>(col_linklist).column<Double>(2).max() == 789.0;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Double>(2).max() == 456.0;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    // Sum.
    // Floating point results below may be inexact for some combination of architectures, compilers, and compiler
    // flags.

    q = table2->column<LinkList>(col_linklist).column<Int>(0).sum() == 1245;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Int>(0).sum() == 456;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Int>(0).sum() == table2->link(col_link).column<Int>(0);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Int>(0).sum() == table2->column<Double>(col_double);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LinkList>(col_linklist).column<Float>(1).sum() == 1245.0f;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Float>(1).sum() == 456.0f;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LinkList>(col_linklist).column<Double>(2).sum() == 1245.0;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Double>(2).sum() == 456.0;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    // Average.
    // Floating point results below may be inexact for some combination of architectures, compilers, and compiler
    // flags.

    q = table2->column<LinkList>(col_linklist).column<Int>(0).average() == 622.5;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Int>(0).average() == 456;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Int>(0).average() == null();
    match = q.find();
    CHECK_EQUAL(0, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Int>(0).average() < table2->link(col_link).column<Int>(0);
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Int>(0).average() == table2->column<Double>(col_double);
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LinkList>(col_linklist).column<Float>(1).average() == 622.5;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Float>(1).average() == 456.0f;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);


    q = table2->column<LinkList>(col_linklist).column<Double>(2).average() == 622.5;
    match = q.find();
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(3, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    q = table2->column<LinkList>(col_linklist).column<Double>(2).average() == 456.0;
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

TEST(Query_ReferDeletedLinkView)
{
    // Queries and TableViews that depend on a deleted LinkList will now produce valid empty-like results
    // (find() returns npos, find_all() returns empty TableView, sum() returns 0, etc.).
    // They will no longer throw exceptions or crash.
    Group group;
    TableRef table = group.add_table("table");
    table->add_column_link(type_LinkList, "children", *table);
    table->add_column(type_Int, "age");
    table->add_empty_row();
    table->set_int(1, 0, 123);
    LinkViewRef links = table->get_linklist(0, 0);
    Query q = table->where(links);
    TableView tv = q.find_all();

    // TableView that depends on LinkView soon to be deleted
    TableView tv_sorted = links->get_sorted_view(1);

    // First test depends_on_deleted_object()
    CHECK(!tv_sorted.depends_on_deleted_object());
    TableView tv2 = table->where(&tv).find_all();
    CHECK(!tv2.depends_on_deleted_object());

    // Delete LinkList so LinkView gets detached
    table->move_last_over(0);
    CHECK(!links->is_attached());
    CHECK(tv_sorted.depends_on_deleted_object());

    // See if "Query that depends on LinkView" returns sane "empty"-like values
    CHECK_EQUAL(q.find_all().size(), 0);
    CHECK_EQUAL(q.find(), npos);
    CHECK_EQUAL(q.sum_int(1), 0);
    CHECK_EQUAL(q.count(), 0);
    size_t rows;
    q.average_int(1, &rows);
    CHECK_EQUAL(rows, 0);

    tv_sorted.sync_if_needed();
    // See if "TableView that depends on LinkView" returns sane "empty"-like values
    tv_sorted.average_int(1, &rows);
    CHECK_EQUAL(rows, 0);

    // Now check a "Query that depends on (TableView that depends on LinkView)"
    Query q2 = table->where(&tv_sorted);
    CHECK_EQUAL(q2.count(), 0);
    CHECK_EQUAL(q2.find(), npos);

    CHECK(!links->is_attached());
    tv.sync_if_needed();

    // PLEASE NOTE that 'tv' will still return true in this case! Even though it indirectly depends on
    // the LinkView through multiple levels!
    CHECK(tv.is_attached());

    // Before executing any methods on a LinkViewRef, you must still always check is_attached(). If you
    // call links->add() on a deleted LinkViewRef (where is_attached() == false), it will assert
    CHECK(!links->is_attached());
}

TEST(Query_SubQueries)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");

    table2->add_column(type_Int, "col1");
    table2->add_column(type_String, "str2");

    // add some rows
    table1->add_empty_row();
    table1->set_int(0, 0, 100);
    table1->set_string(1, 0, "foo");
    table1->add_empty_row();
    table1->set_int(0, 1, 200);
    table1->set_string(1, 1, "!");
    table1->add_empty_row();
    table1->set_int(0, 2, 300);
    table1->set_string(1, 2, "bar");

    table2->add_empty_row();
    table2->set_int(0, 0, 400);
    table2->set_string(1, 0, "hello");
    table2->add_empty_row();
    table2->set_int(0, 1, 500);
    table2->set_string(1, 1, "world");
    table2->add_empty_row();
    table2->set_int(0, 2, 600);
    table2->set_string(1, 2, "!");
    table2->add_empty_row();
    table2->set_int(0, 2, 600);
    table2->set_string(1, 1, "world");


    size_t col_link2 = table1->add_column_link(type_LinkList, "link", *table2);

    // set some links
    LinkViewRef links1;

    links1 = table1->get_linklist(col_link2, 0);
    links1->add(1);

    links1 = table1->get_linklist(col_link2, 1);
    links1->add(1);
    links1->add(2);


    size_t match;
    Query q;

    // The linked rows for rows 0 and 2 all match ("world", 500). Row 2 does by virtue of having no rows.
    q = table1->column<LinkList>(col_link2, table2->column<String>(1) == "world" && table2->column<Int>(0) == 500)
            .count() == table1->column<LinkList>(col_link2).count();
    match = q.find();
    CHECK_EQUAL(0, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    // No linked rows match ("world, 600).
    q = table1->column<LinkList>(col_link2, table2->column<String>(1) == "world" && table2->column<Int>(0) == 600)
            .count() >= 1;
    match = q.find();
    CHECK_EQUAL(not_found, match);

    // Rows 0 and 1 both have at least one linked row that matches ("world", 500).
    q = table1->column<LinkList>(col_link2, table2->column<String>(1) == "world" && table2->column<Int>(0) == 500)
            .count() >= 1;
    match = q.find();
    CHECK_EQUAL(0, match);
    match = q.find(match + 1);
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    // Row 1 has at least one linked row that matches ("!", 600).
    q = table1->column<LinkList>(col_link2, table2->column<String>(1) == "!" && table2->column<Int>(0) == 600)
            .count() >= 1;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    // Row 1 has two linked rows that contain either "world" or 600.
    q = table1->column<LinkList>(col_link2, table2->column<String>(1) == "world" || table2->column<Int>(0) == 600)
            .count() == 2;
    match = q.find();
    CHECK_EQUAL(1, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);

    // Rows 0 and 2 have at most one linked row that contains either "world" or 600. Row 2 does by virtue of having no
    // rows.
    q = table1->column<LinkList>(col_link2, table2->column<String>(1) == "world" || table2->column<Int>(0) == 600)
            .count() <= 1;
    match = q.find();
    CHECK_EQUAL(0, match);
    match = q.find(match + 1);
    CHECK_EQUAL(2, match);
    match = q.find(match + 1);
    CHECK_EQUAL(not_found, match);
}

// Ensure that Query's move constructor and move assignment operator don't result in
// a TableView owned by the query being double-deleted when the queries are destroyed.
TEST(Query_MoveDoesntDoubleDelete)
{
    Table table;

    {
        Query q1(table, std::unique_ptr<TableViewBase>(new TableView()));
        Query q2 = std::move(q1);
    }

    {
        Query q1(table, std::unique_ptr<TableViewBase>(new TableView()));
        Query q2;
        q2 = std::move(q1);
    }
}

TEST(Query_Timestamp)
{
    size_t match;
    Table table;
    table.add_column(type_Timestamp, "first", true);
    table.add_column(type_Timestamp, "second", true);
    Columns<Timestamp> first = table.column<Timestamp>(0);
    Columns<Timestamp> second = table.column<Timestamp>(1);

    table.add_empty_row(6);
    table.set_timestamp(0, 0, Timestamp(111, 222));
    table.set_timestamp(0, 1, Timestamp(111, 333));
    table.set_timestamp(0, 2, Timestamp(333, 444));
    table.set_timestamp(0, 3, Timestamp{});
    table.set_timestamp(0, 4, Timestamp(0, 0));
    table.set_timestamp(0, 5, Timestamp(-1000, 0));

    table.set_timestamp(1, 2, Timestamp(222, 222));

    CHECK(table.get_timestamp(0, 0) == Timestamp(111, 222));

    match = (first == Timestamp(111, 222)).find();
    CHECK_EQUAL(match, 0);

    match = (first != Timestamp(111, 222)).find();
    CHECK_EQUAL(match, 1);

    match = (first > Timestamp(111, 222)).find();
    CHECK_EQUAL(match, 1);

    match = (first < Timestamp(111, 333)).find();
    CHECK_EQUAL(match, 0);

    match = (first == Timestamp(0, 0)).find();
    CHECK_EQUAL(match, 4);

    match = (first < Timestamp(111, 333)).find();
    CHECK_EQUAL(match, 0);

    match = (first < Timestamp(0, 0)).find();
    CHECK_EQUAL(match, 5);

    // Note: .count(), not find()
    match = (first < Timestamp(0, 0)).count();
    CHECK_EQUAL(match, 1);

    match = (first != Timestamp{}).count();
    CHECK_EQUAL(match, 5);

    match = (first != null{}).count();
    CHECK_EQUAL(match, 5);

    match = (first != Timestamp(0, 0)).count();
    CHECK_EQUAL(match, 5);

    match = (first < Timestamp(-100, 0)).find();
    CHECK_EQUAL(match, 5);

    // Left-hand-side being Timestamp() constant, right being column
    match = (Timestamp(111, 222) == first).find();
    CHECK_EQUAL(match, 0);

    match = (Timestamp{} == first).find();
    CHECK_EQUAL(match, 3);

    match = (Timestamp(111, 222) > first).find();
    CHECK_EQUAL(match, 4);

    match = (Timestamp(111, 333) < first).find();
    CHECK_EQUAL(match, 2);

    match = (Timestamp(111, 222) >= first).find();
    CHECK_EQUAL(match, 0);

    match = (Timestamp(111, 111) >= first).find();
    CHECK_EQUAL(match, 4);

    match = (Timestamp(333, 444) <= first).find();
    CHECK_EQUAL(match, 2);

    match = (Timestamp(111, 300) <= first).find();
    CHECK_EQUAL(match, 1);

    match = (Timestamp(111, 222) != first).find();
    CHECK_EQUAL(match, 1);

    // Compare column with self
    match = (first == first).find();
    CHECK_EQUAL(match, 0);

    match = (first != first).find();
    CHECK_EQUAL(match, npos);

    match = (first > first).find();
    CHECK_EQUAL(match, npos);

    match = (first < first).find();
    CHECK_EQUAL(match, npos);

    match = (first >= first).find();
    CHECK_EQUAL(match, 0);

    match = (first <= first).find();
    CHECK_EQUAL(match, 0);

    // Two different columns
    match = (first == second).find();
    CHECK_EQUAL(match, 3); // null == null

    match = (first > second).find();
    CHECK_EQUAL(match, 2); // Timestamp(333, 444) > Timestamp(111, 222)

    match = (first < second).find();
    CHECK_EQUAL(match, npos); // Note that (null < null) == false
}

TEST(Query_Timestamp_Null)
{
    // Test that querying for null on non-nullable column (with default value being non-null value) is
    // possible (i.e. does not throw or fail) and also gives no search matches.
    Table table;
    size_t match;

    table.add_column(type_Timestamp, "first", false);
    table.add_column(type_Timestamp, "second", true);
    table.add_empty_row();

    Columns<Timestamp> first = table.column<Timestamp>(0);
    Columns<Timestamp> second = table.column<Timestamp>(1);

    match = (first == Timestamp{}).find();
    CHECK_EQUAL(match, npos);

    match = (second == Timestamp{}).find();
    CHECK_EQUAL(match, 0);
}

// Ensure that coyping a Query copies a restricting TableView if the query owns the view.
TEST(Query_CopyRestrictingTableViewWhenOwned)
{
    Table table;

    {
        Query q1(table, std::unique_ptr<TableViewBase>(new TableView()));
        Query q2(q1);

        // Reset the source query, destroying the original TableView.
        q1 = {};

        // Operations on the copied query that touch the restricting view should not crash.
        CHECK_EQUAL(0, q2.count());
    }

    {
        Query q1(table, std::unique_ptr<TableViewBase>(new TableView()));
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

    size_t col_links = source->add_column_link(type_LinkList, "link", *target);
    size_t col_id = target->add_column(type_Int, "id");

    auto reset_table_contents = [&] {
        source->clear();
        target->clear();

        for (size_t i = 0; i < 15; ++i) {
            target->add_empty_row();
            target->set_int(col_id, i, i);
        }

        source->add_empty_row();
        LinkViewRef ll = source->get_linklist(col_links, 0);
        for (size_t i = 6; i < 15; ++i) {
            ll->add(i);
        }
    };

    // Restricting TableView. Query::sync_view_if_needed() syncs the TableView if needed.
    {
        reset_table_contents();
        TableView restricting_view = target->where().greater(col_id, 5).find_all();
        Query q = target->where(&restricting_view).less(col_id, 10);

        // Bring the view out of sync with the table.
        target->set_int(col_id, 7, -7);
        target->set_int(col_id, 8, -8);

        // Verify that the query uses the view as-is.
        CHECK_EQUAL(4, q.count());
        CHECK_EQUAL(false, restricting_view.is_in_sync());

        // And that syncing the query brings the view back into sync.
        auto version = q.sync_view_if_needed();
        CHECK_EQUAL(true, restricting_view.is_in_sync());
        CHECK_EQUAL(2, q.count());
        CHECK_EQUAL(version, target->get_version_counter());
    }

    // Restricting LinkView. Query::sync_view_if_needed() does nothing as LinkViews are always in sync.
    {
        reset_table_contents();
        LinkViewRef restricting_view = source->get_linklist(col_links, 0);
        Query q = target->where(restricting_view).less(col_id, 10);

        // Modify the underlying table to remove rows from the LinkView.
        target->move_last_over(7);
        target->move_last_over(8);

        // Verify that the view has remained in sync.
        CHECK_EQUAL(true, restricting_view->is_in_sync());
        CHECK_EQUAL(2, q.count());

        // And that syncing the query does nothing.
        auto version = q.sync_view_if_needed();
        CHECK_EQUAL(true, restricting_view->is_in_sync());
        CHECK_EQUAL(version, target->get_version_counter());
        CHECK_EQUAL(2, q.count());
    }

    // No restricting view. Query::sync_view_if_needed() does nothing.
    {
        reset_table_contents();
        Query q = target->where().greater(col_id, 5).less(col_id, 10);

        target->set_int(col_id, 7, -7);
        target->set_int(col_id, 8, -8);

        CHECK_EQUAL(2, q.count());

        auto version = q.sync_view_if_needed();
        CHECK_EQUAL(version, target->get_version_counter());
        CHECK_EQUAL(2, q.count());
    }

    // Query that is not associated with a Table. Query::sync_view_if_needed() does nothing.
    {
        reset_table_contents();
        Query q;

        auto version = q.sync_view_if_needed();
        CHECK_EQUAL(bool(version), false);
    }
}

// Ensure that two queries can be combined via Query::and_query, &&, and || even if one of them has no conditions.
TEST(Query_CombineWithEmptyQueryDoesntCrash)
{
    Table table;
    size_t col_id = table.add_column(type_Int, "id");
    table.add_empty_row(3);
    table.set_int(col_id, 0, 0);
    table.set_int(col_id, 1, 1);
    table.set_int(col_id, 2, 2);

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
    size_t col_id = table.add_column(type_Int, "id");
    table.add_empty_row(3);
    table.set_int(col_id, 0, 42);
    table.set_int(col_id, 1, 43);
    table.set_int(col_id, 2, 44);

    {
        // Create initial table view
        TableView results = table.where().equal(col_id, 44).find_all();
        CHECK_EQUAL(1, results.size());
        CHECK_EQUAL(44, results.get(0).get_int(col_id));

        // Create query based on restricting view
        Query q = Query(results.get_parent().where(&results));
        size_t table_index = q.find(0);
        CHECK_EQUAL(2, table_index);
    }
}

namespace {
struct QueryInitHelper;

// Test a whole bunch of various permutations of operations on every query node
// type. This is done in a somewhat ridiculous CPS style to ensure complete
// control over when the Query instances are copied.
struct PreRun {
    template <typename Next>
    auto operator()(Query& q, Next&& next)
    {
        q.count();
        return next(q);
    }
};
struct CopyQuery {
    template <typename Next>
    auto operator()(Query& q, Next&& next)
    {
        Query copy(q);
        return next(copy);
    }
};
struct AndQuery {
    template <typename Next>
    auto operator()(Query& q, Next&& next)
    {
        return next(q.get_table()->where().and_query(q));
    }
};
struct HandoverQuery {
    template <typename Next>
    auto operator()(Query& q, Next&& next)
    {
        auto main_table = next.state.table;

        // Hand over the query to the secondary SG and continue processing on that
        std::swap(next.state.sg, next.state.sg2);
        auto& group = next.state.sg->begin_read(next.state.sg2->get_version_of_current_transaction());
        auto copy =
            next.state.sg->import_from_handover(next.state.sg2->export_for_handover(q, ConstSourcePayload::Copy));
        next.state.table = const_cast<Table*>(group.get_table(0).get());

        auto ret = next(*copy);

        // Restore the old state
        next.state.sg->end_read();
        next.state.table = main_table;
        std::swap(next.state.sg, next.state.sg2);
        return ret;
    }
};
struct SelfHandoverQuery {
    template <typename Next>
    auto operator()(Query& q, Next&& next)
    {
        // Export the query and then re-import it to the same SG
        auto handover = next.state.sg->export_for_handover(q, ConstSourcePayload::Copy);
        return next(*next.state.sg->import_from_handover(std::move(handover)));
    }
};
struct InsertColumn {
    template <typename Next>
    auto operator()(Query& q, Next&& next)
    {
        LangBindHelper::advance_read(*next.state.sg);
        return next(q);
    }
};
struct GetCount {
    auto operator()(Query& q)
    {
        return q.count();
    }
};

template <typename Func, typename... Rest>
struct Compose {
    QueryInitHelper& state;
    auto operator()(Query& q)
    {
        return Func()(q, Compose<Rest...>{state});
    }
};

template <typename Func>
struct Compose<Func> {
    QueryInitHelper& state;
    auto operator()(Query& q)
    {
        return Func()(q);
    }
};

struct QueryInitHelper {
    test_util::unit_test::TestContext& test_context;
    SharedGroup* sg;
    SharedGroup* sg2;
    SharedGroup::VersionID initial_version, extra_col_version;
    Table* table;

    template <typename Func>
    REALM_NOINLINE void operator()(Func&& fn);

    template <typename Func, typename... Mutations>
    REALM_NOINLINE size_t run(Func& fn);
};

template <typename Func>
void QueryInitHelper::operator()(Func&& fn)
{
    // get baseline result with no copies
    size_t count = run(fn);
    CHECK_EQUAL(count, (run<Func, InsertColumn>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, InsertColumn>(fn)));

    // copy the query, then run
    CHECK_EQUAL(count, (run<Func, CopyQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, AndQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, HandoverQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, SelfHandoverQuery>(fn)));

    // run, copy the query, rerun
    CHECK_EQUAL(count, (run<Func, PreRun, CopyQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, AndQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, HandoverQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, SelfHandoverQuery>(fn)));

    // copy the query, insert column, then run
    CHECK_EQUAL(count, (run<Func, CopyQuery, InsertColumn>(fn)));
    CHECK_EQUAL(count, (run<Func, AndQuery, InsertColumn>(fn)));
    CHECK_EQUAL(count, (run<Func, HandoverQuery, InsertColumn>(fn)));

    // run, copy the query, insert column, rerun
    CHECK_EQUAL(count, (run<Func, PreRun, CopyQuery, InsertColumn>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, AndQuery, InsertColumn>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, HandoverQuery, InsertColumn>(fn)));

    // insert column, copy the query, then run
    CHECK_EQUAL(count, (run<Func, InsertColumn, CopyQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, InsertColumn, AndQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, InsertColumn, HandoverQuery>(fn)));

    // run, insert column, copy the query, rerun
    CHECK_EQUAL(count, (run<Func, PreRun, InsertColumn, CopyQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, InsertColumn, AndQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, InsertColumn, HandoverQuery>(fn)));
}

template <typename Func, typename... Mutations>
size_t QueryInitHelper::run(Func& fn)
{
    auto& group = sg->begin_read(initial_version);
    table = const_cast<Table*>(group.get_table(0).get());
    size_t count;
    Query query = table->where();
    fn(query, [&](auto&& q2) { count = Compose<Mutations..., GetCount>{*this}(q2); });
    sg->end_read();
    return count;
}
} // anonymous namespace

// Test that queries properly bind to their tables and columns by constructing
// a query, maybe copying it in one of several ways, inserting a column at the
// beginning of the table, and then rerunning the query
TEST(Query_TableInitialization)
{
    SHARED_GROUP_TEST_PATH(path);

    auto repl = make_in_realm_history(path);
    auto repl2 = make_in_realm_history(path);
    SharedGroup sg(*repl, SharedGroupOptions(SharedGroupOptions::Durability::MemOnly));
    SharedGroup sg2(*repl2, SharedGroupOptions(SharedGroupOptions::Durability::MemOnly));
    Group& g = const_cast<Group&>(sg.begin_read());
    LangBindHelper::promote_to_write(sg);

    SharedGroup::VersionID initial_version, extra_col_version;

    Table& table = *g.add_table("table");
    // The columns are ordered to avoid having types which are backed by the
    // same implementation column type next to each other so that being
    // off-by-one doesn't work by coincidence
    size_t col_int = table.add_column(type_Int, "int");
    size_t col_float = table.add_column(type_Float, "float");
    size_t col_bool = table.add_column(type_Bool, "bool");
    size_t col_link = table.add_column_link(type_Link, "link", table);
    size_t col_string_enum = table.add_column(type_String, "string enum");
    table.optimize();
    size_t col_double = table.add_column(type_Double, "double");
    size_t col_string = table.add_column(type_String, "string");
    size_t col_list = table.add_column_link(type_LinkList, "list", table);
    size_t col_binary = table.add_column(type_Binary, "binary");
    size_t col_timestamp = table.add_column(type_Timestamp, "timestamp");
    size_t col_string_indexed = table.add_column(type_String, "indexed string");

    size_t col_int_null = table.add_column(type_Int, "int", true);
    size_t col_float_null = table.add_column(type_Float, "float", true);
    size_t col_bool_null = table.add_column(type_Bool, "bool", true);
    size_t col_double_null = table.add_column(type_Double, "double", true);
    size_t col_string_null = table.add_column(type_String, "string", true);
    size_t col_binary_null = table.add_column(type_Binary, "binary", true);
    size_t col_timestamp_null = table.add_column(type_Timestamp, "timestamp", true);

    DescriptorRef subdesc;
    size_t col_table = table.add_column(type_Table, "table", &subdesc);
    subdesc->add_column(type_Int, "col");

    std::string str(5, 'z');
    table.add_empty_row(20);
    for (size_t i = 0; i < 10; ++i) {
        table.set_binary(col_binary, i, BinaryData(str), false);
        table.set_link(col_link, i, i);
        table.get_linklist(col_list, i)->add(i);
        table.get_subtable(col_table, i)->add_empty_row();
    }
    LangBindHelper::commit_and_continue_as_read(sg);

    // Save this version so we can go back to it before every test
    initial_version = sg.get_version_of_current_transaction();
    sg.pin_version();

    // Create a second version which has an extra column at the beginning
    // of the table, so that anything which relies on stable column numbers
    // will use the wrong column after advancing
    LangBindHelper::promote_to_write(sg);
    table.insert_column(0, type_Double, "new col");
    LangBindHelper::commit_and_continue_as_read(sg);
    sg.pin_version();
    extra_col_version = sg.get_version_of_current_transaction();
    sg.end_read();

    QueryInitHelper helper{test_context, &sg, &sg2, initial_version, extra_col_version, nullptr};

    // links_to
    helper([&](Query& q, auto&& test) { test(q.links_to(col_link, (*q.get_table())[0])); });
    helper([&](Query& q, auto&& test) { test(q.links_to(col_list, (*q.get_table())[0])); });
    helper([&](Query& q, auto&& test) { test(q.Not().links_to(col_link, (*q.get_table())[0])); });
    helper([&](Query& q, auto&& test) {
        test(q.links_to(col_link, (*q.get_table())[0]).Or().links_to(col_link, (*q.get_table())[1]));
    });

    // subtable
    helper([&](Query& q, auto&& test) { test(q.subtable(col_table).equal(0, 0).end_subtable()); });

    // compare to null
    helper([&](Query& q, auto&& test) { test(q.equal(col_int_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.equal(col_float_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.equal(col_bool_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.equal(col_double_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.equal(col_string_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.equal(col_binary_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.equal(col_timestamp_null, null{})); });

    helper([&](Query& q, auto&& test) { test(q.not_equal(col_int_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_float_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_bool_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_double_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_string_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_binary_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_timestamp_null, null{})); });

    // Conditions: int64_t
    helper([&](Query& q, auto&& test) { test(q.equal(col_int, int64_t{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_int, int64_t{})); });
    helper([&](Query& q, auto&& test) { test(q.greater(col_int, int64_t{})); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal(col_int, int64_t{})); });
    helper([&](Query& q, auto&& test) { test(q.less(col_int, int64_t{})); });
    helper([&](Query& q, auto&& test) { test(q.less_equal(col_int, int64_t{})); });
    helper([&](Query& q, auto&& test) { test(q.between(col_int, int64_t{}, {})); });

    // Conditions: int
    helper([&](Query& q, auto&& test) { test(q.equal(col_int, int{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_int, int{})); });
    helper([&](Query& q, auto&& test) { test(q.greater(col_int, int{})); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal(col_int, int{})); });
    helper([&](Query& q, auto&& test) { test(q.less(col_int, int{})); });
    helper([&](Query& q, auto&& test) { test(q.less_equal(col_int, int{})); });
    helper([&](Query& q, auto&& test) { test(q.between(col_int, int{}, {})); });

    // Conditions: 2 int columns
    helper([&](Query& q, auto&& test) { test(q.equal_int(col_int, col_int)); });
    helper([&](Query& q, auto&& test) { test(q.not_equal_int(col_int, col_int)); });
    helper([&](Query& q, auto&& test) { test(q.greater_int(col_int, col_int)); });
    helper([&](Query& q, auto&& test) { test(q.less_int(col_int, col_int)); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal_int(col_int, col_int)); });
    helper([&](Query& q, auto&& test) { test(q.less_equal_int(col_int, col_int)); });

    // Conditions: float
    helper([&](Query& q, auto&& test) { test(q.equal(col_float, float{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_float, float{})); });
    helper([&](Query& q, auto&& test) { test(q.greater(col_float, float{})); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal(col_float, float{})); });
    helper([&](Query& q, auto&& test) { test(q.less(col_float, float{})); });
    helper([&](Query& q, auto&& test) { test(q.less_equal(col_float, float{})); });
    helper([&](Query& q, auto&& test) { test(q.between(col_float, float{}, {})); });

    // Conditions: 2 float columns
    helper([&](Query& q, auto&& test) { test(q.equal_float(col_float, col_float)); });
    helper([&](Query& q, auto&& test) { test(q.not_equal_float(col_float, col_float)); });
    helper([&](Query& q, auto&& test) { test(q.greater_float(col_float, col_float)); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal_float(col_float, col_float)); });
    helper([&](Query& q, auto&& test) { test(q.less_float(col_float, col_float)); });
    helper([&](Query& q, auto&& test) { test(q.less_equal_float(col_float, col_float)); });

    // Conditions: double
    helper([&](Query& q, auto&& test) { test(q.equal(col_double, double{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_double, double{})); });
    helper([&](Query& q, auto&& test) { test(q.greater(col_double, double{})); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal(col_double, double{})); });
    helper([&](Query& q, auto&& test) { test(q.less(col_double, double{})); });
    helper([&](Query& q, auto&& test) { test(q.less_equal(col_double, double{})); });
    helper([&](Query& q, auto&& test) { test(q.between(col_double, double{}, {})); });

    // Conditions: 2 double columns
    helper([&](Query& q, auto&& test) { test(q.equal_double(col_double, col_double)); });
    helper([&](Query& q, auto&& test) { test(q.not_equal_double(col_double, col_double)); });
    helper([&](Query& q, auto&& test) { test(q.greater_double(col_double, col_double)); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal_double(col_double, col_double)); });
    helper([&](Query& q, auto&& test) { test(q.less_double(col_double, col_double)); });
    helper([&](Query& q, auto&& test) { test(q.less_equal_double(col_double, col_double)); });

    // Conditions: timestamp
    helper([&](Query& q, auto&& test) { test(q.equal(col_timestamp, Timestamp{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_timestamp, Timestamp{})); });
    helper([&](Query& q, auto&& test) { test(q.greater(col_timestamp, Timestamp{})); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal(col_timestamp, Timestamp{})); });
    helper([&](Query& q, auto&& test) { test(q.less_equal(col_timestamp, Timestamp{})); });
    helper([&](Query& q, auto&& test) { test(q.less(col_timestamp, Timestamp{})); });

    // Conditions: bool
    helper([&](Query& q, auto&& test) { test(q.equal(col_bool, bool{})); });

    // Conditions: strings
    helper([&](Query& q, auto&& test) { test(q.equal(col_string, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_string, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.begins_with(col_string, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.ends_with(col_string, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.contains(col_string, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.like(col_string, StringData{})); });

    helper([&](Query& q, auto&& test) { test(q.equal(col_string, StringData{}, false)); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_string, StringData{}, false)); });
    helper([&](Query& q, auto&& test) { test(q.begins_with(col_string, StringData{}, false)); });
    helper([&](Query& q, auto&& test) { test(q.ends_with(col_string, StringData{}, false)); });
    helper([&](Query& q, auto&& test) { test(q.contains(col_string, StringData{}, false)); });
    helper([&](Query& q, auto&& test) { test(q.like(col_string, StringData{}, false)); });

    helper([&](Query& q, auto&& test) { test(q.equal(col_string_enum, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_string_enum, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.begins_with(col_string_enum, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.ends_with(col_string_enum, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.contains(col_string_enum, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.like(col_string_enum, StringData{})); });

    helper([&](Query& q, auto&& test) { test(q.equal(col_string_indexed, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_string_indexed, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.begins_with(col_string_indexed, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.ends_with(col_string_indexed, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.contains(col_string_indexed, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.like(col_string_indexed, StringData{})); });

    // Conditions: binary data
    helper([&](Query& q, auto&& test) { test(q.equal(col_binary, BinaryData{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_binary, BinaryData{})); });
    helper([&](Query& q, auto&& test) { test(q.begins_with(col_binary, BinaryData{})); });
    helper([&](Query& q, auto&& test) { test(q.ends_with(col_binary, BinaryData{})); });
    helper([&](Query& q, auto&& test) { test(q.contains(col_binary, BinaryData{})); });

    enum class Mode { Direct, Link, LinkList };

    // note: using std::function<> rather than auto&& here for the sake of compilation speed
    auto test_query_expression = [&](std::function<Table&()> get_table, Mode mode) {
        auto test_operator = [&](auto&& op, auto&& column, auto&& v) {
            if (mode != Mode::LinkList)
                helper([&](Query&, auto&& test) { test(op(column(), column())); });
            helper([&](Query&, auto&& test) { test(op(column(), v)); });
        };
        auto test_numeric = [&](auto value, size_t col, size_t null_col) {
            using Type = decltype(value);
            auto get_column = [&] { return get_table().template column<Type>(col); };
            test_operator(std::equal_to<>(), get_column, value);
            test_operator(std::not_equal_to<>(), get_column, value);
            test_operator(std::greater<>(), get_column, value);
            test_operator(std::less<>(), get_column, value);
            test_operator(std::greater_equal<>(), get_column, value);
            test_operator(std::less_equal<>(), get_column, value);

            auto get_null_column = [&] { return get_table().template column<Type>(null_col); };
            test_operator(std::equal_to<>(), get_null_column, null{});
            test_operator(std::not_equal_to<>(), get_null_column, null{});
        };

        test_numeric(Int(), col_int, col_int_null);
        test_numeric(Float(), col_float, col_float_null);
        test_numeric(Bool(), col_bool, col_bool_null);
        test_numeric(Double(), col_double, col_double_null);
        test_numeric(Timestamp(), col_timestamp, col_timestamp_null);

        auto string_col = [&] { return get_table().template column<String>(col_string); };
        test_operator(std::equal_to<>(), string_col, StringData());
        test_operator(std::not_equal_to<>(), string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.begins_with(b); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.ends_with(b); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.contains(b); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.like(b); }, string_col, StringData());

        test_operator([](auto&& a, auto&& b) { return a.equal(b, false); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.not_equal(b, false); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.begins_with(b, false); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.ends_with(b, false); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.contains(b, false); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.like(b, false); }, string_col, StringData());

        auto null_string_col = [&] { return get_table().template column<String>(col_string_null); };
        test_operator(std::equal_to<>(), null_string_col, null());
        test_operator(std::not_equal_to<>(), null_string_col, null());

        auto binary_col = [&] { return get_table().template column<Binary>(col_binary); };
        helper([&](Query&, auto&& test) { test(binary_col() == BinaryData()); });
        helper([&](Query&, auto&& test) { test(binary_col() != BinaryData()); });
        helper([&](Query&, auto&& test) { test(binary_col().size() != 0); });

        auto link_col = [&] { return get_table().template column<Link>(col_link); };
        auto list_col = [&] { return get_table().template column<Link>(col_list); };

        if (mode == Mode::Direct) { // link equality over links isn't implemented
            helper([&](Query&, auto&& test) { test(link_col().is_null()); });
            helper([&](Query&, auto&& test) { test(link_col().is_not_null()); });
            helper([&](Query&, auto&& test) { test(link_col() == (*helper.table)[0]); });
            helper([&](Query&, auto&& test) { test(link_col() != (*helper.table)[0]); });

            helper([&](Query&, auto&& test) { test(list_col() == (*helper.table)[0]); });
            helper([&](Query&, auto&& test) { test(list_col() != (*helper.table)[0]); });
        }

        helper([&](Query&, auto&& test) { test(list_col().count() == 1); });
        helper([&](Query&, auto&& test) { test(list_col().size() == 1); });
        helper([&](Query&, auto&& test) { test(list_col().column<Int>(col_int).max() > 0); });
        helper([&](Query&, auto&& test) { test(list_col().column<Int>(col_int).min() > 0); });
        helper([&](Query&, auto&& test) { test(list_col().column<Int>(col_int).sum() > 0); });
        helper([&](Query&, auto&& test) { test(list_col().column<Int>(col_int).average() > 0); });

        auto list_table = [&] { return get_table().template column<SubTable>(col_table); };
        helper([&](Query&, auto&& test) { test(list_table().size() == 1); });
    };

    // Test all of the query expressions directly, over a link, over a backlink
    // over a linklist, and over two links
    test_query_expression([&]() -> Table& { return *helper.table; }, Mode::Direct);
    test_query_expression(
        [&]() -> Table& {
            helper.table->link(col_link);
            return *helper.table;
        },
        Mode::Link);
    test_query_expression(
        [&]() -> Table& {
            helper.table->backlink(*helper.table, col_link);
            return *helper.table;
        },
        Mode::LinkList);
    test_query_expression(
        [&]() -> Table& {
            helper.table->link(col_list);
            return *helper.table;
        },
        Mode::LinkList);
    test_query_expression(
        [&]() -> Table& {
            helper.table->link(col_link);
            helper.table->link(col_list);
            return *helper.table;
        },
        Mode::LinkList);

    helper([&](Query& q, auto&& test) {
        test(helper.table->column<LinkList>(col_list, q.equal_int(col_int, 0)).count() > 0);
    });
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
        Group group;

        TableRef contact = group.add_table("contact");
        TableRef contact_type = group.add_table("contact_type");

        contact_type->add_column(type_Int, "id");
        contact_type->add_column(type_String, "str");
        contact->add_column_link(type_LinkList, "link", *contact_type);

        contact_type.get()->add_empty_row(10);
        contact.get()->add_empty_row(10);

        Query q1 = (contact.get()->link(0).column<Int>(0) == 0);
        Query q2 = contact_type.get()->where().equal(0, 0);
        Query q3 = (contact_type.get()->column<Int>(0) + contact_type.get()->column<Int>(0) == 0);
        Query q4 = (contact_type.get()->column<Int>(0) == 0);
        Query q5 = (contact_type.get()->column<String>(1) == "hejsa");

        TableView tv = q1.find_all();
        TableView tv2 = q2.find_all();
        TableView tv3 = q3.find_all();
        TableView tv4 = q4.find_all();
        TableView tv5 = q5.find_all();

        contact.get()->insert_column(0, type_Float, "extra");
        contact_type.get()->insert_column(0, type_Float, "extra");

        for (size_t t = 0; t < REALM_MAX_BPNODE_SIZE + 1; t++) {
            contact.get()->add_empty_row();
            contact_type.get()->add_empty_row();
            //  contact_type.get()->set_string(1, t, "hejsa");

            LinkViewRef lv = contact.get()->get_linklist(1, contact.get()->size() - 1);
            lv->add(contact_type.get()->size() - 1);

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
    foo.add_column(type_Int, "a");
    foo.add_column(type_Int, "b");
    foo.add_empty_row(10);
    foo.set_int(0, 3, 123);
    foo.set_int(0, 4, 123);
    foo.set_int(0, 7, 123);
    foo.set_int(1, 2, 456);
    foo.set_int(1, 4, 456);

    auto q1 = foo.column<Int>(0) == 123;
    auto q2 = foo.column<Int>(1) == 456;
    auto q3 = q1 || q2;
    TableView tv1 = q1.find_all();
    TableView tv2 = q2.find_all();
    TableView tv3 = q3.find_all();
    CHECK_EQUAL(tv1.size(), 3);
    CHECK_EQUAL(tv2.size(), 2);
    CHECK_EQUAL(tv3.size(), 4);

    foo.remove_column(0);

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
    foo.add_column(type_Int, "a");
    foo.add_column(type_Int, "b");
    foo.add_column(type_Timestamp, "c");
    foo.add_column(type_Timestamp, "d");
    foo.add_column(type_String, "e");
    foo.add_column(type_Float, "f");
    foo.add_column(type_Binary, "g");
    foo.add_empty_row(5);
    foo.set_int(0, 0, 0);
    foo.set_int(0, 1, 1);
    foo.set_int(0, 2, 2);
    foo.set_int(0, 3, 3);
    foo.set_int(0, 4, 4);
    foo.set_int(1, 0, 0);
    foo.set_int(1, 1, 0);
    foo.set_int(1, 2, 3);
    foo.set_int(1, 3, 5);
    foo.set_int(1, 4, 3);
    foo.set_timestamp(2, 0, Timestamp(100, 100));
    foo.set_timestamp(3, 0, Timestamp(200, 100));
    foo.set_string(4, 0, StringData("Hello, world"));
    foo.set_float(5, 0, 3.141592f);
    foo.set_float(5, 1, 1.0f);
    foo.set_binary(6, 0, BinaryData("Binary", 6));

    // Expression
    auto q = foo.column<Int>(0) == foo.column<Int>(1) + 1;
    // TwoColumnsNode
    auto q1 = foo.column<Int>(0) == foo.column<Int>(1);
    TableView tv = q.find_all();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv1.size(), 1);

    foo.remove_column(0);
    size_t x = 0;
    CHECK_LOGIC_ERROR(x = q.count(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv1.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_EQUAL(x, 0);
    CHECK_EQUAL(tv.size(), 0);

    q = foo.column<Timestamp>(1) < foo.column<Timestamp>(2);
    // TimestampNode
    q1 = foo.column<Timestamp>(2) == Timestamp(200, 100);
    tv = q.find_all();
    tv1 = q1.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv1.size(), 1);
    foo.remove_column(2);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv1.sync_if_needed(), LogicError::column_does_not_exist);

    // StringNodeBase
    q = foo.column<String>(2) == StringData("Hello, world");
    q1 = !(foo.column<String>(2) == StringData("Hello, world"));
    tv = q.find_all();
    tv1 = q1.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv1.size(), 4);
    foo.remove_column(2);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv1.sync_if_needed(), LogicError::column_does_not_exist);

    // FloatDoubleNode
    q = foo.column<Float>(2) > 0.0f;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    foo.remove_column(2);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);

    // BinaryNode
    q = foo.column<Binary>(2) != BinaryData("Binary", 6);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 4);
    foo.remove_column(2);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
}

TEST(Query_ColumnDeletionLinks)
{
    Group g;
    TableRef foo = g.add_table("foo");
    TableRef bar = g.add_table("bar");
    TableRef foobar = g.add_table("foobar");

    foobar->add_column(type_Int, "int");

    bar->add_column(type_Int, "int");
    bar->add_column_link(type_Link, "link", *foobar);

    foo->add_column_link(type_Link, "link", *bar);
    DescriptorRef subdesc;
    foo->add_column(type_Table, "sub", &subdesc);
    subdesc->add_column(type_Int, "int");

    foobar->add_empty_row(5);
    bar->add_empty_row(5);
    foo->add_empty_row(10);
    for (size_t i = 0; i < 5; i++) {
        foobar->set_int(0, i, i);
        bar->set_int(0, i, i);
        bar->set_link(1, i, i);
        foo->set_link(0, i, i);
        auto sub = foo->get_subtable(1, 0);
        auto r = sub->add_empty_row();
        sub->set_int(0, r, i);
    }
    auto q = foo->link(0).link(1).column<Int>(0) == 2;
    auto q1 = foo->column<Link>(0).is_null();
    auto q2 = foo->column<Link>(0) == bar->get(2);
    auto q3 = foo->where().subtable(1).greater(0, 3).end_subtable();
    auto tv = q.find_all();
    auto cnt = q1.count();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(cnt, 5);
    cnt = q2.count();
    CHECK_EQUAL(cnt, 1);
    cnt = q3.count();
    CHECK_EQUAL(cnt, 1);
    // remove integer column, should not affect query
    bar->remove_column(0);
    tv.sync_if_needed();
    CHECK_EQUAL(tv.size(), 1);
    // remove link column, disaster
    bar->remove_column(0);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
    foo->remove_column(0);
    CHECK_LOGIC_ERROR(q1.count(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(q2.count(), LogicError::column_does_not_exist);
    // Remove subtable column
    foo->remove_column(0);
    CHECK_LOGIC_ERROR(q3.count(), LogicError::column_does_not_exist);
}

#endif // TEST_QUERY
