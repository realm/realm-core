#include "testsettings.hpp"
#ifdef TEST_QUERY

#include <cstdlib> // itoa()
#include <limits>
#include <vector>

#include <realm.hpp>
#include <realm/lang_bind_helper.hpp>
#include <realm/column.hpp>
#include <realm/query_engine.hpp>

#include "test.hpp"

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


namespace {

    REALM_TABLE_2(TwoIntTable,
        first, Int,
        second, Int)

        REALM_TABLE_1(SingleStringTable,
        first, String)

        REALM_TABLE_3(TripleTable,
        first, String,
        second, String,
        third, Int)

        REALM_TABLE_1(OneIntTable,
        first, Int)

        REALM_TABLE_2(TupleTableType,
        first, Int,
        second, String)

        REALM_TABLE_5(DateIntStringFloatDouble,
        first, Int,
        second, String,
        third, DateTime,
        fourth, Float,
        fifth, Double)

        REALM_TABLE_2(TupleTableTypeBin,
        first, Int,
        second, Binary)

        REALM_TABLE_2(BoolTupleTable,
        first, Int,
        second, Bool)

        REALM_TABLE_5(PeopleTable,
        name, String,
        age, Int,
        male, Bool,
        hired, DateTime,
        photo, Binary)

        REALM_TABLE_2(FloatTable,
        col_float, Float,
        col_double, Double)

        REALM_TABLE_3(FloatTable3,
        col_float, Float,
        col_double, Double,
        col_int, Int)

        REALM_TABLE_3(PHPMinimumCrash,
        firstname, String,
        lastname, String,
        salary, Int)

        REALM_TABLE_3(TableViewSum,
        col_float, Float,
        col_double, Double,
        col_int, Int)

        REALM_TABLE_5(GATable,
        user_id, String,
        country, String,
        build, String,
        event_1, Int,
        event_2, Int)

        REALM_TABLE_2(PeopleTable2,
        name, String,
        age, Int)

        REALM_TABLE_5(ThreeColTable,
        first, Int,
        second, Float,
        third, Double,
        fourth, Bool,
        fifth, String)

        REALM_TABLE_3(Books,
        title, String,
        author, String,
        pages, Int)

        REALM_TABLE_3(Types,
        ints, Int,
        strings, String,
        doubles, Double)

} // anonymous namespace



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

        size_t count = 0;
        size_t rows = random.draw_int_mod(5 * REALM_MAX_BPNODE_SIZE); // to cross some leaf boundaries

        for (size_t i = 0; i < rows; ++i) {
            table.add_empty_row();
            int64_t val = random.draw_int_mod(5);
            table.set_int(0, i, val);
            if (val == 2)
                count++;
        }

        size_t count2 = table.where().equal(0, 2).count();
        CHECK_EQUAL(count, count2);
    }

}


TEST(Query_NextGenSyntaxTypedString)
{
    Books books;

    books.add("Computer Architecture and Organization", "B. Govindarajalu", 752);
    books.add("Introduction to Quantum Mechanics", "David Griffiths", 480);
    books.add("Biophysics: Searching for Principles", "William Bialek", 640);

    // Typed table:
    Query q = books.column().pages >= 200 && books.column().author == "David Griffiths";
    size_t match = q.find();
    CHECK_EQUAL(1, match);
    // You don't need to create a query object first:
    match = (books.column().pages >= 200 && books.column().author == "David Griffiths").find();
    CHECK_EQUAL(1, match);

    // You can also create column objects and use them in expressions:
    Columns<Int> pages = books.column().pages;
    Columns<String> author = books.column().author;
    match = (pages >= 200 && author == "David Griffiths").find();
    CHECK_EQUAL(1, match);
}

TEST(Query_NextGenSyntax)
{
    volatile size_t match;

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

    // Setup typed table, same contents as untyped
    ThreeColTable typed;
    typed.add(20, 19.9f, 3.0, true, "hello");
    typed.add(20, 20.1f, 4.0, false, "world");


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
    match = (typed.column().second + 100 > 120 && typed.column().first > 2).find();
    CHECK_EQUAL(match, 1);

    // internal negation (rewrite of test above):
    match = (!(!(typed.column().second + 100 > 120) || !(typed.column().first > 2))).find();
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
    Query q1 = typed.column().second + typed.column().first > 40;
    match = q1.find();
    CHECK_EQUAL(match, 1);


    match = (typed.column().first + typed.column().second > 40).find();
    CHECK_EQUAL(match, 1);


    Query tq1 = typed.column().first + typed.column().second >= typed.column().first + typed.column().second;
    match = tq1.find();
    CHECK_EQUAL(match, 0);


    // Typed, column objects
    Columns<int64_t> t0 = typed.column().first;
    Columns<float> t1 = typed.column().second;

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

    match = (untyped.column<int64_t>(0) + untyped.column<float>(1) >= untyped.column<int64_t>(0) + untyped.column<float>(1)).find();
    CHECK_EQUAL(match, 0);

    // Untyped, column objects
    Columns<int64_t> u0 = untyped.column<int64_t>(0);
    Columns<float> u1 = untyped.column<float>(1);

    match = (u0 + u1 > 40).find();
    CHECK_EQUAL(match, 1);

    // No longer supported
    /*
    // Flexible language binding style
    Subexpr* first = new Columns<int64_t>(0);
    Subexpr* second = new Columns<float>(1);
    Subexpr* third = new Columns<double>(2);
    Subexpr* constant = new Value<int64_t>(40);
    Subexpr* plus = new Operator<Plus<float>>(*first, *second);
    Expression *e = new Compare<Greater, float>(*plus, *constant);


    // Bind table and do search
    match = untyped.where().expression(e).find();
    CHECK_EQUAL(match, 1);

    Query q9 = untyped.where().expression(e);
    match = q9.find();
    CHECK_EQUAL(match, 1);


    Subexpr* first2 = new Columns<int64_t>(0);
    Subexpr* second2 = new Columns<float>(1);
    Subexpr* third2 = new Columns<double>(2);
    Subexpr* constant2 = new Value<int64_t>(40);
    Subexpr* plus2 = new Operator<Plus<float>>(*first, *second);
    Expression *e2 = new Compare<Greater, float>(*plus, *constant);

    match = untyped.where().expression(e).expression(e2).find();
    CHECK_EQUAL(match, 1);

    Query q10 = untyped.where().and_query(q9).expression(e2);
    match = q10.find();
    CHECK_EQUAL(match, 1);


    Query tq3 = tq1;
    match = tq3.find();
    CHECK_EQUAL(match, 0);

    delete e;
    delete plus;
    delete constant;
    delete third;
    delete second;
    delete first;


    delete e2;
    delete plus2;
    delete constant2;
    delete third2;
    delete second2;
    delete first2;
    */

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
}


TEST(Query_NextGenSyntaxMonkey0)
{
    // Intended to test eval() for columns in query_expression.hpp which fetch 8 values at a time. This test varies
    // table size to test out-of-bounds bugs.

    Random random(random_int<unsigned long>()); // Seed from slow global generator
    for (int iter = 1; iter < 10 + TEST_DURATION * 1000; iter++)
    {
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
            if (r >= start && tvpos < limit && table.get_int(0, r) > table.get_float(1, r) && table.get_string(2, r) == "a") {
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv.size());

    }

}

TEST(Query_NextGenSyntaxMonkey)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    for (int iter = 1; iter < 10 * (TEST_DURATION * TEST_DURATION * TEST_DURATION + 1); iter++) {
        // Set 'rows' to at least '* 20' else some tests will give 0 matches and bad coverage
        const size_t rows =
            1 + random.draw_int_mod<size_t>(REALM_MAX_BPNODE_SIZE * 20 *
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
        realm::Query q2_1 = (table.column<int64_t>(0) == 0 || table.column<int64_t>(0) == 1) && table.column<int64_t>(1) == 1;
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
        realm::Query q2_2 = table.column<int64_t>(0) == 0 || (table.column<int64_t>(0) == 1 && table.column<int64_t>(1) == 1);
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
        realm::Query q4_8 = table.column<int64_t>(1) == 0 && (table.column<int64_t>(0) == 0 || table.column<int64_t>(0) == 2);
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
        realm::Query q3_7 = (table.column<int64_t>(0) == 0 || table.column<int64_t>(0) == 2) && (table.column<int64_t>(0) == 1 || table.column<int64_t>(1) == 1);
        realm::TableView tv_7 = q3_7.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if ((table.get_int(0, r) == 0 || table.get_int(0, r) == 2) && (table.get_int(0, r) == 1 || table.get_int(1, r) == 1)) {
                CHECK_EQUAL(r, tv_7.get_source_ndx(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv_7.size());


        // (first == 0 || first == 2) || (first == 1 || second == 1)
        realm::Query q4_7 = (table.column<int64_t>(0) == 0 || table.column<int64_t>(0) == 2) || (table.column<int64_t>(0) == 1 || table.column<int64_t>(1) == 1);
        realm::TableView tv_10 = q4_7.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if ((table.get_int(0, r) == 0 || table.get_int(0, r) == 2) || (table.get_int(0, r) == 1 || table.get_int(1, r) == 1)) {
                CHECK_EQUAL(r, tv_10.get_source_ndx(tvpos));
                tvpos++;
            }
        }
        CHECK_EQUAL(tvpos, tv_10.size());


        TableView tv;

        // first == 0 || first == 2 || first == 1 || second == 1
        realm::Query q20 = table.column<int64_t>(0) == 0 || table.column<int64_t>(0) == 2 || table.column<int64_t>(0) == 1 || table.column<int64_t>(1) == 1;
        tv = q20.find_all();
        tvpos = 0;
        for (size_t r = 0; r < rows; r++) {
            if (table.get_int(0, r) == 0 || table.get_int(0, r) == 2 || table.get_int(0, r) == 1 || table.get_int(1, r) == 1) {
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

        // first * 2 > second / 2 + third + 1 + third - third + third - third + third - third + third - third + third - third
        realm::Query q22 = table.column<int64_t>(0) * 2 > table.column<int64_t>(1) / 2 + table.column<int64_t>(2) + 1 + table.column<int64_t>(2) - table.column<int64_t>(2) + table.column<int64_t>(2) - table.column<int64_t>(2) + table.column<int64_t>(2) - table.column<int64_t>(2) + table.column<int64_t>(2) - table.column<int64_t>(2) + table.column<int64_t>(2) - table.column<int64_t>(2);
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

    sum = q.sum_int(0, NULL, 0, -1, 1);
    CHECK_EQUAL(10000, sum);

    sum = q.sum_int(0, NULL, 0, -1, 2);
    CHECK_EQUAL(40000, sum);

    sum = q.sum_int(0, NULL, 0, -1, 3);
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
    CHECK_EQUAL(1, q0d.count());  // FAILS

    realm::Query q0e = table.where().Not().Not().Not().equal(0, 10);
    CHECK_EQUAL(2, q0e.count());  // FAILS

    // just checking the above
    realm::Query q0f = table.where().Not().not_equal(0, 10);
    CHECK_EQUAL(1, q0f.count());

    realm::Query q0g = table.where().Not().Not().not_equal(0, 10);
    CHECK_EQUAL(2, q0g.count());   // FAILS

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
    CHECK_EQUAL(2, q3.count());  // FAILS

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
            if ((table.get_int(0, r) == 0 || table.get_int(0, r) == 2) || (table.get_int(0, r) == 1 && table.get_int(1, r) == 1)) {
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
            if ((table.get_int(0, r) == 0 || table.get_int(0, r) == 2) && (table.get_int(0, r) == 1 || table.get_int(1, r) == 1)) {
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
    PeopleTable2 table;

    table.add("Mary", 14);
    table.add("Joe", 17);
    table.add("Alice", 42);
    table.add("Jack", 22);
    table.add("Bob", 50);
    table.add("Frank", 12);

    // Select rows where age < 18
    PeopleTable2::Query query = table.where().age.less(18);

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
    table.set_float(1, 0, 3.0);
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

}

TEST(Query_LimitUntyped2)
{
    Table table;
    table.add_column(type_Int, "first1");
    table.add_column(type_Float, "second1");
    table.add_column(type_Double, "second1");

    table.add_empty_row(3);
    table.set_int(0, 0, 10000);
    table.set_int(0, 1, 30000);
    table.set_int(0, 2, 40000);

    table.set_float(1, 0, 10000.);
    table.set_float(1, 1, 30000.);
    table.set_float(1, 2, 40000.);

    table.set_double(2, 0, 10000.);
    table.set_double(2, 1, 30000.);
    table.set_double(2, 2, 40000.);


    Query q = table.where();
    int64_t sum;
    float sumf;
    double sumd;

    // sum, limited by 'limit'
    sum = q.sum_int(0, NULL, 0, -1, 1);
    CHECK_EQUAL(10000, sum);
    sum = q.sum_int(0, NULL, 0, -1, 2);
    CHECK_EQUAL(40000, sum);
    sum = q.sum_int(0, NULL, 0, -1);
    CHECK_EQUAL(80000, sum);

    sumd = q.sum_float(1, NULL, 0, -1, 1);
    CHECK_EQUAL(10000., sumd);
    sumd = q.sum_float(1, NULL, 0, -1, 2);
    CHECK_EQUAL(40000., sumd);
    sumd = q.sum_float(1, NULL, 0, -1);
    CHECK_EQUAL(80000., sumd);

    sumd = q.sum_double(2, NULL, 0, -1, 1);
    CHECK_EQUAL(10000., sumd);
    sumd = q.sum_double(2, NULL, 0, -1, 2);
    CHECK_EQUAL(40000., sumd);
    sumd = q.sum_double(2, NULL, 0, -1);
    CHECK_EQUAL(80000., sumd);

    // sum, limited by 'end', but still having 'limit' specified
    sum = q.sum_int(0, NULL, 0, 1, 3);
    CHECK_EQUAL(10000, sum);
    sum = q.sum_int(0, NULL, 0, 2, 3);
    CHECK_EQUAL(40000, sum);

    sumd = q.sum_float(1, NULL, 0, 1, 3);
    CHECK_EQUAL(10000., sumd);
    sumd = q.sum_float(1, NULL, 0, 2, 3);
    CHECK_EQUAL(40000., sumd);

    sumd = q.sum_double(2, NULL, 0, 1, 3);
    CHECK_EQUAL(10000., sumd);
    sumd = q.sum_double(2, NULL, 0, 2, 3);
    CHECK_EQUAL(40000., sumd);

    size_t ndx = not_found;

    // max, limited by 'limit'
    sum = q.maximum_int(0, NULL, 0, -1, 1);
    CHECK_EQUAL(10000, sum);
    q.maximum_int(0, NULL, 0, -1, 1, &ndx);
    CHECK_EQUAL(0, ndx);

    sum = q.maximum_int(0, NULL, 0, -1, 2);
    CHECK_EQUAL(30000, sum);
    q.maximum_int(0, NULL, 0, -1, 2, &ndx);
    CHECK_EQUAL(1, ndx);

    sum = q.maximum_int(0, NULL, 0, -1);
    CHECK_EQUAL(40000, sum);
    q.maximum_int(0, NULL, 0, -1, -1, &ndx);
    CHECK_EQUAL(2, ndx);

    sumf = q.maximum_float(1, NULL, 0, -1, 1);
    CHECK_EQUAL(10000., sumf);
    q.maximum_float(1, NULL, 0, -1, 1, &ndx);
    CHECK_EQUAL(0, ndx);

    sumf = q.maximum_float(1, NULL, 0, -1, 2);
    CHECK_EQUAL(30000., sumf);
    q.maximum_float(1, NULL, 0, -1, 2, &ndx);
    CHECK_EQUAL(1, ndx);

    sumf = q.maximum_float(1, NULL, 0, -1);
    CHECK_EQUAL(40000., sumf);
    q.maximum_float(1, NULL, 0, -1, -1, &ndx);
    CHECK_EQUAL(2, ndx);

    sumd = q.maximum_double(2, NULL, 0, -1, 1);
    CHECK_EQUAL(10000., sumd);
    q.maximum_double(2, NULL, 0, -1, 1, &ndx);
    CHECK_EQUAL(0, ndx);

    sumd = q.maximum_double(2, NULL, 0, -1, 2);
    CHECK_EQUAL(30000., sumd);
    q.maximum_double(2, NULL, 0, -1, 2, &ndx);
    CHECK_EQUAL(1, ndx);

    sumd = q.maximum_double(2, NULL, 0, -1);
    CHECK_EQUAL(40000., sumd);
    q.maximum_double(2, NULL, 0, -1, -1, &ndx);
    CHECK_EQUAL(2, ndx);

    // max, limited by 'end', but still having 'limit' specified
    sum = q.maximum_int(0, NULL, 0, 1, 3);
    CHECK_EQUAL(10000, sum);
    q.maximum_int(0, NULL, 0, 1, 3, &ndx);
    CHECK_EQUAL(0, ndx);

    sum = q.maximum_int(0, NULL, 0, 2, 3);
    CHECK_EQUAL(30000, sum);
    q.maximum_int(0, NULL, 0, 2, 3, &ndx);
    CHECK_EQUAL(1, ndx);

    sumf = q.maximum_float(1, NULL, 0, 1, 3);
    CHECK_EQUAL(10000., sumf);
    q.maximum_float(1, NULL, 0, 1, 3, &ndx);
    CHECK_EQUAL(0, ndx);

    sumf = q.maximum_float(1, NULL, 0, 2, 3);
    CHECK_EQUAL(30000., sumf);
    q.maximum_float(1, NULL, 0, 2, 3, &ndx);
    CHECK_EQUAL(1, ndx);

    sumd = q.maximum_double(2, NULL, 0, 1, 3);
    CHECK_EQUAL(10000., sumd);
    q.maximum_double(2, NULL, 0, 1, 3, &ndx);
    CHECK_EQUAL(0, ndx);

    sumd = q.maximum_double(2, NULL, 0, 2, 3);
    CHECK_EQUAL(30000., sumd);
    q.maximum_double(2, NULL, 0, 2, 3, &ndx);
    CHECK_EQUAL(1, ndx);


    // avg
    sumd = q.average_int(0, NULL, 0, -1, 1);
    CHECK_EQUAL(10000, sumd);
    sumd = q.average_int(0, NULL, 0, -1, 2);
    CHECK_EQUAL((10000 + 30000) / 2, sumd);

    sumd = q.average_float(1, NULL, 0, -1, 1);
    CHECK_EQUAL(10000., sumd);
    sumd = q.average_float(1, NULL, 0, -1, 2);
    CHECK_EQUAL((10000. + 30000.) / 2., sumd);


    // avg, limited by 'end', but still having 'limit' specified
    sumd = q.average_int(0, NULL, 0, 1, 3);
    CHECK_EQUAL(10000, sumd);
    sumd = q.average_int(0, NULL, 0, 2, 3);
    CHECK_EQUAL((10000 + 30000) / 2, sumd);

    sumd = q.average_float(1, NULL, 0, 1, 3);
    CHECK_EQUAL(10000., sumd);
    sumd = q.average_float(1, NULL, 0, 2, 3);
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
            table->insert_string(0, i, dst);
            table->insert_done();
        }

        table->add_search_index(0);
        TableView v = table->where().equal(0, StringData("8")).find_all();
        CHECK_EQUAL(eights, v.size());

        v = table->where().equal(0, StringData("10")).find_all();

        v = table->where().equal(0, StringData("8")).find_all();
        CHECK_EQUAL(eights, v.size());
    }
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
    table.set_float(2, 0, 5);
    table.set_float(3, 0, 10);
    table.set_double(4, 0, 5);
    table.set_double(5, 0, 10);

    // row 1
    table.add_empty_row();
    table.set_int(0, 1, 10);
    table.set_int(1, 1, 5);
    table.set_float(2, 1, 10);
    table.set_float(3, 1, 5);
    table.set_double(4, 1, 10);
    table.set_double(5, 1, 5);

    // row 2
    table.add_empty_row();
    table.set_int(0, 2, -10);
    table.set_int(1, 2, -5);
    table.set_float(2, 2, -10);
    table.set_float(3, 2, -5);
    table.set_double(4, 2, -10);
    table.set_double(5, 2, -5);


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
    table.add_column(type_DateTime, "second1");
    table.add_column(type_DateTime, "second2");
    table.add_column(type_String, "third1");
    table.add_column(type_String, "third2");

    table.add_empty_row();
    table.set_bool(0, 0, false);
    table.set_bool(1, 0, true);
    table.set_datetime(2, 0, DateTime(0));
    table.set_datetime(3, 0, DateTime(1));
    table.set_string(4, 0, StringData("a"));
    table.set_string(5, 0, StringData("b"));

    table.add_empty_row();
    table.set_bool(0, 1, true);
    table.set_bool(1, 1, true);
    table.set_datetime(2, 1, DateTime(1));
    table.set_datetime(3, 1, DateTime(1));
    table.set_string(4, 1, StringData("b"));
    table.set_string(5, 1, StringData("b"));

    table.add_empty_row();
    table.set_bool(0, 2, false);
    table.set_bool(1, 2, true);
    table.set_datetime(2, 2, DateTime(0));
    table.set_datetime(3, 2, DateTime(1));
    table.set_string(4, 2, StringData("a"));
    table.set_string(5, 2, StringData("b"));

    Query q1 = table.column<Bool>(0) == table.column<Bool>(1);
    Query q2 = table.column<DateTime>(2) == table.column<DateTime>(3);
    Query q3 = table.column<String>(4) == table.column<String>(5);

    CHECK_EQUAL(1, q1.find());
    CHECK_EQUAL(1, q2.find());
    CHECK_EQUAL(1, q3.find());
    CHECK_EQUAL(1, q1.count());
    CHECK_EQUAL(1, q2.count());
    CHECK_EQUAL(1, q3.count());

    Query q4 = table.column<Bool>(0) != table.column<Bool>(1);
    Query q5 = table.column<DateTime>(2) != table.column<DateTime>(3);
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
    table.add_column(type_DateTime, "second1");

    for (int i = 1; i < 10; i++) {
        table.add_empty_row();
        table.set_datetime(0, i - 1, DateTime(i * 1000));
    }

    Query q = table.where().equal_datetime(0, DateTime(5000));
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

        TripleTable tt;
        TripleTable::View v;
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

            tt[row].first = first;
            tt[row].second = second;
            tt[row].third = third;

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

            if ((row >= start && row < end && limit > res7) && (first != "longlonglonglonglonglonglong A" && second == "A" && third == 1))
                res7++;

            if ((row >= start && row < end && limit > res8) && (first != "longlonglonglonglonglonglong A" && second == "A" && third == 2))
                res8++;
        }

        for (size_t t = 0; t < 4; t++) {

            if (t == 1)
                tt.optimize();
            else if (t == 2)
                tt.column().first.add_search_index();
            else if (t == 3)
                tt.column().second.add_search_index();



            v = tt.where().first.equal("A").second.equal("A").third.equal(1).find_all(start, end, limit);
            CHECK_EQUAL(res1, v.size());

            v = tt.where().second.equal("A").first.equal("A").third.equal(1).find_all(start, end, limit);
            CHECK_EQUAL(res1, v.size());

            v = tt.where().third.equal(1).second.equal("A").first.equal("A").find_all(start, end, limit);
            CHECK_EQUAL(res1, v.size());

            v = tt.where().group().first.equal("A").Or().second.equal("A").end_group().third.equal(1).find_all(start, end, limit);
            CHECK_EQUAL(res2, v.size());

            v = tt.where().first.equal("A").group().second.equal("A").Or().third.equal(1).end_group().find_all(start, end, limit);
            CHECK_EQUAL(res3, v.size());

            TripleTable::Query q = tt.where().group().first.equal("A").Or().third.equal(1).end_group().second.equal("A");
            v = q.find_all(start, end, limit);
            CHECK_EQUAL(res4, v.size());

            v = tt.where().group().first.equal("A").Or().third.equal(1).end_group().second.equal("A").find_all(start, end, limit);
            CHECK_EQUAL(res4, v.size());

            v = tt.where().first.equal("A").Or().second.equal("A").Or().third.equal(1).find_all(start, end, limit);
            CHECK_EQUAL(res5, v.size());

            v = tt.where().first.not_equal("A").second.equal("A").third.equal(1).find_all(start, end, limit);
            CHECK_EQUAL(res6, v.size());

            v = tt.where().first.not_equal("longlonglonglonglonglonglong A").second.equal("A").third.equal(1).find_all(start, end, limit);
            CHECK_EQUAL(res7, v.size());

            v = tt.where().first.not_equal("longlonglonglonglonglonglong A").second.equal("A").third.equal(2).find_all(start, end, limit);
            CHECK_EQUAL(res8, v.size());
        }
    }
}


TEST(Query_OnTableView_where)
{
    Random random;

    for (int iter = 0; iter < 100 * (1 + TEST_DURATION * TEST_DURATION * TEST_DURATION * TEST_DURATION * TEST_DURATION); iter++) {
        random.seed(164);
        OneIntTable oti;
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

            oti.add(v);
        }

        OneIntTable::View v = oti.where().first.not_equal(0).find_all(lbound, ubound, limit);
        size_t cnt2 = oti.where(&v).first.equal(1).count();

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
        TupleTableType ttt;

        std::vector<size_t> vec;
        size_t row = 0;

        size_t n = 0;
#ifdef REALM_DEBUG
        for (int i = 0; i < 4; i++) {
#else
        for (int i = 0; i < 20; i++) {
#endif
            // 1/500 match probability because we want possibility for a 1000 sized leaf to contain 0 matches (important
            // edge case)
            int f1 = random.draw_int_mod(REALM_MAX_BPNODE_SIZE) / 2 + 1;
            int f2 = random.draw_int_mod(REALM_MAX_BPNODE_SIZE) / 2 + 1;
            bool longstrings = random.chance(1, 5);

            // 2200 entries with that probability to fill out two concecutive 1000 sized leaves with above probability,
            // plus a remainder (edge case)
            for (int j = 0; j < REALM_MAX_BPNODE_SIZE * 2 + REALM_MAX_BPNODE_SIZE / 5; j++) {
                if (random.chance(1, f1)) {
                    if (random.chance(1, f2)) {
                        ttt.add(0, longstrings ? "AAAAAAAAAAAAAAAAAAAAAAAA" : "AA");
                        if (!longstrings) {
                            n++;
                            vec.push_back(row);
                        }
                    }
                    else {
                        ttt.add(0, "BB");
                    }
                }
                else {
                    if (random.chance(1, f2)) {
                        ttt.add(1, "AA");
                    }
                    else {
                        ttt.add(1, "BB");
                    }
                }
                ++row;
            }
        }

        TupleTableType::View v;

        // Both linear scans
        v = ttt.where().second.equal("AA").first.equal(0).find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_source_ndx(t));
        v.clear();
        vec.clear();

        v = ttt.where().first.equal(0).second.equal("AA").find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_source_ndx(t));
        v.clear();
        vec.clear();

        ttt.optimize();

        // Linear scan over enum, plus linear integer column scan
        v = ttt.where().second.equal("AA").first.equal(0).find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_source_ndx(t));
        v.clear();
        vec.clear();

        v = ttt.where().first.equal(0).second.equal("AA").find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_source_ndx(t));
        v.clear();
        vec.clear();

        ttt.column().second.add_search_index();

        // Index lookup, plus linear integer column scan
        v = ttt.where().second.equal("AA").first.equal(0).find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_source_ndx(t));
        v.clear();
        vec.clear();

        v = ttt.where().first.equal(0).second.equal("AA").find_all();
        CHECK_EQUAL(vec.size(), v.size());
        for (size_t t = 0; t < vec.size(); t++)
            CHECK_EQUAL(vec[t], v.get_source_ndx(t));
        v.clear();
        vec.clear();
    }
}


TEST(Query_StrIndex2)
{
    TupleTableType ttt;

    int64_t s;

    for (int i = 0; i < 100; ++i) {
        ttt.add(1, "AA");
    }
    ttt.add(1, "BB");
    ttt.column().second.add_search_index();

    s = ttt.where().second.equal("AA").count();
    CHECK_EQUAL(100, s);

    s = ttt.where().second.equal("BB").count();
    CHECK_EQUAL(1, s);

    s = ttt.where().second.equal("CC").count();
    CHECK_EQUAL(0, s);
}


TEST(Query_StrEnum)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    TupleTableType ttt;

    int aa;
    int64_t s;

    for (int i = 0; i < 100; ++i) {
        ttt.clear();
        aa = 0;
        for (size_t t = 0; t < REALM_MAX_BPNODE_SIZE * 2; ++t) {
            if (random.chance(1, 3)) {
                ttt.add(1, "AA");
                ++aa;
            }
            else {
                ttt.add(1, "BB");
            }
        }
        ttt.optimize();
        s = ttt.where().second.equal("AA").count();
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
        TupleTableType ttt;
        aa = 0;
        for (size_t t = 0; t < iterb; t++) {
            if (random.chance(1, 3)) {
                ttt.add(1, "AA");
                aa++;
            }
            else {
                ttt.add(1, "BB");
            }
        }

        s = ttt.where().second.equal("AA").count();
        CHECK_EQUAL(aa, s);

        ttt.optimize();
        s = ttt.where().second.equal("AA").count();
        CHECK_EQUAL(aa, s);

        ttt.column().second.add_search_index();
        s = ttt.where().second.equal("AA").count();
        CHECK_EQUAL(aa, s);
    }

}


TEST(Query_GameAnalytics)
{
    GROUP_TEST_PATH(path);
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    {
        Group g;
        GATable::Ref t = g.add_table<GATable>("firstevents");

        for (size_t i = 0; i < 100; ++i) {
            int64_t r1 = random.draw_int_mod(100);
            int64_t r2 = random.draw_int_mod(100);

            t->add("10", "US", "1.0", r1, r2);
        }
        t->optimize();
        g.write(path);
    }

    Group g(path);
    GATable::Ref t = g.get_table<GATable>("firstevents");

    GATable::Query q = t->where().country.equal("US");

    size_t c1 = 0;
    for (size_t i = 0; i < 100; ++i)
        c1 += t->column().country.count("US");

    size_t c2 = 0;
    for (size_t i = 0; i < 100; ++i)
        c2 += q.count();

    CHECK_EQUAL(c1, t->size() * 100);
    CHECK_EQUAL(c1, c2);
}


TEST(Query_Float3)
{
    FloatTable3 t;

    t.add(float(1.1), double(2.1), 1);
    t.add(float(1.2), double(2.2), 2);
    t.add(float(1.3), double(2.3), 3);
    t.add(float(1.4), double(2.4), 4); // match
    t.add(float(1.5), double(2.5), 5); // match
    t.add(float(1.6), double(2.6), 6); // match
    t.add(float(1.7), double(2.7), 7);
    t.add(float(1.8), double(2.8), 8);
    t.add(float(1.9), double(2.9), 9);

    FloatTable3::Query q1 = t.where().col_float.greater(1.35f).col_double.less(2.65);
    int64_t a1 = q1.col_int.sum();
    CHECK_EQUAL(15, a1);

    FloatTable3::Query q2 = t.where().col_double.less(2.65).col_float.greater(1.35f);
    int64_t a2 = q2.col_int.sum();
    CHECK_EQUAL(15, a2);

    FloatTable3::Query q3 = t.where().col_double.less(2.65).col_float.greater(1.35f);
    double a3 = q3.col_float.sum();
    double sum3 = double(1.4f) + double(1.5f) + double(1.6f);
    CHECK_EQUAL(sum3, a3);

    FloatTable3::Query q4 = t.where().col_float.greater(1.35f).col_double.less(2.65);
    double a4 = q4.col_float.sum();
    CHECK_EQUAL(sum3, a4);

    FloatTable3::Query q5 = t.where().col_int.greater_equal(4).col_double.less(2.65);
    double a5 = q5.col_float.sum();
    CHECK_EQUAL(sum3, a5);

    FloatTable3::Query q6 = t.where().col_double.less(2.65).col_int.greater_equal(4);
    double a6 = q6.col_float.sum();
    CHECK_EQUAL(sum3, a6);

    FloatTable3::Query q7 = t.where().col_int.greater(3).col_int.less(7);
    int64_t a7 = q7.col_int.sum();
    CHECK_EQUAL(15, a7);
    FloatTable3::Query q8 = t.where().col_int.greater(3).col_int.less(7);
    int64_t a8 = q8.col_int.sum();
    CHECK_EQUAL(15, a8);
}

TEST(Query_Float3_where)
{
    // Sum on query on tableview
    FloatTable3 t;

    t.add(float(1.1), double(2.1), 1);
    t.add(float(1.2), double(2.2), 2);
    t.add(float(1.3), double(2.3), 3);
    t.add(float(1.4), double(2.4), 4); // match
    t.add(float(1.5), double(2.5), 5); // match
    t.add(float(1.6), double(2.6), 6); // match
    t.add(float(1.7), double(2.7), 7);
    t.add(float(1.8), double(2.8), 8);
    t.add(float(1.9), double(2.9), 9);

    FloatTable3::View v = t.where().find_all();

    FloatTable3::Query q1 = t.where(&v).col_float.greater(1.35f).col_double.less(2.65);
    int64_t a1 = q1.col_int.sum();
    CHECK_EQUAL(15, a1);

    FloatTable3::Query q2 = t.where(&v).col_double.less(2.65).col_float.greater(1.35f);
    int64_t a2 = q2.col_int.sum();
    CHECK_EQUAL(15, a2);

    FloatTable3::Query q3 = t.where(&v).col_double.less(2.65).col_float.greater(1.35f);
    double a3 = q3.col_float.sum();
    double sum3 = double(1.4f) + double(1.5f) + double(1.6f);
    CHECK_EQUAL(sum3, a3);

    FloatTable3::Query q4 = t.where(&v).col_float.greater(1.35f).col_double.less(2.65);
    double a4 = q4.col_float.sum();
    CHECK_EQUAL(sum3, a4);

    FloatTable3::Query q5 = t.where(&v).col_int.greater_equal(4).col_double.less(2.65);
    double a5 = q5.col_float.sum();
    CHECK_EQUAL(sum3, a5);

    FloatTable3::Query q6 = t.where(&v).col_double.less(2.65).col_int.greater_equal(4);
    double a6 = q6.col_float.sum();
    CHECK_EQUAL(sum3, a6);

    FloatTable3::Query q7 = t.where(&v).col_int.greater(3).col_int.less(7);
    int64_t a7 = q7.col_int.sum();
    CHECK_EQUAL(15, a7);
    FloatTable3::Query q8 = t.where(&v).col_int.greater(3).col_int.less(7);
    int64_t a8 = q8.col_int.sum();
    CHECK_EQUAL(15, a8);
}

TEST(Query_TableViewSum)
{
    TableViewSum ttt;

    ttt.add(1.0, 1.0, 1);
    ttt.add(2.0, 2.0, 2);
    ttt.add(3.0, 3.0, 3);
    ttt.add(4.0, 4.0, 4);
    ttt.add(5.0, 5.0, 5);
    ttt.add(6.0, 6.0, 6);
    ttt.add(7.0, 7.0, 7);
    ttt.add(8.0, 8.0, 8);
    ttt.add(9.0, 9.0, 9);
    ttt.add(10.0, 10.0, 10);

    TableViewSum::Query q1 = ttt.where().col_int.between(5, 9);
    TableViewSum::View tv1 = q1.find_all();
    int64_t s = tv1.column().col_int.sum();
    CHECK_EQUAL(5 + 6 + 7 + 8 + 9, s);
}


TEST(Query_JavaMinimumCrash)
{
    // Test that triggers a bug that was discovered through Java intnerface and has been fixed
    PHPMinimumCrash ttt;

    ttt.add("Joe", "John", 1);
    ttt.add("Jane", "Doe", 2);
    ttt.add("Bob", "Hanson", 3);

    PHPMinimumCrash::Query q1 = ttt.where().firstname.equal("Joe").Or().firstname.equal("Bob");
    int64_t m = q1.salary.minimum();
    CHECK_EQUAL(1, m);
}




TEST(Query_Float4)
{
    FloatTable3 t;

    t.add(std::numeric_limits<float>::max(), std::numeric_limits<double>::max(), 11111);
    t.add(std::numeric_limits<float>::infinity(), std::numeric_limits<double>::infinity(), 11111);
    t.add(12345.0, 12345.0, 11111);

    FloatTable3::Query q1 = t.where();
    float a1 = q1.col_float.maximum();
    double a2 = q1.col_double.maximum();
    CHECK_EQUAL(std::numeric_limits<float>::infinity(), a1);
    CHECK_EQUAL(std::numeric_limits<double>::infinity(), a2);


    FloatTable3::Query q2 = t.where();
    float a3 = q1.col_float.minimum();
    double a4 = q1.col_double.minimum();
    CHECK_EQUAL(12345.0, a3);
    CHECK_EQUAL(12345.0, a4);
}

TEST(Query_Float)
{
    FloatTable t;

    t.add(1.10f, 2.20);
    t.add(1.13f, 2.21);
    t.add(1.13f, 2.22);
    t.add(1.10f, 2.20);
    t.add(1.20f, 3.20);

    // Test find_all()
    FloatTable::View v = t.where().col_float.equal(1.13f).find_all();
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(1.13f, v[0].col_float.get());
    CHECK_EQUAL(1.13f, v[1].col_float.get());

    FloatTable::View v2 = t.where().col_double.equal(3.2).find_all();
    CHECK_EQUAL(1, v2.size());
    CHECK_EQUAL(3.2, v2[0].col_double.get());

    // Test operators (and count)
    CHECK_EQUAL(2, t.where().col_float.equal(1.13f).count());
    CHECK_EQUAL(3, t.where().col_float.not_equal(1.13f).count());
    CHECK_EQUAL(3, t.where().col_float.greater(1.1f).count());
    CHECK_EQUAL(3, t.where().col_float.greater_equal(1.13f).count());
    CHECK_EQUAL(4, t.where().col_float.less_equal(1.13f).count());
    CHECK_EQUAL(2, t.where().col_float.less(1.13f).count());
    CHECK_EQUAL(3, t.where().col_float.between(1.13f, 1.2f).count());

    CHECK_EQUAL(2, t.where().col_double.equal(2.20).count());
    CHECK_EQUAL(3, t.where().col_double.not_equal(2.20).count());
    CHECK_EQUAL(2, t.where().col_double.greater(2.21).count());
    CHECK_EQUAL(3, t.where().col_double.greater_equal(2.21).count());
    CHECK_EQUAL(4, t.where().col_double.less_equal(2.22).count());
    CHECK_EQUAL(3, t.where().col_double.less(2.22).count());
    CHECK_EQUAL(4, t.where().col_double.between(2.20, 2.22).count());

    double epsilon = std::numeric_limits<double>::epsilon();

    // ------ Test sum()
    // ... NO conditions
    double sum1_d = 2.20 + 2.21 + 2.22 + 2.20 + 3.20;
    CHECK_APPROXIMATELY_EQUAL(sum1_d, t.where().col_double.sum(), 10 * epsilon);

    // Note: sum of float is calculated by having a double aggregate to where each float is added
    // (thereby getting casted to double).
    double sum1_f = double(1.10f) + double(1.13f) + double(1.13f) + double(1.10f) + double(1.20f);
    double res = t.where().col_float.sum();
    CHECK_APPROXIMATELY_EQUAL(sum1_f, res, 10 * epsilon);

    // ... with conditions
    double sum2_f = double(1.13f) + double(1.20f);
    double sum2_d = 2.21 + 3.20;
    FloatTable::Query q2 = t.where().col_float.between(1.13f, 1.20f).col_double.not_equal(2.22);
    CHECK_APPROXIMATELY_EQUAL(sum2_f, q2.col_float.sum(), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum2_d, q2.col_double.sum(), 10 * epsilon);

    // ------ Test average()

    // ... NO conditions
    CHECK_APPROXIMATELY_EQUAL(sum1_f / 5, t.where().col_float.average(), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum1_d / 5, t.where().col_double.average(), 10 * epsilon);
    // ... with conditions
    CHECK_APPROXIMATELY_EQUAL(sum2_f / 2, q2.col_float.average(), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum2_d / 2, q2.col_double.average(), 10 * epsilon);

    // -------- Test minimum(), maximum()

    size_t ndx = not_found;

    // ... NO conditions
    CHECK_EQUAL(1.20f, t.where().col_float.maximum());
    t.where().col_float.maximum(nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(4, ndx);

    CHECK_EQUAL(1.10f, t.where().col_float.minimum());
    t.where().col_float.minimum(nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(3.20, t.where().col_double.maximum());
    CHECK_EQUAL(3.20, t.where().col_double.maximum(nullptr, 0, not_found, not_found, &ndx));

    CHECK_EQUAL(2.20, t.where().col_double.minimum());
    t.where().col_double.minimum(nullptr, 0, not_found, not_found, &ndx);

    // ... with conditions
    CHECK_EQUAL(1.20f, q2.col_float.maximum());
    q2.col_float.maximum(nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(4, ndx);

    CHECK_EQUAL(1.13f, q2.col_float.minimum());
    q2.col_float.minimum(nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(1, ndx);

    CHECK_EQUAL(3.20, q2.col_double.maximum());
    q2.col_double.maximum(nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(4, ndx);

    CHECK_EQUAL(2.21, q2.col_double.minimum());
    q2.col_double.minimum(nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(1, ndx);

    size_t count = 0;
    // ... NO conditions
    CHECK_EQUAL(1.20f, t.where().col_float.maximum(&count));
    CHECK_EQUAL(5, count);
    t.where().col_float.maximum(&count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(4, ndx);

    CHECK_EQUAL(1.10f, t.where().col_float.minimum(&count));
    CHECK_EQUAL(5, count);
    t.where().col_float.minimum(&count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(3.20, t.where().col_double.maximum(&count));
    CHECK_EQUAL(5, count);
    t.where().col_double.maximum(&count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(4, ndx);

    CHECK_EQUAL(2.20, t.where().col_double.minimum(&count));
    CHECK_EQUAL(5, count);
    t.where().col_double.minimum(&count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(0, ndx);

    // ... with conditions
    CHECK_EQUAL(1.20f, q2.col_float.maximum(&count));
    CHECK_EQUAL(2, count);
    q2.col_float.maximum(&count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(4, ndx);

    CHECK_EQUAL(1.13f, q2.col_float.minimum(&count));
    CHECK_EQUAL(2, count);
    q2.col_float.minimum(&count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(1, ndx);

    CHECK_EQUAL(3.20, q2.col_double.maximum(&count));
    CHECK_EQUAL(2, count);
    q2.col_double.maximum(&count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(4, ndx);

    CHECK_EQUAL(2.21, q2.col_double.minimum(&count));
    CHECK_EQUAL(2, count);
    q2.col_double.minimum(&count, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(1, ndx);
}


TEST(Query_DateQuery)
{
    PeopleTable table;

    table.add("Mary", 28, false, realm::DateTime(2012, 1, 24), realm::BinaryData("bin \0\n data 1", 13));
    table.add("Frank", 56, true, realm::DateTime(2008, 4, 15), realm::BinaryData("bin \0\n data 2", 13));
    table.add("Bob", 24, true, realm::DateTime(2010, 12, 1), realm::BinaryData("bin \0\n data 3", 13));

    // Find people where hired year == 2012 (hour:minute:second is default initialized to 00:00:00)
    PeopleTable::View view5 = table.where().hired.greater_equal(realm::DateTime(2012, 1, 1).get_datetime())
        .hired.less(realm::DateTime(2013, 1, 1).get_datetime()).find_all();
    CHECK_EQUAL(1, view5.size());
    CHECK_EQUAL("Mary", view5[0].name);
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
        table->set_double(0, t, (t * 12345) % 1000);
        table->set_double(1, t, (t * 12345) % 1000);

        if (table->get_double(0, t) >= 100. && table->get_double(0, t) <= 110. &&
            table->get_double(1, t) >= 100. && table->get_double(1, t) <= 110.)
        {
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
    TupleTableType ttt;

    for (size_t t = 0; t < 10; t++) {
        ttt.add(1, "a");
        ttt.add(4, "b");
        ttt.add(7, "c");
        ttt.add(10, "a");
        ttt.add(1, "b");
        ttt.add(4, "c");
    }

    ttt.optimize();

    ttt.column().second.add_search_index();

    int64_t s = ttt.where().second.equal("a").first.sum();
    CHECK_EQUAL(10 * 11, s);

    s = ttt.where().second.equal("a").first.equal(10).first.sum();
    CHECK_EQUAL(100, s);

    s = ttt.where().first.equal(10).second.equal("a").first.sum();
    CHECK_EQUAL(100, s);

    TupleTableType::View tv = ttt.where().second.equal("a").find_all();
    CHECK_EQUAL(10 * 2, tv.size());
}


TEST(Query_StrIndexedNonEnum)
{
    TupleTableType ttt;

    for (size_t t = 0; t < 10; t++) {
        ttt.add(1, "a");
        ttt.add(4, "b");
        ttt.add(7, "c");
        ttt.add(10, "a");
        ttt.add(1, "b");
        ttt.add(4, "c");
    }

    ttt.column().second.add_search_index();

    int64_t s = ttt.where().second.equal("a").first.sum();
    CHECK_EQUAL(10 * 11, s);

    s = ttt.where().second.equal("a").first.equal(10).first.sum();
    CHECK_EQUAL(100, s);

    s = ttt.where().first.equal(10).second.equal("a").first.sum();
    CHECK_EQUAL(100, s);

    TupleTableType::View tv = ttt.where().second.equal("a").find_all();
    CHECK_EQUAL(10 * 2, tv.size());
}

TEST(Query_FindAllContains2_2)
{
    TupleTableType ttt;

    ttt.add(0, "foo");
    ttt.add(1, "foobar");
    ttt.add(2, "hellofoobar");
    ttt.add(3, "foO");
    ttt.add(4, "foObar");
    ttt.add(5, "hellofoObar");
    ttt.add(6, "hellofo");
    ttt.add(7, "fobar");
    ttt.add(8, "oobar");

    // FIXME: UTF-8 case handling is only implemented on msw for now
    TupleTableType::Query q1 = ttt.where().second.contains("foO", false);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(6, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(1, tv1.get_source_ndx(1));
    CHECK_EQUAL(2, tv1.get_source_ndx(2));
    CHECK_EQUAL(3, tv1.get_source_ndx(3));
    CHECK_EQUAL(4, tv1.get_source_ndx(4));
    CHECK_EQUAL(5, tv1.get_source_ndx(5));
    TupleTableType::Query q2 = ttt.where().second.contains("foO", true);
    TupleTableType::View tv2 = q2.find_all();
    CHECK_EQUAL(3, tv2.size());
    CHECK_EQUAL(3, tv2.get_source_ndx(0));
    CHECK_EQUAL(4, tv2.get_source_ndx(1));
    CHECK_EQUAL(5, tv2.get_source_ndx(2));
}

TEST(Query_SumNewAggregates)
{
    // test the new ACTION_FIND_PATTERN() method in array
    OneIntTable t;
    for (size_t i = 0; i < 1000; i++) {
        t.add(1);
        t.add(2);
        t.add(4);
        t.add(6);
    }
    size_t c = t.where().first.equal(2).count();
    CHECK_EQUAL(1000, c);

    c = t.where().first.greater(2).count();
    CHECK_EQUAL(2000, c);
}


TEST(Query_SumMinMaxAvgForeignCol)
{
    TwoIntTable t;
    t.add(1, 10);
    t.add(2, 20);
    t.add(2, 30);
    t.add(3, 40);

    CHECK_EQUAL(50, t.where().first.equal(2).second.sum());
}


TEST(Query_AggregateSingleCond)
{
    OneIntTable ttt;

    ttt.add(1);
    ttt.add(2);
    ttt.add(2);
    ttt.add(3);
    ttt.add(3);
    ttt.add(4);

    int64_t s = ttt.where().first.equal(2).first.sum();
    CHECK_EQUAL(4, s);

    s = ttt.where().first.greater(2).first.sum();
    CHECK_EQUAL(10, s);

    s = ttt.where().first.less(3).first.sum();
    CHECK_EQUAL(5, s);

    s = ttt.where().first.not_equal(3).first.sum();
    CHECK_EQUAL(9, s);
}

TEST(Query_FindAllRange1)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(4, "a");
    ttt.add(7, "a");
    ttt.add(10, "a");
    ttt.add(1, "a");
    ttt.add(4, "a");
    ttt.add(7, "a");
    ttt.add(10, "a");
    ttt.add(1, "a");
    ttt.add(4, "a");
    ttt.add(7, "a");
    ttt.add(10, "a");

    TupleTableType::Query q1 = ttt.where().second.equal("a");
    TupleTableType::View tv1 = q1.find_all(4, 10);
    CHECK_EQUAL(6, tv1.size());
}


TEST(Query_FindAllRangeOrMonkey2)
{
    const size_t ROWS = 20;
    const size_t ITER = 100;

    Random random(random_int<unsigned long>()); // Seed from slow global generator

    for (size_t u = 0; u < ITER; u++) {
        TwoIntTable tit;
        ArrayInteger a(Allocator::get_default());
        a.create(Array::type_Normal);
        size_t start = random.draw_int_max(ROWS);
        size_t end = start + random.draw_int_max(ROWS);

        if (end > ROWS)
            end = ROWS;

        for (size_t t = 0; t < ROWS; t++) {
            int64_t r1 = random.draw_int_mod(10);
            int64_t r2 = random.draw_int_mod(10);
            tit.add(r1, r2);
        }

        TwoIntTable::Query q1 = tit.where().group().first.equal(3).Or().first.equal(7).end_group().second.greater(5);
        TwoIntTable::View tv1 = q1.find_all(start, end);

        for (size_t t = start; t < end; t++) {
            if ((tit[t].first == 3 || tit[t].first == 7) && tit[t].second > 5)
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
    TupleTableType ttt;

    ttt.add(1, "b");
    ttt.add(2, "a"); //// match
    ttt.add(3, "b"); //
    ttt.add(1, "a"); //// match
    ttt.add(2, "b"); //// match
    ttt.add(3, "a");
    ttt.add(1, "b");
    ttt.add(2, "a"); //// match
    ttt.add(3, "b"); //

    TupleTableType::Query q1 = ttt.where().group().first.greater(1).Or().second.equal("a").end_group().first.less(3);
    TupleTableType::View tv1 = q1.find_all(1, 8);
    CHECK_EQUAL(4, tv1.size());

    TupleTableType::View tv2 = q1.find_all(2, 8);
    CHECK_EQUAL(3, tv2.size());

    TupleTableType::View tv3 = q1.find_all(1, 7);
    CHECK_EQUAL(3, tv3.size());
}


TEST(Query_SimpleStr)
{
    TupleTableType ttt;

    ttt.add(1, "X");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "X");
    ttt.add(6, "X");
    TupleTableType::Query q = ttt.where().second.equal("X");
    size_t c = q.count();

    CHECK_EQUAL(4, c);
}

TEST(Query_Delete)
{
    TupleTableType ttt;

    ttt.add(1, "X");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "X");
    ttt.add(6, "X");

    TupleTableType::Query q = ttt.where().second.equal("X");
    size_t r = q.remove();

    CHECK_EQUAL(4, r);
    CHECK_EQUAL(2, ttt.size());
    CHECK_EQUAL(2, ttt[0].first);
    CHECK_EQUAL(4, ttt[1].first);

    // test remove of all
    ttt.clear();
    ttt.add(1, "X");
    ttt.add(2, "X");
    ttt.add(3, "X");
    TupleTableType::Query q2 = ttt.where().second.equal("X");
    r = q2.remove();
    CHECK_EQUAL(3, r);
    CHECK_EQUAL(0, ttt.size());
}

TEST(Query_DeleteRange)
{
    TupleTableType ttt;

    ttt.add(0, "X");
    ttt.add(1, "X");
    ttt.add(2, "X");
    ttt.add(3, "X");
    ttt.add(4, "X");
    ttt.add(5, "X");

    TupleTableType::Query q = ttt.where().second.equal("X");
    size_t r = q.remove(1, 4);

    CHECK_EQUAL(3, r);
    CHECK_EQUAL(3, ttt.size());
    CHECK_EQUAL(0, ttt[0].first);
    CHECK_EQUAL(4, ttt[1].first);
    CHECK_EQUAL(5, ttt[2].first);
}

TEST(Query_DeleteRange_where)
{
    TupleTableType ttt;

    ttt.add(0, "X");
    ttt.add(1, "X");
    ttt.add(2, "X");
    ttt.add(3, "X");
    ttt.add(4, "X");
    ttt.add(5, "X");

    TupleTableType::View tv = ttt.where().second.equal("X").find_all();
    TupleTableType::Query q = ttt.where(&tv).second.equal("X");

    size_t r = q.remove(1, 4);

    CHECK_EQUAL(3, r);
    CHECK_EQUAL(3, ttt.size());
    CHECK_EQUAL(0, ttt[0].first);
    CHECK_EQUAL(4, ttt[1].first);
    CHECK_EQUAL(5, ttt[2].first);
}

TEST(Query_DeleteLimit)
{
    TupleTableType ttt;

    ttt.add(0, "X");
    ttt.add(1, "X");
    ttt.add(2, "X");
    ttt.add(3, "X");
    ttt.add(4, "X");
    ttt.add(5, "X");

    TupleTableType::Query q = ttt.where().second.equal("X");
    size_t r = q.remove(1, 4, 2);

    CHECK_EQUAL(2, r);
    CHECK_EQUAL(4, ttt.size());
    CHECK_EQUAL(0, ttt[0].first);
    CHECK_EQUAL(3, ttt[1].first);
    CHECK_EQUAL(4, ttt[2].first);
    CHECK_EQUAL(5, ttt[3].first);
}



TEST(Query_Simple)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");

    TupleTableType::Query q1 = ttt.where().first.equal(2);

    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
}

TEST(Query_Not2)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");

    TupleTableType::Query q1 = ttt.where().Not().second.equal("a");

    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(2, tv1.get_source_ndx(0));
}

TEST(Query_SimpleBugDetect)
{
    TupleTableType ttt;
    ttt.add(1, "a");
    ttt.add(2, "a");

    TupleTableType::Query q1 = ttt.where();

    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));

    TupleTableType::View resView = tv1.column().second.find_all("Foo");

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
    table->insert_int(0, 0, 111);
    table->insert_string(1, 0, "this");
    table->insert_subtable(2, 0);
    table->insert_done();

    table->insert_int(0, 1, 222);
    table->insert_string(1, 1, "is");
    table->insert_subtable(2, 1);
    table->insert_done();

    table->insert_int(0, 2, 333);
    table->insert_string(1, 2, "a test");
    table->insert_subtable(2, 2);
    table->insert_done();

    table->insert_int(0, 3, 444);
    table->insert_string(1, 3, "of queries");
    table->insert_subtable(2, 3);
    table->insert_done();


    // Sub tables
    TableRef subtable = table->get_subtable(2, 0);
    subtable->insert_int(0, 0, 11);
    subtable->insert_string(1, 0, "a");
    subtable->insert_done();

    subtable = table->get_subtable(2, 1);
    subtable->insert_int(0, 0, 22);
    subtable->insert_string(1, 0, "b");
    subtable->insert_done();
    subtable->insert_int(0, 1, 33);
    subtable->insert_string(1, 1, "c");
    subtable->insert_done();

    //  Intentioally have empty (degenerate) subtable at 2,2

    subtable = table->get_subtable(2, 3);
    subtable->insert_int(0, 0, 55);
    subtable->insert_string(1, 0, "e");
    subtable->insert_done();


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
    q4.equal(0, (int64_t)333);
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

    for (int i = 0; i<5; i++) {
        table->insert_int(0, i, 100);
        table->insert_subtable(1, i);
        table->insert_done();
    }
    TableRef subtable = table->get_subtable(1, 0);
    subtable->insert_int(0, 0, 11);
    subtable->insert_string(1, 0, "a");
    subtable->insert_bool(2, 0, true);
    subtable->insert_done();

    Query q1 = table->where();
    q1.subtable(1);
    q1.equal(2, true);
    q1.end_subtable();
    std::string s = q1.validate();

    TableView t1 = q1.find_all(0, size_t(-1));
    CHECK_EQUAL(1, t1.size());
}

/*
// Disabled because assert has now been added to disallow adding rows when no columns exist
TEST(Query_SubtableViewSizeBug)
{
Table table;
table.add_column(type_Table, "subtab");
table.add_empty_row(1);
TableRef subtab = table.get_subtable(0,0);
subtab->add_empty_row(1);
TableView subview = subtab->where().find_all();
CHECK_EQUAL(1, subview.size());
}
*/

TEST(Query_Sort1)
{
    TupleTableType ttt;

    ttt.add(1, "a"); // 0
    ttt.add(2, "a"); // 1
    ttt.add(3, "X"); // 2
    ttt.add(1, "a"); // 3
    ttt.add(2, "a"); // 4
    ttt.add(3, "X"); // 5
    ttt.add(9, "a"); // 6
    ttt.add(8, "a"); // 7
    ttt.add(7, "X"); // 8

    // tv.get_source_ndx()  = 0, 2, 3, 5, 6, 7, 8
    // Vals         = 1, 3, 1, 3, 9, 8, 7
    // result       = 3, 0, 5, 2, 8, 7, 6

    TupleTableType::Query q = ttt.where().first.not_equal(2);
    TupleTableType::View tv = q.find_all();
    tv.column().first.sort();

    CHECK(tv.size() == 7);
    CHECK(tv[0].first == 1);
    CHECK(tv[1].first == 1);
    CHECK(tv[2].first == 3);
    CHECK(tv[3].first == 3);
    CHECK(tv[4].first == 7);
    CHECK(tv[5].first == 8);
    CHECK(tv[6].first == 9);
}



TEST(Query_QuickSort)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    // Triggers QuickSort because range > len
    TupleTableType ttt;

    for (size_t t = 0; t < 1000; t++)
        ttt.add(random.draw_int_mod(1100), "a"); // 0

    TupleTableType::Query q = ttt.where();
    TupleTableType::View tv = q.find_all();
    tv.column().first.sort();

    CHECK(tv.size() == 1000);
    for (size_t t = 1; t < tv.size(); t++) {
        CHECK(tv[t].first >= tv[t - 1].first);
    }
}

TEST(Query_CountSort)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    // Triggers CountSort because range <= len
    TupleTableType ttt;

    for (size_t t = 0; t < 1000; t++)
        ttt.add(random.draw_int_mod(900), "a"); // 0

    TupleTableType::Query q = ttt.where();
    TupleTableType::View tv = q.find_all();
    tv.column().first.sort();

    CHECK(tv.size() == 1000);
    for (size_t t = 1; t < tv.size(); t++) {
        CHECK(tv[t].first >= tv[t - 1].first);
    }
}


TEST(Query_SortDescending)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    TupleTableType ttt;

    for (size_t t = 0; t < 1000; t++)
        ttt.add(random.draw_int_mod(1100), "a"); // 0

    TupleTableType::Query q = ttt.where();
    TupleTableType::View tv = q.find_all();
    tv.column().first.sort(false);

    CHECK(tv.size() == 1000);
    for (size_t t = 1; t < tv.size(); t++) {
        CHECK(tv[t].first <= tv[t - 1].first);
    }
}


TEST(Query_SortDates)
{
    Table table;
    table.add_column(type_DateTime, "first");

    table.insert_datetime(0, 0, 1000);
    table.insert_done();
    table.insert_datetime(0, 1, 3000);
    table.insert_done();
    table.insert_datetime(0, 2, 2000);
    table.insert_done();

    TableView tv = table.where().find_all();
    CHECK(tv.size() == 3);
    CHECK(tv.get_source_ndx(0) == 0);
    CHECK(tv.get_source_ndx(1) == 1);
    CHECK(tv.get_source_ndx(2) == 2);

    tv.sort(0);

    CHECK(tv.size() == 3);
    CHECK(tv.get_datetime(0, 0) == DateTime(1000));
    CHECK(tv.get_datetime(0, 1) == DateTime(2000));
    CHECK(tv.get_datetime(0, 2) == DateTime(3000));
}


TEST(Query_SortBools)
{
    Table table;
    table.add_column(type_Bool, "first");

    table.insert_bool(0, 0, true);
    table.insert_done();
    table.insert_bool(0, 0, false);
    table.insert_done();
    table.insert_bool(0, 0, true);
    table.insert_done();

    TableView tv = table.where().find_all();
    tv.sort(0);

    CHECK(tv.size() == 3);
    CHECK(tv.get_bool(0, 0) == false);
    CHECK(tv.get_bool(0, 1) == true);
    CHECK(tv.get_bool(0, 2) == true);
}


TEST(Query_Sort_And_Requery_Typed1)
{
    TupleTableType ttt;

    ttt.add(1, "a"); // 0 *
    ttt.add(2, "a"); // 1
    ttt.add(3, "X"); // 2
    ttt.add(1, "a"); // 3 *
    ttt.add(2, "a"); // 4
    ttt.add(3, "X"); // 5
    ttt.add(9, "a"); // 6 *
    ttt.add(8, "a"); // 7 *
    ttt.add(7, "X"); // 8

    // tv.get_source_ndx()  = 0, 2, 3, 5, 6, 7, 8
    // Vals         = 1, 3, 1, 3, 9, 8, 7
    // result       = 3, 0, 5, 2, 8, 7, 6

    TupleTableType::Query q = ttt.where().first.not_equal(2);
    TupleTableType::View tv = q.find_all();

    size_t match = ttt.where(&tv).first.equal(7).find();
    CHECK_EQUAL(match, 6);

    tv.column().first.sort();

    CHECK(tv.size() == 7);
    CHECK(tv[0].first == 1);
    CHECK(tv[1].first == 1);
    CHECK(tv[2].first == 3);
    CHECK(tv[3].first == 3);
    CHECK(tv[4].first == 7);
    CHECK(tv[5].first == 8);
    CHECK(tv[6].first == 9);

    TupleTableType::Query q2 = ttt.where(&tv).second.not_equal("X");
    TupleTableType::View tv2 = q2.find_all();

    CHECK_EQUAL(4, tv2.size());
    CHECK_EQUAL(1, tv2[0].first);
    CHECK_EQUAL(1, tv2[1].first);
    CHECK_EQUAL(8, tv2[2].first); // 8, 9 (sort order) instead of 9, 8 (table order)
    CHECK_EQUAL(9, tv2[3].first);

    match = ttt.where(&tv).second.not_equal("X").find();
    CHECK_EQUAL(match, 0);

    match = ttt.where(&tv).second.not_equal("X").find(1);
    CHECK_EQUAL(match, 1);

    match = ttt.where(&tv).second.not_equal("X").find(2);
    CHECK_EQUAL(match, 5);

    match = ttt.where(&tv).second.not_equal("X").find(6);
    CHECK_EQUAL(match, 6);
}



TEST(Query_Sort_And_Requery_FindFirst)
{
    TwoIntTable ttt;

    ttt.add(1, 60);
    ttt.add(2, 50); // **
    ttt.add(3, 40); // *
    ttt.add(1, 30);
    ttt.add(2, 20); // **
    ttt.add(3, 10); // **

    TwoIntTable::Query q = ttt.where().first.greater(1);
    TwoIntTable::View tv = q.find_all();
    tv.column().second.sort();

    // 3, 2, 1, 3, 2, 1
    size_t t = ttt.where(&tv).first.equal(3).find();
    int64_t s = ttt.where(&tv).second.not_equal(40).first.sum();

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

        for (size_t t = 0; t < tv5.size() - 1; t++) {
            CHECK(tv5.get_source_ndx(t) < tv5.get_source_ndx(t + 1));
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
    TupleTableType ttt;

    // Spread query search hits in an odd way to test more edge cases
    // (thread job size is THREAD_CHUNK_SIZE = 10)
    for (int i = 0; i < 30; i++) {
        for (int j = 0; j < 10; j++) {
            ttt.add(5, "a");
            ttt.add(j, "b");
            ttt.add(6, "c");
            ttt.add(6, "a");
            ttt.add(6, "b");
            ttt.add(6, "c");
            ttt.add(6, "a");
        }
    }
    TupleTableType::Query q1 = ttt.where().first.equal(2).second.equal("b");

    // Note, set THREAD_CHUNK_SIZE to 1.000.000 or more for performance
    //q1.set_threads(5);
    TupleTableType::View tv = q1.find_all();

    CHECK_EQUAL(30, tv.size());
    for (int i = 0; i < 30; i++) {
        const size_t expected = i * 7 * 10 + 14 + 1;
        const size_t actual = tv.get_source_ndx(i);
        CHECK_EQUAL(expected, actual);
    }
}


TEST(Query_LongString)
{
    TupleTableType ttt;

    // Spread query search hits in an odd way to test more edge cases
    // (thread job size is THREAD_CHUNK_SIZE = 10)
    for (int i = 0; i < 30; i++) {
        for (int j = 0; j < 10; j++) {
            ttt.add(5, "aaaaaaaaaaaaaaaaaa");
            ttt.add(j, "bbbbbbbbbbbbbbbbbb");
            ttt.add(6, "cccccccccccccccccc");
            ttt.add(6, "aaaaaaaaaaaaaaaaaa");
            ttt.add(6, "bbbbbbbbbbbbbbbbbb");
            ttt.add(6, "cccccccccccccccccc");
            ttt.add(6, "aaaaaaaaaaaaaaaaaa");
        }
    }
    TupleTableType::Query q1 = ttt.where().first.equal(2).second.equal("bbbbbbbbbbbbbbbbbb");

    // Note, set THREAD_CHUNK_SIZE to 1.000.000 or more for performance
    //q1.set_threads(5);
    TupleTableType::View tv = q1.find_all();

    CHECK_EQUAL(30, tv.size());
    for (int i = 0; i < 30; i++) {
        const size_t expected = i * 7 * 10 + 14 + 1;
        const size_t actual = tv.get_source_ndx(i);
        CHECK_EQUAL(expected, actual);
    }
}


TEST(Query_LongEnum)
{
    TupleTableType ttt;

    // Spread query search hits in an odd way to test more edge cases
    // (thread job size is THREAD_CHUNK_SIZE = 10)
    for (int i = 0; i < 30; i++) {
        for (int j = 0; j < 10; j++) {
            ttt.add(5, "aaaaaaaaaaaaaaaaaa");
            ttt.add(j, "bbbbbbbbbbbbbbbbbb");
            ttt.add(6, "cccccccccccccccccc");
            ttt.add(6, "aaaaaaaaaaaaaaaaaa");
            ttt.add(6, "bbbbbbbbbbbbbbbbbb");
            ttt.add(6, "cccccccccccccccccc");
            ttt.add(6, "aaaaaaaaaaaaaaaaaa");
        }
    }
    ttt.optimize();
    TupleTableType::Query q1 = ttt.where().first.equal(2).second.not_equal("aaaaaaaaaaaaaaaaaa");

    // Note, set THREAD_CHUNK_SIZE to 1.000.000 or more for performance
    //q1.set_threads(5);
    TupleTableType::View tv = q1.find_all();

    CHECK_EQUAL(30, tv.size());
    for (int i = 0; i < 30; i++) {
        const size_t expected = i * 7 * 10 + 14 + 1;
        const size_t actual = tv.get_source_ndx(i);
        CHECK_EQUAL(expected, actual);
    }
}

TEST(Query_BigString)
{
    TupleTableType ttt;
    ttt.add(1, "a");
    size_t res1 = ttt.where().second.equal("a").find();
    CHECK_EQUAL(0, res1);

    ttt.add(2, "40 chars  40 chars  40 chars  40 chars  ");
    size_t res2 = ttt.where().second.equal("40 chars  40 chars  40 chars  40 chars  ").find();
    CHECK_EQUAL(1, res2);

    ttt.add(1, "70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
    size_t res3 = ttt.where().second.equal("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ").find();
    CHECK_EQUAL(2, res3);
}

TEST(Query_Simple2)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");

    TupleTableType::Query q1 = ttt.where().first.equal(2);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(4, tv1.get_source_ndx(1));
    CHECK_EQUAL(7, tv1.get_source_ndx(2));
}


TEST(Query_Limit)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a"); //
    ttt.add(3, "X");
    ttt.add(1, "a");
    ttt.add(2, "a"); //
    ttt.add(3, "X");
    ttt.add(1, "a");
    ttt.add(2, "a"); //
    ttt.add(3, "X");
    ttt.add(1, "a");
    ttt.add(2, "a"); //
    ttt.add(3, "X");
    ttt.add(1, "a");
    ttt.add(2, "a"); //
    ttt.add(3, "X");

    TupleTableType::Query q1 = ttt.where().first.equal(2);

    TupleTableType::View tv1 = q1.find_all(0, size_t(-1), 2);
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(4, tv1.get_source_ndx(1));

    TupleTableType::View tv2 = q1.find_all(tv1.get_source_ndx(tv1.size() - 1) + 1, size_t(-1), 2);
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(7, tv2.get_source_ndx(0));
    CHECK_EQUAL(10, tv2.get_source_ndx(1));

    TupleTableType::View tv3 = q1.find_all(tv2.get_source_ndx(tv2.size() - 1) + 1, size_t(-1), 2);
    CHECK_EQUAL(1, tv3.size());
    CHECK_EQUAL(13, tv3.get_source_ndx(0));


    TupleTableType::Query q2 = ttt.where();
    TupleTableType::View tv4 = q2.find_all(0, 5, 3);
    CHECK_EQUAL(3, tv4.size());

    TupleTableType::Query q3 = ttt.where();
    TupleTableType::View tv5 = q3.find_all(0, 3, 5);
    CHECK_EQUAL(3, tv5.size());
}


TEST(Query_FindNext)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(6, "X");
    ttt.add(7, "X");

    TupleTableType::Query q1 = ttt.where().second.equal("X").first.greater(4);

    const size_t res1 = q1.find();
    const size_t res2 = q1.find(res1 + 1);
    const size_t res3 = q1.find(res2 + 1);

    CHECK_EQUAL(5, res1);
    CHECK_EQUAL(6, res2);
    CHECK_EQUAL(not_found, res3); // no more matches

    // Do same searches with new query every time
    const size_t res4 = ttt.where().second.equal("X").first.greater(4).find();
    const size_t res5 = ttt.where().second.equal("X").first.greater(4).find(res1 + 1);
    const size_t res6 = ttt.where().second.equal("X").first.greater(4).find(res2 + 1);

    CHECK_EQUAL(5, res4);
    CHECK_EQUAL(6, res5);
    CHECK_EQUAL(not_found, res6); // no more matches
}


TEST(Query_FindNextBackwards)
{
    TupleTableType ttt;

    // Create multiple leaves
    for (size_t i = 0; i < REALM_MAX_BPNODE_SIZE * 4; i++) {
        ttt.add(6, "X");
        ttt.add(7, "X");
    }

    TupleTableType::Query q = ttt.where().first.greater(4);

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

    TupleTableType ttt;
    int64_t search = REALM_MAX_BPNODE_SIZE / 2;
    size_t rows = REALM_MAX_BPNODE_SIZE * 20;

    // Create multiple leaves
    for (size_t i = 0; i < rows; i++) {
        // This value distribution makes us sometimes cross a leaf boundary, and sometimes not, with both having
        // a fair probability of happening
        ttt.add(random.draw_int_mod(REALM_MAX_BPNODE_SIZE), "X");
    }

    TupleTableType::Query q = ttt.where().first.equal(search);

    for (size_t t = 0; t < 100; t++) {
        size_t begin = random.draw_int_mod(rows);
        size_t res = q.find(begin);

        // Find correct match position manually in a for-loop
        size_t expected = not_found;
        for (size_t u = begin; u < rows; u++) {
            if (ttt.column().first[u] == search) {
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
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(6, "X");
    ttt.add(7, "X"); // match

    TupleTableType::Query q1 = ttt.where().second.equal("X").first.greater(4);

    const size_t res1 = q1.find(6);
    CHECK_EQUAL(6, res1);
}

TEST(Query_FindAll1)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(6, "X");
    ttt.add(7, "X");

    TupleTableType::Query q1 = ttt.where().second.equal("a").first.greater(2).first.not_equal(4);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.get_source_ndx(0));

    TupleTableType::Query q2 = ttt.where().second.equal("X").first.greater(4);
    TupleTableType::View tv2 = q2.find_all();
    CHECK_EQUAL(5, tv2.get_source_ndx(0));
    CHECK_EQUAL(6, tv2.get_source_ndx(1));

}

TEST(Query_FindAll2)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");
    ttt.add(0, "X");

    TupleTableType::Query q2 = ttt.where().second.not_equal("a").first.less(3);
    TupleTableType::View tv2 = q2.find_all();
    CHECK_EQUAL(6, tv2.get_source_ndx(0));
}

TEST(Query_FindAllBetween)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");
    ttt.add(3, "X");

    TupleTableType::Query q2 = ttt.where().first.between(3, 5);
    TupleTableType::View tv2 = q2.find_all();
    CHECK_EQUAL(2, tv2.get_source_ndx(0));
    CHECK_EQUAL(3, tv2.get_source_ndx(1));
    CHECK_EQUAL(4, tv2.get_source_ndx(2));
    CHECK_EQUAL(6, tv2.get_source_ndx(3));
}


TEST(Query_FindAllRange)
{
    TupleTableType ttt;

    ttt.add(5, "a");
    ttt.add(5, "a");
    ttt.add(5, "a");

    TupleTableType::Query q1 = ttt.where().second.equal("a").first.greater(2).first.not_equal(4);
    TupleTableType::View tv1 = q1.find_all(1, 2);
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
}


TEST(Query_FindAllOr)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(6, "a");
    ttt.add(7, "X");

    // first == 5 || second == X
    TupleTableType::Query q1 = ttt.where().first.equal(5).Or().second.equal("X");
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(2, tv1.get_source_ndx(0));
    CHECK_EQUAL(4, tv1.get_source_ndx(1));
    CHECK_EQUAL(6, tv1.get_source_ndx(2));
}


TEST(Query_FindAllParens1)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");

    // first > 3 && (second == X)
    TupleTableType::Query q1 = ttt.where().first.greater(3).group().second.equal("X").end_group();
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(6, tv1.get_source_ndx(0));
}


TEST(Query_FindAllOrParan)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X"); //
    ttt.add(4, "a");
    ttt.add(5, "a"); //
    ttt.add(6, "a");
    ttt.add(7, "X"); //
    ttt.add(2, "X");

    // (first == 5 || second == X && first > 2)
    TupleTableType::Query q1 = ttt.where().group().first.equal(5).Or().second.equal("X").first.greater(2).end_group();
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(2, tv1.get_source_ndx(0));
    CHECK_EQUAL(4, tv1.get_source_ndx(1));
    CHECK_EQUAL(6, tv1.get_source_ndx(2));
}


TEST(Query_FindAllOrNested0)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");
    ttt.add(8, "Y");

    // first > 3 && (first == 5 || second == X)
    TupleTableType::Query q1 = ttt.where().first.greater(3).group().first.equal(5).Or().second.equal("X").end_group();
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(5, tv1.get_source_ndx(0));
    CHECK_EQUAL(6, tv1.get_source_ndx(1));
}

TEST(Query_FindAllOrNested)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");
    ttt.add(8, "Y");

    // first > 3 && (first == 5 || second == X || second == Y)
    TupleTableType::Query q1 = ttt.where().first.greater(3).group().first.equal(5).Or().second.equal("X").Or().second.equal("Y").end_group();
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(5, tv1.get_source_ndx(0));
    CHECK_EQUAL(6, tv1.get_source_ndx(1));
    CHECK_EQUAL(7, tv1.get_source_ndx(2));
}

TEST(Query_FindAllOrNestedInnerGroup)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");
    ttt.add(8, "Y");

    // first > 3 && (first == 5 || (second == X || second == Y))
    TupleTableType::Query q1 = ttt.where().first.greater(3).group().first.equal(5).Or().group().second.equal("X").Or().second.equal("Y").end_group().end_group();
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(5, tv1.get_source_ndx(0));
    CHECK_EQUAL(6, tv1.get_source_ndx(1));
    CHECK_EQUAL(7, tv1.get_source_ndx(2));
}

TEST(Query_FindAllOrPHP)
{
    TupleTableType ttt;

    ttt.add(1, "Joe");
    ttt.add(2, "Sara");
    ttt.add(3, "Jim");

    // (second == Jim || second == Joe) && first = 1
    TupleTableType::Query q1 = ttt.where().group().second.equal("Jim").Or().second.equal("Joe").end_group().first.equal(1);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
}

TEST(Query_FindAllOr2)
{
    TupleTableType ttt;

    ttt.add(1, "Joe");
    ttt.add(2, "Sara");
    ttt.add(3, "Jim");

    // (second == Jim || second == Joe) && first = 1
    TupleTableType::Query q1 = ttt.where().group().second.equal("Jim").Or().second.equal("Joe").end_group().first.equal(3);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.get_source_ndx(0));
}





TEST(Query_FindAllParens2)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");

    // ()((first > 3()) && (()))
    TupleTableType::Query q1 = ttt.where().group().end_group().group().group().first.greater(3).group().end_group().end_group().group().group().end_group().end_group().end_group();
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(4, tv1.get_source_ndx(0));
    CHECK_EQUAL(5, tv1.get_source_ndx(1));
    CHECK_EQUAL(6, tv1.get_source_ndx(2));
}

TEST(Query_FindAllParens4)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");

    // ()
    TupleTableType::Query q1 = ttt.where().group().end_group();
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(7, tv1.size());
}


TEST(Query_FindAllBool)
{
    BoolTupleTable btt;

    btt.add(1, true);
    btt.add(2, false);
    btt.add(3, true);
    btt.add(3, false);

    BoolTupleTable::Query q1 = btt.where().second.equal(true);
    BoolTupleTable::View tv1 = q1.find_all();
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));

    BoolTupleTable::Query q2 = btt.where().second.equal(false);
    BoolTupleTable::View tv2 = q2.find_all();
    CHECK_EQUAL(1, tv2.get_source_ndx(0));
    CHECK_EQUAL(3, tv2.get_source_ndx(1));
}

TEST(Query_FindAllBegins)
{
    TupleTableType ttt;

    ttt.add(0, "fo");
    ttt.add(0, "foo");
    ttt.add(0, "foobar");

    TupleTableType::Query q1 = ttt.where().second.begins_with("foo");
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));
}

TEST(Query_FindAllEnds)
{

    TupleTableType ttt;

    ttt.add(0, "barfo");
    ttt.add(0, "barfoo");
    ttt.add(0, "barfoobar");

    TupleTableType::Query q1 = ttt.where().second.ends_with("foo");
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
}


TEST(Query_FindAllContains)
{
    TupleTableType ttt;

    ttt.add(0, "foo");
    ttt.add(0, "foobar");
    ttt.add(0, "barfoo");
    ttt.add(0, "barfoobaz");
    ttt.add(0, "fo");
    ttt.add(0, "fobar");
    ttt.add(0, "barfo");

    TupleTableType::Query q1 = ttt.where().second.contains("foo");
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(1, tv1.get_source_ndx(1));
    CHECK_EQUAL(2, tv1.get_source_ndx(2));
    CHECK_EQUAL(3, tv1.get_source_ndx(3));
}

TEST(Query_Binary)
{
    TupleTableTypeBin t;

    const char bin[64] = {
        6, 3, 9, 5, 9, 7, 6, 3, 2, 6, 0, 0, 5, 4, 2, 4,
        5, 7, 9, 5, 7, 1, 1, 2, 0, 8, 3, 8, 0, 9, 6, 8,
        4, 7, 3, 4, 9, 5, 2, 3, 6, 2, 7, 4, 0, 3, 7, 6,
        2, 3, 5, 9, 3, 1, 2, 1, 0, 5, 5, 2, 9, 4, 5, 9
    };

    const char bin_2[4] = { 6, 6, 6, 6 }; // Not occuring above

    t.add(0, BinaryData(bin + 0, 16));
    t.add(0, BinaryData(bin + 0, 32));
    t.add(0, BinaryData(bin + 0, 48));
    t.add(0, BinaryData(bin + 0, 64));
    t.add(0, BinaryData(bin + 16, 48));
    t.add(0, BinaryData(bin + 32, 32));
    t.add(0, BinaryData(bin + 48, 16));
    t.add(0, BinaryData(bin + 24, 16)); // The "odd ball"
    t.add(0, BinaryData(bin + 0, 32)); // Repeat an entry

    CHECK_EQUAL(0, t.where().second.equal(BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(1, t.where().second.equal(BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(1, t.where().second.equal(BinaryData(bin + 48, 16)).count());
    CHECK_EQUAL(2, t.where().second.equal(BinaryData(bin + 0, 32)).count());

    CHECK_EQUAL(9, t.where().second.not_equal(BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(8, t.where().second.not_equal(BinaryData(bin + 0, 16)).count());

    CHECK_EQUAL(0, t.where().second.begins_with(BinaryData(bin + 8, 16)).count());
    CHECK_EQUAL(1, t.where().second.begins_with(BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(4, t.where().second.begins_with(BinaryData(bin + 0, 32)).count());
    CHECK_EQUAL(5, t.where().second.begins_with(BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(1, t.where().second.begins_with(BinaryData(bin + 48, 16)).count());
    CHECK_EQUAL(9, t.where().second.begins_with(BinaryData(bin + 0, 0)).count());

    CHECK_EQUAL(0, t.where().second.ends_with(BinaryData(bin + 40, 16)).count());
    CHECK_EQUAL(1, t.where().second.ends_with(BinaryData(bin + 32, 16)).count());
    CHECK_EQUAL(3, t.where().second.ends_with(BinaryData(bin + 32, 32)).count());
    CHECK_EQUAL(4, t.where().second.ends_with(BinaryData(bin + 48, 16)).count());
    CHECK_EQUAL(1, t.where().second.ends_with(BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(9, t.where().second.ends_with(BinaryData(bin + 64, 0)).count());

    CHECK_EQUAL(0, t.where().second.contains(BinaryData(bin_2)).count());
    CHECK_EQUAL(5, t.where().second.contains(BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(5, t.where().second.contains(BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(4, t.where().second.contains(BinaryData(bin + 24, 16)).count());
    CHECK_EQUAL(4, t.where().second.contains(BinaryData(bin + 32, 16)).count());
    CHECK_EQUAL(9, t.where().second.contains(BinaryData(bin + 0, 0)).count());

    {
        TupleTableTypeBin::View tv = t.where().second.equal(BinaryData(bin + 0, 32)).find_all();
        if (tv.size() == 2) {
            CHECK_EQUAL(1, tv.get_source_ndx(0));
            CHECK_EQUAL(8, tv.get_source_ndx(1));
        }
        else CHECK(false);
    }

    {
        TupleTableTypeBin::View tv = t.where().second.contains(BinaryData(bin + 24, 16)).find_all();
        if (tv.size() == 4) {
            CHECK_EQUAL(2, tv.get_source_ndx(0));
            CHECK_EQUAL(3, tv.get_source_ndx(1));
            CHECK_EQUAL(4, tv.get_source_ndx(2));
            CHECK_EQUAL(7, tv.get_source_ndx(3));
        }
        else CHECK(false);
    }
}


TEST(Query_Enums)
{
    TupleTableType table;

    for (size_t i = 0; i < 5; ++i) {
        table.add(1, "abd");
        table.add(2, "eftg");
        table.add(5, "hijkl");
        table.add(8, "mnopqr");
        table.add(9, "stuvxyz");
    }

    table.optimize();

    TupleTableType::Query q1 = table.where().second.equal("eftg");
    TupleTableType::View tv1 = q1.find_all();

    CHECK_EQUAL(5, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(6, tv1.get_source_ndx(1));
    CHECK_EQUAL(11, tv1.get_source_ndx(2));
    CHECK_EQUAL(16, tv1.get_source_ndx(3));
    CHECK_EQUAL(21, tv1.get_source_ndx(4));
}


#define uY  "\x0CE\x0AB"              // greek capital letter upsilon with dialytika (U+03AB)
#define uYd "\x0CE\x0A5\x0CC\x088"    // decomposed form (Y followed by two dots)
#define uy  "\x0CF\x08B"              // greek small letter upsilon with dialytika (U+03AB)
#define uyd "\x0cf\x085\x0CC\x088"    // decomposed form (Y followed by two dots)

#define uA  "\x0c3\x085"         // danish capital A with ring above (as in BLAABAERGROED)
#define uAd "\x041\x0cc\x08a"    // decomposed form (A (41) followed by ring)
#define ua  "\x0c3\x0a5"         // danish lower case a with ring above (as in blaabaergroed)
#define uad "\x061\x0cc\x08a"    // decomposed form (a (41) followed by ring)

TEST(Query_CaseSensitivity)
{
    TupleTableType ttt;

    ttt.add(1, "BLAAbaergroed");
    ttt.add(1, "BLAAbaergroedandMORE");
    ttt.add(1, "BLAAbaergroed2");

    TupleTableType::Query q1 = ttt.where().second.equal("blaabaerGROED", false);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
}

#if (defined(_WIN32) || defined(__WIN32__) || defined(_WIN64))

TEST(Query_Unicode2)
{
    TupleTableType ttt;

    ttt.add(1, uY);
    ttt.add(1, uYd);
    ttt.add(1, uy);
    ttt.add(1, uyd);

    TupleTableType::Query q1 = ttt.where().second.equal(uY, false);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));

    TupleTableType::Query q2 = ttt.where().second.equal(uYd, false);
    TupleTableType::View tv2 = q2.find_all();
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(1, tv2.get_source_ndx(0));
    CHECK_EQUAL(3, tv2.get_source_ndx(1));

    TupleTableType::Query q3 = ttt.where().second.equal(uYd, true);
    TupleTableType::View tv3 = q3.find_all();
    CHECK_EQUAL(1, tv3.size());
    CHECK_EQUAL(1, tv3.get_source_ndx(0));
}

TEST(Query_Unicode3)
{
    TupleTableType ttt;

    ttt.add(1, uA);
    ttt.add(1, uAd);
    ttt.add(1, ua);
    ttt.add(1, uad);

    TupleTableType::Query q1 = ttt.where().second.equal(uA, false);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));

    TupleTableType::Query q2 = ttt.where().second.equal(ua, false);
    TupleTableType::View tv2 = q2.find_all();
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(0, tv2.get_source_ndx(0));
    CHECK_EQUAL(2, tv2.get_source_ndx(1));


    TupleTableType::Query q3 = ttt.where().second.equal(uad, false);
    TupleTableType::View tv3 = q3.find_all();
    CHECK_EQUAL(2, tv3.size());
    CHECK_EQUAL(1, tv3.get_source_ndx(0));
    CHECK_EQUAL(3, tv3.get_source_ndx(1));

    TupleTableType::Query q4 = ttt.where().second.equal(uad, true);
    TupleTableType::View tv4 = q4.find_all();
    CHECK_EQUAL(1, tv4.size());
    CHECK_EQUAL(3, tv4.get_source_ndx(0));
}

#endif

TEST(Query_FindAllBeginsUnicode)
{
    TupleTableType ttt;

    ttt.add(0, uad "fo");
    ttt.add(0, uad "foo");
    ttt.add(0, uad "foobar");

    TupleTableType::Query q1 = ttt.where().second.begins_with(uad "foo");
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));
}


TEST(Query_FindAllEndsUnicode)
{
    TupleTableType ttt;

    ttt.add(0, "barfo");
    ttt.add(0, "barfoo" uad);
    ttt.add(0, "barfoobar");

    TupleTableType::Query q1 = ttt.where().second.ends_with("foo" uad);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));

    TupleTableType::Query q2 = ttt.where().second.ends_with("foo" uAd, false);
    TupleTableType::View tv2 = q2.find_all();
    CHECK_EQUAL(1, tv2.size());
    CHECK_EQUAL(1, tv2.get_source_ndx(0));
}


TEST(Query_FindAllContainsUnicode)
{
    TupleTableType ttt;

    ttt.add(0, uad "foo");
    ttt.add(0, uad "foobar");
    ttt.add(0, "bar" uad "foo");
    ttt.add(0, uad "bar" uad "foobaz");
    ttt.add(0, uad "fo");
    ttt.add(0, uad "fobar");
    ttt.add(0, uad "barfo");

    TupleTableType::Query q1 = ttt.where().second.contains(uad "foo");
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(1, tv1.get_source_ndx(1));
    CHECK_EQUAL(2, tv1.get_source_ndx(2));
    CHECK_EQUAL(3, tv1.get_source_ndx(3));

    TupleTableType::Query q2 = ttt.where().second.contains(uAd "foo", false);
    TupleTableType::View tv2 = q1.find_all();
    CHECK_EQUAL(4, tv2.size());
    CHECK_EQUAL(0, tv2.get_source_ndx(0));
    CHECK_EQUAL(1, tv2.get_source_ndx(1));
    CHECK_EQUAL(2, tv2.get_source_ndx(2));
    CHECK_EQUAL(3, tv2.get_source_ndx(3));
}

TEST(Query_SyntaxCheck)
{
    TupleTableType ttt;
    std::string s;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");

    TupleTableType::Query q1 = ttt.where().first.equal(2).end_group();
    s = q1.validate();
    CHECK(s != "");

    TupleTableType::Query q2 = ttt.where().group().group().first.equal(2).end_group();
    s = q2.validate();
    CHECK(s != "");

    TupleTableType::Query q3 = ttt.where().first.equal(2).Or();
    s = q3.validate();
    CHECK(s != "");

    TupleTableType::Query q4 = ttt.where().Or().first.equal(2);
    s = q4.validate();
    CHECK(s != "");

    TupleTableType::Query q5 = ttt.where().first.equal(2);
    s = q5.validate();
    CHECK(s == "");

    TupleTableType::Query q6 = ttt.where().group().first.equal(2);
    s = q6.validate();
    CHECK(s != "");

    // FIXME: Work is currently underway to fully support locale
    // indenepdent case folding as defined by Unicode. Reenable this test
    // when is becomes available.
    /*
    TupleTableType::Query q7 = ttt.where().second.equal("\xa0", false);
    #ifdef REALM_DEBUG
    s = q7.Verify();
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
    table->insert_int(0, 0, 111);
    table->insert_string(1, 0, "this");
    table->insert_subtable(2, 0);
    table->insert_done();

    table->insert_int(0, 1, 222);
    table->insert_string(1, 1, "is");
    table->insert_subtable(2, 1);
    table->insert_done();

    table->insert_int(0, 2, 333);
    table->insert_string(1, 2, "a test");
    table->insert_subtable(2, 2);
    table->insert_done();

    table->insert_int(0, 3, 444);
    table->insert_string(1, 3, "of queries");
    table->insert_subtable(2, 3);
    table->insert_done();


    // Sub tables
    TableRef subtable = table->get_subtable(2, 0);
    subtable->insert_int(0, 0, 11);
    subtable->insert_string(1, 0, "a");
    subtable->insert_done();

    subtable = table->get_subtable(2, 1);
    subtable->insert_int(0, 0, 22);
    subtable->insert_string(1, 0, "b");
    subtable->insert_done();
    subtable->insert_int(0, 1, 33);
    subtable->insert_string(1, 1, "c");
    subtable->insert_done();

    subtable = table->get_subtable(2, 2);
    subtable->insert_int(0, 0, 44);
    subtable->insert_string(1, 0, "d");
    subtable->insert_done();

    subtable = table->get_subtable(2, 3);
    subtable->insert_int(0, 0, 55);
    subtable->insert_string(1, 0, "e");
    subtable->insert_done();

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
    TupleTableType t;
    t.add(1, "a");
    t.add(2, "a");
    t.add(3, "c");

    TupleTableType::View v = t.where().first.greater(1).find_all();

    TupleTableType::Query q1 = t.where(&v);
    CHECK_EQUAL(2, q1.count());

    TupleTableType::Query q3 = t.where(&v).second.equal("a");
    CHECK_EQUAL(1, q3.count());

    TupleTableType::Query q4 = t.where(&v).first.between(3, 6);
    CHECK_EQUAL(1, q4.count());
}

TEST(Query_SumMinMaxAvg)
{
    DateIntStringFloatDouble t;

    t.add(1, "a", DateTime(100), 1.0f, 1.0);
    t.add(1, "a", DateTime(100), 1.0f, 1.0);
    t.add(1, "a", DateTime(100), 1.0f, 1.0);
    t.add(1, "a", DateTime(100), 1.0f, 1.0);
    t.add(2, "b", DateTime(300), 3.0f, 3.0);
    t.add(3, "c", DateTime(50), 5.0f, 5.0);
    t.add(0, "a", DateTime(100), 1.0f, 1.0);
    t.add(0, "b", DateTime(3000), 30.0f, 30.0);
    t.add(0, "c", DateTime(5), 0.5f, 0.5);

    CHECK_EQUAL(9, t.where().first.sum());

    CHECK_EQUAL(0, t.where().first.minimum());
    CHECK_EQUAL(3, t.where().first.maximum());

    size_t resindex = not_found;

    t.where().first.maximum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(5, resindex);

    t.where().first.minimum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(6, resindex);

    t.where().third.maximum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(7, resindex);

    t.where().third.minimum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(8, resindex);

    t.where().fourth.maximum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(7, resindex);

    t.where().fourth.minimum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(8, resindex);

    t.where().fifth.maximum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(7, resindex);

    t.where().fifth.minimum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(8, resindex);

    // Now with condition (tests another code path in Array::minmax())
    t.where().first.not_equal(0).fifth.minimum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(0, resindex);

    t.where().first.not_equal(0).fourth.minimum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(0, resindex);

    t.where().first.not_equal(0).third.minimum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(5, resindex);

    t.where().first.not_equal(0).third.maximum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(4, resindex);

    CHECK_APPROXIMATELY_EQUAL(1, t.where().first.average(), 0.001);

    CHECK_EQUAL(DateTime(3000), t.where().third.maximum());
    CHECK_EQUAL(DateTime(5), t.where().third.minimum());

    size_t cnt;
    CHECK_EQUAL(0, t.where().first.sum(&cnt, 0, 0));
    CHECK_EQUAL(0, cnt);
    CHECK_EQUAL(0, t.where().first.sum(&cnt, 1, 1));
    CHECK_EQUAL(0, cnt);
    CHECK_EQUAL(0, t.where().first.sum(&cnt, 2, 2));
    CHECK_EQUAL(0, cnt);

    CHECK_EQUAL(1, t.where().first.sum(&cnt, 0, 1));
    CHECK_EQUAL(1, cnt);
    CHECK_EQUAL(2, t.where().first.sum(&cnt, 4, 5));
    CHECK_EQUAL(1, cnt);
    CHECK_EQUAL(3, t.where().first.sum(&cnt, 5, 6));
    CHECK_EQUAL(1, cnt);

    CHECK_EQUAL(2, t.where().first.sum(&cnt, 0, 2));
    CHECK_EQUAL(2, cnt);
    CHECK_EQUAL(5, t.where().first.sum(&cnt, 1, 5));
    CHECK_EQUAL(4, cnt);

    CHECK_EQUAL(3, t.where().first.sum(&cnt, 0, 3));
    CHECK_EQUAL(3, cnt);
    CHECK_EQUAL(9, t.where().first.sum(&cnt, 0, size_t(-1)));
    CHECK_EQUAL(9, cnt);
}

TEST(Query_SumMinMaxAvg_where)
{
    DateIntStringFloatDouble t;

    t.add(1, "a", DateTime(100), 1.0f, 1.0);
    t.add(1, "a", DateTime(100), 1.0f, 1.0);
    t.add(1, "a", DateTime(100), 1.0f, 1.0);
    t.add(1, "a", DateTime(100), 1.0f, 1.0);
    t.add(2, "b", DateTime(300), 3.0f, 3.0);
    t.add(3, "c", DateTime(50), 5.0f, 5.0);
    t.add(0, "a", DateTime(100), 1.0f, 1.0);
    t.add(0, "b", DateTime(3000), 30.0f, 30.0);
    t.add(0, "c", DateTime(5), 0.5f, 0.5);

    DateIntStringFloatDouble::View v = t.where().find_all();

    CHECK_EQUAL(9, t.where(&v).first.sum());

    CHECK_EQUAL(0, t.where(&v).first.minimum());
    CHECK_EQUAL(3, t.where(&v).first.maximum());

    size_t resindex = not_found;

    t.where(&v).first.maximum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(5, resindex);

    t.where(&v).first.minimum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(6, resindex);

    t.where(&v).third.maximum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(7, resindex);

    t.where(&v).third.minimum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(8, resindex);

    t.where(&v).fourth.maximum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(7, resindex);

    t.where(&v).fourth.minimum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(8, resindex);

    t.where(&v).fifth.maximum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(7, resindex);

    t.where(&v).fifth.minimum(nullptr, 0, -1, -1, &resindex);
    CHECK_EQUAL(8, resindex);

    CHECK_APPROXIMATELY_EQUAL(1, t.where(&v).first.average(), 0.001);

    CHECK_EQUAL(DateTime(3000), t.where(&v).third.maximum());
    CHECK_EQUAL(DateTime(5), t.where(&v).third.minimum());

    size_t cnt;
    CHECK_EQUAL(0, t.where(&v).first.sum(&cnt, 0, 0));
    CHECK_EQUAL(0, cnt);
    CHECK_EQUAL(0, t.where(&v).first.sum(&cnt, 1, 1));
    CHECK_EQUAL(0, cnt);
    CHECK_EQUAL(0, t.where(&v).first.sum(&cnt, 2, 2));
    CHECK_EQUAL(0, cnt);

    CHECK_EQUAL(1, t.where(&v).first.sum(&cnt, 0, 1));
    CHECK_EQUAL(1, cnt);
    CHECK_EQUAL(2, t.where(&v).first.sum(&cnt, 4, 5));
    CHECK_EQUAL(1, cnt);
    CHECK_EQUAL(3, t.where(&v).first.sum(&cnt, 5, 6));
    CHECK_EQUAL(1, cnt);

    CHECK_EQUAL(2, t.where(&v).first.sum(&cnt, 0, 2));
    CHECK_EQUAL(2, cnt);
    CHECK_EQUAL(5, t.where(&v).first.sum(&cnt, 1, 5));
    CHECK_EQUAL(4, cnt);

    CHECK_EQUAL(3, t.where(&v).first.sum(&cnt, 0, 3));
    CHECK_EQUAL(3, cnt);
    CHECK_EQUAL(9, t.where(&v).first.sum(&cnt, 0, size_t(-1)));
    CHECK_EQUAL(9, cnt);



}

TEST(Query_Avg)
{
    TupleTableType t;
    size_t cnt;
    t.add(10, "a");
    CHECK_EQUAL(10, t.where().first.average());
    t.add(30, "b");
    CHECK_EQUAL(20, t.where().first.average());

    CHECK_EQUAL(0, t.where().first.average(NULL, 0, 0));     // none
    CHECK_EQUAL(0, t.where().first.average(NULL, 1, 1));     // none
    CHECK_EQUAL(20, t.where().first.average(NULL, 0, 2));     // both
    CHECK_EQUAL(20, t.where().first.average(NULL, 0, -1));     // both

    CHECK_EQUAL(10, t.where().first.average(&cnt, 0, 1));     // first

    CHECK_EQUAL(30, t.where().first.sum(NULL, 1, 2));     // second
    CHECK_EQUAL(30, t.where().first.average(NULL, 1, 2));     // second
}

TEST(Query_Avg2)
{
    TupleTableType t;
    size_t cnt;

    t.add(10, "a");
    t.add(100, "b");
    t.add(20, "a");
    t.add(100, "b");
    t.add(100, "b");
    t.add(30, "a");
    TupleTableType::Query q = t.where().second.equal("a");
    CHECK_EQUAL(3, q.count());
    q.first.sum();

    CHECK_EQUAL(60, t.where().second.equal("a").first.sum());

    CHECK_EQUAL(0, t.where().second.equal("a").first.average(&cnt, 0, 0));
    CHECK_EQUAL(0, t.where().second.equal("a").first.average(&cnt, 1, 1));
    CHECK_EQUAL(0, t.where().second.equal("a").first.average(&cnt, 2, 2));
    CHECK_EQUAL(0, cnt);

    CHECK_EQUAL(10, t.where().second.equal("a").first.average(&cnt, 0, 1));
    CHECK_EQUAL(20, t.where().second.equal("a").first.average(&cnt, 1, 5));
    CHECK_EQUAL(30, t.where().second.equal("a").first.average(&cnt, 5, 6));
    CHECK_EQUAL(1, cnt);

    CHECK_EQUAL(15, t.where().second.equal("a").first.average(&cnt, 0, 3));
    CHECK_EQUAL(20, t.where().second.equal("a").first.average(&cnt, 2, 5));
    CHECK_EQUAL(1, cnt);

    CHECK_EQUAL(20, t.where().second.equal("a").first.average(&cnt));
    CHECK_EQUAL(3, cnt);
    CHECK_EQUAL(15, t.where().second.equal("a").first.average(&cnt, 0, 3));
    CHECK_EQUAL(2, cnt);
    CHECK_EQUAL(20, t.where().second.equal("a").first.average(&cnt, 0, size_t(-1)));
    CHECK_EQUAL(3, cnt);
}


TEST(Query_OfByOne)
{
    TupleTableType t;
    for (size_t i = 0; i < REALM_MAX_BPNODE_SIZE * 2; ++i) {
        t.add(1, "a");
    }

    // Top
    t[0].first = 0;
    size_t res = t.where().first.equal(0).find();
    CHECK_EQUAL(0, res);
    t[0].first = 1; // reset

    // Before split
    t[REALM_MAX_BPNODE_SIZE - 1].first = 0;
    res = t.where().first.equal(0).find();
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE - 1, res);
    t[REALM_MAX_BPNODE_SIZE - 1].first = 1; // reset

    // After split
    t[REALM_MAX_BPNODE_SIZE].first = 0;
    res = t.where().first.equal(0).find();
    CHECK_EQUAL(REALM_MAX_BPNODE_SIZE, res);
    t[REALM_MAX_BPNODE_SIZE].first = 1; // reset

    // Before end
    const size_t last_pos = (REALM_MAX_BPNODE_SIZE * 2) - 1;
    t[last_pos].first = 0;
    res = t.where().first.equal(0).find();
    CHECK_EQUAL(last_pos, res);
}

TEST(Query_Const)
{
    TupleTableType t;
    t.add(10, "a");
    t.add(100, "b");
    t.add(20, "a");

    const TupleTableType& const_table = t;

    const size_t count = const_table.where().second.equal("a").count();
    CHECK_EQUAL(2, count);

    //TODO: Should not be possible
    const_table.where().second.equal("a").remove();
}

namespace {

    REALM_TABLE_2(PhoneTable,
        type, String,
        number, String)

        REALM_TABLE_4(EmployeeTable,
        name, String,
        age, Int,
        hired, Bool,
        phones, Subtable<PhoneTable>)

} // anonymous namespace

TEST(Query_SubtablesTyped)
{
    // Create table
    EmployeeTable employees;

    // Add initial rows
    employees.add("joe", 42, false, NULL);
    employees[0].phones->add("home", "324-323-3214");
    employees[0].phones->add("work", "321-564-8678");

    employees.add("jessica", 22, true, NULL);
    employees[1].phones->add("mobile", "434-426-4646");
    employees[1].phones->add("school", "345-543-5345");

    // Do a query
    EmployeeTable::Query q = employees.where().hired.equal(true);
    EmployeeTable::View view = q.find_all();

    // Verify result
    CHECK(view.size() == 1 && view[0].name == "jessica");
}


TEST(Query_AllTypesDynamicallyTyped)
{
    Table table;
    DescriptorRef sub1;
    table.add_column(type_Bool, "boo");
    table.add_column(type_Int, "int");
    table.add_column(type_Float, "flt");
    table.add_column(type_Double, "dbl");
    table.add_column(type_String, "str");
    table.add_column(type_Binary, "bin");
    table.add_column(type_DateTime, "dat");
    table.add_column(type_Table, "tab", &sub1);
    table.add_column(type_Mixed, "mix");
    sub1->add_column(type_Int, "sub_int");
    sub1.reset();

    const char bin[4] = { 0, 1, 2, 3 };
    BinaryData bin1(bin, sizeof bin / 2);
    BinaryData bin2(bin, sizeof bin);
    time_t time_now = time(0);
    Mixed mix_int(int64_t(1));
    Mixed mix_subtab((Mixed::subtable_tag()));

    table.add_empty_row();
    table.set_bool(0, 0, false);
    table.set_int(1, 0, 54);
    table.set_float(2, 0, 0.7f);
    table.set_double(3, 0, 0.8);
    table.set_string(4, 0, "foo");
    table.set_binary(5, 0, bin1);
    table.set_datetime(6, 0, 0);
    table.set_mixed(8, 0, mix_int);

    table.add_empty_row();
    table.set_bool(0, 1, true);
    table.set_int(1, 1, 506);
    table.set_float(2, 1, 7.7f);
    table.set_double(3, 1, 8.8);
    table.set_string(4, 1, "banach");
    table.set_binary(5, 1, bin2);
    table.set_datetime(6, 1, time_now);
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
    CHECK_EQUAL(1, table.where().equal_datetime(6, 0).count());
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

namespace {
    REALM_TABLE_1(TestQuerySub,
        age, Int)

        REALM_TABLE_9(TestQueryAllTypes,
        bool_col, Bool,
        int_col, Int,
        float_col, Float,
        double_col, Double,
        string_col, String,
        binary_col, Binary,
        date_col, DateTime,
        table_col, Subtable<TestQuerySub>,
        mixed_col, Mixed)
}

TEST(Query_AllTypesStaticallyTyped)
{
    TestQueryAllTypes table;

    const char bin[4] = { 0, 1, 2, 3 };
    BinaryData bin1(bin, sizeof bin / 2);
    BinaryData bin2(bin, sizeof bin);
    time_t time_now = time(0);
    TestQuerySub subtab;
    subtab.add(100);
    Mixed mix_int(int64_t(1));
    Mixed mix_subtab((Mixed::subtable_tag()));

    table.add(false, 54, 0.7f, 0.8, "foo", bin1, 0, 0, mix_int);
    table.add(true, 506, 7.7f, 8.8, "banach", bin2, time_now, &subtab, mix_subtab);

    CHECK_EQUAL(1, table.where().bool_col.equal(false).count());
    CHECK_EQUAL(1, table.where().int_col.equal(54).count());
    CHECK_EQUAL(1, table.where().float_col.equal(0.7f).count());
    CHECK_EQUAL(1, table.where().double_col.equal(0.8).count());
    CHECK_EQUAL(1, table.where().string_col.equal("foo").count());
    CHECK_EQUAL(1, table.where().binary_col.equal(bin1).count());
    CHECK_EQUAL(1, table.where().date_col.equal(0).count());
    //    CHECK_EQUAL(1, table.where().table_col.equal(subtab).count());
    //    CHECK_EQUAL(1, table.where().mixed_col.equal(mix_int).count());
    // FIXME: It's not possible to construct a subtable query. .table_col.subtable() does not return an object with 'age':
    //    CHECK_EQUAL(1, table.where().table_col.subtable().age.end_subtable().count());

    TestQueryAllTypes::Query query = table.where().bool_col.equal(false);

    size_t ndx = not_found;

    CHECK_EQUAL(54, query.int_col.minimum());
    query.int_col.minimum(nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(54, query.int_col.maximum());
    query.int_col.maximum(nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(54, query.int_col.sum());
    CHECK_EQUAL(54, query.int_col.average());

    CHECK_EQUAL(0.7f, query.float_col.minimum());
    query.float_col.minimum(nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(0.7f, query.float_col.maximum());
    query.float_col.maximum(nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(0.7f, query.float_col.sum());
    CHECK_EQUAL(0.7f, query.float_col.average());

    CHECK_EQUAL(0.8, query.double_col.minimum());
    query.double_col.minimum(nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(0.8, query.double_col.maximum());
    query.double_col.maximum(nullptr, 0, not_found, not_found, &ndx);
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(0.8, query.double_col.sum());
    CHECK_EQUAL(0.8, query.double_col.average());
}


TEST(Query_RefCounting)
{
    Table* t = LangBindHelper::new_table();
    t->add_column(type_Int, "myint");
    t->insert_int(0, 0, 12);
    t->insert_done();

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

    Types t;

    t.add(1, "1", 1.1);
    t.add(2, "2", 2.2);
    t.add(3, "3", 3.3);
    t.add(4, "4", 4.4);

    Query q = t.column().ints > 2 + 0; // + 0 makes query_expression node instead of query_engine.


    // Test if we can execute a copy
    Query q2 = Query(q, Query::TCopyExpressionTag());

    CHECK_EQUAL(2, q2.find());


    // See if we can execute a copy of a delted query (copy should not contain references to original)
    Query* q3 = new Query(q, Query::TCopyExpressionTag());
    Query* q4 = new Query(*q3, Query::TCopyExpressionTag());
    delete q3;


    // Attempt to overwrite memory of the deleted q3 by allocating various sized objects so that a spurious execution
    // of methods on q3 can be detected (by making unit test crash).
    char* tmp[1000];
    for (size_t t = 0; t < sizeof(tmp) / sizeof(tmp[0]); t++) {
        tmp[t] = new char[t];
        memset(tmp[t], 0, t);
    }
    for (size_t t = 0; t < sizeof(tmp) / sizeof(tmp[0]); t++) {
        delete[] tmp[t];
    }

    CHECK_EQUAL(2, q4->find());
    delete q4;


    // See if we can append a criteria to a query
    Query q5 = t.column().ints > 2 + 0; // + 0 makes query_expression node instead of query_engine
    q5.greater(2, 4.0);
    CHECK_EQUAL(3, q5.find());


    // See if we can append a criteria to a copy without modifying the original (copy should not contain references
    // to original). Tests query_expression integer node.
    Query q6 = t.column().ints > 2 + 0; // + 0 makes query_expression node instead of query_engine
    Query q7 = Query(q6, Query::TCopyExpressionTag());

    q7.greater(2, 4.0);
    CHECK_EQUAL(3, q7.find());
    CHECK_EQUAL(2, q6.find());


    // See if we can append a criteria to a copy without modifying the original (copy should not contain references
    // to original). Tests query_engine integer node.
    Query q8 = t.column().ints > 2;
    Query q9 = Query(q8, Query::TCopyExpressionTag());

    q9.greater(2, 4.0);
    CHECK_EQUAL(3, q9.find());
    CHECK_EQUAL(2, q8.find());


    // See if we can append a criteria to a copy without modifying the original (copy should not contain references
    // to original). Tests query_engine string node.
    Query q10 = t.column().strings != "2";
    Query q11 = Query(q10, Query::TCopyExpressionTag());

    q11.greater(2, 4.0);
    CHECK_EQUAL(3, q11.find());
    CHECK_EQUAL(0, q10.find());

    // Test and_query() on a copy
    Query q12 = t.column().ints > 2;
    Query q13 = Query(q12, Query::TCopyExpressionTag());

    q13.and_query(t.column().strings != "3");
    CHECK_EQUAL(3, q13.find());
    CHECK_EQUAL(2, q12.find());
}

TEST(Query_TableViewMoveAssign1)
{
    Types t;

    t.add(1, "1", 1.1);
    t.add(2, "2", 2.2);
    t.add(3, "3", 3.3);
    t.add(4, "4", 4.4);

    // temporary query is created, then q makes and stores a deep copy and then temporary is destructed
    Query q = t.column().ints > 2 + 0; // + 0 makes query_expression node instead of query_engine

    // now deep copy should be destructed and replaced by new temporary
    TableView tv = q.find_all();

    // the original should still work; destruction of temporaries and deep copies should have no references
    // to original
    tv = q.find_all();
}

TEST(Query_TableViewMoveAssignLeak2)
{
    Types t;

    t.add(1, "1", 1.1);
    t.add(2, "2", 2.2);
    t.add(3, "3", 3.3);
    t.add(4, "4", 4.4);

    Query q = t.column().ints < t.column().doubles && t.column().strings == "4";
    TableView tv = q.find_all();

    // Upon each find_all() call, tv copies the query 'q' into itself. See if this copying works
    tv = q.find_all();
    tv = q.find_all();
    tv = q.find_all();
    tv = q.find_all();
    tv = q.find_all();

    tv.sort(0, true);

    tv = q.find_all();

    Query q2 = t.column().ints <= t.column().doubles;
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

    q2 = t.column().ints <= t.column().doubles;
    q3 = q2;

    q3.find();
    q2.find();
}



TEST(Query_DeepCopyLeak1)
{
    // NOTE: You can only create a copy of a fully constructed; i.e. you cannot copy a query which is missing an
    // end_group(). Run Query::validate() to see if it's fully constructed.

    Types t;

    t.add(1, "1", 1.1);
    t.add(2, "2", 2.2);
    t.add(3, "3", 3.3);
    t.add(4, "4", 4.4);

    // See if copying of a mix of query_expression and query_engine nodes will leak
    Query q = !(t.column().ints > 2 + 0 && t.column().ints > 2 && t.column().doubles > 2.2) || t.column().ints == 4 || t.column().ints == 4 + 0;
    Query q2 = Query(q, Query::TCopyExpressionTag());
    Query q3 = Query(q2, Query::TCopyExpressionTag());
}

TEST(Query_DeepCopyTest)
{
    // If Query::first vector was relocated because of push_back, then Query would crash, because referenced 
    // pointers were pointing into it.
    Table table;
    table.add_column(type_Int, "first");

    Query q1 = table.where();

    Query q2(q1, Query::TCopyExpressionTag());

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
    Query(q, Query::TCopyExpressionTag());
}


TEST(Query_NullStrings)
{
    Table table;
    table.add_column(type_String, "s", true);
    table.add_empty_row(3);

    Query q;
    TableView v;

    // Short strings
    table.set_string(0, 0, "Albertslund");      // Normal non-empty string
    table.set_string(0, 1, realm::null());    // NULL string
    table.set_string(0, 2, "");                 // Empty string

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

#if REALM_NULL_STRINGS == 1
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
                    char buf[] = "This string is around 90 bytes long, which falls in the long-string type of Realm strings";
                    char* buf1 = static_cast<char*>(malloc(sizeof(buf)));
                    memcpy(buf1, buf, sizeof(buf));
                    char buf2[] = "                                                                                         ";

                    StringData sd;
                    std::string st;

                    if (fastrand(1) == 0) {
                        // null string
                        sd = realm::null();
                        st = "null";
                    }
                    else {
                        // non-null string
                        size_t len = fastrand(3);
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
                            for (size_t t = 0; t < len; t++) {
                                if (fastrand(100) > 20)
                                    buf2[t] = 0;                        // zero byte
                                else
                                    buf2[t] = static_cast<char>(fastrand(255));  // random byte
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
                for (size_t i = 0; i < table.size(); i++) {
                    if (v[i] == "null") {
                        CHECK(table.get_string(0, i).is_null());
                    }
                    else {
                        CHECK(table.get_string(0, i) == v[i]);
                    }
                }

            }

        }
    }
}
#endif

#endif // TEST_QUERY
