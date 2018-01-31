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

#if defined(TEST_PARSER)

#include "test.hpp"


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

#include <realm/descriptor.hpp>
#include <realm/history.hpp>
#include <realm/lang_bind_helper.hpp>
#include <realm/parser/parser.hpp>
#include <realm/parser/query_builder.hpp>
#include <realm/query_expression.hpp>
#include <realm/replication.hpp>
#include <realm/util/any.hpp>
#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/to_string.hpp>

#include <chrono>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace realm;
using namespace realm::metrics;
using namespace realm::test_util;
using namespace realm::util;

static std::vector<std::string> valid_queries = {
    // true/false predicates
    "truepredicate",
    "falsepredicate",
    " TRUEPREDICATE ",
    " FALSEPREDICATE ",
    "truepredicates = falsepredicates", // keypaths

    // characters/strings
    "\"\" = ''",
    "'azAZ09/ :()[]{}<>,.^@-+=*&~`' = '\\\" \\' \\\\ \\/ \\b \\f \\n \\r \\t \\0'",
    "\"azAZ09/\" = \"\\\" \\' \\\\ \\/ \\b \\f \\n \\r \\t \\0\"",
    "'\\uffFf' = '\\u0020'",
    "'\\u01111' = 'asdf\\u0111asdf'",

    // expressions (numbers, bools, keypaths, arguments)
    "-1 = 12",
    "0 = 001",
    "0x0 = -0X398235fcAb",
    "10. = -.034",
    "10.0 = 5.034",
    "true = false",
    "truelove = false",
    "true = falsey",
    "nullified = null",
    "_ = a",
    "_a = _.aZ",
    "a09._br.z = __-__.Z-9",
    "$0 = $19",
    "$0=$0",
    // properties can contain '$'
    "a$a = a",
    "$-1 = $0",
    "$a = $0",
    "$ = $",

    // operators
    "0=0",
    "0 = 0",
    "0 =[c] 0",
    "0!=0",
    "0 != 0",
    "0 !=[c] 0",
    "0!=[c]0",
    "0 <> 0",
    "0<>0",
    "0 <>[c] 0",
    "0<>[c]0",
    "0==0",
    "0 == 0",
    "0==[c]0",
    "0 == [c] 0",
    "0>0",
    "0 > 0",
    "0>=0",
    "0 >= 0",
    "0 => 0",
    "0=>0",
    "0<0",
    "0 < 0",
    "0<=0",
    "0 <= 0",
    "0 =< 0",
    "0<=0",
    "0 contains 0",
    "a CONTAINS[c] b",
    "a contains [c] b",
    "'a'CONTAINS[c]b",
    "0 BeGiNsWiTh 0",
    "0 ENDSWITH 0",
    "contains contains 'contains'",
    "beginswith beginswith 'beginswith'",
    "endswith endswith 'endswith'",
    "NOT NOT != 'NOT'",
    "AND == 'AND' AND OR == 'OR'",
    // FIXME - bug
    // "truepredicate == 'falsepredicate' && truepredicate",

    // atoms/groups
    "(0=0)",
    "( 0=0 )",
    "((0=0))",
    "!0=0",
    "! 0=0",
    "!(0=0)",
    "! (0=0)",
    "NOT0=0",   // keypath NOT0
    "NOT0.a=0", // keypath NOT0
    "NOT0a.b=0", // keypath NOT0a
    "not-1=1",
    "not 0=0",
    "NOT(0=0)",
    "not (0=0)",
    "NOT (!0=0)",

    // compound
    "a==a && a==a",
    "a==a || a==a",
    "a==a&&a==a||a=a",
    "a==a and a==a",
    "a==a OR a==a",
    "and=='AND'&&'or'=='||'",
    "and == or && ORE > GRAND",
    "a=1AND NOTb=2",

    // sort/distinct
    "a=b SORT(p ASCENDING)",
    "a=b SORT(p asc)",
    "a=b SORT(p Descending)",
    "a=b sort (p.q desc)",
    "a=b distinct(p)",
    "a=b DISTINCT(P)",
    "a=b DISTINCT(p)",
    "a == b sort(a ASC, b DESC)",
    "a == b sort(a ASC, b DESC) sort(c ASC)",
    "a=b DISTINCT(p) DISTINCT(q)",
    "a=b DISTINCT(p, q, r) DISTINCT(q)",
    "a == b sort(a ASC, b DESC) DISTINCT(p)",
    "a == b sort(a ASC, b DESC) DISTINCT(p) sort(c ASC, d DESC) DISTINCT(q.r)",
    "a == b and c==d sort(a ASC, b DESC) DISTINCT(p) sort(c ASC, d DESC) DISTINCT(q.r)",
    "a == b  sort(     a   ASC  ,  b DESC) and c==d   DISTINCT(   p )  sort(   c   ASC  ,  d   DESC  )  DISTINCT(   q.r ,   p)   ",

    // subquery expression
    "SUBQUERY(items, $x, $x.name == 'Tom').@size > 0",
    "SUBQUERY(items, $x, $x.name == 'Tom').@count > 0",
    "SUBQUERY(items, $x, $x.allergens.@min.population_affected < 0.10).@count > 0",
    "SUBQUERY(items, $x, $x.name == 'Tom').@count == SUBQUERY(items, $x, $x.price < 10).@count",
};

static std::vector<std::string> invalid_queries = {
    "predicate",
    "'\\a' = ''",        // invalid escape

    // invalid unicode
    "'\\u0' = ''",

    // invalid strings
    "\"' = ''",
    "\" = ''",
    "' = ''",

    // expressions
    "03a = 1",
    "1..0 = 1",
    "1.0. = 1",
    "1-0 = 1",
    "0x = 1",
    "- = a",
    "a..b = a",
    "{} = $0",

    // operators
    "0===>0",
    "0 contains1",
    "a contains_something",
    "endswith 0",

    // atoms/groups
    "0=0)",
    "(0=0",
    "(0=0))",
    "! =0",
    "NOTNOT(0=0)",
    "not.a=0",
    "(!!0=0)",
    "0=0 !",

    // compound
    "a==a & a==a",
    "a==a | a==a",
    "a==a &| a==a",
    "a==a && OR a==a",
    "a==aORa==a",
    "a==a ORa==a",
    "a==a AND==a",
    "a==a ANDa==a",
    "a=1ANDNOT b=2",

    "truepredicate &&",
    "truepredicate & truepredicate",

    // sort/distinct
    "SORT(p ASCENDING)",  // no query conditions
    "a=b SORT(p)",        // no asc/desc
    "a=b SORT(0 Descending)", // bad keypath
    "a=b sort()",             // missing condition
    "a=b sort",      // no target property
    "distinct(p)",           // no query condition
    "a=b DISTINCT()",      // no target property
    "a=b Distinct",      // no target property
    "sort(a ASC b, DESC) a == b", // before query condition
    "sort(a ASC b, DESC) a == b sort(c ASC)", // before query condition
    "a=bDISTINCT(p)", // bad spacing
    "a=b sort p.q desc", // no braces
    "a=b sort(p.qDESC)", // bad spacing
    "a=b DISTINCT p", // no braces
    "a=b SORT(p ASC", // bad braces
    "a=b DISTINCT(p", // no braces
    "a=b sort(p.q DESC a ASC)", // missing comma
    "a=b DISTINCT(p q)", // missing comma

    // subquery
    "SUBQUERY(items, $x, $x.name == 'Tom') > 0", // missing .@count
    "SUBQUERY(items, $x, $x.name == 'Tom').@min > 0", // @min not yet supported
    "SUBQUERY(items, $x, $x.name == 'Tom').@max > 0", // @max not yet supported
    "SUBQUERY(items, $x, $x.name == 'Tom').@sum > 0", // @sum not yet supported
    "SUBQUERY(items, $x, $x.name == 'Tom').@avg > 0", // @avg not yet supported
    "SUBQUERY(items, var, var.name == 'Tom').@avg > 0", // variable must start with '$'
    "SUBQUERY(, $x, $x.name == 'Tom').@avg > 0", // a target keypath is required
    "SUBQUERY(items, , name == 'Tom').@avg > 0", // a variable name is required
    "SUBQUERY(items, $x, ).@avg > 0", // the subquery is required
};

TEST(Parser_valid_queries) {
    for (auto& query : valid_queries) {
        //std::cout << "query: " << query << std::endl;
        realm::parser::parse(query);
    }
}

TEST(Parser_invalid_queries) {
    for (auto& query : invalid_queries) {
        //std::cout << "query: " << query << std::endl;
        CHECK_THROW_ANY(realm::parser::parse(query));
    }
}

TEST(Parser_grammar_analysis)
{
    CHECK(realm::parser::analyze_grammar() == 0);
}

Query verify_query(test_util::unit_test::TestContext& test_context, TableRef t, std::string query_string, size_t num_results) {
    Query q = t->where();

    realm::parser::Predicate p = realm::parser::parse(query_string).predicate;
    realm::query_builder::apply_predicate(q, p);

    CHECK_EQUAL(q.count(), num_results);
    std::string description = q.get_description();
    //std::cerr << "original: " << query_string << "\tdescribed: " << description << "\n";
    Query q2 = t->where();

    realm::parser::Predicate p2 = realm::parser::parse(description).predicate;
    realm::query_builder::apply_predicate(q2, p2);

    CHECK_EQUAL(q2.count(), num_results);
    return q2;
}


TEST(Parser_empty_input)
{
    Group g;
    std::string table_name = "table";
    TableRef t = g.add_table(table_name);
    t->add_column(type_Int, "int_col");
    t->add_empty_row(5);

    // an empty query string is an invalid predicate
    CHECK_THROW_ANY(verify_query(test_context, t, "", 5));

    Query q = t->where(); // empty query
    std::string empty_description = q.get_description();
    CHECK(!empty_description.empty());
    CHECK_EQUAL(0, empty_description.compare("TRUEPREDICATE"));
    realm::parser::Predicate p = realm::parser::parse(empty_description).predicate;
    realm::query_builder::apply_predicate(q, p);
    CHECK_EQUAL(q.count(), 5);

    verify_query(test_context, t, "TRUEPREDICATE", 5);
    verify_query(test_context, t, "!TRUEPREDICATE", 0);

    verify_query(test_context, t, "FALSEPREDICATE", 0);
    verify_query(test_context, t, "!FALSEPREDICATE", 5);
}


TEST(Parser_ConstrainedQuery)
{
    Group g;
    std::string table_name = "table";
    TableRef t = g.add_table(table_name);
    size_t int_col = t->add_column(type_Int, "age");
    size_t list_col = t->add_column_link(type_LinkList, "self_list", *t);
    t->add_empty_row(2);
    t->set_int(int_col, 1, 1);

    LinkViewRef list_0 = t->get_linklist(list_col, 0);
    list_0->add(0);
    list_0->add(1);

    TableView tv = t->get_backlink_view(0, t.get(), list_col);
    Query q(const_cast<const Table&>(*t), &tv);
    CHECK_EQUAL(q.count(), 1);
    q.and_query(t->column<Int>(int_col) <= 0);
    CHECK_EQUAL(q.count(), 1);
    CHECK_THROW(q.get_description(), SerialisationError);

    Query q2(const_cast<const Table&>(*t), list_0);
    CHECK_EQUAL(q2.count(), 2);
    q2.and_query(t->column<Int>(int_col) <= 0);
    CHECK_EQUAL(q2.count(), 1);
    CHECK_THROW(q2.get_description(), SerialisationError);
}


TEST(Parser_basic_serialisation)
{
    Group g;
    std::string table_name = "person";
    TableRef t = g.add_table(table_name);
    size_t int_col_ndx = t->add_column(type_Int, "age");
    size_t str_col_ndx = t->add_column(type_String, "name");
    size_t double_col_ndx = t->add_column(type_Double, "fees");
    size_t link_col_ndx = t->add_column_link(type_Link, "buddy", *t);
    size_t time_col_ndx = t->add_column(type_Timestamp, "time", true);
    t->add_empty_row(5);
    std::vector<std::string> names = {"Billy", "Bob", "Joe", "Jane", "Joel"};
    std::vector<double> fees = { 2.0, 2.23, 2.22, 2.25, 3.73 };

    for (size_t i = 0; i < t->size(); ++i) {
        t->set_int(int_col_ndx, i, i);
        t->set_string(str_col_ndx, i, names[i]);
        t->set_double(double_col_ndx, i, fees[i]);
    }
    t->set_timestamp(time_col_ndx, 0, Timestamp(realm::null()));
    t->set_timestamp(time_col_ndx, 1, Timestamp(1512130073, 0)); // 2017/12/02 @ 12:47am (UTC)
    t->set_timestamp(time_col_ndx, 2, Timestamp(1512130073, 505)); // with nanoseconds
    t->set_timestamp(time_col_ndx, 3, Timestamp(1, 2));
    t->set_timestamp(time_col_ndx, 4, Timestamp(0, 0));
    t->set_link(link_col_ndx, 0, 1);

    Query q = t->where();

    verify_query(test_context, t, "time == NULL", 1);
    verify_query(test_context, t, "time != NULL", 4);
    verify_query(test_context, t, "time > T0:0", 3);
    verify_query(test_context, t, "time == T1:2", 1);
    verify_query(test_context, t, "time > 2017-12-1@12:07:53", 1);
    verify_query(test_context, t, "time == 2017-12-01@12:07:53:505", 1);
    verify_query(test_context, t, "buddy == NULL", 4);
    verify_query(test_context, t, "buddy != NULL", 1);
    verify_query(test_context, t, "buddy <> NULL", 1);
    verify_query(test_context, t, "age > 2", 2);
    verify_query(test_context, t, "!(age >= 2)", 2);
    verify_query(test_context, t, "!(age => 2)", 2);
    verify_query(test_context, t, "3 <= age", 2);
    verify_query(test_context, t, "3 =< age", 2);
    verify_query(test_context, t, "age > 2 and age < 4", 1);
    verify_query(test_context, t, "age = 1 || age == 3", 2);
    verify_query(test_context, t, "fees != 2.22 && fees > 2.2", 3);
    verify_query(test_context, t, "(age > 1 || fees >= 2.25) && age == 4", 1);
    verify_query(test_context, t, "name = \"Joe\"", 1);
    verify_query(test_context, t, "buddy.age > 0", 1);
    verify_query(test_context, t, "name BEGINSWITH \"J\"", 3);
    verify_query(test_context, t, "name ENDSWITH \"E\"", 0);
    verify_query(test_context, t, "name ENDSWITH[c] \"E\"", 2);
    verify_query(test_context, t, "name CONTAINS \"OE\"", 0);
    verify_query(test_context, t, "name CONTAINS[c] \"OE\"", 2);
    verify_query(test_context, t, "name LIKE \"b*\"", 0);
    verify_query(test_context, t, "name LIKE[c] \"b*\"", 2);
    verify_query(test_context, t, "TRUEPREDICATE", 5);
    verify_query(test_context, t, "FALSEPREDICATE", 0);
    verify_query(test_context, t, "age > 2 and TRUEPREDICATE", 2);
    verify_query(test_context, t, "age > 2 && FALSEPREDICATE", 0);
    verify_query(test_context, t, "age > 2 or TRUEPREDICATE", 5);
    verify_query(test_context, t, "age > 2 || FALSEPREDICATE", 2);
    verify_query(test_context, t, "age > 2 AND !FALSEPREDICATE", 2);
    verify_query(test_context, t, "age > 2 AND !TRUEPREDICATE", 0);

    CHECK_THROW_ANY(verify_query(test_context, t, "buddy.age > $0", 0)); // no external parameters provided

    std::string message;
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "missing_property > 2", 0), message);
    CHECK(message.find(table_name) != std::string::npos); // no prefix modification for names without "class_"
    CHECK(message.find("missing_property") != std::string::npos);
}

TEST(Parser_LinksToSameTable)
{
    Group g;
    TableRef t = g.add_table("class_Person");
    size_t age_col_ndx = t->add_column(type_Int, "age");
    size_t name_col_ndx = t->add_column(type_String, "name");
    size_t link_col_ndx = t->add_column_link(type_Link, "buddy", *t);
    std::vector<std::string> names = {"Billy", "Bob", "Joe", "Jane", "Joel"};
    t->add_empty_row(5);
    for (size_t i = 0; i < t->size(); ++i) {
        t->set_int(age_col_ndx, i, i);
        t->set_string(name_col_ndx, i, names[i]);
        t->set_link(link_col_ndx, i, (i + 1) % t->size());
    }
    t->nullify_link(link_col_ndx, 4);

    verify_query(test_context, t, "age > 0", 4);
    verify_query(test_context, t, "buddy.age > 0", 4);
    verify_query(test_context, t, "buddy.buddy.age > 0", 3);
    verify_query(test_context, t, "buddy.buddy.buddy.age > 0", 2);
    verify_query(test_context, t, "buddy.buddy.buddy.buddy.age > 0", 1);
    verify_query(test_context, t, "buddy.buddy.buddy.buddy.buddy.age > 0", 0);

    std::string message;
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "buddy.buddy.missing_property > 2", 0), message);
    CHECK(message.find("Person") != std::string::npos); // without the "class_" prefix
    CHECK(message.find("missing_property") != std::string::npos);
}

TEST(Parser_LinksToDifferentTable)
{
    Group g;

    TableRef discounts = g.add_table("class_Discounts");
    size_t discount_off_col = discounts->add_column(type_Double, "reduced_by");
    size_t discount_active_col = discounts->add_column(type_Bool, "active");

    using discount_t = std::pair<double, bool>;
    std::vector<discount_t> discount_info = {{3.0, false}, {2.5, true}, {0.50, true}, {1.50, true}};
    for (discount_t i : discount_info) {
        size_t row_ndx = discounts->add_empty_row();
        discounts->set_double(discount_off_col, row_ndx, i.first);
        discounts->set_bool(discount_active_col, row_ndx, i.second);
    }

    TableRef items = g.add_table("class_Items");
    size_t item_name_col = items->add_column(type_String, "name");
    size_t item_price_col = items->add_column(type_Double, "price");
    size_t item_discount_col = items->add_column_link(type_Link, "discount", *discounts);
    using item_t = std::pair<std::string, double>;
    std::vector<item_t> item_info = {{"milk", 5.5}, {"oranges", 4.0}, {"pizza", 9.5}, {"cereal", 6.5}};
    for (item_t i : item_info) {
        size_t row_ndx = items->add_empty_row();
        items->set_string(item_name_col, row_ndx, i.first);
        items->set_double(item_price_col, row_ndx, i.second);
    }
    items->set_link(item_discount_col, 0, 2); // milk -0.50
    items->set_link(item_discount_col, 2, 1); // pizza -2.5
    items->set_link(item_discount_col, 3, 0); // cereal -3.0 inactive

    TableRef t = g.add_table("class_Person");
    size_t id_col_ndx = t->add_column(type_Int, "customer_id");
    size_t items_col_ndx = t->add_column_link(type_LinkList, "items", *items);
    t->add_empty_row(3);
    for (size_t i = 0; i < t->size(); ++i) {
        t->set_int(id_col_ndx, i, i);
    }

    LinkViewRef list_0 = t->get_linklist(items_col_ndx, 0);
    list_0->add(0);
    list_0->add(1);
    list_0->add(2);
    list_0->add(3);

    LinkViewRef list_1 = t->get_linklist(items_col_ndx, 1);
    for (size_t i = 0; i < 10; ++i) {
        list_1->add(0);
    }

    LinkViewRef list_2 = t->get_linklist(items_col_ndx, 2);
    list_2->add(2);
    list_2->add(2);
    list_2->add(3);

    verify_query(test_context, t, "items.@count > 2", 3); // how many people bought more than two items?
    verify_query(test_context, t, "items.price > 3.0", 3); // how many people buy items over $3.0?
    verify_query(test_context, t, "items.name ==[c] 'milk'", 2); // how many people buy milk?
    verify_query(test_context, t, "items.discount.active == true", 3); // how many people bought items with an active sale?
    verify_query(test_context, t, "items.discount.reduced_by > 2.0", 2); // how many people bought an item marked down by more than $2.0?
    verify_query(test_context, t, "items.@sum.price > 50", 1); // how many people would spend more than $50 without sales applied?
    verify_query(test_context, t, "items.@avg.price > 7", 1); // how manay people like to buy items more expensive on average than $7?

    std::string message;
    // missing property
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "missing_property > 2", 0), message);
    CHECK(message.find("Person") != std::string::npos); // without the "class_" prefix
    CHECK(message.find("missing_property") != std::string::npos);
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "items.absent_property > 2", 0), message);
    CHECK(message.find("Items") != std::string::npos); // without the "class_" prefix
    CHECK(message.find("absent_property") != std::string::npos);
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "items.discount.nonexistent_property > 2", 0), message);
    CHECK(message.find("Discounts") != std::string::npos); // without the "class_" prefix
    CHECK(message.find("nonexistent_property") != std::string::npos);
    // property is not a link
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "customer_id.property > 2", 0), message);
    CHECK(message.find("Person") != std::string::npos); // without the "class_" prefix
    CHECK(message.find("customer_id") != std::string::npos); // is not a link
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "items.price.property > 2", 0), message);
    CHECK(message.find("Items") != std::string::npos); // without the "class_" prefix
    CHECK(message.find("price") != std::string::npos); // is not a link
    // Null cannot be compared to lists
    CHECK_THROW_ANY(verify_query(test_context, t, "items == NULL", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "items != NULL", 0));
}


TEST(Parser_StringOperations)
{
    Group g;
    TableRef t = g.add_table("person");
    size_t name_col_ndx = t->add_column(type_String, "name", true);
    size_t link_col_ndx = t->add_column_link(type_Link, "father", *t);
    std::vector<std::string> names = {"Billy", "Bob", "Joe", "Jake", "Joel"};
    t->add_empty_row(5);
    for (size_t i = 0; i < t->size(); ++i) {
        t->set_string(name_col_ndx, i, names[i]);
        t->set_link(link_col_ndx, i, (i + 1) % t->size());
    }
    t->add_empty_row(); // null
    t->nullify_link(link_col_ndx, 4);

    verify_query(test_context, t, "name == 'Bob'", 1);
    verify_query(test_context, t, "father.name == 'Bob'", 1);
    verify_query(test_context, t, "name ==[c] 'Bob'", 1);
    verify_query(test_context, t, "father.name ==[c] 'Bob'", 1);

    verify_query(test_context, t, "name != 'Bob'", 5);
    verify_query(test_context, t, "father.name != 'Bob'", 5);
    verify_query(test_context, t, "name !=[c] 'bOB'", 5);
    verify_query(test_context, t, "father.name !=[c] 'bOB'", 5);

    verify_query(test_context, t, "name contains \"oe\"", 2);
    verify_query(test_context, t, "father.name contains \"oe\"", 2);
    verify_query(test_context, t, "name contains[c] \"OE\"", 2);
    verify_query(test_context, t, "father.name contains[c] \"OE\"", 2);

    verify_query(test_context, t, "name beginswith \"J\"", 3);
    verify_query(test_context, t, "father.name beginswith \"J\"", 3);
    verify_query(test_context, t, "name beginswith[c] \"j\"", 3);
    verify_query(test_context, t, "father.name beginswith[c] \"j\"", 3);

    verify_query(test_context, t, "name endswith \"e\"", 2);
    verify_query(test_context, t, "father.name endswith \"e\"", 2);
    verify_query(test_context, t, "name endswith[c] \"E\"", 2);
    verify_query(test_context, t, "father.name endswith[c] \"E\"", 2);

    verify_query(test_context, t, "name like \"?o?\"", 2);
    verify_query(test_context, t, "father.name like \"?o?\"", 2);
    verify_query(test_context, t, "name like[c] \"?O?\"", 2);
    verify_query(test_context, t, "father.name like[c] \"?O?\"", 2);

    verify_query(test_context, t, "name == NULL", 1);
    verify_query(test_context, t, "NULL == name", 1);
    verify_query(test_context, t, "name != NULL", 5);
    verify_query(test_context, t, "NULL != name", 5);
    verify_query(test_context, t, "name ==[c] NULL", 1);
    verify_query(test_context, t, "NULL ==[c] name", 1);
    verify_query(test_context, t, "name !=[c] NULL", 5);
    verify_query(test_context, t, "NULL !=[c] name", 5);

    // for strings 'NULL' is also a synonym for the null string
    verify_query(test_context, t, "name CONTAINS NULL", 6);
    verify_query(test_context, t, "name CONTAINS[c] NULL", 6);
    verify_query(test_context, t, "name BEGINSWITH NULL", 6);
    verify_query(test_context, t, "name BEGINSWITH[c] NULL", 6);
    verify_query(test_context, t, "name ENDSWITH NULL", 6);
    verify_query(test_context, t, "name ENDSWITH[c] NULL", 6);
    verify_query(test_context, t, "name LIKE NULL", 1);
    verify_query(test_context, t, "name LIKE[c] NULL", 1);

    // string operators are not commutative
    CHECK_THROW_ANY(verify_query(test_context, t, "NULL CONTAINS name", 6));
    CHECK_THROW_ANY(verify_query(test_context, t, "NULL CONTAINS[c] name", 6));
    CHECK_THROW_ANY(verify_query(test_context, t, "NULL BEGINSWITH name", 6));
    CHECK_THROW_ANY(verify_query(test_context, t, "NULL BEGINSWITH[c] name", 6));
    CHECK_THROW_ANY(verify_query(test_context, t, "NULL ENDSWITH name", 6));
    CHECK_THROW_ANY(verify_query(test_context, t, "NULL ENDSWITH[c] name", 6));
    CHECK_THROW_ANY(verify_query(test_context, t, "NULL LIKE name", 1));
    CHECK_THROW_ANY(verify_query(test_context, t, "NULL LIKE[c] name", 1));
}


TEST(Parser_Timestamps)
{
    Group g;
    TableRef t = g.add_table("person");
    size_t birthday_col_ndx = t->add_column(type_Timestamp, "birthday");           // disallow null
    size_t internal_col_ndx = t->add_column(type_Timestamp, "T399", true);         // allow null
    size_t readable_col_ndx = t->add_column(type_Timestamp, "T2017-12-04", true);  // allow null
    size_t link_col_ndx = t->add_column_link(type_Link, "linked", *t);
    t->add_empty_row(5);

    t->set_timestamp(birthday_col_ndx, 0, Timestamp(-1, -1)); // before epoch by 1 second and one nanosecond
    t->set_timestamp(birthday_col_ndx, 1, Timestamp(0, -1)); // before epoch by one nanosecond

    t->set_timestamp(internal_col_ndx, 0, Timestamp(realm::null()));
    t->set_timestamp(internal_col_ndx, 1, Timestamp(1512130073, 0)); // 2017/12/02 @ 12:47am (UTC)
    t->set_timestamp(internal_col_ndx, 2, Timestamp(1512130073, 505)); // with nanoseconds
    t->set_timestamp(internal_col_ndx, 3, Timestamp(1, 2));
    t->set_timestamp(internal_col_ndx, 4, Timestamp(0, 0));

    t->set_timestamp(readable_col_ndx, 0, Timestamp(1512130073, 0));
    t->set_timestamp(readable_col_ndx, 1, Timestamp(1512130073, 505));

    t->set_link(link_col_ndx, 0, 1);
    t->set_link(link_col_ndx, 2, 0);

    Query q = t->where();
    verify_query(test_context, t, "T399 == NULL", 1);
    verify_query(test_context, t, "T399 != NULL", 4);
    verify_query(test_context, t, "linked.T399 == NULL", 4); // null links count as a match for null here
    verify_query(test_context, t, "linked != NULL && linked.T399 == NULL", 1);
    verify_query(test_context, t, "linked.T399 != NULL", 1);
    verify_query(test_context, t, "linked != NULL && linked.T399 != NULL", 1);
    verify_query(test_context, t, "T399 == T399:0", 0);
    verify_query(test_context, t, "linked.T399 == T399:0", 0);
    verify_query(test_context, t, "T399 == 2017-12-04@0:0:0", 0);

    verify_query(test_context, t, "T2017-12-04 == NULL", 3);
    verify_query(test_context, t, "T2017-12-04 != NULL", 2);
    verify_query(test_context, t, "linked.T2017-12-04 == NULL", 3); // null links count as a match for null here
    verify_query(test_context, t, "linked != NULL && linked.T2017-12-04 == NULL", 0);
    verify_query(test_context, t, "linked.T2017-12-04 != NULL", 2);
    verify_query(test_context, t, "linked != NULL && linked.T2017-12-04 != NULL", 2);
    verify_query(test_context, t, "T2017-12-04 == T399:0", 0);
    verify_query(test_context, t, "linked.T2017-12-04 == T399:0", 0);
    verify_query(test_context, t, "T2017-12-04 == 2017-12-04@0:0:0", 0);

    verify_query(test_context, t, "birthday == NULL", 0);
    verify_query(test_context, t, "birthday != NULL", 5);
    verify_query(test_context, t, "birthday == T0:0", 3);
    verify_query(test_context, t, "birthday == 1970-1-1@0:0:0:0", 3); // epoch is default non-null Timestamp

#ifndef _WIN32 // windows native functions do not support pre epoch conversions, other platforms stop at ~1901
    verify_query(test_context, t, "birthday == 1969-12-31@23:59:59:1", 1); // just before epoch
    verify_query(test_context, t, "birthday > 1905-12-31@23:59:59", 5);
    verify_query(test_context, t, "birthday > 1905-12-31@23:59:59:2020", 5);
#endif

    // two column timestamps
    verify_query(test_context, t, "birthday == T399", 1); // a null entry matches

    // dates pre 1900 are not supported by functions like timegm
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday > 1800-12-31@23:59:59", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday > 1800-12-31@23:59:59:2020", 4));

    // negative nanoseconds are not allowed
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T-1:1", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1:-1", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == 1970-1-1@0:0:1:-1", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == 1969-12-31@23:59:59:-1", 1));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == 1970-1-1@0:0:0:-1", 1));

    // Invalid predicate
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1:", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T:1", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == 1970-1-1", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == 1970-1-1@", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == 1970-1-1@0", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == 1970-1-1@0:", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == 1970-1-1@0:0", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == 1970-1-1@0:0:", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == 1970-1-1@0:0:0:", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == 1970-1-1@0:0:0:0:", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == 1970-1-1@0:0:0:0:0", 0));
}


TEST(Parser_NullableBinaries)
{
    Group g;
    TableRef items = g.add_table("item");
    TableRef people = g.add_table("person");
    size_t binary_col_ndx = items->add_column(type_Binary, "data");
    size_t nullable_binary_col_ndx = items->add_column(type_Binary, "nullable_data", true);
    items->add_empty_row(5);
    BinaryData bd0("knife", 5);
    items->set_binary(binary_col_ndx, 0, bd0);
    items->set_binary(nullable_binary_col_ndx, 0, bd0);
    BinaryData bd1("plate", 5);
    items->set_binary(binary_col_ndx, 1, bd1);
    items->set_binary(nullable_binary_col_ndx, 1, bd1);
    BinaryData bd2("fork", 4);
    items->set_binary(binary_col_ndx, 2, bd2);
    items->set_binary(nullable_binary_col_ndx, 2, bd2);

    size_t fav_item_col_ndx = people->add_column_link(type_Link, "fav_item", *items);
    people->add_empty_row(5);
    people->set_link(fav_item_col_ndx, 0, 0);
    people->set_link(fav_item_col_ndx, 1, 1);
    people->set_link(fav_item_col_ndx, 2, 2);
    people->set_link(fav_item_col_ndx, 3, 3);
    people->set_link(fav_item_col_ndx, 4, 4);

    // direct checks
    verify_query(test_context, items, "data == NULL", 0);
    verify_query(test_context, items, "data != NULL", 5);
    verify_query(test_context, items, "nullable_data == NULL", 2);
    verify_query(test_context, items, "nullable_data != NULL", 3);

    verify_query(test_context, items, "nullable_data CONTAINS 'f'", 2);
    verify_query(test_context, items, "nullable_data BEGINSWITH 'f'", 1);
    verify_query(test_context, items, "nullable_data ENDSWITH 'e'", 2);
    verify_query(test_context, items, "nullable_data LIKE 'f*'", 1);
    verify_query(test_context, items, "nullable_data CONTAINS[c] 'F'", 2);
    verify_query(test_context, items, "nullable_data BEGINSWITH[c] 'F'", 1);
    verify_query(test_context, items, "nullable_data ENDSWITH[c] 'E'", 2);
    verify_query(test_context, items, "nullable_data LIKE[c] 'F*'", 1);

    verify_query(test_context, items, "nullable_data CONTAINS NULL", 5);
    verify_query(test_context, items, "nullable_data BEGINSWITH NULL", 5);
    verify_query(test_context, items, "nullable_data ENDSWITH NULL", 5);
    verify_query(test_context, items, "nullable_data LIKE NULL", 2);
    verify_query(test_context, items, "nullable_data CONTAINS[c] NULL", 3);
    verify_query(test_context, items, "nullable_data BEGINSWITH[c] NULL", 5);
    verify_query(test_context, items, "nullable_data ENDSWITH[c] NULL", 5);
    verify_query(test_context, items, "nullable_data LIKE[c] NULL", 2);

    // these operators are not commutative
    CHECK_THROW_ANY(verify_query(test_context, items, "NULL CONTAINS nullable_data", 5));
    CHECK_THROW_ANY(verify_query(test_context, items, "NULL BEGINSWITH nullable_data", 5));
    CHECK_THROW_ANY(verify_query(test_context, items, "NULL ENDSWITH nullable_data", 5));
    CHECK_THROW_ANY(verify_query(test_context, items, "NULL LIKE nullable_data", 2));
    CHECK_THROW_ANY(verify_query(test_context, items, "NULL CONTAINS[c] nullable_data", 5));
    CHECK_THROW_ANY(verify_query(test_context, items, "NULL BEGINSWITH[c] nullable_data", 5));
    CHECK_THROW_ANY(verify_query(test_context, items, "NULL ENDSWITH[c] nullable_data", 5));
    CHECK_THROW_ANY(verify_query(test_context, items, "NULL LIKE[c] nullable_data", 2));

    // check across links
    verify_query(test_context, people, "fav_item.data == NULL", 0);
    verify_query(test_context, people, "fav_item.data != NULL", 5);
    verify_query(test_context, people, "fav_item.nullable_data == NULL", 2);
    verify_query(test_context, people, "fav_item.nullable_data != NULL", 3);
    verify_query(test_context, people, "NULL == fav_item.data", 0);

    verify_query(test_context, people, "fav_item.data ==[c] NULL", 0);
    verify_query(test_context, people, "fav_item.data !=[c] NULL", 5);
    verify_query(test_context, people, "fav_item.nullable_data ==[c] NULL", 2);
    verify_query(test_context, people, "fav_item.nullable_data !=[c] NULL", 3);
    verify_query(test_context, people, "NULL ==[c] fav_item.data", 0);

    verify_query(test_context, people, "fav_item.data CONTAINS 'f'", 2);
    verify_query(test_context, people, "fav_item.data BEGINSWITH 'f'", 1);
    verify_query(test_context, people, "fav_item.data ENDSWITH 'e'", 2);
    verify_query(test_context, people, "fav_item.data LIKE 'f*'", 1);
    verify_query(test_context, people, "fav_item.data CONTAINS[c] 'F'", 2);
    verify_query(test_context, people, "fav_item.data BEGINSWITH[c] 'F'", 1);
    verify_query(test_context, people, "fav_item.data ENDSWITH[c] 'E'", 2);
    verify_query(test_context, people, "fav_item.data LIKE[c] 'F*'", 1);

    // two column
    verify_query(test_context, people, "fav_item.data == fav_item.nullable_data", 3);
    verify_query(test_context, people, "fav_item.data == fav_item.data", 5);
    verify_query(test_context, people, "fav_item.nullable_data == fav_item.nullable_data", 5);
}

TEST(Parser_OverColumnIndexChanges)
{
    Group g;
    TableRef table = g.add_table("table");
    size_t first_col_ndx = table->add_column(type_Int, "to_remove");
    size_t int_col_ndx = table->add_column(type_Int, "ints");
    size_t double_col_ndx = table->add_column(type_Double, "doubles");
    size_t string_col_ndx = table->add_column(type_String, "strings");
    table->add_empty_row(3);
    for (size_t i = 0; i < table->size(); ++i) {
        table->set_int(int_col_ndx, i, i);
        table->set_double(double_col_ndx, i, double(i));
        std::string str(i, 'a');
        table->set_string(string_col_ndx, i, StringData(str));
    }

    std::string ints_before = verify_query(test_context, table, "ints >= 1", 2).get_description();
    std::string doubles_before = verify_query(test_context, table, "doubles >= 1", 2).get_description();
    std::string strings_before = verify_query(test_context, table, "strings.@count >= 1", 2).get_description();

    table->remove_column(first_col_ndx);

    std::string ints_after = verify_query(test_context, table, "ints >= 1", 2).get_description();
    std::string doubles_after = verify_query(test_context, table, "doubles >= 1", 2).get_description();
    std::string strings_after = verify_query(test_context, table, "strings.@count >= 1", 2).get_description();

    CHECK_EQUAL(ints_before, ints_after);
    CHECK_EQUAL(doubles_before, doubles_after);
    CHECK_EQUAL(strings_before, strings_after);
}


TEST(Parser_TwoColumnExpressionBasics)
{
    Group g;
    TableRef table = g.add_table("table");
    size_t int_col_ndx = table->add_column(type_Int, "ints", true);
    size_t double_col_ndx = table->add_column(type_Double, "doubles");
    size_t string_col_ndx = table->add_column(type_String, "strings");
    size_t link_col_ndx = table->add_column_link(type_Link, "link", *table);
    table->add_empty_row(3);
    for (size_t i = 0; i < table->size(); ++i) {
        table->set_int(int_col_ndx, i, i);
        table->set_double(double_col_ndx, i, double(i));
        std::string str(i, 'a');
        table->set_string(string_col_ndx, i, StringData(str));
    }
    table->set_link(link_col_ndx, 1, 0);

    Query q = table->where().and_query(table->column<Int>(int_col_ndx) == table->column<String>(string_col_ndx).size());
    CHECK_EQUAL(q.count(), 3);
    std::string desc = q.get_description();

    verify_query(test_context, table, "ints == 0", 1);
    verify_query(test_context, table, "ints == ints", 3);
    verify_query(test_context, table, "ints == strings.@count", 3);
    verify_query(test_context, table, "strings.@count == ints", 3);
    verify_query(test_context, table, "ints == NULL", 0);
    verify_query(test_context, table, "doubles == doubles", 3);
    verify_query(test_context, table, "strings == strings", 3);
    verify_query(test_context, table, "ints == link.@count", 2); // row 0 has 0 links, row 1 has 1 link

    // type mismatch
    CHECK_THROW_ANY(verify_query(test_context, table, "doubles == ints", 0));
    CHECK_THROW_ANY(verify_query(test_context, table, "doubles == strings", 0));
    CHECK_THROW_ANY(verify_query(test_context, table, "ints == doubles", 0));
    CHECK_THROW_ANY(verify_query(test_context, table, "strings == doubles", 0));

}

TEST(Parser_TwoColumnAggregates)
{
    Group g;

    TableRef discounts = g.add_table("class_Discounts");
    size_t discount_name_col = discounts->add_column(type_String, "promotion", true);
    size_t discount_off_col = discounts->add_column(type_Double, "reduced_by");
    size_t discount_active_col = discounts->add_column(type_Bool, "active");

    using discount_t = std::pair<double, bool>;
    std::vector<discount_t> discount_info = {{3.0, false}, {2.5, true}, {0.50, true}, {1.50, true}};
    for (discount_t i : discount_info) {
        size_t row_ndx = discounts->add_empty_row();
        discounts->set_double(discount_off_col, row_ndx, i.first);
        discounts->set_bool(discount_active_col, row_ndx, i.second);
    }
    discounts->set_string(discount_name_col, 0, "back to school");
    discounts->set_string(discount_name_col, 1, "pizza lunch special");
    discounts->set_string(discount_name_col, 2, "manager's special");

    TableRef items = g.add_table("class_Items");
    size_t item_name_col = items->add_column(type_String, "name");
    size_t item_price_col = items->add_column(type_Double, "price");
    size_t item_discount_col = items->add_column_link(type_Link, "discount", *discounts);
    using item_t = std::pair<std::string, double>;
    std::vector<item_t> item_info = {{"milk", 5.5}, {"oranges", 4.0}, {"pizza", 9.5}, {"cereal", 6.5}};
    for (item_t i : item_info) {
        size_t row_ndx = items->add_empty_row();
        items->set_string(item_name_col, row_ndx, i.first);
        items->set_double(item_price_col, row_ndx, i.second);
    }
    items->set_link(item_discount_col, 0, 2); // milk -0.50
    items->set_link(item_discount_col, 2, 1); // pizza -2.5
    items->set_link(item_discount_col, 3, 0); // cereal -3.0 inactive

    TableRef t = g.add_table("class_Person");
    size_t id_col_ndx = t->add_column(type_Int, "customer_id");
    size_t account_col_ndx = t->add_column(type_Double, "account_balance");
    size_t items_col_ndx = t->add_column_link(type_LinkList, "items", *items);
    t->add_empty_row(3);
    for (size_t i = 0; i < t->size(); ++i) {
        t->set_int(id_col_ndx, i, i);
        t->set_double(account_col_ndx, i, double((i + 1) * 10.0));
    }

    LinkViewRef list_0 = t->get_linklist(items_col_ndx, 0);
    list_0->add(0);
    list_0->add(1);
    list_0->add(2);
    list_0->add(3);

    LinkViewRef list_1 = t->get_linklist(items_col_ndx, 1);
    for (size_t i = 0; i < 10; ++i) {
        list_1->add(0);
    }

    LinkViewRef list_2 = t->get_linklist(items_col_ndx, 2);
    list_2->add(2);
    list_2->add(2);
    list_2->add(3);

    // int vs linklist count/size
    verify_query(test_context, t, "customer_id < items.@count", 3);
    verify_query(test_context, t, "customer_id < items.@size", 3);

    // double vs linklist count/size
    verify_query(test_context, t, "items.@min.price > items.@count", 1);
    verify_query(test_context, t, "items.@min.price > items.@size", 1);

    // double vs string/binary count/size is not supported due to a core implementation limitation
    CHECK_THROW_ANY(verify_query(test_context, items, "name.@count > price", 3));
    CHECK_THROW_ANY(verify_query(test_context, items, "price < name.@size", 3));

    // double vs double
    verify_query(test_context, t, "items.@sum.price > account_balance", 2);
    verify_query(test_context, t, "items.@min.price > account_balance", 0);
    verify_query(test_context, t, "items.@max.price > account_balance", 0);
    verify_query(test_context, t, "items.@avg.price > account_balance", 0);

    // cannot aggregate string
    CHECK_THROW_ANY(verify_query(test_context, t, "items.@min.name > account_balance", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "items.@max.name > account_balance", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "items.@sum.name > account_balance", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "items.@avg.name > account_balance", 0));
    // cannot aggregate link
    CHECK_THROW_ANY(verify_query(test_context, t, "items.@min.discount > account_balance", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "items.@max.discount > account_balance", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "items.@sum.discount > account_balance", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "items.@avg.discount > account_balance", 0));

    verify_query(test_context, t, "items.@count < account_balance", 3); // linklist count vs double
    verify_query(test_context, t, "items.@count > 3", 2);   // linklist count vs literal int
    // linklist count vs literal double, integer promotion done here so this is true!
    verify_query(test_context, t, "items.@count == 3.1", 1);

    // two string counts is allowed (int comparison)
    verify_query(test_context, items, "discount.promotion.@count > name.@count", 3);
    // link count vs string count (int comparison)
    verify_query(test_context, items, "discount.@count < name.@count", 4);

    // string operators
    verify_query(test_context, items, "discount.promotion == name", 0);
    verify_query(test_context, items, "discount.promotion != name", 4);
    verify_query(test_context, items, "discount.promotion CONTAINS name", 1);
    verify_query(test_context, items, "discount.promotion BEGINSWITH name", 1);
    verify_query(test_context, items, "discount.promotion ENDSWITH name", 0);
    verify_query(test_context, items, "discount.promotion LIKE name", 0);
    verify_query(test_context, items, "discount.promotion ==[c] name", 0);
    verify_query(test_context, items, "discount.promotion !=[c] name", 4);
    verify_query(test_context, items, "discount.promotion CONTAINS[c] name", 1);
    verify_query(test_context, items, "discount.promotion BEGINSWITH[c] name", 1);
    verify_query(test_context, items, "discount.promotion ENDSWITH[c] name", 0);
    verify_query(test_context, items, "discount.promotion LIKE[c] name", 0);
}

void verify_query_sub(test_util::unit_test::TestContext& test_context, TableRef t, std::string query_string, const util::Any* arg_list, size_t num_args, size_t num_results) {

    query_builder::AnyContext ctx;
    std::string empty_string;
    realm::query_builder::ArgumentConverter<util::Any, query_builder::AnyContext> args(ctx, arg_list, num_args);

    Query q = t->where();

    realm::parser::Predicate p = realm::parser::parse(query_string).predicate;
    realm::query_builder::apply_predicate(q, p, args);

    CHECK_EQUAL(q.count(), num_results);
    std::string description = q.get_description();
    //std::cerr << "original: " << query_string << "\tdescribed: " << description << "\n";
    Query q2 = t->where();

    realm::parser::Predicate p2 = realm::parser::parse(description).predicate;
    realm::query_builder::apply_predicate(q2, p2);

    CHECK_EQUAL(q2.count(), num_results);
}

TEST(Parser_substitution)
{
    Group g;
    TableRef t = g.add_table("person");
    size_t int_col_ndx = t->add_column(type_Int, "age");
    size_t str_col_ndx = t->add_column(type_String, "name");
    size_t double_col_ndx = t->add_column(type_Double, "fees");
    size_t bool_col_ndx = t->add_column(type_Bool, "paid", true);
    size_t time_col_ndx = t->add_column(type_Timestamp, "time", true);
    size_t binary_col_ndx = t->add_column(type_Binary, "binary", true);
    size_t float_col_ndx = t->add_column(type_Float, "floats", true);
    size_t link_col_ndx = t->add_column_link(type_Link, "links", *t);
    size_t list_col_ndx = t->add_column_link(type_LinkList, "list", *t);
    t->add_empty_row(5);
    std::vector<std::string> names = {"Billy", "Bob", "Joe", "Jane", "Joel"};
    std::vector<double> fees = { 2.0, 2.23, 2.22, 2.25, 3.73 };

    for (size_t i = 0; i < t->size(); ++i) {
        t->set_int(int_col_ndx, i, i);
        t->set_string(str_col_ndx, i, names[i]);
        t->set_double(double_col_ndx, i, fees[i]);
    }
    t->set_bool(bool_col_ndx, 0, true);
    t->set_bool(bool_col_ndx, 1, false);
    t->set_timestamp(time_col_ndx, 1, Timestamp(1512130073, 505)); // 2017/12/02 @ 12:47am (UTC) + 505 nanoseconds
    BinaryData bd0("oe");
    BinaryData bd1("eo");
    t->set_binary(binary_col_ndx, 0, bd0);
    t->set_binary(binary_col_ndx, 1, bd1);
    t->set_float(float_col_ndx, 0, 2.33f);
    t->set_float(float_col_ndx, 1, 2.22f);
    t->set_link(link_col_ndx, 0, 1);
    t->set_link(link_col_ndx, 1, 0);
    LinkViewRef list_0 = t->get_linklist(list_col_ndx, 0);
    list_0->add(0);
    list_0->add(1);
    list_0->add(2);
    LinkViewRef list_1 = t->get_linklist(list_col_ndx, 1);
    list_1->add(0);

    util::Any args [] = { Int(2), Double(2.22), String("oe"), realm::null{}, Bool(true),
                            Timestamp(1512130073, 505), bd0, Float(2.33), Int(1), Int(3), Int(4) };
    size_t num_args = 11;
    verify_query_sub(test_context, t, "age > $0", args, num_args, 2);
    verify_query_sub(test_context, t, "age > $0 || fees == $1", args, num_args, 3);
    verify_query_sub(test_context, t, "name CONTAINS[c] $2", args, num_args, 2);
    verify_query_sub(test_context, t, "paid == $3", args, num_args, 3);
    verify_query_sub(test_context, t, "paid == $4", args, num_args, 1);
    verify_query_sub(test_context, t, "time == $5", args, num_args, 1);
    verify_query_sub(test_context, t, "time == $3", args, num_args, 4);
    verify_query_sub(test_context, t, "binary == $6", args, num_args, 1);
    verify_query_sub(test_context, t, "binary == $3", args, num_args, 3);
    verify_query_sub(test_context, t, "floats == $7", args, num_args, 1);
    verify_query_sub(test_context, t, "floats == $3", args, num_args, 3);
    verify_query_sub(test_context, t, "links == $3", args, num_args, 3);

    // substitutions through collection aggregates is a different code path
    verify_query_sub(test_context, t, "list.@min.age < $0", args, num_args, 2);
    verify_query_sub(test_context, t, "list.@max.age >= $0", args, num_args, 1);
    verify_query_sub(test_context, t, "list.@sum.age >= $0", args, num_args, 1);
    verify_query_sub(test_context, t, "list.@avg.age < $0", args, num_args, 2);
    verify_query_sub(test_context, t, "list.@count > $0", args, num_args, 1);
    verify_query_sub(test_context, t, "list.@size > $0", args, num_args, 1);
    verify_query_sub(test_context, t, "name.@count > $0", args, num_args, 5);
    verify_query_sub(test_context, t, "name.@size > $0", args, num_args, 5);
    verify_query_sub(test_context, t, "binary.@count > $0", args, num_args, 2);
    verify_query_sub(test_context, t, "binary.@size > $0", args, num_args, 2);

    // reusing properties, mixing order
    verify_query_sub(test_context, t, "(age > $0 || fees == $1) && age == $0", args, num_args, 1);

    // negative index
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "age > $-1", args, num_args, 0));
    // missing index
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "age > $", args, num_args, 0));
    // non-numerical index
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "age > $age", args, num_args, 0));
    // leading zero index
    verify_query_sub(test_context, t, "name CONTAINS[c] $002", args, num_args, 2);
    // double digit index
    verify_query_sub(test_context, t, "age == $10", args, num_args, 1);

    // referencing a parameter outside of the list size throws
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "age > $0", args, /*num_args*/ 0, 0));

    // invalid types
    // int
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "age > $1", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "age > $2", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "age > $3", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "age > $4", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "age > $5", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "age > $6", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "age > $7", args, num_args, 0));
    // double
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "fees > $0", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "fees > $2", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "fees > $3", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "fees > $4", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "fees > $5", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "fees > $6", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "fees > $7", args, num_args, 0));
    // float
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "floats > $0", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "floats > $1", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "floats > $2", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "floats > $3", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "floats > $4", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "floats > $5", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "floats > $6", args, num_args, 0));
    // string
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "name == $0", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "name == $1", args, num_args, 0));
                    verify_query_sub(test_context, t, "name == $3", args, num_args, 0);
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "name == $4", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "name == $5", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "name == $6", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "name == $7", args, num_args, 0));
    // bool
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "paid == $0", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "paid == $1", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "paid == $2", args, num_args, 0));
                    verify_query_sub(test_context, t, "paid == $3", args, num_args, 3);
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "paid == $5", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "paid == $6", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "paid == $7", args, num_args, 0));
    // timestamp
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "time == $0", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "time == $1", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "time == $2", args, num_args, 0));
                    verify_query_sub(test_context, t, "time == $3", args, num_args, 4);
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "time == $4", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "time == $6", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "time == $7", args, num_args, 0));
    // binary
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "binary == $0", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "binary == $1", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "binary == $2", args, num_args, 0));
                    verify_query_sub(test_context, t, "binary == $3", args, num_args, 3);
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "binary == $4", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "binary == $5", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "binary == $7", args, num_args, 0));
}

TEST(Parser_string_binary_encoding)
{
    Group g;
    TableRef t = g.add_table("person");
    size_t str_col_ndx = t->add_column(type_String, "string_col", true);
    size_t bin_col_ndx = t->add_column(type_Binary, "binary_col", true);

    std::vector<std::string> test_strings = {
        // Credit of the following list to https://github.com/minimaxir/big-list-of-naughty-strings (MIT)
        "undefined",
        "undef",
        "null",
        "NULL",
        "(null)",
        "nil",
        "NIL",
        "true",
        "false",
        "True",
        "False",
        "TRUE",
        "FALSE",
        "None",
        "hasOwnProperty",
        "\\",
        "\\\\",
        "1.00",
        "$1.00",
        "1/2",
        "1E2",
        "1E02",
        "1E+02",
        "-1",
        "-1.00",
        "-$1.00",
        "-1/2",
        "-1E2",
        "-1E02",
        "-1E+02",
        "1/0",
        "0/0",
        "-2147483648/-1",
        "-9223372036854775808/-1",
        "-0",
        "-0.0",
        "+0",
        "+0.0",
        "0.00",
        "0..0",
        "0.0.0",
        "0,00",
        "0,,0",
        "0,0,0",
        "0.0/0",
        "1.0/0.0",
        "0.0/0.0",
        "1,0/0,0",
        "0,0/0,0",
        "--1",
        "-.",
        "-,",
        "999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
        "NaN",
        "Infinity",
        "-Infinity",
        "INF",
        "1#INF",
        "-1#IND",
        "1#QNAN",
        "1#SNAN",
        "1#IND",
        "0x0",
        "0xffffffff",
        "0xffffffffffffffff",
        "0xabad1dea",
        "123456789012345678901234567890123456789",
        "1,000.00",
        "1 000.00",
        "1'000.00",
        "1,000,000.00",
        "1 000 000.00",
        "1'000'000.00",
        "1.000,00",
        "1 000,00",
        "1'000,00",
        "1.000.000,00",
        "1 000 000,00",
        "1'000'000,00",
        "01000",
        "08",
        "09",
        "2.2250738585072011e-308",
        ",./;'[]\\-=",
        "<>?:\"{}|_+",
        "!@#$%^&*()`~",
        "''",
        "\"\"",
        "'\"'",
        "\"''''\"'\"",
        "\"'\"'\"''''\"",
        "<foo val=“bar” />",
        "<foo val=`bar' />"
    };

    t->add_empty_row(); // nulls
    // add a single char of each value
    for (size_t i = 0; i < 255; ++i) {
        unsigned char c = static_cast<unsigned char>(i);
        test_strings.push_back(std::string(c, 1));
    }
    // a single string of 100 nulls
    test_strings.push_back(std::string(100, '\0'));

    for (const std::string& buff : test_strings) {
        StringData sd(buff);
        BinaryData bd(buff);
        size_t row_ndx = t->add_empty_row();
        t->set_string(str_col_ndx, row_ndx, sd);
        t->set_binary(bin_col_ndx, row_ndx, bd);
    }

    for (const std::string& buff : test_strings) {
        size_t num_results = 1;
        Query qstr = t->where().equal(str_col_ndx, StringData(buff), true);
        Query qbin = t->where().equal(bin_col_ndx, BinaryData(buff));
        CHECK_EQUAL(qstr.count(), num_results);
        CHECK_EQUAL(qbin.count(), num_results);
        std::string string_description = qstr.get_description();
        std::string binary_description = qbin.get_description();
        //std::cerr << "original: " << buff << "\tdescribed: " << string_description << "\n";

        Query qstr2 = t->where();
        realm::parser::Predicate pstr2 = realm::parser::parse(string_description).predicate;
        realm::query_builder::apply_predicate(qstr2, pstr2);
        CHECK_EQUAL(qstr2.count(), num_results);

        Query qbin2 = t->where();
        realm::parser::Predicate pbin2 = realm::parser::parse(binary_description).predicate;
        realm::query_builder::apply_predicate(qbin2, pbin2);
        CHECK_EQUAL(qbin2.count(), num_results);
    }
}

TEST(Parser_collection_aggregates)
{
    Group g;
    TableRef people = g.add_table("class_Person");
    TableRef courses = g.add_table("class_Course");
    size_t title_col_ndx = courses->add_column(type_String, "title");
    size_t credits_col_ndx = courses->add_column(type_Double, "credits");
    size_t hours_col_ndx = courses->add_column(type_Int, "hours_required");
    size_t fail_col_ndx = courses->add_column(type_Float, "failure_percentage");
    size_t int_col_ndx = people->add_column(type_Int, "age");
    size_t str_col_ndx = people->add_column(type_String, "name");
    size_t courses_col_ndx = people->add_column_link(type_LinkList, "courses_taken", *courses);
    size_t binary_col_ndx = people->add_column(type_Binary, "hash");
    using info_t = std::pair<std::string, size_t>;
    std::vector<info_t> person_info
        = {{"Billy", 18}, {"Bob", 17}, {"Joe", 19}, {"Jane", 20}, {"Joel", 18}};
    for (info_t i : person_info) {
        size_t row_ndx = people->add_empty_row();
        people->set_string(str_col_ndx, row_ndx, i.first);
        people->set_int(int_col_ndx, row_ndx, i.second);
        std::string hash(row_ndx, 'a'); // a repeated i times
        BinaryData payload(hash);
        people->set_binary(binary_col_ndx, row_ndx, payload);
    }
    using cinfo = std::tuple<std::string, double, size_t, float>;
    std::vector<cinfo> course_info
            = { cinfo{"Math", 5.0, 42, 0.36f}, cinfo{"Comp Sci", 4.5, 45, 0.25f}, cinfo{"Chemistry", 4.0, 41, 0.40f},
            cinfo{"English", 3.5, 40, 0.07f}, cinfo{"Physics", 4.5, 42, 0.42f} };
    for (cinfo course : course_info) {
        size_t row_ndx = courses->add_empty_row();
        courses->set_string(title_col_ndx, row_ndx, std::get<0>(course));
        courses->set_double(credits_col_ndx, row_ndx, std::get<1>(course));
        courses->set_int(hours_col_ndx, row_ndx, std::get<2>(course));
        courses->set_float(fail_col_ndx, row_ndx, std::get<3>(course));
    }
    LinkViewRef billy_courses = people->get_linklist(courses_col_ndx, 0);
    billy_courses->add(0);
    billy_courses->add(1);
    billy_courses->add(4);
    LinkViewRef bob_courses = people->get_linklist(courses_col_ndx, 1);
    bob_courses->add(0);
    bob_courses->add(1);
    bob_courses->add(1);
    LinkViewRef joe_courses = people->get_linklist(courses_col_ndx, 2);
    joe_courses->add(3);
    LinkViewRef jane_courses = people->get_linklist(courses_col_ndx, 3);
    jane_courses->add(2);
    jane_courses->add(4);

    Query q = people->where();

    // int
    verify_query(test_context, people, "courses_taken.@min.hours_required <= 41", 2);
    verify_query(test_context, people, "courses_taken.@max.hours_required >= 45", 2);
    verify_query(test_context, people, "courses_taken.@sum.hours_required <= 100", 3);
    verify_query(test_context, people, "courses_taken.@avg.hours_required > 41", 3);

    // double
    verify_query(test_context, people, "courses_taken.@min.credits == 4.5", 2);
    verify_query(test_context, people, "courses_taken.@max.credits == 5.0", 2);
    verify_query(test_context, people, "courses_taken.@sum.credits > 8.6", 2);
    verify_query(test_context, people, "courses_taken.@avg.credits > 4.0", 3);

    // float
    verify_query(test_context, people, "courses_taken.@min.failure_percentage < 0.10", 1);
    verify_query(test_context, people, "courses_taken.@max.failure_percentage > 0.40", 2);
    verify_query(test_context, people, "courses_taken.@sum.failure_percentage > 0.5", 3);
    verify_query(test_context, people, "courses_taken.@avg.failure_percentage > 0.40", 1);

    // count and size are interchangeable but only operate on certain types
    // count of lists
    verify_query(test_context, people, "courses_taken.@count > 2", 2);
    verify_query(test_context, people, "courses_taken.@size > 2", 2);
    verify_query(test_context, people, "courses_taken.@count == 0", 1);
    verify_query(test_context, people, "courses_taken.@size == 0", 1);

    // size of strings
    verify_query(test_context, people, "name.@count == 0", 0);
    verify_query(test_context, people, "name.@size == 0", 0);
    verify_query(test_context, people, "name.@count > 3", 3);
    verify_query(test_context, people, "name.@size > 3", 3);

    // size of binary data
    verify_query(test_context, people, "hash.@count == 0", 1);
    verify_query(test_context, people, "hash.@size == 0", 1);
    verify_query(test_context, people, "hash.@count > 2", 2);
    verify_query(test_context, people, "hash.@size > 2", 2);

    std::string message;

    // string
    CHECK_THROW_ANY(verify_query(test_context, people, "courses_taken.@min.title <= 41", 2));
    CHECK_THROW_ANY(verify_query(test_context, people, "courses_taken.@max.title <= 41", 2));
    CHECK_THROW_ANY(verify_query(test_context, people, "courses_taken.@sum.title <= 41", 2));
    CHECK_THROW_ANY(verify_query(test_context, people, "courses_taken.@avg.title <= 41", 2));

    // min, max, sum, avg require a target property on the linked table
    CHECK_THROW_ANY(verify_query(test_context, people, "courses_taken.@min <= 41", 2));
    CHECK_THROW_ANY(verify_query(test_context, people, "courses_taken.@max <= 41", 2));
    CHECK_THROW_ANY(verify_query(test_context, people, "courses_taken.@sum <= 41", 2));
    CHECK_THROW_ANY(verify_query(test_context, people, "courses_taken.@avg <= 41", 2));

    // aggregate operations on a non-linklist column must throw
    CHECK_THROW_ANY(verify_query(test_context, people, "name.@min.hours_required <= 41", 2));
    CHECK_THROW_ANY(verify_query(test_context, people, "name.@max.hours_required <= 41", 2));
    CHECK_THROW_ANY(verify_query(test_context, people, "name.@sum.hours_required <= 41", 2));
    CHECK_THROW_ANY(verify_query(test_context, people, "name.@avg.hours_required <= 41", 2));
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, people, "name.@min.hours_required <= 41", 2), message);
    CHECK(message.find("list") != std::string::npos);
    CHECK(message.find("name") != std::string::npos);

    // size and count do not allow paths on the destination object
    CHECK_THROW_ANY(verify_query(test_context, people, "name.@count.hours_required <= 2", 0));
    CHECK_THROW_ANY(verify_query(test_context, people, "name.@size.hours_required <= 2", 0));

    // size is only allowed on certain types
    CHECK_THROW_ANY(verify_query(test_context, people, "age.@size <= 2", 0));
    CHECK_THROW_ANY(verify_query(test_context, courses, "credits.@size == 2", 0));
    CHECK_THROW_ANY(verify_query(test_context, courses, "failure_percentage.@size <= 2", 0));
}


TEST(Parser_SortAndDistinctSerialisation)
{
    Group g;
    TableRef people = g.add_table("person");
    TableRef accounts = g.add_table("account");

    size_t name_col = people->add_column(type_String, "name");
    size_t age_col = people->add_column(type_Int, "age");
    size_t account_col = people->add_column_link(type_Link, "account", *accounts);

    size_t balance_col = accounts->add_column(type_Double, "balance");
    size_t transaction_col = accounts->add_column(type_Int, "num_transactions");

    accounts->add_empty_row(3);
    accounts->set_double(balance_col, 0, 50.55);
    accounts->set_int(transaction_col, 0, 2);
    accounts->set_double(balance_col, 1, 175.23);
    accounts->set_int(transaction_col, 1, 73);
    accounts->set_double(balance_col, 2, 98.92);
    accounts->set_int(transaction_col, 2, 17);

    people->add_empty_row(3);
    people->set_string(name_col, 0, "Adam");
    people->set_int(age_col, 0, 28);
    people->set_link(account_col, 0, 0);
    people->set_string(name_col, 1, "Frank");
    people->set_int(age_col, 1, 30);
    people->set_link(account_col, 1, 1);
    people->set_string(name_col, 2, "Ben");
    people->set_int(age_col, 2, 18);
    people->set_link(account_col, 2, 2);

    // person:                      | account:
    // name     age     account     | balance       num_transactions
    // Adam     28      0 ->        | 50.55         2
    // Frank    30      1 ->        | 175.23        73
    // Ben      18      2 ->        | 98.92         17

    // sort serialisation
    TableView tv = people->where().find_all();
    tv.sort(name_col, false);
    tv.sort(age_col, true);
    tv.sort(SortDescriptor(*people, {{account_col, balance_col}, {account_col, transaction_col}}, {true, false}));
    std::string description = tv.get_descriptor_ordering_description();
    CHECK(description.find("SORT(account.balance ASC, account.num_transactions DESC, age ASC, name DESC)") != std::string::npos);

    // distinct serialisation
    tv = people->where().find_all();
    tv.distinct(name_col);
    tv.distinct(age_col);
    tv.distinct(DistinctDescriptor(*people, {{account_col, balance_col}, {account_col, transaction_col}}));
    description = tv.get_descriptor_ordering_description();
    CHECK(description.find("DISTINCT(name) DISTINCT(age) DISTINCT(account.balance, account.num_transactions)") != std::string::npos);

    // combined sort and distinct serialisation
    tv = people->where().find_all();
    tv.distinct(DistinctDescriptor(*people, {{name_col}, {age_col}}));
    tv.sort(SortDescriptor(*people, {{account_col, balance_col}, {account_col, transaction_col}}, {true, false}));
    description = tv.get_descriptor_ordering_description();
    CHECK(description.find("DISTINCT(name, age)") != std::string::npos);
    CHECK(description.find("SORT(account.balance ASC, account.num_transactions DESC)") != std::string::npos);
}

TableView get_sorted_view(TableRef t, std::string query_string)
{
    Query q = t->where();

    parser::ParserResult result = realm::parser::parse(query_string);
    realm::query_builder::apply_predicate(q, result.predicate);
    DescriptorOrdering ordering;
    realm::query_builder::apply_ordering(ordering, t, result.ordering);

    std::string query_description = q.get_description();
    std::string ordering_description = ordering.get_description(t);
    std::string combined = query_description + " " + ordering_description;

    //std::cerr << "original: " << query_string << "\tdescribed: " << combined << "\n";
    Query q2 = t->where();

    parser::ParserResult result2 = realm::parser::parse(combined);
    realm::query_builder::apply_predicate(q2, result2.predicate);
    DescriptorOrdering ordering2;
    realm::query_builder::apply_ordering(ordering2, t, result2.ordering);

    TableView tv = q2.find_all();
    tv.apply_descriptor_ordering(ordering2);
    return tv;
}

TEST(Parser_SortAndDistinct)
{
    Group g;
    TableRef people = g.add_table("person");
    TableRef accounts = g.add_table("account");

    size_t name_col = people->add_column(type_String, "name");
    size_t age_col = people->add_column(type_Int, "age");
    size_t account_col = people->add_column_link(type_Link, "account", *accounts);

    size_t balance_col = accounts->add_column(type_Double, "balance");
    size_t transaction_col = accounts->add_column(type_Int, "num_transactions");

    accounts->add_empty_row(3);
    accounts->set_double(balance_col, 0, 50.55);
    accounts->set_int(transaction_col, 0, 2);
    accounts->set_double(balance_col, 1, 50.55);
    accounts->set_int(transaction_col, 1, 73);
    accounts->set_double(balance_col, 2, 98.92);
    accounts->set_int(transaction_col, 2, 17);

    people->add_empty_row(3);
    people->set_string(name_col, 0, "Adam");
    people->set_int(age_col, 0, 28);
    people->set_link(account_col, 0, 0);
    people->set_string(name_col, 1, "Frank");
    people->set_int(age_col, 1, 30);
    people->set_link(account_col, 1, 1);
    people->set_string(name_col, 2, "Ben");
    people->set_int(age_col, 2, 28);
    people->set_link(account_col, 2, 2);

    // person:                      | account:
    // name     age     account     | balance       num_transactions
    // Adam     28      0 ->        | 50.55         2
    // Frank    30      1 ->        | 50.55         73
    // Ben      28      2 ->        | 98.92         17

    // sort serialisation
    TableView tv = get_sorted_view(people, "age > 0 SORT(age ASC)");
    for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
        CHECK(tv.get_int(age_col, row_ndx - 1) <= tv.get_int(age_col, row_ndx));
    }

    tv = get_sorted_view(people, "age > 0 SORT(age DESC)");
    for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
        CHECK(tv.get_int(age_col, row_ndx - 1) >= tv.get_int(age_col, row_ndx));
    }

    tv = get_sorted_view(people, "age > 0 SORT(age ASC, name DESC)");
    CHECK_EQUAL(tv.size(), 3);
    CHECK_EQUAL(tv.get_string(name_col, 0), "Ben");
    CHECK_EQUAL(tv.get_string(name_col, 1), "Adam");
    CHECK_EQUAL(tv.get_string(name_col, 2), "Frank");

    tv = get_sorted_view(people, "TRUEPREDICATE SORT(account.balance ascending)");
    for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
        size_t link_ndx1 = tv.get_link(account_col, row_ndx - 1);
        size_t link_ndx2 = tv.get_link(account_col, row_ndx);
        CHECK(accounts->get_double(balance_col, link_ndx1) <= accounts->get_double(balance_col, link_ndx2));
    }

    tv = get_sorted_view(people, "TRUEPREDICATE SORT(account.balance descending)");
    for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
        size_t link_ndx1 = tv.get_link(account_col, row_ndx - 1);
        size_t link_ndx2 = tv.get_link(account_col, row_ndx);
        CHECK(accounts->get_double(balance_col, link_ndx1) >= accounts->get_double(balance_col, link_ndx2));
    }

    tv = get_sorted_view(people, "TRUEPREDICATE DISTINCT(age)");
    CHECK_EQUAL(tv.size(), 2);
    for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
        CHECK(tv.get_int(age_col, row_ndx - 1) != tv.get_int(age_col, row_ndx));
    }

    tv = get_sorted_view(people, "TRUEPREDICATE DISTINCT(age, account.balance)");
    CHECK_EQUAL(tv.size(), 3);
    CHECK_EQUAL(tv.get_string(name_col, 0), "Adam");
    CHECK_EQUAL(tv.get_string(name_col, 1), "Frank");
    CHECK_EQUAL(tv.get_string(name_col, 2), "Ben");

    tv = get_sorted_view(people, "TRUEPREDICATE DISTINCT(age) DISTINCT(account.balance)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_string(name_col, 0), "Adam");

    tv = get_sorted_view(people, "TRUEPREDICATE SORT(age ASC) DISTINCT(age)");
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_int(age_col, 0), 28);
    CHECK_EQUAL(tv.get_int(age_col, 1), 30);

    tv = get_sorted_view(people, "TRUEPREDICATE SORT(name DESC) DISTINCT(age) SORT(name ASC) DISTINCT(name)");
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_string(name_col, 0), "Ben");
    CHECK_EQUAL(tv.get_string(name_col, 1), "Frank");

    tv = get_sorted_view(people, "account.num_transactions > 10 SORT(name ASC)");
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_string(name_col, 0), "Ben");
    CHECK_EQUAL(tv.get_string(name_col, 1), "Frank");

    std::string message;
    CHECK_THROW_ANY_GET_MESSAGE(get_sorted_view(people, "TRUEPREDICATE DISTINCT(balance)"), message);
    CHECK_EQUAL(message, "No property 'balance' found on object type 'person' specified in 'distinct' clause");

    CHECK_THROW_ANY_GET_MESSAGE(get_sorted_view(people, "TRUEPREDICATE sort(account.name ASC)"), message);
    CHECK_EQUAL(message, "No property 'name' found on object type 'account' specified in 'sort' clause");
}


TEST(Parser_BacklinkSerialisation)
{
    Group g;

    TableRef items = g.add_table("class_Items");
    size_t item_name_col = items->add_column(type_String, "name");
    size_t item_price_col = items->add_column(type_Double, "price");
    using item_t = std::pair<std::string, double>;
    std::vector<item_t> item_info = {{"milk", 5.5}, {"oranges", 4.0}, {"pizza", 9.5}, {"cereal", 6.5}};
    for (item_t i : item_info) {
        size_t row_ndx = items->add_empty_row();
        items->set_string(item_name_col, row_ndx, i.first);
        items->set_double(item_price_col, row_ndx, i.second);
    }

    TableRef t = g.add_table("class_Person");
    size_t id_col_ndx = t->add_column(type_Int, "customer_id");
    size_t account_col_ndx = t->add_column(type_Double, "account_balance");
    size_t items_col_ndx = t->add_column_link(type_LinkList, "items", *items);
    size_t fav_col_ndx = t->add_column_link(type_Link, "fav_item", *items);
    t->add_empty_row(3);
    for (size_t i = 0; i < t->size(); ++i) {
        t->set_int(id_col_ndx, i, i);
        t->set_double(account_col_ndx, i, double((i + 1) * 10.0));
        t->set_link(fav_col_ndx, i, i);
    }

    LinkViewRef list_0 = t->get_linklist(items_col_ndx, 0);
    list_0->add(0);
    list_0->add(1);
    list_0->add(2);
    list_0->add(3);

    LinkViewRef list_1 = t->get_linklist(items_col_ndx, 1);
    for (size_t i = 0; i < 10; ++i) {
        list_1->add(0);
    }

    LinkViewRef list_2 = t->get_linklist(items_col_ndx, 2);
    list_2->add(2);
    list_2->add(2);
    list_2->add(3);

    Query q = items->backlink(*t, fav_col_ndx).column<Double>(account_col_ndx) > 20;
    CHECK_EQUAL(q.count(), 1);
    std::string desc = q.get_description();
    CHECK(desc.find("@links.class_Person.fav_item.account_balance") != std::string::npos);

    q = items->backlink(*t, items_col_ndx).column<Double>(account_col_ndx) > 20;
    CHECK_EQUAL(q.count(), 2);
    desc = q.get_description();
    CHECK(desc.find("@links.class_Person.items.account_balance") != std::string::npos);

}


TEST(Parser_SubqueryVariableNames)
{
    Group g;
    util::serializer::SerialisationState test_state;

    TableRef test_table = g.add_table("test");

    CHECK_EQUAL(test_state.get_variable_name(test_table), "$x");

    for (char c = 'a'; c <= 'z'; ++c) {
        std::string col_name = std::string("$") + c;
        test_table->add_column(type_Int, col_name);
    }
    test_state.subquery_prefix_list.push_back("$xx");
    test_state.subquery_prefix_list.push_back("$xy");
    test_state.subquery_prefix_list.push_back("$xz");
    test_state.subquery_prefix_list.push_back("$xa");

    std::string unique_variable = test_state.get_variable_name(test_table);

    CHECK_EQUAL(unique_variable, "$xb");
}


TEST(Parser_Subquery)
{
    Group g;

    TableRef discounts = g.add_table("class_Discounts");
    size_t discount_name_col = discounts->add_column(type_String, "promotion", true);
    size_t discount_off_col = discounts->add_column(type_Double, "reduced_by");
    size_t discount_active_col = discounts->add_column(type_Bool, "active");

    using discount_t = std::pair<double, bool>;
    std::vector<discount_t> discount_info = {{3.0, false}, {2.5, true}, {0.50, true}, {1.50, true}};
    for (discount_t i : discount_info) {
        size_t row_ndx = discounts->add_empty_row();
        discounts->set_double(discount_off_col, row_ndx, i.first);
        discounts->set_bool(discount_active_col, row_ndx, i.second);
    }
    discounts->set_string(discount_name_col, 0, "back to school");
    discounts->set_string(discount_name_col, 1, "pizza lunch special");
    discounts->set_string(discount_name_col, 2, "manager's special");

    TableRef ingredients = g.add_table("class_Allergens");
    size_t ingredient_name_col = ingredients->add_column(type_String, "name");
    size_t population_col = ingredients->add_column(type_Double, "population_affected");
    std::vector<std::pair<std::string, double>> ingredients_list = { {"dairy", 0.75}, {"nuts", 0.01}, {"wheat", 0.01}, {"soy", 0.005} };
    for (size_t i = 0; i < ingredients_list.size(); ++i) {
        size_t row_ndx = ingredients->add_empty_row();
        ingredients->set_string(ingredient_name_col, row_ndx, ingredients_list[i].first);
        ingredients->set_double(population_col, row_ndx, ingredients_list[i].second);
    }

    TableRef items = g.add_table("class_Items");
    size_t item_name_col = items->add_column(type_String, "name");
    size_t item_price_col = items->add_column(type_Double, "price");
    size_t item_discount_col = items->add_column_link(type_Link, "discount", *discounts);
    size_t item_contains_col = items->add_column_link(type_LinkList, "allergens", *ingredients);
    using item_t = std::pair<std::string, double>;
    std::vector<item_t> item_info = {{"milk", 5.5}, {"oranges", 4.0}, {"pizza", 9.5}, {"cereal", 6.5}};
    for (item_t i : item_info) {
        size_t row_ndx = items->add_empty_row();
        items->set_string(item_name_col, row_ndx, i.first);
        items->set_double(item_price_col, row_ndx, i.second);
    }
    items->set_link(item_discount_col, 0, 2); // milk -0.50
    items->set_link(item_discount_col, 2, 1); // pizza -2.5
    items->set_link(item_discount_col, 3, 0); // cereal -3.0 inactive
    LinkViewRef milk_contains = items->get_linklist(item_contains_col, 0);
    milk_contains->add(0);
    LinkViewRef pizza_contains = items->get_linklist(item_contains_col, 2);
    pizza_contains->add(0);
    pizza_contains->add(2);
    pizza_contains->add(3);
    LinkViewRef cereal_contains = items->get_linklist(item_contains_col, 3);
    cereal_contains->add(0);
    cereal_contains->add(1);
    cereal_contains->add(2);

    TableRef t = g.add_table("class_Person");
    size_t id_col_ndx = t->add_column(type_Int, "customer_id");
    size_t account_col_ndx = t->add_column(type_Double, "account_balance");
    size_t items_col_ndx = t->add_column_link(type_LinkList, "items", *items);
    size_t fav_col_ndx = t->add_column_link(type_Link, "fav_item", *items);
    t->add_empty_row(3);
    for (size_t i = 0; i < t->size(); ++i) {
        t->set_int(id_col_ndx, i, i);
        t->set_double(account_col_ndx, i, double((i + 1) * 10.0));
        t->set_link(fav_col_ndx, i, i);
    }

    LinkViewRef list_0 = t->get_linklist(items_col_ndx, 0);
    list_0->add(0);
    list_0->add(1);
    list_0->add(2);
    list_0->add(3);

    LinkViewRef list_1 = t->get_linklist(items_col_ndx, 1);
    for (size_t i = 0; i < 10; ++i) {
        list_1->add(0);
    }

    LinkViewRef list_2 = t->get_linklist(items_col_ndx, 2);
    list_2->add(2);
    list_2->add(2);
    list_2->add(3);

    Query q = t->column<LinkList>(items_col_ndx, items->column<String>(item_name_col).contains("a")
                                  && items->column<Double>(item_price_col) > 5.0
                                  && items->link(item_discount_col).column<Double>(discount_off_col) > 0.5
                                  && items->column<Link>(item_contains_col).count() > 1).count() > 1;

    std::string subquery_description = q.get_description();
    CHECK(subquery_description.find("SUBQUERY(items, $x,") != std::string::npos);
    CHECK(subquery_description.find(" $x.name ") != std::string::npos);
    CHECK(subquery_description.find(" $x.price ") != std::string::npos);
    CHECK(subquery_description.find(" $x.discount.reduced_by ") != std::string::npos);
    CHECK(subquery_description.find(" $x.allergens.@count") != std::string::npos);
    TableView tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);

    // not variations inside/outside subquery, no variable substitution
    verify_query(test_context, t, "SUBQUERY(items, $x, TRUEPREDICATE).@count > 0", 3);
    verify_query(test_context, t, "!SUBQUERY(items, $x, TRUEPREDICATE).@count > 0", 0);
    verify_query(test_context, t, "SUBQUERY(items, $x, !TRUEPREDICATE).@count > 0", 0);
    verify_query(test_context, t, "SUBQUERY(items, $x, FALSEPREDICATE).@count == 0", 3);
    verify_query(test_context, t, "!SUBQUERY(items, $x, FALSEPREDICATE).@count == 0", 0);
    verify_query(test_context, t, "SUBQUERY(items, $x, !FALSEPREDICATE).@count == 0", 0);

    // simple variable substitution
    verify_query(test_context, t, "SUBQUERY(items, $x, 5.5 == $x.price ).@count > 0", 2);
    // string constraint subquery
    verify_query(test_context, t, "SUBQUERY(items, $x, $x.name CONTAINS[c] 'MILK').@count >= 1", 2);
    // compound subquery &&
    verify_query(test_context, t, "SUBQUERY(items, $x, $x.name CONTAINS[c] 'MILK' && $x.price == 5.5).@count >= 1", 2);
    // compound subquery ||
    verify_query(test_context, t, "SUBQUERY(items, $x, $x.name CONTAINS[c] 'MILK' || $x.price >= 5.5).@count >= 1", 3);
    // variable name change
    verify_query(test_context, t, "SUBQUERY(items, $anyNAME_-0123456789, 5.5 == $anyNAME_-0123456789.price ).@count > 0", 2);
    // variable names cannot contain '.'
    CHECK_THROW_ANY(verify_query(test_context, t, "SUBQUERY(items, $x.y, 5.5 == $x.y.price ).@count > 0", 2));
    // variable name must begin with '$'
    CHECK_THROW_ANY(verify_query(test_context, t, "SUBQUERY(items, x, 5.5 == x.y.price ).@count > 0", 2));
    // subquery with string size
    verify_query(test_context, t, "SUBQUERY(items, $x, $x.name.@size == 4).@count > 0", 2);
    // subquery with list count
    verify_query(test_context, t, "SUBQUERY(items, $x, $x.allergens.@count > 1).@count > 0", 2);
    // subquery with list aggregate operation
    verify_query(test_context, t, "SUBQUERY(items, $x, $x.allergens.@min.population_affected < 0.10).@count > 0", 2);
    verify_query(test_context, t, "SUBQUERY(items, $x, $x.allergens.@max.population_affected > 0.50).@count > 0", 3);
    verify_query(test_context, t, "SUBQUERY(items, $x, $x.allergens.@sum.population_affected > 0.75).@count > 0", 2);
    verify_query(test_context, t, "SUBQUERY(items, $x, $x.allergens.@avg.population_affected > 0.50).@count > 0", 2);
    // two column subquery
    verify_query(test_context, t, "SUBQUERY(items, $x, $x.discount.promotion CONTAINS[c] $x.name).@count > 0", 2);
    // subquery count (int) vs double
    verify_query(test_context, t, "SUBQUERY(items, $x, $x.discount.promotion CONTAINS[c] $x.name).@count < account_balance", 3);
    // subquery over link
    verify_query(test_context, t, "SUBQUERY(fav_item.allergens, $x, $x.name CONTAINS[c] 'dairy').@count > 0", 2);
    // nested subquery
    verify_query(test_context, t, "SUBQUERY(items, $x, SUBQUERY($x.allergens, $allergy, $allergy.name CONTAINS[c] 'dairy').@count > 0).@count > 0", 3);
    // target property must be a list
    std::string message;
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "SUBQUERY(account_balance, $x, TRUEPREDICATE).@count > 0", 3), message);
    CHECK_EQUAL(message, "A subquery must operate on a list property, but 'account_balance' is type 'Double'");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "SUBQUERY(fav_item, $x, TRUEPREDICATE).@count > 0", 3), message);
    CHECK_EQUAL(message, "A subquery must operate on a list property, but 'fav_item' is type 'Link'");
}

#endif // TEST_PARSER
