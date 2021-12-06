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

#include <realm.hpp>
#include <realm/parser/keypath_mapping.hpp>
#include <realm/parser/query_parser.hpp>
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

#include <realm.hpp>
#include <realm/history.hpp>
#include <realm/query_expression.hpp>
#include <realm/replication.hpp>
#include <realm/util/any.hpp>
#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/to_string.hpp>
#include "test_table_helper.hpp"
#include "test_types_helper.hpp"

#include <chrono>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <set>

using namespace realm;
using namespace realm::metrics;
using namespace realm::test_util;
using namespace realm::util;

// clang-format off
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
    "stringCol == \"\\\"\\n\\0\\r\\\\'\"",
    "'\\uffFf' = '\\u0020'",
    "'\\u01111' = 'asdf\\u0111asdf'",

    // utf8
    "你好=5",

    // expressions (numbers, bools, keypaths, arguments)
    "-1 = 12",
    "0 = 001",
    "0x0 = 0X398235fcAb",
    "10. = -.034",
    "10.0 = 5.034",
    "true = false",
    "true\\ love = false",
    "true = falsey",
    "nullified = null",
    "nullified = nil",
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
    "a BETWEEN {4, 5}",
    "sort > 0",
    "distinct > 0",
    "limit > 0",
    "0 contains 0",
    "a CONTAINS[c] b",
    "a contains [c] b",
    "'a'CONTAINS[c]b",
    "0 BeGiNsWiTh 0",
    "0 ENDSWITH 0",
    "contains contains 'contains'",
    "beginswith beginswith 'beginswith'",
    "endswith endswith 'endswith'",
    // "NOT NOT != 'NOT'",
    // "AND == 'AND' AND OR == 'OR'",
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
    "NOT0=0",    // keypath NOT0
    "NOT0.a=0",  // keypath NOT0
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
    // "and=='AND'&&'or'=='||'",
    // "and == or && ORE > GRAND",
    "a=1AND NOTb=2",

    // sort/distinct
    "a=b SORT(p ASCENDING)",
    "TRUEPREDICATE SORT(sort ASCENDING)",
    "TRUEPREDICATE SORT(distinct ASCENDING)",
    "TRUEPREDICATE SORT(limit ASC)",
    "TRUEPREDICATE SORT(sort ASC, distinct ASC, limit ASC)",
    "TRUEPREDICATE DISTINCT(disstinct)",
    "TRUEPREDICATE DISTINCT(sort)",
    "TRUEPREDICATE DISTINCT(limit)",
    "TRUEPREDICATE DISTINCT(sort, distinct, limit)",
    "TRUEPREDICATE SORT(sort ASC, distinct ASC, limit ASC) DISTINCT(sort, distinct, limit) LIMIT(1)",
    "TRUEPREDICATE LIMIT(1)",
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
    "a == b  and c==d sort(a   ASC, b DESC)   DISTINCT( p )  sort( c   ASC  ,  d  DESC  ) DISTINCT(q.r ,   p)   ",

    // limit
    "a=b LIMIT(1)",
    "a=b LIMIT ( 1 )",
    "a=b LIMIT( 1234567890 )",
    "a=b && c=d LIMIT(1)",
    "a=b && c=d || e=f LIMIT(1)",
    "a=b LIMIT(1) SORT(a ASC) DISTINCT(b)",
    "a=b SORT(a ASC) LIMIT(1) DISTINCT(b)",
    "a=b SORT(a ASC) DISTINCT(b) LIMIT(1)",
    "a=b LIMIT(2) LIMIT(1)",
    "a=b && c=d LIMIT(5) LIMIT(2)",
    "a=b LIMIT(5) SORT(age ASC) DISTINCT(name) LIMIT(2)",

    /*
    // include
    "a=b INCLUDE(c)",
    "a=b include(c,d)",
    "a=b INCLUDE(c.d)",
    "a=b INCLUDE(c.d.e, f.g, h)",
    "a=b INCLUDE ( c )",
    "a=b INCLUDE(d, e, f    , g )",
    "a=b INCLUDE(c) && d=f",
    "a=b INCLUDE(c) INCLUDE(d)",
    "a=b && c=d || e=f INCLUDE(g)",
    "a=b LIMIT(5) SORT(age ASC) DISTINCT(name) INCLUDE(links1, links2)",
    "a=b INCLUDE(links1, links2) LIMIT(5) SORT(age ASC) DISTINCT(name)",
     */
    // subquery expression
    "SUBQUERY(items, $x, $x.name == 'Tom').@size > 0",
    "SUBQUERY(items, $x, $x.name == 'Tom').@count > 0",
    "SUBQUERY(items, $x, $x.allergens.@min.population_affected < 0.10).@count > 0",
    "SUBQUERY(items, $x, $x.name == 'Tom').@count == SUBQUERY(items, $x, $x.price < 10).@count",

    // backlinks
    "p.@links.class.prop.@count > 2",
    "p.@links.class.prop.@sum.prop2 > 2",
};

static std::vector<std::string> invalid_queries = {
    "predicate",
    "'\\a' = ''", // invalid escape

    // invalid unicode
    "'\\u0' = ''",

    // invalid strings
    "\"' = ''",
    "\" = ''",
    "' = ''",

    // invalid property names
    "stone#age = 5",
    "true\\flove = false",

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
    "a between {}",
    "a between {1 2}",
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
    // "(!!0=0)",
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
    "SORT(p ASCENDING)",                      // no query conditions
    "a=b SORT(p)",                            // no asc/desc
    "a=b SORT(0 Descending)",                 // bad keypath
    "a=b sort()",                             // missing condition
    "a=b sort",                               // no target property
    "distinct(p)",                            // no query condition
    "a=b DISTINCT()",                         // no target property
    "a=b Distinct",                           // no target property
    "sort(a ASC b, DESC) a == b",             // before query condition
    "sort(a ASC b, DESC) a == b sort(c ASC)", // before query condition
    "a=bDISTINCT(p)",                         // bad spacing
    "a=b sort p.q desc",                      // no braces
    "a=b sort(p.qDESC)",                      // bad spacing
    "a=b DISTINCT p",                         // no braces
    "a=b SORT(p ASC",                         // bad braces
    "a=b DISTINCT(p",                         // no braces
    "a=b sort(p.q DESC a ASC)",               // missing comma
    "a=b DISTINCT(p q)",                      // missing comma

    // limit
    "LIMIT(1)",          // no query conditions
    "a=b LIMIT",         // no params
    "a=b LIMIT()",       // no params
    "a=b LIMIT(2",       // missing end paren
    "a=b LIMIT2)",       // missing open paren
    "a=b LIMIT(-1)",     // negative limit
    "a=b LIMIT(2.7)",    // input must be an integer
    "a=b LIMIT(0xFFEE)", // input must be an integer
    "a=b LIMIT(word)",   // non numeric limit
    "a=b LIMIT(11asdf)", // non numeric limit
    "a=b LIMIT(1, 1)",   // only accept one input

    // include
    "INCLUDE(a)",         // no query conditions
    "a=b INCLUDE",        // no parameters
    "a=b INCLUDE()",      // empty params
    "a=b INCLUDE(a",      // missing end paren
    "a=b INCLUDEb)",      // missing open paren
    "a=b INCLUDE(1)",     // numeric input
    "a=b INCLUDE(a,)",    // missing param
    "a=b INCLUDE(,a)",    // missing param
    "a=b INCLUDE(a.)",    // incomplete keypath
    "a=b INCLUDE(a b)",   // missing comma
    "a=b INCLUDE(a < b)", // parameters should not be a predicate

    // subquery
    "SUBQUERY(items, $x, $x.name == 'Tom') > 0",        // missing .@count
    "SUBQUERY(items, $x, $x.name == 'Tom').@min > 0",   // @min not yet supported
    "SUBQUERY(items, $x, $x.name == 'Tom').@max > 0",   // @max not yet supported
    "SUBQUERY(items, $x, $x.name == 'Tom').@sum > 0",   // @sum not yet supported
    "SUBQUERY(items, $x, $x.name == 'Tom').@avg > 0",   // @avg not yet supported
    "SUBQUERY(items, var, var.name == 'Tom').@avg > 0", // variable must start with '$'
    "SUBQUERY(, $x, $x.name == 'Tom').@avg > 0",        // a target keypath is required
    "SUBQUERY(items, , name == 'Tom').@avg > 0",        // a variable name is required
    "SUBQUERY(items, $x, ).@avg > 0",                   // the subquery is required

    // no @ allowed in keypaths except for keyword '@links'
    "@prop > 2",
    "@backlinks.@count > 2",
    "prop@links > 2",
};
// clang-format on

TEST(Parser_valid_queries)
{
    for (auto& query : valid_queries) {
        // std::cout << "query: " << query << std::endl;
        realm::query_parser::parse(query);
    }
}

TEST(Parser_invalid_queries)
{
    for (auto& query : invalid_queries) {
        // std::cout << "query: " << query << std::endl;
        CHECK_THROW(realm::query_parser::parse(query), realm::query_parser::SyntaxError);
    }
}

Query verify_query(test_util::unit_test::TestContext& test_context, TableRef t, std::string query_string,
                   size_t num_results, query_parser::KeyPathMapping mapping = {})
{
    realm::query_parser::NoArguments args;
    Query q = t->query(query_string, args, mapping);

    size_t q_count = q.count();
    CHECK_EQUAL(q_count, num_results);
    std::string description = q.get_description(mapping.get_backlink_class_prefix());
    // std::cerr << "original: " << query_string << "\tdescribed: " << description << "\n";
    Query q2 = t->query(description, args, mapping);

    size_t q2_count = q2.count();
    CHECK_EQUAL(q2_count, num_results);
    if (q_count != num_results || q2_count != num_results) {
        std::cout << "the query for the above failure is: '" << description << "'" << std::endl;
    }
    return q2;
}

void verify_query_sub(test_util::unit_test::TestContext& test_context, TableRef t, std::string query_string,
                      const util::Any* arg_list, size_t num_args, size_t num_results)
{
    query_parser::AnyContext ctx;
    realm::query_parser::ArgumentConverter<util::Any, query_parser::AnyContext> args(ctx, arg_list, num_args);

    Query q = t->query(query_string, args, {});

    size_t q_count = q.count();
    CHECK_EQUAL(q_count, num_results);
    std::string description = q.get_description();
    // std::cerr << "original: " << query_string << "\tdescribed: " << description << "\n";
    Query q2 = t->query(description, args, {});

    size_t q2_count = q2.count();
    CHECK_EQUAL(q2_count, num_results);
    if (q_count != num_results || q2_count != num_results) {
        std::cout << "the query for the above failure is: '" << description << "'" << std::endl;
    }
}

void verify_query_sub(test_util::unit_test::TestContext& test_context, TableRef t, std::string query_string,
                      std::vector<Mixed> args, size_t num_results)
{
    Query q = t->query(query_string, args, {});
    size_t q_count = q.count();
    CHECK_EQUAL(q_count, num_results);
    std::string description = q.get_description();
    // std::cerr << "original: " << query_string << "\tdescribed: " << description << "\n";
    Query q2 = t->query(description, args, {});

    size_t q2_count = q2.count();
    CHECK_EQUAL(q2_count, num_results);
    if (q_count != num_results || q2_count != num_results) {
        std::cout << "the query for the above failure is: '" << description << "'" << std::endl;
    }
}

TEST(Parser_empty_input)
{
    Group g;
    std::string table_name = "table";
    TableRef t = g.add_table(table_name);
    t->add_column(type_Int, "int_col");
    std::vector<ObjKey> keys;
    t->create_objects(5, keys);

    // an empty query string is an invalid predicate
    CHECK_THROW(verify_query(test_context, t, "", 5), realm::query_parser::SyntaxError);

    Query q = t->where(); // empty query
    std::string empty_description = q.get_description();
    CHECK(!empty_description.empty());
    CHECK_EQUAL(0, empty_description.compare("TRUEPREDICATE"));

    q = t->query(empty_description);
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
    auto int_col = t->add_column(type_Int, "age");
    auto list_col = t->add_column_list(*t, "self_list");

    Obj obj0 = t->create_object();
    Obj obj1 = t->create_object();

    obj1.set(int_col, 1);

    auto list_0 = obj0.get_linklist(list_col);
    list_0.add(obj0.get_key());
    list_0.add(obj1.get_key());

    TableView tv = obj0.get_backlink_view(t, list_col);
    Query q(t, &tv);
    CHECK_EQUAL(q.count(), 1);
    q.and_query(t->column<Int>(int_col) <= 0);
    CHECK_EQUAL(q.count(), 1);
    CHECK_THROW(q.get_description(), SerialisationError);

    Query q2(t, list_0);
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
    auto int_col_key = t->add_column(type_Int, "age");
    t->add_column(type_String, "name");
    t->add_column(type_Double, "fees", true);
    t->add_column(type_Float, "float fees", true);
    t->add_column(type_Bool, "licensed", true);
    auto link_col = t->add_column(*t, "buddy");
    auto time_col = t->add_column(type_Timestamp, "time", true);
    t->add_search_index(int_col_key);
    std::vector<std::string> names = {"Billy", "Bob", "Joe", "Jane", "Joel"};
    std::vector<double> fees = {2.0, 2.23, 2.22, 2.25, 3.73};
    std::vector<ObjKey> keys;

    t->create_objects(5, keys);
    for (size_t i = 0; i < t->size(); ++i) {
        t->get_object(keys[i]).set_all(int(i), StringData(names[i]), fees[i], float(fees[i]), (i % 2 == 0));
    }
    t->get_object(keys[0]).set(time_col, Timestamp(realm::null()));
    t->get_object(keys[1]).set(time_col, Timestamp(1512130073, 0));   // 2017/12/02 @ 12:47am (UTC)
    t->get_object(keys[2]).set(time_col, Timestamp(1512130073, 505)); // with nanoseconds
    t->get_object(keys[3]).set(time_col, Timestamp(1, 2));
    t->get_object(keys[4]).set(time_col, Timestamp(0, 0));
    t->get_object(keys[0]).set(link_col, keys[1]);

    Query q = t->where();

    verify_query(test_context, t, "time == NULL", 1);
    verify_query(test_context, t, "time == NIL", 1);
    verify_query(test_context, t, "time != NULL", 4);
    verify_query(test_context, t, "time > T0:0", 3);
    verify_query(test_context, t, "time == T1:2", 1);
    verify_query(test_context, t, "time > 2017-12-1@12:07:53", 1);
    verify_query(test_context, t, "time == 2017-12-01@12:07:53:505", 1);
    verify_query(test_context, t, "buddy == NULL", 4);
    verify_query(test_context, t, "buddy == nil", 4);
    verify_query(test_context, t, "buddy != NULL", 1);
    verify_query(test_context, t, "buddy <> NULL", 1);
    verify_query(test_context, t, "buddy.name == NULL", 4); // matches null links
    verify_query(test_context, t, "buddy.age == NULL", 4);
    verify_query(test_context, t, "age > 2", 2);
    verify_query(test_context, t, "!(age >= 2)", 2);
    verify_query(test_context, t, "!(age => 2)", 2);
    verify_query(test_context, t, "3 <= age", 2);
    verify_query(test_context, t, "3 =< age", 2);
    verify_query(test_context, t, "age > 2 and age < 4", 1);
    verify_query(test_context, t, "age = 1 || age == 3", 2);
    verify_query(test_context, t, "fees = 1.2 || fees = 2.23", 1);
    verify_query(test_context, t, "fees = 2 || fees = 3", 1);
    verify_query(test_context, t, "fees BETWEEN {2, 3}", 3);
    verify_query(test_context, t, "fees BETWEEN {2.20, 2.25}", 2);
    verify_query(test_context, t, "fees = 2 || fees = 3 || fees = 4", 1);
    verify_query(test_context, t, "fees = 0 || fees = 1", 0);

    verify_query(test_context, t, "fees != 2.22 && fees > 2.2", 3);
    verify_query(test_context, t, "fees > 2.0E0", 4);
    verify_query(test_context, t, "fees > 200e-2", 4);
    verify_query(test_context, t, "fees > 0.002e3", 4);
    verify_query(test_context, t, "fees < inf", 5);
    verify_query(test_context, t, "fees < +inf", 5);
    verify_query(test_context, t, "fees > -iNf", 5);
    verify_query(test_context, t, "fees < Infinity", 5);
    verify_query(test_context, t, "fees < +inFINITY", 5);
    verify_query(test_context, t, "fees > -INFinity", 5);
    verify_query(test_context, t, "fees == NaN", 0);
    verify_query(test_context, t, "fees != Nan", 5);
    verify_query(test_context, t, "fees == -naN", 0);
    verify_query(test_context, t, "fees != -nAn", 5);
    verify_query(test_context, t, "float\\ fees > 2.0E0", 4);
    verify_query(test_context, t, "float\\ fees > 200e-2", 4);
    verify_query(test_context, t, "float\\ fees > 0.002E3", 4);
    verify_query(test_context, t, "float\\ fees < INF", 5);
    verify_query(test_context, t, "float\\ fees < +InF", 5);
    verify_query(test_context, t, "float\\ fees > -inf", 5);
    verify_query(test_context, t, "float\\ fees < InFiNiTy", 5);
    verify_query(test_context, t, "float\\ fees < +iNfInItY", 5);
    verify_query(test_context, t, "float\\ fees > -infinity", 5);
    verify_query(test_context, t, "float\\ fees == NAN", 0);
    verify_query(test_context, t, "float\\ fees != nan", 5);
    verify_query(test_context, t, "float\\ fees == -NaN", 0);
    verify_query(test_context, t, "float\\ fees != -NAn", 5);
    verify_query(test_context, t, "(age > 1 || fees >= 2.25) && age == 4", 1);
    verify_query(test_context, t, "licensed == true", 3);
    verify_query(test_context, t, "licensed == false", 2);
    verify_query(test_context, t, "licensed = true || licensed = true", 3);
    verify_query(test_context, t, "licensed = true || licensed = false", 5);
    verify_query(test_context, t, "licensed == true || licensed == false", 5);
    verify_query(test_context, t, "licensed == true || buddy.licensed == true", 3);
    verify_query(test_context, t, "buddy.licensed == true", 0);
    verify_query(test_context, t, "buddy.licensed == false", 1);
    verify_query(test_context, t, "licensed == false || buddy.licensed == false", 3);
    verify_query(test_context, t, "licensed == true or licensed = true || licensed = TRUE", 3);
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

    CHECK_THROW_EX(
        verify_query(test_context, t, "buddy.age > $0", 0), std::out_of_range,
        CHECK_EQUAL(std::string(e.what()), "Attempt to retreive an argument when no arguments were given"));
    CHECK_THROW_EX(verify_query(test_context, t, "age == infinity", 0), query_parser::InvalidQueryError,
                   CHECK_EQUAL(std::string(e.what()), "Infinity not supported for int"));
    CHECK_THROW_EX(verify_query(test_context, t, "name == infinity", 0), query_parser::InvalidQueryError,
                   CHECK_EQUAL(std::string(e.what()), "Infinity not supported for string"));
    CHECK_THROW_EX(verify_query(test_context, t, "missing_property > 2", 0), query_parser::InvalidQueryError,
                   CHECK(std::string(e.what()).find(table_name) !=
                         std::string::npos) && // no prefix modification for names without "class_"
                       CHECK(std::string(e.what()).find("missing_property") != std::string::npos));
}

TEST_TYPES(Parser_Numerics, Prop<Int>, Nullable<Int>, Indexed<Int>, NullableIndexed<Int>, Prop<Decimal128>,
           Nullable<Decimal128>)
{
    Group g;
    std::string table_name = "table";
    TableRef t = g.add_table(table_name);
    using underlying_type = typename TEST_TYPE::underlying_type;
    constexpr bool nullable = TEST_TYPE::is_nullable;
    constexpr bool indexed = TEST_TYPE::is_indexed;
    auto col_key = t->add_column(TEST_TYPE::data_type, "values", nullable);
    if (indexed) {
        t->add_search_index(col_key);
    }
    TestValueGenerator gen;
    auto values = gen.values_from_int<underlying_type>({-1, 0, 1, 4294967295ll, -4294967295ll, 4294967296ll,
                                                        -4294967296ll, std::numeric_limits<int64_t>::max(),
                                                        std::numeric_limits<int64_t>::lowest()});
    std::vector<Mixed> args;
    for (auto val : values) {
        args.push_back(Mixed{val});
    }

    for (size_t i = 0; i < values.size(); ++i) {
        t->create_object(ObjKey{}, {{col_key, values[i]}});
    }
    if (nullable) {
        t->create_object(ObjKey{}, {{col_key, realm::null{}}});
    }
    for (size_t i = 0; i < values.size(); ++i) {
        std::stringstream out;
        out << "values == ";
        out.precision(100);
        out << values[i];
        verify_query(test_context, t, out.str(), 1);
        verify_query_sub(test_context, t, util::format("values == $%1", i), args, 1);
    }
    verify_query(test_context, t, "values == null", nullable ? 1 : 0);
}

TEST(Parser_LinksToSameTable)
{
    Group g;
    TableRef t = g.add_table("class_Person");
    ColKey age_col = t->add_column(type_Int, "age");
    ColKey name_col = t->add_column(type_String, "name");
    ColKey link_col = t->add_column(*t, "buddy");
    std::vector<std::string> names = {"Billy", "Bob", "Joe", "Jane", "Joel"};
    std::vector<ObjKey> people_keys;
    t->create_objects(names.size(), people_keys);
    for (size_t i = 0; i < t->size(); ++i) {
        Obj obj = t->get_object(people_keys[i]);
        obj.set(age_col, int64_t(i));
        obj.set(name_col, StringData(names[i]));
        obj.set(link_col, people_keys[(i + 1) % t->size()]);
    }
    t->get_object(people_keys[4]).set_null(link_col);

    verify_query(test_context, t, "age > 0", 4);
    verify_query(test_context, t, "buddy.age > 0", 4);
    verify_query(test_context, t, "buddy.buddy.age > 0", 3);
    verify_query(test_context, t, "buddy.buddy.buddy.age > 0", 2);
    verify_query(test_context, t, "buddy.buddy.buddy.buddy.age > 0", 1);
    verify_query(test_context, t, "buddy.buddy.buddy.buddy.buddy.age > 0", 0);

    CHECK_THROW_EX(verify_query(test_context, t, "buddy.buddy.missing_property > 2", 0),
                   query_parser::InvalidQueryError,
                   CHECK(std::string(e.what()).find("Person") != std::string::npos) &&
                       CHECK(std::string(e.what()).find("missing_property") != std::string::npos));
}

TEST(Parser_LinksToDifferentTable)
{
    Group g;

    TableRef discounts = g.add_table("class_Discounts");
    ColKey discount_off_col = discounts->add_column(type_Double, "reduced_by");
    ColKey discount_active_col = discounts->add_column(type_Bool, "active");

    using discount_t = std::pair<double, bool>;
    std::vector<discount_t> discount_info = {{3.0, false}, {2.5, true}, {0.50, true}, {1.50, true}};
    std::vector<ObjKey> discount_keys;
    discounts->create_objects(discount_info.size(), discount_keys);
    for (size_t i = 0; i < discount_keys.size(); ++i) {
        Obj obj = discounts->get_object(discount_keys[i]);
        obj.set(discount_off_col, discount_info[i].first);
        obj.set(discount_active_col, discount_info[i].second);
    }

    TableRef items = g.add_table("class_Items");
    ColKey item_name_col = items->add_column(type_String, "name");
    ColKey item_price_col = items->add_column(type_Double, "price");
    ColKey item_discount_col = items->add_column(*discounts, "discount");
    using item_t = std::pair<std::string, double>;
    std::vector<item_t> item_info = {{"milk", 5.5}, {"oranges", 4.0}, {"pizza", 9.5}, {"cereal", 6.5}};
    std::vector<ObjKey> item_keys;
    items->create_objects(item_info.size(), item_keys);
    for (size_t i = 0; i < item_keys.size(); ++i) {
        Obj obj = items->get_object(item_keys[i]);
        obj.set(item_name_col, StringData(item_info[i].first));
        obj.set(item_price_col, item_info[i].second);
    }
    items->get_object(item_keys[0]).set(item_discount_col, discount_keys[2]); // milk -0.50
    items->get_object(item_keys[2]).set(item_discount_col, discount_keys[1]); // pizza -2.5
    items->get_object(item_keys[3]).set(item_discount_col, discount_keys[0]); // cereal -3.0 inactive

    TableRef t = g.add_table("class_Person");
    ColKey id_col = t->add_column(type_Int, "customer_id");
    ColKey items_col = t->add_column_list(*items, "items");

    Obj person0 = t->create_object();
    Obj person1 = t->create_object();
    Obj person2 = t->create_object();
    person0.set(id_col, int64_t(0));
    person1.set(id_col, int64_t(1));
    person2.set(id_col, int64_t(2));

    LnkLst list_0 = person0.get_linklist(items_col);
    list_0.add(item_keys[0]);
    list_0.add(item_keys[1]);
    list_0.add(item_keys[2]);
    list_0.add(item_keys[3]);

    LnkLst list_1 = person1.get_linklist(items_col);
    for (size_t i = 0; i < 10; ++i) {
        list_1.add(item_keys[0]);
    }

    LnkLst list_2 = person2.get_linklist(items_col);
    list_2.add(item_keys[2]);
    list_2.add(item_keys[2]);
    list_2.add(item_keys[3]);

    verify_query(test_context, t, "items.@count > 2", 3);        // how many people bought more than two items?
    verify_query(test_context, t, "items.price > 3.0", 3);       // how many people buy items over $3.0?
    verify_query(test_context, t, "items.name ==[c] 'milk'", 2); // how many people buy milk?
    // how many people bought items with an active sale?
    verify_query(test_context, t, "items.discount.active == true", 3);
    // how many people bought an item marked down by more than $2.0?
    verify_query(test_context, t, "items.discount.reduced_by > 2.0", 2);
    // how many people would spend more than $50 without sales applied?
    verify_query(test_context, t, "items.@sum.price > 50", 1);
    // how manay people like to buy items more expensive on average than $7?
    verify_query(test_context, t, "items.@avg.price > 7", 1);

    // missing property
    CHECK_THROW_EX(verify_query(test_context, t, "missing_property > 2", 0), query_parser::InvalidQueryError,
                   CHECK(std::string(e.what()).find("Person") != std::string::npos) &&
                       CHECK(std::string(e.what()).find("missing_property") != std::string::npos));
    CHECK_THROW_EX(verify_query(test_context, t, "items.absent_property > 2", 0), query_parser::InvalidQueryError,
                   CHECK(std::string(e.what()).find("Items") != std::string::npos) &&
                       CHECK(std::string(e.what()).find("absent_property") != std::string::npos));
    CHECK_THROW_EX(verify_query(test_context, t, "items.discount.nonexistent_property > 2", 0),
                   query_parser::InvalidQueryError,
                   CHECK(std::string(e.what()).find("Discounts") != std::string::npos) &&
                       CHECK(std::string(e.what()).find("nonexistent_property") != std::string::npos));
    // property is not a link
    CHECK_THROW_EX(verify_query(test_context, t, "customer_id.property > 2", 0), query_parser::InvalidQueryError,
                   CHECK(std::string(e.what()).find("Person") != std::string::npos) &&
                       CHECK(std::string(e.what()).find("customer_id") != std::string::npos));
    CHECK_THROW_EX(verify_query(test_context, t, "items.price.property > 2", 0), query_parser::InvalidQueryError,
                   CHECK(std::string(e.what()).find("Items") != std::string::npos) &&
                       CHECK(std::string(e.what()).find("price") != std::string::npos));
    // Null cannot be compared to lists
    CHECK_THROW(verify_query(test_context, t, "items == NULL", 0), query_parser::InvalidQueryError);
    CHECK_THROW(verify_query(test_context, t, "items != NULL", 0), query_parser::InvalidQueryError);
    CHECK_THROW(verify_query(test_context, t, "items.discount == NULL", 0), query_parser::InvalidQueryError);
}


TEST(Parser_StringOperations)
{
    Group g;
    TableRef t = g.add_table("person");
    ColKey name_col = t->add_column(type_String, "name", true);
    ColKey link_col = t->add_column(*t, "father");
    std::vector<std::string> names = {"Billy", "Bob", "Joe", "Jake", "Joel"};
    std::vector<ObjKey> people_keys;
    t->create_objects(names.size(), people_keys);
    for (size_t i = 0; i < t->size(); ++i) {
        Obj obj = t->get_object(people_keys[i]);
        obj.set(name_col, StringData(names[i]));
        obj.set(link_col, people_keys[(i + 1) % people_keys.size()]);
    }
    t->create_object(); // null
    t->get_object(people_keys[4]).set_null(link_col);

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
    verify_query(test_context, t, "name == nil", 1);
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

    // querying in the other direction is also allowed
    verify_query(test_context, t, "NULL CONTAINS name", 0);
    verify_query(test_context, t, "NULL CONTAINS[c] name", 0);
    verify_query(test_context, t, "NULL BEGINSWITH name", 0);
    verify_query(test_context, t, "NULL BEGINSWITH[c] name", 0);
    verify_query(test_context, t, "NULL ENDSWITH name", 0);
    verify_query(test_context, t, "NULL ENDSWITH[c] name", 0);
    verify_query(test_context, t, "NULL LIKE name", 1);
    verify_query(test_context, t, "NULL LIKE[c] name", 1);
}


TEST(Parser_Timestamps)
{
    Group g;
    TableRef t = g.add_table("person");
    ColKey birthday_col = t->add_column(type_Timestamp, "birthday");          // disallow null
    ColKey internal_col = t->add_column(type_Timestamp, "T399", true);        // allow null
    ColKey readable_col = t->add_column(type_Timestamp, "T2017-12-04", true); // allow null
    ColKey link_col = t->add_column(*t, "linked");
    std::vector<ObjKey> keys;
    t->create_objects(5, keys);

    t->get_object(keys[0]).set(birthday_col, Timestamp(-1, -1)); // before epoch by 1 second and one nanosecond
    t->get_object(keys[1]).set(birthday_col, Timestamp(0, -1));  // before epoch by one nanosecond

    t->get_object(keys[0]).set(internal_col, Timestamp(realm::null()));
    t->get_object(keys[1]).set(internal_col, Timestamp(1512130073, 0));   // 2017/12/02 @ 12:47am (UTC)
    t->get_object(keys[2]).set(internal_col, Timestamp(1512130073, 505)); // with nanoseconds
    t->get_object(keys[3]).set(internal_col, Timestamp(1, 2));
    t->get_object(keys[4]).set(internal_col, Timestamp(0, 0));

    t->get_object(keys[0]).set(readable_col, Timestamp(1512130073, 0));
    t->get_object(keys[1]).set(readable_col, Timestamp(1512130073, 505));

    t->get_object(keys[0]).set(link_col, keys[1]);
    t->get_object(keys[2]).set(link_col, keys[0]);

    Query q = t->where();
    auto verify_with_format = [&](const char* separator) {
        verify_query(test_context, t, "T399 == NULL", 1);
        verify_query(test_context, t, "T399 != NULL", 4);
        verify_query(test_context, t, "linked.T399 == NULL", 4); // null links count as a match for null here
        verify_query(test_context, t, "linked != NULL && linked.T399 == NULL", 1);
        verify_query(test_context, t, "linked.T399 != NULL", 1);
        verify_query(test_context, t, "linked != NULL && linked.T399 != NULL", 1);
        verify_query(test_context, t, "T399 == T399:0", 0);
        verify_query(test_context, t, "linked.T399 == T399:0", 0);
        verify_query(test_context, t, std::string("T399 == 2017-12-04") + separator + "0:0:0", 0);

        verify_query(test_context, t, "T2017-12-04 == NULL", 3);
        verify_query(test_context, t, "T2017-12-04 != NULL", 2);
        verify_query(test_context, t, "T2017-12-04 != NIL", 2);
        verify_query(test_context, t, "linked.T2017-12-04 == NULL", 3); // null links count as a match for null here
        verify_query(test_context, t, "linked != NULL && linked.T2017-12-04 == NULL", 0);
        verify_query(test_context, t, "linked.T2017-12-04 != NULL", 2);
        verify_query(test_context, t, "linked != NULL && linked.T2017-12-04 != NULL", 2);
        verify_query(test_context, t, "T2017-12-04 == T399:0", 0);
        verify_query(test_context, t, "linked.T2017-12-04 == T399:0", 0);
        verify_query(test_context, t, "T2017-12-04 == 2017-12-04@0:0:0", 0);

        verify_query(test_context, t, "birthday == NULL", 0);
        verify_query(test_context, t, "birthday == NIL", 0);
        verify_query(test_context, t, "birthday != NULL", 5);
        verify_query(test_context, t, "birthday != NIL", 5);
        verify_query(test_context, t, "birthday == T0:0", 3);
        verify_query(test_context, t, std::string("birthday == 1970-1-1") + separator + "0:0:0:0",
                     3); // epoch is default non-null Timestamp

#ifndef _WIN32 // windows native functions do not support pre epoch conversions, other platforms stop at ~1901
        verify_query(test_context, t, std::string("birthday == 1969-12-31") + separator + "23:59:59:1",
                     1); // just before epoch
        verify_query(test_context, t, std::string("birthday > 1905-12-31") + separator + "23:59:59", 5);
        verify_query(test_context, t, std::string("birthday > 1905-12-31") + separator + "23:59:59:2020", 5);
#endif

        // two column timestamps
        verify_query(test_context, t, "birthday == T399", 1); // a null entry matches

        // dates pre 1900 are not supported by functions like timegm
        CHECK_THROW(verify_query(test_context, t, std::string("birthday > 1800-12-31") + separator + "23:59:59", 0),
                    query_parser::InvalidQueryError);
        CHECK_THROW(
            verify_query(test_context, t, std::string("birthday > 1800-12-31") + separator + "23:59:59:2020", 4),
            query_parser::InvalidQueryError);

        // negative nanoseconds are not allowed
        CHECK_THROW(verify_query(test_context, t, "birthday == T-1:1", 0), query_parser::SyntaxError);
        CHECK_THROW(verify_query(test_context, t, "birthday == T1:-1", 0), query_parser::SyntaxError);
        CHECK_THROW(verify_query(test_context, t, std::string("birthday == 1970-1-1") + separator + "0:0:1:-1", 0),
                    query_parser::SyntaxError);
        CHECK_THROW(
            verify_query(test_context, t, std::string("birthday == 1969-12-31") + separator + "23:59:59:-1", 1),
            query_parser::SyntaxError);
        CHECK_THROW(verify_query(test_context, t, std::string("birthday == 1970-1-1") + separator + "0:0:0:-1", 1),
                    query_parser::SyntaxError);

        // Invalid predicate
        CHECK_THROW(verify_query(test_context, t, "birthday == T1:", 0), query_parser::SyntaxError);
        CHECK_THROW(verify_query(test_context, t, "birthday == T:1", 0), query_parser::SyntaxError);
        CHECK_THROW(verify_query(test_context, t, "birthday == 1970-1-1", 0), query_parser::SyntaxError);
        CHECK_THROW(verify_query(test_context, t, std::string("birthday == 1970-1-1") + separator, 0),
                    query_parser::SyntaxError);
        CHECK_THROW(verify_query(test_context, t, std::string("birthday == 1970-1-1") + separator + "0", 0),
                    query_parser::SyntaxError);
        CHECK_THROW(verify_query(test_context, t, std::string("birthday == 1970-1-1") + separator + "0:", 0),
                    query_parser::SyntaxError);
        CHECK_THROW(verify_query(test_context, t, std::string("birthday == 1970-1-1") + separator + "0:0", 0),
                    query_parser::SyntaxError);
        CHECK_THROW(verify_query(test_context, t, std::string("birthday == 1970-1-1") + separator + "0:0:", 0),
                    query_parser::SyntaxError);
        CHECK_THROW(verify_query(test_context, t, std::string("birthday == 1970-1-1") + separator + "0:0:0:", 0),
                    query_parser::SyntaxError);
        CHECK_THROW(verify_query(test_context, t, std::string("birthday == 1970-1-1") + separator + "0:0:0:0:", 0),
                    query_parser::SyntaxError);
        CHECK_THROW(verify_query(test_context, t, std::string("birthday == 1970-1-1") + separator + "0:0:0:0:0", 0),
                    query_parser::SyntaxError);
    };

    // both versions are allowed
    verify_with_format("@");
    verify_with_format("T");

    // using both separators at the same time is an error
    CHECK_THROW(verify_query(test_context, t, "birthday == 1970-1-1T@0:0:0:0", 3), query_parser::SyntaxError);
    CHECK_THROW(verify_query(test_context, t, "birthday == 1970-1-1@T0:0:0:0", 3), query_parser::SyntaxError);
    // omitting the separator is an error
    CHECK_THROW(verify_query(test_context, t, "birthday == 1970-1-10:0:0:0:0", 0), query_parser::SyntaxError);
}


TEST(Parser_NullableBinaries)
{
    Group g;
    TableRef items = g.add_table("item");
    TableRef people = g.add_table("person");
    ColKey binary_col = items->add_column(type_Binary, "data");
    ColKey nullable_binary_col = items->add_column(type_Binary, "nullable\tdata", true);
    std::vector<ObjKey> item_keys;
    items->create_objects(5, item_keys);
    BinaryData bd0("knife", 5);
    items->get_object(item_keys[0]).set(binary_col, bd0);
    items->get_object(item_keys[0]).set(nullable_binary_col, bd0);
    BinaryData bd1("plate", 5);
    items->get_object(item_keys[1]).set(binary_col, bd1);
    items->get_object(item_keys[1]).set(nullable_binary_col, bd1);
    BinaryData bd2("fork", 4);
    items->get_object(item_keys[2]).set(binary_col, bd2);
    items->get_object(item_keys[2]).set(nullable_binary_col, bd2);

    ColKey fav_item_col = people->add_column(*items, "fav_item");
    std::vector<ObjKey> people_keys;
    people->create_objects(5, people_keys);
    for (size_t i = 0; i < people_keys.size(); ++i) {
        people->get_object(people_keys[i]).set(fav_item_col, item_keys[i]);
    }

    // direct checks
    verify_query(test_context, items, "data == NULL", 0);
    verify_query(test_context, items, "data != NULL", 5);
    verify_query(test_context, items, "nullable\\tdata == NULL", 2);
    verify_query(test_context, items, "nullable\\tdata != NULL", 3);
    verify_query(test_context, items, "data == NIL", 0);
    verify_query(test_context, items, "data != NIL", 5);
    verify_query(test_context, items, "nullable\\tdata == NIL", 2);
    verify_query(test_context, items, "nullable\\tdata != NIL", 3);

    verify_query(test_context, items, "nullable\\tdata CONTAINS 'f'", 2);
    verify_query(test_context, items, "nullable\\tdata BEGINSWITH 'f'", 1);
    verify_query(test_context, items, "nullable\\tdata ENDSWITH 'e'", 2);
    verify_query(test_context, items, "nullable\\tdata LIKE 'f*'", 1);
    verify_query(test_context, items, "nullable\\tdata CONTAINS[c] 'F'", 2);
    verify_query(test_context, items, "nullable\\tdata BEGINSWITH[c] 'F'", 1);
    verify_query(test_context, items, "nullable\\tdata ENDSWITH[c] 'E'", 2);
    verify_query(test_context, items, "nullable\\tdata LIKE[c] 'F*'", 1);

    verify_query(test_context, items, "nullable\\tdata CONTAINS NULL", 5);
    verify_query(test_context, items, "nullable\\tdata BEGINSWITH NULL", 5);
    verify_query(test_context, items, "nullable\\tdata ENDSWITH NULL", 5);
    verify_query(test_context, items, "nullable\\tdata LIKE NULL", 2);
    verify_query(test_context, items, "nullable\\tdata CONTAINS[c] NULL", 3);
    verify_query(test_context, items, "nullable\\tdata BEGINSWITH[c] NULL", 5);
    verify_query(test_context, items, "nullable\\tdata ENDSWITH[c] NULL", 5);
    verify_query(test_context, items, "nullable\\tdata LIKE[c] NULL", 2);

    verify_query(test_context, items, "NULL CONTAINS nullable\\tdata", 0);
    verify_query(test_context, items, "NULL BEGINSWITH nullable\\tdata", 0);
    verify_query(test_context, items, "NULL ENDSWITH nullable\\tdata", 0);
    verify_query(test_context, items, "NULL LIKE nullable\\tdata", 2);
    verify_query(test_context, items, "NULL CONTAINS[c] nullable\\tdata", 0);
    verify_query(test_context, items, "NULL BEGINSWITH[c] nullable\\tdata", 0);
    verify_query(test_context, items, "NULL ENDSWITH[c] nullable\\tdata", 0);
    verify_query(test_context, items, "NULL LIKE[c] nullable\\tdata", 2);

    // check across links
    verify_query(test_context, people, "fav_item.data == NULL", 0);
    verify_query(test_context, people, "fav_item.data != NULL", 5);
    verify_query(test_context, people, "fav_item.nullable\\tdata == NULL", 2);
    verify_query(test_context, people, "fav_item.nullable\\tdata != NULL", 3);
    verify_query(test_context, people, "NULL == fav_item.data", 0);

    verify_query(test_context, people, "fav_item.data ==[c] NULL", 0);
    verify_query(test_context, people, "fav_item.data !=[c] NULL", 5);
    verify_query(test_context, people, "fav_item.nullable\\tdata ==[c] NULL", 2);
    verify_query(test_context, people, "fav_item.nullable\\tdata !=[c] NULL", 3);
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
    verify_query(test_context, people, "fav_item.data == fav_item.nullable\\tdata", 3);
    verify_query(test_context, people, "fav_item.data == fav_item.data", 5);
    verify_query(test_context, people, "fav_item.nullable\\tdata == fav_item.nullable\\tdata", 5);

    verify_query(test_context, items,
                 "data contains NULL && data contains 'fo' && !(data contains 'asdfasdfasdf') && data contains 'rk'",
                 1);
}


TEST(Parser_OverColumnIndexChanges)
{
    Group g;
    TableRef table = g.add_table("table");
    ColKey first_col = table->add_column(type_Int, "to_remove");
    ColKey int_col = table->add_column(type_Int, "ints");
    ColKey double_col = table->add_column(type_Double, "doubles");
    ColKey string_col = table->add_column(type_String, "strings");
    std::vector<ObjKey> keys;
    table->create_objects(3, keys);
    for (size_t i = 0; i < keys.size(); ++i) {
        Obj obj = table->get_object(keys[i]);
        obj.set(int_col, int64_t(i));
        obj.set(double_col, double(i));
        std::string str(i, 'a');
        obj.set(string_col, StringData(str));
    }

    std::string ints_before = verify_query(test_context, table, "ints >= 1", 2).get_description();
    std::string doubles_before = verify_query(test_context, table, "doubles >= 1", 2).get_description();
    std::string strings_before = verify_query(test_context, table, "strings.@count >= 1", 2).get_description();

    table->remove_column(first_col);

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
    ColKey int_col = table->add_column(type_Int, "ints", true);
    ColKey double_col = table->add_column(type_Double, "doubles");
    ColKey string_col = table->add_column(type_String, "strings");
    ColKey decimal_col = table->add_column(type_Decimal, "decimals");
    ColKey objectid_col = table->add_column(type_ObjectId, "objectids");
    ColKey link_col = table->add_column(*table, "link");
    std::vector<ObjKey> keys;
    table->create_objects(3, keys);
    for (size_t i = 0; i < keys.size(); ++i) {
        Obj obj = table->get_object(keys[i]);
        obj.set(int_col, int64_t(i));
        obj.set(double_col, double(i));
        std::string str(i, 'a');
        obj.set(string_col, StringData(str));
        obj.set(decimal_col, Decimal128(int64_t(i)));
        obj.set(objectid_col, ObjectId::gen());
    }
    table->get_object(keys[1]).set(link_col, keys[0]);

    Query q = table->where().and_query(table->column<Int>(int_col) == table->column<String>(string_col).size());
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
    verify_query(test_context, table, "decimals == decimals", 3);
    verify_query(test_context, table, "objectids == objectids", 3);
    verify_query(test_context, table, "doubles == ints", 3);
    verify_query(test_context, table, "ints == doubles", 3);
    verify_query(test_context, table, "ints == decimals", 3);

    // type mismatch
    CHECK_THROW(verify_query(test_context, table, "doubles == strings", 0), query_parser::InvalidQueryError);
    CHECK_THROW(verify_query(test_context, table, "strings == doubles", 0), query_parser::InvalidQueryError);
    CHECK_THROW(verify_query(test_context, table, "objectids == ints", 0), query_parser::InvalidQueryError);
}

TEST(Parser_TwoColumnAggregates)
{
    Group g;

    TableRef discounts = g.add_table("class_Discounts");
    ColKey discount_name_col = discounts->add_column(type_String, "promotion", true);
    ColKey discount_off_col = discounts->add_column(type_Double, "reduced_by");
    ColKey discount_active_col = discounts->add_column(type_Bool, "active");

    using discount_t = std::pair<double, bool>;
    std::vector<discount_t> discount_info = {{3.0, false}, {2.5, true}, {0.50, true}, {1.50, true}};
    std::vector<ObjKey> discount_keys;
    discounts->create_objects(discount_info.size(), discount_keys);
    for (size_t i = 0; i < discount_keys.size(); ++i) {
        Obj obj = discounts->get_object(discount_keys[i]);
        obj.set(discount_off_col, discount_info[i].first);
        obj.set(discount_active_col, discount_info[i].second);
    }
    discounts->get_object(discount_keys[0]).set(discount_name_col, StringData("back to school"));
    discounts->get_object(discount_keys[1]).set(discount_name_col, StringData("pizza lunch special"));
    discounts->get_object(discount_keys[2]).set(discount_name_col, StringData("manager's special"));

    TableRef items = g.add_table("class_Items");
    ColKey item_name_col = items->add_column(type_String, "name");
    ColKey item_price_col = items->add_column(type_Double, "price");
    ColKey item_price_float_col = items->add_column(type_Float, "price_float");
    ColKey item_price_decimal_col = items->add_column(type_Decimal, "price_decimal");
    ColKey item_discount_col = items->add_column(*discounts, "discount");
    ColKey item_creation_date = items->add_column(type_Timestamp, "creation_date");
    using item_t = std::pair<std::string, double>;
    std::vector<item_t> item_info = {{"milk", 5.5}, {"oranges", 4.0}, {"pizza", 9.5}, {"cereal", 6.5}};
    std::vector<ObjKey> item_keys;
    items->create_objects(item_info.size(), item_keys);
    for (size_t i = 0; i < item_keys.size(); ++i) {
        Obj obj = items->get_object(item_keys[i]);
        obj.set(item_name_col, StringData(item_info[i].first));
        obj.set(item_price_col, item_info[i].second);
        obj.set(item_price_float_col, float(item_info[i].second));
        obj.set(item_price_decimal_col, Decimal128(item_info[i].second));
        obj.set(item_creation_date, Timestamp(static_cast<int64_t>(item_info[i].second * 10), 0));
    }
    items->get_object(item_keys[0]).set(item_discount_col, discount_keys[2]); // milk -0.50
    items->get_object(item_keys[2]).set(item_discount_col, discount_keys[1]); // pizza -2.5
    items->get_object(item_keys[3]).set(item_discount_col, discount_keys[0]); // cereal -3.0 inactive

    TableRef t = g.add_table("class_Person");
    ColKey id_col = t->add_column(type_Int, "customer_id");
    ColKey account_col = t->add_column(type_Double, "account_balance");
    ColKey items_col = t->add_column_list(*items, "items");
    ColKey account_float_col = t->add_column(type_Float, "account_balance_float");
    ColKey account_decimal_col = t->add_column(type_Decimal, "account_balance_decimal");
    ColKey account_creation_date_col = t->add_column(type_Timestamp, "account_creation_date");

    Obj person0 = t->create_object();
    Obj person1 = t->create_object();
    Obj person2 = t->create_object();

    person0.set(id_col, int64_t(0));
    person0.set(account_col, double(10.0));
    person0.set(account_float_col, float(10.0));
    person0.set(account_decimal_col, Decimal128(10.0));
    person0.set(account_creation_date_col, Timestamp(30, 0));
    person1.set(id_col, int64_t(1));
    person1.set(account_col, double(20.0));
    person1.set(account_float_col, float(20.0));
    person1.set(account_decimal_col, Decimal128(20.0));
    person1.set(account_creation_date_col, Timestamp(50, 0));
    person2.set(id_col, int64_t(2));
    person2.set(account_col, double(30.0));
    person2.set(account_float_col, float(30.0));
    person2.set(account_decimal_col, Decimal128(30.0));
    person2.set(account_creation_date_col, Timestamp(70, 0));

    LnkLst list_0 = person0.get_linklist(items_col);
    list_0.add(item_keys[0]);
    list_0.add(item_keys[1]);
    list_0.add(item_keys[2]);
    list_0.add(item_keys[3]);

    LnkLst list_1 = person1.get_linklist(items_col);
    for (size_t i = 0; i < 10; ++i) {
        list_1.add(item_keys[0]);
    }

    LnkLst list_2 = person2.get_linklist(items_col);
    list_2.add(item_keys[2]);
    list_2.add(item_keys[2]);
    list_2.add(item_keys[3]);

    // int vs linklist count/size
    verify_query(test_context, t, "customer_id < items.@count", 3);
    verify_query(test_context, t, "customer_id < items.@size", 3);

    // double vs linklist count/size
    verify_query(test_context, t, "items.@min.price > items.@count", 1);
    verify_query(test_context, t, "items.@min.price > items.@size", 1);

    // double vs string/binary count/size; len("oranges") > 4.0
    verify_query(test_context, items, "name.@count > price", 1);
    verify_query(test_context, items, "price < name.@size", 1);

    // double vs double
    verify_query(test_context, t, "items.@sum.price == 25.5", 2);  // person0, person2
    verify_query(test_context, t, "items.@min.price == 4.0", 1);   // person0
    verify_query(test_context, t, "items.@max.price == 9.5", 2);   // person0, person2
    verify_query(test_context, t, "items.@avg.price == 6.375", 1); // person0
    verify_query(test_context, t, "items.@sum.price > account_balance", 2);
    verify_query(test_context, t, "items.@min.price > account_balance", 0);
    verify_query(test_context, t, "items.@max.price > account_balance", 0);
    verify_query(test_context, t, "items.@avg.price > account_balance", 0);
    // float vs float
    verify_query(test_context, t, "items.@sum.price_float == 25.5", 2);  // person0, person2
    verify_query(test_context, t, "items.@min.price_float == 4.0", 1);   // person0
    verify_query(test_context, t, "items.@max.price_float == 9.5", 2);   // person0, person2
    verify_query(test_context, t, "items.@avg.price_float == 6.375", 1); // person0
    verify_query(test_context, t, "items.@sum.price_float > account_balance_float", 2);
    verify_query(test_context, t, "items.@min.price_float > account_balance_float", 0);
    verify_query(test_context, t, "items.@max.price_float > account_balance_float", 0);
    verify_query(test_context, t, "items.@avg.price_float > account_balance_float", 0);
    // Decimal128 vs Decimal128
    verify_query(test_context, t, "items.@sum.price_decimal == 25.5", 2);  // person0, person2
    verify_query(test_context, t, "items.@min.price_decimal == 4.0", 1);   // person0
    verify_query(test_context, t, "items.@max.price_decimal == 9.5", 2);   // person0, person2
    verify_query(test_context, t, "items.@avg.price_decimal == 6.375", 1); // person0
    verify_query(test_context, t, "items.@sum.price_decimal > account_balance_decimal", 2);
    verify_query(test_context, t, "items.@min.price_decimal > account_balance_decimal", 0);
    verify_query(test_context, t, "items.@max.price_decimal > account_balance_decimal", 0);
    verify_query(test_context, t, "items.@avg.price_decimal > account_balance_decimal", 0);
    // Timestamp vs Timestamp
    verify_query(test_context, t, "items.@min.creation_date == T40:0", 1); // person0
    verify_query(test_context, t, "items.@max.creation_date == T95:0", 2); // person0, person2
    verify_query(test_context, t, "items.@min.creation_date > account_creation_date", 2);
    verify_query(test_context, t, "items.@max.creation_date > account_creation_date", 3);

    // cannot aggregate string
    CHECK_THROW(verify_query(test_context, t, "items.@min.name > account_balance", 0),
                query_parser::InvalidQueryError);
    CHECK_THROW(verify_query(test_context, t, "items.@max.name > account_balance", 0),
                query_parser::InvalidQueryError);
    CHECK_THROW(verify_query(test_context, t, "items.@sum.name > account_balance", 0),
                query_parser::InvalidQueryError);
    CHECK_THROW(verify_query(test_context, t, "items.@avg.name > account_balance", 0),
                query_parser::InvalidQueryError);
    // cannot aggregate link
    CHECK_THROW(verify_query(test_context, t, "items.@min.discount > account_balance", 0),
                query_parser::InvalidQueryError);
    CHECK_THROW(verify_query(test_context, t, "items.@max.discount > account_balance", 0),
                query_parser::InvalidQueryError);
    CHECK_THROW(verify_query(test_context, t, "items.@sum.discount > account_balance", 0),
                query_parser::InvalidQueryError);
    CHECK_THROW(verify_query(test_context, t, "items.@avg.discount > account_balance", 0),
                query_parser::InvalidQueryError);
    // cannot do avg and sum on timestamp
    CHECK_THROW(verify_query(test_context, t, "items.@sum.creation_date > account_creation_date", 2),
                query_parser::InvalidQueryError);
    CHECK_THROW(verify_query(test_context, t, "items.@avg.creation_date > account_creation_date", 3),
                query_parser::InvalidQueryError);

    verify_query(test_context, t, "items.@count < account_balance", 3); // linklist count vs double
    verify_query(test_context, t, "items.@count > 3", 2);               // linklist count vs literal int
    // linklist count vs literal double
    verify_query(test_context, t, "items.@count == 3.0", 1);
    verify_query(test_context, t, "items.@count == 3.1", 0); // no integer promotion

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

TEST(Parser_substitution)
{
    Group g;
    TableRef t = g.add_table("person");
    ColKey int_col = t->add_column(type_Int, "age");
    ColKey str_col = t->add_column(type_String, "name");
    ColKey double_col = t->add_column(type_Double, "fees");
    ColKey bool_col = t->add_column(type_Bool, "paid", true);
    ColKey time_col = t->add_column(type_Timestamp, "time", true);
    ColKey binary_col = t->add_column(type_Binary, "binary", true);
    ColKey float_col = t->add_column(type_Float, "floats", true);
    ColKey nullable_double_col = t->add_column(type_Float, "nuldouble", true);
    ColKey link_col = t->add_column(*t, "links");
    ColKey list_col = t->add_column_list(*t, "list");
    std::vector<std::string> names = {"Billy", "Bob", "Joe", "Jane", "Joel"};
    std::vector<double> fees = {2.0, 2.23, 2.25, 2.22, 3.73};
    std::vector<ObjKey> obj_keys;
    t->create_objects(names.size(), obj_keys);

    for (size_t i = 0; i < obj_keys.size(); ++i) {
        Obj obj = t->get_object(obj_keys[i]);
        obj.set(int_col, int64_t(i));
        obj.set(str_col, StringData(names[i]));
        obj.set(double_col, fees[i]);
    }
    t->get_object(obj_keys[0]).set(bool_col, true);
    t->get_object(obj_keys[1]).set(bool_col, false);
    t->get_object(obj_keys[1])
        .set(time_col, Timestamp(1512130073, 505)); // 2017/12/02 @ 12:47am (UTC) + 505 nanoseconds
    std::string str_oe("oe");
    std::string str_eo("eo");
    BinaryData bd0(str_oe);
    BinaryData bd1(str_eo);
    t->get_object(obj_keys[0]).set(binary_col, bd0);
    t->get_object(obj_keys[1]).set(binary_col, bd1);
    t->get_object(obj_keys[0]).set(float_col, 2.33f);
    t->get_object(obj_keys[1]).set(float_col, 2.22f);
    t->get_object(obj_keys[0]).set(nullable_double_col, 2.33f);
    t->get_object(obj_keys[1]).set(nullable_double_col, 2.22f);
    t->get_object(obj_keys[0]).set(link_col, obj_keys[1]);
    t->get_object(obj_keys[1]).set(link_col, obj_keys[0]);
    LnkLst list_0 = t->get_object(obj_keys[0]).get_linklist(list_col);
    list_0.add(obj_keys[0]);
    list_0.add(obj_keys[1]);
    list_0.add(obj_keys[2]);
    LnkLst list_1 = t->get_object(obj_keys[1]).get_linklist(list_col);
    list_1.add(obj_keys[0]);

    util::Any args[] = {Int(2), Double(2.25), String("oe"), realm::null{}, Bool(true), Timestamp(1512130073, 505),
                        bd0,    Float(2.33),  obj_keys[0],  Int(3),        Int(4),     Bool(false)};
    size_t num_args = 12;
    verify_query_sub(test_context, t, "age > $0", args, num_args, 2);
    verify_query_sub(test_context, t, "age > $0 || fees == $1", args, num_args, 3);
    verify_query_sub(test_context, t, "name CONTAINS[c] $2", args, num_args, 2);
    verify_query_sub(test_context, t, "paid == $3", args, num_args, 3);
    verify_query_sub(test_context, t, "paid != $3", args, num_args, 2);
    verify_query_sub(test_context, t, "paid == $4", args, num_args, 1);
    verify_query_sub(test_context, t, "paid != $4", args, num_args, 4);
    verify_query_sub(test_context, t, "paid = $11", args, num_args, 1);
    verify_query_sub(test_context, t, "time == $5", args, num_args, 1);
    verify_query_sub(test_context, t, "time == $3", args, num_args, 4);
    verify_query_sub(test_context, t, "binary == $6", args, num_args, 1);
    verify_query_sub(test_context, t, "binary == $3", args, num_args, 3);
    verify_query_sub(test_context, t, "floats == $7", args, num_args, 1);
    verify_query_sub(test_context, t, "floats == $3", args, num_args, 3);
    verify_query_sub(test_context, t, "nuldouble == $7", args, num_args, 1);
    verify_query_sub(test_context, t, "nuldouble == $3", args, num_args, 3);
    verify_query_sub(test_context, t, "links == $3", args, num_args, 3);
    verify_query_sub(test_context, t, "list == $8", args, num_args, 2);

    // substitutions through collection aggregates is a different code path
    verify_query_sub(test_context, t, "list.@min.age < $0", args, num_args, 2);
    verify_query_sub(test_context, t, "list.@max.age >= $0", args, num_args, 1);
    verify_query_sub(test_context, t, "list.@sum.age >= $0", args, num_args, 1);
    verify_query_sub(test_context, t, "list.@avg.age < $0", args, num_args, 2);
    verify_query_sub(test_context, t, "list.@count > $0", args, num_args, 1);
    verify_query_sub(test_context, t, "list.@size > $0", args, num_args, 1);
    verify_query_sub(test_context, t, "name.@count > $0", args, num_args, 5);
    verify_query_sub(test_context, t, "name.@size > $0", args, num_args, 5);
    verify_query_sub(test_context, t, "binary.@count >= $0", args, num_args, 2);
    verify_query_sub(test_context, t, "binary.@size >= $0", args, num_args, 2);

    // reusing properties, mixing order
    verify_query_sub(test_context, t, "(age > $0 || fees == $1) && age == $0", args, num_args, 1);

    // negative index
    // FIXME: Should the error be std::out_of_range or SyntaxError?
    CHECK_THROW(verify_query_sub(test_context, t, "age > $-1", args, num_args, 0), std::runtime_error);
    // missing index
    // FIXME: Should the error be SyntaxError?
    CHECK_THROW(verify_query_sub(test_context, t, "age > $", args, num_args, 0), std::runtime_error);
    // non-numerical index
    // FIXME: Should the error be SyntaxError?
    CHECK_THROW(verify_query_sub(test_context, t, "age > $age", args, num_args, 0), std::runtime_error);
    // leading zero index
    verify_query_sub(test_context, t, "name CONTAINS[c] $002", args, num_args, 2);
    // double digit index
    verify_query_sub(test_context, t, "age == $10", args, num_args, 1);

    std::string message;
    // referencing a parameter outside of the list size throws
    CHECK_THROW_ANY_GET_MESSAGE(verify_query_sub(test_context, t, "age > $0", args, /*num_args*/ 0, 0), message);
    CHECK_EQUAL(message, "Request for argument at index 0 but no arguments are provided");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query_sub(test_context, t, "age > $1", args, /*num_args*/ 1, 0), message);
    CHECK_EQUAL(message, "Request for argument at index 1 but only 1 argument is provided");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query_sub(test_context, t, "age > $2", args, /*num_args*/ 2, 0), message);
    CHECK_EQUAL(message, "Request for argument at index 2 but only 2 arguments are provided");
    CHECK_THROW_ANY_GET_MESSAGE(t->query("age > $0", std::vector<Mixed>{}), message);
    CHECK_EQUAL(message, "Request for argument at index 0 but no arguments are provided");
    CHECK_THROW_ANY_GET_MESSAGE(t->query("age > $1", std::vector<Mixed>{{1}}), message);
    CHECK_EQUAL(message, "Request for argument at index 1 but only 1 argument is provided");
    CHECK_THROW_ANY_GET_MESSAGE(t->query("age > $2", std::vector<Mixed>{{1}, {2}}), message);
    CHECK_EQUAL(message, "Request for argument at index 2 but only 2 arguments are provided");

    // Mixed types
    // int
    verify_query_sub(test_context, t, "age > $1", args, num_args, 2);
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "age > $2", args, num_args, 0));
    verify_query_sub(test_context, t, "age > $3", args, num_args, 0);
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "age > $5", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "age > $6", args, num_args, 0));
    verify_query_sub(test_context, t, "age > $7", args, num_args, 2);
    // double
    verify_query_sub(test_context, t, "fees > $0", args, num_args, 4);
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "fees > $2", args, num_args, 0));
    verify_query_sub(test_context, t, "fees > $3", args, num_args, 0);
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "fees > $5", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "fees > $6", args, num_args, 0));
    verify_query_sub(test_context, t, "fees > $7", args, num_args, 1);
    // float
    verify_query_sub(test_context, t, "floats > $0", args, num_args, 2);
    verify_query_sub(test_context, t, "floats > $1", args, num_args, 1);
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "floats > $2", args, num_args, 0));
    verify_query_sub(test_context, t, "floats > $3", args, num_args, 0);
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "floats > $5", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "floats > $6", args, num_args, 0));
    // string
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "name == $0", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "name == $1", args, num_args, 0));
    verify_query_sub(test_context, t, "name == $3", args, num_args, 0);
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "name == $4", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "name == $5", args, num_args, 0));
    verify_query_sub(test_context, t, "name == $6", args, num_args, 0);
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
    verify_query_sub(test_context, t, "binary == $2", args, num_args, 1);
    verify_query_sub(test_context, t, "binary == $3", args, num_args, 3);
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "binary == $4", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "binary == $5", args, num_args, 0));
    CHECK_THROW_ANY(verify_query_sub(test_context, t, "binary == $7", args, num_args, 0));
}

TEST(Parser_string_binary_encoding)
{
    Group g;
    TableRef t = g.add_table("person");
    ColKey str_col = t->add_column(type_String, "string_col", true);
    ColKey bin_col = t->add_column(type_Binary, "binary_col", true);

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
        "<foo val=`bar' />"};

    t->create_object(); // nulls
    // add a single char of each value
    for (size_t i = 0; i < 256; ++i) {
        char c = static_cast<char>(i);
        test_strings.push_back(std::string(1, c));
    }
    // a single string of 100 nulls
    test_strings.push_back(std::string(100, '\0'));

    for (const std::string& buff : test_strings) {
        StringData sd(buff);
        BinaryData bd(buff);
        Obj obj = t->create_object();
        obj.set(str_col, sd);
        obj.set(bin_col, bd);
    }

    struct TestValues {
        TestValues(size_t processed, bool replace)
            : num_processed(processed)
            , should_be_replaced(replace)
        {
        }
        TestValues() {}
        size_t num_processed = 0;
        bool should_be_replaced = false;
    };

    std::unordered_map<unsigned char, TestValues> expected_replacements;
    expected_replacements['\x0'] = TestValues{0, true}; // non printable characters require replacement
    expected_replacements['\x7f'] = TestValues{0, true};
    expected_replacements['\x80'] = TestValues{0, true};
    expected_replacements['\xad'] = TestValues{0, true};
    expected_replacements['\xff'] = TestValues{0, true};
    expected_replacements['A'] = TestValues{0, false}; // ascii characters can be represented in plain text
    expected_replacements['z'] = TestValues{0, false};
    expected_replacements['0'] = TestValues{0, false};
    expected_replacements['9'] = TestValues{0, false};
    expected_replacements['"'] = TestValues{0, true}; // quotes must be replaced as b64
    expected_replacements['\''] = TestValues{0, true};
    static const std::string base64_prefix = "B64\"";
    static const std::string base64_suffix = "==\"";

    for (const std::string& buff : test_strings) {
        size_t num_results = 1;
        Query qstr = t->where().equal(str_col, StringData(buff), true);
        Query qbin = t->where().equal(bin_col, BinaryData(buff));
        CHECK_EQUAL(qstr.count(), num_results);
        CHECK_EQUAL(qbin.count(), num_results);
        std::string string_description = qstr.get_description();
        std::string binary_description = qbin.get_description();

        if (buff.size() == 1) {
            auto it = expected_replacements.find(buff[0]);
            if (it != expected_replacements.end()) {
                ++it->second.num_processed;


                // std::cout << "string: '" << it->first << "' described: " << string_description << std::endl;
                if (!it->second.should_be_replaced) {
                    bool validate = string_description.find(base64_prefix) == std::string::npos &&
                                    string_description.find(base64_suffix) == std::string::npos &&
                                    string_description.find(it->first) != std::string::npos;
                    CHECK(validate);
                    if (!validate) {
                        std::stringstream ss;
                        ss << "string should not be replaced: '" << it->first
                           << "' described: " << string_description;
                        CHECK_EQUAL(ss.str(), "");
                    }
                }
                else {
                    size_t str_b64_pre_pos = string_description.find(base64_prefix);
                    size_t str_b64_suf_pos = string_description.find(base64_suffix);
                    size_t bin_b64_pre_pos = binary_description.find(base64_prefix);
                    size_t bin_b64_suf_pos = binary_description.find(base64_suffix);

                    bool validate = str_b64_pre_pos != std::string::npos && str_b64_suf_pos != std::string::npos &&
                                    bin_b64_pre_pos != std::string::npos && bin_b64_suf_pos != std::string::npos;
                    CHECK(validate);

                    size_t contents_str = string_description.find(it->first, str_b64_pre_pos + base64_prefix.size());
                    size_t contents_bin = binary_description.find(it->first, bin_b64_pre_pos + base64_prefix.size());

                    bool validate_contents = contents_str > str_b64_suf_pos && contents_bin > bin_b64_suf_pos;
                    CHECK(validate_contents);
                    if (!validate || !validate_contents) {
                        std::stringstream ss;
                        ss << "string should be replaced: '" << it->first << "' described: " << string_description;
                        CHECK_EQUAL(ss.str(), "");
                    }
                }
            }
        }

        // std::cerr << "original: " << buff << "\tdescribed: " << string_description << " : " << binary_description
        // << "\n";

        Query qstr2 = t->query(string_description);
        CHECK_EQUAL(qstr2.count(), num_results);

        Query qbin2 = t->query(binary_description);
        CHECK_EQUAL(qbin2.count(), num_results);
    }

    for (auto it = expected_replacements.begin(); it != expected_replacements.end(); ++it) {
        bool processed = it->second.num_processed == 1;
        CHECK(processed);
        if (!processed) { // the check is expected to fail, but will print which character is failing
            CHECK_EQUAL(it->first, it->second.num_processed);
        }
    }
}

TEST(Parser_collection_aggregates)
{
    Group g;
    TableRef people = g.add_table("class_Person");
    TableRef courses = g.add_table("class_Course");
    auto title_col = courses->add_column(type_String, "title");
    auto credits_col = courses->add_column(type_Double, "credits");
    auto hours_col = courses->add_column(type_Int, "hours_required");
    auto fail_col = courses->add_column(type_Float, "failure_percentage");
    auto start_date_col = courses->add_column(type_Timestamp, "start_date");
    auto int_col = people->add_column(type_Int, "age");
    auto str_col = people->add_column(type_String, "name");
    auto courses_col = people->add_column_list(*courses, "courses_taken");
    auto binary_col = people->add_column(type_Binary, "hash");
    using info_t = std::pair<std::string, int64_t>;
    std::vector<info_t> person_info = {{"Billy", 18}, {"Bob", 17}, {"Joe", 19}, {"Jane", 20}, {"Joel", 18}};
    size_t j = 0;
    for (info_t i : person_info) {
        Obj obj = people->create_object();
        obj.set(str_col, StringData(i.first));
        obj.set(int_col, i.second);
        std::string hash(j++, 'a'); // a repeated j times
        BinaryData payload(hash);
        obj.set(binary_col, payload);
    }
    using cinfo = std::tuple<std::string, double, int64_t, float, Timestamp>;
    std::vector<cinfo> course_info = {
        cinfo{"Math", 5.0, 42, 0.36f, {10, 0}}, cinfo{"Comp Sci", 4.5, 45, 0.25f, {11, 0}},
        cinfo{"Chemistry", 4.0, 41, 0.40f, {12, 0}}, cinfo{"English", 3.5, 40, 0.07f, {13, 0}},
        cinfo{"Physics", 4.5, 42, 0.42f, {14, 0}}};
    std::vector<ObjKey> course_keys;
    for (cinfo course : course_info) {
        Obj obj = courses->create_object();
        course_keys.push_back(obj.get_key());
        obj.set(title_col, StringData(std::get<0>(course)));
        obj.set(credits_col, std::get<1>(course));
        obj.set(hours_col, std::get<2>(course));
        obj.set(fail_col, std::get<3>(course));
        obj.set(start_date_col, std::get<4>(course));
    }
    auto it = people->begin();
    LnkLstPtr billy_courses = it->get_linklist_ptr(courses_col);
    billy_courses->add(course_keys[0]);
    billy_courses->add(course_keys[1]);
    billy_courses->add(course_keys[4]);
    ++it;
    LnkLstPtr bob_courses = it->get_linklist_ptr(courses_col);
    bob_courses->add(course_keys[0]);
    bob_courses->add(course_keys[1]);
    bob_courses->add(course_keys[1]);
    ++it;
    LnkLstPtr joe_courses = it->get_linklist_ptr(courses_col);
    joe_courses->add(course_keys[3]);
    ++it;
    LnkLstPtr jane_courses = it->get_linklist_ptr(courses_col);
    jane_courses->add(course_keys[2]);
    jane_courses->add(course_keys[4]);

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

    // timestamp
    verify_query(test_context, people, "courses_taken.@min.start_date < T12:0", 2);
    verify_query(test_context, people, "courses_taken.@max.start_date > T12:0", 3);

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

TEST(Parser_NegativeAgg)
{
    Group g;

    TableRef items = g.add_table("class_Items");
    ColKey item_name_col = items->add_column(type_String, "name");
    ColKey item_price_col = items->add_column(type_Double, "price");
    ColKey item_price_float_col = items->add_column(type_Float, "price_float");
    ColKey item_price_decimal_col = items->add_column(type_Decimal, "price_decimal");
    using item_t = std::pair<std::string, double>;
    std::vector<item_t> item_info = {{"milk", -5.5}, {"oranges", -4.0}, {"pizza", -9.5}, {"cereal", -6.5}};
    std::vector<ObjKey> item_keys;
    items->create_objects(item_info.size(), item_keys);
    for (size_t i = 0; i < item_keys.size(); ++i) {
        Obj obj = items->get_object(item_keys[i]);
        obj.set(item_name_col, StringData(item_info[i].first));
        obj.set(item_price_col, item_info[i].second);
        obj.set(item_price_float_col, float(item_info[i].second));
        obj.set(item_price_decimal_col, Decimal128(item_info[i].second));
    }

    TableRef t = g.add_table("class_Person");
    ColKey id_col = t->add_column(type_Int, "customer_id");
    ColKey account_col = t->add_column(type_Double, "account_balance");
    ColKey items_col = t->add_column_list(*items, "items");
    ColKey account_float_col = t->add_column(type_Float, "account_balance_float");
    ColKey account_decimal_col = t->add_column(type_Decimal, "account_balance_decimal");

    Obj person0 = t->create_object();
    Obj person1 = t->create_object();
    Obj person2 = t->create_object();

    person0.set(id_col, int64_t(0));
    person0.set(account_col, double(10.0));
    person0.set(account_float_col, float(10.0));
    person0.set(account_decimal_col, Decimal128(10.0));
    person1.set(id_col, int64_t(1));
    person1.set(account_col, double(20.0));
    person1.set(account_float_col, float(20.0));
    person1.set(account_decimal_col, Decimal128(20.0));
    person2.set(id_col, int64_t(2));
    person2.set(account_col, double(30.0));
    person2.set(account_float_col, float(30.0));
    person2.set(account_decimal_col, Decimal128(30.0));

    LnkLst list_0 = person0.get_linklist(items_col);
    list_0.add(item_keys[0]);
    list_0.add(item_keys[1]);
    list_0.add(item_keys[2]);
    list_0.add(item_keys[3]);

    LnkLst list_1 = person1.get_linklist(items_col);
    for (size_t i = 0; i < 10; ++i) {
        list_1.add(item_keys[0]);
    }

    LnkLst list_2 = person2.get_linklist(items_col);
    list_2.add(item_keys[2]);
    list_2.add(item_keys[2]);
    list_2.add(item_keys[3]);

    verify_query(test_context, t, "items.@min.price == -9.5", 2);   // person0, person2
    verify_query(test_context, t, "items.@max.price == -4.0", 1);   // person0
    verify_query(test_context, t, "items.@sum.price == -25.5", 2);  // person0, person2
    verify_query(test_context, t, "items.@avg.price == -6.375", 1); // person0

    verify_query(test_context, t, "items.@min.price_float == -9.5", 2);   // person0, person2
    verify_query(test_context, t, "items.@max.price_float == -4.0", 1);   // person0
    verify_query(test_context, t, "items.@sum.price_float == -25.5", 2);  // person0, person2
    verify_query(test_context, t, "items.@avg.price_float == -6.375", 1); // person0

    verify_query(test_context, t, "items.@min.price_decimal == -9.5", 2);   // person0, person2
    verify_query(test_context, t, "items.@max.price_decimal == -4.0", 1);   // person0
    verify_query(test_context, t, "items.@sum.price_decimal == -25.5", 2);  // person0, person2
    verify_query(test_context, t, "items.@avg.price_decimal == -6.375", 1); // person0
}


TEST(Parser_list_of_primitive_ints)
{
    Group g;
    TableRef t = g.add_table("table");

    auto col_int_list = t->add_column_list(type_Int, "integers");
    auto col_int = t->add_column(type_Int, "single_int");
    auto col_int_list_nullable = t->add_column_list(type_Int, "integers_nullable", true);
    auto col_int_nullable = t->add_column(type_Int, "single_int_nullable", true);
    CHECK_THROW_ANY(t->add_search_index(col_int_list));

    size_t num_objects = 10;
    for (size_t i = 0; i < num_objects; ++i) {
        Obj obj = t->create_object();
        obj.get_list<Int>(col_int_list).add(i);
        obj.set<Int>(col_int, i);
        obj.get_list<Optional<Int>>(col_int_list_nullable).add(i);
        obj.set<Optional<Int>>(col_int_nullable, i);
    }

    TableRef t2 = g.add_table("table2");

    auto col_link = t2->add_column(*t, "link");
    auto col_list = t2->add_column_list(*t, "list");
    {
        // object with link to 1, list with {1}
        Obj obj0 = t2->create_object();
        ObjKey linkedObj0 = t->find_first(col_int, Int(1));
        obj0.set(col_link, linkedObj0);
        LnkLst list0 = obj0.get_linklist(col_list);
        list0.add(linkedObj0);
        // object with link to 2, list with all values
        Obj obj1 = t2->create_object();
        obj1.set(col_link, t->find_first(col_int, Int(2)));
        LnkLst list1 = obj1.get_linklist(col_list);
        for (auto it = t->begin(); it != t->end(); ++it) {
            list1.add(it->get_key());
        }
        // empty object, null link, empty list
        Obj obj2 = t2->create_object();
    }

    for (size_t i = 0; i < num_objects; ++i) {
        verify_query(test_context, t, util::format("integers == %1", i), 1);
        verify_query(test_context, t, util::format("integers.@min == %1", i), 1);
        verify_query(test_context, t, util::format("integers.@max == %1", i), 1);
        verify_query(test_context, t, util::format("integers.@avg == %1", i), 1);
        verify_query(test_context, t, util::format("integers.@sum == %1", i), 1);
        verify_query(test_context, t, util::format("ANY integers == %1", i), 1);
        verify_query(test_context, t, util::format("SOME integers == %1", i), 1);
        verify_query(test_context, t, util::format("ALL integers == %1", i), 1);
        verify_query(test_context, t, util::format("NONE integers == %1", i), num_objects - 1);
        verify_query(test_context, t, util::format("!(ANY integers == %1)", i), num_objects - 1);
        verify_query(test_context, t, util::format("!(SOME integers == %1)", i), num_objects - 1);
        verify_query(test_context, t, util::format("!(ALL integers == %1)", i), num_objects - 1);
        verify_query(test_context, t, util::format("!(NONE integers == %1)", i), 1);
        verify_query(test_context, t, util::format("ANY integers != %1", i), num_objects - 1);
        verify_query(test_context, t, util::format("SOME integers != %1", i), num_objects - 1);
        verify_query(test_context, t, util::format("ALL integers != %1", i), num_objects - 1);
        verify_query(test_context, t, util::format("NONE integers != %1", i), 1);
        verify_query(test_context, t, util::format("%1 IN integers", i), 1);
    }
    verify_query(test_context, t, "integers.@count == 0", 0);
    verify_query(test_context, t, "integers.@size == 0", 0);
    verify_query(test_context, t, "integers.@count == 1", num_objects);
    verify_query(test_context, t, "integers.@size == 1", num_objects);

    // add two more objects; one with defaults, one with null in the list
    Obj obj_defaults = t->create_object();
    Obj obj_nulls_in_lists = t->create_object();
    obj_nulls_in_lists.get_list<Optional<Int>>(col_int_list_nullable).add(Optional<Int>());
    num_objects += 2;
    verify_query(test_context, t, "integers.@count == 0", 2);
    verify_query(test_context, t, "integers == NULL", 0);
    verify_query(test_context, t, "ALL integers == NULL", 2); // the two empty lists match ALL
    verify_query(test_context, t, "NONE integers == NULL", num_objects);
    verify_query(test_context, t, "integers_nullable.@count == 0", 1);
    verify_query(test_context, t, "integers_nullable == NULL", 1);
    verify_query(test_context, t, "ALL integers_nullable == NULL",
                 2); // matches the empty list and the list containing one NULL
    verify_query(test_context, t, "NONE integers_nullable == NULL", num_objects - 1);
    // list vs property
    verify_query(test_context, t, "integers == single_int", num_objects - 2);
    verify_query(test_context, t, "integers_nullable == single_int", num_objects - 2);
    verify_query(test_context, t, "integers == single_int_nullable", num_objects - 2);
    verify_query(test_context, t, "integers_nullable == single_int_nullable", num_objects - 1);
    // aggregate vs property x nullable
    verify_query(test_context, t, "integers.@min == single_int", num_objects - 2); // two empty lists don't match
    verify_query(test_context, t, "integers.@min == single_int_nullable",
                 num_objects); // the min of 2 empty lists is null which matches the nullable int
    verify_query(test_context, t, "integers_nullable.@min == single_int",
                 num_objects - 2); // two empty lists don't match 0
    verify_query(test_context, t, "integers_nullable.@min == single_int_nullable",
                 num_objects); // the min of empty list matches null, and the min of only null matches null
    verify_query(test_context, t, "integers.@max == single_int", num_objects - 2); // two empty lists don't match 0s
    verify_query(test_context, t, "integers.@max == single_int_nullable",
                 num_objects); // the max of 2 empty lists is null which matches the null int
    verify_query(test_context, t, "integers_nullable.@max == single_int",
                 num_objects - 2); // max of null doesn't match 0
    verify_query(test_context, t, "integers_nullable.@max == single_int_nullable",
                 num_objects); // the max of an empty list matches null, and the max of only null matches null
    verify_query(test_context, t, "integers.@sum == single_int", num_objects); // sum of an empty list matches 0
    verify_query(test_context, t, "integers.@sum == single_int_nullable",
                 num_objects - 2); // sum of empty list does not match null
    verify_query(test_context, t, "integers_nullable.@sum == single_int",
                 num_objects); // sum of empty list matches 0, sum of list containing null matches 0
    verify_query(test_context, t, "integers_nullable.@sum == single_int_nullable",
                 num_objects -
                     2); // sum of empty list does not match null, sum of list containing null does not match null
    verify_query(test_context, t, "integers.@avg == single_int",
                 num_objects - 2); // avg of empty lists is null, does not match 0
    verify_query(test_context, t, "integers.@avg == single_int_nullable",
                 num_objects); // avg of empty lists matches null
    verify_query(test_context, t, "integers_nullable.@avg == single_int",
                 num_objects - 2); // avg of empty list is null does not match 0, avg of list containing null is not 0
    verify_query(test_context, t, "integers_nullable.@avg == single_int_nullable",
                 num_objects); // avg of empty list matches null, avg of list containing null matches null
    verify_query(test_context, t, "integers.@count == single_int",
                 2 + 1); // 2x count of empty list matches 0, count of {1} matches 1
    verify_query(test_context, t, "integers.@count == single_int_nullable", 1); // count of empty list matches 0
    verify_query(test_context, t, "integers_nullable.@count == single_int",
                 1 + 1); // count of {1} matches 1, count of empty list matches 0
    verify_query(test_context, t, "integers_nullable.@count == single_int_nullable", 1); // count of {1} matches 1
    // operations across links
    verify_query(test_context, t2, "link.integers == 0 || link.integers == 3", 0);
    verify_query(test_context, t2, "link.integers == 1", 1);
    verify_query(test_context, t2, "link.integers == 2", 1);
    verify_query(test_context, t2, "link.integers == NULL", 0);
    verify_query(test_context, t2, "link.integers_nullable == NULL", 0);
    verify_query(test_context, t2, "link.integers.@count == 1", 2);
    verify_query(test_context, t2, "link.integers.@count == 0", 1);
    verify_query(test_context, t2, "link.integers.@min == 1", 1);
    verify_query(test_context, t2, "link.integers.@max == 1", 1);
    verify_query(test_context, t2, "link.integers.@sum == 1", 1);
    verify_query(test_context, t2, "link.integers.@avg == 1", 1);
    // operations across lists
    verify_query(test_context, t2, "list.integers == 1", 2);
    verify_query(test_context, t2, "list.integers == 2", 1);
    verify_query(test_context, t2, "list.integers == NULL", 0);
    verify_query(test_context, t2, "list.integers.@count == 1", 2);
    verify_query(test_context, t2, "list.integers.@min == 1", 2);
    verify_query(test_context, t2, "list.integers.@max == 1", 2);
    verify_query(test_context, t2, "list.integers.@avg == 1", 2);
    verify_query(test_context, t2, "list.integers.@sum == 1", 2);
    verify_query(test_context, t2, "list.integers.@min == 1", 2);
    // aggregate operators across multiple lists
    // we haven't supported aggregates across multiple lists previously
    // but the implementation works and applies the aggregate to the primitive list
    // this may be surprising, but it is one way to interpret the expression
    verify_query(test_context, t2, "ALL list.integers == 1", 2);  // row 0 matches {1}. row 1 matches (any of) {} {1}
    verify_query(test_context, t2, "NONE list.integers == 1", 1); // row 1 matches (any of) {}, {0}, {2}, {3} ...

    Obj obj0 = *t->begin();
    util::Any args[] = {Int(1)};
    size_t num_args = 1;
    verify_query_sub(test_context, t, "integers == $0", args, num_args, 1);

    std::string message;
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "integers.@min.no_property == 0", 0), message);
    CHECK_EQUAL(message, "Operation '.@min' cannot apply to property 'integers' because it is not a list");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "SUBQUERY(integers, $x, $x == 1).@count > 0", 0),
                                message);
    CHECK_EQUAL(message, "A subquery can not operate on a list of primitive values (property 'integers')");
    // list vs list is not implemented yet
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "integers == integers", 0), message);
    CHECK_EQUAL(message,
                "Ordered comparison between two primitive lists is not implemented yet ('integers' and 'integers')");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "integers != integers", 0), message);
    CHECK_EQUAL(message,
                "Ordered comparison between two primitive lists is not implemented yet ('integers' and 'integers')");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "integers > integers", 0), message);
    CHECK_EQUAL(message,
                "Ordered comparison between two primitive lists is not implemented yet ('integers' and 'integers')");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "integers < integers", 0), message);
    CHECK_EQUAL(message,
                "Ordered comparison between two primitive lists is not implemented yet ('integers' and 'integers')");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "integers contains integers", 0), message);
    CHECK_EQUAL(message,
                "Ordered comparison between two primitive lists is not implemented yet ('integers' and 'integers')");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "integers beginswith integers", 0), message);
    CHECK_EQUAL(message,
                "Ordered comparison between two primitive lists is not implemented yet ('integers' and 'integers')");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "integers endswith integers", 0), message);
    CHECK_EQUAL(message,
                "Ordered comparison between two primitive lists is not implemented yet ('integers' and 'integers')");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "integers like integers", 0), message);
    CHECK_EQUAL(message,
                "Ordered comparison between two primitive lists is not implemented yet ('integers' and 'integers')");
    // string operators are not supported on an integer column
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "integers like 0", 0), message);
    CHECK_EQUAL(
        message,
        "Unsupported comparison operator 'like' against type 'int', right side must be a string or binary type");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "integers contains[c] 0", 0), message);
    CHECK_EQUAL(
        message,
        "Unsupported comparison operator 'contains' against type 'int', right side must be a string or binary type");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "integers beginswith 0", 0), message);
    CHECK_EQUAL(message, "Unsupported comparison operator 'beginswith' against type 'int', right side must be a "
                         "string or binary type");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "integers ENDSWITH 0", 0), message);
    CHECK_EQUAL(
        message,
        "Unsupported comparison operator 'endswith' against type 'int', right side must be a string or binary type");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "integers == 'string'", 0), message);
    CHECK_EQUAL(message, "Cannot convert 'string' to a number");
}

TEST(Parser_list_of_primitive_strings)
{
    Group g;
    TableRef t = g.add_table("table");

    constexpr bool nullable = true;
    auto col_str_list = t->add_column_list(type_String, "strings", nullable);
    CHECK_THROW_ANY(t->add_search_index(col_str_list));

    auto get_string = [](size_t i) -> std::string {
        return util::format("string_%1", i);
    };
    size_t num_populated_objects = 10;
    for (size_t i = 0; i < num_populated_objects; ++i) {
        Obj obj = t->create_object();
        std::string si = get_string(i);
        obj.get_list<String>(col_str_list).add(si);
    }
    Obj obj_empty_list = t->create_object();
    Obj obj_with_null = t->create_object();
    obj_with_null.get_list<String>(col_str_list).add(StringData(realm::null()));
    Obj obj_with_empty_string = t->create_object();
    obj_with_empty_string.get_list<String>(col_str_list).add("");
    size_t num_special_objects = 3;
    size_t num_total_objects = num_populated_objects + num_special_objects;

    for (size_t i = 0; i < num_populated_objects; ++i) {
        std::string si = get_string(i);
        verify_query(test_context, t, util::format("strings == '%1'", si), 1);
        verify_query(test_context, t, util::format("ANY strings == '%1'", si), 1);
        verify_query(test_context, t, util::format("SOME strings == '%1'", si), 1);
        verify_query(test_context, t, util::format("ALL strings == '%1'", si), 2); // empty list also matches
        verify_query(test_context, t, util::format("NONE strings == '%1'", si), num_total_objects - 1);
        verify_query(test_context, t, util::format("!(ANY strings == '%1')", si), num_total_objects - 1);
        verify_query(test_context, t, util::format("!(SOME strings == '%1')", si), num_total_objects - 1);
        verify_query(test_context, t, util::format("!(ALL strings == '%1')", si),
                     num_total_objects - 2); // empty list also does not match
        verify_query(test_context, t, util::format("!(NONE strings == '%1')", si), 1);
        verify_query(test_context, t, util::format("ANY strings != '%1'", si),
                     num_total_objects - 2); // empty list also does not match
        verify_query(test_context, t, util::format("SOME strings != '%1'", si),
                     num_total_objects - 2); // empty list also does not match
        verify_query(test_context, t, util::format("ALL strings != '%1'", si), num_total_objects - 1);
        verify_query(test_context, t, util::format("NONE strings != '%1'", si), 2); // empty list also matches
        verify_query(test_context, t, util::format("'%1' IN strings", si), 1);
        verify_query(test_context, t, util::format("strings CONTAINS[c] '%1'", si), 1);
        verify_query(test_context, t, util::format("strings BEGINSWITH[c] '%1'", si), 1);
        verify_query(test_context, t, util::format("strings ENDSWITH[c] '%1'", si), 1);
        verify_query(test_context, t, util::format("strings LIKE[c] '%1'", si), 1);
    }
    verify_query(test_context, t, "strings CONTAINS[c] 'STR'", num_populated_objects);
    verify_query(test_context, t, "strings BEGINSWITH[c] 'STR'", num_populated_objects);
    verify_query(test_context, t, "strings ENDSWITH[c] 'G_1'", 1);
    verify_query(test_context, t, "strings LIKE[c] 'StRiNg_*'", num_populated_objects);
    verify_query(test_context, t, "ALL strings CONTAINS[c] 'STR'", num_populated_objects + 1);   // + empty list
    verify_query(test_context, t, "ALL strings BEGINSWITH[c] 'STR'", num_populated_objects + 1); // + empty list
    verify_query(test_context, t, "ALL strings ENDSWITH[c] 'G_1'", 2);                          // {"string_1"} and {}
    verify_query(test_context, t, "ALL strings LIKE[c] 'StRiNg_*'", num_populated_objects + 1); // + empty list
    verify_query(test_context, t, "NONE strings CONTAINS[c] 'STR'", 3);                         // {}, {null}, {""}
    verify_query(test_context, t, "NONE strings BEGINSWITH[c] 'STR'", 3);                       // {}, {null}, {""}
    verify_query(test_context, t, "NONE strings ENDSWITH[c] 'G_1'",
                 num_populated_objects - 1 + 3);                         // - {"string_1"} + {}, {null}, {""}
    verify_query(test_context, t, "NONE strings LIKE[c] 'StRiNg_*'", 3); // {}, {null}, {""}

    verify_query(test_context, t, "strings.@count == 0", 1);                     // {}
    verify_query(test_context, t, "strings.@size == 0", 1);                      // {}
    verify_query(test_context, t, "strings.@count == 1", num_total_objects - 1); // - empty list
    verify_query(test_context, t, "strings.@size == 1", num_total_objects - 1);  // - empty list
    verify_query(test_context, t, "strings.length == 0", 2);                     // {""}, {null}
    verify_query(test_context, t, "strings.length == 8", num_populated_objects); // "strings_0", ...  "strings_9"

    CHECK_THROW(verify_query(test_context, t, "strings.@min == 2", 0), query_parser::InvalidQueryError);
    CHECK_THROW(verify_query(test_context, t, "strings.@max == 2", 0), query_parser::InvalidQueryError);
    CHECK_THROW(verify_query(test_context, t, "strings.@sum == 2", 0), query_parser::InvalidQueryError);
    CHECK_THROW(verify_query(test_context, t, "strings.@avg == 2", 0), query_parser::InvalidQueryError);
}

TEST_TYPES(Parser_list_of_primitive_element_lengths, StringData, BinaryData)
{
    Group g;
    TableRef t = g.add_table("table");

    constexpr bool nullable = true;
    auto col_list = t->add_column_list(ColumnTypeTraits<TEST_TYPE>::id, "values", nullable);
    t->add_column(type_Int, "length"); // "length" is still a usable column name
    auto col_link = t->add_column(*t, "link");

    Obj obj_empty_list = t->create_object();
    Obj obj_with_null = t->create_object();
    TEST_TYPE null_value;
    CHECK(null_value.is_null());
    obj_with_null.get_list<TEST_TYPE>(col_list).add(null_value);
    Obj obj_with_empty_string = t->create_object();
    TEST_TYPE empty_value("", 0);
    CHECK_EQUAL(empty_value.size(), 0);
    CHECK_EQUAL(empty_value.is_null(), false);
    obj_with_empty_string.get_list<TEST_TYPE>(col_list).add(empty_value);
    std::string value1 = "value1";
    std::string value2 = "value2";
    TEST_TYPE v1(value1);
    TEST_TYPE v2(value2);
    Obj obj_with_v1 = t->create_object();
    obj_with_v1.get_list<TEST_TYPE>(col_list).add(v1);
    Obj obj_with_v2 = t->create_object();
    obj_with_v2.get_list<TEST_TYPE>(col_list).add(v2);
    Obj obj_with_v1_v2 = t->create_object();
    obj_with_v1_v2.get_list<TEST_TYPE>(col_list).add(v1);
    obj_with_v1_v2.get_list<TEST_TYPE>(col_list).add(v2);

    for (auto it = t->begin(); it != t->end(); ++it) {
        it->set<ObjKey>(col_link, it->get_key());
    }

    // repeat the same tests but over links, the tests are only the same because the links are self cycles
    std::vector<std::string> column_prefix = {"", "link.", "link.link."};

    for (auto path : column_prefix) {
        // {}, {null}, {""}, {"value1"}, {"value2"}, {"value1", "value2"}
        verify_query(test_context, t, util::format("%1values.@count == 0", path), 1);
        verify_query(test_context, t, util::format("%1values.@size == 0", path), 1);
        verify_query(test_context, t, util::format("%1values.@count == 1", path), 4);
        verify_query(test_context, t, util::format("%1values.@size == 1", path), 4);
        verify_query(test_context, t, util::format("%1values.@count == 2", path), 1);
        verify_query(test_context, t, util::format("%1values.@size == 2", path), 1);
        verify_query(test_context, t, util::format("%1length == 0", path), 6);
        verify_query(test_context, t, util::format("%1link == null", path), 0);
        verify_query(test_context, t, util::format("%1values == null", path), 1);

        std::vector<std::string> any_prefix = {"", "ANY", "SOME", "any", "some"};
        for (auto prefix : any_prefix) {
            verify_query(test_context, t, util::format("0 IN %1 %2values.length", prefix, path), 2);
            verify_query(test_context, t, util::format("%1 %2values.length == 0", prefix, path), 2);
            verify_query(test_context, t, util::format("%1 %2values.length > 0", prefix, path), 3);
            verify_query(test_context, t, util::format("%1 %2values.length == 6", prefix, path), 3);
            verify_query(test_context, t, util::format("%1 %2values.length == length", prefix, path),
                         2); // element length vs column
        }

        verify_query(test_context, t, util::format("ALL %1values.length == 0", path), 3);      // {}, {null}, {""}
        verify_query(test_context, t, util::format("ALL %1values.length == length", path), 3); // {}, {null}, {""}
        verify_query(test_context, t, util::format("ALL %1values.length == 6", path), 4); // the empty list matches

        verify_query(test_context, t, util::format("NONE %1values.length == 0", path),
                     4); // {}, {"value1"}, {"value2"}, {"value1", "value2"}
        verify_query(test_context, t, util::format("NONE %1values.length == length", path),
                     4); // {}, {"value1"}, {"value2"}, {"value1", "value2"}
        verify_query(test_context, t, util::format("NONE %1values.length == 6", path), 3); // {}, {null}, {""}
    }

    std::string message;
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "values.len == 2", 0), message);
    CHECK_EQUAL(message, "Property 'values' in 'table' is not an Object");
}

TEST_TYPES(Parser_list_of_primitive_types, Prop<Int>, Nullable<Int>, Prop<Bool>, Nullable<Bool>, Prop<Float>,
           Nullable<Float>, Prop<Double>, Nullable<Double>, Prop<Decimal128>, Nullable<Decimal128>, Prop<ObjectId>,
           Nullable<ObjectId>, Prop<UUID>, Nullable<UUID>, Prop<Timestamp>, Nullable<Timestamp>)
{
    Group g;
    TableRef t = g.add_table("table");
    TestValueGenerator gen;

    using underlying_type = typename TEST_TYPE::underlying_type;
    using type = typename TEST_TYPE::type;
    constexpr bool is_nullable = TEST_TYPE::is_nullable;
    ColKey col = t->add_column_list(TEST_TYPE::data_type, "values", is_nullable);
    auto col_link = t->add_column(*t, "link");

    auto obj1 = t->create_object();
    std::vector<type> values = gen.values_from_int<type>({0, 9, 4, 2, 7, 4, 1, 8, 11, 3, 4, 5, 22});
    obj1.set_list_values(col, values);
    auto obj2 = t->create_object(); // empty list
    auto obj3 = t->create_object(); // {1}
    underlying_type value_1 = gen.convert_for_test<underlying_type>(1);
    obj3.get_list<type>(col).add(value_1);
    auto obj4 = t->create_object(); // {1, 1}
    obj4.get_list<type>(col).add(value_1);
    obj4.get_list<type>(col).add(value_1);
    auto obj5 = t->create_object(); // {null} or {0}
    obj5.get_list<type>(col).add(TEST_TYPE::default_value());

    for (auto it = t->begin(); it != t->end(); ++it) {
        it->set<ObjKey>(col_link, it->get_key()); // self links
    }

    // repeat the same tests but over links, the tests are only the same because the links are self cycles
    std::vector<std::string> column_prefix = {"", "link.", "link.link."};

    for (auto path : column_prefix) {
        verify_query(test_context, t, util::format("%1values.@count == 0", path), 1);  // obj2
        verify_query(test_context, t, util::format("%1values.@count == 1", path), 2);  // obj3, obj5
        verify_query(test_context, t, util::format("%1values.@count == 2", path), 1);  // obj4
        verify_query(test_context, t, util::format("%1values.@count > 0", path), 4);   // obj1, obj3, obj4, obj5
        verify_query(test_context, t, util::format("%1values.@count == 13", path), 1); // obj1
        verify_query(test_context, t, util::format("%1values == NULL", path), (is_nullable ? 1 : 0)); // obj5

        util::Any args[] = {value_1};
        size_t num_args = 1;
        size_t num_matching_value_1 = 3;                       // obj1, obj3, obj4
        size_t num_not_matching_value_1 = 2;                   // obj1, obj5
        size_t num_all_matching_value_1 = 3;                   // obj2, obj3, obj4
        size_t num_all_not_matching_value_1 = 2;               // obj2, obj5
        size_t num_none_matching_value_1 = 2;                  // obj2, obj5
        size_t num_none_not_matching_value_1 = 3;              // obj2, obj3, obj4
        if constexpr (std::is_same_v<underlying_type, bool>) { // bool reuses values
            num_matching_value_1 = is_nullable ? 3 : 4;
            num_not_matching_value_1 = is_nullable ? 2 : 1;
            num_all_matching_value_1 = is_nullable ? 3 : 4;
            num_all_not_matching_value_1 = is_nullable ? 2 : 1;
            num_none_matching_value_1 = is_nullable ? 2 : 1;
            num_none_not_matching_value_1 = is_nullable ? 3 : 4;
        }
        verify_query_sub(test_context, t, util::format("%1values == $0", path), args, num_args, num_matching_value_1);
        verify_query_sub(test_context, t, util::format("%1values != $0", path), args, num_args,
                         num_not_matching_value_1);
        verify_query_sub(test_context, t, util::format("ANY %1values == $0", path), args, num_args,
                         num_matching_value_1);
        verify_query_sub(test_context, t, util::format("ANY %1values != $0", path), args, num_args,
                         num_not_matching_value_1);

        verify_query_sub(test_context, t, util::format("ALL %1values == $0", path), args, num_args,
                         num_all_matching_value_1);
        verify_query_sub(test_context, t, util::format("ALL %1values != $0", path), args, num_args,
                         num_all_not_matching_value_1);
        verify_query_sub(test_context, t, util::format("NONE %1values == $0", path), args, num_args,
                         num_none_matching_value_1);
        verify_query_sub(test_context, t, util::format("NONE %1values != $0", path), args, num_args,
                         num_none_not_matching_value_1);
    }
    std::string message;
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "missing.length == 2", 0), message);
    CHECK_EQUAL(message, "'table' has no property: 'missing'");
    if constexpr (realm::is_any_v<underlying_type, StringData, BinaryData>) {
        verify_query(test_context, t, "values.length == 0", 1);
    }
    else {
        CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "values.length == 2", 0), message);
        CHECK_EQUAL(message, "Property 'values' in 'table' is not an Object");
    }
}

TEST(Parser_list_of_primitive_mixed)
{
    Group g;
    TableRef t = g.add_table("table");

    constexpr bool nullable = true;
    auto col_list = t->add_column_list(type_Mixed, "values", nullable);
    CHECK_THROW_ANY(t->add_search_index(col_list));

    Obj obj_empty_list = t->create_object();
    auto empty_list = obj_empty_list.get_list<Mixed>(col_list);
    CHECK_EQUAL(empty_list.min(), Mixed{});
    CHECK_EQUAL(empty_list.max(), Mixed{});
    CHECK_EQUAL(empty_list.sum(), Mixed{0});
    CHECK_EQUAL(empty_list.avg(), Mixed{});

    Obj obj_with_null = t->create_object();
    auto list_with_null = obj_with_null.get_list<Mixed>(col_list);
    list_with_null.add(Mixed{});
    CHECK_EQUAL(list_with_null.min(), Mixed{});
    CHECK_EQUAL(list_with_null.max(), Mixed{});
    CHECK_EQUAL(list_with_null.sum(), Mixed{0});
    CHECK_EQUAL(list_with_null.avg(), Mixed{});

    Obj obj_with_empty_string = t->create_object();
    auto empty_string_list = obj_with_empty_string.get_list<Mixed>(col_list);
    empty_string_list.add(Mixed{""});
    CHECK_EQUAL(empty_string_list.min(), Mixed{""});
    CHECK_EQUAL(empty_string_list.max(), Mixed{""});
    CHECK_EQUAL(empty_string_list.sum(), Mixed{0});
    CHECK_EQUAL(empty_string_list.avg(), Mixed{});

    Obj obj_with_ints = t->create_object();
    auto ints_list = obj_with_ints.get_list<Mixed>(col_list);
    ints_list.add(Mixed{0});
    ints_list.add(Mixed{1});
    ints_list.add(Mixed{2});
    CHECK_EQUAL(ints_list.min(), Mixed{0});
    CHECK_EQUAL(ints_list.max(), Mixed{2});
    CHECK_EQUAL(ints_list.sum(), Mixed{3});
    CHECK_EQUAL(ints_list.avg(), Mixed{1});

    Obj obj_with_numerics = t->create_object();
    auto numeric_list = obj_with_numerics.get_list<Mixed>(col_list);
    numeric_list.add(Mixed{1});
    numeric_list.add(Mixed{Decimal128(2.2)});
    numeric_list.add(Mixed{float(3.3f)});
    numeric_list.add(Mixed{double(4.4)});
    CHECK_EQUAL(numeric_list.min(), Mixed{1});
    CHECK_EQUAL(numeric_list.max(), Mixed{4.4});
    CHECK_EQUAL(numeric_list.sum(), Mixed{10.9});
    CHECK_EQUAL(numeric_list.avg(), Mixed{2.725});

    Obj obj_with_strings = t->create_object();
    auto strings_list = obj_with_strings.get_list<Mixed>(col_list);
    strings_list.add(Mixed{"one"});
    strings_list.add(Mixed{"two"});
    strings_list.add(Mixed{"three"});
    strings_list.add(Mixed{""});
    strings_list.add(Mixed{StringData{}});
    CHECK_EQUAL(strings_list.min(), Mixed{""});
    CHECK_EQUAL(strings_list.max(), Mixed{"two"});
    CHECK_EQUAL(strings_list.sum(), Mixed(0));
    CHECK_EQUAL(strings_list.avg(), Mixed());

    Obj obj_with_mixed_types = t->create_object();
    auto mixed_list = obj_with_mixed_types.get_list<Mixed>(col_list);
    mixed_list.add(Mixed{"foo"});
    mixed_list.add(Mixed{1});
    mixed_list.add(Mixed{Timestamp(1, 0)});
    mixed_list.add(Mixed{Decimal128(2.5)});
    mixed_list.add(Mixed{float(3.7)});
    mixed_list.add(Mixed{ObjectId::gen()});
    mixed_list.add(Mixed{UUID()});
    mixed_list.add(Mixed{});
    mixed_list.add(Mixed{false});
    mixed_list.add(Mixed{true});
    mixed_list.add(Mixed{null::get_null_float<float>()});
    mixed_list.add(Mixed{null::get_null_float<double>()});
    mixed_list.add(Mixed{Decimal128{realm::null()}});
    mixed_list.add(Mixed{Decimal128{StringData{}}}); // NaN
    CHECK_EQUAL(mixed_list.min(), Mixed(false));
    CHECK_EQUAL(mixed_list.max(), Mixed(UUID()));
    CHECK_EQUAL(mixed_list.sum(), Mixed(7.2));
    CHECK_EQUAL(mixed_list.avg(), Mixed(2.4));

    verify_query(test_context, t, "values.@count == 0", 1);
    verify_query(test_context, t, "values.@size == 1", 2);
    verify_query(test_context, t, "ANY values == NULL", 3);
    verify_query(test_context, t, "ALL values == NULL", 2);
    verify_query(test_context, t, "ALL values == NULL && values.@size > 0", 1);
    verify_query(test_context, t, "NONE values == NULL", 4);
    verify_query(test_context, t, "NONE values == NULL && values.@size > 0", 3);
    verify_query(test_context, t, "ANY values == 'one'", 1);
    verify_query(test_context, t, "ANY values CONTAINS[c] 'O'", 2);
    verify_query(test_context, t, "values.length == 3", 2); // string lengths
    verify_query(test_context, t, "ANY values == false", 1);
    verify_query(test_context, t, "ANY values == true", 1);
    verify_query(test_context, t, "values.@type == 'string'", 3);
    verify_query(test_context, t, "values == T1:0", 1);
    verify_query(test_context, t, "values.@sum > 0", 3);
    verify_query(test_context, t, "values.@sum == 0", 4);
    verify_query(test_context, t, "values.@sum == 3", 1);
    verify_query(test_context, t, "values.@sum == 10.9", 1);
    verify_query(test_context, t, "values.@sum == 7.2", 1);
    verify_query(test_context, t, "values.@avg == 1", 1);
    verify_query(test_context, t, "values.@avg == 2.725", 1);
    verify_query(test_context, t, "values.@avg == 2.4", 1);
    verify_query(test_context, t, "values.@min == false", 1);
    verify_query(test_context, t, "values.@min == 1", 1);
    verify_query(test_context, t, "values.@max == 2", 1);
    verify_query(test_context, t, "values.@max == 4.4", 1);
    verify_query(test_context, t, "values.@max == uuid(00000000-0000-0000-0000-000000000000)", 1);
}

TEST(Parser_SortAndDistinctSerialisation)
{
    Group g;
    TableRef people = g.add_table("person");
    TableRef accounts = g.add_table("account");

    ColKey name_col = people->add_column(type_String, "name");
    ColKey age_col = people->add_column(type_Int, "age");
    ColKey account_col = people->add_column(*accounts, "account");

    ColKey balance_col = accounts->add_column(type_Double, "balance");
    ColKey transaction_col = accounts->add_column(type_Int, "num_transactions");

    Obj account0 = accounts->create_object();
    account0.set(balance_col, 50.55);
    account0.set(transaction_col, 2);
    Obj account1 = accounts->create_object();
    account1.set(balance_col, 175.23);
    account1.set(transaction_col, 73);
    Obj account2 = accounts->create_object();
    account2.set(balance_col, 98.92);
    account2.set(transaction_col, 17);

    Obj person0 = people->create_object();
    person0.set(name_col, StringData("Adam"));
    person0.set(age_col, 28);
    Obj person1 = people->create_object();
    person1.set(name_col, StringData("Frank"));
    person1.set(age_col, 30);
    Obj person2 = people->create_object();
    person2.set(name_col, StringData("Ben"));
    person2.set(age_col, 18);

    // person:                      | account:
    // name     age     account     | balance       num_transactions
    // Adam     28      0 ->        | 50.55         2
    // Frank    30      1 ->        | 175.23        73
    // Ben      18      2 ->        | 98.92         17

    // sort serialisation
    TableView tv = people->where().find_all();
    tv.sort(name_col, false);
    tv.sort(age_col, true);
    tv.sort(SortDescriptor({{account_col, balance_col}, {account_col, transaction_col}}, {true, false}));
    std::string description = tv.get_descriptor_ordering_description();
    CHECK(description.find("SORT(account.balance ASC, account.num_transactions DESC, age ASC, name DESC)") !=
          std::string::npos);

    // distinct serialisation
    tv = people->where().find_all();
    tv.distinct(name_col);
    tv.distinct(age_col);
    tv.distinct(DistinctDescriptor({{account_col, balance_col}, {account_col, transaction_col}}));
    description = tv.get_descriptor_ordering_description();
    CHECK(description.find("DISTINCT(name) DISTINCT(age) DISTINCT(account.balance, account.num_transactions)") !=
          std::string::npos);

    // combined sort and distinct serialisation
    tv = people->where().find_all();
    tv.distinct(DistinctDescriptor({{name_col}, {age_col}}));
    tv.sort(SortDescriptor({{account_col, balance_col}, {account_col, transaction_col}}, {true, false}));
    description = tv.get_descriptor_ordering_description();
    CHECK(description.find("DISTINCT(name, age)") != std::string::npos);
    CHECK(description.find("SORT(account.balance ASC, account.num_transactions DESC)") != std::string::npos);
}

TableView get_sorted_view(TableRef t, std::string query_string, query_parser::KeyPathMapping mapping = {})
{
    Query q = t->query(query_string, {}, mapping);
    std::string query_description = q.get_description(mapping.get_backlink_class_prefix());
    Query q2 = t->query(query_description, {}, mapping);
    return q2.find_all();
}

TEST(Parser_SortAndDistinct)
{
    Group g;
    TableRef people = g.add_table("person");
    TableRef accounts = g.add_table("account");

    ColKey name_col = people->add_column(type_String, "name");
    ColKey age_col = people->add_column(type_Int, "age");
    ColKey account_col = people->add_column(*accounts, "account");

    ColKey balance_col = accounts->add_column(type_Double, "balance");
    ColKey transaction_col = accounts->add_column(type_Int, "num_transactions");

    Obj account0 = accounts->create_object();
    account0.set(balance_col, 50.55);
    account0.set(transaction_col, 2);
    Obj account1 = accounts->create_object();
    account1.set(balance_col, 50.55);
    account1.set(transaction_col, 73);
    Obj account2 = accounts->create_object();
    account2.set(balance_col, 98.92);
    account2.set(transaction_col, 17);

    Obj p1 = people->create_object();
    p1.set(name_col, StringData("Adam"));
    p1.set(age_col, 28);
    p1.set(account_col, account0.get_key());
    Obj p2 = people->create_object();
    p2.set(name_col, StringData("Frank"));
    p2.set(age_col, 30);
    p2.set(account_col, account1.get_key());
    Obj p3 = people->create_object();
    p3.set(name_col, StringData("Ben"));
    p3.set(age_col, 28);
    p3.set(account_col, account2.get_key());

    query_parser::KeyPathMapping mapping;
    mapping.add_mapping(people, "sol_rotations", "age");
    mapping.add_mapping(people, "nominal_identifier", "name");
    mapping.add_mapping(people, "holdings", "account");
    mapping.add_mapping(accounts, "funds", "balance");
    mapping.add_mapping(accounts, "sum_of_actions", "num_transactions");

    // person:                      | account:
    // name     age     account     | balance       num_transactions
    // Adam     28      0 ->        | 50.55         2
    // Frank    30      1 ->        | 50.55         73
    // Ben      28      2 ->        | 98.92         17

    {
        auto check_tv = [&](TableView tv) {
            for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
                CHECK(tv.get(row_ndx - 1).get<Int>(age_col) <= tv.get(row_ndx).get<Int>(age_col));
            }
        };
        check_tv(get_sorted_view(people, "age > 0 SORT(age ASC)"));
        check_tv(get_sorted_view(people, "sol_rotations > 0 SORT(sol_rotations ASC)", mapping));
    }

    {
        auto check_tv = [&](TableView tv) {
            for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
                CHECK(tv.get(row_ndx - 1).get<Int>(age_col) >= tv.get(row_ndx).get<Int>(age_col));
            }
        };
        check_tv(get_sorted_view(people, "age > 0 SORT(age DESC)"));
        check_tv(get_sorted_view(people, "sol_rotations > 0 SORT(sol_rotations DESC)", mapping));
    }

    {
        auto check_tv = [&](TableView tv) {
            CHECK_EQUAL(tv.size(), 3);
            CHECK_EQUAL(tv.get(0).get<String>(name_col), "Ben");
            CHECK_EQUAL(tv.get(1).get<String>(name_col), "Adam");
            CHECK_EQUAL(tv.get(2).get<String>(name_col), "Frank");
        };
        check_tv(get_sorted_view(people, "age > 0 SORT(age ASC, name DESC)"));
        check_tv(
            get_sorted_view(people, "sol_rotations > 0 SORT(sol_rotations ASC, nominal_identifier DESC)", mapping));
    }

    {
        auto check_tv = [&](TableView tv) {
            for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
                ObjKey link_ndx1 = tv.get(row_ndx - 1).get<ObjKey>(account_col);
                ObjKey link_ndx2 = tv.get(row_ndx).get<ObjKey>(account_col);
                CHECK(accounts->get_object(link_ndx1).get<double>(balance_col) <=
                      accounts->get_object(link_ndx2).get<double>(balance_col));
            }
        };
        check_tv(get_sorted_view(people, "TRUEPREDICATE SORT(account.balance ascending)", mapping));
        check_tv(get_sorted_view(people, "TRUEPREDICATE SORT(holdings.funds ascending)", mapping));
        check_tv(get_sorted_view(people, "TRUEPREDICATE SORT(account.funds ascending)", mapping));
        check_tv(get_sorted_view(people, "TRUEPREDICATE SORT(holdings.balance ascending)", mapping));
    }

    {
        auto check_tv = [&](TableView tv) {
            for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
                ObjKey link_ndx1 = tv.get(row_ndx - 1).get<ObjKey>(account_col);
                ObjKey link_ndx2 = tv.get(row_ndx).get<ObjKey>(account_col);
                CHECK(accounts->get_object(link_ndx1).get<double>(balance_col) >=
                      accounts->get_object(link_ndx2).get<double>(balance_col));
            }
        };
        check_tv(get_sorted_view(people, "TRUEPREDICATE SORT(account.balance descending)", mapping));
        check_tv(get_sorted_view(people, "TRUEPREDICATE SORT(holdings.funds descending)", mapping));
    }

    {
        auto check_tv = [&](TableView tv) {
            CHECK_EQUAL(tv.size(), 2);
            for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
                CHECK(tv.get(row_ndx - 1).get<Int>(age_col) != tv.get(row_ndx).get<Int>(age_col));
            }
        };
        check_tv(get_sorted_view(people, "TRUEPREDICATE DISTINCT(age)"));
        check_tv(get_sorted_view(people, "TRUEPREDICATE DISTINCT(sol_rotations)", mapping));
    }

    {
        auto check_tv = [&](TableView tv) {
            CHECK_EQUAL(tv.size(), 3);
            CHECK_EQUAL(tv.get(0).get<String>(name_col), "Adam");
            CHECK_EQUAL(tv.get(1).get<String>(name_col), "Frank");
            CHECK_EQUAL(tv.get(2).get<String>(name_col), "Ben");
        };
        check_tv(get_sorted_view(people, "TRUEPREDICATE DISTINCT(age, account.balance)", mapping));
        check_tv(get_sorted_view(people, "TRUEPREDICATE DISTINCT(sol_rotations, holdings.funds)", mapping));
    }

    {
        auto check_tv = [&](TableView tv) {
            CHECK_EQUAL(tv.size(), 1);
            CHECK_EQUAL(tv.get(0).get<String>(name_col), "Adam");
        };
        check_tv(get_sorted_view(people, "TRUEPREDICATE DISTINCT(age) DISTINCT(account.balance)"));
        check_tv(get_sorted_view(people, "TRUEPREDICATE DISTINCT(sol_rotations) DISTINCT(holdings.funds)", mapping));
    }

    {
        auto check_tv = [&](TableView tv) {
            CHECK_EQUAL(tv.size(), 2);
            CHECK_EQUAL(tv.get(0).get<Int>(age_col), 28);
            CHECK_EQUAL(tv.get(1).get<Int>(age_col), 30);
        };
        check_tv(get_sorted_view(people, "TRUEPREDICATE SORT(age ASC) DISTINCT(age)"));
        check_tv(get_sorted_view(people, "TRUEPREDICATE SORT(sol_rotations ASC) DISTINCT(sol_rotations)", mapping));
    }

    {
        auto check_tv = [&](TableView tv) {
            CHECK_EQUAL(tv.size(), 2);
            CHECK_EQUAL(tv.get(0).get<String>(name_col), "Ben");
            CHECK_EQUAL(tv.get(1).get<String>(name_col), "Frank");
        };
        check_tv(
            get_sorted_view(people, "TRUEPREDICATE SORT(name DESC) DISTINCT(age) SORT(name ASC) DISTINCT(name)"));
        check_tv(get_sorted_view(people,
                                 "TRUEPREDICATE SORT(nominal_identifier DESC) DISTINCT(sol_rotations) "
                                 "SORT(nominal_identifier ASC) DISTINCT(nominal_identifier)",
                                 mapping));
    }

    {
        auto check_tv = [&](TableView tv) {
            CHECK_EQUAL(tv.size(), 2);
            CHECK_EQUAL(tv.get(0).get<String>(name_col), "Ben");
            CHECK_EQUAL(tv.get(1).get<String>(name_col), "Frank");
        };
        check_tv(get_sorted_view(people, "account.num_transactions > 10 SORT(name ASC)"));
        check_tv(get_sorted_view(people, "holdings.sum_of_actions > 10 SORT(nominal_identifier ASC)", mapping));
    }

    std::string message;
    CHECK_THROW_ANY_GET_MESSAGE(get_sorted_view(people, "TRUEPREDICATE DISTINCT(balance)"), message);
    CHECK_EQUAL(message, "No property 'balance' found on object type 'person' specified in 'distinct' clause");

    CHECK_THROW_ANY_GET_MESSAGE(get_sorted_view(people, "TRUEPREDICATE sort(account.name ASC)"), message);
    CHECK_EQUAL(message, "No property 'name' found on object type 'account' specified in 'sort' clause");
}

TEST(Parser_Limit)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history());
    auto sg = DB::create(*hist, path, DBOptions(crypt_key()));

    auto wt = sg->start_write();
    TableRef people = wt->add_table("person");

    auto name_col = people->add_column(type_String, "name");
    people->add_column(type_Int, "age");

    people->create_object().set_all("Adam", 28);
    people->create_object().set_all("Frank", 30);
    people->create_object().set_all("Ben", 28);

    // solely limit
    TableView tv = get_sorted_view(people, "TRUEPREDICATE LIMIT(0)");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 3);
    tv = get_sorted_view(people, "TRUEPREDICATE LIMIT(1)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 2);
    tv = get_sorted_view(people, "TRUEPREDICATE LIMIT(2)");
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 1);
    tv = get_sorted_view(people, "TRUEPREDICATE LIMIT(3)");
    CHECK_EQUAL(tv.size(), 3);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    tv = get_sorted_view(people, "TRUEPREDICATE LIMIT(4)");
    CHECK_EQUAL(tv.size(), 3);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);

    // sort + limit
    tv = get_sorted_view(people, "TRUEPREDICATE SORT(name ASC) LIMIT(0)");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 3);
    tv = get_sorted_view(people, "TRUEPREDICATE SORT(name ASC) LIMIT(1)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 2);
    CHECK_EQUAL(tv[0].get<String>(name_col), "Adam");
    tv = get_sorted_view(people, "TRUEPREDICATE SORT(name ASC) LIMIT(2)");
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 1);
    CHECK_EQUAL(tv[0].get<String>(name_col), "Adam");
    CHECK_EQUAL(tv[1].get<String>(name_col), "Ben");
    tv = get_sorted_view(people, "TRUEPREDICATE SORT(name ASC) LIMIT(3)");
    CHECK_EQUAL(tv.size(), 3);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    CHECK_EQUAL(tv[0].get<String>(name_col), "Adam");
    CHECK_EQUAL(tv[1].get<String>(name_col), "Ben");
    CHECK_EQUAL(tv[2].get<String>(name_col), "Frank");
    tv = get_sorted_view(people, "TRUEPREDICATE SORT(name ASC) LIMIT(4)");
    CHECK_EQUAL(tv.size(), 3);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);

    // sort + distinct + limit
    tv = get_sorted_view(people, "TRUEPREDICATE SORT(name ASC) DISTINCT(age) LIMIT(0)");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 2);
    tv = get_sorted_view(people, "TRUEPREDICATE SORT(name ASC) DISTINCT(age) LIMIT(1)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 1);
    CHECK_EQUAL(tv[0].get<String>(name_col), "Adam");
    tv = get_sorted_view(people, "TRUEPREDICATE SORT(name ASC) DISTINCT(age) LIMIT(2)");
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    CHECK_EQUAL(tv[0].get<String>(name_col), "Adam");
    CHECK_EQUAL(tv[1].get<String>(name_col), "Frank");
    tv = get_sorted_view(people, "TRUEPREDICATE SORT(name ASC) DISTINCT(age) LIMIT(3)");
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    CHECK_EQUAL(tv[0].get<String>(name_col), "Adam");
    CHECK_EQUAL(tv[1].get<String>(name_col), "Frank");
    tv = get_sorted_view(people, "TRUEPREDICATE SORT(name ASC) DISTINCT(age) LIMIT(4)");
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);

    // query + limit
    tv = get_sorted_view(people, "age < 30 SORT(name ASC) DISTINCT(age) LIMIT(0)");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 1);
    tv = get_sorted_view(people, "age < 30 SORT(name ASC) DISTINCT(age) LIMIT(1)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    CHECK_EQUAL(tv[0].get<String>(name_col), "Adam");
    tv = get_sorted_view(people, "age < 30 SORT(name ASC) DISTINCT(age) LIMIT(2)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    CHECK_EQUAL(tv[0].get<String>(name_col), "Adam");
    tv = get_sorted_view(people, "age < 30 SORT(name ASC) DISTINCT(age) LIMIT(3)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    CHECK_EQUAL(tv[0].get<String>(name_col), "Adam");
    tv = get_sorted_view(people, "age < 30 SORT(name ASC) DISTINCT(age) LIMIT(4)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);

    // compound query + limit
    tv = get_sorted_view(people, "age < 30 && name == 'Adam' LIMIT(0)");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 1);
    tv = get_sorted_view(people, "age < 30 && name == 'Adam' LIMIT(1)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    CHECK_EQUAL(tv[0].get<String>(name_col), "Adam");

    // limit multiple times, order matters
    tv = get_sorted_view(people, "TRUEPREDICATE LIMIT(2) LIMIT(1)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 2);
    tv = get_sorted_view(people, "TRUEPREDICATE LIMIT(3) LIMIT(2) LIMIT(1) LIMIT(10)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 2);
    tv = get_sorted_view(people, "age > 0 SORT(name ASC) LIMIT(2)");
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 1);
    CHECK_EQUAL(tv[0].get<String>(name_col), "Adam");
    CHECK_EQUAL(tv[1].get<String>(name_col), "Ben");
    tv = get_sorted_view(people, "age > 0 LIMIT(2) SORT(name ASC)");
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 1);
    CHECK_EQUAL(tv[0].get<String>(name_col), "Adam");
    CHECK_EQUAL(tv[1].get<String>(name_col), "Frank");
    tv = get_sorted_view(people, "age > 0 SORT(name ASC) LIMIT(2) DISTINCT(age)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 1); // the other result is excluded by distinct not limit
    tv = get_sorted_view(people, "age > 0 SORT(name DESC) LIMIT(2) SORT(age ASC) LIMIT(1)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 2);
    CHECK_EQUAL(tv[0].get<String>(name_col), "Ben");

    // size_unlimited() checks
    tv = get_sorted_view(people, "age == 30");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    tv = get_sorted_view(people, "age == 30 LIMIT(0)");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 1);
    tv = get_sorted_view(people, "age == 1000");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    tv = get_sorted_view(people, "age == 1000 LIMIT(0)");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    tv = get_sorted_view(people, "age == 1000 SORT(name ASC)");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    tv = get_sorted_view(people, "age == 1000 SORT(name ASC) LIMIT(0)");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    tv = get_sorted_view(people, "age == 28 SORT(name ASC)");
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    tv = get_sorted_view(people, "age == 28 SORT(name ASC) LIMIT(1)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 1);
    tv = get_sorted_view(people, "age == 28 DISTINCT(age)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    tv = get_sorted_view(people, "age == 28 DISTINCT(age) LIMIT(0)");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 1);
    tv = get_sorted_view(people, "age == 28 SORT(name ASC) DISTINCT(age)");
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    tv = get_sorted_view(people, "age == 28 SORT(name ASC) DISTINCT(age) LIMIT(0)");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 1);
    tv = get_sorted_view(people, "FALSEPREDICATE");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    tv = get_sorted_view(people, "FALSEPREDICATE LIMIT(0)");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);
    tv = get_sorted_view(people, "FALSEPREDICATE LIMIT(1)");
    CHECK_EQUAL(tv.size(), 0);
    CHECK_EQUAL(tv.get_num_results_excluded_by_limit(), 0);

    // errors
    CHECK_THROW_ANY(get_sorted_view(people, "TRUEPREDICATE LIMIT(-1)"));    // only accepting positive integers
    CHECK_THROW_ANY(get_sorted_view(people, "TRUEPREDICATE LIMIT(age)"));   // only accepting positive integers
    CHECK_THROW_ANY(get_sorted_view(people, "TRUEPREDICATE LIMIT('age')")); // only accepting positive integers

    wt->commit();

    // handover
    auto reader = sg->start_read();
    ConstTableRef peopleRead = reader->get_table("person");

    TableView items = peopleRead->where().find_all();
    CHECK_EQUAL(items.size(), 3);
    realm::DescriptorOrdering desc;
    CHECK(!desc.will_apply_limit());
    desc.append_limit(1);
    CHECK(desc.will_apply_limit());
    items.apply_descriptor_ordering(desc);
    CHECK_EQUAL(items.size(), 1);

    auto tr = reader->duplicate();
    auto tv2 = tr->import_copy_of(items, PayloadPolicy::Copy);
    CHECK(tv2->is_attached());
    CHECK(tv2->is_in_sync());
    CHECK_EQUAL(tv2->size(), 1);
}


TEST(Parser_Backlinks)
{
    Group g;

    TableRef items = g.add_table("class_Items");
    ColKey item_name_col = items->add_column(type_String, "name");
    ColKey item_price_col = items->add_column(type_Double, "price");
    using item_t = std::pair<std::string, double>;
    std::vector<item_t> item_info = {
        {"milk", 5.5}, {"oranges", 4.0}, {"pizza", 9.5}, {"cereal", 6.5}, {"bread", 3.5}};
    std::vector<ObjKey> item_keys;
    items->create_objects(item_info.size(), item_keys);
    for (size_t i = 0; i < item_info.size(); ++i) {
        Obj row_obj = items->get_object(item_keys[i]);
        item_t cur_item = item_info[i];
        row_obj.set(item_name_col, StringData(cur_item.first));
        row_obj.set(item_price_col, cur_item.second);
    }

    TableRef t = g.add_table("class_Person");
    ColKey id_col = t->add_column(type_Int, "customer_id");
    ColKey name_col = t->add_column(type_String, "name");
    ColKey account_col = t->add_column(type_Double, "account_balance");
    ColKey items_col = t->add_column_list(*items, "items");
    ColKey fav_col = t->add_column(*items, "fav item");

    TableRef things = g.add_table("class_class_with_policy");
    auto int_col = things->add_column(type_Int, "pascal_case");
    auto link_col = things->add_column(*things, "with_underscores");

    std::vector<ObjKey> people_keys;
    t->create_objects(3, people_keys);
    for (size_t i = 0; i < people_keys.size(); ++i) {
        Obj obj = t->get_object(people_keys[i]);
        obj.set(id_col, int64_t(i));
        obj.set(account_col, double((i + 1) * 10.0));
        obj.set(fav_col, obj.get_key());
        if (i == 0) {
            obj.set(name_col, StringData("Adam"));
            LnkLst list_0 = obj.get_linklist(items_col);
            list_0.add(item_keys[0]);
            list_0.add(item_keys[1]);
            list_0.add(item_keys[2]);
            list_0.add(item_keys[3]);
        }
        else if (i == 1) {
            obj.set(name_col, StringData("James"));
            LnkLst list_1 = obj.get_linklist(items_col);
            for (size_t j = 0; j < 10; ++j) {
                list_1.add(item_keys[0]);
            }
        }
        else if (i == 2) {
            obj.set(name_col, StringData("John"));
            LnkLst list_2 = obj.get_linklist(items_col);
            list_2.add(item_keys[2]);
            list_2.add(item_keys[2]);
            list_2.add(item_keys[3]);
        }
    }

    {
        auto obj1 = things->create_object().set(int_col, 1);
        auto obj2 = things->create_object().set(int_col, 2);
        auto obj3 = things->create_object().set(int_col, 3);
        obj3.set(link_col, obj2.get_key());
    }
    Query q = items->backlink(*t, fav_col).column<Double>(account_col) > 20;
    CHECK_EQUAL(q.count(), 1);
    std::string desc = q.get_description();
    CHECK(desc.find("@links.class_Person.fav\\ item.account_balance") != std::string::npos);

    q = items->backlink(*t, items_col).column<Double>(account_col) > 20;
    CHECK_EQUAL(q.count(), 2);
    desc = q.get_description();
    CHECK(desc.find("@links.class_Person.items.account_balance") != std::string::npos);

    // favourite items bought by people who have > 20 in their account
    verify_query(test_context, items, "@links.class_Person.fav\\ item.account_balance > 20", 1); // backlinks via link
    // items bought by people who have > 20 in their account
    verify_query(test_context, items, "@links.class_Person.items.account_balance > 20", 2); // backlinks via list
    // items bought by people who have 'J' as the first letter of their name
    verify_query(test_context, items, "@links.class_Person.items.name LIKE[c] 'j*'", 3);
    verify_query(test_context, items, "@links.class_Person.items.name BEGINSWITH 'J'", 3);

    // items purchased more than twice
    verify_query(test_context, items, "@links.class_Person.items.@count > 2", 2);
    verify_query(test_context, items, "@links.class_Person.items.@size > 2", 2);
    // items bought by people with only $10 in their account
    verify_query(test_context, items, "@links.class_Person.items.@min.account_balance <= 10", 4);
    // items bought by people with more than $10 in their account
    verify_query(test_context, items, "@links.class_Person.items.@max.account_balance > 10", 3);
    // items bought where the sum of the account balance of purchasers is more than $20
    verify_query(test_context, items, "@links.class_Person.items.@sum.account_balance > 20", 3);
    verify_query(test_context, items, "@links.class_Person.items.@avg.account_balance > 20", 1);
    // subquery over backlinks
    verify_query(test_context, items, "SUBQUERY(@links.class_Person.items, $x, $x.account_balance >= 20).@count > 2",
                 1);

    // backlinks over link
    // people having a favourite item which is also the favourite item of another person
    verify_query(test_context, t, "fav\\ item.@links.class_Person.fav\\ item.@count > 1", 0);
    // people having a favourite item which is purchased more than once (by anyone)
    verify_query(test_context, t, "fav\\ item.@links.class_Person.items.@count > 1 ", 2);

    std::string message;
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, items, "@links.class_Person.items == NULL", 1), message);
    CHECK_EQUAL(message, "Cannot compare linklist ('@links.class_Person.items') with NULL");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, items, "@links.class_Person.fav\\ item == NULL", 1),
                                message);
    CHECK_EQUAL(message, "Cannot compare linklist ('@links.class_Person.fav\\ item') with NULL");
    CHECK_THROW_ANY(verify_query(test_context, items, "@links.attr. > 0", 1));

    // check that arbitrary aliasing for named backlinks works
    query_parser::KeyPathMapping mapping;
    mapping.add_mapping(items, "purchasers", "@links.class_Person.items");
    mapping.add_mapping(t, "money", "account_balance");
    mapping.add_table_mapping(t, "my-custom-class-name");

    verify_query(test_context, items, "purchasers.@count > 2", 2, mapping);
    verify_query(test_context, items, "purchasers.@max.money >= 20", 3, mapping);
    verify_query(test_context, items, "@links.my-custom-class-name.items.@count > 2", 2, mapping);

    // check that arbitrary aliasing for named backlinks works with a arbitrary prefix
    query_parser::KeyPathMapping mapping_with_prefix;
    mapping_with_prefix.set_backlink_class_prefix("class_");
    mapping_with_prefix.add_mapping(items, "purchasers", "@links.Person.items");
    mapping_with_prefix.add_mapping(t, "things", "items");
    mapping_with_prefix.add_mapping(t, "money", "account_balance");
    mapping_with_prefix.add_mapping(t, "funds", "money");     // double indirection
    mapping_with_prefix.add_mapping(t, "capital", "capital"); // self loop
    mapping_with_prefix.add_mapping(t, "banknotes", "finances");
    mapping_with_prefix.add_mapping(t, "finances", "banknotes"); // indirect loop
    mapping_with_prefix.add_mapping(things, "parents", "@links.class_with_policy.with_underscores");
    CHECK(mapping_with_prefix.add_table_mapping(t, "CustomPersonClassName"));
    CHECK(!mapping_with_prefix.add_table_mapping(t, t->get_name()));

    verify_query(test_context, items, "purchasers.@count > 2", 2, mapping_with_prefix);
    verify_query(test_context, items, "purchasers.@max.money >= 20", 3, mapping_with_prefix);
    // double substitution via subquery "$x"->"" and "money"->"account_balance"
    verify_query(test_context, items, "SUBQUERY(purchasers, $x, $x.money >= 20).@count > 2", 1, mapping_with_prefix);
    // double indirection is allowed
    verify_query(test_context, items, "purchasers.@max.funds >= 20", 3, mapping_with_prefix);
    // verbose backlinks syntax
    verify_query(test_context, items, "@links.Person.items.@count > 2", 2, mapping_with_prefix);
    // class name substitution
    verify_query(test_context, items, "@links.CustomPersonClassName.items.@count > 2", 2, mapping_with_prefix);
    // property translation
    verify_query(test_context, items, "@links.Person.things.@count > 2", 2, mapping_with_prefix);
    // class and property translation
    verify_query(test_context, items, "@links.CustomPersonClassName.things.@count > 2", 2, mapping_with_prefix);
    // Check that mapping works for tables named "class_class..."
    verify_query(test_context, things, "parents.pascal_case == 3", 1, mapping_with_prefix);

    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, items, "@links.Factory.items > 0", 1, mapping_with_prefix),
                                message);
    CHECK_EQUAL(message, "No property 'items' found in type 'Factory' which links to type 'Items'");
    CHECK_THROW_ANY_GET_MESSAGE(
        verify_query(test_context, items, "@links.Person.artifacts > 0", 1, mapping_with_prefix), message);
    CHECK_EQUAL(message, "No property 'artifacts' found in type 'Person' which links to type 'Items'");

    // verbose backlinks syntax with 'class_' prefix not allowed
    CHECK_THROW_ANY(
        verify_query(test_context, items, "@links.class_Person.items.@count > 2", 2, mapping_with_prefix));

    // infinite loops are detected
    CHECK_THROW_ANY_GET_MESSAGE(
        verify_query(test_context, items, "purchasers.@max.banknotes >= 20", 3, mapping_with_prefix), message);
    CHECK_EQUAL(message,
                "Substitution loop detected while processing 'finances' -> 'banknotes' found in type 'Person'");
    CHECK_THROW_ANY_GET_MESSAGE(
        verify_query(test_context, items, "purchasers.@max.capital >= 20", 3, mapping_with_prefix), message);
    CHECK_EQUAL(message, "Substitution loop detected while processing 'capital' -> 'capital' found in type 'Person'");
}

TEST(Parser_BacklinkCount)
{
    Group g;

    TableRef items = g.add_table("class_Items");
    items->add_column(type_Int, "item_id");
    auto item_link_col = items->add_column(*items, "self");
    items->add_column(type_Double, "double_col");

    std::vector<int64_t> item_ids{5, 2, 12, 14, 20};
    ObjKeyVector item_keys(item_ids);
    for (size_t i = 0; i < item_keys.size(); ++i) {
        items->create_object(item_keys[i]).set_all(item_ids[i], item_keys[i], double(i) + 0.5);
    }
    items->get_object(item_keys[4]).set(item_link_col, null_key); // last item will have a total of 0 backlinks

    TableRef t = g.add_table("class_Person");
    auto id_col = t->add_column(type_Int, "customer_id");
    auto items_col = t->add_column_list(*items, "items");
    auto fav_col = t->add_column(*items, "fav_item");
    auto float_col = t->add_column(type_Float, "float_col");

    for (int i = 0; i < 3; ++i) {
        Obj obj = t->create_object();
        obj.set(id_col, i);
        obj.set(fav_col, item_keys[2 - i]);
        obj.set(float_col, float(i) + 0.5f);
    }

    auto it = t->begin();
    auto list_0 = it->get_linklist(items_col);
    list_0.add(item_keys[0]);
    list_0.add(item_keys[1]);
    list_0.add(item_keys[2]);

    ++it;
    auto list_1 = it->get_linklist(items_col);
    for (size_t i = 0; i < 10; ++i) {
        list_1.add(item_keys[0]);
    }

    ++it;
    auto list_2 = it->get_linklist(items_col);
    list_2.add(item_keys[2]);
    list_2.add(item_keys[2]);

    verify_query(test_context, items, "@links.@count == 0", 1);
    verify_query(test_context, items, "@links.@count == 0 && item_id == 20", 1);
    verify_query(test_context, items, "@links.@count == 1", 1);
    verify_query(test_context, items, "@links.@count == 1 && item_id == 14", 1);
    verify_query(test_context, items, "@links.@count == 5", 1);
    verify_query(test_context, items, "@links.@count == 5 && item_id == 12", 1);
    verify_query(test_context, items, "@links.@count == 3", 1);
    verify_query(test_context, items, "@links.@count == 3 && item_id == 2", 1);
    verify_query(test_context, items, "@links.@count == 13", 1);
    verify_query(test_context, items, "@links.@count == 13 && item_id == 5", 1);

    // @size is still a synonym to @count
    verify_query(test_context, items, "@links.@size == 0", 1);
    verify_query(test_context, items, "@links.@size == 0 && item_id == 20", 1);

    // backlink count through forward links
    verify_query(test_context, t, "fav_item.@links.@count == 5 && fav_item.item_id == 12", 1);
    verify_query(test_context, t, "fav_item.@links.@count == 3 && fav_item.item_id == 2", 1);
    verify_query(test_context, t, "fav_item.@links.@count == 13 && fav_item.item_id == 5", 1);

    // backlink count through lists; the semantics are to sum the backlinks for each connected row
    verify_query(test_context, t, "items.@links.@count == 21 && customer_id == 0", 1);  // 13 + 3 + 5
    verify_query(test_context, t, "items.@links.@count == 130 && customer_id == 1", 1); // 13 * 10
    verify_query(test_context, t, "items.@links.@count == 10 && customer_id == 2", 1);  // 5 + 5

    // backlink count through backlinks first
    verify_query(test_context, items, "@links.class_Items.self.@links.@count == 1 && item_id == 14", 1);
    verify_query(test_context, items, "@links.class_Person.items.@links.@count == 0", 5);

    // backlink count through backlinks and forward links
    verify_query(test_context, items, "@links.class_Person.fav_item.items.@links.@count == 130 && item_id == 2", 1);
    verify_query(test_context, items, "@links.class_Person.fav_item.fav_item.@links.@count == 3 && item_id == 2", 1);

    // backlink count compared to int
    verify_query(test_context, items, "@links.@count == 0", 1);
    verify_query(test_context, items, "@links.@count >= item_id", 2); // 2 items have an id less than
                                                                      // their backlink count
    verify_query(test_context, items, "@links.@count >= @links.class_Person.fav_item.customer_id", 3);

    // backlink count compared to double
    verify_query(test_context, items, "@links.@count == 0.0", 1);
    verify_query(test_context, items, "@links.@count >= double_col", 3);

    // backlink count compared to float
    verify_query(test_context, items, "@links.@count >= @links.class_Person.fav_item.float_col", 3);

    // backlink count compared to link count
    verify_query(test_context, items, "@links.@count >= self.@count", 5);
    verify_query(test_context, t, "items.@count >= fav_item.@links.@count", 1); // second object

    // all backlinks count compared to single column backlink count
    // this is essentially checking if a single column contains all backlinks of a object
    verify_query(test_context, items, "@links.@count == @links.class_Person.fav_item.@count", 1); // item 5 (0 links)
    verify_query(test_context, items, "@links.@count == @links.class_Person.items.@count", 1);    // item 5 (0 links)
    verify_query(test_context, items, "@links.@count == @links.class_Items.self.@count", 2); // items 4,5 (1,0 links)

    std::string message;
    // backlink count requires comparison to a numeric type
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, items, "@links.@count == 'string'", -1), message);
    CHECK_EQUAL(message, "Cannot convert 'string' to a number");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, items, "@links.@count == 2018-04-09@14:21:0", -1),
                                message);
    CHECK_EQUAL(message, "Unsupported comparison between type 'int' and type 'timestamp'");

    // no suffix after @links.@count is allowed
    CHECK_THROW_ANY(verify_query(test_context, items, "@links.@count.item_id == 0", -1));
    CHECK_THROW_ANY(verify_query(test_context, items, "@links.@count.@avg.items_id == 0", -1));

    // other aggregate operators are not supported
    CHECK_THROW_ANY(verify_query(test_context, items, "@links.@avg == 1", -1));
    CHECK_THROW_ANY(verify_query(test_context, items, "@links.@sum == 1", -1));
    CHECK_THROW_ANY(verify_query(test_context, items, "@links.@min == 1", -1));
    CHECK_THROW_ANY(verify_query(test_context, items, "@links.@max == 1", -1));
    CHECK_THROW_ANY(verify_query(test_context, items, "@links.@avg.item_id == 1", -1));
    CHECK_THROW_ANY(verify_query(test_context, items, "@links.@sum.item_id == 1", -1));
    CHECK_THROW_ANY(verify_query(test_context, items, "@links.@min.item_id == 1", -1));
    CHECK_THROW_ANY(verify_query(test_context, items, "@links.@max.item_id == 1", -1));
}

TEST(Parser_BacklinksIndex)
{
    Group g;

    TableRef items = g.add_table("items");
    auto col_id = items->add_column(type_Int, "item_id");

    std::vector<int64_t> item_ids{5, 2, 12, 14, 20};
    ObjKeys item_keys(item_ids);
    for (size_t i = 0; i < item_keys.size(); ++i) {
        items->create_object(item_keys[i]).set(col_id, item_ids[i]);
    }

    auto person = g.add_table("person");
    auto col_age = person->add_column(type_Int, "age");
    person->add_search_index(col_age);
    auto col_link = person->add_column_list(*items, "owns");
    auto col_set = person->add_column_set(*items, "wish");
    auto col_dict = person->add_column_dictionary(*items, "borrowed");

    auto paul = person->create_object().set(col_age, 48);
    auto list = paul.get_linklist(col_link);
    list.add(item_keys[0]);
    list.add(item_keys[1]);
    auto set = paul.get_linkset(col_set);
    set.insert(item_keys[2]);
    set.insert(item_keys[3]);

    auto peter = person->create_object().set(col_age, 25);
    list = peter.get_linklist(col_link);
    list.add(item_keys[0]);
    list.add(item_keys[4]);
    auto dict = peter.get_dictionary(col_dict);
    dict.insert("Mary", Mixed(item_keys[3]));
    dict.insert("Paul", Mixed());

    verify_query(test_context, items, "@links.person.owns.age == 48", 2);
    verify_query(test_context, items, "@links.person.wish.age == 48", 2);
    verify_query(test_context, items, "@links.person.borrowed.age == 25", 1);
}


TEST(Parser_SubqueryVariableNames)
{
    Group g;
    util::serializer::SerialisationState test_state("");

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
    ColKey discount_name_col = discounts->add_column(type_String, "promotion", true);
    ColKey discount_off_col = discounts->add_column(type_Double, "reduced_by");
    ColKey discount_active_col = discounts->add_column(type_Bool, "active");

    using discount_t = std::pair<double, bool>;
    std::vector<discount_t> discount_info = {{3.0, false}, {2.5, true}, {0.50, true}, {1.50, true}};
    std::vector<ObjKey> discount_keys;
    discounts->create_objects(discount_info.size(), discount_keys);
    for (size_t i = 0; i < discount_keys.size(); ++i) {
        Obj obj = discounts->get_object(discount_keys[i]);
        obj.set(discount_off_col, discount_info[i].first);
        obj.set(discount_active_col, discount_info[i].second);
        if (i == 0) {
            obj.set(discount_name_col, StringData("back to school"));
        }
        else if (i == 1) {
            obj.set(discount_name_col, StringData("pizza lunch special"));
        }
        else if (i == 2) {
            obj.set(discount_name_col, StringData("manager's special"));
        }
    }

    TableRef ingredients = g.add_table("class_Allergens");
    ColKey ingredient_name_col = ingredients->add_column(type_String, "name");
    ColKey population_col = ingredients->add_column(type_Double, "population_affected");
    std::vector<std::pair<std::string, double>> ingredients_list = {
        {"dairy", 0.75}, {"nuts", 0.01}, {"wheat", 0.01}, {"soy", 0.005}};
    std::vector<ObjKey> ingredients_keys;
    ingredients->create_objects(ingredients_list.size(), ingredients_keys);
    for (size_t i = 0; i < ingredients_list.size(); ++i) {
        Obj obj = ingredients->get_object(ingredients_keys[i]);
        obj.set(ingredient_name_col, StringData(ingredients_list[i].first));
        obj.set(population_col, ingredients_list[i].second);
    }

    TableRef items = g.add_table("class_Items");
    ColKey item_name_col = items->add_column(type_String, "name");
    ColKey item_price_col = items->add_column(type_Double, "price");
    ColKey item_discount_col = items->add_column(*discounts, "discount");
    ColKey item_contains_col = items->add_column_list(*ingredients, "allergens");
    using item_t = std::pair<std::string, double>;
    std::vector<item_t> item_info = {{"milk", 5.5}, {"oranges", 4.0}, {"pizza", 9.5}, {"cereal", 6.5}};
    std::vector<ObjKey> item_keys;
    items->create_objects(item_info.size(), item_keys);
    for (size_t i = 0; i < item_info.size(); ++i) {
        Obj obj = items->get_object(item_keys[i]);
        obj.set(item_name_col, StringData(item_info[i].first));
        obj.set(item_price_col, item_info[i].second);
        if (i == 0) {
            obj.set(item_discount_col, discount_keys[2]); // milk -0.50
            LnkLst milk_contains = obj.get_linklist(item_contains_col);
            milk_contains.add(ingredients_keys[0]);
        }
        else if (i == 2) {
            obj.set(item_discount_col, discount_keys[1]); // pizza -2.5
            LnkLst pizza_contains = obj.get_linklist(item_contains_col);
            pizza_contains.add(ingredients_keys[0]);
            pizza_contains.add(ingredients_keys[2]);
            pizza_contains.add(ingredients_keys[3]);
        }
        else if (i == 3) {
            obj.set(item_discount_col, discount_keys[0]); // cereal -3.0 inactive
            LnkLst cereal_contains = obj.get_linklist(item_contains_col);
            cereal_contains.add(ingredients_keys[0]);
            cereal_contains.add(ingredients_keys[1]);
            cereal_contains.add(ingredients_keys[2]);
        }
    }

    TableRef t = g.add_table("class_Person");
    ColKey id_col = t->add_column(type_Int, "customer_id");
    ColKey account_col = t->add_column(type_Double, "account_balance");
    ColKey items_col = t->add_column_list(*items, "items");
    ColKey fav_col = t->add_column(*items, "fav_item");
    std::vector<ObjKey> people_keys;
    t->create_objects(3, people_keys);
    for (size_t i = 0; i < t->size(); ++i) {
        Obj obj = t->get_object(people_keys[i]);
        obj.set(id_col, int64_t(i));
        obj.set(account_col, double((i + 1) * 10.0));
        obj.set(fav_col, item_keys[i]);
        LnkLst list = obj.get_linklist(items_col);
        if (i == 0) {
            list.add(item_keys[0]);
            list.add(item_keys[1]);
            list.add(item_keys[2]);
            list.add(item_keys[3]);
        }
        else if (i == 1) {
            for (size_t j = 0; j < 10; ++j) {
                list.add(item_keys[0]);
            }
        }
        else if (i == 2) {
            list.add(item_keys[2]);
            list.add(item_keys[2]);
            list.add(item_keys[3]);
        }
    }


    Query sub = items->column<String>(item_name_col).contains("a") && items->column<Double>(item_price_col) > 5.0 &&
                items->link(item_discount_col).column<Double>(discount_off_col) > 0.5 &&
                items->column<Link>(item_contains_col).count() > 1;
    Query q = t->column<Link>(items_col, sub).count() > 1;

    std::string subquery_description = q.get_description("class_");
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
    verify_query(test_context, t, "SUBQUERY(items, $x, $x.name CONTAINS[c] 'MILK' && $x.price == 5.5).@count >= 1",
                 2);
    // compound subquery ||
    verify_query(test_context, t, "SUBQUERY(items, $x, $x.name CONTAINS[c] 'MILK' || $x.price >= 5.5).@count >= 1",
                 3);
    // variable name change
    verify_query(test_context, t,
                 "SUBQUERY(items, $anyNAME_-0123456789, 5.5 == $anyNAME_-0123456789.price ).@count > 0", 2);
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
    verify_query(test_context, t,
                 "SUBQUERY(items, $x, $x.discount.promotion CONTAINS[c] $x.name).@count < account_balance", 3);
    // subquery over link
    verify_query(test_context, t, "SUBQUERY(fav_item.allergens, $x, $x.name CONTAINS[c] 'dairy').@count > 0", 2);
    // nested subquery
    verify_query(test_context, t,
                 "SUBQUERY(items, $x, SUBQUERY($x.allergens, $allergy, $allergy.name CONTAINS[c] "
                 "'dairy').@count > 0).@count > 0",
                 3);
    // nested subquery operating on the same table with same variable is not allowed
    std::string message;
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t,
                                             "SUBQUERY(items, $x, "
                                             "SUBQUERY($x.discount.@links.class_Items.discount, $x, "
                                             "$x.price > 5).@count > 0).@count > 0",
                                             2),
                                message);
    CHECK_EQUAL(message, "Unable to create a subquery expression with variable '$x' since an identical variable "
                         "already exists in this context");

    // target property must be a list
    CHECK_THROW_ANY_GET_MESSAGE(
        verify_query(test_context, t, "SUBQUERY(account_balance, $x, TRUEPREDICATE).@count > 0", 3), message);
    CHECK_EQUAL(message, "A subquery must operate on a list property, but 'account_balance' is type 'double'");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "SUBQUERY(fav_item, $x, TRUEPREDICATE).@count > 0", 3),
                                message);
    CHECK_EQUAL(message, "A subquery must operate on a list property, but 'fav_item' is type 'link'");
}

TEST_TYPES(Parser_AggregateShortcuts, std::true_type, std::false_type)
{
    Group g;
    constexpr bool indexed_toggle = TEST_TYPE::value;
    TableRef allergens = g.add_table("class_Allergens");
    ColKey ingredient_name_col = allergens->add_column(type_String, "name");
    ColKey population_col = allergens->add_column(type_Double, "population_affected");
    std::vector<std::pair<std::string, double>> allergens_list = {
        {"dairy", 0.75}, {"nuts", 0.01}, {"wheat", 0.01}, {"soy", 0.005}};
    std::vector<ObjKey> allergens_keys;
    allergens->create_objects(allergens_list.size(), allergens_keys);
    for (size_t i = 0; i < allergens_list.size(); ++i) {
        Obj obj = allergens->get_object(allergens_keys[i]);
        obj.set(ingredient_name_col, StringData(allergens_list[i].first));
        obj.set(population_col, allergens_list[i].second);
    }

    TableRef items = g.add_table("class_Items");
    ColKey item_name_col = items->add_column(type_String, "name");
    ColKey item_price_col = items->add_column(type_Double, "price");
    ColKey item_contains_col = items->add_column_list(*allergens, "allergens");
    using item_t = std::pair<std::string, double>;
    std::vector<item_t> item_info = {{"milk", 5.5}, {"oranges", 4.0}, {"pizza", 9.5}, {"cereal", 6.5}};
    std::vector<ObjKey> items_keys;
    items->create_objects(item_info.size(), items_keys);
    for (size_t i = 0; i < item_info.size(); ++i) {
        Obj obj = items->get_object(items_keys[i]);
        obj.set(item_name_col, StringData(item_info[i].first));
        obj.set(item_price_col, item_info[i].second);
        if (i == 0) {
            LnkLst milk_contains = obj.get_linklist(item_contains_col);
            milk_contains.add(allergens_keys[0]);
        }
        else if (i == 2) {
            LnkLst pizza_contains = obj.get_linklist(item_contains_col);
            pizza_contains.add(allergens_keys[0]);
            pizza_contains.add(allergens_keys[2]);
            pizza_contains.add(allergens_keys[3]);
        }
        else if (i == 3) {
            LnkLst cereal_contains = obj.get_linklist(item_contains_col);
            cereal_contains.add(allergens_keys[0]);
            cereal_contains.add(allergens_keys[1]);
            cereal_contains.add(allergens_keys[2]);
        }
    }

    TableRef t = g.add_table("class_Person");
    ColKey id_col = t->add_column(type_Int, "customer_id");
    ColKey account_col = t->add_column(type_Double, "account_balance");
    ColKey items_col = t->add_column_list(*items, "items");
    ColKey fav_col = t->add_column(*items, "fav_item");
    std::vector<ObjKey> people_keys;
    t->create_objects(3, people_keys);
    for (size_t i = 0; i < people_keys.size(); ++i) {
        Obj obj = t->get_object(people_keys[i]);
        obj.set(id_col, int64_t(i));
        obj.set(account_col, double((i + 1) * 10.0));
        obj.set(fav_col, items_keys[i]);
        LnkLst list = obj.get_linklist(items_col);
        if (i == 0) {
            list.add(items_keys[0]);
            list.add(items_keys[1]);
            list.add(items_keys[2]);
            list.add(items_keys[3]);
        }
        else if (i == 1) {
            for (size_t j = 0; j < 10; ++j) {
                list.add(items_keys[0]);
            }
        }
        else if (i == 2) {
            list.add(items_keys[2]);
            list.add(items_keys[2]);
            list.add(items_keys[3]);
        }
    }

    if (indexed_toggle) {
        allergens->add_search_index(ingredient_name_col);
        items->add_search_index(item_name_col);
        t->add_search_index(id_col);
    }

    // any is implied over list properties
    verify_query(test_context, t, "items.price == 5.5", 2);

    // check basic equality
    verify_query(test_context, t, "ANY items.price == 5.5", 2);  // 0, 1
    verify_query(test_context, t, "SOME items.price == 5.5", 2); // 0, 1
    verify_query(test_context, t, "ALL items.price == 5.5", 1);  // 1
    verify_query(test_context, t, "NONE items.price == 5.5", 1); // 2

    // basic string equality
    verify_query(test_context, t, "ANY items.name == 'milk'", 2);  // 0, 1
    verify_query(test_context, t, "SOME items.name == 'milk'", 2); // 0, 1
    verify_query(test_context, t, "ALL items.name == 'milk'", 1);  // 1
    verify_query(test_context, t, "NONE items.name == 'milk'", 1); // 2

    // and
    verify_query(test_context, t, "customer_id > 0 and ANY items.price == 5.5", 1);
    verify_query(test_context, t, "customer_id > 0 and SOME items.price == 5.5", 1);
    verify_query(test_context, t, "customer_id > 0 and ALL items.price == 5.5", 1);
    verify_query(test_context, t, "customer_id > 0 and NONE items.price == 5.5", 1);
    // or
    verify_query(test_context, t, "customer_id > 1 or ANY items.price == 5.5", 3);
    verify_query(test_context, t, "customer_id > 1 or SOME items.price == 5.5", 3);
    verify_query(test_context, t, "customer_id > 1 or ALL items.price == 5.5", 2);
    verify_query(test_context, t, "customer_id > 1 or NONE items.price == 5.5", 1);
    // not
    verify_query(test_context, t, "!(ANY items.price == 5.5)", 1);
    verify_query(test_context, t, "!(SOME items.price == 5.5)", 1);
    verify_query(test_context, t, "!(ALL items.price == 5.5)", 2);
    verify_query(test_context, t, "!(NONE items.price == 5.5)", 2);

    // inside subquery people with any items containing WHEAT
    verify_query(test_context, t, "SUBQUERY(items, $x, $x.allergens.name CONTAINS[c] 'WHEAT').@count > 0", 2);
    verify_query(test_context, t, "SUBQUERY(items, $x, ANY $x.allergens.name CONTAINS[c] 'WHEAT').@count > 0", 2);
    verify_query(test_context, t, "SUBQUERY(items, $x, SOME $x.allergens.name CONTAINS[c] 'WHEAT').@count > 0", 2);
    verify_query(test_context, t, "SUBQUERY(items, $x, ALL $x.allergens.name CONTAINS[c] 'WHEAT').@count > 0", 1);
    verify_query(test_context, t, "SUBQUERY(items, $x, NONE $x.allergens.name CONTAINS[c] 'WHEAT').@count > 0", 2);

    // backlinks
    verify_query(test_context, items, "ANY @links.class_Person.items.account_balance > 15", 3);
    verify_query(test_context, items, "SOME @links.class_Person.items.account_balance > 15", 3);
    verify_query(test_context, items, "ALL @links.class_Person.items.account_balance > 15", 0);
    verify_query(test_context, items, "NONE @links.class_Person.items.account_balance > 15", 1);

    // links in prefix
    verify_query(test_context, t, "ANY fav_item.allergens.name CONTAINS 'dairy'", 2);
    verify_query(test_context, t, "SOME fav_item.allergens.name CONTAINS 'dairy'", 2);
    verify_query(test_context, t, "ALL fav_item.allergens.name CONTAINS 'dairy'", 2);
    verify_query(test_context, t, "NONE fav_item.allergens.name CONTAINS 'dairy'", 1);

    // links in suffix
    verify_query(test_context, items, "ANY @links.class_Person.items.fav_item.name CONTAINS 'milk'", 4);
    verify_query(test_context, items, "SOME @links.class_Person.items.fav_item.name CONTAINS 'milk'", 4);
    verify_query(test_context, items, "ALL @links.class_Person.items.fav_item.name CONTAINS 'milk'", 1);
    verify_query(test_context, items, "NONE @links.class_Person.items.fav_item.name CONTAINS 'milk'", 0);

    // compare with property
    verify_query(test_context, t, "ANY items.name == fav_item.name", 2);
    verify_query(test_context, t, "SOME items.name == fav_item.name", 2);
    verify_query(test_context, t, "ANY items.price == items.@max.price", 3);
    verify_query(test_context, t, "SOME items.price == items.@max.price", 3);
    verify_query(test_context, t, "ANY items.price == items.@min.price", 3);
    verify_query(test_context, t, "SOME items.price == items.@min.price", 3);
    verify_query(test_context, t, "ANY items.price > items.@avg.price", 2);
    verify_query(test_context, t, "SOME items.price > items.@avg.price", 2);

    // aggregate list compared with column (over links)
    verify_query(test_context, t, "ALL items.name == fav_item.name",
                 0); // no people have bought only their favourite item
    verify_query(test_context, t, "NONE items.name == fav_item.name",
                 1); // only person 1 has items which are not their favourite

    // ANY/SOME is not necessary but accepted
    verify_query(test_context, t, "ANY fav_item.name == 'milk'", 1);
    verify_query(test_context, t, "SOME fav_item.name == 'milk'", 1);

    // multiple lists in path is supported
    verify_query(test_context, t, "ANY items.allergens.name == 'dairy'", 3);
    verify_query(test_context, t, "SOME items.allergens.name == 'dairy'", 3);
    verify_query(test_context, t, "ALL items.allergens.name == 'dairy'", 1);
    verify_query(test_context, t, "NONE items.allergens.name == 'dairy'", 0);

    std::string message;
    // no list in path should throw
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "ALL fav_item.name == 'milk'", 1), message);
    CHECK_EQUAL(message, "The keypath following 'ALL' must contain a list");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, t, "NONE fav_item.name == 'milk'", 1), message);
    CHECK_EQUAL(message, "The keypath following 'NONE' must contain a list");

    // the expression following ANY/SOME/ALL/NONE must be a keypath list
    // currently this is restricted by the parser syntax so it is a predicate error
    CHECK_THROW_ANY(verify_query(test_context, t, "ANY 'milk' == fav_item.name", 1));
    CHECK_THROW_ANY(verify_query(test_context, t, "SOME 'milk' == fav_item.name", 1));
    CHECK_THROW_ANY(verify_query(test_context, t, "ALL 'milk' == fav_item.name", 1));
    CHECK_THROW_ANY(verify_query(test_context, t, "NONE 'milk' == fav_item.name", 1));
}

TEST(Parser_OperatorIN)
{
    Group g;

    TableRef allergens = g.add_table("class_Allergens");
    ColKey ingredient_name_col = allergens->add_column(type_String, "name");
    ColKey population_col = allergens->add_column(type_Double, "population_affected");
    std::vector<std::pair<std::string, double>> allergens_list = {
        {"dairy", 0.75}, {"nuts", 0.01}, {"wheat", 0.01}, {"soy", 0.005}};
    std::vector<ObjKey> allergens_keys;
    allergens->create_objects(allergens_list.size(), allergens_keys);
    for (size_t i = 0; i < allergens_list.size(); ++i) {
        Obj obj = allergens->get_object(allergens_keys[i]);
        obj.set(ingredient_name_col, StringData(allergens_list[i].first));
        obj.set(population_col, allergens_list[i].second);
    }

    TableRef items = g.add_table("class_Items");
    ColKey item_name_col = items->add_column(type_String, "name");
    ColKey item_price_col = items->add_column(type_Double, "price");
    ColKey item_contains_col = items->add_column_list(*allergens, "allergens");
    using item_t = std::pair<std::string, double>;
    std::vector<item_t> item_info = {{"milk", 5.5}, {"oranges", 4.0}, {"pizza", 9.5}, {"cereal", 6.5}};
    std::vector<ObjKey> items_keys;
    items->create_objects(item_info.size(), items_keys);
    for (size_t i = 0; i < item_info.size(); ++i) {
        Obj obj = items->get_object(items_keys[i]);
        obj.set(item_name_col, StringData(item_info[i].first));
        obj.set(item_price_col, item_info[i].second);
        if (i == 0) {
            LnkLst milk_contains = obj.get_linklist(item_contains_col);
            milk_contains.add(allergens_keys[0]);
        }
        else if (i == 2) {
            LnkLst pizza_contains = obj.get_linklist(item_contains_col);
            pizza_contains.add(allergens_keys[0]);
            pizza_contains.add(allergens_keys[2]);
            pizza_contains.add(allergens_keys[3]);
        }
        else if (i == 3) {
            LnkLst cereal_contains = obj.get_linklist(item_contains_col);
            cereal_contains.add(allergens_keys[0]);
            cereal_contains.add(allergens_keys[1]);
            cereal_contains.add(allergens_keys[2]);
        }
    }

    TableRef t = g.add_table("class_Person");
    ColKey id_col = t->add_column(type_Int, "customer_id");
    ColKey account_col = t->add_column(type_Double, "account_balance");
    ColKey items_col = t->add_column_list(*items, "items");
    ColKey fav_col = t->add_column(*items, "fav_item");
    std::vector<ObjKey> people_keys;
    t->create_objects(3, people_keys);
    for (size_t i = 0; i < people_keys.size(); ++i) {
        Obj obj = t->get_object(people_keys[i]);
        obj.set(id_col, int64_t(i));
        obj.set(account_col, double((i + 1) * 10.0));
        obj.set(fav_col, items_keys[i]);
        LnkLst list = obj.get_linklist(items_col);
        if (i == 0) {
            list.add(items_keys[0]);
            list.add(items_keys[1]);
            list.add(items_keys[2]);
            list.add(items_keys[3]);
        }
        else if (i == 1) {
            for (size_t j = 0; j < 10; ++j) {
                list.add(items_keys[0]);
            }
        }
        else if (i == 2) {
            list.add(items_keys[2]);
            list.add(items_keys[2]);
            list.add(items_keys[3]);
        }
    }

    verify_query(test_context, t, "5.5 IN items.price", 2);
    verify_query(test_context, t, "!(5.5 IN items.price)", 1);              // group not
    verify_query(test_context, t, "'milk' IN items.name", 2);               // string compare
    verify_query(test_context, t, "'MiLk' IN[c] items.name", 2);            // string compare with insensitivity
    verify_query(test_context, t, "NULL IN items.price", 0);                // null
    verify_query(test_context, t, "'dairy' IN fav_item.allergens.name", 2); // through link prefix
    verify_query(test_context, items, "20 IN @links.class_Person.items.account_balance", 1); // backlinks
    verify_query(test_context, t, "fav_item.price IN items.price", 2); // single property in list

    // aggregate modifiers must operate on a list
    CHECK_THROW(verify_query(test_context, t, "ANY 5.5 IN items.price", 2), query_parser::SyntaxError);
    CHECK_THROW(verify_query(test_context, t, "SOME 5.5 IN items.price", 2), query_parser::SyntaxError);
    CHECK_THROW(verify_query(test_context, t, "ALL 5.5 IN items.price", 1), query_parser::SyntaxError);
    CHECK_THROW(verify_query(test_context, t, "NONE 5.5 IN items.price", 1), query_parser::SyntaxError);

    CHECK_THROW_EX(verify_query(test_context, t, "items.price IN 5.5", 1), query_parser::InvalidQueryArgError,
                   CHECK_EQUAL(e.what(), "The keypath following 'IN' must contain a list"));
    CHECK_THROW_EX(verify_query(test_context, t, "5.5 in fav_item.price", 1), query_parser::InvalidQueryArgError,
                   CHECK_EQUAL(e.what(), "The keypath following 'IN' must contain a list"));
    verify_query(test_context, t, "'dairy' in items.allergens.name", 3);
    // list property vs list property is not supported by core yet
    CHECK_THROW_EX(
        verify_query(test_context, t, "items.price IN items.price", 0), query_parser::InvalidQueryError,
        CHECK_EQUAL(e.what(), "Comparison between two lists is not supported ('items.price' and 'items.price')"));
}

TEST(Parser_Object)
{
    Group g;
    TableRef table = g.add_table("table");
    auto link_col = table->add_column(*table, "link");
    auto linkx_col = table->add_column(*table, "linkx");
    ObjKeys keys;
    table->create_objects(3, keys);
    table->get_object(keys[0]).set(link_col, keys[1]).set(linkx_col, keys[1]);
    table->get_object(keys[1]).set(link_col, keys[1]);
    TableView tv = table->where().find_all();

    verify_query(test_context, table, "link == NULL", 1); // vanilla base check
    verify_query(test_context, table, "link == O1", 2);

    Query q0 = table->where().and_query(table->column<Link>(link_col) == tv.get(0));
    std::string description = q0.get_description(); // shouldn't throw
    CHECK(description.find("O0") != std::string::npos);

    Query q1 = table->column<Link>(link_col) == realm::null();
    description = q1.get_description(); // shouldn't throw
    CHECK(description.find("NULL") != std::string::npos);
    CHECK_EQUAL(q1.count(), 1);

    verify_query(test_context, table, "link == linkx", 2);
}


TEST(Parser_Between)
{
    Group g;
    TableRef table = g.add_table("table");
    auto int_col_key = table->add_column(type_Int, "age", true);
    auto between_col_key = table->add_column(type_Int, "between", true);
    for (int i = 0; i < 3; ++i) {
        table->create_object().set(int_col_key, i + 24).set(between_col_key, i);
    }

    // normal querying on a property named "between" is allowed.
    verify_query(test_context, table, "between == 0", 1);
    verify_query(test_context, table, "between > 0", 2);
    verify_query(test_context, table, "between <= 3", 3);

    verify_query(test_context, table, "age between {20, 25}", 1);
    CHECK_THROW_ANY(verify_query(test_context, table, "age between {20}", 1));
    CHECK_THROW_ANY(verify_query(test_context, table, "age between {20, 25, 34}", 1));
}

TEST(Parser_ChainedStringEqualQueries)
{
    Group g;
    TableRef table = g.add_table("table");
    ColKey a_col_ndx = table->add_column(type_String, "a", false);
    ColKey b_col_ndx = table->add_column(type_String, "b", true);
    ColKey c_col_ndx = table->add_column(type_String, "c", false);
    ColKey d_col_ndx = table->add_column(type_String, "d", true);

    table->add_search_index(c_col_ndx);
    table->add_search_index(d_col_ndx);

    std::vector<std::string> populated_data;
    std::stringstream ss;
    for (size_t i = 0; i < 100; ++i) {
        ss.str({});
        ss << i;
        std::string sd(ss.str());
        populated_data.push_back(sd);
        table->create_object().set(a_col_ndx, sd).set(b_col_ndx, sd).set(c_col_ndx, sd).set(d_col_ndx, sd);
    }
    table->create_object(); // one null/empty string

    verify_query(test_context, table, "a == '0' or a == '1' or a == '2'", 3);
    verify_query(test_context, table, "a == '0' or b == '2' or a == '3' or b == '4'", 4);
    verify_query(test_context, table,
                 "(a == '0' or b == '2' or a == '3' or b == '4') and (c == '0' or d == '2' or c == '3' or d == '4')",
                 4);
    verify_query(test_context, table, "a == '' or a == null", 1);
    verify_query(test_context, table, "b == '' or b == null", 1);
    verify_query(test_context, table, "c == '' or c == null", 1);
    verify_query(test_context, table, "d == '' or d == null", 1);
    verify_query(
        test_context, table,
        "(a == null or a == '') and (b == null or b == '') and (c == null or c == '') and (d == null or d == '')", 1);

    Random rd;
    rd.shuffle(populated_data.begin(), populated_data.end());
    std::string query;
    bool first = true;
    char column_to_query = 0;
    for (auto s : populated_data) {
        std::string column_name(1, 'a' + column_to_query);
        query += (first ? "" : " or ") + column_name + " == '" + s + "'";
        first = false;
        column_to_query = (column_to_query + 1) % 4;
    }
    verify_query(test_context, table, query, populated_data.size());
}

TEST(Parser_ChainedIntEqualQueries)
{
    Group g;
    TableRef table = g.add_table("table");
    auto a_col_key = table->add_column(type_Int, "a", false);
    auto b_col_key = table->add_column(type_Int, "b", true);
    auto c_col_key = table->add_column(type_Int, "c", false);
    auto d_col_key = table->add_column(type_Int, "d", true);

    table->add_search_index(c_col_key);
    table->add_search_index(d_col_key);

    std::vector<ObjKey> keys;
    table->create_objects(100, keys);
    std::vector<int64_t> populated_data;
    for (auto o = table->begin(); o != table->end(); ++o) {
        auto payload = o->get_key().value;
        populated_data.push_back(payload);
        o->set(a_col_key, payload);
        o->set(b_col_key, payload);
        o->set(c_col_key, payload);
        o->set(d_col_key, payload);
    }
    auto default_obj = table->create_object(); // one null/default 0 object

    verify_query(test_context, table, "a == NULL", 0);
    verify_query(test_context, table, "a == 0 or a == 1 or a == 2", 4);
    verify_query(test_context, table, "a == 1 or b == 2 or a == 3 or b == 4", 4);
    verify_query(test_context, table,
                 "(a == 0 or b == 2 or a == 3 or b == 4) and (c == 0 or d == 2 or c == 3 or d == 4)", 5);
    verify_query(test_context, table, "a == 0 or a == null", 2);
    verify_query(test_context, table, "b == 0 or b == null", 2);
    verify_query(test_context, table, "c == 0 or c == null", 2);
    verify_query(test_context, table, "d == 0 or d == null", 2);
    verify_query(
        test_context, table,
        "(a == null or a == 0) and (b == null or b == 0) and (c == null or c == 0) and (d == null or d == 0)", 2);

    Random rd;
    rd.shuffle(populated_data.begin(), populated_data.end());
    std::string query;
    bool first = true;
    char column_to_query = 0;
    for (auto s : populated_data) {
        std::string column_name(1, 'a' + column_to_query);
        std::stringstream ss;
        ss << s;
        query += (first ? "" : " or ") + column_name + " == '" + ss.str() + "'";
        first = false;
        column_to_query = (column_to_query + 1) % 4;
    }
    default_obj.remove();
    verify_query(test_context, table, query, populated_data.size());
}

TEST(Parser_TimestampNullable)
{
    Group g;
    TableRef table = g.add_table("table");
    ColKey a_col = table->add_column(type_Timestamp, "a", false);
    ColKey b_col = table->add_column(type_Timestamp, "b", false);
    table->create_object().set(a_col, Timestamp(7, 0)).set(b_col, Timestamp(17, 0));
    table->create_object().set(a_col, Timestamp(7, 0)).set(b_col, Timestamp(17, 0));

    Query q = table->where()
                  .equal(b_col, Timestamp(200, 0))
                  .group()
                  .equal(a_col, Timestamp(100, 0))
                  .Or()
                  .equal(a_col, Timestamp(realm::null()))
                  .end_group();
    std::string description = q.get_description();
    CHECK(description.find("NULL") != std::string::npos);
    CHECK_EQUAL(description, "b == T200:0 and (a == T100:0 or a == NULL)");
}


TEST(Parser_ObjectId)
{
    using util::serializer::print_value;
    Group g;
    auto table = g.add_table_with_primary_key("table", type_ObjectId, "id");
    auto nullable_oid_col_key = table->add_column(type_ObjectId, "nid", true);

    ObjectId generated_at_now = ObjectId::gen();
    Timestamp ts_t1{1, 1};
    Timestamp ts_before_now{generated_at_now.get_timestamp().get_time_point() - std::chrono::seconds(1)};
    Timestamp ts_after_now{generated_at_now.get_timestamp().get_time_point() + std::chrono::seconds(25)};
    Timestamp ts_00{0, 0};
    CHECK_GREATER(generated_at_now.get_timestamp().get_seconds(), 0);
    CHECK_GREATER_EQUAL(generated_at_now.get_timestamp().get_seconds() - ts_before_now.get_seconds(), 1);
    std::vector<Timestamp> times = {ts_t1, ts_before_now, ts_after_now, ts_00};
    int machine_id = 0;
    int process_id = 0;
    ObjectId oid_1{ts_t1, machine_id, process_id};
    ObjectId oid_before_now{ts_before_now, machine_id, process_id};
    ObjectId oid_after_now{ts_after_now, machine_id, process_id};
    ObjectId oid_0{ts_00, machine_id, process_id};
    std::vector<ObjectId> ids = {oid_1, oid_before_now, oid_after_now, oid_0};

    for (size_t i = 0; i < times.size(); ++i) {
        auto obj = table->create_object_with_primary_key({ids[i]});
        obj.set(nullable_oid_col_key, ids[i]);
    }
    auto obj_generated = table->create_object_with_primary_key(generated_at_now);
    auto generated_nullable = obj_generated.get<util::Optional<ObjectId>>(nullable_oid_col_key);
    CHECK(!generated_nullable);

    //  id  |  nid  |
    // --------------
    //  t1  |  t1   |
    //  tNow|  tNow |
    //  t25 |  t25  |
    //  t00 |  t00  |
    //  tNow|  null |

    // g.to_json(std::cout);
    verify_query(test_context, table, "id == oid(" + generated_at_now.to_string() + ")", 1);
    verify_query(test_context, table, "nid == NULL", 1);

    for (auto oid : ids) {
        verify_query(test_context, table, "id == oid(" + oid.to_string() + ")", 1);
        verify_query(test_context, table, "id != oid(" + oid.to_string() + ")", table->size() - 1);
        verify_query(test_context, table, "nid == oid(" + oid.to_string() + ")", 1);
        verify_query(test_context, table, "nid != oid(" + oid.to_string() + ")", table->size() - 1);
    }

    // everything should match >= 0, except for null
    verify_query(test_context, table, "id >= oid(000000000000000000000000)", table->size());
    verify_query(test_context, table, "nid >= oid(000000000000000000000000)", table->size() - 1);
    // everything should match <= max value, except for null
    verify_query(test_context, table, "id <= oid(ffffffffffffffffffffffff)", table->size());
    verify_query(test_context, table, "nid <= oid(ffffffffffffffffffffffff)", table->size() - 1);
    // a non nullable column should never contain null values
    verify_query(test_context, table, "id == NULL", 0);
    // a nullable column should find the null created by the default constructed row
    verify_query(test_context, table, "nid == NULL", 1);

    // argument substitution checks with an ObjectId
    util::Any args[] = {oid_1, oid_before_now, oid_after_now, oid_0, realm::null()};
    size_t num_args = 5;

    verify_query_sub(test_context, table, "id == $0", args, num_args, 1);
    verify_query_sub(test_context, table, "id == $1", args, num_args, 1);
    verify_query_sub(test_context, table, "id == $2", args, num_args, 1);
    verify_query_sub(test_context, table, "id == $3", args, num_args, 1);
    verify_query_sub(test_context, table, "id == $4", args, num_args, 0);
    verify_query_sub(test_context, table, "nid == $0", args, num_args, 1);
    verify_query_sub(test_context, table, "nid == $1", args, num_args, 1);
    verify_query_sub(test_context, table, "nid == $2", args, num_args, 1);
    verify_query_sub(test_context, table, "nid == $3", args, num_args, 1);
    verify_query_sub(test_context, table, "nid == $4", args, num_args, 1);

    // greater
    verify_query_sub(test_context, table, "id > $0", args, num_args, 3);
    verify_query_sub(test_context, table, "id > $1", args, num_args, 2);
    verify_query_sub(test_context, table, "id > $2", args, num_args, 0);
    verify_query_sub(test_context, table, "id > $3", args, num_args, 4);
    verify_query_sub(test_context, table, "nid > $0", args, num_args, 2);
    verify_query_sub(test_context, table, "nid > $1", args, num_args, 1);
    verify_query_sub(test_context, table, "nid > $2", args, num_args, 0);
    verify_query_sub(test_context, table, "nid > $3", args, num_args, 3);

    // greater equal
    verify_query_sub(test_context, table, "id >= $0", args, num_args, 4);
    verify_query_sub(test_context, table, "id >= $1", args, num_args, 3);
    verify_query_sub(test_context, table, "id >= $2", args, num_args, 1);
    verify_query_sub(test_context, table, "id >= $3", args, num_args, 5);
    verify_query_sub(test_context, table, "nid >= $0", args, num_args, 3);
    verify_query_sub(test_context, table, "nid >= $1", args, num_args, 2);
    verify_query_sub(test_context, table, "nid >= $2", args, num_args, 1);
    verify_query_sub(test_context, table, "nid >= $3", args, num_args, 4);

    // less
    verify_query_sub(test_context, table, "id < $0", args, num_args, 1);
    verify_query_sub(test_context, table, "id < $1", args, num_args, 2);
    verify_query_sub(test_context, table, "id < $2", args, num_args, 4);
    verify_query_sub(test_context, table, "id < $3", args, num_args, 0);
    verify_query_sub(test_context, table, "nid < $0", args, num_args, 1);
    verify_query_sub(test_context, table, "nid < $1", args, num_args, 2);
    verify_query_sub(test_context, table, "nid < $2", args, num_args, 3);
    verify_query_sub(test_context, table, "nid < $3", args, num_args, 0);

    // less equal
    verify_query_sub(test_context, table, "id <= $0", args, num_args, 2);
    verify_query_sub(test_context, table, "id <= $1", args, num_args, 3);
    verify_query_sub(test_context, table, "id <= $2", args, num_args, 5);
    verify_query_sub(test_context, table, "id <= $3", args, num_args, 1);
    verify_query_sub(test_context, table, "nid <= $0", args, num_args, 2);
    verify_query_sub(test_context, table, "nid <= $1", args, num_args, 3);
    verify_query_sub(test_context, table, "nid <= $2", args, num_args, 4);
    verify_query_sub(test_context, table, "nid <= $3", args, num_args, 1);
}


TEST(Parser_UUID)
{
    Group g;
    auto table = g.add_table_with_primary_key("table", type_UUID, "id");
    auto pk_col_key = table->get_primary_key_column();
    auto nullable_id_col_key = table->add_column(type_UUID, "nid", true);

    UUID u1("3b241101-e2bb-4255-8caf-4136c566a961");
    UUID u2("3b241101-e2bb-4255-8caf-294299afdce2");
    UUID u3("3b241101-e2bb-4255-8caf-000000000003");
    std::vector<UUID> ids = {u1, u2, u3};

    for (auto id : ids) {
        auto obj = table->create_object_with_primary_key({id});
        obj.set(nullable_id_col_key, id);
    }
    // add one object with default values, it should be null for the nullable column
    auto obj_generated = table->create_object_with_primary_key(UUID("3b241101-0000-0000-0000-4136c566a964"));
    UUID generated_pk = obj_generated.get<UUID>(pk_col_key);
    auto generated_nullable = obj_generated.get<util::Optional<UUID>>(nullable_id_col_key);
    CHECK_NOT(generated_nullable);
    size_t num_rows = table->size();
    verify_query(test_context, table, "id == uuid(" + generated_pk.to_string() + ")", 1);
    verify_query(test_context, table, "nid == uuid(" + generated_pk.to_string() + ")", 0);

    // checks for NULL
    verify_query(test_context, table, "id == NULL", 0);
    verify_query(test_context, table, "nid == NULL", 1);
    verify_query(test_context, table, "id != NULL", num_rows);
    verify_query(test_context, table, "nid != NULL", num_rows - 1);

    for (auto id : ids) {
        verify_query(test_context, table, "id == uuid(" + id.to_string() + ")", 1);
        verify_query(test_context, table, "nid == uuid(" + id.to_string() + ")", 1);
        verify_query(test_context, table, "id != uuid(" + id.to_string() + ")", num_rows - 1);
        verify_query(test_context, table, "nid != uuid(" + id.to_string() + ")", num_rows - 1);
        CHECK_THROW_ANY(verify_query(test_context, table, "nid BEGINSWITH uuid(" + id.to_string() + ")", 0));
        CHECK_THROW_ANY(verify_query(test_context, table, "nid ENDSWITH uuid(" + id.to_string() + ")", 0));
        CHECK_THROW_ANY(verify_query(test_context, table, "nid CONTAINS uuid(" + id.to_string() + ")", 0));
        CHECK_THROW_ANY(verify_query(test_context, table, "nid LIKE uuid(" + id.to_string() + ")", 0));
    }

    UUID min;
    UUID max("ffffffff-ffff-ffff-ffff-ffffffffffff");
    std::vector<std::string> props = {"id", "nid"};
    for (auto prop_name : props) {
        // a null value is neither greater nor less than any valid value
        size_t num_valid_values = (prop_name == "nid" ? num_rows - 1 : num_rows);
        verify_query(test_context, table, util::format("%1 > uuid(%2)", prop_name, min.to_string()),
                     num_valid_values);
        verify_query(test_context, table, util::format("%1 >= uuid(%2)", prop_name, min.to_string()),
                     num_valid_values);
        verify_query(test_context, table, util::format("%1 < uuid(%2)", prop_name, min.to_string()), 0);
        verify_query(test_context, table, util::format("%1 <= uuid(%2)", prop_name, min.to_string()), 0);
        verify_query(test_context, table, util::format("%1 > uuid(%2)", prop_name, max.to_string()), 0);
        verify_query(test_context, table, util::format("%1 >= uuid(%2)", prop_name, max.to_string()), 0);
        verify_query(test_context, table, util::format("%1 < uuid(%2)", prop_name, max.to_string()),
                     num_valid_values);
        verify_query(test_context, table, util::format("%1 <= uuid(%2)", prop_name, max.to_string()),
                     num_valid_values);
    }

    // argument substitution checks
    util::Any args[] = {u1, u2, u3, realm::null()};
    size_t num_args = 4;
    verify_query_sub(test_context, table, "id == $0", args, num_args, 1);
    verify_query_sub(test_context, table, "id == $1", args, num_args, 1);
    verify_query_sub(test_context, table, "id == $2", args, num_args, 1);
    verify_query_sub(test_context, table, "id == $3", args, num_args, 0);
    verify_query_sub(test_context, table, "id > $3", args, num_args, 0);
    verify_query_sub(test_context, table, "id < $3", args, num_args, 0);
    verify_query_sub(test_context, table, "id >= $3", args, num_args, 0);
    verify_query_sub(test_context, table, "id <= $3", args, num_args, 0);
    verify_query_sub(test_context, table, "nid == $0", args, num_args, 1);
    verify_query_sub(test_context, table, "nid == $1", args, num_args, 1);
    verify_query_sub(test_context, table, "nid == $2", args, num_args, 1);
    verify_query_sub(test_context, table, "nid == $3", args, num_args, 1);
    verify_query_sub(test_context, table, "nid > $3", args, num_args, 0);
    verify_query_sub(test_context, table, "nid < $3", args, num_args, 0);
    verify_query_sub(test_context, table, "nid >= $3", args, num_args, 1);
    verify_query_sub(test_context, table, "nid <= $3", args, num_args, 1);
}


TEST(Parser_Decimal128)
{
    Group g;
    auto table = g.add_table("table");
    auto col_key = table->add_column(type_Decimal, "dec");
    auto nullable_col_key = table->add_column(type_Decimal, "nullable_dec", true);

    // the test assumes that these are all unique
    std::vector<std::string> values = {
        "123",
        "0.1",
        "3.141592653589793238", // currently limited to 19 digits
        // sign variations
        "1E1",
        "+2E2",
        "+3E+3",
        "4E+4",
        "-5E5",
        "-6E-6",
        "7E-7",
        "+8E-8",
        "-9E+9",
        // decimal sign variations
        "1.1E1",
        "+2.1E2",
        "+3.1E+3",
        "4.1E+4",
        "-5.1E5",
        "-6.1E-6",
        "7.1E-7",
        "+8.1E-8",
        "-9.1E+9",
        // + and - infinity are treated differently
        "inf",
        "-infinity",
    };

    std::vector<std::string> invalids = {
        ".", "e10", "E-12", "infin", "-+2", "+-2", "2e+-12", "2e-+12", "2e1.3", "/2.0", "*2.0",
    };

    for (auto value : values) {
        auto obj = table->create_object();
        obj.set(col_key, Decimal128(value));
        obj.set(nullable_col_key, Decimal128(value));
    }
    // add one object with default values, 0 and null
    auto obj_generated = table->create_object();
    Decimal128 generated = obj_generated.get<Decimal128>(col_key);
    Decimal128 generated_nullable = obj_generated.get<Decimal128>(nullable_col_key);
    CHECK_EQUAL(generated, Decimal128(0));
    CHECK(generated_nullable.is_null());
    verify_query(test_context, table, "dec == " + generated.to_string(), 1);
    verify_query(test_context, table, "nullable_dec == " + generated_nullable.to_string(), 1);
    verify_query(test_context, table, "dec == 0.", 1);

    for (auto value : values) {
        verify_query(test_context, table, "dec == " + value, 1);
        verify_query(test_context, table, "nullable_dec == " + value, 1);
    }

    for (auto value : invalids) {
        CHECK_THROW_ANY(verify_query(test_context, table, "dec == " + value, 0));
        CHECK_THROW_ANY(verify_query(test_context, table, "nullable_dec == " + value, 0));
    }

    // none of the non-nullable values are null
    verify_query(test_context, table, "dec == NULL", 0);
    // the default generated nullable value is null
    verify_query(test_context, table, "nullable_dec == NULL", 1);
    constexpr size_t num_nans = 0;
    // everything should be less than positive infinity (except NaNs)
    verify_query(test_context, table, "dec <= infinity", table->size() - num_nans);
    // everything should be greater than or equal to negative infinity (except NaNs)
    verify_query(test_context, table, "dec >= -infinity", table->size() - num_nans);

    // argument substitution checks
    util::Any args[] = {Decimal128("0"), Decimal128("123"), realm::null{}};
    size_t num_args = 3;
    verify_query_sub(test_context, table, "dec == $0", args, num_args, 1);
    verify_query_sub(test_context, table, "dec == $1", args, num_args, 1);
    verify_query_sub(test_context, table, "dec == $2", args, num_args, 0);
    verify_query_sub(test_context, table, "nullable_dec == $2", args, num_args, 1);

    // column vs column
    constexpr size_t num_different_rows = 1; // default generated row is (0, null)
    verify_query(test_context, table, "dec == nullable_dec", table->size() - num_different_rows);
    verify_query(test_context, table, "dec >= nullable_dec", table->size() - num_different_rows);
    verify_query(test_context, table, "dec <= nullable_dec", table->size() - num_different_rows);
    verify_query(test_context, table, "dec > nullable_dec", 0);
    verify_query(test_context, table, "dec < nullable_dec", 0);
    verify_query(test_context, table, "dec != nullable_dec", num_different_rows);
}

TEST(Parser_Mixed)
{
    Group g;
    auto table = g.add_table("Foo");
    auto origin = g.add_table("Origin");
    auto col_any = table->add_column(type_Mixed, "mixed");
    auto col_int = table->add_column(type_Int, "int");
    auto col_link = origin->add_column(*table, "link");
    auto col_mixed = origin->add_column(type_Mixed, "mixed");
    auto col_links = origin->add_column_list(*table, "links");

    size_t int_over_50 = 0;
    size_t nb_strings = 0;
    for (int64_t i = 0; i < 100; i++) {
        if (i % 4) {
            if (i > 50)
                int_over_50++;
            table->create_object().set(col_any, Mixed(i)).set(col_int, i);
        }
        else {
            std::string str = "String" + util::to_string(i);
            table->create_object().set(col_any, Mixed(str)).set(col_int, i);
            nb_strings++;
        }
    }
    std::string bin_data("String2Binary");
    table->get_object(15).set(col_any, Mixed());
    table->get_object(75).set(col_any, Mixed(75.));
    table->get_object(28).set(col_any, Mixed(BinaryData(bin_data)));
    table->get_object(25).set(col_any, Mixed(3.));
    table->get_object(35).set(col_any, Mixed(Decimal128("3")));
    auto id = ObjectId::gen();
    table->get_object(37).set(col_any, Mixed(id));

    auto it = table->begin();
    for (int64_t i = 0; i < 10; i++) {
        auto obj = origin->create_object();
        auto ll = obj.get_linklist(col_links);
        obj.set(col_link, it->get_key());
        obj.set(col_mixed, Mixed(it->get_link()));
        for (int64_t j = 0; j < 10; j++) {
            ll.add(it->get_key());
            ++it;
        }
    }
    origin->create_object(); // one with null links

    verify_query(test_context, table, "mixed > 50", int_over_50);
    verify_query(test_context, table, "mixed >= 50", int_over_50 + 1);
    verify_query(test_context, table, "mixed <= 50", 100 - int_over_50 - nb_strings - 2);
    verify_query(test_context, table, "mixed < 50", 100 - int_over_50 - nb_strings - 3);
    verify_query(test_context, table, "mixed < 50 || mixed > 50", 100 - nb_strings - 3);
    verify_query(test_context, table, "mixed != 50", 99);
    verify_query(test_context, table, "mixed == null", 1);
    verify_query(test_context, table, "mixed != null", 99);
    verify_query(test_context, table, "mixed beginswith 'String2'", 3); // 20, 24, 28
    verify_query(test_context, table, "mixed beginswith B64\"U3RyaW5nMg==\"",
                 3); // 20, 24, 28, this string literal is base64 for 'String2'
    verify_query(test_context, table, "mixed contains \"trin\"", 25);
    verify_query(test_context, table, "mixed like \"Strin*\"", 25);
    verify_query(test_context, table, "mixed endswith \"4\"", 5); // 4, 24, 44, 64, 84
    verify_query(test_context, table, "mixed == oid(" + id.to_string() + ")", 1);

    char bin[1] = {0x34};
    util::Any args[] = {BinaryData(bin), ObjLink(table->get_key(), table->begin()->get_key()),
                        ObjLink(origin->get_key(), origin->begin()->get_key()),
                        ObjLink(TableKey(), ObjKey()), // null link
                        realm::null{}};
    size_t num_args = 5;
    verify_query_sub(test_context, table, "mixed endswith $0", args, num_args, 5); // 4, 24, 44, 64, 84
    verify_query_sub(test_context, origin, "link == $1", args, num_args, 1);
    verify_query_sub(test_context, origin, "link == $3", args, num_args, 1);
    verify_query_sub(test_context, origin, "link == $4", args, num_args, 1);
    verify_query_sub(test_context, origin, "link.@links.Origin.link == $2", args, num_args, 1); // poor man's SELF
    verify_query_sub(test_context, origin, "ANY links == $1", args, num_args, 1);
    verify_query_sub(test_context, origin, "ALL links == $1 && links.@size > 0", args, num_args, 0);
    verify_query_sub(test_context, origin, "NONE links == $1 && links.@size > 0", args, num_args, 9);
    verify_query_sub(test_context, origin, "mixed == $1", args, num_args, 1);

    verify_query(test_context, table, "mixed == \"String2Binary\"", 1);
    verify_query(test_context, table, "mixed ==[c] \"string2binary\"", 1);
    verify_query(test_context, table, "mixed !=[c] \"string2binary\"", 99);
    verify_query(test_context, table, "mixed == \"String48\"", 1);
    verify_query(test_context, table, "mixed == 3.0", 3);
    verify_query(test_context, table, "mixed == NULL", 1);
    verify_query(test_context, origin, "links.mixed > 50", 5);
    verify_query(test_context, origin, "links.mixed beginswith[c] \"string\"", 10);
    verify_query(test_context, origin, "link.mixed > 50", 2);
    verify_query(test_context, origin, "link.mixed beginswith[c] \"string\"", 5);
    verify_query(test_context, origin, "link == NULL", 1);
    verify_query(test_context, origin, "link.mixed == NULL", 1);
    verify_query(test_context, origin, "links.mixed == NULL", 1);

    // non-uniform type cross column comparisons
    verify_query(test_context, table, "mixed == int", 71);

    std::string message;
    CHECK_THROW_ANY_GET_MESSAGE(verify_query_sub(test_context, origin, "link == $2", args, num_args, 0), message);
    CHECK_EQUAL(message, "The relationship 'link' which links to type 'Foo' cannot be compared to an argument of "
                         "type 'Origin' with primary key 'O0'");
    CHECK_THROW_ANY_GET_MESSAGE(verify_query_sub(test_context, origin, "links == $2", args, num_args, 0), message);
    CHECK_EQUAL(message, "The relationship 'links' which links to type 'Foo' cannot be compared to an argument of "
                         "type 'Origin' with primary key 'O0'");
}

TEST(Parser_TypeOfValue)
{
    Group g;
    auto table = g.add_table("Foo");
    auto origin = g.add_table("Origin");
    auto col_any = table->add_column(type_Mixed, "mixed");
    auto col_int = table->add_column(type_Int, "int");
    auto col_primitive_list = table->add_column_list(type_Mixed, "list");
    auto col_link = origin->add_column(*table, "link");
    auto col_links = origin->add_column_list(*table, "links");
    size_t int_over_50 = 0;
    size_t nb_strings = 0;
    for (int64_t i = 0; i < 100; i++) {
        if (i % 4) {
            if (i > 50)
                int_over_50++;
            table->create_object().set(col_any, Mixed(i)).set(col_int, i);
        }
        else {
            std::string str = "String" + util::to_string(i);
            table->create_object().set(col_any, Mixed(str)).set(col_int, i);
            nb_strings++;
        }
    }
    std::string bin_data("String2Binary");
    table->get_object(15).set(col_any, Mixed());
    table->get_object(75).set(col_any, Mixed(75.));
    table->get_object(28).set(col_any, Mixed(BinaryData(bin_data)));
    nb_strings--;
    table->get_object(25).set(col_any, Mixed(3.));
    table->get_object(35).set(col_any, Mixed(Decimal128("3")));

    auto list_0 = table->get_object(0).get_list<Mixed>(col_primitive_list);
    list_0.add(Mixed{1});
    list_0.add(Mixed{Decimal128(10)});
    list_0.add(Mixed{Double{100}});
    auto list_1 = table->get_object(1).get_list<Mixed>(col_primitive_list);
    list_1.add(Mixed{std::string("hello")});
    list_1.add(Mixed{1000});

    auto it = table->begin();
    for (int64_t i = 0; i < 10; i++) {
        auto obj = origin->create_object();
        auto ll = obj.get_linklist(col_links);

        obj.set(col_link, it->get_key());
        for (int64_t j = 0; j < 10; j++) {
            ll.add(it->get_key());
            ++it;
        }
    }
    size_t nb_ints = 71;
    verify_query(test_context, table, "mixed.@type == 'string'", nb_strings);
    verify_query(test_context, table, "mixed.@type == 'double'", 2);
    verify_query(test_context, table, "mixed.@type == 'float'", 0);
    verify_query(test_context, table, "mixed.@type == 'Decimal'", 1);
    verify_query(test_context, table, "mixed.@type == 'decimal128'", 1);
    verify_query(test_context, table, "mixed.@type == 'binary'", 1);
    verify_query(test_context, table, "mixed.@type == 'bytearray'", 1);
    verify_query(test_context, table, "mixed.@type == 'byte[]'", 1);
    verify_query(test_context, table, "mixed.@type == 'uuid'", 0);
    verify_query(test_context, table, "mixed.@type == 'guid'", 0);
    verify_query(test_context, table, "mixed.@type == 'bool'", 0);
    verify_query(test_context, table, "mixed.@type == 'boolean'", 0);
    verify_query(test_context, table, "mixed.@type == 'int'", nb_ints);
    verify_query(test_context, table, "mixed.@type == 'integer'", nb_ints);
    verify_query(test_context, table, "mixed.@type == 'int16'", nb_ints);
    verify_query(test_context, table, "mixed.@type == 'int32'", nb_ints);
    verify_query(test_context, table, "mixed.@type == 'int64'", nb_ints);
    verify_query(test_context, table, "mixed.@type == 'short'", nb_ints);
    verify_query(test_context, table, "mixed.@type == 'long'", nb_ints);
    verify_query(test_context, table, "mixed.@type == 'byte'", nb_ints);
    verify_query(test_context, table, "mixed.@type == 'char'", nb_ints);
    verify_query(test_context, table, "mixed.@type == 'timestamp'", 0);
    verify_query(test_context, table, "mixed.@type == 'datetimeoffset'", 0);
    verify_query(test_context, table, "mixed.@type == 'object'", 0);

    verify_query(test_context, table,
                 "mixed.@type == 'binary' || mixed.@type == 'DECIMAL' || mixed.@type == 'Double'", 4);
    verify_query(test_context, table, "mixed.@type == 'null'", 1);
    verify_query(test_context, table, "mixed.@type == 'numeric'", table->size() - nb_strings - 2);
    verify_query(
        test_context, table,
        "mixed.@type == 'numeric' || mixed.@type == 'string' || mixed.@type == 'binary' || mixed.@type == 'null'",
        table->size());
    verify_query(test_context, table, "mixed.@type == mixed.@type", table->size());
    verify_query(test_context, origin, "link.mixed.@type == 'numeric' || link.mixed.@type == 'string'",
                 origin->size());
    verify_query(test_context, origin, "links.mixed.@type == 'numeric' || links.mixed.@type == 'string'",
                 origin->size());
    // TODO: enable this when IN is supported for list constants
    // verify_query(test_context, origin, "links.mixed.@type IN {'numeric', 'string'}", origin->size());

    verify_query(test_context, table, "mixed.@type == int.@type", table->size() - nb_strings - 5);
    verify_query(test_context, origin, "link.@type == link.mixed.@type", 0);
    verify_query(test_context, origin, "links.@type == links.mixed.@type", 0);

    verify_query(test_context, table, "mixed > 50", int_over_50);
    verify_query(test_context, table, "mixed > 50 && mixed.@type == 'double'", 1);
    verify_query(test_context, table, "mixed > 50 && mixed.@type != 'double'", int_over_50 - 1);
    verify_query(test_context, table, "mixed > 50 && mixed.@type == 'int'", int_over_50 - 1);

    verify_query(test_context, table, "list.@type == 'numeric'", 2);
    verify_query(test_context, table, "list.@type == 'numeric' AND list >= 10 ", 2);
    verify_query(test_context, table, "list.@type == mixed.@type", 1);
    verify_query(test_context, table, "NONE list.@type == mixed.@type && list.@size > 0", 1);
    verify_query(test_context, table, "ALL list.@type == mixed.@type && list.@size > 0", 0);
    verify_query(test_context, table, "ALL list.@type == 'numeric' && list.@size > 0", 1);
    verify_query(test_context, table, "NONE list.@type == 'binary' && list.@size > 0", 2);
    verify_query(test_context, table, "NONE list.@type == 'string' && list.@size > 0", 1);

    verify_query(test_context, origin, "links.mixed > 0", 10);
    verify_query(test_context, origin, "links.mixed.@type == 'double'", 2);
    verify_query(test_context, origin, "links.mixed > 0 && links.mixed.@type == 'double'", 2);
    verify_query(test_context, origin,
                 "SUBQUERY(links, $x, $x.mixed.@type == 'double' && $x.mixed == $x.int).@count > 0", 1);

    std::string message;
    CHECK_THROW_EX(
        verify_query(test_context, table, "mixed.@type == 'asdf'", 1), query_parser::InvalidQueryArgError,
        CHECK(std::string(e.what()).find("Unable to parse the type attribute string 'asdf'") != std::string::npos));
    CHECK_THROW_EX(
        verify_query(test_context, table, "mixed.@type == ''", 1), query_parser::InvalidQueryArgError,
        CHECK(std::string(e.what()).find("Unable to parse the type attribute string ''") != std::string::npos));
    CHECK_THROW_EX(
        verify_query(test_context, table, "mixed.@type == 'string|double|'", 1), query_parser::InvalidQueryArgError,
        CHECK(std::string(e.what()).find("Unable to parse the type attribute string") != std::string::npos));
    CHECK_THROW_EX(
        verify_query(test_context, table, "mixed.@type == '|'", 1), query_parser::InvalidQueryArgError,
        CHECK(std::string(e.what()).find("Unable to parse the type attribute string '") != std::string::npos));
    CHECK_THROW_EX(
        verify_query(test_context, table, "mixed.@type == 23", 1), query_parser::InvalidQueryArgError,
        CHECK(std::string(e.what()).find("Unsupported comparison between @type and raw value: '@type' and 'int'") !=
              std::string::npos));
    CHECK_THROW_EX(
        verify_query(test_context, table, "mixed.@type == 2.5", 1), query_parser::InvalidQueryArgError,
        CHECK(std::string(e.what()).find(
                  "Unsupported comparison between @type and raw value: '@type' and 'double'") != std::string::npos));
    CHECK_THROW_EX(
        verify_query(test_context, table, "mixed.@type == int", 1), query_parser::InvalidQueryArgError,
        CHECK(std::string(e.what()).find("Unsupported comparison between @type and raw value: '@type' and 'int'") !=
              std::string::npos));
    CHECK_THROW_EX(verify_query(test_context, table, "int.@type == 'int'", 1), query_parser::InvalidQueryError,
                   std::string(e.what()).find("Comparison between two constants is not supported") !=
                       std::string::npos);
    CHECK_THROW_EX(verify_query(test_context, origin, "link.@type == 'object'", 1), query_parser::InvalidQueryError,
                   CHECK(std::string(e.what()).find(
                             "Comparison between two constants is not supported ('\"object\"' and '\"object\"')") !=
                         std::string::npos));
    CHECK_THROW_EX(verify_query(test_context, table, "mixed.@type =[c] 'string'", 1), query_parser::InvalidQueryError,
                   CHECK_EQUAL(std::string(e.what()), "Unsupported comparison operator '=[c]' against type '@type', "
                                                      "right side must be a string or binary type"));
}

TEST(Parser_Dictionary)
{
    Group g;
    auto foo = g.add_table("foo");
    auto origin = g.add_table("origin");
    auto col_dict = foo->add_column_dictionary(type_Mixed, "dict");
    auto col_link = origin->add_column(*foo, "link");
    auto col_links = origin->add_column_list(*foo, "links");
    size_t expected = 0;
    size_t num_ints_for_value = 0;

    for (int64_t i = 0; i < 100; i++) {
        auto obj = foo->create_object();
        Dictionary dict = obj.get_dictionary(col_dict);
        bool incr = false;
        bool incr_num_ints = false;
        if ((i % 4) == 0) {
            dict.insert("Value", i);
            incr_num_ints = true;
            if (i > 50)
                incr = true;
        }
        else if ((i % 10) == 0) {
            dict.insert("Value", 100);
            incr = true;
            incr_num_ints = true;
        }
        if (i % 3) {
            dict.insert("Value", 3);
            incr = false;
            incr_num_ints = true;
        }
        if ((i % 5) == 0) {
            dict.insert("Foo", 5);
        }
        dict.insert("Bar", i);
        if (incr) {
            expected++;
        }
        if (incr_num_ints) {
            num_ints_for_value++;
        }
    }

    auto it = foo->begin();
    for (int64_t i = 0; i < 10; i++) {
        auto obj = origin->create_object();

        obj.set(col_link, it->get_key());

        auto ll = obj.get_linklist(col_links);
        for (int64_t j = 0; j < 10; j++) {
            ll.add(it->get_key());
            ++it;
        }
    }

    util::Any args[] = {String("Value")};
    size_t num_args = 1;

    verify_query(test_context, foo, "dict.@values > 50", 50);
    verify_query(test_context, foo, "dict['Value'] > 50", expected);
    verify_query_sub(test_context, foo, "dict[$0] > 50", args, num_args, expected);
    verify_query(test_context, foo, "dict['Value'] > 50", expected);
    verify_query(test_context, foo, "ANY dict.@keys == 'Foo'", 20);
    verify_query(test_context, foo, "NONE dict.@keys == 'Value'", 23);
    verify_query(test_context, foo, "dict['Value'].@type == 'int'", num_ints_for_value);
    verify_query(test_context, foo, "dict.@type == 'int'", 100);      // ANY is implied, all have int values
    verify_query(test_context, foo, "ALL dict.@type == 'int'", 100);  // all dictionaries have ints
    verify_query(test_context, foo, "NONE dict.@type == 'int'", 0);   // each object has Bar:i
    verify_query(test_context, foo, "ANY dict.@type == 'string'", 0); // no strings present

    verify_query(test_context, origin, "link.dict['Value'] > 50", 3);
    verify_query(test_context, origin, "links.dict['Value'] > 50", 5);
    verify_query(test_context, origin, "links.dict > 50", 6);
    verify_query(test_context, origin, "links.dict['Value'] == NULL", 10);

    verify_query(test_context, foo, "dict.@size == 3", 17);
    verify_query(test_context, foo, "dict.@max == 100", 2);
    verify_query(test_context, foo, "dict.@min < 2", 2);
    verify_query(test_context, foo, "dict.@sum >= 100", 9);
    verify_query(test_context, foo, "dict.@avg < 10", 16);

    verify_query(test_context, origin, "links.dict.@max == 100", 2);
    verify_query(test_context, origin, "link.dict.@max == 100", 2);

    auto dict = foo->begin()->get_dictionary(col_dict);

    dict.insert("some extra", 42);
    verify_query(test_context, foo, "dict['some extra'] == 42", 1);

    dict.insert("Value", 4.5);
    std::string message;

    CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, origin, "link.dict.Value > 50", 3), message);
    CHECK_EQUAL(message, "Property 'dict' in 'foo' is not an Object");

    // aggregates still work with mixed types
    verify_query(test_context, foo, "dict.@max == 100", 2);
    verify_query(test_context, foo, "dict.@min < 2", 2);
    verify_query(test_context, foo, "dict.@sum >= 100", 9);
    verify_query(test_context, foo, "dict.@avg < 10", 15);
    dict.insert("Bar", Timestamp(1234, 5678));
    verify_query(test_context, foo, "dict.@max == 100", 2);
    verify_query(test_context, foo, "dict.@min < 2", 1);
    verify_query(test_context, foo, "dict.@sum >= 100", 9);
    verify_query(test_context, foo, "dict.@avg < 10", 15);
}

TEST(Parser_DictionaryObjects)
{
    Group g;
    auto dogs = g.add_table_with_primary_key("dog", type_String, "name");
    auto col_age = dogs->add_column(type_Int, "age");
    auto persons = g.add_table_with_primary_key("person", type_String, "name");
    auto col_dict = persons->add_column_dictionary(*dogs, "pets");
    auto col_friend = persons->add_column(*persons, "friend");

    Obj adam = persons->create_object_with_primary_key("adam");
    Obj bernie = persons->create_object_with_primary_key("bernie");

    Obj astro = dogs->create_object_with_primary_key("astro", {{col_age, 4}});
    Obj pluto = dogs->create_object_with_primary_key("pluto", {{col_age, 5}});
    Obj lady = dogs->create_object_with_primary_key("lady", {{col_age, 5}});
    Obj snoopy = dogs->create_object_with_primary_key("snoopy", {{col_age, 3}});

    auto adam_pets = adam.get_dictionary(col_dict);
    adam_pets.insert("dog1", pluto);
    adam_pets.insert("dog2", lady);
    adam_pets.insert("none", ObjKey());

    auto bernie_pets = bernie.get_dictionary(col_dict);
    bernie_pets.insert("dog1", astro);
    bernie_pets.insert("dog2", snoopy);

    adam.set(col_friend, bernie.get_key());
    bernie.set(col_friend, adam.get_key());

    auto q = persons->link(col_dict).column<Int>(col_age) > 4;
    CHECK_EQUAL(q.count(), 1);
    q = persons->link(col_friend).link(col_dict).column<Int>(col_age) > 4;
    CHECK_EQUAL(q.count(), 1);

    verify_query(test_context, persons, "pets.@values.age > 4", 1);
}

TEST_TYPES(Parser_DictionaryAggregates, Prop<float>, Prop<double>, Prop<Decimal128>)
{
    using type = typename TEST_TYPE::type;

    std::array<type, 3> values = {type(5.55444333), type(6.55444333), type(7.55444333)};

    Group g;
    auto table = g.add_table("table");
    auto col = table->add_column_dictionary(TEST_TYPE::data_type, "dict");
    auto obj = table->create_object();
    Dictionary dict = obj.get_dictionary(col);
    dict.insert("1", values[0]);
    dict.insert("2", values[1]);
    dict.insert("3", values[2]);
    auto empty_obj = table->create_object();

    auto link_table = g.add_table("link");
    auto link_col = link_table->add_column(*table, "link");
    link_table->create_object().set(link_col, obj.get_key());
    link_table->create_object().set(link_col, empty_obj.get_key());
    link_table->create_object();

    Any arg = values[0];
    verify_query_sub(test_context, table, "dict.@min == $0", &arg, 1, 1);
    verify_query_sub(test_context, link_table, "link.dict.@min == $0", &arg, 1, 1);
    arg = values[2];
    verify_query_sub(test_context, table, "dict.@max == $0", &arg, 1, 1);
    verify_query_sub(test_context, link_table, "link.dict.@max == $0", &arg, 1, 1);
    arg = values[0] + values[1] + values[2];
    verify_query_sub(test_context, table, "dict.@sum == $0", &arg, 1, 1);
    verify_query_sub(test_context, link_table, "link.dict.@sum == $0", &arg, 1, 1);
    arg = (values[0] + values[1] + values[2]) / 3;
    verify_query_sub(test_context, table, "dict.@avg == $0", &arg, 1, 1);
    verify_query_sub(test_context, link_table, "link.dict.@avg == $0", &arg, 1, 1);

    arg = Any();
    verify_query_sub(test_context, table, "dict.@min == $0", &arg, 1, 1);
    verify_query_sub(test_context, link_table, "link.dict.@min == $0", &arg, 1, 2);
    verify_query_sub(test_context, table, "dict.@max == $0", &arg, 1, 1);
    verify_query_sub(test_context, link_table, "link.dict.@max == $0", &arg, 1, 2);
    verify_query_sub(test_context, table, "dict.@sum == $0", &arg, 1, 0);
    verify_query_sub(test_context, link_table, "link.dict.@sum == $0", &arg, 1, 0);
    verify_query_sub(test_context, table, "dict.@avg == $0", &arg, 1, 1);
    verify_query_sub(test_context, link_table, "link.dict.@avg == $0", &arg, 1, 2);

    arg = type(0);
    verify_query_sub(test_context, table, "dict.@sum == $0", &arg, 1, 1);
    verify_query_sub(test_context, link_table, "link.dict.@sum == $0", &arg, 1, 2);
}

TEST_TYPES(Parser_Set, Prop<int64_t>, Prop<float>, Prop<double>, Prop<Decimal128>, Prop<ObjectId>, Prop<Timestamp>,
           Prop<String>, Prop<BinaryData>, Prop<UUID>, Nullable<int64_t>, Nullable<float>, Nullable<double>,
           Nullable<Decimal128>, Nullable<ObjectId>, Nullable<Timestamp>, Nullable<String>, Nullable<BinaryData>,
           Nullable<UUID>)
{
    using type = typename TEST_TYPE::type;
    using underlying_type = typename TEST_TYPE::underlying_type;
    TestValueGenerator gen;
    Group g;
    auto table = g.add_table("foo");
    auto col_set = table->add_column_set(TEST_TYPE::data_type, "set", TEST_TYPE::is_nullable);
    auto col_prop = table->add_column(TEST_TYPE::data_type, "value", TEST_TYPE::is_nullable);
    std::vector<ObjKey> keys;

    table->create_objects(5, keys);

    auto set_values = [](Set<type> set, const std::vector<type>& value_list) {
        for (auto val : value_list)
            set.insert(val);
    };
    constexpr int64_t same_value = 3;
    auto item_3 = gen.convert_for_test<underlying_type>(same_value);
    for (size_t i = 0; i < table->size(); ++i) {
        table->get_object(keys[i]).set(col_prop, item_3);
    }

    set_values(table->get_object(keys[0]).get_set<type>(col_set), gen.values_from_int<type>({0, 1}));
    set_values(table->get_object(keys[1]).get_set<type>(col_set), gen.values_from_int<type>({2, same_value, 4, 5}));
    set_values(table->get_object(keys[2]).get_set<type>(col_set), gen.values_from_int<type>({6, 7, 100, 8, 9}));
    set_values(table->get_object(keys[3]).get_set<type>(col_set), gen.values_from_int<type>({same_value}));
    // the fifth set is empty

    verify_query(test_context, table, "set.@count == 0", 1);
    verify_query(test_context, table, "set.@size >= 1", 4);
    verify_query(test_context, table, "set.@size == 4", 1);

    util::Any args[] = {item_3};
    size_t num_args = 1;
    verify_query_sub(test_context, table, "set == $0", args, num_args, 2);      // 1, 3
    verify_query_sub(test_context, table, "$0 IN set", args, num_args, 2);      // 1, 3
    verify_query_sub(test_context, table, "ALL set == $0", args, num_args, 2);  // 3, 4
    verify_query_sub(test_context, table, "NONE set == $0", args, num_args, 3); // 0, 2, 4

    // single property vs set
    verify_query(test_context, table, "set == value", 2);      // 1, 3
    verify_query(test_context, table, "ANY set == value", 2);  // 1, 3
    verify_query(test_context, table, "ALL set == value", 2);  // 3, 4
    verify_query(test_context, table, "NONE set == value", 3); // 0, 2, 4

    if constexpr (realm::is_any_v<underlying_type, Int, Double, Float, Decimal128>) {
        verify_query(test_context, table, "set == 3", 2);          // 1, 3
        verify_query(test_context, table, "set.@max == 100", 1);   // 2
        verify_query(test_context, table, "set.@min == 0", 1);     // 0
        verify_query(test_context, table, "set.@avg == 3", 1);     // 3
        verify_query(test_context, table, "set.@avg >= 3", 3);     // 1, 2, 3
        verify_query(test_context, table, "set.@sum == 1", 1);     // 0
        verify_query(test_context, table, "set.@sum == 0", 1);     // 4
        verify_query(test_context, table, "set.@sum > 100", 1);    // 2
        verify_query(test_context, table, "set.@max == value", 1); // 3
        verify_query(test_context, table, "set.@min == value", 1); // 3
        verify_query(test_context, table, "set.@avg == value", 1); // 3
        verify_query(test_context, table, "set.@sum == value", 1); // 3
    }
    else {
        CHECK_THROW_ANY(verify_query(test_context, table, "set.@min > 100", 1));
        CHECK_THROW_ANY(verify_query(test_context, table, "set.@max > 100", 1));
        CHECK_THROW_ANY(verify_query(test_context, table, "set.@sum > 100", 1));
        CHECK_THROW_ANY(verify_query(test_context, table, "set.@avg > 100", 1));
    }
    if constexpr (realm::is_any_v<underlying_type, StringData, BinaryData>) {
        verify_query_sub(test_context, table, "set ==[c] $0", args, num_args, 2);           // 1, 3
        verify_query_sub(test_context, table, "set LIKE $0", args, num_args, 2);            // 1, 3
        verify_query_sub(test_context, table, "set BEGINSWITH $0", args, num_args, 2);      // 1, 3
        verify_query_sub(test_context, table, "set ENDSWITH $0", args, num_args, 2);        // 1, 3
        verify_query_sub(test_context, table, "set CONTAINS $0", args, num_args, 2);        // 1, 3
        verify_query_sub(test_context, table, "ALL set LIKE $0", args, num_args, 2);        // 3, 4
        verify_query_sub(test_context, table, "ALL set BEGINSWITH $0", args, num_args, 2);  // 3, 4
        verify_query_sub(test_context, table, "ALL set ENDSWITH $0", args, num_args, 2);    // 3, 4
        verify_query_sub(test_context, table, "ALL set CONTAINS $0", args, num_args, 2);    // 3, 4
        verify_query_sub(test_context, table, "NONE set LIKE $0", args, num_args, 3);       // 0, 2, 4
        verify_query_sub(test_context, table, "NONE set BEGINSWITH $0", args, num_args, 3); // 0, 2, 4
        verify_query_sub(test_context, table, "NONE set ENDSWITH $0", args, num_args, 3);   // 0, 2, 4
        verify_query_sub(test_context, table, "NONE set CONTAINS $0", args, num_args, 3);   // 0, 2, 4
        verify_query(test_context, table, "set.length == 10", 1);                           // 2 == "String 100"
        verify_query(test_context, table, "set.length == 0", 0);
        verify_query(test_context, table, "set.length > 0", 4); // 0, 1, 2, 3
    }
    else {
        CHECK_THROW_ANY(verify_query_sub(test_context, table, "set ==[c] $0", args, num_args, 0));
        CHECK_THROW_ANY(verify_query_sub(test_context, table, "set LIKE $0", args, num_args, 2));
        CHECK_THROW_ANY(verify_query_sub(test_context, table, "set BEGINSWITH $0", args, num_args, 2));
        CHECK_THROW_ANY(verify_query_sub(test_context, table, "set ENDSWITH $0", args, num_args, 2));
        CHECK_THROW_ANY(verify_query_sub(test_context, table, "set CONTAINS $0", args, num_args, 2));
    }
}

TEST(Parser_SetMixed)
{
    Group g;
    auto table = g.add_table("foo");
    bool is_nullable = true;
    auto col_set = table->add_column_set(type_Mixed, "set", is_nullable);
    auto col_prop = table->add_column(type_Mixed, "value", is_nullable);
    std::vector<ObjKey> keys;
    table->create_objects(5, keys);
    auto set_values = [](Set<Mixed> set, const std::vector<Mixed>& value_list) {
        for (auto val : value_list)
            set.insert(val);
    };
    const Mixed same_value(300);
    for (size_t i = 0; i < table->size(); ++i) {
        table->get_object(keys[i]).set(col_prop, same_value);
    }

    BinaryData data("foo", 3);
    set_values(table->get_object(keys[0]).get_set<Mixed>(col_set), {{3}, {"hello"}, same_value});
    set_values(table->get_object(keys[1]).get_set<Mixed>(col_set),
               {{3.5f}, {"world"}, {data}, {ObjectId::gen()}, {UUID()}, {}});
    set_values(table->get_object(keys[2]).get_set<Mixed>(col_set), {same_value});
    // the fourth set is empty
    set_values(table->get_object(keys[4]).get_set<Mixed>(col_set),
               {int64_t(-1), Decimal128(StringData(/*NaN*/)), 4.4f, 7.6, 0, realm::null()});
    auto list0 = table->get_object(keys[0]).get_set<Mixed>(col_set);
    CHECK_EQUAL(list0.min(), 3);
    CHECK_EQUAL(list0.max(), StringData("hello"));
    CHECK_EQUAL(list0.sum(), 303);
    CHECK_EQUAL(list0.avg(), 151.5);
    auto list1 = table->get_object(keys[1]).get_set<Mixed>(col_set);
    CHECK_EQUAL(list1.min(), 3.5);
    CHECK_EQUAL(list1.max(), UUID());
    CHECK_EQUAL(list1.sum(), 3.5);
    CHECK_EQUAL(list1.avg(), 3.5);
    auto list2 = table->get_object(keys[2]).get_set<Mixed>(col_set);
    CHECK_EQUAL(list2.min(), 300);
    CHECK_EQUAL(list2.max(), 300);
    CHECK_EQUAL(list2.sum(), 300);
    CHECK_EQUAL(list2.avg(), 300);
    auto list3 = table->get_object(keys[3]).get_set<Mixed>(col_set);
    CHECK_EQUAL(list3.min(), Mixed{});
    CHECK_EQUAL(list3.max(), Mixed{});
    CHECK_EQUAL(list3.sum(), 0);
    CHECK_EQUAL(list3.avg(), Mixed{});
    auto list4 = table->get_object(keys[4]).get_set<Mixed>(col_set);
    CHECK_EQUAL(list4.min(), -1);
    CHECK_EQUAL(list4.max(), 7.6);
    CHECK_EQUAL(list4.sum(), 11);
    CHECK_EQUAL(list4.avg(), 2.75);

    verify_query(test_context, table, "set.@min == 3", 1);
    verify_query(test_context, table, "set.@min == 3.5", 1);
    verify_query(test_context, table, "set.@min == 300", 1);
    verify_query(test_context, table, "set.@min == NULL", 1);
    verify_query(test_context, table, "set.@min == -1", 1);
    verify_query(test_context, table, "set.@max == 'hello'", 1);
    verify_query(test_context, table, "set.@max == uuid(00000000-0000-0000-0000-000000000000)", 1);
    verify_query(test_context, table, "set.@max == 7.6", 1);
    verify_query(test_context, table, "set.@max == 300", 1);
    verify_query(test_context, table, "set.@max == NULL", 1);
    verify_query(test_context, table, "set.@max == 7.6", 1);
    verify_query(test_context, table, "set.@sum == 303", 1);
    verify_query(test_context, table, "set.@sum == 3.5", 1);
    verify_query(test_context, table, "set.@sum == 300", 1);
    verify_query(test_context, table, "set.@sum == 0", 1);
    verify_query(test_context, table, "set.@sum == 11", 1);
    verify_query(test_context, table, "set.@avg == 151.5", 1);
    verify_query(test_context, table, "set.@avg == 3.5", 1);
    verify_query(test_context, table, "set.@avg == 300", 1);
    verify_query(test_context, table, "set.@avg == NULL", 1);
    verify_query(test_context, table, "set.@avg == 2.75", 1);

    verify_query(test_context, table, "set.@count == 0", 1);
    verify_query(test_context, table, "set.@size >= 1", 4);
    verify_query(test_context, table, "set.@size == 6", 2);
    verify_query(test_context, table, "3.5 IN set", 1);
    verify_query(test_context, table, "'WorLD' IN[c] set", 1);
    verify_query(test_context, table, "set == value", 2);
    verify_query(test_context, table, "set < value", 3);
    verify_query(test_context, table, "ALL set < value", 1); // 3
    verify_query(test_context, table, "ALL set < value && set.@size > 0", 0);
    verify_query(test_context, table, "ALL set == value", 2);  // 2, 3
    verify_query(test_context, table, "NONE set == value", 3); // 1, 3, 5
    verify_query(test_context, table, "set == NULL", 2);
    verify_query(test_context, table, "set beginswith[c] 'HE'", 1);
    verify_query(test_context, table, "set endswith[c] 'D'", 1);
    verify_query(test_context, table, "set LIKE[c] '*O*'", 2);
    verify_query(test_context, table, "set CONTAINS 'r'", 1);
    verify_query(test_context, table, "set.length == 5", 2);
    verify_query(test_context, table, "set.length == 3", 1);
}

TEST(Parser_CollectionsConsistency)
{
    Group g;
    auto table = g.add_table("foo");
    bool is_nullable = true;
    auto col_set = table->add_column_set(type_Mixed, "set", is_nullable);
    auto col_list = table->add_column_list(type_Mixed, "list", is_nullable);
    auto col_dict = table->add_column_dictionary(type_Mixed, "dict");
    std::vector<ObjKey> keys;
    table->create_objects(5, keys);
    size_t key_ndx = 0;
    auto set_values = [&](ObjKey key, const std::vector<Mixed>& value_list) {
        auto obj = table->get_object(key);
        auto set = obj.get_set<Mixed>(col_set);
        auto list = obj.get_list<Mixed>(col_list);
        auto dict = obj.get_dictionary(col_dict);
        for (auto val : value_list) {
            set.insert(val);
            list.add(val);
            dict.insert(util::format("key_%1", key_ndx++), val);
        }
    };
    auto check_agg = [&](ObjKey key, Mixed min, Mixed max, Mixed sum, Mixed avg) {
        auto obj = table->get_object(key);
        auto set = obj.get_set<Mixed>(col_set);
        auto list = obj.get_list<Mixed>(col_list);
        auto dict = obj.get_dictionary(col_dict);
        CHECK_EQUAL(set.min(), min);
        CHECK_EQUAL(list.min(), min);
        CHECK_EQUAL(dict.min(), min);
        CHECK_EQUAL(set.max(), max);
        CHECK_EQUAL(list.max(), max);
        CHECK_EQUAL(dict.max(), max);
        CHECK_EQUAL(set.sum(), sum);
        CHECK_EQUAL(list.sum(), sum);
        CHECK_EQUAL(dict.sum(), sum);
        CHECK_EQUAL(set.avg(), avg);
        CHECK_EQUAL(list.avg(), avg);
        CHECK_EQUAL(dict.avg(), avg);

        std::vector<Mixed> args = {min, max, sum, avg};
        verify_query_sub(test_context, table, "set.@min == $0", args, 1);
        verify_query_sub(test_context, table, "list.@min == $0", args, 1);
        verify_query_sub(test_context, table, "dict.@min == $0", args, 1);
        verify_query_sub(test_context, table, "set.@max == $1", args, 1);
        verify_query_sub(test_context, table, "list.@max == $1", args, 1);
        verify_query_sub(test_context, table, "dict.@max == $1", args, 1);
        verify_query_sub(test_context, table, "set.@sum == $2", args, 1);
        verify_query_sub(test_context, table, "list.@sum == $2", args, 1);
        verify_query_sub(test_context, table, "dict.@sum == $2", args, 1);
        verify_query_sub(test_context, table, "set.@avg == $3", args, 1);
        verify_query_sub(test_context, table, "list.@avg == $3", args, 1);
        verify_query_sub(test_context, table, "dict.@avg == $3", args, 1);
    };
    const Mixed same_value(300);

    BinaryData data("foo", 3);
    set_values(keys[0], {{3}, {"hello"}, same_value});
    set_values(keys[1], {{3.5f}, {"world"}, {data}, {ObjectId::gen()}, {UUID()}, {}});
    set_values(keys[2], {same_value});
    // the collections at keys[3] are empty
    set_values(keys[4], {int64_t(-1), Decimal128(StringData(/*NaN*/)), 4.4f, 7.6, 0, realm::null()});

    check_agg(keys[0], 3, StringData("hello"), 303, 151.5);
    check_agg(keys[1], 3.5, UUID(), 3.5, 3.5);
    check_agg(keys[2], same_value, same_value, same_value, same_value);
    check_agg(keys[3], Mixed{}, Mixed{}, 0, Mixed{});
    check_agg(keys[4], -1, 7.6, 11, 2.75);
}

TEST(Parser_SetLinks)
{
    Group g;
    auto origin = g.add_table("origin");
    auto table = g.add_table("foo");
    auto target = g.add_table("bar");
    auto col_link = origin->add_column(*table, "link");
    auto col_set = table->add_column_set(*target, "set");
    auto col_int = target->add_column(type_Int, "val");

    ObjKeys target_keys;
    for (int64_t i = 0; i < 10; i++) {
        target_keys.push_back(target->create_object().set(col_int, i).get_key());
    }
    auto set = table->create_object().get_linkset(col_set);
    for (size_t i = 0; i < 6; i++) {
        set.insert(target_keys[i]);
    }
    origin->create_object().set(col_link, set.get_obj().get_key());
    set = table->create_object().get_linkset(col_set);
    for (size_t i = 4; i < 10; i++) {
        set.insert(target_keys[i]);
    }
    origin->create_object().set(col_link, set.get_obj().get_key());

    // g.to_json(std::cout);

    verify_query(test_context, table, "set.@count == 6", 2);

    verify_query(test_context, origin, "link.set.val == 3", 1);
    verify_query(test_context, origin, "link.set.val == 5", 2);
}

namespace {

void worker(test_util::unit_test::TestContext& test_context, TransactionRef frozen)
{
    auto table = frozen->get_table("Foo");
    for (auto obj : *table) {
        auto val = obj.get_key().value;
        std::string query_str = "value == " + util::to_string(val);
        auto cnt = table->query(query_str).count();
        CHECK_EQUAL(cnt, 1);
    }
}

} // namespace

TEST(Parser_Threads)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history());
    DBRef db = DB::create(*hist, path);
    TransactionRef frozen;

    {
        auto wt = db->start_write();
        auto table = wt->add_table("Foo");
        auto col_int = table->add_column(type_Int, "value");

        for (int i = 0; i < 1000; i++) {
            auto obj = table->create_object();
            obj.set(col_int, obj.get_key().value);
        }
        wt->commit_and_continue_as_read();
        frozen = wt->freeze();
    }
    const int num_threads = 2;
    std::vector<std::thread> workers;
    for (int j = 0; j < num_threads; ++j)
        workers.emplace_back([&] {
            worker(test_context, frozen);
        });
    for (auto& w : workers)
        w.join();
}

TEST(Parser_ClassPrefix)
{
    for (const char* prefix : {"class_", "cl#"}) {
        Group g;
        std::string table_name = std::string(prefix) + "foo";
        auto table = g.add_table(table_name);
        auto col = table->add_column(type_Int, "val");
        auto col_link = table->add_column(*table, "parent");
        auto top = table->create_object();
        for (int64_t i : {1, 2, 3, 4, 5}) {
            table->create_object().set(col, i).set(col_link, top.get_key());
        }
        query_parser::KeyPathMapping mapping_with_prefix;
        mapping_with_prefix.set_backlink_class_prefix(prefix);

        verify_query(test_context, table, "val > 3", 2, mapping_with_prefix);
        verify_query(test_context, table, "@links.foo.parent.val > 0", 1, mapping_with_prefix);
        std::string message;
        CHECK_THROW_ANY_GET_MESSAGE(verify_query(test_context, table, "id > 5", 0, mapping_with_prefix), message);
        CHECK_EQUAL(message, "'foo' has no property: 'id'");
    }
}

TEST(Parser_UTF8)
{
    Group g;
    TableRef t = g.add_table("person");
    ColKey col_dk = t->add_column(type_Int, "løbenummer");
    ColKey col_ch = t->add_column(type_String, "姓名");

    std::vector<std::string> names = {"Billy", "Bob", "Joe", "Jake", "Joel"};
    for (size_t i = 0; i < names.size(); ++i) {
        Obj obj = t->create_object();
        obj.set(col_dk, int64_t(i));
        obj.set(col_ch, StringData(names[i]));
    }

    verify_query(test_context, t, "løbenummer > 2", 2);
    verify_query(test_context, t, "姓名 == 'Bob'", 1);
}

TEST(Parser_Logical)
{
    Group g;
    TableRef t = g.add_table("table");
    ColKey col1 = t->add_column(type_Int, "id1");
    ColKey col2 = t->add_column(type_Int, "id2");
    ColKey col3 = t->add_column(type_Int, "id3");

    for (int64_t i = 0; i < 10; i++) {
        t->create_object().set(col1, i).set(col2, 2 * i).set(col3, 3 * i);
    }

    verify_query(test_context, t, "id1 == 5 || id1 == 9 || id2 == 10 || id2 == 16", 3);
    verify_query(test_context, t, "id1 == 5 && id2 == 10 || id1 == 7 && id2 == 14", 2);
    verify_query(test_context, t, "id1 == 5 && id2 == 10 && id3 == 15", 1);
    verify_query(test_context, t, "id1 == 5 && (id2 == 10 || id1 == 7) && id3 == 15", 1);
    verify_query(test_context, t, "!id1 == 5 && !(id2 == 12) && !id3 == 12", 7);
}

TEST_TYPES(Parser_Arithmetic, Prop<int64_t>, Prop<float>, Prop<double>, Prop<Decimal128>)
{
    using type = typename TEST_TYPE::type;
    Group g;
    TableRef person = g.add_table_with_primary_key("person", type_String, "name");
    ColKey col_age = person->add_column(type_Int, "age");
    ColKey col_tag = person->add_column(type_Mixed, "tag");
    ColKey col_number = person->add_column(TEST_TYPE::data_type, "number");
    ColKey col_spouse = person->add_column(*person, "spouse");

    auto per = person->create_object_with_primary_key("Per").set(col_age, 42).set(col_number, type(1));
    auto poul = person->create_object_with_primary_key("Poul").set(col_age, 25).set(col_tag, Mixed(2));
    auto anne = person->create_object_with_primary_key("Anne")
                    .set(col_age, 40)
                    .set(col_number, type(2))
                    .set(col_tag, Mixed("Cool"));
    auto andrea = person->create_object_with_primary_key("Andrea").set(col_age, 27).set(col_tag, Mixed(2));
    per.set(col_spouse, anne.get_key());
    poul.set(col_spouse, andrea.get_key());
    anne.set(col_spouse, per.get_key());
    andrea.set(col_spouse, poul.get_key());

    verify_query(test_context, person, "2 * age > 60", 2);
    verify_query(test_context, person, "2 * age + 5 == 55", 1);
    verify_query(test_context, person, "2 * (age - 5) == 70", 1);
    verify_query(test_context, person, "age / 3 == 14", 1);
    verify_query(test_context, person, "age / 0 == 14", 0);
    verify_query(test_context, person, "age / number == 20", 1);
    verify_query(test_context, person, "age / number > 20", 3);
    verify_query(test_context, person, "age == (10 + 11)*2", 1);
    verify_query(test_context, person, "age + tag > 28", 1);
    CHECK_THROW_ANY(verify_query(test_context, person, "age + spouse.name == 50", 2));

    std::vector<Mixed> args = {2, 50};
    verify_query_sub(test_context, person, "age * $0 == $1", args, 1);
}

#endif // TEST_PARSER
