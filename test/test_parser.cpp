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
#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/to_string.hpp>

#include <chrono>
#include <string>
#include <thread>
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

    // operators
    "0=0",
    "0 = 0",
    "0 =[c] 0",
    "0!=0",
    "0 != 0",
    "0==0",
    "0 == 0",
    "0==[c]0",
    "0 == [c] 0",
    "0>0",
    "0 > 0",
    "0>=0",
    "0 >= 0",
    "0<0",
    "0 < 0",
    "0<=0",
    "0 <= 0",
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
    "a$a = a",
    "{} = $0",
    "$-1 = $0",
    "$a = $0",
    "$ = $",

    // operators
    "0===>0",
    "0 <> 0",
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
};

TEST(Parser_valid_queries) {
    for (auto& query : valid_queries) {
        std::cout << "query: " << query << std::endl;
        realm::parser::parse(query);
    }
}

TEST(Parser_invalid_queries) {
    for (auto& query : invalid_queries) {
        std::cout << "query: " << query << std::endl;
        CHECK_THROW_ANY(realm::parser::parse(query));
    }
}

#if REALM_METRICS

struct temp_hax
{
    template<typename T>
    T unbox(std::string) {
        return T{}; //dummy
    }
    bool is_null(std::string) {
        return false;
    }
};

ONLY(Parser_basic_serialisation)
{
    Group g;
    TableRef t = g.add_table("person");
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
    t->set_timestamp(time_col_ndx, 1, Timestamp(1512130073, 0)); // 12/02/2017 @ 12:47am (UTC)
    t->set_timestamp(time_col_ndx, 2, Timestamp(1512130073, 505)); // with nanoseconds
    t->set_timestamp(time_col_ndx, 3, Timestamp(1, 2));
    t->set_timestamp(time_col_ndx, 4, Timestamp(0, 0));
    t->set_link(link_col_ndx, 0, 1);

    auto verify_query = [&](TableRef t, std::string query, size_t num_results) {
        Query q = t->where();

        realm::parser::Predicate p = realm::parser::parse(query);

        temp_hax h;
        std::string s;
        realm::query_builder::ArgumentConverter<std::string, temp_hax> args(h, &s, 0);
        realm::query_builder::apply_predicate(q, p, args, "");

        CHECK_EQUAL(q.count(), num_results);
        std::string description = q.get_description();
        std::cerr << "original: " << query << "\tdescribed: " << description << "\n";
        Query q2 = t->where();

        realm::parser::Predicate p2 = realm::parser::parse(description);
        realm::query_builder::apply_predicate(q2, p2, args, "");

        CHECK_EQUAL(q2.count(), num_results);
    };

    Query q = t->where();
    //verify_query(t, "buddy.age > $0", 0);
    verify_query(t, "time == NULL", 1);
    verify_query(t, "time != NULL", 4);
    verify_query(t, "time > T0:0", 3);
    verify_query(t, "time == T1:2", 1);
    verify_query(t, "time > T2017-12-1@12:07:53", 1);
    verify_query(t, "time == T2017-12-01@12:07:53:505", 1);

    verify_query(t, "buddy == NULL", 4);
    verify_query(t, "buddy != NULL", 1);
    verify_query(t, "age > 2", 2);
    verify_query(t, "!(age >= 2)", 2);
    verify_query(t, "3 <= age", 2);
    verify_query(t, "age > 2 and age < 4", 1);
    verify_query(t, "age = 1 || age == 3", 2);
    verify_query(t, "fees != 2.22 && fees > 2.2", 3);
    verify_query(t, "name = \"Joe\"", 1);
    verify_query(t, "buddy.age > 0", 1);
    verify_query(t, "name BEGINSWITH \"J\"", 3);
    verify_query(t, "name ENDSWITH \"E\"", 0);
    verify_query(t, "name ENDSWITH[c] \"E\"", 2);
    verify_query(t, "name CONTAINS \"OE\"", 0);
    verify_query(t, "name CONTAINS[c] \"OE\"", 2);
    verify_query(t, "name LIKE \"b*\"", 0);
    verify_query(t, "name LIKE[c] \"b*\"", 2);

}

#endif // REALM_METRICS

#endif // TEST_PARSER
