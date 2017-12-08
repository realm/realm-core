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

void verify_query(test_util::unit_test::TestContext& test_context, TableRef t, std::string query_string, size_t num_results) {
    Query q = t->where();

    realm::parser::Predicate p = realm::parser::parse(query_string);
    realm::query_builder::apply_predicate(q, p);

    CHECK_EQUAL(q.count(), num_results);
    std::string description = q.get_description();
    //std::cerr << "original: " << query_string << "\tdescribed: " << description << "\n";
    Query q2 = t->where();

    realm::parser::Predicate p2 = realm::parser::parse(description);
    realm::query_builder::apply_predicate(q2, p2);

    CHECK_EQUAL(q2.count(), num_results);
}


TEST(Parser_basic_serialisation)
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
    verify_query(test_context, t, "time > T2017-12-1@12:07:53", 1);
    verify_query(test_context, t, "time == T2017-12-01@12:07:53:505", 1);
    verify_query(test_context, t, "buddy == NULL", 4);
    verify_query(test_context, t, "buddy != NULL", 1);
    verify_query(test_context, t, "age > 2", 2);
    verify_query(test_context, t, "!(age >= 2)", 2);
    verify_query(test_context, t, "3 <= age", 2);
    verify_query(test_context, t, "age > 2 and age < 4", 1);
    verify_query(test_context, t, "age = 1 || age == 3", 2);
    verify_query(test_context, t, "fees != 2.22 && fees > 2.2", 3);
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
    verify_query(test_context, t, "T399 == T2017-12-04@0:0:0", 0);

    verify_query(test_context, t, "T2017-12-04 == NULL", 3);
    verify_query(test_context, t, "T2017-12-04 != NULL", 2);
    verify_query(test_context, t, "linked.T2017-12-04 == NULL", 3); // null links count as a match for null here
    verify_query(test_context, t, "linked != NULL && linked.T2017-12-04 == NULL", 0);
    verify_query(test_context, t, "linked.T2017-12-04 != NULL", 2);
    verify_query(test_context, t, "linked != NULL && linked.T2017-12-04 != NULL", 2);
    verify_query(test_context, t, "T2017-12-04 == T399:0", 0);
    verify_query(test_context, t, "linked.T2017-12-04 == T399:0", 0);
    verify_query(test_context, t, "T2017-12-04 == T2017-12-04@0:0:0", 0);

    verify_query(test_context, t, "birthday == NULL", 0);
    verify_query(test_context, t, "birthday != NULL", 5);
    verify_query(test_context, t, "birthday == T0:0", 3);
    verify_query(test_context, t, "birthday == T1970-1-1@0:0:0:0", 3); // epoch is default non-null Timestamp
    verify_query(test_context, t, "birthday == T1969-12-31@23:59:59:-1", 1);
    verify_query(test_context, t, "birthday == T1970-1-1@0:0:0:-1", 1);

    // invalid timestamps, if these were constructed core would assert
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T-1:1", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1:-1", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1969-12-31@23:59:59:1", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1970-1-1@0:0:1:-1", 0));

    // Invalid predicate
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1:", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T:1", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T399", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1970-1-1", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1970-1-1@", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1970-1-1@0", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1970-1-1@0:", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1970-1-1@0:0", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1970-1-1@0:0:", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1970-1-1@0:0:0:", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1970-1-1@0:0:0:0:", 0));
    CHECK_THROW_ANY(verify_query(test_context, t, "birthday == T1970-1-1@0:0:0:0:0", 0));
}

TEST(Parser_collection_aggregates)
{
    Group g;
    TableRef people = g.add_table("person");
    TableRef courses = g.add_table("courses");
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

    // size and count do not allow paths on the destination object
    CHECK_THROW_ANY(verify_query(test_context, people, "name.@count.hours_required <= 2", 0));
    CHECK_THROW_ANY(verify_query(test_context, people, "name.@size.hours_required <= 2", 0));

    // size is only allowed on certain types
    CHECK_THROW_ANY(verify_query(test_context, people, "age.@size <= 2", 0));
    CHECK_THROW_ANY(verify_query(test_context, courses, "credits.@size == 2", 0));
    CHECK_THROW_ANY(verify_query(test_context, courses, "failure_percentage.@size <= 2", 0));
}

#endif // TEST_PARSER
