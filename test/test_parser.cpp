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
        std::cout << "query: " << query;
        realm::parser::parse(query);
    }
}

TEST(Parser_invalid_queries) {
    for (auto& query : invalid_queries) {
        std::cout << "query: " << query;
        CHECK_THROW_ANY(realm::parser::parse(query));
    }
}

#if REALM_METRICS


#endif // REALM_METRICS

#endif // TEST_PARSER
