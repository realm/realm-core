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
#ifdef TEST_UTIL_TO_STRING

#include <realm/util/to_string.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;

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

TEST(ToString_Basic)
{
    CHECK_EQUAL(to_string(false), "false");
    CHECK_EQUAL(to_string(true), "true");
    CHECK_EQUAL(to_string(static_cast<signed char>(-1)), "-1");
    CHECK_EQUAL(to_string(static_cast<unsigned char>(255)), "255");
    CHECK_EQUAL(to_string(-1), "-1");
    CHECK_EQUAL(to_string(0xFFFF0000U), "4294901760");
    CHECK_EQUAL(to_string(-1L), "-1");
    CHECK_EQUAL(to_string(0xFFFF0000UL), "4294901760");
    CHECK_EQUAL(to_string(-1LL), "-1");
    CHECK_EQUAL(to_string(0xFFFF0000ULL), "4294901760");
    CHECK_EQUAL(to_string("Foo"), "\"Foo\"");

    {
        std::ostringstream ostr;
        Printable::print_all(ostr, {0, true, "Hello"}, false);
        std::string s = ostr.str();
        CHECK_EQUAL(s, " [0, true, Hello]");
    }

    {
        std::ostringstream ostr;
        Printable::print_all(ostr, {0, true, "Hello"}, true);
        std::string s = ostr.str();
        CHECK_EQUAL(s, " [0, true, \"Hello\"]");
    }

    {
        std::ostringstream ostr;
        Printable::print_all(ostr, {}, false);
        std::string s = ostr.str();
        CHECK_EQUAL(s, "");
    }
}

#endif
