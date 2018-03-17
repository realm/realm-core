/*************************************************************************
 *
 * Copyright 2018 Realm Inc.
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

#include <realm/array_dictionary.hpp>

#include "test.hpp"

using namespace realm;
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

TEST(Dictionary_Basics)
{
    Dictionary dict;
    dict.create();
    CHECK(dict.insert("Hello", 9));
    CHECK_EQUAL(dict.get("Hello").get_int(), 9);
    CHECK_NOT(dict.insert("Hello", 10));
    CHECK_EQUAL(dict.get("Hello").get_int(), 9);
    CHECK(dict.insert("Goodbye", "cruel world"));
    CHECK_EQUAL(dict.get("Goodbye").get_string(), "cruel world");
    CHECK_THROW_ANY(dict.get("Baa").get_string()); // Within range
    CHECK_THROW_ANY(dict.get("Foo").get_string()); // Outside range

    auto it = dict.begin();
    auto end = dict.end();
    CHECK(it != end);
    CHECK_EQUAL(it->first, Mixed("Hello"));
    ++it;
    CHECK(it != end);
    CHECK_EQUAL(it->first, Mixed("Goodbye"));
    ++it;
    CHECK(it == end);

    Dictionary other(dict);
    CHECK(other == dict);

    dict.clear();
    CHECK_EQUAL(dict.size(), 0);
    CHECK_THROW_ANY(dict.get("Goodbye").get_string());

    CHECK_EQUAL(other.size(), 2);
    CHECK_EQUAL(other.get("Goodbye").get_string(), "cruel world");
    other.update("Goodbye", 100.0);
    CHECK_EQUAL(other.get("Goodbye").get_double(), 100.0);
}

TEST(ArrayDictionary_Basics)
{
    ArrayDictionary arr(Allocator::get_default());
    arr.create();
    arr.add(ArrayDictionary::default_value(true));

    {
        Dictionary dict;
        dict.create();
        dict.insert("Hello", 9);
        dict.insert("Goodbye", "cruel world");

        arr.add(std::move(dict));
    }

    CHECK_EQUAL(arr.size(), 2);
    CHECK(arr.is_null(0));
    CHECK_NOT(arr.is_null(1));
    auto dict = arr.get(1);
    CHECK_EQUAL(dict.get("Hello").get_int(), 9);
    CHECK_EQUAL(dict.get("Goodbye").get_string(), "cruel world");
    arr.update(1, "Hello", Int(11));
    CHECK_EQUAL(arr.get(1, "Hello").get_int(), 11);

    arr.destroy();
}
