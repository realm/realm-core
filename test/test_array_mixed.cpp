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

#include <realm/array_mixed.hpp>

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

#ifdef TEST_ARRAY_MIXED

TEST(ArrayMixed_Basics)
{
    ArrayMixed arr(Allocator::get_default());
    arr.create();
    arr.add(int64_t(5));
    arr.add(true);
    arr.add(3.5f);
    arr.add(17.87);
    arr.add("Goodbye cruel world");
    arr.add("Goodbye yellow brick road");
    std::string bin(42, 'x');
    arr.add(BinaryData(bin.data(), bin.size()));
    arr.add(Timestamp(1234, 5678));
    arr.add({});
    arr.add(Timestamp(2345, 6789));

    CHECK_EQUAL(arr.size(), 10);
    CHECK_EQUAL(arr.get(0).get_int(), 5);
    CHECK_EQUAL(arr.get(1).get_bool(), true);
    CHECK_EQUAL(arr.get(2).get_float(), 3.5f);
    CHECK_EQUAL(arr.get(3).get_double(), 17.87);
    CHECK_EQUAL(arr.get(4).get_string(), "Goodbye cruel world");
    CHECK_EQUAL(arr.get(5).get_string(), "Goodbye yellow brick road");
    CHECK_EQUAL(arr.get(6).get_binary(), BinaryData(bin.data(), bin.size()));
    CHECK_EQUAL(arr.get(7).get_timestamp(), Timestamp(1234, 5678));
    CHECK(arr.is_null(8));
    CHECK(arr.get(8).is_null());
    CHECK_EQUAL(arr.get(8).get_timestamp(), Timestamp());
    CHECK_EQUAL(arr.get(9).get_timestamp(), Timestamp(2345, 6789));

    CHECK_NOT(arr.get(4) == arr.get(5));

    arr.set(4, -177); // Replace string with int
    CHECK_EQUAL(arr.get(4).get_int(), -177);
    CHECK_EQUAL(arr.get(5).get_string(), "Goodbye yellow brick road");
    CHECK_EQUAL(arr.get(6).get_binary(), BinaryData(bin.data(), bin.size()));

    CHECK_EQUAL(arr.find_first("Goodbye yellow brick road"), 5);

    arr.erase(5); // Erase string
    CHECK_EQUAL(arr.get(5).get_binary(), BinaryData(bin.data(), bin.size()));

    arr.insert(2, Mixed());    // null
    arr.insert(2, int64_t(4500000000)); // Requires more than 32 bit

    CHECK_EQUAL(arr.get(2).get_int(), 4500000000);
    CHECK(arr.is_null(3));

    arr.set(8, Mixed()); // null replaces Timestamp
    CHECK_EQUAL(arr.get(8).get_timestamp(), Timestamp());
    CHECK_EQUAL(arr.get(10).get_timestamp(), Timestamp(2345, 6789));

    arr.set(4, 123.456); // double replaces float
    CHECK_EQUAL(arr.get(4).get_double(), 123.456);
    CHECK_EQUAL(arr.get(2).get_int(), 4500000000);

    ArrayMixed arr2(Allocator::get_default());
    arr2.create();

    arr.move(arr2, 4);
    CHECK_EQUAL(arr.size(), 4);
    CHECK_EQUAL(arr2.size(), 7);
    CHECK_EQUAL(arr.get(0).get_int(), 5);
    CHECK_EQUAL(arr.get(1).get_bool(), true);
    CHECK_EQUAL(arr.get(2).get_int(), 4500000000);
    CHECK(arr.is_null(3));

    /*
    for (size_t i = 0; i < 7; i++)
        std::cout << arr2.get(i) << std::endl;
    */

    CHECK_EQUAL(arr2.get(0).get_double(), 123.456);
    CHECK_EQUAL(arr2.get(1).get_double(), 17.87);
    CHECK_EQUAL(arr2.get(2).get_int(), -177);
    CHECK_EQUAL(arr2.get(3).get_binary(), BinaryData(bin.data(), bin.size()));
    CHECK(arr2.is_null(4));
    CHECK(arr2.is_null(5));
    CHECK_EQUAL(arr2.get(6).get_timestamp(), Timestamp(2345, 6789));

    arr.destroy();
    arr2.destroy();
}

#endif // TEST_ARRAY_VARIANT
