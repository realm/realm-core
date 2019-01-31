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
#ifdef TEST_UTIL_CIRCULAR_BUFFER

#include <realm/util/circular_buffer.hpp>
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

TEST(Utils_CircularBuffer_Basics)
{
    CircularBuffer<int> buffer(10);
    std::vector<int> vec;

    for (int i = 0; i < 5; i++) {
        buffer.insert(i);
    }
    CHECK_EQUAL(buffer.size(), 5);

    for (int i = 5; i < 10; i++) {
        buffer.insert(i);
    }
    CHECK_EQUAL(buffer.size(), 10);
    for (int i = 0; i < 10; i++) {
        CHECK_EQUAL(buffer[i], i);
    }

    buffer.insert(10);
    CHECK_EQUAL(buffer.size(), 10);
    for (int i = 0; i < 10; i++) {
        CHECK_EQUAL(buffer[i], i + 1);
    }

    for (int i = 0; i < 8; i++) {
        buffer.insert(i + 11);
    }
    for (int i = 0; i < 10; i++) {
        CHECK_EQUAL(buffer[i], i + 9);
    }
    int val = 9;
    for (auto i : buffer) {
        CHECK_EQUAL(i, val++);
    }

    bool ok = false;
    try {
        CircularBuffer<std::string> str_buf(0);
    }
    catch (const std::exception&) {
        ok = true;
    }
    CHECK(ok);

    CircularBuffer<std::string> str_buf(1);
    str_buf.insert("Foo");
    str_buf.insert("Bar");
    CHECK_EQUAL(str_buf[0], "Bar");
}
#endif
