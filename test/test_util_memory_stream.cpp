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

#include <realm/util/memory_stream.hpp>

#include "test.hpp"

using namespace realm;

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

TEST(MemoryStream_InputBasic)
{
    realm::util::MemoryInputStream in;
    in.set_c_string("123 4567");
    in.unsetf(std::ios_base::skipws);

    CHECK_EQUAL(in.eof(), false);
    CHECK_EQUAL(in.tellg(), 0);

    int number;
    char sp;

    in >> number;
    CHECK_EQUAL(number, 123);
    CHECK_EQUAL(in.eof(), false);
    CHECK_EQUAL(in.tellg(), 3);

    in >> sp;
    CHECK_EQUAL(sp, ' ');
    CHECK_EQUAL(in.eof(), false);
    CHECK_EQUAL(in.tellg(), 4);

    in.seekg(1);
    in >> number;
    CHECK_EQUAL(number, 23);
    CHECK_EQUAL(in.eof(), false);
    CHECK_EQUAL(in.tellg(), 3);

    in.seekg(5);
    in >> number;
    CHECK_EQUAL(number, 567);
    CHECK_EQUAL(in.eof(), true);
    CHECK_EQUAL(in.tellg(), -1);
}


TEST(MemoryStream_InputSeek)
{
    realm::util::MemoryInputStream in;

    // No buffer
    CHECK_EQUAL(0, in.tellg());
    in.seekg(0);
    CHECK(in);
    CHECK_EQUAL(0, in.tellg());
    in.seekg(0);
    CHECK(in);
    in.seekg(1); // Out of range
    CHECK_NOT(in);
    in.clear();
    CHECK(in);
    in.seekg(-1); // Out of range
    CHECK_NOT(in);

    // Absolute
    in.set_c_string("AB");
    CHECK_EQUAL(0, in.tellg());
    in.seekg(0);
    CHECK(in);
    CHECK_EQUAL(0, in.tellg());
    in.seekg(1);
    CHECK(in);
    CHECK_EQUAL(1, in.tellg());
    in.seekg(2);
    CHECK(in);
    CHECK_EQUAL(2, in.tellg());
    in.seekg(3); // Out of range
    CHECK_NOT(in);
    in.clear();
    CHECK_EQUAL(2, in.tellg());
    CHECK(in);
    in.seekg(-1); // Out of range
    CHECK_NOT(in);
    in.clear();
    CHECK_EQUAL(2, in.tellg());

    // Relative
    in.set_c_string("AB");
    CHECK_EQUAL(0, in.tellg());
    in.seekg(0, std::ios_base::beg);
    CHECK(in);
    CHECK_EQUAL(0, in.tellg());
    in.seekg(0, std::ios_base::cur);
    CHECK(in);
    CHECK_EQUAL(0, in.tellg());
    in.seekg(0, std::ios_base::end);
    CHECK(in);
    CHECK_EQUAL(2, in.tellg());
    in.seekg(+1, std::ios_base::beg);
    CHECK(in);
    CHECK_EQUAL(1, in.tellg());
    in.seekg(+1, std::ios_base::cur);
    CHECK(in);
    CHECK_EQUAL(2, in.tellg());
    in.seekg(-1, std::ios_base::end);
    CHECK(in);
    CHECK_EQUAL(1, in.tellg());
    in.seekg(-1, std::ios_base::cur);
    CHECK(in);
    CHECK_EQUAL(0, in.tellg());
    in.seekg(-1, std::ios_base::beg); // Out of range
    CHECK_NOT(in);
    in.clear();
    CHECK_EQUAL(0, in.tellg());
    in.seekg(+3, std::ios_base::beg); // Out of range
    CHECK_NOT(in);
    in.clear();
    CHECK_EQUAL(0, in.tellg());
    in.seekg(+1, std::ios_base::cur);
    in.seekg(-2, std::ios_base::cur); // Out of range
    CHECK_NOT(in);
    in.clear();
    CHECK_EQUAL(1, in.tellg());
    in.seekg(+2, std::ios_base::cur); // Out of range
    CHECK_NOT(in);
    in.clear();
    CHECK_EQUAL(1, in.tellg());
    in.seekg(+1, std::ios_base::cur);
    in.seekg(-3, std::ios_base::end); // Out of range
    CHECK_NOT(in);
    in.clear();
    CHECK_EQUAL(2, in.tellg());
    in.seekg(+1, std::ios_base::end); // Out of range
    CHECK_NOT(in);
    in.clear();
    CHECK_EQUAL(2, in.tellg());
}

} // unnamed namespace
