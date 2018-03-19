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

#include <realm/column_integer.hpp>
#include <realm/array_key.hpp>

#include "../util/number_names.hpp"
#include "../util/verified_string.hpp"

#include "../testsettings.hpp"
#include "../test.hpp"

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


namespace {

std::string randstring(Random& random)
{
    // If there are in the order of REALM_MAX_BPNODE_SIZE different strings, then we'll get a good
    // distribution btw. arrays with no matches and arrays with multiple matches, when
    // testing Find/FindAll
    int64_t t = random.draw_int_mod(100) * 100;
    size_t len = random.draw_int_mod(10) * 100 + 1;
    std::string s;
    while (s.length() < len)
        s += number_name(static_cast<size_t>(t));

    s = s.substr(0, len);
    return s;
}

} // anonymous namespace


TEST_IF(Strings_Monkey2, TEST_DURATION >= 1)
{
    uint64_t ITER = 16 * 5000 * TEST_DURATION * TEST_DURATION * TEST_DURATION;
    int seed = 123;

    VerifiedString a;
    IntegerColumn res(Allocator::get_default());
    res.create();

    Random random(seed);
    int trend = 5;

    for (size_t iter = 0; iter < ITER; iter++) {

        //        if (random.chance(1, 10))
        //            std::cout << "Input bitwidth around ~"<<current_bitwidth<<", , a.Size()="<<a.size()<<"\n";

        if (random.draw_int_mod(ITER / 100) == 0) {
            trend = random.draw_int_mod(10);
            std::string rand1(randstring(random));
            a.find_first(rand1);
        }

        if (random.draw_int_mod(10) > trend && a.size() < ITER / 100) {
            if (random.draw_bool()) {
                // Insert
                size_t pos = random.draw_int_max(a.size());
                std::string rstr(randstring(random));
                a.insert(pos, rstr);
            }
            else {
                // Add
                std::string rstr(randstring(random));
                a.add(rstr);
            }
        }
        else if (a.size() > 0) {
            // Delete
            size_t i = random.draw_int_mod(a.size());
            a.erase(i);
        }
    }

    // Cleanup
    res.destroy();
}
