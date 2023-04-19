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

#include <realm/impl/simulated_failure.hpp>

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

TEST_IF(Impl_SimulatedFailure_OneShot, _impl::SimulatedFailure::is_enabled())
{
    using sf = _impl::SimulatedFailure;
    sf::OneShotPrimeGuard pg(sf::generic);
    CHECK_THROW(sf::trigger(sf::generic), sf);
    sf::trigger(sf::generic);
}


TEST_IF(Impl_SimulatedFailure_Random, _impl::SimulatedFailure::is_enabled())
{
    using sf = _impl::SimulatedFailure;
    sf::RandomPrimeGuard pg(sf::generic, 1, 2,                       // 50% chance of failure
                            test_util::random_int<uint_fast64_t>()); // Seed from global generator

    // Must be possible to find a case where it triggers
    for (;;) {
        if (sf::check_trigger(sf::generic))
            break;
    }

    // Must be possible to find a case where it does not trigger
    for (;;) {
        if (!sf::check_trigger(sf::generic))
            break;
    }

    // Must be possible to find a second case where it triggers
    for (;;) {
        if (sf::check_trigger(sf::generic))
            break;
    }

    // Must be possible to find a second case where it does not trigger
    for (;;) {
        if (!sf::check_trigger(sf::generic))
            break;
    }
}

} // unnamed namespace
