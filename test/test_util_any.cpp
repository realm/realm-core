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
#ifdef TEST_UTIL_ANY

#include <realm/util/any.hpp>

#include <memory>
#include <string>

#include "test.hpp"

using namespace realm;

TEST(Util_AnyCast_Basics)
{
    std::any any(15);
    CHECK_EQUAL(util::any_cast<int>(any), 15);
    CHECK_EQUAL(util::any_cast<int&>(any), 15);
    CHECK_EQUAL(util::any_cast<int&&>(std::move(any)), 15);
    CHECK_THROW(util::any_cast<bool>(any), std::bad_cast);
    CHECK_THROW(util::any_cast<bool&>(any), std::bad_cast);
    CHECK_THROW(util::any_cast<bool&&>(std::move(any)), std::bad_cast);

    const std::any const_any(15);
    CHECK_EQUAL(util::any_cast<const int>(any), 15);
    CHECK_EQUAL(util::any_cast<const int&>(any), 15);
    CHECK_EQUAL(util::any_cast<const int&&>(std::move(any)), 15);
    CHECK_THROW(util::any_cast<const bool>(const_any), std::bad_cast);
    CHECK_THROW(util::any_cast<const bool&>(const_any), std::bad_cast);
}

// Verify that the references we hand out are actually references to the correct
// thing and not some dangling local
TEST(Util_AnyCast_MutateViaReference)
{
    std::any any(std::string("a"));
    util::any_cast<std::string&>(any) = "b";
    CHECK_EQUAL(util::any_cast<const std::string&>(any), "b");

    // Set it to something which won't fit in the small-string buffer so that
    // moving from it will mutate the source. This is of course not guaranteed
    // to actually happen, but it'll work with any sensible implementation.
    any = std::string('a', 100);
    std::string str = util::any_cast<std::string&&>(std::move(any));
    CHECK_EQUAL(str, std::string('a', 100));
    CHECK_EQUAL(util::any_cast<std::string>(any), "");
}
#endif // TEST_UTIL_ANY
