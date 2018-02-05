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

TEST(Util_Any_CopyConstructor)
{
    util::Any first_any(15);
    util::Any second_any(first_any);
    CHECK_EQUAL(util::any_cast<int>(first_any), util::any_cast<int>(second_any));
}

TEST(Util_Any_MoveConstructor)
{
    const int value = 15;
    util::Any first_any(15);
    util::Any second_any(std::move(first_any));
    CHECK(!first_any.has_value());
    CHECK(second_any.has_value());
    CHECK_EQUAL(util::any_cast<int>(second_any), value);
}

TEST(Util_Any_CopyAssignment)
{
    auto first_any = util::Any(15);
    auto second_any = util::Any(first_any);
    CHECK_EQUAL(util::any_cast<int>(first_any), util::any_cast<int>(second_any));
}

TEST(Util_Any_MoveAssignment)
{
    const int value = 15;
    util::Any first_any(15);
    auto second_any = std::move(first_any);
    CHECK(!first_any.has_value());
    CHECK(second_any.has_value());
    CHECK_EQUAL(util::any_cast<int>(second_any), value);
}

TEST(Util_Any_Reset)
{
    auto bool_any = util::Any(false);
    CHECK(bool_any.has_value());
    bool_any.reset();
    CHECK(!bool_any.has_value());
}

TEST(Util_Any_Swap)
{
    const int first_value = 15;
    const bool second_value = false;
    auto first_any = util::Any(first_value);
    auto second_any = util::Any(second_value);
    first_any.swap(second_any);
    CHECK_EQUAL(util::any_cast<int>(second_any), first_value);
    CHECK_EQUAL(util::any_cast<bool>(first_any), second_value);
}

TEST(Util_Any_Bool)
{
    const bool bool_value = true;
    auto bool_any = util::Any(bool_value);
    CHECK_EQUAL(util::any_cast<bool>(bool_any), bool_value);
}

TEST(Util_Any_Long)
{
    const long long_value = 31415927;
    auto long_any = util::Any(long_value);
    CHECK_EQUAL(util::any_cast<long>(long_any), long_value);
}

TEST(Util_Any_String)
{
    const std::string str_value = "util::Any is a replacement for the 'any' type in C++17";
    auto str_any = util::Any(str_value);
    CHECK_EQUAL(util::any_cast<std::string>(str_any), str_value);
}

TEST(Util_Any_SharedPointer)
{
    const std::shared_ptr<bool> ptr_value = std::make_shared<bool>(true);
    auto ptr_any = util::Any(ptr_value);
    CHECK_EQUAL(util::any_cast<std::shared_ptr<bool>>(ptr_any), ptr_value);
}

TEST(Util_Any_ThrowOnError)
{
    const std::string str_value = "util::Any is a replacement for the 'any' type in C++17";
    auto str_any = util::Any(str_value);
    CHECK_THROW_ANY(util::any_cast<bool>(str_any));
}

TEST(Util_Any_ThrowOnEmpty)
{
    util::Any any(true);
    any.reset();
    CHECK_THROW_ANY(util::any_cast<bool>(any));
}

#endif // TEST_UTIL_ANY
