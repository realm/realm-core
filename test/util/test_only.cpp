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

#include "test_only.hpp"

namespace {

struct TestOnly {
    const char* m_test_name;
    TestOnly()
        : m_test_name(nullptr)
    {
    }
};

TestOnly& access_test_only()
{
    static TestOnly x;
    return x;
}

} // anonymous namespace

namespace realm {
namespace test_util {

SetTestOnly::SetTestOnly(const char* test_name)
{
    TestOnly& test_only = access_test_only();
    test_only.m_test_name = test_name;
}

const char* get_test_only()
{
    TestOnly& test_only = access_test_only();
    return test_only.m_test_name;
}

} // namespace test_util
} // namespace realm
