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

#ifndef REALM_TEST_UTIL_TEST_ONLY_HPP
#define REALM_TEST_UTIL_TEST_ONLY_HPP

#include "unit_test.hpp"

#define ONLY(name)                                                                                                   \
    realm::test_util::SetTestOnly realm_set_test_only__##name(#name);                                                \
    TEST(name)

#define NONCONCURRENT_ONLY(name)                                                                                     \
    realm::test_util::SetTestOnly realm_set_test_only__##name(#name);                                                \
    NONCONCURRENT_TEST(name)

#define ONLY_TYPES(name, ...)                                                                                        \
    realm::test_util::SetTestOnly realm_set_test_only__##name(#name "*");                                            \
    TEST_TYPES(name, __VA_ARGS__)

namespace realm {
namespace test_util {

struct SetTestOnly {
    SetTestOnly(const char* test_name);
};

const char* get_test_only();

} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_TEST_ONLY_HPP
