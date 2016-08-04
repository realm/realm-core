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

#ifndef REALM_TEST_UTIL_VERIFIED_STRING_HPP
#define REALM_TEST_UTIL_VERIFIED_STRING_HPP

#include <cstddef>
#include <vector>
#include <string>

#include <realm/string_data.hpp>
#include <realm/column_string.hpp>

namespace realm {
namespace test_util {


class VerifiedString {
public:
    VerifiedString();
    ~VerifiedString();
    void add(StringData value);
    void insert(size_t ndx, StringData value);
    StringData get(size_t ndx);
    void set(size_t ndx, StringData value);
    void erase(size_t ndx);
    void clear();
    size_t find_first(StringData value);
    void find_all(IntegerColumn& c, StringData value, size_t start = 0, size_t end = -1);
    size_t size();
    bool verify();
    bool conditional_verify();
    void verify_neighbours(size_t ndx);

private:
    std::vector<std::string> v;
    StringColumn u;
};


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_VERIFIED_STRING_HPP
