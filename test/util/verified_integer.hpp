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

#ifndef REALM_TEST_UTIL_VERIFIED_INTEGER_HPP
#define REALM_TEST_UTIL_VERIFIED_INTEGER_HPP

#include <cstddef>
#include <vector>

#include <realm/array.hpp>
#include <realm/column_integer.hpp>

#include "random.hpp"

namespace realm {
namespace test_util {


class VerifiedInteger {
public:
    VerifiedInteger(Random&);
    ~VerifiedInteger();
    void add(int64_t value);
    void insert(size_t ndx, int64_t value);
    void insert(size_t ndx, const char* value);
    int64_t get(size_t ndx);
    void set(size_t ndx, int64_t value);
    void erase(size_t ndx);
    void clear();
    size_t find_first(int64_t value);
    size_t size();
    int64_t sum(size_t start = 0, size_t end = -1);
    int64_t maximum(size_t start = 0, size_t end = -1);
    int64_t minimum(size_t start = 0, size_t end = -1);
    bool verify();
    bool occasional_verify();
    void verify_neighbours(size_t ndx);

private:
    std::vector<int64_t> v;
    IntegerColumn u;
    Random& m_random;
};


// Implementation

inline VerifiedInteger::VerifiedInteger(Random& random)
    : u(Allocator::get_default())
    , m_random(random)
{
    u.create();
}


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_VERIFIED_INTEGER_HPP
