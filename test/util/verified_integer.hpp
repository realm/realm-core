/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/
#ifndef REALM_TEST_UTIL_VERIFIED_INTEGER_HPP
#define REALM_TEST_UTIL_VERIFIED_INTEGER_HPP

#include <cstddef>
#include <vector>

#ifdef _WIN32
#  include <win32\stdint.h>
#endif

#include <realm/array.hpp>
#include <realm/column.hpp>

#include "random.hpp"

namespace realm {
namespace test_util {


class VerifiedInteger {
public:
    VerifiedInteger(Random&);
    ~VerifiedInteger();
    void add(int64_t value);
    void insert(std::size_t index, int64_t value);
    void insert(std::size_t index, const char *value);
    int64_t get(std::size_t index);
    void set(std::size_t index, int64_t value);
    void erase(std::size_t index);
    void clear();
    size_t find_first(int64_t value);
    void find_all(IntegerColumn &c, int64_t value, std::size_t start = 0, std::size_t end = -1);
    std::size_t size();
    int64_t sum(std::size_t start = 0, std::size_t end = -1);
    int64_t maximum(std::size_t start = 0, std::size_t end = -1);
    int64_t minimum(std::size_t start = 0, std::size_t end = -1);
    bool verify();
    bool occasional_verify();
    void verify_neighbours(std::size_t index);

private:
    std::vector<int64_t> v;
    IntegerColumn u;
    Random& m_random;
};



// Implementation

inline VerifiedInteger::VerifiedInteger(Random& random):
    u(IntegerColumn::unattached_root_tag(), Allocator::get_default()),
    m_random(random)
{
    u.get_root_array()->create(Array::type_Normal); // Throws
}


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_VERIFIED_INTEGER_HPP
