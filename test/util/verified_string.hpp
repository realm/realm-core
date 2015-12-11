/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
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
#ifndef REALM_TEST_UTIL_VERIFIED_STRING_HPP
#define REALM_TEST_UTIL_VERIFIED_STRING_HPP

#include <cstddef>
#include <vector>
#include <string>

#ifdef _WIN32
#  include <win32\stdint.h>
#endif

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
