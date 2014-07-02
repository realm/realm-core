/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_TEST_UTIL_VERIFIED_STRING_HPP
#define TIGHTDB_TEST_UTIL_VERIFIED_STRING_HPP

#include <cstddef>
#include <vector>
#include <string>

#ifdef _WIN32
#  include <win32\stdint.h>
#endif

#include <tightdb/string_data.hpp>
#include <tightdb/column_string.hpp>

namespace tightdb {
namespace test_util {


class VerifiedString {
public:
    void add(StringData value);
    void insert(std::size_t ndx, StringData value);
    StringData get(std::size_t ndx);
    void set(std::size_t ndx, StringData value);
    void erase(std::size_t ndx);
    void clear();
    std::size_t find_first(StringData value);
    void find_all(Column& c, StringData value, std::size_t start = 0, std::size_t end = -1);
    std::size_t size();
    bool Verify();
    bool conditional_verify();
    void verify_neighbours(std::size_t ndx);
    void destroy();

private:
    std::vector<std::string> v;
    AdaptiveStringColumn u;
};


} // namespace test_util
} // namespace tightdb

#endif // TIGHTDB_TEST_UTIL_VERIFIED_STRING_HPP
