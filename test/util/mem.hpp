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
#ifndef REALM_TEST_UTIL_MEM_HPP
#define REALM_TEST_UTIL_MEM_HPP

#include <cstddef>

namespace realm {
namespace test_util {


/// Returns the amount (in number of bytes) of virtaul memory
/// allocated to the calling process.
///
/// FIXME: 'std::size_t' is inappropriate for holding the total memory
/// usage. C++11 guarantees only that it can hold the size of a single
/// object or array. 'std::uintptr_t' would have been the ideal type
/// to use here, but C++11 does not required it to be available (see
/// 18.4.1 "Header <cstdint> synopsis".)
std::size_t get_mem_usage();


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_MEM_HPP
