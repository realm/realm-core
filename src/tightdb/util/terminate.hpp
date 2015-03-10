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
#ifndef TIGHTDB_UTIL_TERMINATE_HPP
#define TIGHTDB_UTIL_TERMINATE_HPP

#include <sstream>
#include <cstdlib>
#include <string>
#include <stdint.h>
#include <tightdb/util/features.h>

#define TIGHTDB_TERMINATE(msg) tightdb::util::terminate((msg), __FILE__, __LINE__)

namespace tightdb {
namespace util {
TIGHTDB_NORETURN void terminate_internal(std::stringstream&) TIGHTDB_NOEXCEPT;

TIGHTDB_NORETURN inline void terminate(const char* message, const char* file, long line) TIGHTDB_NOEXCEPT {
    std::stringstream ss;
    ss << file << ":" << line << ": " << message << "\n";
    terminate_internal(ss);
}

template <typename T1, typename T2>
TIGHTDB_NORETURN void terminate(const char* message, const char* file, long line, T1 info1, T2 info2) TIGHTDB_NOEXCEPT {
    std::stringstream ss;
    ss << file << ":" << line << ": " << message << " [" << info1 << ", " << info2 << "]\n";
    ss << file << ":" << line << ": " << message << "\n";
    terminate_internal(ss);
};

} // namespace util
} // namespace tightdb

#endif // TIGHTDB_UTIL_TERMINATE_HPP
