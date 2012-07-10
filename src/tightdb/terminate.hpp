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
#ifndef TIGHTDB_TERMINATE_HPP
#define TIGHTDB_TERMINATE_HPP

#include <cstdlib>

#ifdef NDEBUG

#define TIGHTDB_TERMINATE(msg) std::abort()

#else // !NDEBUG

#include <iostream>

#define TIGHTDB_TERMINATE(msg) tightdb::terminate((msg), __FILE__, __LINE__)

namespace tightdb {
    inline void terminate(std::string message, const char* file, long line)
    {
        std::cerr << file << ":" << line << ": " << message << std::endl;
        std::abort();
    }
} // namespace tightdb

#endif // !NDEBUG

#endif // TIGHTDB_TERMINATE_HPP
