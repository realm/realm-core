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
#include <iostream>

#ifdef __APPLE__
#include <execinfo.h>
#endif

#include <tightdb/util/terminate.hpp>

using namespace std;

namespace tightdb {
namespace util {


TIGHTDB_NORETURN void terminate(string message, const char* file, long line) TIGHTDB_NOEXCEPT
{
    cerr << file << ":" << line << ": " << message << endl;

#ifdef __APPLE__
    void* callstack[128];
    int frames = backtrace(callstack, 128);
    char** strs = backtrace_symbols(callstack, frames);
    for (int i = 0; i < frames; ++i) {
        cerr << strs[i] << endl;
    }
    free(strs);
#endif

    abort();
}


} // namespace util
} // namespace tightdb
