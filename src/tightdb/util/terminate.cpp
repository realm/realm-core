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
#include <CoreFoundation/CoreFoundation.h>
#endif

#ifdef __ANDROID__
#include <android/log.h>
#endif

#include <tightdb/util/terminate.hpp>

using namespace std;

namespace tightdb {
namespace util {

#ifdef __APPLE__
void nslog(const char *message) {
    CFStringRef str = CFStringCreateWithCString(kCFAllocatorDefault, message, kCFStringEncodingMacRoman);
    CFShow(str);
    CFRelease(str);
}
#endif

TIGHTDB_NORETURN void abort() TIGHTDB_NOEXCEPT
{
    std::abort();
}


TIGHTDB_NORETURN void terminate(string message, const char* file, long line) TIGHTDB_NOEXCEPT
{
    const char *support_message = "Please send the log and Realm files to help@realm.io.";
    cerr << file << ":" << line << ": " << message << endl;

#if defined __APPLE__
    void* callstack[128];
    int frames = backtrace(callstack, 128);
    char** strs = backtrace_symbols(callstack, frames);
    for (int i = 0; i < frames; ++i) {
        cerr << strs[i] << endl;
        nslog(strs[i]);
    }
    free(strs);
    nslog(support_message);
#elif defined __ANDROID__
    __android_log_print(ANDROID_LOG_ERROR, "TIGHTDB", support_message);
#else
    cerr << support_message << endl;
#endif
    abort();
}


} // namespace util
} // namespace tightdb
