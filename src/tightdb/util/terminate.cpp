/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
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
#include <iostream>
#include <sstream>

#ifdef __APPLE__
#include <execinfo.h>
#include <CoreFoundation/CoreFoundation.h>
#endif

#ifdef __ANDROID__
#include <android/log.h>
#endif

#include <tightdb/util/terminate.hpp>

// extern "C" and noinline so that a readable message shows up in the stack trace
// of the crash
extern "C" REALM_NORETURN REALM_NOINLINE
void please_report_this_error_to_help_at_realm_dot_io() {
    std::abort();
}

namespace realm {
namespace util {

#ifdef __APPLE__
void nslog(const char *message) {
    CFStringRef str = CFStringCreateWithCStringNoCopy(kCFAllocatorDefault, message, kCFStringEncodingUTF8, kCFAllocatorNull);
    CFShow(str);
    CFRelease(str);
}
#endif

REALM_NORETURN void terminate_internal(std::stringstream& ss) REALM_NOEXCEPT
{

#if defined(__APPLE__)
    void* callstack[128];
    int frames = backtrace(callstack, 128);
    char** strs = backtrace_symbols(callstack, frames);
    for (int i = 0; i < frames; ++i) {
        ss << strs[i] << "\n";
    }
    free(strs);
#endif

    ss << "IMPORTANT: if you see this error, please send this log to help@realm.io.";
#ifdef REALM_DEBUG
    std::cerr << ss.rdbuf();
#endif

#if defined(__APPLE__)
    nslog(ss.str().c_str());
#elif defined(__ANDROID__)
    __android_log_print(ANDROID_LOG_ERROR, "TIGHTDB", ss.str().c_str());
#endif

    please_report_this_error_to_help_at_realm_dot_io();
}


} // namespace util
} // namespace realm
