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
#include <iostream>
#include <sstream>
#include <realm/util/features.h>

#if REALM_PLATFORM_APPLE
#  include <dlfcn.h>
#  include <execinfo.h>
#  include <CoreFoundation/CoreFoundation.h>
#endif

#ifdef __ANDROID__
#  include <android/log.h>
#endif

#include <realm/util/terminate.hpp>

// extern "C" and noinline so that a readable message shows up in the stack trace
// of the crash
// prototype here to silence warning
extern "C" REALM_NORETURN REALM_NOINLINE
void please_report_this_error_to_help_at_realm_dot_io();

extern "C" REALM_NORETURN REALM_NOINLINE
void please_report_this_error_to_help_at_realm_dot_io() {
    std::abort();
}

namespace {

#if REALM_PLATFORM_APPLE
void nslog(const char *message) {
    CFStringRef str = CFStringCreateWithCStringNoCopy(kCFAllocatorDefault, message, kCFStringEncodingUTF8, kCFAllocatorNull);
    CFShow(str);

    // Log the message to Crashlytics if it's loaded into the process
    void* addr = dlsym(RTLD_DEFAULT, "CLSLog");
    if (addr) {
        auto fn = reinterpret_cast<void (*)(CFStringRef, ...)>(reinterpret_cast<size_t>(addr));
        fn(CFSTR("%@"), str);
    }

    CFRelease(str);
}
#endif

} // unnamed namespace

namespace realm {
namespace util {

REALM_NORETURN void terminate_internal(std::stringstream& ss) noexcept
{

#if REALM_PLATFORM_APPLE
    void* callstack[128];
    int frames = backtrace(callstack, 128);
    char** strs = backtrace_symbols(callstack, frames);
    for (int i = 0; i < frames; ++i) {
        ss << strs[i] << '\n';
    }
    free(strs);
#endif

    ss << "IMPORTANT: if you see this error, please send this log to help@realm.io.";
#ifdef REALM_DEBUG
    std::cerr << ss.rdbuf() << '\n';
#endif

#if REALM_PLATFORM_APPLE
    nslog(ss.str().c_str());
#elif defined(__ANDROID__)
    __android_log_print(ANDROID_LOG_ERROR, "REALM", ss.str().c_str());
#endif

    please_report_this_error_to_help_at_realm_dot_io();
}

REALM_NORETURN void terminate(const char* message, const char* file, long line) noexcept
{
    std::stringstream ss;
    ss << file << ":" << line << ": " REALM_VER_CHUNK " " << message << '\n';

    throw("");

    terminate_internal(ss);
}

} // namespace util
} // namespace realm
