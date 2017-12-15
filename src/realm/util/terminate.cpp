/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/util/terminate.hpp>

#include <iostream>
#include <sstream>
#include <realm/util/features.h>
#include <realm/util/thread.hpp>

#if REALM_PLATFORM_APPLE

#if REALM_APPLE_OS_LOG
#include <os/log.h>
#else
#include <asl.h>
#endif

#include <dlfcn.h>
#include <execinfo.h>
#include <CoreFoundation/CoreFoundation.h>
#endif

#if REALM_ANDROID
#include <android/log.h>
#endif


// extern "C" and noinline so that a readable message shows up in the stack trace
// of the crash
// prototype here to silence warning
extern "C" REALM_NORETURN REALM_NOINLINE void please_report_this_error_to_help_at_realm_dot_io();

// LCOV_EXCL_START
extern "C" REALM_NORETURN REALM_NOINLINE void please_report_this_error_to_help_at_realm_dot_io()
{
    std::abort();
}
// LCOV_EXCL_STOP

namespace {

#if REALM_PLATFORM_APPLE
void nslog(const char* message) noexcept
{
    // Standard error goes nowhere for applications managed by launchd,
    // so log to ASL/unified logging system logs as well.
    fputs(message, stderr);
#if REALM_APPLE_OS_LOG
    // The unified logging system considers dynamic strings to be private in
    // order to protect users. This means we must specify "%{public}s" to get
    // the message here. See `man os_log` for more details.
    os_log_error(OS_LOG_DEFAULT, "%{public}s", message);
#else
    asl_log(nullptr, nullptr, ASL_LEVEL_ERR, "%s", message);
#endif
    // Log the message to Crashlytics if it's loaded into the process
    void* addr = dlsym(RTLD_DEFAULT, "CLSLog");
    if (addr) {
        CFStringRef str =
            CFStringCreateWithCStringNoCopy(kCFAllocatorDefault, message, kCFStringEncodingUTF8, kCFAllocatorNull);
        auto fn = reinterpret_cast<void (*)(CFStringRef, ...)>(reinterpret_cast<size_t>(addr));
        fn(CFSTR("%@"), str);
        CFRelease(str);
    }
}

void (*termination_notification_callback)(const char*) noexcept = nslog;

#elif REALM_ANDROID

void android_log(const char* message) noexcept
{
    __android_log_write(ANDROID_LOG_ERROR, "REALM", message);
}

void (*termination_notification_callback)(const char*) noexcept = android_log;

#else

void (*termination_notification_callback)(const char*) noexcept = nullptr;

#endif

} // unnamed namespace

namespace realm {
namespace util {

// LCOV_EXCL_START
REALM_NORETURN static void terminate_internal(std::stringstream& ss) noexcept
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

    ss << "IMPORTANT: if you see this error, please send this log and info about which version you are using and other relevant reproduction info to help@realm.io.";
#ifdef REALM_DEBUG
    std::cerr << ss.rdbuf() << '\n';
    std::string thread_name;
    if (Thread::get_name(thread_name))
        std::cerr << "Thread name: " << thread_name << "\n";
#endif

    if (termination_notification_callback) {
        termination_notification_callback(ss.str().c_str());
    }

    please_report_this_error_to_help_at_realm_dot_io();
}

REALM_NORETURN void terminate(const char* message, const char* file, long line) noexcept
{
    std::stringstream ss;
    ss << file << ":" << line << ": " REALM_VER_CHUNK " " << message << '\n';
    terminate_internal(ss);
}

REALM_NORETURN void terminate(const char* message, const char* file, long line,
                              std::initializer_list<Printable>&& values) noexcept
{
    std::stringstream ss;
    ss << file << ':' << line << ": " REALM_VER_CHUNK " " << message;
    Printable::print_all(ss, values, false);
    ss << '\n';
    terminate_internal(ss);
}
REALM_NORETURN void terminate_with_info(const char* message, const char* file, long line,
                                        const char* interesting_names,
                                        std::initializer_list<Printable>&& values) noexcept
{
    std::stringstream ss;
    ss << file << ':' << line << ": " REALM_VER_CHUNK " " << message << " with " << interesting_names << " = ";
    Printable::print_all(ss, values, true);
    ss << '\n';
    terminate_internal(ss);
}
// LCOV_EXCL_STOP

} // namespace util
} // namespace realm
