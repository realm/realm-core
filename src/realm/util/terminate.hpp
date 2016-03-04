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
#ifndef REALM_UTIL_TERMINATE_HPP
#define REALM_UTIL_TERMINATE_HPP

#include <cstdlib>

#include <realm/util/features.h>
#include <realm/util/inspect.hpp>
#include <realm/version.hpp>

#define REALM_TERMINATE(msg) realm::util::terminate((msg), __FILE__, __LINE__)

namespace realm {
namespace util {

/// Install a custom termination notification callback. This will only be called as a result of
/// Realm crashing internally, i.e. a failed assertion or an otherwise irrecoverable error
/// condition. The termination notification callback is supplied with a zero-terminated string
/// containing information relevant for debugging the issue leading to the crash.
///
/// The termination notification callback is shared by all threads, which is another way of saying
/// that it must be reentrant, in case multiple threads crash simultaneously.
///
/// Furthermore, the provided callback must be `noexcept`, indicating that if an exception
/// is thrown in the callback, the process is terminated with a call to `std::terminate`.
void set_termination_notification_callback(void(*callback)(const char* message) noexcept) noexcept;

REALM_NORETURN void terminate(const char* message, const char* file, long line) noexcept;

REALM_NORETURN
void terminate_internal(const char* message, const char* file, long line,
                        const char* interesting_names,
                        std::initializer_list<std::string> interesting_values_stringified) noexcept;

// LCOV_EXCL_START
template<class... Ts>
REALM_NORETURN void terminate(const char* message, const char* file, long line,
                              Ts... infos) noexcept
{
    terminate_internal(message, file, line, "info", {inspect(infos)...});
}
// LCOV_EXCL_STOP

template<class... Args>
REALM_NORETURN void terminate_with_info(const char* message, int line, const char* file,
                                        const char* interesting_names,
                                        Args&&... interesting_values) noexcept
{
    terminate_internal(message, file, line, interesting_names, {inspect(interesting_values)...});
}

} // namespace util
} // namespace realm

#endif // REALM_UTIL_TERMINATE_HPP
