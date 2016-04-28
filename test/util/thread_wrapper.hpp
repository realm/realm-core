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
#ifndef REALM_TEST_UTIL_THREAD_WRAPPER_HPP
#define REALM_TEST_UTIL_THREAD_WRAPPER_HPP

#include <exception>
#include <stdexcept> // For Android hack
#include <string> // For Android hack

#include <realm/util/features.h> // For Android hack
#include <realm/util/thread.hpp>

namespace realm {
namespace test_util {


/// Same as util::Thread, but if an uncaught exception terminates the thread,
/// transport that exception to the joining thread, that is, rethrow that
/// exception from the call to join().
///
/// FIXME: Unfortunately, Android NDK has no support for exception
/// transportation (at least with some of the offered STL implementations, and
/// at least up to version 10e of the NDK), so on that platform, the exception
/// thrown by join() will be of type std::runtime_error regardless of the type
/// of exception that terminated the thread.
class ThreadWrapper {
public:
    template<class F>
    void start(const F& func)
    {
        m_thread.start([=] { Runner<F>::run(func, this); });
    }

    void join()
    {
        m_thread.join();
#if REALM_ANDROID
        if (m_exception_thrown)
            throw std::runtime_error(std::move(m_exception_message));
#else
        if (m_exception)
            std::rethrow_exception(m_exception);
#endif
    }

    bool joinable() noexcept
    {
        return m_thread.joinable();
    }

private:
    util::Thread m_thread;
#if REALM_ANDROID
    std::string m_exception_message;
    bool m_exception_thrown = false;
#else
    std::exception_ptr m_exception;
#endif

    template<class F>
    struct Runner {
        static void run(F func, ThreadWrapper* tw)
        {
            try {
                func();
            }
#if REALM_ANDROID
            catch (std::exception& ex) {
                tw->m_exception_message = ex.what(); // Throws
                tw->m_exception_thrown = true;
            }
#endif
            catch (...) {
#if REALM_ANDROID
                tw->m_exception_message = "Unknown"; // Throws
                tw->m_exception_thrown = true;
#else
                tw->m_exception = std::current_exception();
#endif
            }
        }
    };
};


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_THREAD_WRAPPER_HPP
