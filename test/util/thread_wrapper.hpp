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

#ifndef REALM_TEST_UTIL_THREAD_WRAPPER_HPP
#define REALM_TEST_UTIL_THREAD_WRAPPER_HPP

#include <exception>
#include <string>
#include <iostream>

#include <realm/util/thread.hpp>

namespace realm {
namespace test_util {


/// Catch exceptions thrown in threads and make the exception message
/// available to the thread that calls ThreadWrapper::join().
class ThreadWrapper {
public:
    template <class F>
    void start(const F& func)
    {
        m_except = false;
        m_thread.start([func, this] {
            Runner<F>::run(func, this);
        });
    }

    /// Returns 'true' if thread has thrown an exception. In that case
    /// the exception message will also be writte to std::cerr.
    bool join()
    {
        std::string except_msg;
        if (join(except_msg)) {
            std::cerr << "Exception thrown in thread: " << except_msg << "\n";
            return true;
        }
        return false;
    }

    /// Returns 'true' if thread has thrown an exception. In that
    /// case the exception message will have been assigned to \a
    /// except_msg.
    bool join(std::string& except_msg)
    {
        m_thread.join();
        if (m_except) {
            except_msg = m_except_msg;
            return true;
        }
        return false;
    }

    bool joinable() noexcept
    {
        return m_thread.joinable();
    }

private:
    util::Thread m_thread;
    bool m_except;
    std::string m_except_msg;

    template <class F>
    struct Runner {
        static void run(F func, ThreadWrapper* tw)
        {
            try {
                func();
            }
            catch (std::exception& e) {
                tw->m_except = true;
                tw->m_except_msg = e.what();
            }
            catch (...) {
                tw->m_except = true;
                tw->m_except_msg = "Unknown error";
            }
        }
    };
};


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_THREAD_WRAPPER_HPP
