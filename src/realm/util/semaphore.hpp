/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
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

// An implementation of C++20's counting_semaphore
// See https://en.cppreference.com/w/cpp/thread/counting_semaphore for documentation
// on how to use this.

#ifndef REALM_UTIL_SEMAPHORE_HPP
#define REALM_UTIL_SEMAPHORE_HPP

#include <realm/util/features.h>

#if __has_include(<version>)
#include <version>
#endif

// C++20 semaphores require iOS 14, so don't use them on Apple platforms
// even when building in C++20 mode.
#if defined(__cpp_lib_semaphore) && !REALM_PLATFORM_APPLE
#include <semaphore>

namespace realm::util {
using CountingSemaphore = std::counting_semaphore;
using BinarySemaphore = std::binary_semaphore;
} // namespace realm::util
#elif REALM_PLATFORM_APPLE
#include <dispatch/dispatch.h>

namespace realm::util {
template <ptrdiff_t LeastMaxValue = std::numeric_limits<ptrdiff_t>::max()>
class CountingSemaphore {
public:
    static constexpr ptrdiff_t max() noexcept
    {
        return LeastMaxValue;
    }

    constexpr explicit CountingSemaphore(ptrdiff_t count)
        : m_semaphore(dispatch_semaphore_create(count))
    {
    }
    ~CountingSemaphore()
    {
        dispatch_release(m_semaphore);
    }

    CountingSemaphore(const CountingSemaphore&) = delete;
    CountingSemaphore& operator=(const CountingSemaphore&) = delete;

    void release(ptrdiff_t update = 1)
    {
        while (update--)
            dispatch_semaphore_signal(m_semaphore);
    }
    void acquire()
    {
        dispatch_semaphore_wait(m_semaphore, DISPATCH_TIME_FOREVER);
    }
    template <class Rep, class Period>
    bool try_acquire_for(std::chrono::duration<Rep, Period> const& rel_time)
    {
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(rel_time);
        return dispatch_semaphore_wait(m_semaphore, dispatch_time(DISPATCH_TIME_NOW, ns)) != 0;
    }
    bool try_acquire()
    {
        dispatch_semaphore_wait(m_semaphore, DISPATCH_TIME_NOW);
    }
    template <class Clock, class Duration>
    bool try_acquire_until(std::chrono::time_point<Clock, Duration> const& abs_time)
    {
        auto const current = Clock::now();
        if (current >= abs_time)
            return try_acquire();
        return try_acquire_for(abs_time - current);
    }

private:
    dispatch_semaphore_t m_semaphore;
};

using BinarySemaphore = CountingSemaphore<1>;
} // namespace realm::util

#else // !__has_include(<semaphore>) && !REALM_PLATFORM_APPLE

#include <condition_variable>
#include <mutex>

namespace realm::util {
template <ptrdiff_t LeastMaxValue = std::numeric_limits<ptrdiff_t>::max()>
class CountingSemaphore {
public:
    static constexpr ptrdiff_t max() noexcept
    {
        return LeastMaxValue;
    }

    constexpr explicit CountingSemaphore(ptrdiff_t count)
        : m_count(count)
    {
    }

    CountingSemaphore(const CountingSemaphore&) = delete;
    CountingSemaphore& operator=(const CountingSemaphore&) = delete;

    void release(ptrdiff_t update = 1)
    {
        {
            std::lock_guard lock(m_mutex);
            m_count += update;
        }
        m_cv.notify_all();
    }
    void acquire()
    {
        std::unique_lock lock(m_mutex);
        m_cv.wait(lock, [this] {
            return m_count > 0;
        });
        --m_count;
    }
    template <class Rep, class Period>
    bool try_acquire_for(std::chrono::duration<Rep, Period> const& rel_time)
    {
        std::unique_lock lock(m_mutex);
        if (m_cv.wait_for(lock, rel_time, [this] {
                return m_count > 0;
            })) {
            --m_count;
            return true;
        }
        return false;
    }
    bool try_acquire()
    {
        std::unique_lock lock(m_mutex);
        if (m_count > 0) {
            --m_count;
            return true;
        }
        return false;
    }
    template <class Clock, class Duration>
    bool try_acquire_until(std::chrono::time_point<Clock, Duration> const& abs_time)
    {
        std::unique_lock lock(m_mutex);
        if (m_cv.wait_until(lock, abs_time, [this] {
                return m_count > 0;
            })) {
            --m_count;
            return true;
        }
        return false;
    }

private:
    std::mutex m_mutex;
    std::condition_variable m_cv;
    ptrdiff_t m_count;
};

using BinarySemaphore = CountingSemaphore<1>;
} // namespace realm::util

#endif

#endif // REALM_UTIL_SEMAPHORE_HPP
