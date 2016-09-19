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

#ifndef REALM_TEST_UTIL_TIMER_HPP
#define REALM_TEST_UTIL_TIMER_HPP

#include <cstdint>
#include <ostream>
#include <string>

namespace realm {
namespace test_util {


class Timer {
public:
    enum Type {
        type_UserTime, // Counting only while the process is running (if supported)
        type_RealTime
    };

    Timer(Type type = type_RealTime)
        : m_type(type)
    {
        reset();
    }

    void reset();

    /// Note: Pausing the timer causes an imprecision of ~1 tick per pause+unpause.
    void pause();
    void unpause();

    /// Returns elapsed time in seconds since last call to reset().
    double get_elapsed_time() const
    {
        return calc_elapsed_seconds(get_timer_ticks() - m_start);
    }

    /// Same as get_elapsed_time().
    operator double() const
    {
        return get_elapsed_time();
    }

    /// Format the elapsed time on the form 0h00m, 00m00s, 00.00s, or
    /// 000.0ms depending on magnitude.
    static void format(double seconds, std::ostream&);

    static std::string format(double seconds);

private:
    const Type m_type;
    uint_fast64_t m_start;
    uint_fast64_t m_paused_at;

    uint_fast64_t get_timer_ticks() const;
    double calc_elapsed_seconds(uint_fast64_t ticks) const;
};


// Implementation:


inline void Timer::reset()
{
    m_start = get_timer_ticks();
    m_paused_at = 0;
}

inline void Timer::pause()
{
    m_paused_at = get_timer_ticks();
}

inline void Timer::unpause()
{
    if (m_paused_at) {
        m_start += get_timer_ticks() - m_paused_at;
        m_paused_at = 0;
    }
}

inline std::ostream& operator<<(std::ostream& out, const Timer& timer)
{
    Timer::format(timer, out);
    return out;
}


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_TIMER_HPP
