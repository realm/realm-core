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
#ifndef REALM_TEST_UTIL_TIMER_HPP
#define REALM_TEST_UTIL_TIMER_HPP

#include <stdint.h>
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

    Timer(Type type = type_RealTime): m_type(type) { reset(); }

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
    operator double() const { return get_elapsed_time(); }

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


inline void Timer::reset() {
    m_start = get_timer_ticks();
    m_paused_at = 0;
}

inline void Timer::pause() {
    m_paused_at = get_timer_ticks();
}

inline void Timer::unpause() {
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
