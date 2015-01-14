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
#ifndef TIGHTDB_TEST_UTIL_TIMER_HPP
#define TIGHTDB_TEST_UTIL_TIMER_HPP

#include <stdint.h>
#include <cmath>
#include <string>
#include <sstream>
#include <ostream>

namespace tightdb {
namespace test_util {


class Timer {
public:
    enum Type {
        type_UserTime, // Counting only while the process is running (if supported)
        type_RealTime
    };

    Timer(Type type = type_RealTime): m_type(type) { reset(); }

    void reset();
    void pause();
    void unpause();

    /// Returns elapsed time in seconds since last call to reset().
    double get_elapsed_time() const
    {
        return calc_elapsed_seconds(get_timer_ticks() - m_start - m_paused_for);
    }

    /// Same as get_elapsed_time().
    operator double() const { return get_elapsed_time(); }

    /// Format the elapsed time on the form 0h00m, 00m00s, 00.00s, or
    /// 000.0ms depending on magnitude.
    template<class Ch, class Tr>
    friend std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>&, const Timer&);

    template<class Ch, class Tr>
    static void format(double seconds, std::basic_ostream<Ch, Tr>&);

    static std::string format(double seconds);

private:
    const Type m_type;
    uint_fast64_t m_start;
    uint_fast64_t m_paused_at;
    uint_fast64_t m_paused_for;

    uint_fast64_t get_timer_ticks() const;
    static double calc_elapsed_seconds(uint_fast64_t ticks);
};


// Implementation:


inline void Timer::reset() {
    m_start = get_timer_ticks();
    m_paused_at = 0;
    m_paused_for = 0;
}

inline void Timer::pause() {
    m_paused_at = get_timer_ticks();
}

inline void Timer::unpause() {
    if (m_paused_at) {
        m_paused_for += get_timer_ticks() - m_paused_at;
        m_paused_at = 0;
    }
}

template<class Ch, class Tr>
inline std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out, const Timer& timer)
{
    Timer::format(timer, out);
    return out;
}

template<class Ch, class Tr>
void Timer::format(double seconds_float, std::basic_ostream<Ch, Tr>& out)
{
    uint_fast64_t rounded_minutes = uint_fast64_t(std::floor(seconds_float/60 + 0.5));
    if (60 <= rounded_minutes) {
        // 1h0m -> inf
        uint_fast64_t hours             = rounded_minutes / 60;
        uint_fast64_t remaining_minutes = rounded_minutes - hours*60;
        out << hours             << "h";
        out << remaining_minutes << "m";
    }
    else {
        uint_fast64_t rounded_seconds = uint_fast64_t(std::floor(seconds_float + 0.5));
        if (60 <= rounded_seconds) {
            // 1m0s -> 59m59s
            uint_fast64_t minutes           = rounded_seconds / 60;
            uint_fast64_t remaining_seconds = rounded_seconds - minutes*60;
            out << minutes           << "m";
            out << remaining_seconds << "s";
        }
        else {
            uint_fast64_t rounded_centies = uint_fast64_t(std::floor(seconds_float*100 + 0.5));
            if (100 <= rounded_centies) {
                // 1s -> 59.99s
                uint_fast64_t seconds           = rounded_centies / 100;
                uint_fast64_t remaining_centies = rounded_centies - seconds*100;
                out << seconds;
                if (0 < remaining_centies) {
                    out << ".";
                    if (remaining_centies % 10 == 0) {
                        out << (remaining_centies / 10);
                    }
                    else {
                        out << remaining_centies;
                    }
                }
                out << "s";
            }
            else {
                // 0ms -> 999.9ms
                uint_fast64_t rounded_centi_centies =
                    uint_fast64_t(std::floor(seconds_float*10000 + 0.5));
                uint_fast64_t millis                  = rounded_centi_centies / 10;
                uint_fast64_t remaining_centi_centies = rounded_centi_centies - millis*10;
                out << millis;
                if (0 < remaining_centi_centies) {
                    out << "." << remaining_centi_centies;
                }
                out << "ms";
            }
        }
    }
}


inline std::string Timer::format(double seconds)
{
    std::ostringstream out;
    format(seconds, out);
    return out.str();
}


} // namespace test_util
} // namespace tightdb

#endif // TIGHTDB_TEST_UTIL_TIMER_HPP
