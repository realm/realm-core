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

#ifndef REALM_TIMESTAMP_HPP
#define REALM_TIMESTAMP_HPP

#include <chrono>
#include <cstdint>
#include <ostream>
#include <realm/util/assert.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/null.hpp>

namespace realm {

class Timestamp {
public:
    // Construct from the number of seconds and nanoseconds since the UNIX epoch: 00:00:00 UTC on 1 January 1970
    //
    // To split a native nanosecond representation, only division and modulo are necessary:
    //
    //     s = native_nano / nanoseconds_per_second
    //     n = native_nano % nanoseconds_per_second
    //     Timestamp ts(s, n);
    //
    // To convert back into native nanosecond representation, simple multiply and add:
    //
    //     native_nano = ts.s * nanoseconds_per_second + ts.n
    //
    // Specifically this allows the nanosecond part to become negative (only) for Timestamps before the UNIX epoch.
    // Usually this will not need special attention, but for reference, valid Timestamps will have one of the
    // following sign combinations:
    //
    //     s | n
    //     -----
    //     + | +
    //     + | 0
    //     0 | +
    //     0 | 0
    //     0 | -
    //     - | 0
    //     - | -
    //
    // Examples:
    //     The UNIX epoch is constructed by Timestamp(0, 0)
    //     Relative times are constructed as follows:
    //       +1 second is constructed by Timestamp(1, 0)
    //       +1 nanosecond is constructed by Timestamp(0, 1)
    //       +1.1 seconds (1100 milliseconds after the epoch) is constructed by Timestamp(1, 100000000)
    //       -1.1 seconds (1100 milliseconds before the epoch) is constructed by Timestamp(-1, -100000000)
    //
    Timestamp(int64_t seconds, int32_t nanoseconds)
        : m_seconds(seconds)
        , m_nanoseconds(nanoseconds)
        , m_is_null(false)
    {
        REALM_ASSERT_EX(-nanoseconds_per_second < nanoseconds && nanoseconds < nanoseconds_per_second, nanoseconds);
        const bool both_non_negative = seconds >= 0 && nanoseconds >= 0;
        const bool both_non_positive = seconds <= 0 && nanoseconds <= 0;
        REALM_ASSERT_EX(both_non_negative || both_non_positive, both_non_negative, both_non_positive);
    }
    Timestamp(realm::null)
        : m_is_null(true)
    {
    }
    Timestamp()
        : Timestamp(null{})
    {
    }

    // Creates a timestamp representing `now` as defined by the system clock.
    static Timestamp now()
    {
        auto now = std::chrono::system_clock::now();
        auto sec = std::chrono::time_point_cast<std::chrono::seconds>(now);
        auto ns = static_cast<int32_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now - sec).count());
        return {sec.time_since_epoch().count(), ns};
    }

    // Returns a timestamp representing the UNIX Epoch.
    static inline Timestamp epoch() {
        return Timestamp(0, 0);
    }

    // Convert milliseconds from UNIX epoch to a Timestamp.
    static Timestamp from_milliseconds(int64_t ms)
    {
        return epoch().add_milliseconds(ms);
    }

    // Creates lowest possible date expressible.
    static inline Timestamp min()
    {
        return Timestamp(INT64_MIN, -nanoseconds_per_second + 1);
    }

    // Creates highest possible date expressible.
    static inline Timestamp max()
    {
        return Timestamp(INT64_MAX, nanoseconds_per_second - 1);
    }


    // Return a copy of this timestamp that has been adjusted by the given number of seconds. If the Timestamp
    // overflows in a positive direction it clamps to Timestamp::max(). If it overflows in negative direction it clamps
    // to Timestamp::min().
    Timestamp add_seconds(int64_t s)
    {
        int64_t seconds = m_seconds;
        if (util::int_add_with_overflow_detect(seconds, s)) {
            return (s < 0) ? min() : max();
        }
        else {
            return Timestamp(seconds, m_nanoseconds);
        }
    }

    // Return a copy of this timestamp that has been adjusted by the given number of nanoseconds. If the Timestamp
    // overflows in a positive direction it clamps to Timestamp::max(). If it overflows in negative direction it clamps
    // to Timestamp::min().
    Timestamp add_nanoseconds(int64_t ns)
    {
        int64_t extra_seconds = ns / static_cast<int64_t>(nanoseconds_per_second);
        int32_t extra_nanoseconds = ns % Timestamp::nanoseconds_per_second; // Restrict ns to [-999.999.999, 999.999.999]

        // The nano-second part can never overflow the int32_t type itself as the maximum result
        // is `999.999.999ns + 999.999.999ns`, but we need to handle the case where it
        // exceeds `nanoseconds_pr_second`
        int32_t final_nanoseconds = extra_nanoseconds + m_nanoseconds;
        if (final_nanoseconds <= -nanoseconds_per_second) {
            extra_seconds--;
            final_nanoseconds += nanoseconds_per_second;
        }
        else if (final_nanoseconds >= nanoseconds_per_second) {
            extra_seconds++;
            final_nanoseconds -= nanoseconds_per_second;
        }

        // Adjust seconds while also checking for overflow since the combined nanosecond value could also cause
        // overflow in the seconds field.
        int64_t final_seconds = m_seconds;
        if (util::int_add_with_overflow_detect(final_seconds, extra_seconds))
            return (extra_seconds < 0) ? min() : max();

        return Timestamp(final_seconds, final_nanoseconds);
    }

    // Return a copy of this timestamp that has been adjusted by the given number of milliseconds. If the Timestamp
    // overflows in a positive direction it clamps to Timestamp::max(). If it overflows in negative direction it clamps
    // to Timestamp::min().
    Timestamp add_milliseconds(int64_t ms)
    {
        int64_t seconds = ms/1000;
        int32_t nanoseconds = (ms % 1000) * 1000000;
        return add_seconds(seconds).add_nanoseconds(nanoseconds);
    }

    // Converts this timestamp to milliseconds from UNIX epoch. If the Timestamp overflows in a positive direction it
    // returns INT64_MAX. If it overflows in negative direction it returns INT64_MIN.
    int64_t to_milliseconds() const
    {
        if (m_seconds > INT64_MAX/1000)
            return INT64_MAX;
        if (m_seconds < INT64_MIN/1000)
            return INT64_MIN;
        int64_t ms = m_seconds * 1000; // This will never overflow
        if (util::int_add_with_overflow_detect(ms, m_nanoseconds/1000000))
            return (ms < 0) ? INT64_MIN : INT64_MAX;

        return ms;
    }

    bool is_null() const
    {
        return m_is_null;
    }

    int64_t get_seconds() const noexcept
    {
        REALM_ASSERT(!m_is_null);
        return m_seconds;
    }

    int32_t get_nanoseconds() const noexcept
    {
        REALM_ASSERT(!m_is_null);
        return m_nanoseconds;
    }

    // Note that only == and != operators work if one of the Timestamps are null! Else use realm::Greater,
    // realm::Less, etc, instead. This is in order to collect all treatment of null behaviour in a single place for all
    // types (query_conditions.hpp) to ensure that all types sort and compare null vs. non-null in the same manner,
    // especially for int/float where we cannot override operators. This design is open for discussion, though,
    // because it has usability drawbacks
    bool operator==(const Timestamp& rhs) const
    {
        if (is_null() && rhs.is_null())
            return true;

        if (is_null() != rhs.is_null())
            return false;

        return m_seconds == rhs.m_seconds && m_nanoseconds == rhs.m_nanoseconds;
    }
    bool operator!=(const Timestamp& rhs) const
    {
        return !(*this == rhs);
    }
    bool operator>(const Timestamp& rhs) const
    {
        REALM_ASSERT(!is_null());
        REALM_ASSERT(!rhs.is_null());
        return (m_seconds > rhs.m_seconds) || (m_seconds == rhs.m_seconds && m_nanoseconds > rhs.m_nanoseconds);
    }
    bool operator<(const Timestamp& rhs) const
    {
        REALM_ASSERT(!is_null());
        REALM_ASSERT(!rhs.is_null());
        return (m_seconds < rhs.m_seconds) || (m_seconds == rhs.m_seconds && m_nanoseconds < rhs.m_nanoseconds);
    }
    bool operator<=(const Timestamp& rhs) const
    {
        REALM_ASSERT(!is_null());
        REALM_ASSERT(!rhs.is_null());
        return *this < rhs || *this == rhs;
    }
    bool operator>=(const Timestamp& rhs) const
    {
        REALM_ASSERT(!is_null());
        REALM_ASSERT(!rhs.is_null());
        return *this > rhs || *this == rhs;
    }
    Timestamp& operator=(const Timestamp& rhs) = default;

    template <class Ch, class Tr>
    friend std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out, const Timestamp&);
    static constexpr int32_t nanoseconds_per_second = 1000000000;

private:
    int64_t m_seconds;
    int32_t m_nanoseconds;
    bool m_is_null;
};

// LCOV_EXCL_START
template <class C, class T>
inline std::basic_ostream<C, T>& operator<<(std::basic_ostream<C, T>& out, const Timestamp& d)
{
    out << "Timestamp(" << d.m_seconds << ", " << d.m_nanoseconds << ")";
    return out;
}
// LCOV_EXCL_STOP

} // namespace realm

#endif // REALM_TIMESTAMP_HPP
