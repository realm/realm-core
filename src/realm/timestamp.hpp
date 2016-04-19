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
#ifndef REALM_TIMESTAMP_HPP
#define REALM_TIMESTAMP_HPP

#include <stdint.h>
#include <ostream>
#include <realm/util/assert.hpp>

namespace realm {

struct Timestamp {
    Timestamp(int64_t seconds, uint32_t nanoseconds) : m_seconds(seconds), m_nanoseconds(nanoseconds), m_is_null(false) 
    {
        REALM_ASSERT_3(nanoseconds, <, nanoseconds_per_second);
    }
    Timestamp() : m_is_null(true) { }

    bool is_null() const { return m_is_null; }
    
    // Note that nullability is handled by query system. These operators are only invoked for non-null dates.
    bool operator==(const Timestamp& rhs) const { return m_seconds == rhs.m_seconds && m_nanoseconds == rhs.m_nanoseconds; }
    bool operator!=(const Timestamp& rhs) const { return m_seconds != rhs.m_seconds || m_nanoseconds != rhs.m_nanoseconds; }
    bool operator>(const Timestamp& rhs) const { return (m_seconds > rhs.m_seconds) || (m_seconds == rhs.m_seconds && m_nanoseconds > rhs.m_nanoseconds); }
    bool operator<(const Timestamp& rhs) const { return (m_seconds < rhs.m_seconds) || (m_seconds == rhs.m_seconds && m_nanoseconds < rhs.m_nanoseconds); }
    bool operator<=(const Timestamp& rhs) const { return *this < rhs || *this == rhs; }
    bool operator>=(const Timestamp& rhs) const { return *this > rhs || *this == rhs; }
    Timestamp& operator=(const Timestamp& rhs) = default;

    template<class Ch, class Tr>
    friend std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out, const Timestamp&);

    int64_t m_seconds;
    uint32_t m_nanoseconds;
    bool m_is_null;

    static constexpr uint32_t nanoseconds_per_second = 1000000000;
};

template<class C, class T>
inline std::basic_ostream<C, T>& operator<<(std::basic_ostream<C, T>& out, const Timestamp& d)
{
    out << "Timestamp(" << d.m_seconds << ", " << d.m_nanoseconds << ")";
    return out;
}

} // namespace realm

#endif // REALM_TIMESTAMP_HPP
