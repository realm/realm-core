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
#ifndef REALM_COLUMN_DATETIME_HPP
#define REALM_COLUMN_DATETIME_HPP

#include <realm/column.hpp>

namespace realm {

struct NewDate
{
    NewDate(int64_t seconds, int64_t nanoseconds) : m_seconds(seconds), m_nanoseconds(nanoseconds) { }
    NewDate() { }

    bool is_null() const { return m_seconds == 0 && m_seconds == 0; }
    bool operator == (const NewDate& rhs) const { return m_seconds == rhs.m_seconds && m_nanoseconds == rhs.m_nanoseconds; }
    bool operator != (const NewDate& rhs) const { return m_seconds != rhs.m_seconds || m_nanoseconds != rhs.m_nanoseconds; }
    bool operator > (const NewDate& rhs) const { return (m_seconds > rhs.m_seconds) || (m_seconds == rhs.m_seconds && m_seconds > rhs.m_nanoseconds); }
    bool operator < (const NewDate& rhs) const { return (m_seconds < rhs.m_seconds) || (m_seconds == rhs.m_seconds && m_seconds < rhs.m_nanoseconds); }
    NewDate& operator = (const NewDate& rhs) = default;

    int64_t m_seconds = 0;
    int64_t m_nanoseconds = 0;
};

class DateTimeColumn {
public:
};

} // namespace realm

#endif // REALM_COLUMN_DATETIME_HPP
