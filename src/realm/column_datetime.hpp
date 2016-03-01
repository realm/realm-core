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
    NewDate(int64_t seconds, int32_t nanoseconds) : m_seconds(seconds), m_nanoseconds(nanoseconds) { }
    NewDate() { }

    bool is_null() const { return m_seconds == 0 && m_seconds == 0; }
    bool operator == (const NewDate& rhs) const { return m_seconds == rhs.m_seconds && m_nanoseconds == rhs.m_nanoseconds; }
    bool operator != (const NewDate& rhs) const { return m_seconds != rhs.m_seconds || m_nanoseconds != rhs.m_nanoseconds; }
    bool operator > (const NewDate& rhs) const { return (m_seconds > rhs.m_seconds) || (m_seconds == rhs.m_seconds && m_seconds > rhs.m_nanoseconds); }
    bool operator < (const NewDate& rhs) const { return (m_seconds < rhs.m_seconds) || (m_seconds == rhs.m_seconds && m_seconds < rhs.m_nanoseconds); }
    NewDate& operator = (const NewDate& rhs) = default;

    int64_t m_seconds = 0;
    int32_t m_nanoseconds = 0;
};

class DateTimeColumn : public ColumnBase {
public:
    /// Get the number of entries in this column. This operation is relatively
    /// slow.
    size_t size() const noexcept override {
        // FIXME: Consider debug asserts on the columns having the same size
        return m_seconds.size();
    }

    /// Whether or not this column is nullable.
    bool is_nullable() const noexcept override {
        return m_seconds.is_nullable();
    }

    /// Whether or not the value at \a row_ndx is NULL. If the column is not
    /// nullable, always returns false.
    bool is_null(size_t row_ndx) const noexcept override {
        return m_seconds.is_null(row_ndx);
    }

    /// Sets the value at \a row_ndx to be NULL.
    /// \throw LogicError Thrown if this column is not nullable.
    void set_null(size_t row_ndx) override {
        m_seconds.set_null(row_ndx);
    }

private:
    IntNullColumn m_seconds;
    IntegerColumn m_nanoseconds;
};

} // namespace realm

#endif // REALM_COLUMN_DATETIME_HPP
