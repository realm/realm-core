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
#ifndef TIGHTDB_DATE_HPP
#define TIGHTDB_DATE_HPP

#include <ctime>
#include <ostream>

namespace tightdb {


class Date {
public:
    Date(std::time_t d) { set_date(d); }
    Date(size_t year, size_t month, size_t day, size_t hour = 0, size_t minute = 0, size_t second = 0) { set_date(year, month, day, hour, minute, second); }

    std::time_t get_date() const { return m_time; }

    bool set_date(time_t date) {
        m_time = date;
        return true;
    }

    std::time_t set_date(size_t year, size_t month, size_t day, size_t hour = 0, size_t minute = 0, size_t second = 0) {
        memset(&m_date, 0, sizeof(m_date));
        m_date.tm_year = (int)year - 1900;
        m_date.tm_mon = (int)month;
        m_date.tm_mday = (int)day;
        m_date.tm_hour = (int)hour;
        m_date.tm_min = (int)minute;
        m_date.tm_sec = (int)second;
        m_date.tm_isdst = 0;
#ifdef _MSC_VER
        m_time = _mkgmtime64(&m_date);  // fixme: verify that _mkgmtime64 interprets input time as UTC time zone. Verify how daylight saving behaves too
#else
        m_time = mktime (&m_date);
#endif
        return m_time;
    }

    bool operator==(const Date& d) const { return m_time == d.m_time; }
    bool operator!=(const Date& d) const { return m_time != d.m_time; }

    template<class Ch, class Tr>
    friend std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out, const Date& d)
    {
        out << "Date("<<d.m_time<<")";
        return out;
    }


private:
    std::time_t m_time;
    tm m_date;
};


} // namespace tightdb

#endif // TIGHTDB_DATE_HPP

