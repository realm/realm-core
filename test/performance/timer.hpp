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
#ifndef TIGHTDB_TIMER_HPP
#define TIGHTDB_TIMER_HPP

namespace tightdb {


/// Get the number of milliseconds since the system was started (or
/// since some other arbitrary point in time after the system was
/// started). The timer is guaranteed to increase without overflow for
/// at least 24.8 days after the system was started (corresponding to
/// a 31-bit representation).
long get_timer_millis();

class Timer {
public:
    void start() { m_start = get_timer_millis(); }
    double get_elapsed_millis() const;

private:
    long m_start;
};


} // namespace tightdb

#endif // TIGHTDB_TIMER_HPP
