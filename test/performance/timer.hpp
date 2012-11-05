#ifndef TIGHTDB_TIMER_HPP
#define TIGHTDB_TIMER_HPP

#ifndef _MSC_VER
#  include <time.h>
#else
#  include <windows.h>
#endif

namespace tightdb {

/// Get the number of milliseconds since the system was started (or
/// since some other arbitrary point in time after the system was
/// started). The timer is guaranteed to increase without overflow for
/// at least 24.8 days after the system was started (corresponding to
/// a 31-bit representation).
long get_timer_millis();


#ifndef _MSC_VER
struct InitialTime {
    timespec m_ts;
    InitialTime()
    {
        clock_gettime(CLOCK_MONOTONIC, &m_ts);
    }
};
long get_timer_millis()
{
    static const InitialTime init;
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    if (ts.tv_nsec < init.m_ts.tv_nsec) {
        ts.tv_sec  -= 1;
        ts.tv_nsec += 1000000000L;
    }
    return long(ts.tv_sec - init.m_ts.tv_sec) * 1000 + (ts.tv_nsec - init.m_ts.tv_nsec) / 1000000L;
}
#else
long get_timer_millis()
{
    return GetTickCount();
}
#endif

} // namespace tightdb

#endif // TIGHTDB_TIMER_HPP
