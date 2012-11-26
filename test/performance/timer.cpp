#ifndef _MSC_VER
#  include <time.h>
#else
#  include <windows.h>
#endif

#include "timer.hpp"


namespace {

#ifndef _MSC_VER
struct InitialTime {
    timespec m_ts;
    InitialTime()
    {
        clock_gettime(CLOCK_MONOTONIC, &m_ts);
    }
};
#endif

} // anonymous namespace


namespace tightdb {

#ifndef _MSC_VER
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
