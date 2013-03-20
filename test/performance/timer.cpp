
#ifdef _MSC_VER
#  define NOMINMAX
#  include <windows.h>
#elif __APPLE__
#  include <mach/mach_time.h>
#else
#  include <time.h>
#endif

#include "timer.hpp"


namespace tightdb {

#ifdef _MSC_VER

long get_timer_millis()
{
    return GetTickCount();
}

#elif __APPLE__

long get_timer_millis()
{
    return mach_absolute_time();
}

#else

namespace {
struct InitialTime {
    timespec m_ts;
    InitialTime()
    {
        clock_gettime(CLOCK_MONOTONIC, &m_ts);
    }
};
} // anonymous namespace

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
#endif

#ifdef __APPLE__
double Timer::get_elapsed_millis() const
{
    const long endTime = get_timer_millis();
    const long difference = endTime - m_start;
    static double conversion = 0.0;

    if (conversion == 0.0) {
        mach_timebase_info_data_t info;
        kern_return_t err = mach_timebase_info( &info );

        // Convert the timebase into seconds
        if (err == 0)
            conversion = 1e-9 * (double)info.numer / (double)info.denom;
    }

    return conversion * (double)difference;
}
#else
double Timer::get_elapsed_millis() const
{
    return get_timer_millis() - m_start;
}
#endif

} // namespace tightdb