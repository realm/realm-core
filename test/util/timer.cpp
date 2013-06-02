#if defined _WIN32
#  define NOMINMAX
#  include <windows.h>
#elif defined __APPLE__
#  include <mach/mach_time.h>
#else
#  include <time.h>
#endif

#include "timer.hpp"

using namespace tightdb::test_util;


#ifdef _WIN32


uint_fast64_t Timer::get_timer_ticks()
{
    return GetTickCount();
}

double Timer::calc_elapsed_seconds(uint_fast64_t ticks)
{
    return ticks * 1E-3;
}


#elif defined __APPLE__


uint_fast64_t Timer::get_timer_ticks()
{
    return mach_absolute_time();
}

namespace {

struct TimeBase {
    double m_seconds_per_tick;
    TimeBase()
    {
        mach_timebase_info_data_t info;
        kern_return_t err = mach_timebase_info(&info);
        if (err) throw runtime_error("Failed to get absolute time base");
        m_seconds_per_tick = (1E-9 * info.numer) / info.denom;
    }
};

} // anonymous namespace

double Timer::calc_elapsed_seconds(uint_fast64_t ticks)
{
    static TimeBase base;
    return ticks * base.m_seconds_per_tick;
}


#else


namespace {

struct InitialTime {
    timespec m_ts;
    InitialTime()
    {
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &m_ts);
    }
};

} // anonymous namespace

uint_fast64_t Timer::get_timer_ticks()
{
    static const InitialTime init;
    timespec ts;
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
    if (ts.tv_nsec < init.m_ts.tv_nsec) {
        ts.tv_sec  -= 1;
        ts.tv_nsec += 1000000000;
    }
    return uint_fast64_t(ts.tv_sec - init.m_ts.tv_sec) * 1000000000 + (ts.tv_nsec - init.m_ts.tv_nsec);
}

double Timer::calc_elapsed_seconds(uint_fast64_t ticks)
{
    return ticks * 1E-9;
}


#endif
