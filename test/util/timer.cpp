#include <stdexcept>

#if defined _WIN32
#  define NOMINMAX
#  include <windows.h>
#elif defined __APPLE__
#  include <mach/mach_time.h>
#else
#  include <time.h>
#endif

#include "timer.hpp"

using namespace std;
using namespace tightdb::test_util;


#ifdef _WIN32


uint_fast64_t Timer::get_timer_ticks() const
{
    return GetTickCount();
}

double Timer::calc_elapsed_seconds(uint_fast64_t ticks)
{
    return ticks * 1E-3;
}


#elif defined __APPLE__


uint_fast64_t Timer::get_timer_ticks() const
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

#ifdef CLOCK_MONOTONIC_RAW
const clockid_t real_time_clock_id = CLOCK_MONOTONIC_RAW; // (since Linux 2.6.28; Linux-specific)
#else
const clockid_t real_time_clock_id = CLOCK_MONOTONIC;
#endif

const clockid_t user_time_clock_id = CLOCK_PROCESS_CPUTIME_ID;


struct InitialTimes {
    timespec m_real, m_user;
    InitialTimes()
    {
        clock_gettime(real_time_clock_id, &m_real);
        clock_gettime(user_time_clock_id, &m_user);
    }
};

} // anonymous namespace

uint_fast64_t Timer::get_timer_ticks() const
{
    static const InitialTimes init_times;
    clockid_t clock_id = clockid_t();
    const timespec* init_time = 0;
    switch (m_type) {
        case type_RealTime:
            clock_id = real_time_clock_id;
            init_time = &init_times.m_real;
            break;
        case type_UserTime:
            clock_id = user_time_clock_id;
            init_time = &init_times.m_user;
            break;
    }
    timespec time;
    clock_gettime(clock_id, &time);
    if (time.tv_nsec < init_time->tv_nsec) {
        time.tv_sec  -= 1;
        time.tv_nsec += 1000000000;
    }
    return uint_fast64_t(time.tv_sec - init_time->tv_sec) *
        1000000000 + (time.tv_nsec - init_time->tv_nsec);
}

double Timer::calc_elapsed_seconds(uint_fast64_t ticks)
{
    return ticks * 1E-9;
}


#endif
