#include <stdexcept>
#include <iomanip>
#include <cmath>
#include <sstream>

#if defined _WIN32
#  define NOMINMAX
#  include <windows.h>
#elif defined __APPLE__
#  include <sys/resource.h>
#  include <mach/mach_time.h>
#  include <sys/time.h>
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

double Timer::calc_elapsed_seconds(uint_fast64_t ticks) const
{
    return ticks * 1E-3;
}


#elif defined __APPLE__


uint_fast64_t Timer::get_timer_ticks() const
{
    switch (m_type) {
        case type_RealTime:
            return mach_absolute_time();
        case type_UserTime: {
            rusage ru;
            getrusage(RUSAGE_SELF, &ru);
            timeval tv;
            timeradd(&ru.ru_utime, &ru.ru_stime, &tv);
            return tv.tv_sec * 1000000 + tv.tv_usec;
        }
    }
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

double Timer::calc_elapsed_seconds(uint_fast64_t ticks) const
{
    static TimeBase base;
    switch (m_type) {
        case type_RealTime:
            return ticks * base.m_seconds_per_tick;
        case type_UserTime:
            return static_cast<double>(ticks) / 1000000;
    }
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

double Timer::calc_elapsed_seconds(uint_fast64_t ticks) const
{
    return ticks * 1E-9;
}

#endif

std::string Timer::format(double seconds)
{
    std::ostringstream out;
    format(seconds, out);
    return out.str();
}

namespace {
    // FIXME: This should be std::llround once we switch to >= C++11.
    int64_t round_to_int64(double x)
    {
        // FIXME: Assumes x >= 0.
        // FIXME: The adding of 0.5 is error-prone, see: http://blog.frama-c.com/index.php?post/2013/05/02/nearbyintf1
        return static_cast<int64_t>(x + 0.5);
    }
}


void Timer::format(double seconds_float, std::ostream& out)
{
    int64_t rounded_minutes = round_to_int64(seconds_float / 60);
    if (rounded_minutes > 60) {
        // 1h0m -> inf
        int64_t hours   = rounded_minutes / 60;
        int64_t minutes = rounded_minutes % 60;
        out << hours << "h" << minutes << "m";
    }
    else {
        int64_t rounded_seconds = round_to_int64(seconds_float);
        if (rounded_seconds > 60) {
            // 1m0s -> 59m59s
            int64_t minutes = rounded_seconds / 60;
            int64_t seconds = rounded_seconds % 60;
            out << minutes << "m" << seconds << "s";
        }
        else {
            int64_t rounded_centies = round_to_int64(seconds_float * 100);
            if (rounded_centies > 100) {
                // 1s -> 59.99s
                int64_t seconds = rounded_centies / 100;
                int64_t centies = rounded_centies % 100;
                out << seconds;
                if (centies > 0) {
                    out << '.' << std::setw(2) << std::setfill('0') << centies;
                }
                out << 's';
            }
            else {
                int64_t rounded_centi_ms = round_to_int64(seconds_float * 100000);
                if (rounded_centi_ms > 100) {
                    // 0.1ms -> 999.99ms
                    int64_t ms = rounded_centi_ms / 100;
                    int64_t centi_ms = rounded_centi_ms % 100;
                    out << ms;
                    if (centi_ms > 0) {
                        out << '.' << std::setw(2) << std::setfill('0') << centi_ms;
                    }
                    out << "ms";
                }
                else {
                    // 0 -> 999µs
                    int64_t us = round_to_int64(seconds_float * 1000000);
                    out << us << "us";
                }
            }
        }
    }
}
