#include <time.h>
#include <stdexcept>
#include <iomanip>
#include <chrono>

#include <realm/util/features.h>
#include <realm/util/time.hpp>
#include <realm/util/backtrace.hpp>

namespace realm {
namespace util {

std::tm localtime(std::time_t time)
{
#ifdef _WIN32
    std::tm tm;
    if (localtime_s(&tm, &time) != 0)
        throw util::invalid_argument("localtime_s() failed");
    return tm;
#else
    // Assuming POSIX.1-2008 for now.
    std::tm tm;
    if (!localtime_r(&time, &tm))
        throw util::invalid_argument("localtime_r() failed");
    return tm;
#endif
}

std::tm gmtime(std::time_t time)
{
#ifdef _WIN32
    std::tm tm;
    if (REALM_UNLIKELY(gmtime_s(&tm, &time) != 0))
        throw util::invalid_argument("gmtime_s() failed");
    return tm;
#else
    // Assuming POSIX.1-2008 for now.
    std::tm tm;
    if (!gmtime_r(&time, &tm))
        throw util::invalid_argument("gmtime_r() failed");
    return tm;
#endif
}

} // namespace util
} // namespace realm
