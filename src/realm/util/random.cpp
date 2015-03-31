#include <ctime>

#ifdef _WIN32
#  define NOMINMAX
#  include <windows.h>
#else
#  include <unistd.h>
#endif

#include "realm/util/random.hpp"

namespace realm {
namespace util {

unsigned long produce_nondeterministic_random_seed()
{
    typedef unsigned long ulong;
    ulong value = ulong(time(0));

#ifdef _WIN32
    value ^= ulong(GetCurrentProcessId());
#else
    value ^= ulong(getpid());
#endif

    return value;
}

} // namespace util

namespace _impl {

GlobalRandom& GlobalRandom::get() REALM_NOEXCEPT
{
    // FIXME: Initialization of local statics are not guaranteed to be
    // thread safe.
    static GlobalRandom r;
    return r;
}

} // namespace _impl

} // namespace realm
