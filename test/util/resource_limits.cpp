#include <stdexcept>

#include <realm/util/assert.hpp>

#include "resource_limits.hpp"

#ifndef _WIN32
#  define REALM_HAVE_POSIX_RLIMIT 1
#endif

#if REALM_HAVE_POSIX_RLIMIT
#  include <sys/resource.h>
#endif

using namespace realm;
using namespace realm::test_util;

#if REALM_HAVE_POSIX_RLIMIT

namespace {

long get_rlimit(Resource resource, bool hard)
{
    int resource_2 = -1;
    switch (resource) {
        case resource_NumOpenFiles:
            resource_2 = RLIMIT_NOFILE;
            break;
    }
    REALM_ASSERT(resource_2 != -1);
    rlimit rlimit;
    int status = getrlimit(resource_2, &rlimit);
    if (status < 0)
        throw std::runtime_error("getrlimit() failed");
    rlim_t value = hard ? rlimit.rlim_max : rlimit.rlim_cur;
    return value == RLIM_INFINITY ? -1 : long(value);
}

void set_rlimit(Resource resource, long value, bool hard)
{
    int resource_2 = -1;
    switch (resource) {
        case resource_NumOpenFiles:
            resource_2 = RLIMIT_NOFILE;
            break;
    }
    REALM_ASSERT(resource_2 != -1);
    rlimit rlimit;
    int status = getrlimit(resource_2, &rlimit);
    if (status < 0)
        throw std::runtime_error("getrlimit() failed");
    rlim_t value_2 = value < 0 ? RLIM_INFINITY : rlim_t(value);
    (hard ? rlimit.rlim_max : rlimit.rlim_cur) = value_2;
    status = setrlimit(resource_2, &rlimit);
    if (status < 0)
        throw std::runtime_error("setrlimit() failed");
}

} // anonymous namespace

#endif // REALM_HAVE_POSIX_RLIMIT


namespace realm {
namespace test_util {

#if REALM_HAVE_POSIX_RLIMIT

bool system_has_rlimit(Resource) REALM_NOEXCEPT
{
    return true;
}

long get_hard_rlimit(Resource resource)
{
    bool hard = true;
    return get_rlimit(resource, hard);
}

long get_soft_rlimit(Resource resource)
{
    bool hard = false;
    return get_rlimit(resource, hard);
}

void set_soft_rlimit(Resource resource, long value)
{
    bool hard = false;
    set_rlimit(resource, value, hard);
}

#else // ! REALM_HAVE_POSIX_RLIMIT

bool system_has_rlimit(Resource) REALM_NOEXCEPT
{
    return false;
}

long get_hard_rlimit(Resource)
{
    throw std::runtime_error("Not supported");
}

long get_soft_rlimit(Resource)
{
    throw std::runtime_error("Not supported");
}

void set_soft_rlimit(Resource, long)
{
    throw std::runtime_error("Not supported");
}

#endif // ! REALM_HAVE_POSIX_RLIMIT


} // namespace test_util
} // namespace realm

