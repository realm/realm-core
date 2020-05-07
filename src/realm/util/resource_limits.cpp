#include <system_error>

#include <realm/util/assert.hpp>
#include <realm/util/resource_limits.hpp>
#include <realm/util/backtrace.hpp>

#ifndef _WIN32
#define REALM_HAVE_POSIX_RLIMIT 1
#endif

#if REALM_HAVE_POSIX_RLIMIT
#include <sys/resource.h>
#endif

using namespace realm;
using namespace realm::util;


#if REALM_HAVE_POSIX_RLIMIT

namespace {

int map_resource_ident(Resource resource) noexcept
{
    switch (resource) {
        case Resource::core_dump_size:
            return RLIMIT_CORE;
        case Resource::cpu_time:
            return RLIMIT_CPU;
        case Resource::data_segment_size:
            return RLIMIT_DATA;
        case Resource::file_size:
            return RLIMIT_FSIZE;
        case Resource::num_open_files:
            return RLIMIT_NOFILE;
        case Resource::stack_size:
            return RLIMIT_STACK;
        case Resource::virtual_memory_size:
            return RLIMIT_AS;
    }
    REALM_ASSERT(false);
    return 0;
}

long get_rlimit(Resource resource, bool hard)
{
    int resource_2 = map_resource_ident(resource);
    rlimit rlimit;
    int status = getrlimit(resource_2, &rlimit);
    if (status < 0)
        throw std::system_error(errno, std::system_category(), "getrlimit() failed");
    rlim_t value = (hard ? rlimit.rlim_max : rlimit.rlim_cur);
    return value == RLIM_INFINITY ? -1 : long(value);
}

void set_rlimit(Resource resource, long value, bool hard)
{
    int resource_2 = map_resource_ident(resource);
    rlimit rlimit;
    int status = getrlimit(resource_2, &rlimit);
    if (status < 0)
        throw std::system_error(errno, std::system_category(), "getrlimit() failed");
    rlim_t value_2 = (value < 0 ? RLIM_INFINITY : rlim_t(value));
    (hard ? rlimit.rlim_max : rlimit.rlim_cur) = value_2;
    status = setrlimit(resource_2, &rlimit);
    if (status < 0)
        throw std::system_error(errno, std::system_category(), "setrlimit() failed");
}

} // anonymous namespace

#endif // REALM_HAVE_POSIX_RLIMIT


#if REALM_HAVE_POSIX_RLIMIT

bool util::system_has_rlimit(Resource) noexcept
{
    return true;
}

long util::get_hard_rlimit(Resource resource)
{
    bool hard = true;
    return get_rlimit(resource, hard);
}

long util::get_soft_rlimit(Resource resource)
{
    bool hard = false;
    return get_rlimit(resource, hard);
}

void util::set_soft_rlimit(Resource resource, long value)
{
    bool hard = false;
    set_rlimit(resource, value, hard);
}

#else // ! REALM_HAVE_POSIX_RLIMIT

bool util::system_has_rlimit(Resource) noexcept
{
    return false;
}

long util::get_hard_rlimit(Resource)
{
    throw util::runtime_error("Not supported");
}

long util::get_soft_rlimit(Resource)
{
    throw util::runtime_error("Not supported");
}

void util::set_soft_rlimit(Resource, long)
{
    throw util::runtime_error("Not supported");
}

#endif // ! REALM_HAVE_POSIX_RLIMIT
