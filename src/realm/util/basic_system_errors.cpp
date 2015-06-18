#include <stdlib.h>
#include <string.h>
#include <string>

#include <realm/util/features.h>
#include <realm/util/basic_system_errors.hpp>

using namespace realm::util;


namespace {

class system_category: public std::error_category {
    const char* name() const REALM_NOEXCEPT override;
    std::string message(int) const override;
};

system_category g_system_category;

const char* system_category::name() const REALM_NOEXCEPT
{
    return "realm.basic_system";
}

std::string system_category::message(int value) const
{
#if defined _WIN32 // Windows version <stdlib.h>

    if (REALM_LIKELY(0 <= value || value < _sys_nerr))
        return _sys_errlist[value];

#elif _GNU_SOURCE && !REALM_ANDROID // GNU specific version <string.h>

    // Note that Linux provides the GNU specific version even though
    // it sets _POSIX_C_SOURCE >= 200112L.

    const size_t max_msg_size = 256;
    char buffer[max_msg_size+1];
    if (char* msg = strerror_r(value, buffer, max_msg_size)) {
        buffer[max_msg_size] = 0; // For safety's sake
        return msg;
    }

#else // POSIX.1-2001 fallback version <string.h>

    const size_t max_msg_size = 256;
    char buffer[max_msg_size+1];
    if (REALM_LIKELY(strerror_r(value, buffer, max_msg_size) == 0)) {
        buffer[max_msg_size] = 0; // For safety's sake
        return buffer;
    }

#endif

    return "Unknown error";
}

} // anonymous namespace


namespace realm {
namespace util {
namespace error {

std::error_code make_error_code(basic_system_errors err)
{
    return std::error_code(err, g_system_category);
}

} // namespace error
} // namespace util
} // namespace realm
