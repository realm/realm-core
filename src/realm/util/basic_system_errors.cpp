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
    const size_t max_msg_size = 256;
    char buffer[max_msg_size+1];

#ifdef _WIN32 // Windows version

    if (REALM_LIKELY(strerror_s(buffer, max_msg_size, value) == 0)) {
        return buffer; // Guaranteed to be truncated
    }

#elif defined __APPLE__ && defined __MACH__ // OSX, iOS and WatchOS version

    {
        int result = strerror_r(value, buffer, max_msg_size);
        if (REALM_LIKELY(result == 0 || result == ERANGE)) {
            return buffer; // Guaranteed to be truncated
        }
    }

#elif (_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600) && ! _GNU_SOURCE // XSI-compliant version

    if (REALM_LIKELY(strerror_r(value, buffer, max_msg_size) == 0)) {
        buffer[max_msg_size] = 0; // For safety's sake, not guaranteed to be truncated by POSIX
        return buffer;
    }

#else // GNU specific version

    {
        char* msg = nullptr;
        if (REALM_LIKELY((msg = strerror_r(value, buffer, max_msg_size)) != nullptr)) {
            return msg; // Guaranteed to be truncated
        }
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
