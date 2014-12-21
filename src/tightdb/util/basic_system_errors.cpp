#include <stdlib.h>
#include <string.h>

#include <tightdb/util/basic_system_errors.hpp>

using namespace std;
using namespace tightdb::util;


namespace {

class system_category: public error_category {
    const char* name() const TIGHTDB_OVERRIDE;
    string message(int) const TIGHTDB_OVERRIDE;
};

system_category g_system_category;

const char* system_category::name() const
{
    return "tightdb.basic_system";
}

string system_category::message(int value) const
{
#if defined _WIN32 // Windows version <stdlib.h>

    if (TIGHTDB_LIKELY(0 <= value || value < _sys_nerr))
        return _sys_errlist[value];

#elif _GNU_SOURCE && !TIGHTDB_ANDROID // GNU specific version <string.h>

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
    if (TIGHTDB_LIKELY(strerror_r(value, buffer, max_msg_size) == 0)) {
        buffer[max_msg_size] = 0; // For safety's sake
        return buffer;
    }

#endif

    return "Unknown error";
}

} // anonymous namespace


namespace tightdb {
namespace util {
namespace error {

error_code make_error_code(basic_system_errors err)
{
    return error_code(err, g_system_category);
}

} // namespace error
} // namespace util
} // namespace tightdb
