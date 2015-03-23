#include "tightdb/util/errno.hpp"

#include <tightdb/util/string_buffer.hpp>

#include <cerrno>
#include <cstdlib>
#include <cstring>

namespace tightdb {
namespace util {

std::string get_errno_msg(const char* prefix, int err)
{
    StringBuffer buffer;
    buffer.append_c_str(prefix);

#if defined _WIN32 // Windows version <stdlib.h>

    if (REALM_LIKELY(0 <= err || err < _sys_nerr)) {
        buffer.append_c_str(_sys_errlist[err]);
        return buffer.str();
    }

#elif _GNU_SOURCE && !REALM_ANDROID // GNU specific version <string.h>

    // Note that Linux provides the GNU specific version even though
    // it sets _POSIX_C_SOURCE >= 200112L.

    size_t offset = buffer.size();
    size_t max_msg_size = 256;
    buffer.resize(offset + max_msg_size);
    if (char* msg = strerror_r(err, buffer.data()+offset, max_msg_size))
        return msg;
    buffer.resize(offset);

#else // POSIX.1-2001 fallback version <string.h>

    size_t offset = buffer.size();
    size_t max_msg_size = 256;
    buffer.resize(offset + max_msg_size);
    if (REALM_LIKELY(strerror_r(err, buffer.data()+offset, max_msg_size) == 0))
        return buffer.str();
    buffer.resize(offset);

#endif

    buffer.append_c_str("Unknown error");
    return buffer.str();
}

}
}
