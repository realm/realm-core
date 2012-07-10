#include <cerrno>
#include <cstdlib>

#include <tightdb/file.hpp>

using namespace std;

namespace tightdb {


error_code create_temp_dir(StringBuffer& buffer)
{
#if _POSIX_C_SOURCE >= 200809L

    buffer.clear();
    // FIXME: Can we rely on /tmp/ to always be available when _POSIX_C_SOURCE >= 200809L?
    error_code err = buffer.append_c_str("/tmp/tightdb_XXXXXX");
    if (err) return err;
    if (mkdtemp(buffer.c_str()) == 0) {
        switch (errno) {
        case EEXIST:
        case ENOMEM:
        case ENOSPC: return ERROR_NO_RESOURCE;
        default:     return ERROR_OTHER;
        }
    }
    return ERROR_NONE;

#else // _POSIX_C_SOURCE < 200809L

    static_cast<void>(buffer);
    return ERROR_NOT_IMPLEMENTED;

#endif // _POSIX_C_SOURCE < 200809L
}


} // namespace tightdb
