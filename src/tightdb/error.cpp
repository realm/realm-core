#include <tightdb/error.hpp>

using namespace std;

namespace tightdb {


string get_message(error_code err)
{
    switch (err) {
    case ERROR_NONE:             return "No error";
    case ERROR_INVALID_ARG:      return "Invalid argument";
    case ERROR_NO_SUCH_FILE:     return "No such file or directory";
    case ERROR_BAD_FILESYS_PATH: return "Bad file system path";
    case ERROR_PERMISSION:       return "Permission denied";
    case ERROR_OUT_OF_MEMORY:    return "Out of memory";
    case ERROR_NO_RESOURCE:      return "Not enough resources";
    case ERROR_IO:               return "Input/output error";
    case ERROR_INTERRUPTED:      return "Interrupted blocking operation";
    case ERROR_OTHER:            break;
    }
    return "Other/unknown error";
}


} // namespace tightdb
