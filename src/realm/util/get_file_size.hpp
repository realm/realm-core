#ifndef REALM_UTIL_GET_FILE_SIZE_HPP
#define REALM_UTIL_GET_FILE_SIZE_HPP

#include <realm/util/file.hpp>

namespace realm {
namespace util {

/// FIXME: This function ought to be moved to <realm/util/file.hpp> in the
/// realm-core repository.
util::File::SizeType get_file_size(const std::string& path);


// Implementation

inline util::File::SizeType get_file_size(const std::string& path)
{
    util::File file{path};  // Throws
    return file.get_size(); // Throws
}

} // namespace util
} // namespace realm

#endif // REALM_UTIL_GET_FILE_SIZE_HPP
