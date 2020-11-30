#ifndef REALM_UTIL_FILE_IS_REGULAR_HPP
#define REALM_UTIL_FILE_IS_REGULAR_HPP

#include <string>

namespace realm {
namespace util {

/// Check whether the specified file is a reguler file.
///
/// FIXME: This function ought to be moved to <realm/util/file.hpp> in the
/// realm-core repository.
bool file_is_regular(const std::string& path);

} // namespace util
} // namespace realm

#endif // REALM_UTIL_FILE_IS_REGULAR_HPP
