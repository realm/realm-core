#ifndef REALM_UTIL_PARENT_DIR_HPP
#define REALM_UTIL_PARENT_DIR_HPP

#include <string>

namespace realm {
namespace util {

/// Same effect as std::filesystem::path::parent_path().
///
/// FIXME: This function ought to be moved to <realm/util/file.hpp> in the
/// realm-core repository.
std::string parent_dir(const std::string& path);

} // namespace util
} // namespace realm

#endif // REALM_UTIL_PARENT_DIR_HPP
