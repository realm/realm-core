#ifndef REALM_NOINST_COMMON_DIR_HPP
#define REALM_NOINST_COMMON_DIR_HPP

#include <string>

namespace realm {
namespace _impl {

/// Remove the Realm file at the specified path (real filesystem path),
/// including its associated files and directories.
void remove_realm_file(const std::string& real_path);

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_COMMON_DIR_HPP
