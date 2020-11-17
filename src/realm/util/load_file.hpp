#ifndef REALM_UTIL_LOAD_FILE_HPP
#define REALM_UTIL_LOAD_FILE_HPP

#include <string>

namespace realm {
namespace util {

// FIXME: These functions ought to be moved to <realm/util/file.hpp> in the
// realm-core repository.
std::string load_file(const std::string& path);
std::string load_file_and_chomp(const std::string& path);

} // namespace util
} // namespace realm

#endif // REALM_UTIL_LOAD_FILE_HPP
