#ifndef REALM_NOINST_SERVER_DIR_HPP
#define REALM_NOINST_SERVER_DIR_HPP

#include <string>
#include <set>
#include <random>
#include <stdexcept>

#include <realm/util/backtrace.hpp>
#include <realm/util/file.hpp>
#include <realm/util/functional.hpp>
#include <realm/util/optional.hpp>
#include <realm/string_data.hpp>
#include <realm/binary_data.hpp>


namespace realm {
namespace _impl {

struct VirtualPathComponents {
    bool is_valid = false;
    std::string real_realm_path;
    bool is_partial_view = false;
    std::string reference_path;
    std::string user_identity;
};

// parse_virtual_path() validates and parses a virtual path. The format of a
// virtual path, also called a server path, is described in doc/protocol.md.
//
// The return value is a VirtualPathComponents struct. If the member is_valid
// is false, no other members must be used. reference_path and user_identity
// only make sense if is_partial_view is true.
//
// The argument root_path can be any valid path and is only used as a base
// directory for realm_realm_path.
VirtualPathComponents parse_virtual_path(const std::string& root_path, const std::string& virt_path);


// If virt_path is valid, the return value is the local Realm path corresponding to the
// virtual path. If the virtual path is invalid, the return value is util::none.
bool map_virt_to_real_realm_path(const std::string& root_path, const std::string& virt_path, std::string& real_path);

std::string map_virt_to_real_realm_path(const std::string& root_path, const std::string& virt_path);

bool map_partial_to_reference_virt_path(const std::string& partial_path, std::string& reference_path);

std::string map_partial_to_reference_virt_path(const std::string& partial_path);

void make_dirs(const std::string& root_path, const std::string& virt_path);

/// Returns the set of virtual paths corresponding to the Realm files found
/// under the specified root directory.
std::set<std::string> find_realm_files(const std::string& root_dir);

/// Invoke the specified handler for each Realm file found under the specified
/// root directory. The handler will be invoked by an explression on the form
/// `handler(std::move(real_path), std::move(virt_path))`. Both `real_path` and
/// `virt_path` are of type `std::string`. The first argument is the real path
/// of the Realm file, which is an extension of the specified root directory
/// path. The second argument is the virtual path of the Realm file relative to
/// the specified root directory.
template <class H>
void find_realm_files(const std::string& root_dir, H handler);


// Implementation

inline std::string map_virt_to_real_realm_path(const std::string& root_path, const std::string& virt_path)
{
    std::string real_path;
    if (map_virt_to_real_realm_path(root_path, virt_path, real_path))
        return real_path;
    throw util::runtime_error("Bad virtual path");
}

inline std::string map_partial_to_reference_virt_path(const std::string& partial_path)
{
    std::string reference_path;
    if (map_partial_to_reference_virt_path(partial_path, reference_path))
        return reference_path;
    throw util::runtime_error("Not a virtual path of a partial file");
}

inline std::set<std::string> find_realm_files(const std::string& root_dir)
{
    std::set<std::string> virt_paths;
    auto handler = [&virt_paths](std::string, std::string virt_path) {
        virt_paths.insert(std::move(virt_path)); // Throws
    };
    find_realm_files(root_dir, handler); // Throws
    return virt_paths;
}

template <class H>
void find_realm_files(const std::string& root_dir, H handler)
{
    StringData realm_suffix = ".realm";
    util::UniqueFunction<void(const std::string&, const std::string&)> scan_dir;
    scan_dir = [&](const std::string& real_path, const std::string& virt_path) {
        util::DirScanner ds{real_path}; // Throws
        std::string name;
        while (ds.next(name)) {                                              // Throws
            std::string real_subpath = util::File::resolve(name, real_path); // Throws
            if (util::File::is_dir(real_subpath)) {                          // Throws
                if (StringData{name}.ends_with(realm_suffix))
                    throw std::runtime_error("Illegal directory path: " + real_subpath);
                std::string virt_subpath = virt_path + "/" + name; // Throws
                scan_dir(real_subpath, virt_subpath);              // Throws
            }
            else if (StringData{name}.ends_with(realm_suffix)) {
                std::string base_name = name.substr(0, name.size() - realm_suffix.size()); // Throws
                std::string virt_subpath = virt_path + "/" + base_name;                    // Throws
                handler(std::move(real_subpath), std::move(virt_subpath));                 // Throws
            }
        }
        return true;
    };
    scan_dir(root_dir, ""); // Throws
}

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_SERVER_DIR_HPP
