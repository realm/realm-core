#include <realm/util/file.hpp>
#include <realm/util/file_is_regular.hpp>
#include <realm/util/copy_dir_recursive.hpp>

using namespace realm;


void util::copy_dir_recursive(const std::string& origin_path, const std::string& target_path, bool skip_special_files)
{
    // FIXME: This function ought to be implemented in a nonrecursive manner in
    // order to maximally reduce the risk of memory exhaustion due to stack size
    // growth.
    try_make_dir(target_path); // Throws
    bool allow_missing = false;
    DirScanner ds{origin_path, allow_missing}; // Throws
    std::string name;
    while (ds.next(name)) {                                                         // Throws
        std::string origin_subpath = File::resolve(name, origin_path);              // Throws
        std::string target_subpath = File::resolve(name, target_path);              // Throws
        if (File::is_dir(origin_subpath)) {                                         // Throws
            copy_dir_recursive(origin_subpath, target_subpath, skip_special_files); // Throws
        }
        else if (file_is_regular(origin_subpath)) {
            File::copy(origin_subpath, target_subpath); // Throws
        }
        else if (!skip_special_files) {
            throw util::File::AccessError("Cannot copy special file", origin_subpath);
        }
    }
}
