#include <realm/db.hpp>
#include <realm/sync/noinst/common_dir.hpp>
#include <realm/util/file.hpp>

using namespace realm;

void _impl::remove_realm_file(const std::string& real_path)
{
    std::vector<std::pair<std::string, bool>> core_files = DB::get_core_files(real_path);
    for (std::pair<std::string, bool>& pair : core_files) {
        if (pair.second)
            util::try_remove_dir_recursive(pair.first);
        else
            util::File::try_remove(pair.first);
    }

    std::string lock_path = real_path + ".lock";
    util::File::try_remove(lock_path);
}
