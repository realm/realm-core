#include <realm/db.hpp>
#include <realm/sync/noinst/common_dir.hpp>
#include <realm/util/file.hpp>

using namespace realm;

void _impl::remove_realm_file(const std::string& realm_path)
{
    const auto& core_files = DB::get_core_files(realm_path);
    for (const auto& file_information : core_files) {
        const auto& is_folder = file_information.second.second;
        const auto& file_path = file_information.second.first;
        if (is_folder)
            util::try_remove_dir_recursive(file_path);
        else
            util::File::try_remove(file_path);
    }
}
