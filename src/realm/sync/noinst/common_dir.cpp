#include <realm/db.hpp>
#include <realm/sync/noinst/common_dir.hpp>
#include <realm/util/file.hpp>

using namespace realm;

void _impl::remove_realm_file(const std::string& real_path)
{
    const auto& core_files = DB::get_core_files(real_path, DB::CoreFileType::All);
    for (const auto& pair : core_files) {
        if (pair.second)
            util::try_remove_dir_recursive(pair.first);
        else
            util::File::try_remove(pair.first);
    }
}
