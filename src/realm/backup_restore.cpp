/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/backup_restore.hpp>
#include <realm/util/file.hpp>

#include <vector>
#include <filesystem>
#include <chrono>

namespace realm {

/*
 * IMPORTANT: The following two arrays must be kept updated
 * as new versions are released or if rollback is ever done.
 */


// Note: accepted versions should have new versions added at front
const version_list_t accepted_versions_{20, 11, 10, 9, 8, 7, 6, 0};

// the pair is <version, age-in-seconds>
// we keep backup files in 3 months: 3*30*24*60*60 secs
constexpr int three_months = 3*30*24*60*60;
const version_time_list_t delete_versions_{
    {20, three_months},
    {11, three_months},
    {10, three_months},
    {9, three_months},
    {8, three_months},
    {7, three_months},
    {6, three_months}
};

version_list_t accepted_versions{accepted_versions_};
version_time_list_t delete_versions{delete_versions_};

void fake_versions(const version_list_t& accepted, const version_time_list_t& not_accepted)
{
    accepted_versions = accepted;
    delete_versions = not_accepted;
}

void unfake_versions()
{
    accepted_versions = accepted_versions_;
    delete_versions = delete_versions_;
}

std::string get_prefix_from_path(std::string path)
{
    // prefix is everything but the suffix here, so start from the back
    for (int i = path.size() - 1; i; --i) {
        if (path[i] == '.')
            return path.substr(0, i + 1);
    }
    // if not on normal "prefix.suffix" form add "."
    return path + ".";
}

std::string backup_name(std::string prefix, int version)
{
    return prefix + "v" + std::to_string(version) + ".backup.realm";
}

bool backup_exists(std::string prefix, int version)
{
    std::string fname = backup_name(prefix, version);
    return util::File::exists(fname);
}

bool must_restore_from_backup(std::string path, int current_file_format_version)
{
    if (current_file_format_version == 0)
        return false;
    std::string prefix = get_prefix_from_path(path);
    for (auto i : accepted_versions) {
        if (i == current_file_format_version)
            return false;
        if (backup_exists(prefix, i))
            return true;
    }
    return false;
}

bool is_accepted_file_format(int version)
{
    for (auto i : accepted_versions) {
        if (i == version)
            return true;
    }
    return false;
}

void restore_from_backup(std::string path)
{
    std::string prefix = get_prefix_from_path(path);
    for (auto i : accepted_versions) {
        if (backup_exists(prefix, i)) {
            auto backup_nm = backup_name(prefix, i);
            std::cout << "Restoring from:    " << backup_nm << std::endl;
            util::File::move(backup_nm, path);
        }
    }
}

void cleanup_backups(std::string path)
{
    std::string prefix = get_prefix_from_path(path);
    auto now = time(nullptr);
    for (auto i : delete_versions) {
        try {
            if (backup_exists(prefix, i.first)) {
                std::string fn = backup_name(prefix, i.first);
                // Assuming time_t is in seconds (should be on posix, but...)
                auto last_modified = util::File::last_write_time(fn);
                double diff = difftime(now, last_modified);
                if (diff > i.second) {
                    std::cout << "Removing backup:   " << fn << "  - age: " << diff << std::endl;
                    util::File::remove(fn);
                }
            }
        }
        catch (...) // ignore any problems, just leave the files
        {
        }
    }
}

void backup_realm_if_needed(std::string path, int current_file_format_version, int target_file_format_version)
{
    if (current_file_format_version == 0)
        return;
    // FIXME ^^ Is this correct??
    if (current_file_format_version >= target_file_format_version)
        return;
    std::string prefix = get_prefix_from_path(path);
    std::string backup_nm = backup_name(prefix, current_file_format_version);
    if (util::File::exists(backup_nm)) {
        std::cout << "Backup file already exists: " << backup_nm << std::endl;
        return;
    }
    std::cout << "Creating backup:   " << backup_nm << std::endl;
    std::string part_name = backup_nm + ".part";
    // FIXME: Consider if it's better to do a writeToFile a la compact?
    // FIXME: Handle running out of file space
    util::File::copy(path, part_name);
    util::File::move(part_name, backup_nm);
}

} // namespace realm
