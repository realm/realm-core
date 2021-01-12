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
#include <chrono>

namespace realm {

/*
 * IMPORTANT: The following two arrays must be kept updated
 * as new versions are released or if rollback is ever done.
 */

using version_list_t = BackupHandler::version_list_t;
using version_time_list_t = BackupHandler::version_time_list_t;

// Note: accepted versions should have new versions added at front
version_list_t BackupHandler::accepted_versions_ = {20, 11, 10, 9, 8, 7, 6, 0};

// the pair is <version, age-in-seconds>
// we keep backup files in 3 months.
static constexpr int three_months = 3 * 31 * 24 * 60 * 60;
version_time_list_t BackupHandler::delete_versions_{{20, three_months}, {11, three_months}, {10, three_months},
                                                    {9, three_months},  {8, three_months},  {7, three_months},
                                                    {6, three_months}};


// helper functions
std::string backup_name(std::string prefix, int version)
{
    return prefix + "v" + std::to_string(version) + ".backup.realm";
}

bool backup_exists(std::string prefix, int version)
{
    std::string fname = backup_name(prefix, version);
    return util::File::exists(fname);
}

std::string BackupHandler::get_prefix_from_path(const std::string& path)
{
    auto size = path.size();

    // remove a suffix ".realm" but add back the "."
    if (size > 6 && path.substr(size - 6, 6) == ".realm") {
        return path.substr(0, size - 5); // include '.'
    }

    // if no ".realm" suffix, at least ensure a terminating "."
    if (path[size - 1] == '.') {
        return path;
    }
    return path + ".";
}

BackupHandler::BackupHandler(const std::string& path, const version_list_t& accepted,
                             const version_time_list_t& to_be_deleted)
{
    m_path = path;
    m_prefix = get_prefix_from_path(path);
    m_accepted_versions = accepted;
    m_delete_versions = to_be_deleted;
}

bool BackupHandler::must_restore_from_backup(int current_file_format_version)
{
    if (current_file_format_version == 0)
        return false;
    if (is_accepted_file_format(current_file_format_version))
        return false;
    for (auto v : m_accepted_versions) {
        if (backup_exists(m_prefix, v))
            return true;
    }
    return false;
}

bool BackupHandler::is_accepted_file_format(int version)
{
    for (auto v : m_accepted_versions) {
        if (v == version)
            return true;
    }
    return false;
}

void BackupHandler::restore_from_backup(util::Logger& logger)
{
    for (auto v : m_accepted_versions) {
        if (backup_exists(m_prefix, v)) {
            auto backup_nm = backup_name(m_prefix, v);
            logger.info("Restoring from backup: %s", backup_nm);
            std::cout << "Restoring from:    " << backup_nm << std::endl;
            util::File::move(backup_nm, m_path);
            return;
        }
    }
}

void BackupHandler::cleanup_backups()
{
    auto now = time(nullptr);
    for (auto v : m_delete_versions) {
        try {
            if (backup_exists(m_prefix, v.first)) {
                std::string fn = backup_name(m_prefix, v.first);
                // Assuming time_t is in seconds (should be on posix, but...)
                auto last_modified = util::File::last_write_time(fn);
                double diff = difftime(now, last_modified);
                if (diff > v.second) {
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

void BackupHandler::backup_realm_if_needed(int current_file_format_version, int target_file_format_version,
                                           util::Logger& logger)
{
    if (current_file_format_version == 0)
        return;
    if (current_file_format_version >= target_file_format_version)
        return;
    std::string backup_nm = backup_name(m_prefix, current_file_format_version);
    if (util::File::exists(backup_nm)) {
        std::cout << "Backup file already exists: " << backup_nm << std::endl;
        return;
    }
    try {
        // ignore it, if attempt to get free space fails for any reason
        if (util::File::get_free_space(m_path) < util::File::get_size_static(m_path) * 2) {
            logger.error("Insufficient free space for backup: %s", backup_nm);
            std::cout << "Insufficient free space for backup: " << backup_nm << std::endl;
            return;
        }
    }
    catch (...) {
        // ignore error
    }
    logger.info("Creating backup: %s", backup_nm);
    std::cout << "Creating backup:   " << backup_nm << std::endl;
    std::string part_name = backup_nm + ".part";
    // The backup file should be a 1-1 copy, so that we can get the original
    // contents including unchanged layout of data, freelists, etc
    // In doing so we forego the option of compacting the backup.
    // Silence any errors during the backup process, but should one occur
    // remove any backup files, since they cannot be trusted.
    try {
        util::File::copy(m_path, part_name);
        util::File::move(part_name, backup_nm);
    }
    catch (...) {
        try {
            util::File::try_remove(part_name);
            util::File::try_remove(backup_nm);
        }
        catch (...) {
        }
    }
}

} // namespace realm
