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

#include <string>
#include <vector>

#include <realm/util/logger.hpp>

namespace realm {

class BackupHandler {
public:
    using version_list_t = std::vector<int>;
    using version_time_list_t = std::vector<std::pair<int, int>>;

    BackupHandler(const std::string& path, const version_list_t& accepted, const version_time_list_t& to_be_deleted);
    bool is_accepted_file_format(int current_file_format_version);
    bool must_restore_from_backup(int current_file_format_version);
    void restore_from_backup(util::Logger& logger);
    void cleanup_backups(util::Logger& logger);
    void backup_realm_if_needed(int current_file_format_version, int target_file_format_version,
                                util::Logger& logger);
    std::string get_prefix();

    static std::string get_prefix_from_path(const std::string& path);
    // default lists of accepted versions and backups to delete when they get old enough
    static version_list_t accepted_versions_;
    static version_time_list_t delete_versions_;

private:
    std::string m_path;
    std::string m_prefix;

    version_list_t m_accepted_versions;
    version_time_list_t m_delete_versions;
};

} // namespace realm
