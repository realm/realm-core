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
namespace realm {

class BackupHandler {
public:
    BackupHandler(const std::string& path);
    bool is_accepted_file_format(int current_file_format_version);
    bool must_restore_from_backup(int current_file_format_version);
    void restore_from_backup();
    void cleanup_backups();
    void backup_realm_if_needed(int current_file_format_version, int target_file_format_version);
    std::string get_prefix();

    // functions to mock version lists for testing purposes

    using version_list_t = std::initializer_list<int>;
    using version_time_list_t = std::initializer_list<std::pair<int, int>>;

    static void fake_versions(const version_list_t& accepted, const version_time_list_t& to_be_deleted);
    static void unfake_versions();
    static std::string get_prefix_from_path(const std::string& path);

private:
    std::string m_path;
    std::string m_prefix;

    static std::vector<int> s_accepted_versions;
    static std::vector<std::pair<int, int>> s_delete_versions;
};

} // namespace realm
