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

bool is_accepted_file_format(int current_file_format_version);
bool must_restore_from_backup(std::string path, int current_file_format_version);
void restore_from_backup(std::string path);
void cleanup_backups(std::string path);
void backup_realm_if_needed(std::string path, int current_file_format_version, int target_file_format_version);
std::string get_prefix_from_path(std::string path);

// functions to mock version lists for testing purposes

using version_list_t = std::vector<int>;
using version_time_list_t = std::vector<std::pair<int, int>>;

void fake_versions(const version_list_t& accepted, const version_time_list_t& to_be_deleted);
void unfake_versions();

} // namespace realm
