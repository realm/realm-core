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

namespace realm {

/*
 * IMPORTANT: The following two arrays must be kept updated
 * as new versions are released or if rollback is ever done.
*/

// Note: accepted versions should have new versions added at front
const std::vector<int> accepted_versions_{20, 11, 10, 9, 8, 7, 6, 0};
const std::vector<int> not_accepted_versions_{};

std::vector<int> accepted_versions{accepted_versions_};
std::vector<int> not_accepted_versions{not_accepted_versions_};

void fake_versions(const std::vector<int> &accepted,
                   const std::vector<int> &not_accepted) {
  accepted_versions = accepted;
  not_accepted_versions = not_accepted;
}

void unfake_versions() {
  accepted_versions = accepted_versions_;
  not_accepted_versions = not_accepted_versions_;
}

std::string get_prefix_from_path(std::string path) {
  // prefix is everything but the suffix here, so start from the back
  for (int i = path.size() - 1; i; --i) {
    if (path[i] == '.')
      return path.substr(0, i + 1);
  }
  // if not on normal "prefix.suffix" form add "."
  return path + ".";
}

std::string backup_name(std::string prefix, int version) {
  return prefix + "v" + std::to_string(version) + ".backup.realm";
}

bool backup_exists(std::string prefix, int version) {
  std::string fname = backup_name(prefix, version);
  return util::File::exists(fname);
}

bool must_restore_from_backup(std::string path,
                              int current_file_format_version) {
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

bool is_accepted_file_format(int version) {
  for (auto i : accepted_versions) {
    if (i == version)
      return true;
  }
  return false;
}

void restore_from_backup(std::string path) {
  std::string prefix = get_prefix_from_path(path);
  for (auto i : accepted_versions) {
    if (backup_exists(prefix, i)) {
      auto backup_nm = backup_name(prefix, i);
      std::cout << "Restoring from backup " << backup_nm << std::endl;
      util::File::move(backup_nm, path);
    }
  }
  for (auto i : not_accepted_versions) {
    if (backup_exists(prefix, i)) {
      util::File::remove(backup_name(prefix, i));
    }
  }
}

void backup_realm_if_needed(std::string path, int current_file_format_version,
                            int target_file_format_version) {
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
  std::cout << "Creating backup " << backup_nm << std::endl;
  std::string part_name = backup_nm + ".part";
  // FIXME: Consider if it's better to do a writeToFile a la compact?
  // FIXME: Handle running out of file space
  util::File::copy(path, part_name);
  util::File::move(part_name, backup_nm);
}

} // namespace realm
