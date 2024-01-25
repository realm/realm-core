////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_OS_APP_BACKING_STORE_HPP
#define REALM_OS_APP_BACKING_STORE_HPP

#include <realm/object-store/sync/app_config.hpp>
#include <realm/object-store/sync/app_user.hpp>
#include <realm/util/function_ref.hpp>

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace realm {
class SyncFileManager;

namespace app {
class App;

class MetadataStore {
public:
    virtual ~MetadataStore();

    // Attempt to perform all pending file actions for the given path. Returns
    // true if any were performed.
    virtual bool immediately_run_file_actions(SyncFileManager& fm, std::string_view realm_path) = 0;

    virtual void create_file_action(SyncFileAction action, std::string_view original_path,
                                    std::string_view recovery_path) = 0;

    virtual bool has_logged_in_user(std::string_view user_id) = 0;
    virtual std::optional<UserData> get_user(std::string_view user_id) = 0;

    // Create a user if no user with this id exists, or update only the given
    // fields if one does
    virtual void create_user(std::string_view user_id, std::string_view refresh_token, std::string_view access_token,
                             std::string_view device_id) = 0;

    // Update the stored data for an existing user
    virtual void update_user(std::string_view user_id, const UserData& data) = 0;

    // Discard tokens, set state to the given one, and if the user is the current
    // user set it to the new active user
    virtual void log_out(std::string_view user_id, SyncUser::State new_state) = 0;
    virtual void delete_user(SyncFileManager& file_manager, std::string_view user_id) = 0;

    virtual std::string get_current_user() = 0;
    virtual void set_current_user(std::string_view user_id) = 0;

    virtual std::vector<std::string> get_all_users() = 0;

    virtual void add_realm_path(std::string_view user_id, std::string_view path) = 0;
};

std::unique_ptr<MetadataStore> create_metadata_store(const AppConfig& config, SyncFileManager& file_manager);

} // namespace app
} // namespace realm

#endif // REALM_OS_APP_BACKING_STORE_HPP
