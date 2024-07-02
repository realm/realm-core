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
    // Get the user data for the given user if it exists and is not Removed,
    // or nullopt otherwise.
    virtual std::optional<UserData> get_user(std::string_view user_id) = 0;

    // Create a user if no user with this id exists, or update only the given
    // fields if one does
    virtual void create_user(std::string_view user_id, std::string_view refresh_token, std::string_view access_token,
                             std::string_view device_id) = 0;

    // Update the stored data for an existing user
    virtual void update_user(std::string_view user_id, util::FunctionRef<void(UserData&)>) = 0;

    // Discard the given user's tokens and set its state to the given one (LoggedOut or Removed).
    // If the user was the active user, a new active user is selected from the
    // other logged in users, or set to null if there are none. If the new state
    // is Removed, the user and their local Realm files are scheduled for deletion
    // on next launch.
    virtual void log_out(std::string_view user_id, SyncUser::State new_state) = 0;
    // As log_out(user_id, State::Removed), but also attempt to immediately
    // delete all of the user's local Realm files and only create file actions
    // for ones which cannot be deleted immediately.
    virtual void delete_user(SyncFileManager& file_manager, std::string_view user_id) = 0;

    // Get the user_id of the designated active user, or empty string if there
    // are none. The active user will always be logged in, and there will always
    // be an active user if any users are logged in.
    virtual std::string get_current_user() = 0;
    // Select the new active user. If the given user_id does not exist or is not
    // a logged in user an arbitrary logged-in user will be used instead.
    virtual void set_current_user(std::string_view user_id) = 0;

    // Get all non-Removed users, including ones which are currently logged out
    virtual std::vector<std::string> get_all_users() = 0;

    virtual void add_realm_path(std::string_view user_id, std::string_view path) = 0;
};

std::unique_ptr<MetadataStore> create_metadata_store(const AppConfig& config, SyncFileManager& file_manager);

} // namespace app
} // namespace realm

#endif // REALM_OS_APP_BACKING_STORE_HPP
