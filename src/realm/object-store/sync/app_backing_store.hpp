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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <realm/util/function_ref.hpp>

namespace realm {

class SyncAppMetadata;
class SyncMetadataManager;
class SyncUser;

namespace app {

class App;

struct BackingStoreConfig {
    enum class MetadataMode {
        NoEncryption, // Enable metadata, but disable encryption.
        Encryption,   // Enable metadata, and use encryption (automatic if possible).
        NoMetadata,   // Disable metadata.
    };

    std::string base_file_path;
    MetadataMode metadata_mode = MetadataMode::Encryption;
    std::optional<std::vector<char>> custom_encryption_key;
};

class BackingStore {
public:
    BackingStore(std::weak_ptr<app::App> parent, BackingStoreConfig config)
        : m_config(config)
        , m_parent_app(parent)
    {
    }
    // Get a sync user for a given identity, or create one if none exists yet, and set its token.
    // If a logged-out user exists, it will marked as logged back in.
    virtual std::shared_ptr<SyncUser> get_user(const std::string& user_id, const std::string& refresh_token,
                                               const std::string& access_token, const std::string& device_id) = 0;

    // Get an existing user for a given identifier, if one exists and is logged in.
    virtual std::shared_ptr<SyncUser> get_existing_logged_in_user(const std::string& user_id) const = 0;

    // Get all the users that are logged in and not errored out.
    virtual std::vector<std::shared_ptr<SyncUser>> all_users() = 0;

    // Gets the currently active user.
    virtual std::shared_ptr<SyncUser> get_current_user() const = 0;

    // Log out a given user
    virtual void log_out_user(const SyncUser& user) = 0;

    // Sets the currently active user.
    virtual void set_current_user(const std::string& user_id) = 0;

    // Removes a user
    virtual void remove_user(const std::string& user_id) = 0;

    // Permanently deletes a user.
    virtual void delete_user(const std::string& user_id) = 0;

    // Destroy all users persisted state and mark oustanding User instances as Removed
    // clean up persisted state.
    virtual void reset_for_testing() = 0;

    // Called on start up after construction.
    // The benefit being that `shared_from_this()` will work here.
    virtual void initialize() = 0;

    // FIXME: this is an implementation detail leak and doesn't belong in this API
    // FIXME: consider abstracting it to something called `on_manual_client_reset()`
    // Immediately run file actions for a single Realm at a given original path.
    // Returns whether or not a file action was successfully executed for the specified Realm.
    // Preconditions: all references to the Realm at the given path must have already been invalidated.
    // The metadata and file management subsystems must also have already been configured.
    virtual bool immediately_run_file_actions(const std::string& original_name) = 0;

    // If the metadata manager is configured, perform an update. Returns `true` if the code was run.
    virtual bool perform_metadata_update(util::FunctionRef<void(SyncMetadataManager&)> update_function) const = 0;

    // Get the default path for a Realm for the given SyncUser.
    // The default value is `<rootDir>/<appId>/<userId>/<partitionValue>.realm`.
    // If the file cannot be created at this location, for example due to path length restrictions,
    // this function may pass back `<rootDir>/<hashedFileName>.realm`
    // The `user` is required.
    // If partition_value is empty, FLX sync is requested
    // otherwise this is for a PBS Realm and the string
    // is a BSON formatted value.
    virtual std::string path_for_realm(std::shared_ptr<SyncUser> user,
                                       std::optional<std::string> custom_file_name = std::nullopt,
                                       std::optional<std::string> partition_value = std::nullopt) const = 0;

    // FIXME: see if this can be removed
    // Get the path of the recovery directory for backed-up or recovered Realms.
    virtual std::string
    recovery_directory_path(std::optional<std::string> const& custom_dir_name = std::nullopt) const = 0;

    // Get the app metadata for the active app.
    virtual std::optional<SyncAppMetadata> app_metadata() const = 0;

    // Access to the config that was used to create this instance.
    const BackingStoreConfig& config() const
    {
        return m_config;
    }

protected:
    const BackingStoreConfig m_config;
    std::weak_ptr<app::App> m_parent_app;
};

} // namespace app
} // namespace realm

#endif // REALM_OS_APP_BACKING_STORE_HPP
