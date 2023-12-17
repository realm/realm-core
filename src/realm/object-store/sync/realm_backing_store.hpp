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

#ifndef REALM_OS_BACKING_STORE_HPP
#define REALM_OS_BACKING_STORE_HPP

#include <realm/object-store/sync/app_backing_store.hpp>

#include <realm/util/checked_mutex.hpp>
#include <memory>

namespace realm {
class SyncUser;
class SyncFileManager;
class SyncMetadataManager;
class SyncFileActionMetadata;
class SyncAppMetadata;
} // namespace realm

namespace realm::app {
class App;

struct RealmBackingStoreConfig {
    enum class MetadataMode {
        NoEncryption, // Enable metadata, but disable encryption.
        Encryption,   // Enable metadata, and use encryption (automatic if possible).
        NoMetadata,   // Disable metadata.
    };

    std::string base_file_path;
    MetadataMode metadata_mode = MetadataMode::Encryption;
    std::optional<std::vector<char>> custom_encryption_key;
};

struct RealmBackingStore final : public app::BackingStore {

    RealmBackingStore(std::weak_ptr<app::App> parent, RealmBackingStoreConfig config);
    virtual ~RealmBackingStore();
    std::shared_ptr<SyncUser> get_user(std::string_view user_id, std::string_view refresh_token,
                                       std::string_view access_token, std::string_view device_id) override
        REQUIRES(!m_user_mutex, !m_file_system_mutex);
    std::shared_ptr<SyncUser> get_existing_logged_in_user(std::string_view user_id) const override
        REQUIRES(!m_user_mutex);
    std::vector<std::shared_ptr<SyncUser>> all_users() override REQUIRES(!m_user_mutex);
    std::shared_ptr<SyncUser> get_current_user() const override REQUIRES(!m_user_mutex, !m_file_system_mutex);
    void log_out_user(const SyncUser& user) override REQUIRES(!m_user_mutex, !m_file_system_mutex);
    void set_current_user(std::string_view user_id) override REQUIRES(!m_user_mutex, !m_file_system_mutex);
    void remove_user(std::string_view user_id) override REQUIRES(!m_user_mutex, !m_file_system_mutex);
    void delete_user(std::string_view user_id) override REQUIRES(!m_user_mutex, !m_file_system_mutex);
    void reset_for_testing() override REQUIRES(!m_user_mutex, !m_file_system_mutex);
    bool immediately_run_file_actions(std::string_view realm_path) override REQUIRES(!m_file_system_mutex);
    bool perform_metadata_update(util::FunctionRef<void(SyncMetadataManager&)> update_function) const override
        REQUIRES(!m_file_system_mutex);

    std::string path_for_realm(std::shared_ptr<SyncUser> user, std::optional<std::string> custom_file_name = none,
                               std::optional<std::string> partition_value = none) const override
        REQUIRES(!m_file_system_mutex);

    std::string audit_path_root(std::shared_ptr<SyncUser> user, std::string_view app_id,
                                std::string_view partition_prefix) const override;

    std::string recovery_directory_path(std::optional<std::string> const& custom_dir_name = none) const override
        REQUIRES(!m_file_system_mutex);
    std::optional<SyncAppMetadata> app_metadata() const override REQUIRES(!m_file_system_mutex);

    // Access to the config that was used to create this instance.
    const RealmBackingStoreConfig& config() const
    {
        return m_config;
    }

private:
    std::shared_ptr<SyncUser> get_user_for_id(std::string_view user_id) const noexcept REQUIRES(m_user_mutex);
    bool run_file_action(SyncFileActionMetadata&) REQUIRES(m_file_system_mutex);
    void initialize() REQUIRES(!m_file_system_mutex, !m_user_mutex);

    const RealmBackingStoreConfig m_config;

    // Protects m_users
    mutable util::CheckedMutex m_user_mutex;

    // A vector of all SyncUser objects.
    std::vector<std::shared_ptr<SyncUser>> m_users GUARDED_BY(m_user_mutex);
    std::shared_ptr<SyncUser> m_current_user GUARDED_BY(m_user_mutex);

    // Protects m_file_manager and m_metadata_manager
    mutable util::CheckedMutex m_file_system_mutex;
    std::unique_ptr<SyncFileManager> m_file_manager GUARDED_BY(m_file_system_mutex);
    std::unique_ptr<SyncMetadataManager> m_metadata_manager GUARDED_BY(m_file_system_mutex);
};

} // namespace realm::app

#endif // REALM_OS_BACKING_STORE_HPP
