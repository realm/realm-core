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

#include <realm/object-store/sync/realm_backing_store.hpp>

#include <realm/object-store/sync/app_backing_store.hpp>
#include <realm/object-store/sync/impl/sync_file.hpp>
#include <realm/object-store/sync/impl/sync_metadata.hpp>
#include <realm/object-store/sync/sync_user.hpp>

using namespace realm;
using namespace realm::app;

RealmBackingStore::RealmBackingStore(std::weak_ptr<app::App> parent, RealmBackingStoreConfig config)
    : app::BackingStore(parent)
    , m_config(config)
{
    initialize();
}

RealmBackingStore::~RealmBackingStore()
{
    util::CheckedLockGuard lk(m_user_mutex);
    for (auto& user : m_users) {
        user->detach_from_backing_store();
    }
}

void RealmBackingStore::reset_for_testing()
{
    {
        util::CheckedLockGuard lock(m_file_system_mutex);
        m_metadata_manager = nullptr;
    }

    {
        // Destroy all the users.
        util::CheckedLockGuard lock(m_user_mutex);
        for (auto& user : m_users) {
            user->detach_from_backing_store();
        }
        m_users.clear();
        m_current_user = nullptr;
    }
    // FIXME: clearing disk state might be happening too soon?
    {
        util::CheckedLockGuard lock(m_file_system_mutex);
        if (m_file_manager)
            util::try_remove_dir_recursive(m_file_manager->base_path());
        m_file_manager = nullptr;
    }
}

void RealmBackingStore::initialize()
{
    std::vector<std::shared_ptr<SyncUser>> users_to_add;
    {
        util::CheckedLockGuard lock(m_file_system_mutex);
        // The RealmBackingStore is not designed to be used
        // across multiple App instances.
        REALM_ASSERT_RELEASE(!m_file_manager);
        // Set up the file manager.
        m_file_manager =
            std::make_unique<SyncFileManager>(m_config.base_file_path, m_parent_app.lock()->config().app_id);

        // Set up the metadata manager, and perform initial loading/purging work.
        if (m_metadata_manager || m_config.metadata_mode == app::RealmBackingStoreConfig::MetadataMode::NoMetadata) {
            return;
        }

        bool encrypt = m_config.metadata_mode == app::RealmBackingStoreConfig::MetadataMode::Encryption;
        m_metadata_manager = std::make_unique<SyncMetadataManager>(m_file_manager->metadata_path(), encrypt,
                                                                   m_config.custom_encryption_key);

        REALM_ASSERT(m_metadata_manager);

        // Perform our "on next startup" actions such as deleting Realm files
        // which we couldn't delete immediately due to them being in use
        std::vector<SyncFileActionMetadata> completed_actions;
        SyncFileActionMetadataResults file_actions = m_metadata_manager->all_pending_actions();
        for (size_t i = 0; i < file_actions.size(); i++) {
            auto file_action = file_actions.get(i);
            if (run_file_action(file_action)) {
                completed_actions.emplace_back(std::move(file_action));
            }
        }
        for (auto& action : completed_actions) {
            action.remove();
        }

        // Load persisted users into the users map.
        SyncUserMetadataResults users = m_metadata_manager->all_unmarked_users();
        for (size_t i = 0; i < users.size(); i++) {
            auto user_data = users.get(i);
            auto refresh_token = user_data.refresh_token();
            auto access_token = user_data.access_token();
            if (!refresh_token.empty() && !access_token.empty()) {
                users_to_add.push_back(BackingStore::make_user(user_data, m_parent_app.lock()));
            }
        }

        // Delete any users marked for death.
        std::vector<SyncUserMetadata> dead_users;
        SyncUserMetadataResults users_to_remove = m_metadata_manager->all_users_marked_for_removal();
        dead_users.reserve(users_to_remove.size());
        for (size_t i = 0; i < users_to_remove.size(); i++) {
            auto user = users_to_remove.get(i);
            // FIXME: delete user data in a different way? (This deletes a logged-out user's data as soon as the
            // app launches again, which might not be how some apps want to treat their data.)
            try {
                m_file_manager->remove_user_realms(user.user_id(), user.realm_file_paths());
                dead_users.emplace_back(std::move(user));
            }
            catch (FileAccessError const&) {
                continue;
            }
        }
        for (auto& user : dead_users) {
            user.remove();
        }
    }
    {
        util::CheckedLockGuard lock(m_user_mutex);
        m_users.insert(m_users.end(), users_to_add.begin(), users_to_add.end());
    }
}

bool RealmBackingStore::immediately_run_file_actions(std::string_view realm_path)
{
    util::CheckedLockGuard lock(m_file_system_mutex);
    if (!m_metadata_manager) {
        return false;
    }
    if (auto metadata = m_metadata_manager->get_file_action_metadata(realm_path)) {
        if (run_file_action(*metadata)) {
            metadata->remove();
            return true;
        }
    }
    return false;
}

// Perform a file action. Returns whether or not the file action can be removed.
bool RealmBackingStore::run_file_action(SyncFileActionMetadata& md)
{
    switch (md.action()) {
        case SyncFileActionMetadata::Action::DeleteRealm:
            // Delete all the files for the given Realm.
            return m_file_manager->remove_realm(md.original_name());
        case SyncFileActionMetadata::Action::BackUpThenDeleteRealm:
            // Copy the primary Realm file to the recovery dir, and then delete the Realm.
            auto new_name = md.new_name();
            auto original_name = md.original_name();
            if (!util::File::exists(original_name)) {
                // The Realm file doesn't exist anymore.
                return true;
            }
            if (new_name && !util::File::exists(*new_name) &&
                m_file_manager->copy_realm_file(original_name, *new_name)) {
                // We successfully copied the Realm file to the recovery directory.
                bool did_remove = m_file_manager->remove_realm(original_name);
                // if the copy succeeded but not the delete, then running BackupThenDelete
                // a second time would fail, so change this action to just delete the original file.
                if (did_remove) {
                    return true;
                }
                md.set_action(SyncFileActionMetadata::Action::DeleteRealm);
                return false;
            }
            return false;
    }
    return false;
}

bool RealmBackingStore::perform_metadata_update(util::FunctionRef<void(SyncMetadataManager&)> update_function) const
{
    util::CheckedLockGuard lock(m_file_system_mutex);
    if (!m_metadata_manager) {
        return false;
    }
    update_function(*m_metadata_manager);
    return true;
}

std::shared_ptr<SyncUser> RealmBackingStore::get_user(std::string_view user_id, std::string_view refresh_token,
                                                      std::string_view access_token, std::string_view device_id)
{
    std::shared_ptr<SyncUser> user;
    {
        util::CheckedLockGuard lock(m_user_mutex);
        auto it = std::find_if(m_users.begin(), m_users.end(), [&](const auto& user) {
            return user->user_id() == user_id && user->state() != SyncUser::State::Removed;
        });
        if (it == m_users.end()) {
            // No existing user.
            auto new_user =
                BackingStore::make_user(refresh_token, user_id, access_token, device_id, m_parent_app.lock());
            m_users.emplace(m_users.begin(), new_user);
            {
                util::CheckedLockGuard lock(m_file_system_mutex);
                // m_current_user is normally set very indirectly via the metadata manger
                if (!m_metadata_manager)
                    m_current_user = new_user;
            }
            return new_user;
        }

        // LoggedOut => LoggedIn
        user = *it;
        REALM_ASSERT(user->state() != SyncUser::State::Removed);
    }
    user->log_in(access_token, refresh_token);
    return user;
}

std::vector<std::shared_ptr<SyncUser>> RealmBackingStore::all_users()
{
    util::CheckedLockGuard lock(m_user_mutex);
    m_users.erase(std::remove_if(m_users.begin(), m_users.end(),
                                 [](auto& user) {
                                     bool should_remove = (user->state() == SyncUser::State::Removed);
                                     if (should_remove) {
                                         user->detach_from_backing_store();
                                     }
                                     return should_remove;
                                 }),
                  m_users.end());
    return m_users;
}

std::shared_ptr<SyncUser> RealmBackingStore::get_user_for_id(std::string_view user_id) const noexcept
{
    auto is_active_user = [user_id](auto& el) {
        return el->user_id() == user_id;
    };
    auto it = std::find_if(m_users.begin(), m_users.end(), is_active_user);
    return it == m_users.end() ? nullptr : *it;
}

std::shared_ptr<SyncUser> RealmBackingStore::get_current_user() const
{
    util::CheckedLockGuard lock(m_user_mutex);

    if (m_current_user)
        return m_current_user;
    util::CheckedLockGuard fs_lock(m_file_system_mutex);
    if (!m_metadata_manager)
        return nullptr;

    auto cur_user_ident = m_metadata_manager->get_current_user_id();
    return cur_user_ident ? get_user_for_id(*cur_user_ident) : nullptr;
}

void RealmBackingStore::log_out_user(const SyncUser& user)
{
    util::CheckedLockGuard lock(m_user_mutex);

    // Move this user to the end of the vector
    auto user_pos = std::partition(m_users.begin(), m_users.end(), [&](auto& u) {
        return u.get() != &user;
    });

    auto active_user = std::find_if(m_users.begin(), user_pos, [](auto& u) {
        return u->state() == SyncUser::State::LoggedIn;
    });

    util::CheckedLockGuard fs_lock(m_file_system_mutex);
    bool was_active = m_current_user.get() == &user ||
                      (m_metadata_manager && m_metadata_manager->get_current_user_id() == user.user_id());
    if (!was_active)
        return;

    // Set the current active user to the next logged in user, or null if none
    if (active_user != user_pos) {
        m_current_user = *active_user;
        if (m_metadata_manager)
            m_metadata_manager->set_current_user_id((*active_user)->user_id());
    }
    else {
        m_current_user = nullptr;
        if (m_metadata_manager)
            m_metadata_manager->set_current_user_id("");
    }
}

void RealmBackingStore::set_current_user(std::string_view user_id)
{
    util::CheckedLockGuard lock(m_user_mutex);

    m_current_user = get_user_for_id(user_id);
    util::CheckedLockGuard fs_lock(m_file_system_mutex);
    if (m_metadata_manager)
        m_metadata_manager->set_current_user_id(user_id);
}

void RealmBackingStore::remove_user(std::string_view user_id)
{
    util::CheckedLockGuard lock(m_user_mutex);
    if (auto user = get_user_for_id(user_id))
        user->invalidate();
}

void RealmBackingStore::delete_user(std::string_view user_id)
{
    util::CheckedLockGuard lock(m_user_mutex);
    // Avoid iterating over m_users twice by not calling `get_user_for_id`.
    auto it = std::find_if(m_users.begin(), m_users.end(), [&user_id](auto& user) {
        return user->user_id() == user_id;
    });
    auto user = it == m_users.end() ? nullptr : *it;

    if (!user)
        return;

    // Deletion should happen immediately, not when we do the cleanup
    // task on next launch.
    m_users.erase(it);
    user->detach_from_backing_store();

    if (m_current_user && m_current_user->user_id() == user->user_id())
        m_current_user = nullptr;

    util::CheckedLockGuard fs_lock(m_file_system_mutex);
    if (!m_metadata_manager)
        return;

    auto users = m_metadata_manager->all_unmarked_users();
    for (size_t i = 0; i < users.size(); i++) {
        auto metadata = users.get(i);
        if (user->user_id() == metadata.user_id()) {
            m_file_manager->remove_user_realms(metadata.user_id(), metadata.realm_file_paths());
            metadata.remove();
            break;
        }
    }
}

std::shared_ptr<SyncUser> RealmBackingStore::get_existing_logged_in_user(std::string_view user_id) const
{
    util::CheckedLockGuard lock(m_user_mutex);
    auto user = get_user_for_id(user_id);
    return user && user->state() == SyncUser::State::LoggedIn ? user : nullptr;
}

struct UnsupportedBsonPartition : public std::logic_error {
    UnsupportedBsonPartition(std::string msg)
        : std::logic_error(msg)
    {
    }
};

static std::string string_from_partition(std::string_view partition)
{
    bson::Bson partition_value = bson::parse(partition);
    switch (partition_value.type()) {
        case bson::Bson::Type::Int32:
            return util::format("i_%1", static_cast<int32_t>(partition_value));
        case bson::Bson::Type::Int64:
            return util::format("l_%1", static_cast<int64_t>(partition_value));
        case bson::Bson::Type::String:
            return util::format("s_%1", static_cast<std::string>(partition_value));
        case bson::Bson::Type::ObjectId:
            return util::format("o_%1", static_cast<ObjectId>(partition_value).to_string());
        case bson::Bson::Type::Uuid:
            return util::format("u_%1", static_cast<UUID>(partition_value).to_string());
        case bson::Bson::Type::Null:
            return "null";
        default:
            throw UnsupportedBsonPartition(util::format("Unsupported partition key value: '%1'. Only int, string "
                                                        "UUID and ObjectId types are currently supported.",
                                                        partition_value.to_string()));
    }
}

std::string RealmBackingStore::path_for_realm(std::shared_ptr<SyncUser> user,
                                              std::optional<std::string> custom_file_name,
                                              std::optional<std::string> partition_value) const
{
    REALM_ASSERT(user);
    std::string path;
    {
        util::CheckedLockGuard lock(m_file_system_mutex);
        REALM_ASSERT(m_file_manager);

        // Attempt to make a nicer filename which will ease debugging when
        // locating files in the filesystem.
        auto file_name = [&]() -> std::string {
            if (custom_file_name) {
                return *custom_file_name;
            }
            if (!partition_value) {
                return "flx_sync_default";
            }
            return string_from_partition(*partition_value);
        }();
        path = m_file_manager->realm_file_path(user->user_id(), user->legacy_identities(), file_name,
                                               partition_value.value_or(""));
    }
    // Report the use of a Realm for this user, so the metadata can track it for clean up.
    perform_metadata_update([&](const auto& manager) {
        auto metadata = manager.get_or_make_user_metadata(user->user_id());
        metadata->add_realm_file_path(path);
    });
    return path;
}

std::string RealmBackingStore::audit_path_root(std::shared_ptr<SyncUser> user, std::string_view app_id,
                                               std::string_view partition_prefix) const
{
    REALM_ASSERT(user);

#ifdef _WIN32 // Move to File?
    const char separator[] = "\\";
#else
    const char separator[] = "/";
#endif

    // "$root/realm-audit/$appId/$userId/$partitonPrefix/"
    return util::format("%2%1realm-audit%1%3%1%4%1%5%1", separator, m_config.base_file_path, app_id, user->user_id(),
                        partition_prefix);
}

std::string RealmBackingStore::recovery_directory_path(util::Optional<std::string> const& custom_dir_name) const
{
    util::CheckedLockGuard lock(m_file_system_mutex);
    REALM_ASSERT(m_file_manager);
    return m_file_manager->recovery_directory_path(custom_dir_name);
}

std::optional<SyncAppMetadata> RealmBackingStore::app_metadata() const
{
    util::CheckedLockGuard lock(m_file_system_mutex);
    if (!m_metadata_manager) {
        return util::none;
    }
    return m_metadata_manager->get_app_metadata();
}
