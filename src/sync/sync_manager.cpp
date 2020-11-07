////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
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

#include "sync/sync_manager.hpp"

#include "sync/impl/sync_client.hpp"
#include "sync/impl/sync_file.hpp"
#include "sync/impl/sync_metadata.hpp"
#include "sync/sync_session.hpp"
#include "sync/sync_user.hpp"
#include "sync/app.hpp"
#include "util/uuid.hpp"

#include <realm/util/sha_crypto.hpp>
#include <realm/util/hex_dump.hpp>

using namespace realm;
using namespace realm::_impl;

void SyncManager::configure(std::shared_ptr<app::App> app,
                            const std::string& sync_route,
                            const SyncClientConfig& config)
{
    m_app = app;
    m_sync_route = sync_route;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_config = std::move(config);
        if (m_sync_client)
            return;
    }

    struct UserCreationData {
        std::string identity;
        std::string refresh_token;
        std::string access_token;
        std::string provider_type;
        std::vector<SyncUserIdentity> identities;
        SyncUser::State state;
        std::string device_id;
    };

    std::vector<UserCreationData> users_to_add;
    {
        std::lock_guard<std::mutex> lock(m_file_system_mutex);

        // Set up the file manager.
        if (m_file_manager) {
            // Changing the base path for tests requires calling reset_for_testing()
            // first, and otherwise isn't supported
            REALM_ASSERT(m_file_manager->base_path() == m_config.base_file_path);
        } else {
            m_file_manager = std::make_unique<SyncFileManager>(m_config.base_file_path, app->config().app_id);
        }

        // Set up the metadata manager, and perform initial loading/purging work.
        if (m_metadata_manager || m_config.metadata_mode == MetadataMode::NoMetadata) {
            // No metadata means we use a new client uuid each time
            if (!m_metadata_manager)
                m_client_uuid = util::uuid_string();
            return;
        }

        bool encrypt = m_config.metadata_mode == MetadataMode::Encryption;
        try {
            m_metadata_manager = std::make_unique<SyncMetadataManager>(m_file_manager->metadata_path(),
                                                                       encrypt,
                                                                       m_config.custom_encryption_key);
        } catch (RealmFileException const& ex) {
            if (m_config.reset_metadata_on_error && m_file_manager->remove_metadata_realm()) {
                m_metadata_manager = std::make_unique<SyncMetadataManager>(m_file_manager->metadata_path(),
                                                                           encrypt,
                                                                           std::move(m_config.custom_encryption_key));
            } else {
                throw;
            }
        }

        REALM_ASSERT(m_metadata_manager);
        m_client_uuid = m_metadata_manager->client_uuid();

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
            auto device_id = user_data.device_id();
            if (!refresh_token.empty() && !access_token.empty()) {
                users_to_add.push_back(UserCreationData{
                    user_data.identity(),
                    std::move(refresh_token),
                    std::move(access_token),
                    user_data.provider_type(),
                    user_data.identities(),
                    user_data.state(),
                    device_id
                });
            }
        }

        // Delete any users marked for death.
        std::vector<SyncUserMetadata> dead_users;
        SyncUserMetadataResults users_to_remove = m_metadata_manager->all_users_marked_for_removal();
        dead_users.reserve(users_to_remove.size());
        for (size_t i = 0; i < users_to_remove.size(); i++) {
            auto user = users_to_remove.get(i);
            // FIXME: delete user data in a different way? (This deletes a logged-out user's data as soon as the app
            // launches again, which might not be how some apps want to treat their data.)
            try {
                m_file_manager->remove_user_directory(user.identity());
                dead_users.emplace_back(std::move(user));
            } catch (util::File::AccessError const&) {
                continue;
            }
        }
        for (auto& user : dead_users) {
            user.remove();
        }
    }
    {
        std::lock_guard<std::mutex> lock(m_user_mutex);
        for (auto& user_data : users_to_add) {
            auto& identity = user_data.identity;
            auto& provider_type = user_data.provider_type;
            auto user = std::make_shared<SyncUser>(user_data.refresh_token,
                                                   identity,
                                                   provider_type,
                                                   user_data.access_token,
                                                   user_data.state,
                                                   user_data.device_id,
                                                   shared_from_this());
            user->update_identities(user_data.identities);
            m_users.emplace_back(std::move(user));
        }
    }
}

bool SyncManager::immediately_run_file_actions(const std::string& realm_path)
{
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
bool SyncManager::run_file_action(const SyncFileActionMetadata& md)
{
    switch (md.action()) {
        case SyncFileActionMetadata::Action::DeleteRealm:
            // Delete all the files for the given Realm.
            m_file_manager->remove_realm(md.original_name());
            return true;
        case SyncFileActionMetadata::Action::BackUpThenDeleteRealm:
            // Copy the primary Realm file to the recovery dir, and then delete the Realm.
            auto new_name = md.new_name();
            auto original_name = md.original_name();
            if (!util::File::exists(original_name)) {
                // The Realm file doesn't exist anymore.
                return true;
            }
            if (new_name && !util::File::exists(*new_name) && m_file_manager->copy_realm_file(original_name, *new_name)) {
                // We successfully copied the Realm file to the recovery directory.
                m_file_manager->remove_realm(original_name);
                return true;
            }
            return false;
    }
    return false;
}

void SyncManager::reset_for_testing()
{
    std::lock_guard<std::mutex> lock(m_file_system_mutex);
    if (m_file_manager)
        util::try_remove_dir_recursive(m_file_manager->base_path());
    m_file_manager = nullptr;
    m_metadata_manager = nullptr;
    m_client_uuid = util::none;

    {
        // Destroy all the users.
        std::lock_guard<std::mutex> lock(m_user_mutex);
        m_users.clear();
        m_current_user = nullptr;
    }
    {
        std::lock_guard<std::mutex> lock(m_mutex);

        // Stop the client. This will abort any uploads that inactive sessions are waiting for.
        if (m_sync_client)
            m_sync_client->stop();

        {
            std::lock_guard<std::mutex> lock(m_session_mutex);
            // Callers of `SyncManager::reset_for_testing` should ensure there are no existing sessions
            // prior to calling `reset_for_testing`.
            bool no_sessions = !do_has_existing_sessions();
            REALM_ASSERT_RELEASE(no_sessions);

            // Destroy any inactive sessions.
            // FIXME: We shouldn't have any inactive sessions at this point! Sessions are expected to
            // remain inactive until their final upload completes, at which point they are unregistered
            // and destroyed. Our call to `sync::Client::stop` above aborts all uploads, so all sessions
            // should have already been destroyed.
            m_sessions.clear();
        }

        // Destroy the client now that we have no remaining sessions.
        m_sync_client = nullptr;

        // Reset even more state.
        m_config = {};

        m_sync_route = "";
    }
}

void SyncManager::set_log_level(util::Logger::Level level) noexcept
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_config.log_level = level;
}

void SyncManager::set_logger_factory(SyncLoggerFactory& factory) noexcept
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_config.logger_factory = &factory;
}

std::unique_ptr<util::Logger> SyncManager::make_logger() const
{
    if (m_config.logger_factory) {
        return m_config.logger_factory->make_logger(m_config.log_level); // Throws
    }

    auto stderr_logger = std::make_unique<util::StderrLogger>(); // Throws
    stderr_logger->set_level_threshold(m_config.log_level);
    return std::unique_ptr<util::Logger>(stderr_logger.release());
}

void SyncManager::set_user_agent(std::string user_agent)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_config.user_agent_application_info = std::move(user_agent);
}

void SyncManager::set_timeouts(SyncClientTimeouts timeouts)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_config.timeouts = timeouts;
}

void SyncManager::reconnect() const
{
    std::lock_guard<std::mutex> lock(m_session_mutex);
    for (auto& it : m_sessions) {
        it.second->handle_reconnect();
    }
}

util::Logger::Level SyncManager::log_level() const noexcept
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_config.log_level;
}

bool SyncManager::perform_metadata_update(std::function<void(const SyncMetadataManager&)> update_function) const
{
    std::lock_guard<std::mutex> lock(m_file_system_mutex);
    if (!m_metadata_manager) {
        return false;
    }
    update_function(*m_metadata_manager);
    return true;
}

std::shared_ptr<SyncUser> SyncManager::get_user(const std::string& user_id,
                                                std::string refresh_token,
                                                std::string access_token,
                                                const std::string provider_type,
                                                std::string device_id)
{
    std::lock_guard<std::mutex> lock(m_user_mutex);
    auto it = std::find_if(m_users.begin(),
                           m_users.end(),
                           [user_id, provider_type](const auto& user) {
        return user->identity() == user_id && user->provider_type() == provider_type;
    });
    if (it == m_users.end()) {
        // No existing user.
        auto new_user = std::make_shared<SyncUser>(std::move(refresh_token),
                                                   user_id,
                                                   provider_type,
                                                   std::move(access_token),
                                                   SyncUser::State::LoggedIn,
                                                   device_id,
                                                   shared_from_this());
        m_users.emplace(m_users.begin(), new_user);
        if (!m_metadata_manager)
            m_current_user = new_user;
        return new_user;
    } else {
        auto user = *it;
        if (user->state() == SyncUser::State::Removed) {
            return nullptr;
        }

        // It is important that the access token is set before the refresh token
        // as once each token is set it attempts to revive any pending sessions
        // (e.g. as user logs out and logs back in they would be using an empty access token with the sync client
        // if the order of these were flipped).
        user->update_access_token(std::move(access_token));
        user->update_refresh_token(std::move(refresh_token));

        if (user->state() == SyncUser::State::LoggedOut) {
            user->set_state(SyncUser::State::LoggedIn);
        }
        return user;
    }
}

std::vector<std::shared_ptr<SyncUser>> SyncManager::all_users()
{
    std::lock_guard<std::mutex> lock(m_user_mutex);
    m_users.erase(std::remove_if(m_users.begin(), m_users.end(), [](auto& user) {
        return user->state() == SyncUser::State::Removed;
    }), m_users.end());
    return m_users;
}

std::shared_ptr<SyncUser> SyncManager::get_user_for_identity(std::string const& identity) const noexcept
{
    auto is_active_user = [identity](auto& el) {
        return el->identity() == identity;
    };
    auto it = std::find_if(m_users.begin(), m_users.end(), is_active_user);
    return it == m_users.end() ? nullptr : *it;
}

std::shared_ptr<SyncUser> SyncManager::get_current_user() const
{
    std::lock_guard<std::mutex> lock(m_user_mutex);

    if (m_current_user)
        return m_current_user;
    if (!m_metadata_manager)
        return nullptr;

    auto cur_user_ident = m_metadata_manager->get_current_user_identity();
    return cur_user_ident ? get_user_for_identity(*cur_user_ident) : nullptr;
}

void SyncManager::log_out_user(const std::string& user_id)
{
    std::lock_guard<std::mutex> lock(m_user_mutex);

    // Move this user to the end of the vector
    if (m_users.size() > 1) {
        auto it = std::find_if(m_users.begin(),
                               m_users.end(),
                               [user_id](const auto& user) {
            return user->identity() == user_id;
        });

        if (it != m_users.end())
            std::rotate(it, it + 1, m_users.end());
    }

    bool was_active = (m_current_user && m_current_user->identity() == user_id)
        || (m_metadata_manager && m_metadata_manager->get_current_user_identity() == user_id);
    if (!was_active)
        return;

    // Set the current active user to the next logged in user, or null if none
    for (auto& user : m_users) {
        if (user->state() == SyncUser::State::LoggedIn) {
            if (m_metadata_manager)
                m_metadata_manager->set_current_user_identity(user->identity());
            m_current_user = user;
            return;
        }
    }

    if (m_metadata_manager)
        m_metadata_manager->set_current_user_identity("");
    m_current_user = nullptr;
}

void SyncManager::set_current_user(const std::string& user_id)
{
    std::lock_guard<std::mutex> lock(m_user_mutex);

    m_current_user = get_user_for_identity(user_id);
    if (m_metadata_manager)
        m_metadata_manager->set_current_user_identity(user_id);
}

void SyncManager::remove_user(const std::string& user_id)
{
    std::lock_guard<std::mutex> lock(m_user_mutex);
    auto user = get_user_for_identity(user_id);
    if (!user)
        return;
    user->set_state(SyncUser::State::Removed);

    if (!m_metadata_manager)
        return;

    for (size_t i = 0; i < m_metadata_manager->all_unmarked_users().size(); i++) {
        auto metadata = m_metadata_manager->all_unmarked_users().get(i);
        if (user->identity() == metadata.identity()) {
            metadata.mark_for_removal();
        }
    }
}

std::shared_ptr<SyncUser> SyncManager::get_existing_logged_in_user(const std::string& user_id) const
{
    std::lock_guard<std::mutex> lock(m_user_mutex);
    auto user = get_user_for_identity(user_id);
    return user && user->state() == SyncUser::State::LoggedIn ? user : nullptr;
}

struct UnsupportedBsonPartition : public std::logic_error {
    UnsupportedBsonPartition(std::string msg) : std::logic_error(msg) {}
};

static std::string string_from_partition(const std::string& partition)
{
    try {
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
            case bson::Bson::Type::Null:
                return "null";
            default:
                throw UnsupportedBsonPartition(util::format("Unsupported partition key value: '%1'. Only int, string and ObjectId types are currently supported.", partition_value.to_string()));
        }
    }
    catch (const UnsupportedBsonPartition&) {
        throw;
    }
    catch (...) {
        // FIXME: the partition wasn't a bson formatted string, this can happen when using the
        // test sync server which only accepts filesystem type paths, in this case return the raw partition.
        // Once we migrate away from using the sync server in tests, this code path should not be necessary.
        return partition;
    }
}

std::string SyncManager::path_for_realm(const SyncUser& user, const std::string& realm_file_name) const
{
    std::lock_guard<std::mutex> lock(m_file_system_mutex);
    REALM_ASSERT(m_file_manager);
    return m_file_manager->realm_file_path(user.identity(), user.local_identity(), realm_file_name);
}

std::string SyncManager::path_for_realm(const SyncConfig& config, util::Optional<std::string> custom_file_name) const
{
    std::lock_guard<std::mutex> lock(m_file_system_mutex);
    REALM_ASSERT(m_file_manager);
    REALM_ASSERT(config.user);

    // We used to hash the string value of the partition. For compatibility, check that SHA256
    // hash file name exists, and if it does, continue to use it.
    std::array<unsigned char, 32> hash;
    util::sha256(config.partition_value.data(), config.partition_value.size(), hash.data());
    std::string legacy_hashed_file_name = util::hex_dump(hash.data(), hash.size(), "");
    std::string legacy_file_path = m_file_manager->realm_file_path(config.user->identity(), config.user->local_identity(), legacy_hashed_file_name);
    if (m_file_manager->try_file_exists(legacy_hashed_file_name)) {
        return legacy_file_path;
    }

    // Attempt to make a nicer filename which will ease debugging when
    // locating files in the filesystem.
    std::string file_name = (custom_file_name) ? custom_file_name.value() : string_from_partition(config.partition_value);
    return m_file_manager->realm_file_path(config.user->identity(), config.user->local_identity(), file_name);
}

std::string SyncManager::recovery_directory_path(util::Optional<std::string> const& custom_dir_name) const
{
    std::lock_guard<std::mutex> lock(m_file_system_mutex);
    REALM_ASSERT(m_file_manager);
    return m_file_manager->recovery_directory_path(custom_dir_name);
}

std::shared_ptr<SyncSession> SyncManager::get_existing_active_session(const std::string& path) const
{
    std::lock_guard<std::mutex> lock(m_session_mutex);
    if (auto session = get_existing_session_locked(path)) {
        if (auto external_reference = session->existing_external_reference())
            return external_reference;
    }
    return nullptr;
}

std::shared_ptr<SyncSession> SyncManager::get_existing_session_locked(const std::string& path) const
{
    REALM_ASSERT(!m_session_mutex.try_lock());
    auto it = m_sessions.find(path);
    return it == m_sessions.end() ? nullptr : it->second;
}

std::shared_ptr<SyncSession> SyncManager::get_existing_session(const std::string& path) const
{
    std::lock_guard<std::mutex> lock(m_session_mutex);
    if (auto session = get_existing_session_locked(path))
        return session->external_reference();

    return nullptr;
}

std::shared_ptr<SyncSession> SyncManager::get_session(const std::string& path, const SyncConfig& sync_config, bool force_client_resync)
{
    auto& client = get_sync_client(); // Throws

    std::lock_guard<std::mutex> lock(m_session_mutex);
    if (auto session = get_existing_session_locked(path)) {
        sync_config.user->register_session(session);
        return session->external_reference();
    }

    auto shared_session = SyncSession::create(client, path, sync_config, force_client_resync);
    m_sessions[path] = shared_session;

    // Create the external reference immediately to ensure that the session will become
    // inactive if an exception is thrown in the following code.
    auto external_reference = shared_session->external_reference();

    sync_config.user->register_session(std::move(shared_session));

    return external_reference;
}


bool SyncManager::has_existing_sessions()
{
    std::lock_guard<std::mutex> lock(m_session_mutex);
    return do_has_existing_sessions();
}

bool SyncManager::do_has_existing_sessions(){
    return std::any_of(m_sessions.begin(), m_sessions.end(), [](auto& element){
        return element.second->existing_external_reference();
    });
}

void SyncManager::unregister_session(const std::string& path)
{
    std::lock_guard<std::mutex> lock(m_session_mutex);
    auto it = m_sessions.find(path);
    REALM_ASSERT(it != m_sessions.end());

    // If the session has an active external reference, leave it be. This will happen if the session
    // moves to an inactive state while still externally reference, for instance, as a result of
    // the session's user being logged out.
    if (it->second->existing_external_reference())
        return;

    m_sessions.erase(path);
}

void SyncManager::enable_session_multiplexing()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_config.multiplex_sessions)
        return; // Already enabled, we can ignore

    if (m_sync_client)
        throw std::logic_error("Cannot enable session multiplexing after creating the sync client");

    m_config.multiplex_sessions = true;
}

SyncClient& SyncManager::get_sync_client() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_sync_client)
        m_sync_client = create_sync_client(); // Throws
    return *m_sync_client;
}

std::unique_ptr<SyncClient> SyncManager::create_sync_client() const
{
    REALM_ASSERT(!m_mutex.try_lock());
    return std::make_unique<SyncClient>(make_logger(), m_config, shared_from_this());
}

std::string SyncManager::client_uuid() const
{
    REALM_ASSERT(m_client_uuid);
    return *m_client_uuid;
}

util::Optional<SyncAppMetadata> SyncManager::app_metadata() const
{
    if (!m_metadata_manager) {
        return util::none;
    }
    return m_metadata_manager->get_app_metadata();
}
