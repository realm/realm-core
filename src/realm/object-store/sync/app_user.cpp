////////////////////////////////////////////////////////////////////////////
//
// Copyright 2024 Realm Inc.
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

#include <realm/object-store/sync/app_user.hpp>

#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/sync/impl/app_metadata.hpp>
#include <realm/object-store/sync/impl/sync_file.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/sync_manager.hpp>

namespace realm::app {

UserIdentity::UserIdentity(const std::string& id, const std::string& provider_type)
    : id(id)
    , provider_type(provider_type)
{
}

User::User(Private, std::shared_ptr<app::App> app, std::string_view user_id)
    : m_app(std::move(app))
    , m_app_id(m_app->app_id())
    , m_user_id(user_id)
{
    REALM_ASSERT(m_app);
    m_app->register_sync_user(*this);
}

User::~User()
{
    if (m_app) {
        m_app->unregister_sync_user(*this);
    }
}

std::string User::user_id() const noexcept
{
    return m_user_id;
}

std::string User::app_id() const noexcept
{
    return m_app_id;
}

std::vector<std::string> User::legacy_identities() const
{
    util::CheckedLockGuard lock(m_mutex);
    return m_data.legacy_identities;
}

std::string User::access_token() const
{
    util::CheckedLockGuard lock(m_mutex);
    return m_data.access_token.token;
}

std::string User::refresh_token() const
{
    util::CheckedLockGuard lock(m_mutex);
    return m_data.refresh_token.token;
}

SyncUser::State User::state() const
{
    util::CheckedLockGuard lock(m_mutex);
    if (!m_app)
        return SyncUser::State::Removed;
    return m_data.access_token ? SyncUser::State::LoggedIn : SyncUser::State::LoggedOut;
}

bool User::is_anonymous() const
{
    util::CheckedLockGuard lock(m_mutex);
    return do_is_anonymous();
}

bool User::do_is_anonymous() const
{
    return m_data.access_token && m_data.identities.size() == 1 &&
           m_data.identities[0].provider_type == app::IdentityProviderAnonymous;
}

std::string User::device_id() const
{
    util::CheckedLockGuard lock(m_mutex);
    return m_data.device_id;
}

bool User::has_device_id() const
{
    // The server will sometimes send us an all-zero device ID as a way to
    // explicitly signal that it did not generate a device ID for this login.
    util::CheckedLockGuard lock(m_mutex);
    return !m_data.device_id.empty() && m_data.device_id != "000000000000000000000000";
}

UserProfile User::user_profile() const
{
    util::CheckedLockGuard lock(m_mutex);
    return m_data.profile;
}

std::vector<UserIdentity> User::identities() const
{
    util::CheckedLockGuard lock(m_mutex);
    return m_data.identities;
}

std::optional<bson::BsonDocument> User::custom_data() const
{
    util::CheckedLockGuard lock(m_mutex);
    return m_data.access_token.user_data;
}

std::shared_ptr<App> User::app() const
{
    util::CheckedLockGuard lock(m_mutex);
    return m_app;
}

SyncManager* User::sync_manager()
{
    util::CheckedLockGuard lock(m_mutex);
    return m_app ? m_app->sync_manager().get() : nullptr;
}

app::MongoClient User::mongo_client(const std::string& service_name)
{
    util::CheckedLockGuard lock(m_mutex);
    return app::MongoClient(shared_from_this(), m_app->app_service_client(), service_name);
}

bool User::access_token_refresh_required() const
{
    using namespace std::chrono;
    constexpr size_t buffer_seconds = 5; // arbitrary
    util::CheckedLockGuard lock(m_mutex);
    const auto now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count() +
                     m_seconds_to_adjust_time_for_testing.load(std::memory_order_relaxed);
    const auto threshold = now - buffer_seconds;
    return m_data.access_token && m_data.access_token.expires_at < static_cast<int64_t>(threshold);
}

void User::log_out()
{
    if (auto app = this->app()) {
        app->log_out(shared_from_this(), nullptr);
    }
}

void User::detach_and_tear_down()
{
    std::shared_ptr<App> app;
    {
        util::CheckedLockGuard lk(m_mutex);
        m_data.access_token.token.clear();
        m_data.refresh_token.token.clear();
        app = std::exchange(m_app, nullptr);
    }

    if (app) {
        app->sync_manager()->update_sessions_for(*this, SyncUser::State::LoggedIn, SyncUser::State::Removed, {});
        app->unregister_sync_user(*this);
    }
}

void User::update_data_for_testing(util::FunctionRef<void(UserData&)> fn)
{
    UserData data;
    {
        util::CheckedLockGuard lock(m_mutex);
        data = m_data;
    }
    fn(data);
    update_backing_data(std::move(data));
}

void User::update_backing_data(std::optional<UserData>&& data)
{
    if (!data) {
        detach_and_tear_down();
        emit_change_to_subscribers(*this);
        return;
    }

    std::string new_token;
    SyncUser::State old_state;
    SyncUser::State new_state = data->access_token ? SyncUser::State::LoggedIn : SyncUser::State::LoggedOut;
    std::shared_ptr<SyncManager> sync_manager;
    {
        util::CheckedLockGuard lock(m_mutex);
        if (!m_app) {
            return; // is already detached
        }
        sync_manager = m_app->sync_manager();
        old_state = m_data.access_token ? SyncUser::State::LoggedIn : SyncUser::State::LoggedOut;
        if (new_state == SyncUser::State::LoggedIn && data->access_token != m_data.access_token)
            new_token = data->access_token.token;
        m_data = std::move(*data);
    }

    sync_manager->update_sessions_for(*this, old_state, new_state, new_token);
    emit_change_to_subscribers(*this);
}

void User::request_log_out()
{
    if (auto app = this->app()) {
        auto new_state = is_anonymous() ? SyncUser::State::Removed : SyncUser::State::LoggedOut;
        app->m_metadata_store->log_out(m_user_id, new_state);
        update_backing_data(app->m_metadata_store->get_user(m_user_id));
    }
}

void User::request_refresh_location(util::UniqueFunction<void(util::Optional<app::AppError>)>&& completion)
{
    if (auto app = this->app()) {
        bool update_location = true;
        app->refresh_access_token(shared_from_this(), update_location, std::move(completion));
    }
}

void User::request_access_token(util::UniqueFunction<void(util::Optional<app::AppError>)>&& completion)
{
    if (auto app = this->app()) {
        bool update_location = false;
        app->refresh_access_token(shared_from_this(), update_location, std::move(completion));
    }
}

void User::track_realm(std::string_view path)
{
    if (auto app = this->app()) {
        app->m_metadata_store->add_realm_path(m_user_id, path);
    }
}

std::string User::create_file_action(SyncFileAction action, std::string_view original_path,
                                     std::optional<std::string> requested_recovery_dir)
{
    if (auto app = this->app()) {
        std::string recovery_path;
        if (action == SyncFileAction::BackUpThenDeleteRealm) {
            recovery_path =
                util::reserve_unique_file_name(app->m_file_manager->recovery_directory_path(requested_recovery_dir),
                                               util::create_timestamped_template("recovered_realm"));
        }
        app->m_metadata_store->create_file_action(action, original_path, recovery_path);
        return recovery_path;
    }
    return "";
}

void User::refresh_custom_data(util::UniqueFunction<void(util::Optional<app::AppError>)> completion_block)
    REQUIRES(!m_mutex)
{
    refresh_custom_data(false, std::move(completion_block));
}

void User::refresh_custom_data(bool update_location,
                               util::UniqueFunction<void(util::Optional<app::AppError>)> completion_block)
{
    if (auto app = this->app()) {
        app->refresh_custom_data(shared_from_this(), update_location, std::move(completion_block));
        return;
    }
    completion_block(app::AppError(
        ErrorCodes::ClientUserNotFound,
        util::format("Cannot initiate a refresh on user '%1' because the user has been removed", m_user_id)));
}

std::string User::path_for_realm(const SyncConfig& config, std::optional<std::string> custom_file_name) const
{
    if (auto app = this->app()) {
        return app->m_file_manager->path_for_realm(config, std::move(custom_file_name));
    }
    return "";
}
} // namespace realm::app

namespace std {
size_t hash<realm::app::UserIdentity>::operator()(const realm::app::UserIdentity& k) const
{
    return ((hash<string>()(k.id) ^ (hash<string>()(k.provider_type) << 1)) >> 1);
}
} // namespace std
