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

#include <realm/object-store/sync/sync_user.hpp>

#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/sync/impl/sync_metadata.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_session.hpp>

#include <realm/util/base64.hpp>

namespace realm {

static std::string base64_decode(const std::string& in)
{
    std::string out;
    out.resize(util::base64_decoded_size(in.size()));
    util::base64_decode(in, &out[0], out.size());
    return out;
}

static std::vector<std::string> split_token(const std::string& jwt)
{
    constexpr static char delimiter = '.';

    std::vector<std::string> parts;
    size_t pos = 0, start_from = 0;

    while ((pos = jwt.find(delimiter, start_from)) != std::string::npos) {
        parts.push_back(jwt.substr(start_from, pos - start_from));
        start_from = pos + 1;
    }

    parts.push_back(jwt.substr(start_from));

    if (parts.size() != 3) {
        throw app::AppError(ErrorCodes::BadToken, "jwt missing parts");
    }

    return parts;
}

RealmJWT::RealmJWT(std::string_view token)
    : token(token)
{
    auto parts = split_token(this->token);

    auto json_str = base64_decode(parts[1]);
    auto json = static_cast<bson::BsonDocument>(bson::parse(json_str));

    this->expires_at = static_cast<int64_t>(json["exp"]);
    this->issued_at = static_cast<int64_t>(json["iat"]);

    if (json.find("user_data") != json.end()) {
        this->user_data = static_cast<bson::BsonDocument>(json["user_data"]);
    }
}

RealmJWT::RealmJWT(StringData token)
    : RealmJWT(std::string_view(token))
{
}

RealmJWT::RealmJWT(const std::string& token)
    : RealmJWT(std::string_view(token))
{
}


SyncUserIdentity::SyncUserIdentity(const std::string& id, const std::string& provider_type)
    : id(id)
    , provider_type(provider_type)
{
}

SyncUserContextFactory SyncUser::s_binding_context_factory;
std::mutex SyncUser::s_binding_context_factory_mutex;

static std::shared_ptr<app::App> lock_or_throw(std::weak_ptr<app::App> app)
{
    if (auto locked = app.lock()) {
        return locked;
    }
    throw RuntimeError(ErrorCodes::RuntimeError, "Invalid operation on user which has become detached.");
}

SyncUser::SyncUser(Private, std::string_view refresh_token, std::string_view id, std::string_view access_token,
                   std::string_view device_id, std::shared_ptr<app::App> app)
    : m_state(State::LoggedIn)
    , m_user_id(id)
    , m_refresh_token(RealmJWT(refresh_token))
    , m_access_token(RealmJWT(access_token))
    , m_device_id(device_id)
    , m_app(std::move(app))
{
    REALM_ASSERT(!access_token.empty() && !refresh_token.empty());
    {
        std::lock_guard lock(s_binding_context_factory_mutex);
        if (s_binding_context_factory) {
            m_binding_context = s_binding_context_factory();
        }
    }

    lock_or_throw(m_app)->backing_store()->perform_metadata_update(
        [&](const auto& manager) NO_THREAD_SAFETY_ANALYSIS {
            auto metadata = manager.get_or_make_user_metadata(m_user_id);
            metadata->set_state_and_tokens(State::LoggedIn, m_access_token.token, m_refresh_token.token);
            metadata->set_device_id(m_device_id);
            m_legacy_identities = metadata->legacy_identities();
            this->m_user_profile = metadata->profile();
        });
}

SyncUser::SyncUser(Private, const SyncUserMetadata& data, std::shared_ptr<app::App> app)
    : m_state(data.state())
    , m_legacy_identities(data.legacy_identities())
    , m_user_id(data.user_id())
    , m_refresh_token(RealmJWT(data.refresh_token()))
    , m_access_token(RealmJWT(data.access_token()))
    , m_user_identities(data.identities())
    , m_user_profile(data.profile())
    , m_device_id(data.device_id())
    , m_app(std::move(app))
{
    // Check for inconsistent state in the metadata Realm. This shouldn't happen,
    // but previous versions could sometimes mark a user as logged in with an
    // empty refresh token.
    if (m_state == State::LoggedIn && (m_refresh_token.token.empty() || m_access_token.token.empty())) {
        m_state = State::LoggedOut;
        m_refresh_token = {};
        m_access_token = {};
    }

    {
        std::lock_guard lock(s_binding_context_factory_mutex);
        if (s_binding_context_factory) {
            m_binding_context = s_binding_context_factory();
        }
    }
}

std::weak_ptr<app::App> SyncUser::app() const
{
    util::CheckedLockGuard lk(m_mutex);
    if (m_state == State::Removed) {
        throw app::AppError(
            ErrorCodes::ClientUserNotFound,
            util::format("Cannot start a sync session for user '%1' because this user has been removed.", m_user_id));
    }
    return m_app;
}

void SyncUser::detach_from_backing_store()
{
    util::CheckedLockGuard lk(m_mutex);
    m_state = SyncUser::State::Removed;
    m_app.reset();
}

void SyncUser::log_in(std::string_view access_token, std::string_view refresh_token)
{
    REALM_ASSERT(!access_token.empty());
    REALM_ASSERT(!refresh_token.empty());
    {
        util::CheckedLockGuard lock1(m_mutex);
        util::CheckedLockGuard lock2(m_tokens_mutex);
        m_state = State::LoggedIn;
        m_access_token = RealmJWT(access_token);
        m_refresh_token = RealmJWT(refresh_token);

        lock_or_throw(m_app)->backing_store()->perform_metadata_update([&](const auto& manager) {
            auto metadata = manager.get_or_make_user_metadata(m_user_id);
            metadata->set_state_and_tokens(State::LoggedIn, access_token, refresh_token);
        });
    }
#if REALM_ENABLE_SYNC
    // (Re)activate all sessions associated with this user.
    // Note that we do this after releasing the lock, since the session may
    // need to access protected User state in the process of binding itself.
    if (auto manager = lock_or_throw(m_app)->sync_manager()) {
        for (auto session : manager->get_all_sessions_for(*this)) {
            session->revive_if_needed();
        }
    }
#endif

    emit_change_to_subscribers(*this);
}

void SyncUser::invalidate()
{
    {
        util::CheckedLockGuard lock1(m_mutex);
        util::CheckedLockGuard lock2(m_tokens_mutex);
        m_state = State::Removed;
        m_access_token = {};
        m_refresh_token = {};

        lock_or_throw(m_app)->backing_store()->perform_metadata_update([&](const auto& manager) {
            auto metadata = manager.get_or_make_user_metadata(m_user_id);
            metadata->set_state_and_tokens(State::Removed, "", "");
        });
    }
    emit_change_to_subscribers(*this);
}

void SyncUser::update_access_token(std::string&& token)
{
    {
        util::CheckedLockGuard lock(m_mutex);
        if (m_state != State::LoggedIn)
            return;

        util::CheckedLockGuard lock2(m_tokens_mutex);
        m_access_token = RealmJWT(std::move(token));
        lock_or_throw(m_app)->backing_store()->perform_metadata_update(
            [&, raw_access_token = m_access_token.token](const auto& manager) {
                auto metadata = manager.get_or_make_user_metadata(m_user_id);
                metadata->set_access_token(raw_access_token);
            });
    }

    emit_change_to_subscribers(*this);
}

std::vector<SyncUserIdentity> SyncUser::identities() const
{
    util::CheckedLockGuard lock(m_mutex);
    return m_user_identities;
}

void SyncUser::log_out()
{
    // We'll extend the lifetime of the app while holding m_mutex so that we
    // know it's safe to call methods on it after we've been marked as logged out.
    std::shared_ptr<app::App> app;
    {
        util::CheckedLockGuard lock(m_mutex);
        app = lock_or_throw(m_app);
        bool is_anonymous = false;
        {
            util::CheckedLockGuard lock2(m_tokens_mutex);
            if (m_state != State::LoggedIn) {
                return;
            }
            is_anonymous = do_is_anonymous();
            m_state = State::LoggedOut;
            m_access_token = RealmJWT{};
            m_refresh_token = RealmJWT{};
        }

        if (is_anonymous) {
            // An Anonymous user can not log back in.
            // Mark the user as 'dead' in the persisted metadata Realm.
            m_state = State::Removed;
            app->backing_store()->perform_metadata_update([&](const auto& manager) {
                auto metadata = manager.get_or_make_user_metadata(m_user_id, false);
                if (metadata)
                    metadata->remove();
            });
        }
        else {
            app->backing_store()->perform_metadata_update([&](const auto& manager) {
                auto metadata = manager.get_or_make_user_metadata(m_user_id);
                metadata->set_state_and_tokens(State::LoggedOut, "", "");
            });
        }
    }
#if REALM_ENABLE_SYNC
    // Close all sessions that belong to this user
    if (auto sync_manager = app->sync_manager()) {
        for (auto session : sync_manager->get_all_sessions_for(*this)) {
            session->force_close();
        }
    }
#endif

    app->backing_store()->log_out_user(*this);
    emit_change_to_subscribers(*this);
}

bool SyncUser::is_logged_in() const
{
    util::CheckedLockGuard lock(m_mutex);
    return m_state == State::LoggedIn;
}

bool SyncUser::is_anonymous() const
{
    util::CheckedLockGuard lock(m_mutex);
    util::CheckedLockGuard lock2(m_tokens_mutex);
    return do_is_anonymous();
}

bool SyncUser::do_is_anonymous() const
{
    return m_state == State::LoggedIn && m_user_identities.size() == 1 &&
           m_user_identities[0].provider_type == app::IdentityProviderAnonymous;
}

std::string SyncUser::refresh_token() const
{
    util::CheckedLockGuard lock(m_tokens_mutex);
    return m_refresh_token.token;
}

std::string SyncUser::access_token() const
{
    util::CheckedLockGuard lock(m_tokens_mutex);
    return m_access_token.token;
}

std::string SyncUser::device_id() const
{
    util::CheckedLockGuard lock(m_mutex);
    return m_device_id;
}

bool SyncUser::has_device_id() const
{
    util::CheckedLockGuard lock(m_mutex);
    return !m_device_id.empty() && m_device_id != "000000000000000000000000";
}

SyncUser::State SyncUser::state() const
{
    util::CheckedLockGuard lock(m_mutex);
    return m_state;
}

SyncUserProfile SyncUser::user_profile() const
{
    util::CheckedLockGuard lock(m_mutex);
    return m_user_profile;
}

util::Optional<bson::BsonDocument> SyncUser::custom_data() const
{
    util::CheckedLockGuard lock(m_tokens_mutex);
    return m_access_token.user_data;
}

void SyncUser::update_user_profile(std::vector<SyncUserIdentity> identities, SyncUserProfile profile)
{
    util::CheckedLockGuard lock(m_mutex);
    if (m_state == SyncUser::State::Removed) {
        return;
    }

    m_user_identities = std::move(identities);
    m_user_profile = std::move(profile);

    lock_or_throw(m_app)->backing_store()->perform_metadata_update(
        [&](const auto& manager) NO_THREAD_SAFETY_ANALYSIS {
            auto metadata = manager.get_or_make_user_metadata(m_user_id);
            metadata->set_identities(m_user_identities);
            metadata->set_user_profile(m_user_profile);
        });
}

app::MongoClient SyncUser::mongo_client(const std::string& service_name)
{
    util::CheckedLockGuard lk(m_mutex);
    REALM_ASSERT(m_state == SyncUser::State::LoggedIn);
    return app::MongoClient(shared_from_this(), lock_or_throw(m_app), service_name);
}

void SyncUser::set_binding_context_factory(SyncUserContextFactory factory)
{
    std::lock_guard<std::mutex> lock(s_binding_context_factory_mutex);
    s_binding_context_factory = std::move(factory);
}

void SyncUser::refresh_custom_data(util::UniqueFunction<void(util::Optional<app::AppError>)> completion_block)
    REQUIRES(!m_mutex)
{
    refresh_custom_data(false, std::move(completion_block));
}

void SyncUser::refresh_custom_data(bool update_location,
                                   util::UniqueFunction<void(util::Optional<app::AppError>)> completion_block)
{
    std::shared_ptr<app::App> app;
    std::shared_ptr<SyncUser> user;
    {
        util::CheckedLockGuard lk(m_mutex);
        if (m_state != SyncUser::State::Removed) {
            user = shared_from_this();
        }
        app = m_app.lock();
    }
    if (!user) {
        completion_block(app::AppError(
            ErrorCodes::ClientUserNotFound,
            util::format("Cannot initiate a refresh on user '%1' because the user has been removed", m_user_id)));
    }
    else if (!app) {
        completion_block(app::AppError(
            ErrorCodes::ClientAppDeallocated,
            util::format("Cannot initiate a refresh on user '%1' because the app has been deallocated", m_user_id)));
    }
    else {
        std::weak_ptr<SyncUser> weak_user = user->weak_from_this();
        app->refresh_custom_data(user, update_location,
                                 [completion_block = std::move(completion_block), weak_user](auto error) {
                                     if (auto strong = weak_user.lock()) {
                                         strong->emit_change_to_subscribers(*strong);
                                     }
                                     completion_block(error);
                                 });
    }
}

bool SyncUser::access_token_refresh_required() const
{
    using namespace std::chrono;
    constexpr size_t buffer_seconds = 5; // arbitrary
    util::CheckedLockGuard lock(m_tokens_mutex);
    const auto now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count() +
                     m_seconds_to_adjust_time_for_testing.load(std::memory_order_relaxed);
    const auto threshold = now - buffer_seconds;
    return !m_access_token.token.empty() && m_access_token.expires_at < static_cast<int64_t>(threshold);
}

} // namespace realm

namespace std {
size_t hash<realm::SyncUserIdentity>::operator()(const realm::SyncUserIdentity& k) const
{
    return ((hash<string>()(k.id) ^ (hash<string>()(k.provider_type) << 1)) >> 1);
}
} // namespace std
