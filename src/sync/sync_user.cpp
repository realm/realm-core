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

#include "sync/sync_user.hpp"

#include "sync/app_credentials.hpp"
#include "sync/generic_network_transport.hpp"
#include "sync/impl/sync_metadata.hpp"
#include "sync/remote_mongo_client.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_session.hpp"

#include <realm/util/base64.hpp>

namespace realm {

static std::string base64_decode(const std::string &in) {
    std::string out;
    out.resize(util::base64_decoded_size(in.size()));
    util::base64_decode(in, &out[0], out.size());
    return out;
}

static std::vector<std::string> split_token(const std::string& jwt) {
    constexpr static char delimiter = '.';

    std::vector<std::string> parts;
    size_t pos = 0, start_from = 0;

    while ((pos = jwt.find(delimiter, start_from)) != std::string::npos) {
        parts.push_back(jwt.substr(start_from, pos - start_from));
        start_from = pos + 1;
    }

    parts.push_back(jwt.substr(start_from));

    if (parts.size() != 3) {
        throw app::AppError(make_error_code(app::JSONErrorCode::bad_token), "jwt missing parts");
    }

    return parts;
}

RealmJWT::RealmJWT(std::string&& token)
{
    this->token = std::move(token);

    auto parts = split_token(this->token);

    auto json_str = base64_decode(parts[1]);
    auto json = static_cast<bson::BsonDocument>(bson::parse(json_str));

    this->expires_at = static_cast<int64_t>(json["exp"]);
    this->issued_at = static_cast<int64_t>(json["iat"]);

    if (json.find("user_data") != json.end()) {
        this->user_data = static_cast<bson::BsonDocument>(json["user_data"]);
    }
}

SyncUserProfile::SyncUserProfile(util::Optional<std::string> name,
                                 util::Optional<std::string> email,
                                 util::Optional<std::string> picture_url,
                                 util::Optional<std::string> first_name,
                                 util::Optional<std::string> last_name,
                                 util::Optional<std::string> gender,
                                 util::Optional<std::string> birthday,
                                 util::Optional<std::string> min_age,
                                 util::Optional<std::string> max_age)
: name(std::move(name))
, email(std::move(email))
, picture_url(std::move(picture_url))
, first_name(std::move(first_name))
, last_name(std::move(last_name))
, gender(std::move(gender))
, birthday(std::move(birthday))
, min_age(std::move(min_age))
, max_age(std::move(max_age))
{
}

SyncUserIdentity::SyncUserIdentity(const std::string& id, const std::string& provider_type)
: id(id)
, provider_type(provider_type)
{
}

SyncUserContextFactory SyncUser::s_binding_context_factory;
std::mutex SyncUser::s_binding_context_factory_mutex;

SyncUser::SyncUser(std::string refresh_token,
                   const std::string identity,
                   const std::string provider_type,
                   std::string access_token,
                   SyncUser::State state,
                   const std::string device_id,
                   std::shared_ptr<SyncManager> sync_manager)
: m_state(state)
, m_provider_type(provider_type)
, m_refresh_token(RealmJWT(std::move(refresh_token)))
, m_identity(std::move(identity))
, m_access_token(RealmJWT(std::move(access_token)))
, m_device_id(device_id)
, m_sync_manager(sync_manager)
{
    {
        std::lock_guard<std::mutex> lock(s_binding_context_factory_mutex);
        if (s_binding_context_factory) {
            m_binding_context = s_binding_context_factory();
        }
    }

    bool updated = m_sync_manager->perform_metadata_update([=](const auto& manager) {
        auto metadata = manager.get_or_make_user_metadata(m_identity, m_provider_type);
        metadata->set_refresh_token(m_refresh_token.token);
        metadata->set_access_token(m_access_token.token);
        metadata->set_device_id(m_device_id);
        m_local_identity = metadata->local_uuid();
    });
    if (!updated)
        m_local_identity = m_identity;
}

std::vector<std::shared_ptr<SyncSession>> SyncUser::all_sessions()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    std::vector<std::shared_ptr<SyncSession>> sessions;
    if (m_state == State::Removed) {
        return sessions;
    }
    for (auto it = m_sessions.begin(); it != m_sessions.end();) {
        if (auto ptr_to_session = it->second.lock()) {
            sessions.emplace_back(std::move(ptr_to_session));
            it++;
            continue;
        }
        // This session is bad, destroy it.
        it = m_sessions.erase(it);
    }
    return sessions;
}

std::shared_ptr<SyncSession> SyncUser::session_for_on_disk_path(const std::string& path)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_state == State::Removed) {
        return nullptr;
    }
    auto it = m_sessions.find(path);
    if (it == m_sessions.end()) {
        return nullptr;
    }
    auto locked = it->second.lock();
    if (!locked) {
        // Remove the session from the map, because it has fatally errored out or the entry is invalid.
        m_sessions.erase(it);
    }
    return locked;
}

void SyncUser::update_refresh_token(std::string&& token)
{
    std::vector<std::shared_ptr<SyncSession>> sessions_to_revive;
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        switch (m_state) {
            case State::Removed:
                return;
            case State::LoggedIn:
                m_refresh_token = RealmJWT(std::move(token));
                break;
            case State::LoggedOut: {
                sessions_to_revive.reserve(m_waiting_sessions.size());
                m_refresh_token = RealmJWT(std::move(token));
                m_state = State::LoggedIn;
                for (auto& pair : m_waiting_sessions) {
                    if (auto ptr = pair.second.lock()) {
                        m_sessions[pair.first] = ptr;
                        sessions_to_revive.emplace_back(std::move(ptr));
                    }
                }
                m_waiting_sessions.clear();
                break;
            }
        }

        m_sync_manager->perform_metadata_update([=](const auto& manager) {
            auto metadata = manager.get_or_make_user_metadata(m_identity, m_provider_type);
            metadata->set_refresh_token(m_refresh_token.token);
        });
    }
    // (Re)activate all pending sessions.
    // Note that we do this after releasing the lock, since the session may
    // need to access protected User state in the process of binding itself.
    for (auto& session : sessions_to_revive) {
        session->revive_if_needed();
    }
}

void SyncUser::update_access_token(std::string&& token)
{
    std::vector<std::shared_ptr<SyncSession>> sessions_to_revive;
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        switch (m_state) {
            case State::Removed:
                return;
            case State::LoggedIn:
                m_access_token = RealmJWT(std::move(token));
                break;
            case State::LoggedOut: {
                sessions_to_revive.reserve(m_waiting_sessions.size());
                m_access_token = RealmJWT(std::move(token));
                m_state = State::LoggedIn;
                for (auto& pair : m_waiting_sessions) {
                    if (auto ptr = pair.second.lock()) {
                        m_sessions[pair.first] = ptr;
                        sessions_to_revive.emplace_back(std::move(ptr));
                    }
                }
                m_waiting_sessions.clear();
                break;
            }
        }

        m_sync_manager->perform_metadata_update([=](const auto& manager) {
            auto metadata = manager.get_or_make_user_metadata(m_identity, m_provider_type);
            metadata->set_access_token(m_access_token.token);
        });
    }

    // (Re)activate all pending sessions.
    // Note that we do this after releasing the lock, since the session may
    // need to access protected User state in the process of binding itself.
    for (auto& session : sessions_to_revive) {
        session->revive_if_needed();
    }
}

std::vector<SyncUserIdentity> SyncUser::identities() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_user_identities;
}


void SyncUser::update_identities(std::vector<SyncUserIdentity> identities)
{
    std::unique_lock<std::mutex> lock(m_mutex);

    m_user_identities = identities;

    m_sync_manager->perform_metadata_update([=](const auto& manager) {
        auto metadata = manager.get_or_make_user_metadata(m_identity, m_provider_type);
        metadata->set_identities(identities);
    });
}

void SyncUser::log_out()
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_state == State::LoggedOut) {
            return;
        }
        m_state = State::LoggedOut;
        m_access_token.token = "";
        m_refresh_token.token = "";

        m_sync_manager->perform_metadata_update([=](const auto& manager) {
            auto metadata = manager.get_or_make_user_metadata(m_identity, m_provider_type);
            metadata->set_state(State::LoggedOut);
            metadata->set_access_token("");
            metadata->set_refresh_token("");
        });
        // Move all active sessions into the waiting sessions pool. If the user is
        // logged back in, they will automatically be reactivated.
        for (auto& pair : m_sessions) {
            if (auto ptr = pair.second.lock()) {
                ptr->log_out();
                m_waiting_sessions[pair.first] = ptr;
            }
        }
        m_sessions.clear();
    }

    m_sync_manager->log_out_user(m_identity);

    // Mark the user as 'dead' in the persisted metadata Realm
    // if they were an anonymous user
    if (this->m_provider_type == app::IdentityProviderAnonymous) {
        invalidate();
        m_sync_manager->perform_metadata_update([=](const auto& manager) {
            auto metadata = manager.get_or_make_user_metadata(m_identity, m_provider_type, false);
            if (metadata)
                metadata->remove();
        });
    }
}

bool SyncUser::is_logged_in() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return !m_access_token.token.empty() && !m_refresh_token.token.empty() && m_state == State::LoggedIn;
}

void SyncUser::invalidate()
{
    set_state(SyncUser::State::Removed);
}

std::string SyncUser::refresh_token() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_refresh_token.token;
}

std::string SyncUser::access_token() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_access_token.token;
}

std::string SyncUser::device_id() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_device_id;
}

bool SyncUser::has_device_id() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return !m_device_id.empty() && m_device_id != "000000000000000000000000";
}

SyncUser::State SyncUser::state() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_state;
}

void SyncUser::set_state(SyncUser::State state)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_state = state;
    m_sync_manager->perform_metadata_update([=](const auto& manager) {
        auto metadata = manager.get_or_make_user_metadata(m_identity, m_provider_type);
        metadata->set_state(state);
    });
}

SyncUserProfile SyncUser::user_profile() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_user_profile;
}

util::Optional<bson::BsonDocument> SyncUser::custom_data() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_access_token.user_data;
}

void SyncUser::update_user_profile(const SyncUserProfile& profile)
{
    std::unique_lock<std::mutex> lock(m_mutex);

    m_user_profile = profile;

    m_sync_manager->perform_metadata_update([=](const auto& manager) {
        auto metadata = manager.get_or_make_user_metadata(m_identity, m_provider_type);
        metadata->set_user_profile(profile);
    });
}

void SyncUser::register_session(std::shared_ptr<SyncSession> session)
{
    const std::string& path = session->path();
    std::unique_lock<std::mutex> lock(m_mutex);
    switch (m_state) {
        case State::LoggedIn:
            // Immediately ask the session to come online.
            m_sessions[path] = session;
            lock.unlock();
            session->revive_if_needed();
            break;
        case State::LoggedOut:
            m_waiting_sessions[path] = session;
            break;
        case State::Removed:
            break;
    }
}

app::MongoClient SyncUser::mongo_client(const std::string& service_name)
{
    return app::MongoClient(shared_from_this(), m_sync_manager->app().lock(), service_name);
}

void SyncUser::set_binding_context_factory(SyncUserContextFactory factory)
{
    std::lock_guard<std::mutex> lock(s_binding_context_factory_mutex);
    s_binding_context_factory = std::move(factory);
}

void SyncUser::refresh_custom_data(std::function<void(util::Optional<app::AppError>)> completion_block)
{
    if (auto app = m_sync_manager->app().lock()) {
        app->refresh_custom_data(shared_from_this(), completion_block);
    } else {
        completion_block(app::AppError(app::make_client_error_code(app::ClientErrorCode::app_deallocated),
                                       "App has been deallocated"));
    }
}
} // namespace realm

namespace std {
size_t hash<realm::SyncUserIdentity>::operator()(const realm::SyncUserIdentity& k) const
{
    return ((hash<string>()(k.id) ^ (hash<string>()(k.provider_type) << 1)) >> 1);
}
}
