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

#include "sync_manager.hpp"

#include "impl/sync_client.hpp"
#include "sync_session.hpp"

#include <thread>

using namespace realm;
using namespace realm::_impl;

SyncManager& SyncManager::shared()
{
    static SyncManager& manager = *new SyncManager;
    return manager;
}

void SyncManager::set_log_level(util::Logger::Level level) noexcept
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_log_level = level;
}

void SyncManager::set_logger_factory(SyncLoggerFactory& factory) noexcept
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_logger_factory = &factory;
}

void SyncManager::set_error_handler(std::function<sync::Client::ErrorHandler> handler)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto wrapped_handler = [=](int error_code, std::string message) {
        // FIXME: If the sync team decides to route all errors through the session-level error handler, the client-level
        // error handler might go away altogether.
        switch (error_code) {
            case 100:       // Connection closed (no error)
            case 101:       // Unspecified non-critical error
                return;
            default:
                handler(error_code, message);
        }
    };
    m_error_handler = std::move(wrapped_handler);
}

void SyncManager::set_login_function(SyncLoginFunction login_function)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_login_function = std::move(login_function);
}

SyncLoginFunction& SyncManager::get_sync_login_function()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    // Precondition: binding must set a login callback before connecting any synced Realms.
    REALM_ASSERT(m_login_function);
    return m_login_function;
}

void SyncManager::set_client_should_reconnect_immediately(bool reconnect_immediately)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    using Reconnect = sync::Client::Reconnect;
    m_client_reconnect_mode = reconnect_immediately ? Reconnect::immediately : Reconnect::normal;
}

void SyncManager::set_client_should_validate_ssl(bool validate_ssl)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_client_validate_ssl = validate_ssl;
}

std::shared_ptr<SyncSession> SyncManager::get_existing_active_session(const std::string& path) const
{
    std::lock_guard<std::mutex> lock(m_session_mutex);
    return get_existing_active_session_locked(path);
}

std::shared_ptr<SyncSession> SyncManager::get_existing_active_session_locked(const std::string& path) const
{
    REALM_ASSERT(!m_session_mutex.try_lock());
    auto it = m_active_sessions.find(path);
    if (it == m_active_sessions.end()) {
        return nullptr;
    }
    if (auto session = it->second.lock()) {
        return session;
    }
    return nullptr;
}

std::shared_ptr<SyncSession> SyncManager::get_existing_dying_session_locked(const std::string& path) const
{
    REALM_ASSERT(!m_session_mutex.try_lock());
    auto it = m_dying_sessions.find(path);
    if (it == m_dying_sessions.end()) {
        return nullptr;
    }
    return it->second;
}

std::shared_ptr<SyncSession> SyncManager::get_session(const std::string& path, const SyncConfig& sync_config)
{
    auto client = get_sync_client(); // Throws

    std::lock_guard<std::mutex> lock(m_session_mutex);
    if (auto session = get_existing_active_session_locked(path)) {
        return session;
    }

    if (auto session = get_existing_dying_session_locked(path)) {
        m_dying_sessions.erase(path);
        m_active_sessions[path] = session;
        session->revive_if_needed();
        return session;
    }

    auto session_deleter = [this](SyncSession *session) { dropped_last_reference_to_session(session); };
    auto session = std::shared_ptr<SyncSession>(new SyncSession(std::move(client), path, sync_config),
                                                std::move(session_deleter));
    m_active_sessions[path] = session;
    return session;
}

void SyncManager::dropped_last_reference_to_session(SyncSession* session)
{
    std::lock_guard<std::mutex> lock(m_session_mutex);
    auto path = session->path();
    auto it = m_active_sessions.find(path);
    if (it == m_active_sessions.end()) {
        // A dying session finally kicked the bucket. Clean up after it.
        REALM_ASSERT_DEBUG(m_dying_sessions.find(path) == m_dying_sessions.end());
        delete session;
        return;
    }

    // An active session has become inactive. Move it to the dying list, and ask it to die when it is ready.
    m_active_sessions.erase(it);
    auto session_deleter = [this](SyncSession *session) { dropped_last_reference_to_session(session); };
    m_dying_sessions[path] = std::shared_ptr<SyncSession>(session, std::move(session_deleter));
    session->close();
}

void SyncManager::unregister_session(const std::string& path)
{
    std::lock_guard<std::mutex> lock(m_session_mutex);
    // FIXME: Is it true that we can only unregister sessions that were dying?
    REALM_ASSERT(m_active_sessions.find(path) == m_active_sessions.end());
    m_dying_sessions.erase(path);
}

std::shared_ptr<SyncClient> SyncManager::get_sync_client() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_sync_client)
        m_sync_client = create_sync_client(); // Throws
    return m_sync_client;
}

std::shared_ptr<SyncClient> SyncManager::create_sync_client() const
{
    REALM_ASSERT(!m_mutex.try_lock());

    std::unique_ptr<util::Logger> logger;
    if (m_logger_factory) {
        logger = m_logger_factory->make_logger(m_log_level); // Throws
    }
    else {
        auto stderr_logger = std::make_unique<util::StderrLogger>(); // Throws
        stderr_logger->set_level_threshold(m_log_level);
        logger = std::move(stderr_logger);
    }
    return std::make_shared<SyncClient>(std::move(logger),
                                        std::move(m_error_handler),
                                        m_client_reconnect_mode,
                                        m_client_validate_ssl);
}
