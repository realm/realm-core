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

#include <realm/object-store/sync/sync_manager.hpp>

#include <realm/object-store/sync/impl/sync_client.hpp>
#include <realm/object-store/sync/impl/sync_file.hpp>
#include <realm/object-store/sync/impl/app_metadata.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/util/uuid.hpp>

#include <realm/util/sha_crypto.hpp>
#include <realm/util/hex_dump.hpp>

#include <realm/exceptions.hpp>

using namespace realm;
using namespace realm::_impl;

SyncClientTimeouts::SyncClientTimeouts()
    : connect_timeout(sync::default_connect_timeout)
    , connection_linger_time(sync::default_connection_linger_time)
    , ping_keepalive_period(sync::default_ping_keepalive_period)
    , pong_keepalive_timeout(sync::default_pong_keepalive_timeout)
    , fast_reconnect_limit(sync::default_fast_reconnect_limit)
{
}

std::shared_ptr<SyncManager> SyncManager::create(const SyncClientConfig& config)
{
    return std::make_shared<SyncManager>(Private(), config);
}

SyncManager::SyncManager(Private, const SyncClientConfig& config)
    : m_config(config)
{
    // create the initial logger - if the logger_factory is updated later, a new
    // logger will be created at that time.
    do_make_logger();
}

void SyncManager::tear_down_for_testing()
{
    close_all_sessions();

    {
        util::CheckedLockGuard lock(m_mutex);
        // Stop the client. This will abort any uploads that inactive sessions are waiting for.
        if (m_sync_client)
            m_sync_client->stop();
    }

    {
        util::CheckedUniqueLock lock(m_session_mutex);

        bool no_sessions = !do_has_existing_sessions();
        // There's a race between this function and sessions tearing themselves down waiting for m_session_mutex.
        // So we give up to a 5 second grace period for any sessions being torn down to unregister themselves.
        auto since_poll_start = [start = std::chrono::steady_clock::now()] {
            return std::chrono::steady_clock::now() - start;
        };
        for (; !no_sessions && since_poll_start() < std::chrono::seconds(5);
             no_sessions = !do_has_existing_sessions()) {
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            lock.lock();
        }
        // Callers of `SyncManager::tear_down_for_testing` should ensure there are no existing sessions
        // prior to calling `tear_down_for_testing`.
        if (!no_sessions) {
            util::CheckedLockGuard lock(m_mutex);
            for (auto session : m_sessions) {
                m_logger_ptr->error("open session at path '%1'", session.first);
            }
        }
        REALM_ASSERT_RELEASE(no_sessions);

        // Destroy any inactive sessions.
        // FIXME: We shouldn't have any inactive sessions at this point! Sessions are expected to
        // remain inactive until their final upload completes, at which point they are unregistered
        // and destroyed. Our call to `sync::Client::stop` above aborts all uploads, so all sessions
        // should have already been destroyed.
        m_sessions.clear();
    }

    {
        util::CheckedLockGuard lock(m_mutex);
        // Destroy the client now that we have no remaining sessions.
        m_sync_client.reset();
        m_logger_ptr.reset();
    }
}

void SyncManager::set_log_level(util::Logger::Level level) noexcept
{
    util::CheckedLockGuard lock(m_mutex);
    m_config.log_level = level;
    // Update the level threshold in the already created logger
    if (m_logger_ptr) {
        m_logger_ptr->set_level_threshold(level);
    }
}

void SyncManager::set_logger_factory(SyncClientConfig::LoggerFactory factory)
{
    util::CheckedLockGuard lock(m_mutex);
    m_config.logger_factory = std::move(factory);

    if (m_sync_client)
        throw LogicError(ErrorCodes::IllegalOperation,
                         "Cannot set the logger factory after creating the sync client");

    // Create a new logger using the new factory
    do_make_logger();
}

void SyncManager::do_make_logger()
{
    if (m_config.logger_factory) {
        m_logger_ptr = m_config.logger_factory(m_config.log_level);
    }
    else {
        m_logger_ptr = util::Logger::get_default_logger();
    }
    REALM_ASSERT(m_logger_ptr);
}

const std::shared_ptr<util::Logger>& SyncManager::get_logger() const
{
    util::CheckedLockGuard lock(m_mutex);
    return m_logger_ptr;
}

void SyncManager::set_user_agent(std::string user_agent)
{
    util::CheckedLockGuard lock(m_mutex);
    m_config.user_agent_application_info = std::move(user_agent);
}

void SyncManager::set_timeouts(SyncClientTimeouts timeouts)
{
    util::CheckedLockGuard lock(m_mutex);
    m_config.timeouts = timeouts;
}

void SyncManager::reconnect() const
{
    util::CheckedLockGuard lock(m_session_mutex);
    for (auto& it : m_sessions) {
        it.second->handle_reconnect();
    }
}

util::Logger::Level SyncManager::log_level() const noexcept
{
    util::CheckedLockGuard lock(m_mutex);
    return m_config.log_level;
}

SyncManager::~SyncManager() NO_THREAD_SAFETY_ANALYSIS
{
    // Grab the current sessions under a lock so we can shut them down. We have to
    // release the lock before calling them as shutdown_and_wait() will call
    // back into us.
    decltype(m_sessions) current_sessions;
    {
        util::CheckedLockGuard lk(m_session_mutex);
        m_sessions.swap(current_sessions);
    }

    for (auto& [_, session] : current_sessions) {
        session->detach_from_sync_manager();
    }

    {
        util::CheckedLockGuard lk(m_mutex);
        // Stop the client. This will abort any uploads that inactive sessions are waiting for.
        if (m_sync_client)
            m_sync_client->stop();
    }
}

std::vector<std::shared_ptr<SyncSession>> SyncManager::get_all_sessions() const
{
    util::CheckedLockGuard lock(m_session_mutex);
    std::vector<std::shared_ptr<SyncSession>> sessions;
    for (auto& [_, session] : m_sessions) {
        if (auto external_reference = session->existing_external_reference())
            sessions.push_back(std::move(external_reference));
    }
    return sessions;
}

std::vector<std::shared_ptr<SyncSession>> SyncManager::get_all_sessions_for(const SyncUser& user) const
{
    util::CheckedLockGuard lock(m_session_mutex);
    std::vector<std::shared_ptr<SyncSession>> sessions;
    for (auto& [_, session] : m_sessions) {
        if (session->user().get() == &user) {
            if (auto external_reference = session->existing_external_reference())
                sessions.push_back(std::move(external_reference));
        }
    }
    return sessions;
}

std::shared_ptr<SyncSession> SyncManager::get_existing_active_session(const std::string& path) const
{
    util::CheckedLockGuard lock(m_session_mutex);
    if (auto session = get_existing_session_locked(path)) {
        if (auto external_reference = session->existing_external_reference())
            return external_reference;
    }
    return nullptr;
}

std::shared_ptr<SyncSession> SyncManager::get_existing_session_locked(const std::string& path) const
{
    auto it = m_sessions.find(path);
    return it == m_sessions.end() ? nullptr : it->second;
}

std::shared_ptr<SyncSession> SyncManager::get_existing_session(const std::string& path) const
{
    util::CheckedLockGuard lock(m_session_mutex);
    if (auto session = get_existing_session_locked(path))
        return session->external_reference();

    return nullptr;
}

std::shared_ptr<SyncSession> SyncManager::get_session(std::shared_ptr<DB> db, const RealmConfig& config)
{
    auto& client = get_sync_client(); // Throws
#ifndef __EMSCRIPTEN__
    auto path = db->get_path();
    REALM_ASSERT_EX(path == config.path, path, config.path);
#else
    auto path = config.path;
#endif
    REALM_ASSERT(config.sync_config);

    util::CheckedUniqueLock lock(m_session_mutex);
    if (auto session = get_existing_session_locked(path)) {
        return session->external_reference();
    }

    auto shared_session = SyncSession::create(client, std::move(db), config, this);
    m_sessions[path] = shared_session;

    // Create the external reference immediately to ensure that the session will become
    // inactive if an exception is thrown in the following code.
    return shared_session->external_reference();
}

bool SyncManager::has_existing_sessions()
{
    util::CheckedLockGuard lock(m_session_mutex);
    return do_has_existing_sessions();
}

bool SyncManager::do_has_existing_sessions()
{
    return std::any_of(m_sessions.begin(), m_sessions.end(), [](auto& element) {
        return element.second->existing_external_reference();
    });
}

void SyncManager::wait_for_sessions_to_terminate()
{
    auto& client = get_sync_client(); // Throws
    client.wait_for_session_terminations();
}

void SyncManager::unregister_session(const std::string& path)
{
    util::CheckedUniqueLock lock(m_session_mutex);
    auto it = m_sessions.find(path);
    if (it == m_sessions.end()) {
        // The session may already be unregistered. This always happens in the
        // SyncManager destructor, and can also happen due to multiple threads
        // tearing things down at once.
        return;
    }

    // Sync session teardown calls this function, so we need to be careful with
    // locking here. We need to unlock `m_session_mutex` before we do anything
    // which could result in a re-entrant call or we'll deadlock, which in this
    // function means unlocking before we destroy a `shared_ptr<SyncSession>`
    // (either the external reference or internal reference versions).
    // The external reference version will only be the final reference if
    // another thread drops a reference while we're in this function.
    // Dropping the final internal reference does not appear to ever actually
    // result in a recursive call to this function at the time this comment was
    // written, but releasing the lock in that case as well is still safer.

    if (auto existing_session = it->second->existing_external_reference()) {
        // We got here because the session entered the inactive state, but
        // there's still someone referencing it so we should leave it be. This
        // can happen if the user was logged out, or if all Realms using the
        // session were destroyed but the SDK user is holding onto the session.

        // Explicit unlock so that `existing_session`'s destructor runs after
        // the unlock for the reasons noted above
        lock.unlock();
        return;
    }

    // Remove the session from the map while holding the lock, but then defer
    // destroying it until after we unlock the mutex for the reasons noted above.
    auto session = m_sessions.extract(it);
    lock.unlock();
}

void SyncManager::update_sessions_for(SyncUser& user, SyncUser::State old_state, SyncUser::State new_state,
                                      std::string_view new_access_token)
{
    bool should_revive = old_state != SyncUser::State::LoggedIn && new_state == SyncUser::State::LoggedIn;
    bool should_stop = old_state == SyncUser::State::LoggedIn && new_state != SyncUser::State::LoggedIn;

    auto sessions = get_all_sessions_for(user);
    if (new_access_token.size()) {
        for (auto& session : sessions) {
            session->update_access_token(new_access_token);
        }
    }
    else if (should_revive) {
        for (auto& session : sessions) {
            session->revive_if_needed();
        }
    }
    else if (should_stop) {
        for (auto& session : sessions) {
            session->force_close();
        }
    }
}

void SyncManager::set_session_multiplexing(bool allowed)
{
    util::CheckedLockGuard lock(m_mutex);
    if (m_config.multiplex_sessions == allowed)
        return; // Already enabled, we can ignore

    if (m_sync_client)
        throw LogicError(ErrorCodes::IllegalOperation,
                         "Cannot enable session multiplexing after creating the sync client");

    m_config.multiplex_sessions = allowed;
}

SyncClient& SyncManager::get_sync_client() const
{
    util::CheckedLockGuard lock(m_mutex);
    if (!m_sync_client)
        m_sync_client = create_sync_client(); // Throws
    return *m_sync_client;
}

std::unique_ptr<SyncClient> SyncManager::create_sync_client() const
{
    REALM_ASSERT(m_logger_ptr);
    return std::make_unique<SyncClient>(m_logger_ptr, m_config, weak_from_this());
}

void SyncManager::close_all_sessions()
{
    // force_close() will call unregister_session(), which requires m_session_mutex,
    // so we need to iterate over them without holding the lock.
    decltype(m_sessions) sessions;
    {
        util::CheckedLockGuard lk(m_session_mutex);
        m_sessions.swap(sessions);
    }

    for (auto& [_, session] : sessions) {
        session->force_close();
    }

    get_sync_client().wait_for_session_terminations();
}

void SyncManager::set_sync_route(std::string sync_route, bool verified)
{
    REALM_ASSERT(!sync_route.empty()); // Cannot be set to empty string
    {
        util::CheckedLockGuard lock(m_mutex);
        m_sync_route = sync_route;
        m_sync_route_verified = verified;
    }
}

void SyncManager::restart_all_sessions()
{
    // Restart the sessions that are currently active
    auto sessions = get_all_sessions();
    for (auto& session : sessions) {
        session->restart_session();
    }
}

void SyncManager::OnlyForTesting::voluntary_disconnect_all_connections(SyncManager& mgr)
{
    mgr.get_sync_client().voluntary_disconnect_all_connections();
}
