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

#ifndef REALM_OS_SYNC_MANAGER_HPP
#define REALM_OS_SYNC_MANAGER_HPP

#include <realm/object-store/sync/app_config.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/util/checked_mutex.hpp>

#include <unordered_map>

class TestAppSession;
class TestSyncManager;

namespace realm {

class DB;
struct SyncConfig;
struct RealmConfig;
class SyncSession;

namespace _impl {
struct SyncClient;
}

class SyncManager : public std::enable_shared_from_this<SyncManager> {
    struct Private {};

public:
    // Enables/disables using a single connection for all sync sessions for each host/port/user rather
    // than one per session.
    // This must be called before any sync sessions are created and cannot be
    // disabled afterwards.
    void set_session_multiplexing(bool allowed) REQUIRES(!m_mutex);

    // Destroys the sync manager, terminates all sessions created by it, and stops its SyncClient.
    ~SyncManager();

    // Sets the log level for the Sync Client.
    // The log level can only be set up until the point the Sync Client is
    // created (when the first Session is created) or an App operation is
    // performed (e.g. log in).
    void set_log_level(util::Logger::Level) noexcept REQUIRES(!m_mutex);
    void set_logger_factory(SyncClientConfig::LoggerFactory) REQUIRES(!m_mutex);

    // Sets the application level user agent string.
    // This should have the format specified here:
    // https://github.com/realm/realm-sync/blob/develop/src/realm/sync/client.hpp#L126 The user agent can only be set
    // up  until the  point the Sync Client is created. This happens when the first Session is created.
    void set_user_agent(std::string user_agent) REQUIRES(!m_mutex);

    // Sets client timeout settings.
    // The timeout settings can only be set up until the point the Sync Client is created.
    // This happens when the first Session is created.
    void set_timeouts(SyncClientTimeouts timeouts) REQUIRES(!m_mutex);

    /// Ask all valid sync sessions to perform whatever tasks might be necessary to
    /// re-establish connectivity with the Realm Object Server. It is presumed that
    /// the caller knows that network connectivity has been restored.
    ///
    /// Refer to `SyncSession::handle_reconnect()` to see what sort of work is done
    /// on a per-session basis.
    void reconnect() const REQUIRES(!m_session_mutex);

    util::Logger::Level log_level() const noexcept REQUIRES(!m_mutex);

    std::vector<std::shared_ptr<SyncSession>> get_all_sessions() const REQUIRES(!m_session_mutex);
    std::vector<std::shared_ptr<SyncSession>> get_all_sessions_for(const SyncUser& user) const
        REQUIRES(!m_session_mutex);
    std::shared_ptr<SyncSession> get_session(std::shared_ptr<DB> db, const RealmConfig& config)
        REQUIRES(!m_mutex, !m_session_mutex);
    std::shared_ptr<SyncSession> get_existing_session(const std::string& path) const REQUIRES(!m_session_mutex);
    std::shared_ptr<SyncSession> get_existing_active_session(const std::string& path) const
        REQUIRES(!m_session_mutex);

    // Returns `true` if the SyncManager still contains any existing sessions not yet fully cleaned up.
    // This will return true as long as there is an external reference to a session object, no matter
    // the state of that session.
    bool has_existing_sessions() REQUIRES(!m_session_mutex);

    // Blocking call that only return once all sessions have been terminated.
    // Due to the async nature of the SyncClient, even with `SyncSessionStopPolicy::Immediate`, a
    // session is not guaranteed to stop immediately when a Realm is closed. Using this method
    // makes it possible to guarantee that all sessions have, in fact, been closed.
    void wait_for_sessions_to_terminate() REQUIRES(!m_mutex);

    // DO NOT CALL OUTSIDE OF TESTING CODE.
    // Forcibly close all remaining sync sessions, stop the sync client, and
    // discard all state. The SyncManager must never be used again after this
    // function has been called (note: not after it has returned).
    void tear_down_for_testing() REQUIRES(!m_mutex, !m_session_mutex);

    // Immediately closes any open sync sessions for this sync manager
    void close_all_sessions() REQUIRES(!m_mutex, !m_session_mutex);

    // Force all the active sessions to restart
    void restart_all_sessions() REQUIRES(!m_mutex, !m_session_mutex);

    // Update all sessions for a given user following a state change for that
    // user (and optionally a new access token)
    void update_sessions_for(SyncUser& user, SyncUser::State old_state, SyncUser::State new_state,
                             std::string_view new_access_token) REQUIRES(!m_mutex, !m_session_mutex);


    // Used by App to update the sync route any time the location info has been refreshed.
    // m_sync_route starts out as a generated value based on the configured base_url when
    // the SyncManager is created by App. If this is incorrect, the websocket connection
    // will fail, resulting in an update to the access token (and the location, if it hasn't
    // been updated yet).
    void set_sync_route(std::string sync_route, bool verified) REQUIRES(!m_mutex);

    std::pair<const std::string&, bool> sync_route() REQUIRES(!m_mutex)
    {
        util::CheckedLockGuard lock(m_mutex);
        return {m_sync_route, m_sync_route_verified};
    }

    SyncClientConfig config() const REQUIRES(!m_mutex)
    {
        util::CheckedLockGuard lock(m_mutex);
        return m_config;
    }

    // Return the cached logger
    std::shared_ptr<util::Logger> get_logger() const REQUIRES(!m_mutex);

    struct OnlyForTesting {
        friend class TestHelper;

        static void voluntary_disconnect_all_connections(SyncManager&);
    };

    static std::shared_ptr<SyncManager> create(const SyncClientConfig& config);
    SyncManager(Private, const SyncClientConfig& config);

private:
    friend class SyncSession;
    friend class SyncUser;
    friend class ::TestSyncManager;
    friend class ::TestAppSession;

    util::CheckedMutex m_mutex;
    mutable std::unique_ptr<_impl::SyncClient> m_sync_client GUARDED_BY(m_mutex);
    SyncClientConfig m_config GUARDED_BY(m_mutex);
    std::shared_ptr<util::Logger> m_logger_ptr GUARDED_BY(m_mutex);
    // The sync route URL for the sync connection to the server.
    std::string m_sync_route GUARDED_BY(m_mutex);
    // If true, then the sync route has been verified by querying the location info or successfully
    // connecting to the server.
    bool m_sync_route_verified GUARDED_BY(m_mutex) = false;

    // Map of sessions by path name.
    // Sessions remove themselves from this map by calling `unregister_session` once they're
    // inactive and have performed any necessary cleanup work.
    util::CheckedMutex m_session_mutex;
    std::unordered_map<std::string, std::shared_ptr<SyncSession>> m_sessions GUARDED_BY(m_session_mutex);

    // Stop tracking the session for the given path if it is inactive.
    // No-op if the session is either still active or in the active sessions list
    // due to someone holding a strong reference to it.
    void unregister_session(const std::string& path) REQUIRES(!m_session_mutex);

    _impl::SyncClient& get_sync_client() const REQUIRES(!m_mutex);
    std::unique_ptr<_impl::SyncClient> create_sync_client() const REQUIRES(m_mutex);

    std::shared_ptr<SyncSession> get_existing_session_locked(const std::string& path) const REQUIRES(m_session_mutex);

    void init_metadata(SyncClientConfig config, const std::string& app_id);

    // internally create a new logger - used by configure() and set_logger_factory()
    void do_make_logger() REQUIRES(m_mutex);

    // Internal method returning `true` if the SyncManager still contains sessions not yet fully closed.
    // Callers of this method should hold the `m_session_mutex` themselves.
    bool do_has_existing_sessions() REQUIRES(m_session_mutex);
};

} // namespace realm

#endif // REALM_OS_SYNC_MANAGER_HPP
