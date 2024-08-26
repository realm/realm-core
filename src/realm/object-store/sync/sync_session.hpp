///////////////////////////////////////////////////////////////////////////
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

#ifndef REALM_OS_SYNC_SESSION_HPP
#define REALM_OS_SYNC_SESSION_HPP

#include <realm/object-store/feature_checks.hpp>
#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/sync/client_base.hpp>
#include <realm/sync/config.hpp>
#include <realm/sync/subscriptions.hpp>

#include <realm/util/checked_mutex.hpp>
#include <realm/util/future.hpp>
#include <realm/version_id.hpp>

#include <map>
#include <mutex>
#include <unordered_map>

namespace realm {
class DB;
class SyncManager;
class SyncUser;

namespace sync {
class Session;
class MigrationStore;
} // namespace sync

namespace _impl {
class RealmCoordinator;
struct SyncClient;

class SyncProgressNotifier {
public:
    enum class NotifierType { upload, download };
    using ProgressNotifierCallback = void(uint64_t transferred_bytes, uint64_t transferrable_bytes,
                                          double progress_estimate);

    uint64_t register_callback(std::function<ProgressNotifierCallback>, NotifierType direction, bool is_streaming,
                               int64_t pending_query_version);
    void unregister_callback(uint64_t);

    void set_local_version(uint64_t);
    void update(uint64_t downloaded, uint64_t downloadable, uint64_t uploaded, uint64_t uploadable,
                uint64_t snapshot_version, double download_estimate, double upload_estimate, int64_t query_version);

private:
    mutable std::mutex m_mutex;

    // How many bytes are uploadable or downloadable.
    struct Progress {
        uint64_t uploadable;
        uint64_t downloadable;
        uint64_t uploaded;
        uint64_t downloaded;
        double upload_estimate;
        double download_estimate;
        uint64_t snapshot_version;
        int64_t query_version;
    };

    // A PODS encapsulating some information for progress notifier callbacks a binding
    // can register upon this session.
    struct NotifierPackage {
        std::function<ProgressNotifierCallback> notifier;
        uint64_t snapshot_version;
        bool is_streaming;
        bool is_download;
        int64_t pending_query_version = 0;
        std::optional<uint64_t> captured_transferable;
        util::UniqueFunction<void()> create_invocation(const Progress&, bool& is_expired);
    };

    // A counter used as a token to identify progress notifier callbacks registered on this session.
    uint64_t m_progress_notifier_token = 1;
    // Version of the last locally-created transaction that we're expecting to be uploaded.
    uint64_t m_local_transaction_version = 0;

    // Will be `none` until we've received the initial notification from sync.  Note that this
    // happens only once ever during the lifetime of a given `SyncSession`, since these values are
    // expected to semi-monotonically increase, and a lower-bounds estimate is still useful in the
    // event more up-to-date information isn't yet available.  FIXME: If we support transparent
    // client reset in the future, we might need to reset the progress state variables if the Realm
    // is rolled back.
    std::optional<Progress> m_current_progress;

    std::unordered_map<uint64_t, NotifierPackage> m_packages;
};

} // namespace _impl

class SyncSession : public std::enable_shared_from_this<SyncSession> {
    struct Private {};

public:
    enum class State {
        Active,
        Dying,
        Inactive,
        WaitingForAccessToken,
        Paused,
    };

    enum class ConnectionState {
        Disconnected,
        Connecting,
        Connected,
    };

    using ConnectionStateChangeCallback = void(ConnectionState old_state, ConnectionState new_state);
    using TransactionCallback = void(VersionID old_version, VersionID new_version);
    using ProgressNotifierCallback = _impl::SyncProgressNotifier::ProgressNotifierCallback;
    using ProgressDirection = _impl::SyncProgressNotifier::NotifierType;

    explicit SyncSession(Private, _impl::SyncClient&, std::shared_ptr<DB>, const RealmConfig&,
                         SyncManager* sync_manager);
    SyncSession(const SyncSession&) = delete;
    SyncSession& operator=(const SyncSession&) = delete;
    ~SyncSession();
    State state() const REQUIRES(!m_state_mutex);
    ConnectionState connection_state() const REQUIRES(!m_connection_state_mutex);

    // The on-disk path of the Realm file backing the Realm this `SyncSession` represents.
    std::string const& path() const;

    // Returns the salted file ident of the Realm file if one has been assigned, or returns
    // zero for the file ident/salt if no file ident has been assigned yet. This value can
    // change after a client reset or during the initial download of the realm.
    sync::SaltedFileIdent get_file_ident() const;

    // Register a callback that will be called when all pending uploads have completed.
    // The callback is run asynchronously, and upon whatever thread the underlying sync client
    // chooses to run it on.
    void wait_for_upload_completion(util::UniqueFunction<void(Status)>&& callback) REQUIRES(!m_state_mutex);

    // Register a callback that will be called when all pending downloads have been completed.
    // Works the same way as `wait_for_upload_completion()`.
    void wait_for_download_completion(util::UniqueFunction<void(Status)>&& callback) REQUIRES(!m_state_mutex);

    // Register a notifier that updates the app regarding progress.
    //
    // If `m_current_progress` is populated when this method is called, the notifier
    // will be called synchronously, to provide the caller with an initial assessment
    // of the state of synchronization. Otherwise, the progress notifier will be
    // registered, and only called once sync has begun providing progress data.
    //
    // If `is_streaming` is true, then the notifier will be called forever, and will
    // always contain the most up-to-date number of downloadable or uploadable bytes.
    // Otherwise, the number of downloaded or uploaded bytes will always be reported
    // relative to the number of downloadable or uploadable bytes at the point in time
    // when the notifier was registered.
    //
    // An integer representing a token is returned. This token can be used to manually
    // unregister the notifier. If the integer is 0, the notifier was not registered.
    //
    // Note that bindings should dispatch the callback onto a separate thread or queue
    // in order to avoid blocking the sync client.
    uint64_t register_progress_notifier(std::function<ProgressNotifierCallback>&&, ProgressDirection,
                                        bool is_streaming) REQUIRES(!m_state_mutex);

    // Unregister a previously registered notifier. If the token is invalid,
    // this method does nothing.
    void unregister_progress_notifier(uint64_t);

    // Registers a callback that is invoked when the the underlying sync session changes
    // its connection state
    uint64_t register_connection_change_callback(std::function<ConnectionStateChangeCallback>&&);

    // Unregisters a previously registered callback. If the token is invalid,
    // this method does nothing
    void unregister_connection_change_callback(uint64_t);

    // If possible, take the session and do anything necessary to make it `Active`.
    // Specifically:
    // If the sync session is currently `Dying`, ask it to stay alive instead.
    // If the sync session is currently `Inactive`, recreate it.
    // If the sync session is currently `Paused`, do nothing - call resume() instead.
    // Otherwise, a no-op.
    void revive_if_needed() REQUIRES(!m_state_mutex, !m_config_mutex);

    // Perform any actions needed in response to regaining network connectivity.
    void handle_reconnect() REQUIRES(!m_state_mutex);

    // Inform the sync session that it should close. This will respect the stop policy specified in
    // the SyncConfig, so its possible the session will remain open either until all pending local
    // changes are uploaded or possibly forever.
    void close() REQUIRES(!m_state_mutex, !m_config_mutex, !m_connection_state_mutex);

    // Inform the sync session that it should close immediately, regardless of the stop policy.
    // The session may resume after calling this if a new Realm is opened for the underlying DB
    // of the SyncSession. Use pause() to close the sync session until you want to explicitly
    // resume it.
    void force_close() REQUIRES(!m_state_mutex, !m_connection_state_mutex);

    // Closes the sync session so that it will not resume until resume() is called.
    void pause() REQUIRES(!m_state_mutex, !m_connection_state_mutex);

    // Resumes the sync session after it was paused by calling pause(). If the sync session is inactive
    // for any other reason this will also resume it.
    void resume() REQUIRES(!m_state_mutex, !m_config_mutex);

    // Drop the current session and restart a new one from scratch using the latest configuration in
    // the sync manager. Used to respond to redirect responses from the server when the deployment
    // model has changed while the user is logged in and a session is active.
    // If this sync session is currently paused, a new session will not be started until resume() is
    // called.
    // NOTE: This method ignores the current stop policy and closes the current session immediately,
    //       since a new session will be created as part of this call. The new session will adhere to
    //       the stop policy if it is manually closed.
    void restart_session() REQUIRES(!m_state_mutex, !m_connection_state_mutex, !m_config_mutex);

    // Shut down the synchronization session (sync::Session) and wait for the Realm file to no
    // longer be open on behalf of it.
    void shutdown_and_wait() REQUIRES(!m_state_mutex, !m_connection_state_mutex);

    // DO NOT CALL OUTSIDE OF TESTING CODE.
    void detach_from_sync_manager() REQUIRES(!m_state_mutex, !m_connection_state_mutex);

    // The access token needs to periodically be refreshed and this is how to
    // let the sync session know to update it's internal copy.
    void update_access_token(std::string_view signed_token) REQUIRES(!m_state_mutex, !m_config_mutex);

    // Request an updated access token from this session's sync user.
    void initiate_access_token_refresh() REQUIRES(!m_config_mutex);

    // Update the sync configuration used for this session. The new configuration must have the
    // same user and reference realm url as the old configuration. The session will immediately
    // disconnect (if it was active), and then attempt to connect using the new configuration.
    // This is primarily intended to be used for TESTING only, even though it is used by the
    // Swift SDK in `setCustomRequestHeaders` and is defined in the realm-js bindgen definitions.
    void update_configuration(SyncConfig new_config)
        REQUIRES(!m_state_mutex, !m_config_mutex, !m_connection_state_mutex);

    // An object representing the user who owns the Realm this `SyncSession` represents.
    std::shared_ptr<SyncUser> user() const REQUIRES(!m_config_mutex)
    {
        util::CheckedLockGuard lock(m_config_mutex);
        REALM_ASSERT(m_config.sync_config);
        return m_config.sync_config->user;
    }

    // A copy of the configuration object describing the Realm this `SyncSession` represents.
    SyncConfig config() const REQUIRES(!m_config_mutex)
    {
        util::CheckedLockGuard lock(m_config_mutex);
        REALM_ASSERT(m_config.sync_config);
        return *m_config.sync_config;
    }

    // When App is created, it will provide a generated websocket sync route when the SyncManager is
    // created. When the next AppServices HTTP request is made via the App object, the location info
    // will be requested and a verified websocket sync route will be provided to the SyncManager. If
    // the SyncSession is started with a cached user, the websocket connection will be initially
    // established using the generated sync route. If that connection is successful, then the sync
    // route will be marked verified and used from that point on. If that connection fails, the
    // SyncSession will update the location via an access token update to retrieve the verified
    // sync route from the server's location info. The SyncSessio will then be restarted so it
    // uses the updated sync route value.

    // If the SyncSession is active, this function returns the sync route that is being used
    // by the current underlying session to connect to the server.
    std::string full_realm_url() const REQUIRES(!m_config_mutex)
    {
        util::CheckedLockGuard lock(m_config_mutex);
        return m_server_url;
    }

    // If the sync route value was returned by querying the location information, or the
    // SyncSession has successfully connected to the server using the configured sync route,
    // then the sync route will be marked "verified". Otherwise, the location information will
    // be requested from the server if the connection fails when trying to open a websocket
    // to the server.
    bool realm_url_verified() const REQUIRES(!m_config_mutex)
    {
        util::CheckedLockGuard lock(m_config_mutex);
        return m_server_url_verified;
    }

    std::shared_ptr<sync::SubscriptionStore> get_flx_subscription_store() REQUIRES(!m_state_mutex);

    // Create an external reference to this session. The sync session attempts to remain active
    // as long as an external reference to the session exists.
    std::shared_ptr<SyncSession> external_reference() REQUIRES(!m_external_reference_mutex);

    // Return an existing external reference to this session, if one exists. Otherwise, returns `nullptr`.
    std::shared_ptr<SyncSession> existing_external_reference() REQUIRES(!m_external_reference_mutex);

    struct OnlyForTesting;

    // Expose some internal functionality to other parts of the ObjectStore
    // without making it public to everyone
    class Internal {
        friend class _impl::RealmCoordinator;
        friend struct OnlyForTesting;
        friend class AsyncOpenTask;

        static void nonsync_transact_notify(SyncSession& session, VersionID::version_type version)
        {
            session.nonsync_transact_notify(version);
        }

        static std::shared_ptr<DB> get_db(SyncSession& session)
        {
            return session.m_db;
        }

        static void migrate_schema(SyncSession& session, util::UniqueFunction<void(Status)>&& callback)
        {
            session.migrate_schema(std::move(callback));
        }
    };

    // Expose some internal functionality to testing code.
    struct OnlyForTesting {
        static void handle_error(SyncSession& session, sync::SessionErrorInfo&& error);
        static void nonsync_transact_notify(SyncSession& session, VersionID::version_type version)
        {
            session.nonsync_transact_notify(version);
        }
        static std::shared_ptr<DB> get_db(SyncSession& session)
        {
            return session.m_db;
        }

        static std::string get_appservices_connection_id(SyncSession& session)
        {
            return session.get_appservices_connection_id();
        }

        // Supported commands can be found in `handleTestCommandMessage()`
        // in baas/devicesync/server/qbs_client_handler_functions.go
        static util::Future<std::string> send_test_command(SyncSession& session, std::string request)
        {
            return session.send_test_command(std::move(request));
        }

        static std::shared_ptr<sync::SubscriptionStore> get_subscription_store_base(SyncSession& session)
        {
            return session.get_subscription_store_base();
        }

        static util::Future<void> pause_async(SyncSession& session);
    };

private:
    using std::enable_shared_from_this<SyncSession>::shared_from_this;
    using CompletionCallbacks = std::map<int64_t, std::pair<ProgressDirection, util::UniqueFunction<void(Status)>>>;

    class ConnectionChangeNotifier {
    public:
        uint64_t add_callback(std::function<ConnectionStateChangeCallback> callback);
        void remove_callback(uint64_t token);
        void invoke_callbacks(ConnectionState old_state, ConnectionState new_state);

    private:
        struct Callback {
            std::function<ConnectionStateChangeCallback> fn;
            uint64_t token;
        };

        std::mutex m_callback_mutex;
        std::vector<Callback> m_callbacks;

        size_t m_callback_index = -1;
        size_t m_callback_count = -1;
        uint64_t m_next_token = 0;
    };

    friend class realm::SyncManager;
    // Called by SyncManager {
    static std::shared_ptr<SyncSession> create(_impl::SyncClient& client, std::shared_ptr<DB> db,
                                               const RealmConfig& config, SyncManager* sync_manager)
    {
        REALM_ASSERT(config.sync_config);
        return std::make_shared<SyncSession>(Private(), client, std::move(db), config, sync_manager);
    }
    // }

    std::shared_ptr<SyncManager> sync_manager() const REQUIRES(!m_state_mutex);

    static util::UniqueFunction<void(std::optional<app::AppError>)>
    handle_refresh(const std::shared_ptr<SyncSession>&, bool);

    // Initialize or tear down the subscription store based on whether or not flx_sync_requested is true
    void update_subscription_store(bool flx_sync_requested, std::optional<sync::SubscriptionSet> new_subs)
        REQUIRES(!m_state_mutex);
    void create_subscription_store() REQUIRES(m_state_mutex);
    void set_write_validator_factory(std::weak_ptr<sync::SubscriptionStore> weak_sub_mgr);
    // Update the sync config after a PBS->FLX migration or FLX->PBS rollback occurs
    void apply_sync_config_after_migration_or_rollback() REQUIRES(!m_config_mutex, !m_state_mutex);
    void save_sync_config_after_migration_or_rollback() REQUIRES(!m_config_mutex);

    void download_fresh_realm(const sync::SessionErrorInfo& error_info)
        REQUIRES(!m_config_mutex, !m_state_mutex, !m_connection_state_mutex);
    void handle_fresh_realm_downloaded(DBRef db, Status result, const sync::SessionErrorInfo& cr_error_info,
                                       std::optional<sync::SubscriptionSet> new_subs = std::nullopt)
        REQUIRES(!m_state_mutex, !m_config_mutex, !m_connection_state_mutex);
    void handle_error(sync::SessionErrorInfo) REQUIRES(!m_state_mutex, !m_config_mutex, !m_connection_state_mutex);
    void handle_bad_auth(const std::shared_ptr<SyncUser>& user, Status status)
        REQUIRES(!m_state_mutex, !m_config_mutex);
    void handle_location_update_failed(Status status)
        REQUIRES(!m_state_mutex, !m_config_mutex, !m_connection_state_mutex);
    // If sub_notify_error is set (including Status::OK()), then the pending subscription waiters will
    // also be called with the sub_notify_error status value.
    void cancel_pending_waits(util::CheckedUniqueLock, Status) RELEASE(m_state_mutex);
    enum class ShouldBackup { yes, no };
    void update_error_and_mark_file_for_deletion(SyncError&, ShouldBackup) REQUIRES(m_state_mutex, !m_config_mutex);
    void handle_progress_update(uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, double, double, int64_t);
    void handle_new_flx_sync_query(int64_t version);

    void nonsync_transact_notify(VersionID::version_type) REQUIRES(!m_state_mutex);

    void create_sync_session() REQUIRES(m_state_mutex, !m_config_mutex);
    void did_drop_external_reference()
        REQUIRES(!m_state_mutex, !m_config_mutex, !m_external_reference_mutex, !m_connection_state_mutex);
    void close(util::CheckedUniqueLock) RELEASE(m_state_mutex) REQUIRES(!m_config_mutex, !m_connection_state_mutex);

    void become_active() REQUIRES(m_state_mutex, !m_config_mutex);
    void become_dying(util::CheckedUniqueLock) RELEASE(m_state_mutex) REQUIRES(!m_connection_state_mutex);
    void become_inactive(util::CheckedUniqueLock, Status = Status::OK(), bool = true) RELEASE(m_state_mutex)
        REQUIRES(!m_connection_state_mutex);
    void become_paused(util::CheckedUniqueLock) RELEASE(m_state_mutex) REQUIRES(!m_connection_state_mutex);
    void become_waiting_for_access_token() REQUIRES(m_state_mutex);
    util::Future<void> pause_async() REQUIRES(!m_state_mutex, !m_connection_state_mutex);

    // do restart session restarts the session without freeing any of the waiters
    void do_restart_session(util::CheckedUniqueLock)
        REQUIRES(m_state_mutex, !m_connection_state_mutex, !m_config_mutex);

    // do_become_inactive is called from both become_paused()/become_inactive() and does all the steps to
    // shutdown and cleanup the sync session besides setting m_state.
    void do_become_inactive(util::CheckedUniqueLock, Status, bool) RELEASE(m_state_mutex)
        REQUIRES(!m_connection_state_mutex);
    // do_revive is called from both revive_if_needed() and resume(). It does all the steps to transition
    // from a state that is not Active to Active.
    void do_revive(util::CheckedUniqueLock&& lock) RELEASE(m_state_mutex) REQUIRES(!m_config_mutex);

    void add_completion_callback(util::UniqueFunction<void(Status)> callback, ProgressDirection direction)
        REQUIRES(m_state_mutex);

    std::string get_appservices_connection_id() const REQUIRES(!m_state_mutex);

    util::Future<std::string> send_test_command(std::string body) REQUIRES(!m_state_mutex);

    void migrate_schema(util::UniqueFunction<void(Status)>&& callback)
        REQUIRES(!m_state_mutex, !m_config_mutex, !m_connection_state_mutex);

    std::function<TransactionCallback> m_sync_transact_callback GUARDED_BY(m_state_mutex);

    template <typename Field>
    auto config(Field f) REQUIRES(!m_config_mutex)
    {
        util::CheckedLockGuard lock(m_config_mutex);
        return m_config.sync_config.get()->*f;
    }

    void assert_mutex_unlocked() ASSERT_CAPABILITY(!m_state_mutex) ASSERT_CAPABILITY(!m_config_mutex) {}

    // Return the subscription_store_base - to be used only for testing
    std::shared_ptr<sync::SubscriptionStore> get_subscription_store_base() REQUIRES(!m_state_mutex);

    // Updates the connection state for this SyncSession and notify any registered callbacks if changed.
    // Also ensures server_url_verified is set if the connection state is updated to connected.
    void update_connection_state(ConnectionState new_state) REQUIRES(!m_config_mutex, !m_connection_state_mutex);

    util::CheckedMutex m_state_mutex;
    util::CheckedMutex m_connection_state_mutex;

    State m_state GUARDED_BY(m_state_mutex) = State::Inactive;

    // The underlying state of the connection. Even when sharing connections, the underlying session
    // will always start out as disconnected and then immediately transition to the correct state when calling
    // bind().
    ConnectionState m_connection_state GUARDED_BY(m_connection_state_mutex) = ConnectionState::Disconnected;
    size_t m_death_count GUARDED_BY(m_state_mutex) = 0;

    util::CheckedMutex m_config_mutex;
    RealmConfig m_config GUARDED_BY(m_config_mutex);
    const std::shared_ptr<DB> m_db;
    // The subscription store base is lazily created when needed, but never destroyed
    std::shared_ptr<sync::SubscriptionStore> m_subscription_store_base GUARDED_BY(m_state_mutex);
    // m_flx_subscription_store will either point to m_subscription_store_base if currently using FLX
    // or set to nullptr if currently using PBS (mutable for client PBS->FLX migration)
    std::shared_ptr<sync::SubscriptionStore> m_flx_subscription_store GUARDED_BY(m_state_mutex);
    // Original sync config for reverting back to PBS if FLX migration is rolled back
    const std::shared_ptr<SyncConfig> m_original_sync_config; // does not change after construction
    std::shared_ptr<SyncConfig> m_migrated_sync_config GUARDED_BY(m_config_mutex);
    const std::shared_ptr<sync::MigrationStore> m_migration_store;
    std::optional<int64_t> m_migration_sentinel_query_version GUARDED_BY(m_state_mutex);
    std::optional<sync::SessionErrorInfo> m_client_reset_error GUARDED_BY(m_state_mutex);
    DBRef m_client_reset_fresh_copy GUARDED_BY(m_state_mutex);
    _impl::SyncClient& m_client;
    SyncManager* m_sync_manager GUARDED_BY(m_state_mutex) = nullptr;

    int64_t m_completion_request_counter GUARDED_BY(m_state_mutex) = 0;
    CompletionCallbacks m_completion_callbacks GUARDED_BY(m_state_mutex);

    // The underlying `Session` object that is owned and managed by this `SyncSession`.
    // The session is first created when the `SyncSession` is moved out of its initial `inactive` state.
    // The session might be destroyed if the `SyncSession` becomes inactive again (for example, if the
    // user owning the session logs out). It might be created anew if the session is revived (if a
    // logged-out user logs back in, the object store sync code will revive their sessions).
    std::unique_ptr<sync::Session> m_session GUARDED_BY(m_state_mutex);

    // The fully-resolved URL of this Realm, including the server and the path.
    std::string m_server_url GUARDED_BY(m_config_mutex);
    bool m_server_url_verified GUARDED_BY(m_config_mutex) = false;

    _impl::SyncProgressNotifier m_progress_notifier;
    ConnectionChangeNotifier m_connection_change_notifier;

    util::CheckedMutex m_external_reference_mutex;
    class ExternalReference;
    std::weak_ptr<ExternalReference> m_external_reference GUARDED_BY(m_external_reference_mutex);

    // Set if ProtocolError::schema_version_changed error is received from the server.
    std::optional<uint64_t> m_previous_schema_version GUARDED_BY(m_state_mutex);
    bool m_schema_migration_in_progress GUARDED_BY(m_state_mutex) = false;
};

} // namespace realm

#endif // REALM_OS_SYNC_SESSION_HPP
