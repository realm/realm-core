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

#ifndef REALM_OS_SYNC_SESSION_HPP
#define REALM_OS_SYNC_SESSION_HPP

#include <realm/util/optional.hpp>
#include <realm/version_id.hpp>

#include "sync_config.hpp"

#include <mutex>
#include <unordered_map>

namespace realm {

class SyncManager;
class SyncUser;

namespace _impl {
class RealmCoordinator;
struct SyncClient;

namespace sync_session_states {
struct WaitingForAccessToken;
struct Active;
struct Dying;
struct Inactive;
struct Error;
}
}

namespace sync {
class Session;
}

using SyncSessionTransactCallback = void(VersionID old_version, VersionID new_version);
using SyncProgressNotifierCallback = void(uint64_t transferred_bytes, uint64_t transferrable_bytes);

class SyncSession : public std::enable_shared_from_this<SyncSession> {
public:
    enum class PublicState {
        WaitingForAccessToken,
        Active,
        Dying,
        Inactive,
        Error,
    };
    PublicState state() const;

    bool is_in_error_state() const {
        return state() == PublicState::Error;
    }

    std::string const& path() const { return m_realm_path; }

    bool wait_for_upload_completion(std::function<void(std::error_code)> callback);
    bool wait_for_download_completion(std::function<void(std::error_code)> callback);

    enum class NotifierType {
        upload, download
    };
    // Register a notifier that updates the app regarding progress.
    // The notifier will always be called immediately during the function, to provide
    // the caller with an initial assessment of the state of synchronization.
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
    uint64_t register_progress_notifier(std::function<SyncProgressNotifierCallback>, NotifierType direction, bool is_streaming);

    // Unregister a previously registered notifier. If the token is invalid,
    // this method does nothing.
    void unregister_progress_notifier(uint64_t);

    // Wait for any pending uploads to complete, blocking the calling thread.
    // Returns `false` if the method did not attempt to wait, either because the
    // session is in an error state or because it hasn't yet been `bind()`ed.
    bool wait_for_upload_completion_blocking();

    // If the sync session is currently `Dying`, ask it to stay alive instead.
    // If the sync session is currently `Inactive`, recreate it. Otherwise, a no-op.
    static void revive_if_needed(std::shared_ptr<SyncSession> session);

    void refresh_access_token(std::string access_token, util::Optional<std::string> server_url);
    void bind_with_admin_token(std::string admin_token, std::string server_url);

    // Inform the sync session that it should close.
    void close();

    // Inform the sync session that it should close, but only if it is not yet connected.
    void close_if_connecting();

    // Inform the sync session that it should log out.
    void log_out();

    std::shared_ptr<SyncUser> user() const
    {
        return m_config.user;
    }

    const SyncConfig& config() const
    {
        return m_config;
    }

    util::Optional<std::string> full_realm_url() const
    {
        return m_server_url;
    }

    // Expose some internal functionality to other parts of the ObjectStore
    // without making it public to everyone
    class Internal {
        friend class _impl::RealmCoordinator;

        static void set_sync_transact_callback(SyncSession& session,
                                               std::function<SyncSessionTransactCallback> callback)
        {
            session.set_sync_transact_callback(std::move(callback));
        }

        static void set_error_handler(SyncSession& session, std::function<SyncSessionErrorHandler> callback)
        {
            session.set_error_handler(std::move(callback));
        }

        static void nonsync_transact_notify(SyncSession& session, VersionID::version_type version)
        {
            session.nonsync_transact_notify(version);
        }
    };

    // Expose some internal functionality to testing code.
    struct OnlyForTesting {
        static void handle_error(SyncSession& session, SyncError error)
        {
            session.handle_error(std::move(error));
        }
    };

private:
    struct State;
    friend struct _impl::sync_session_states::WaitingForAccessToken;
    friend struct _impl::sync_session_states::Active;
    friend struct _impl::sync_session_states::Dying;
    friend struct _impl::sync_session_states::Inactive;
    friend struct _impl::sync_session_states::Error;

    friend class realm::SyncManager;
    // Called by SyncManager {
    SyncSession(_impl::SyncClient&, std::string realm_path, SyncConfig);
    // }

    bool can_wait_for_network_completion() const;

    void handle_error(SyncError);
    static std::string get_recovery_file_path();
    void handle_progress_update(uint64_t, uint64_t, uint64_t, uint64_t);

    void set_sync_transact_callback(std::function<SyncSessionTransactCallback>);
    void set_error_handler(std::function<SyncSessionErrorHandler>);
    void nonsync_transact_notify(VersionID::version_type);

    void advance_state(std::unique_lock<std::mutex>& lock, const State&);

    void create_sync_session();
    void unregister(std::unique_lock<std::mutex>& lock);

    std::function<SyncSessionTransactCallback> m_sync_transact_callback;
    std::function<SyncSessionErrorHandler> m_error_handler;

    struct NotifierPackage {
        std::function<SyncProgressNotifierCallback> notifier;
        bool is_streaming;
        NotifierType direction;
        uint64_t captured_transferrable;
    };
    // A counter used as a token for progress notifications.
    uint64_t m_progress_notifier_token = 1;
    // How many bytes are uploadable or downloadable.
    uint64_t m_current_uploadable;
    uint64_t m_current_downloadable;
    uint64_t m_current_uploaded;
    uint64_t m_current_downloaded;
    std::unordered_map<uint64_t, NotifierPackage> m_notifiers;

    std::function<void()> create_notifier_invocation(const NotifierPackage&, bool&);

    mutable std::mutex m_state_mutex;
    mutable std::mutex m_progress_notifier_mutex;

    const State* m_state = nullptr;
    size_t m_death_count = 0;

    SyncConfig m_config;

    std::string m_realm_path;
    _impl::SyncClient& m_client;
    std::unique_ptr<sync::Session> m_session;
    util::Optional<int_fast64_t> m_deferred_commit_notification;
    bool m_deferred_close = false;

    // The fully-resolved URL of this Realm, including the server and the path.
    util::Optional<std::string> m_server_url;
};

}

#endif // REALM_OS_SYNC_SESSION_HPP
