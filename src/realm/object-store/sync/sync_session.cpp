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

#include <realm/object-store/sync/sync_session.hpp>

#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/impl/sync_client.hpp>
#include <realm/object-store/sync/impl/sync_file.hpp>
#include <realm/object-store/sync/impl/app_metadata.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/object-store/util/scheduler.hpp>

#include <realm/db_options.hpp>
#include <realm/sync/client.hpp>
#include <realm/sync/config.hpp>
#include <realm/sync/network/http.hpp>
#include <realm/sync/network/websocket_error.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset_operation.hpp>
#include <realm/sync/noinst/migration_store.hpp>
#include <realm/sync/noinst/sync_schema_migration.hpp>
#include <realm/sync/protocol.hpp>

using namespace realm;
using namespace realm::_impl;

using SessionWaiterPointer = void (sync::Session::*)(util::UniqueFunction<void(std::error_code)>);

constexpr const char SyncError::c_original_file_path_key[];
constexpr const char SyncError::c_recovery_file_path_key[];

/// STATES:
///
/// WAITING_FOR_ACCESS_TOKEN: a request has been initiated to ask
/// for an updated access token and the session is waiting for a response.
/// From: INACTIVE, DYING
/// To:
///    * ACTIVE: when the SDK successfully refreshes the token
///    * INACTIVE: if asked to log out, or if asked to close
///
/// ACTIVE: the session is connected to the Sync Server and is actively
/// transferring data.
/// From: INACTIVE, DYING, WAITING_FOR_ACCESS_TOKEN
/// To:
///    * INACTIVE: if asked to log out, or if asked to close and the stop policy
///                is Immediate.
///    * DYING: if asked to close and the stop policy is AfterChangesUploaded
///
/// DYING: the session is performing clean-up work in preparation to be destroyed.
/// From: ACTIVE
/// To:
///    * INACTIVE: when the clean-up work completes, if the session wasn't
///                revived, or if explicitly asked to log out before the
///                clean-up work begins
///    * ACTIVE: if the session is revived
///    * WAITING_FOR_ACCESS_TOKEN: if the session tried to enter ACTIVE,
///                                but the token is invalid or expired.
///
/// INACTIVE: the user owning this session has logged out, the `sync::Session`
/// owned by this session is destroyed, and the session is quiescent.
/// Note that a session briefly enters this state before being destroyed, but
/// it can also enter this state and stay there if the user has been logged out.
/// From: initial, ACTIVE, DYING, WAITING_FOR_ACCESS_TOKEN
/// To:
///    * ACTIVE: if the session is revived
///    * WAITING_FOR_ACCESS_TOKEN: if the session tried to enter ACTIVE,
///                                but the token is invalid or expired.

void SyncSession::become_active()
{
    REALM_ASSERT(m_state != State::Active);
    m_state = State::Active;

    // First time the session becomes active, register a notification on the sentinel subscription set to restart the
    // session and update to native FLX.
    if (m_migration_sentinel_query_version) {
        m_flx_subscription_store->get_by_version(*m_migration_sentinel_query_version)
            .get_state_change_notification(sync::SubscriptionSet::State::Complete)
            .get_async([=, weak_self = weak_from_this()](StatusWith<sync::SubscriptionSet::State> s) {
                if (!s.is_ok()) {
                    return;
                }
                REALM_ASSERT(s.get_value() == sync::SubscriptionSet::State::Complete);
                if (auto strong_self = weak_self.lock()) {
                    strong_self->m_migration_store->cancel_migration();
                    strong_self->restart_session();
                }
            });
        m_migration_sentinel_query_version.reset();
    }

    // when entering from the Dying state the session will still be bound
    create_sync_session();

    // Register all the pending wait-for-completion blocks. This can
    // potentially add a redundant callback if we're coming from the Dying
    // state, but that's okay (we won't call the user callbacks twice).
    SyncSession::CompletionCallbacks callbacks_to_register;
    std::swap(m_completion_callbacks, callbacks_to_register);

    for (auto& [id, callback_tuple] : callbacks_to_register) {
        add_completion_callback(std::move(callback_tuple.second), callback_tuple.first);
    }
}

void SyncSession::become_dying(util::CheckedUniqueLock lock)
{
    REALM_ASSERT(m_state != State::Dying);
    m_state = State::Dying;

    // If we have no session, we cannot possibly upload anything.
    if (!m_session) {
        become_inactive(std::move(lock));
        return;
    }

    size_t current_death_count = ++m_death_count;
    m_session->async_wait_for_upload_completion([weak_session = weak_from_this(), current_death_count](Status) {
        if (auto session = weak_session.lock()) {
            util::CheckedUniqueLock lock(session->m_state_mutex);
            if (session->m_state == State::Dying && session->m_death_count == current_death_count) {
                session->become_inactive(std::move(lock));
            }
        }
    });
    m_state_mutex.unlock(lock);
}

void SyncSession::become_inactive(util::CheckedUniqueLock lock, Status status, bool cancel_subscription_notifications)
{
    REALM_ASSERT(m_state != State::Inactive);
    m_state = State::Inactive;

    do_become_inactive(std::move(lock), status, cancel_subscription_notifications);
}

void SyncSession::become_paused(util::CheckedUniqueLock lock)
{
    REALM_ASSERT(m_state != State::Paused);
    auto old_state = m_state;
    m_state = State::Paused;

    // Nothing to do if we're already inactive besides update the state.
    if (old_state == State::Inactive) {
        m_state_mutex.unlock(lock);
        return;
    }

    do_become_inactive(std::move(lock), Status::OK(), true);
}

void SyncSession::restart_session()
{
    util::CheckedUniqueLock lock(m_state_mutex);
    switch (m_state) {
        case State::Active:
            do_restart_session(std::move(lock));
            break;
        case State::WaitingForAccessToken:
        case State::Paused:
        case State::Dying:
        case State::Inactive:
            return;
    }
}

void SyncSession::do_restart_session(util::CheckedUniqueLock)
{
    // Go straight to inactive so the progress completion waiters will
    // continue to wait until the session restarts and completes the
    // upload/download sync
    m_state = State::Inactive;

    if (m_session) {
        m_session.reset();
    }

    // Create a new session and re-register the completion callbacks
    // The latest server path will be retrieved from sync_manager when
    // the new session is created by create_sync_session() in become
    // active.
    become_active();
}

void SyncSession::do_become_inactive(util::CheckedUniqueLock lock, Status status,
                                     bool cancel_subscription_notifications)
{
    // Manually set the disconnected state. Sync would also do this, but
    // since the underlying SyncSession object already have been destroyed,
    // we are not able to get the callback.
    util::CheckedUniqueLock connection_state_lock(m_connection_state_mutex);
    auto old_state = m_connection_state;
    auto new_state = m_connection_state = SyncSession::ConnectionState::Disconnected;
    connection_state_lock.unlock();

    SyncSession::CompletionCallbacks waits;
    std::swap(waits, m_completion_callbacks);

    m_session = nullptr;
    if (m_sync_manager) {
        m_sync_manager->unregister_session(m_db->get_path());
    }

    auto subscription_store = m_flx_subscription_store;
    m_state_mutex.unlock(lock);

    // Send notifications after releasing the lock to prevent deadlocks in the callback.
    if (old_state != new_state) {
        m_connection_change_notifier.invoke_callbacks(old_state, connection_state());
    }

    if (status.is_ok())
        status = Status(ErrorCodes::OperationAborted, "Sync session became inactive");

    if (subscription_store && cancel_subscription_notifications) {
        subscription_store->notify_all_state_change_notifications(status);
    }

    // Inform any queued-up completion handlers that they were cancelled.
    for (auto& [id, callback] : waits)
        callback.second(status);
}

void SyncSession::become_waiting_for_access_token()
{
    REALM_ASSERT(m_state != State::WaitingForAccessToken);
    m_state = State::WaitingForAccessToken;
}

void SyncSession::handle_bad_auth(const std::shared_ptr<SyncUser>& user, Status status)
{
    // TODO: ideally this would write to the logs as well in case users didn't set up their error handler.
    {
        util::CheckedUniqueLock lock(m_state_mutex);
        cancel_pending_waits(std::move(lock), status);
    }
    if (user) {
        user->request_log_out();
    }

    if (auto error_handler = config(&SyncConfig::error_handler)) {
        auto user_facing_error = SyncError({ErrorCodes::AuthError, status.reason()}, true);
        error_handler(shared_from_this(), std::move(user_facing_error));
    }
}

static bool check_for_auth_failure(const app::AppError& error)
{
    using namespace realm::sync;
    // Auth failure is returned as a 401 (unauthorized) or 403 (forbidden) response
    if (error.additional_status_code) {
        auto status_code = HTTPStatus(*error.additional_status_code);
        if (status_code == HTTPStatus::Unauthorized || status_code == HTTPStatus::Forbidden)
            return true;
    }

    return false;
}

static bool check_for_redirect_response(const app::AppError& error)
{
    using namespace realm::sync;
    // Check for unhandled 301/308 permanent redirect response
    if (error.additional_status_code) {
        auto status_code = HTTPStatus(*error.additional_status_code);
        if (status_code == HTTPStatus::MovedPermanently || status_code == HTTPStatus::PermanentRedirect)
            return true;
    }

    return false;
}

util::UniqueFunction<void(std::optional<app::AppError>)>
SyncSession::handle_refresh(const std::shared_ptr<SyncSession>& session, bool restart_session)
{
    auto weak_session = session->weak_from_this();
    return [weak_session, restart_session](std::optional<app::AppError> error) {
        auto session = weak_session.lock();
        if (!session) {
            return;
        }

        auto session_user = session->user();
        if (!session_user) {
            util::CheckedUniqueLock lock(session->m_state_mutex);
            auto refresh_error = error ? error->to_status() : Status::OK();
            session->cancel_pending_waits(std::move(lock), refresh_error);
        }
        else if (error) {
            if (ErrorCodes::error_categories(error->code()).test(ErrorCategory::client_error)) {
                // any other client errors other than app_deallocated are considered fatal because
                // there was a problem locally before even sending the request to the server
                // eg. ClientErrorCode::user_not_found, ClientErrorCode::user_not_logged_in,
                // ClientErrorCode::too_many_redirects
                session->handle_bad_auth(session_user, error->to_status());
            }
            else if (check_for_auth_failure(*error)) {
                // A 401 response on a refresh request means that the token cannot be refreshed and we should not
                // retry. This can be because an admin has revoked this user's sessions, the user has been disabled,
                // or the refresh token has expired according to the server's clock.
                session->handle_bad_auth(
                    session_user,
                    {error->code(), util::format("Unable to refresh the user access token: %1", error->reason())});
            }
            else if (check_for_redirect_response(*error)) {
                // A 301 or 308 response is an unhandled permanent redirect response (which should not happen) - if
                // this is received, fail the request with an appropriate error message.
                // Temporary redirect responses (302, 307) are not supported
                session->handle_bad_auth(
                    session_user,
                    {error->code(), util::format("Unhandled redirect response when trying to reach the server: %1",
                                                 error->reason())});
            }
            else {
                // A refresh request has failed. This is an unexpected non-fatal error and we would
                // like to retry but we shouldn't do this immediately in order to not swamp the
                // server with requests. Consider two scenarios:
                // 1) If this request was spawned from the proactive token check, or a user
                // initiated request, the token may actually be valid. Just advance to Active
                // from WaitingForAccessToken if needed and let the sync server tell us if the
                // token is valid or not. If this also fails we will end up in case 2 below.
                // 2) If the sync connection initiated the request because the server is
                // unavailable or the connection otherwise encounters an unexpected error, we want
                // to let the sync client attempt to reinitialize the connection using its own
                // internal backoff timer which will happen automatically so nothing needs to
                // happen here.
                util::CheckedUniqueLock lock(session->m_state_mutex);
                // If updating access token while opening realm, just become active at this point
                // and try to use the current access token.
                if (session->m_state == State::WaitingForAccessToken) {
                    session->become_active();
                }
                // If `cancel_waits_on_nonfatal_error` is true, then cancel the waiters and pass along the error
                else if (session->config(&SyncConfig::cancel_waits_on_nonfatal_error)) {
                    session->cancel_pending_waits(std::move(lock), error->to_status()); // unlocks the mutex
                }
            }
        }
        else {
            // If the session needs to be restarted, then restart the session now
            // The latest access token and server url will be pulled from the sync
            // manager when the new session is started.
            if (restart_session) {
                session->restart_session();
            }
            // Otherwise, update the access token and reconnect
            else {
                session->update_access_token(session_user->access_token());
            }
        }
    };
}

SyncSession::SyncSession(Private, SyncClient& client, std::shared_ptr<DB> db, const RealmConfig& config,
                         SyncManager* sync_manager)
    : m_config{config}
    , m_db{std::move(db)}
    , m_original_sync_config{m_config.sync_config}
    , m_migration_store{sync::MigrationStore::create(m_db)}
    , m_client(client)
    , m_sync_manager(sync_manager)
{
    REALM_ASSERT(m_config.sync_config);
    // we don't want the following configs enabled during a client reset
    m_config.scheduler = nullptr;
    m_config.audit_config = nullptr;

    // Adjust the sync_config if using PBS sync and already in the migrated or rollback state
    if (m_migration_store->is_migrated() || m_migration_store->is_rollback_in_progress()) {
        m_config.sync_config = sync::MigrationStore::convert_sync_config_to_flx(m_original_sync_config);
    }

    // If using FLX, set up m_flx_subscription_store and the history_write_validator
    if (m_config.sync_config->flx_sync_requested) {
        create_subscription_store();
        std::weak_ptr<sync::SubscriptionStore> weak_sub_mgr(m_flx_subscription_store);
        set_write_validator_factory(weak_sub_mgr);
    }

    // After a migration to FLX, if the user opens the realm with a flexible sync configuration, we need to first
    // upload any unsynced changes before updating to native FLX.
    // A subscription set is used as sentinel so we know when to stop uploading.
    // Note: Currently, a sentinel subscription set is always created even if there is nothing to upload.
    if (m_migration_store->is_migrated() && m_original_sync_config->flx_sync_requested) {
        m_migration_store->create_sentinel_subscription_set(*m_flx_subscription_store);
        m_migration_sentinel_query_version = m_migration_store->get_sentinel_subscription_set_version();
        REALM_ASSERT(m_migration_sentinel_query_version);
    }
}

void SyncSession::detach_from_sync_manager()
{
    // Unregister all callbacks when the App and SyncManager are destroyed.
    {
        util::CheckedLockGuard lk(m_state_mutex);
        m_completion_callbacks.clear();
    }
    {
        util::CheckedLockGuard lk(m_connection_state_mutex);
        m_connection_change_notifier.remove_callbacks();
    }
    m_progress_notifier.unregister_callbacks();

    shutdown_and_wait();
    util::CheckedLockGuard lk(m_state_mutex);
    m_sync_manager = nullptr;
}

void SyncSession::update_error_and_mark_file_for_deletion(SyncError& error, ShouldBackup should_backup)
{
    util::CheckedLockGuard config_lock(m_config_mutex);
    // Add a SyncFileActionMetadata marking the Realm as needing to be deleted.
    auto original_path = path();
    error.user_info[SyncError::c_original_file_path_key] = original_path;
    using Action = SyncFileAction;
    auto action = should_backup == ShouldBackup::yes ? Action::BackUpThenDeleteRealm : Action::DeleteRealm;
    std::string recovery_path = m_config.sync_config->user->create_file_action(
        action, original_path, m_config.sync_config->recovery_directory);
    if (should_backup == ShouldBackup::yes) {
        error.user_info[SyncError::c_recovery_file_path_key] = recovery_path;
    }
}

void SyncSession::download_fresh_realm(const sync::SessionErrorInfo& error_info)
{
    // first check that recovery will not be prevented
    if (error_info.server_requests_action == sync::ProtocolErrorInfo::Action::ClientResetNoRecovery) {
        auto mode = config(&SyncConfig::client_resync_mode);
        if (mode == ClientResyncMode::Recover) {
            handle_fresh_realm_downloaded(
                nullptr,
                {ErrorCodes::RuntimeError,
                 "A client reset is required but the server does not permit recovery for this client"},
                error_info);
            return;
        }
    }

    std::vector<char> encryption_key;
    {
        util::CheckedLockGuard lock(m_config_mutex);
        encryption_key = m_config.encryption_key;
    }

    DBOptions options;
    options.allow_file_format_upgrade = false;
    options.enable_async_writes = false;
    if (!encryption_key.empty())
        options.encryption_key = encryption_key.data();

    DBRef db;
    auto fresh_path = client_reset::get_fresh_path_for(m_db->get_path());
    try {
        // We want to attempt to use a pre-existing file to reduce the chance of
        // downloading the first part of the file only to then delete it over
        // and over, but if we fail to open it then we should just start over.
        try {
            db = DB::create(sync::make_client_replication(), fresh_path, options);
        }
        catch (...) {
            util::File::try_remove(fresh_path);
        }

        if (!db) {
            db = DB::create(sync::make_client_replication(), fresh_path, options);
        }
    }
    catch (...) {
        // Failed to open the fresh path after attempting to delete it, so we
        // just can't do automatic recovery.
        handle_fresh_realm_downloaded(nullptr, exception_to_status(), error_info);
        return;
    }

    util::CheckedLockGuard state_lock(m_state_mutex);
    if (m_state != State::Active) {
        return;
    }

    RealmConfig fresh_config;
    {
        util::CheckedLockGuard config_lock(m_config_mutex);
        fresh_config = m_config;
        fresh_config.path = fresh_path;
        // in case of migrations use the migrated config
        auto fresh_sync_config = m_migrated_sync_config ? *m_migrated_sync_config : *m_config.sync_config;
        // deep copy the sync config so we don't modify the live session's config
        fresh_config.sync_config = std::make_shared<SyncConfig>(fresh_sync_config);
        fresh_config.sync_config->client_resync_mode = ClientResyncMode::Manual;
        fresh_config.schema_version = m_previous_schema_version.value_or(m_config.schema_version);
    }

    auto fresh_sync_session = m_sync_manager->get_session(db, fresh_config);
    auto& history = static_cast<sync::ClientReplication&>(*db->get_replication());
    // the fresh Realm may apply writes to this db after it has outlived its sync session
    // the writes are used to generate a changeset for recovery, but are never committed
    history.set_write_validator_factory({});

    fresh_sync_session->assert_mutex_unlocked();
    // The fresh realm uses flexible sync.
    if (auto fresh_sub_store = fresh_sync_session->get_flx_subscription_store()) {
        auto fresh_sub = fresh_sub_store->get_latest();
        // The local realm uses flexible sync as well so copy the active subscription set to the fresh realm.
        if (auto local_subs_store = m_flx_subscription_store) {
            auto fresh_mut_sub = fresh_sub.make_mutable_copy();
            fresh_mut_sub.import(local_subs_store->get_active());
            fresh_sub = fresh_mut_sub.commit();
        }

        auto self = shared_from_this();
        using SubscriptionState = sync::SubscriptionSet::State;
        fresh_sub.get_state_change_notification(SubscriptionState::Complete)
            .then([=](SubscriptionState) -> util::Future<sync::SubscriptionSet> {
                if (error_info.server_requests_action != sync::ProtocolErrorInfo::Action::MigrateToFLX) {
                    return fresh_sub;
                }
                if (!self->m_migration_store->is_migration_in_progress()) {
                    return fresh_sub;
                }

                // fresh_sync_session is using a new realm file that doesn't have the migration_store info
                // so the query string from the local migration store will need to be provided
                auto query_string = self->m_migration_store->get_query_string();
                REALM_ASSERT(query_string);
                // Create subscriptions in the fresh realm based on the schema instructions received in the bootstrap
                // message.
                fresh_sync_session->m_migration_store->create_subscriptions(*fresh_sub_store, *query_string);
                return fresh_sub_store->get_latest()
                    .get_state_change_notification(SubscriptionState::Complete)
                    .then([fresh_sub_store](SubscriptionState) {
                        return fresh_sub_store->get_latest();
                    });
            })
            .get_async([=](StatusWith<sync::SubscriptionSet>&& subs) {
                // Keep the sync session alive while it's downloading, but then close
                // it immediately
                fresh_sync_session->force_close();
                if (subs.is_ok()) {
                    self->handle_fresh_realm_downloaded(db, Status::OK(), error_info, std::move(subs.get_value()));
                }
                else {
                    self->handle_fresh_realm_downloaded(nullptr, std::move(subs.get_status()), error_info);
                }
            });
    }
    else { // pbs
        fresh_sync_session->wait_for_download_completion([=, weak_self = weak_from_this()](Status status) {
            // Keep the sync session alive while it's downloading, but then close
            // it immediately
            fresh_sync_session->force_close();
            if (auto strong_self = weak_self.lock()) {
                if (status.is_ok()) {
                    strong_self->handle_fresh_realm_downloaded(db, Status::OK(), error_info);
                }
                else {
                    strong_self->handle_fresh_realm_downloaded(nullptr, std::move(status), error_info);
                }
            }
        });
    }
    fresh_sync_session->revive_if_needed();
}

void SyncSession::handle_fresh_realm_downloaded(DBRef db, Status result, const sync::SessionErrorInfo& cr_error_info,
                                                std::optional<sync::SubscriptionSet> new_subs)
{
    util::CheckedUniqueLock lock(m_state_mutex);
    if (m_state != State::Active) {
        return;
    }
    // The download can fail for many reasons. For example:
    // - unable to write the fresh copy to the file system
    // - during download of the fresh copy, the fresh copy itself is reset
    // - in FLX mode there was a problem fulfilling the previously active subscription
    if (!result.is_ok()) {
        if (result == ErrorCodes::OperationAborted) {
            return;
        }
        lock.unlock();

        sync::SessionErrorInfo synthetic(
            Status{ErrorCodes::AutoClientResetFailed,
                   util::format("A fatal error occurred during '%1' client reset for %2: '%3'",
                                cr_error_info.server_requests_action, cr_error_info.status, result)},
            sync::IsFatal{true});
        handle_error(synthetic);
        return;
    }

    // Performing a client reset requires tearing down our current
    // sync session and creating a new one with the relevant client reset config. This
    // will result in session completion handlers firing
    // when the old session is torn down, which we don't want as this
    // is supposed to be transparent to the user.
    //
    // To avoid this, we need to move the completion handlers aside temporarily so
    // that moving to the inactive state doesn't clear them - they will be
    // re-registered when the session becomes active again.
    {
        m_client_reset_fresh_copy = db;
        CompletionCallbacks callbacks;
        // Save the client reset error for when the original sync session is revived
        m_client_reset_error = cr_error_info;

        std::swap(m_completion_callbacks, callbacks);
        // always swap back, even if advance_state throws
        auto guard = util::make_scope_exit([&]() noexcept {
            util::CheckedUniqueLock lock(m_state_mutex);
            if (m_completion_callbacks.empty())
                std::swap(callbacks, m_completion_callbacks);
            else
                m_completion_callbacks.merge(std::move(callbacks));
        });
        // Do not cancel the notifications on subscriptions.
        bool cancel_subscription_notifications = false;
        bool is_migration =
            m_client_reset_error->server_requests_action == sync::ProtocolErrorInfo::Action::MigrateToFLX ||
            m_client_reset_error->server_requests_action == sync::ProtocolErrorInfo::Action::RevertToPBS;
        become_inactive(std::move(lock), Status::OK(), cancel_subscription_notifications); // unlocks the lock

        // Once the session is inactive, update sync config and subscription store after migration.
        if (is_migration) {
            apply_sync_config_after_migration_or_rollback();
            auto flx_sync_requested = config(&SyncConfig::flx_sync_requested);
            update_subscription_store(flx_sync_requested, std::move(new_subs));
        }
    }
    revive_if_needed();
}

util::Future<void> SyncSession::pause_async()
{
    {
        util::CheckedUniqueLock lock(m_state_mutex);
        // Nothing to wait for if the session is already paused or inactive.
        if (m_state == SyncSession::State::Paused || m_state == SyncSession::State::Inactive) {
            return util::Future<void>::make_ready();
        }
    }
    // Transition immediately to `paused` state. Calling this function must guarantee that any
    // sync::Session object in SyncSession::m_session that existed prior to the time of invocation
    // must have been destroyed upon return. This allows the caller to follow up with a call to
    // sync::Client::notify_session_terminated() in order to be notified when the Realm file is closed. This works
    // so long as this SyncSession object remains in the `paused` state after the invocation of shutdown().
    pause();
    return m_client.notify_session_terminated();
}

void SyncSession::OnlyForTesting::handle_error(SyncSession& session, sync::SessionErrorInfo&& error)
{
    session.handle_error(std::move(error));
}

util::Future<void> SyncSession::OnlyForTesting::pause_async(SyncSession& session)
{
    return session.pause_async();
}

// This method should only be called from within the error handler callback registered upon the underlying
// `m_session`.
void SyncSession::handle_error(sync::SessionErrorInfo error)
{
    enum class NextStateAfterError { none, inactive, error };
    auto next_state = error.is_fatal ? NextStateAfterError::error : NextStateAfterError::none;
    std::optional<ShouldBackup> delete_file;
    bool log_out_user = false;
    bool unrecognized_by_client = false;

    if (error.status == ErrorCodes::AutoClientResetFailed) {
        // At this point, automatic recovery has been attempted but it failed.
        // Fallback to a manual reset and let the user try to handle it.
        next_state = NextStateAfterError::inactive;
        delete_file = ShouldBackup::yes;
    }
    else if (error.server_requests_action != sync::ProtocolErrorInfo::Action::NoAction) {
        switch (error.server_requests_action) {
            case sync::ProtocolErrorInfo::Action::NoAction:
                REALM_UNREACHABLE(); // This is not sent by the MongoDB server
            case sync::ProtocolErrorInfo::Action::ApplicationBug:
                [[fallthrough]];
            case sync::ProtocolErrorInfo::Action::ProtocolViolation:
                next_state = NextStateAfterError::inactive;
                break;
            case sync::ProtocolErrorInfo::Action::Warning:
                break; // not fatal, but should be bubbled up to the user below.
            case sync::ProtocolErrorInfo::Action::Transient:
                // Not real errors, don't need to be reported to the binding.
                return;
            case sync::ProtocolErrorInfo::Action::DeleteRealm:
                next_state = NextStateAfterError::inactive;
                delete_file = ShouldBackup::no;
                break;
            case sync::ProtocolErrorInfo::Action::ClientReset:
                [[fallthrough]];
            case sync::ProtocolErrorInfo::Action::ClientResetNoRecovery:
                switch (config(&SyncConfig::client_resync_mode)) {
                    case ClientResyncMode::Manual:
                        next_state = NextStateAfterError::inactive;
                        delete_file = ShouldBackup::yes;
                        break;
                    case ClientResyncMode::DiscardLocal:
                        [[fallthrough]];
                    case ClientResyncMode::RecoverOrDiscard:
                        [[fallthrough]];
                    case ClientResyncMode::Recover:
                        download_fresh_realm(error);
                        return; // do not propagate the error to the user at this point
                }
                break;
            case sync::ProtocolErrorInfo::Action::MigrateToFLX:
                // Should not receive this error if original sync config is FLX
                REALM_ASSERT(!m_original_sync_config->flx_sync_requested);
                REALM_ASSERT(error.migration_query_string && !error.migration_query_string->empty());
                // Original config was PBS, migrating to FLX
                m_migration_store->migrate_to_flx(*error.migration_query_string,
                                                  m_original_sync_config->partition_value);
                save_sync_config_after_migration_or_rollback();
                download_fresh_realm(error);
                return;
            case sync::ProtocolErrorInfo::Action::RevertToPBS:
                // If the client was updated to use FLX natively, but the server was rolled back to PBS,
                // the server should be sending switch_to_flx_sync; throw exception if this error is not
                // received.
                if (m_original_sync_config->flx_sync_requested) {
                    throw LogicError(ErrorCodes::InvalidServerResponse,
                                     "Received 'RevertToPBS' from server after rollback while client is natively "
                                     "using FLX - expected 'SwitchToPBS'");
                }
                // Original config was PBS, rollback the migration
                m_migration_store->rollback_to_pbs();
                save_sync_config_after_migration_or_rollback();
                download_fresh_realm(error);
                return;
            case sync::ProtocolErrorInfo::Action::RefreshUser:
                if (auto u = user()) {
                    u->request_access_token(handle_refresh(shared_from_this(), false));
                }
                return;
            case sync::ProtocolErrorInfo::Action::RefreshLocation:
                if (auto u = user()) {
                    u->request_refresh_location(handle_refresh(shared_from_this(), true));
                }
                return;
            case sync::ProtocolErrorInfo::Action::LogOutUser:
                next_state = NextStateAfterError::inactive;
                log_out_user = true;
                break;
            case sync::ProtocolErrorInfo::Action::MigrateSchema:
                util::CheckedUniqueLock lock(m_state_mutex);
                // Should only be received for FLX sync.
                REALM_ASSERT(m_original_sync_config->flx_sync_requested);
                m_previous_schema_version = error.previous_schema_version;
                return; // do not propagate the error to the user at this point
        }
    }
    else {
        // Unrecognized error code.
        unrecognized_by_client = true;
    }

    util::CheckedUniqueLock lock(m_state_mutex);
    SyncError sync_error{error.status, error.is_fatal, error.log_url, std::move(error.compensating_writes)};
    // `action` is used over `shouldClientReset` and `isRecoveryModeDisabled`.
    sync_error.server_requests_action = error.server_requests_action;
    sync_error.is_unrecognized_by_client = unrecognized_by_client;

    if (delete_file)
        update_error_and_mark_file_for_deletion(sync_error, *delete_file);

    if (m_state == State::Dying && error.is_fatal) {
        become_inactive(std::move(lock), error.status);
        return;
    }

    // Don't bother invoking m_config.error_handler if the sync is inactive.
    // It does not make sense to call the handler when the session is closed.
    if (m_state == State::Inactive || m_state == State::Paused) {
        return;
    }

    switch (next_state) {
        case NextStateAfterError::none:
            if (config(&SyncConfig::cancel_waits_on_nonfatal_error)) {
                cancel_pending_waits(std::move(lock), sync_error.status); // unlocks the mutex
            }
            break;
        case NextStateAfterError::inactive: {
            become_inactive(std::move(lock), sync_error.status);
            break;
        }
        case NextStateAfterError::error: {
            cancel_pending_waits(std::move(lock), sync_error.status);
            break;
        }
    }

    if (log_out_user) {
        if (auto u = user())
            u->request_log_out();
    }

    if (auto error_handler = config(&SyncConfig::error_handler)) {
        error_handler(shared_from_this(), std::move(sync_error));
    }
}

void SyncSession::cancel_pending_waits(util::CheckedUniqueLock lock, Status error)
{
    CompletionCallbacks callbacks;
    std::swap(callbacks, m_completion_callbacks);

    // Inform any waiters on pending subscription states that they were cancelled
    if (m_flx_subscription_store) {
        auto subscription_store = m_flx_subscription_store;
        m_state_mutex.unlock(lock);
        subscription_store->notify_all_state_change_notifications(error);
    }
    else {
        m_state_mutex.unlock(lock);
    }

    // Inform any queued-up completion handlers that they were cancelled.
    for (auto& [id, callback] : callbacks)
        callback.second(error);
}

void SyncSession::handle_progress_update(uint64_t downloaded, uint64_t downloadable, uint64_t uploaded,
                                         uint64_t uploadable, uint64_t snapshot_version, double download_estimate,
                                         double upload_estimate, int64_t query_version)
{
    m_progress_notifier.update(downloaded, downloadable, uploaded, uploadable, snapshot_version, download_estimate,
                               upload_estimate, query_version);
}


static sync::Session::Config::ClientReset
make_client_reset_config(const RealmConfig& base_config, const std::shared_ptr<SyncConfig>& sync_config,
                         DBRef&& fresh_copy, sync::SessionErrorInfo&& error_info, bool schema_migration_detected)
{
    REALM_ASSERT(sync_config->client_resync_mode != ClientResyncMode::Manual);

    sync::Session::Config::ClientReset config{sync_config->client_resync_mode, std::move(fresh_copy),
                                              std::move(error_info.status), error_info.server_requests_action};

    // The conditions here are asymmetric because if we have *either* a before
    // or after callback we need to make sure to initialize the local schema
    // before the client reset happens.
    if (!sync_config->notify_before_client_reset && !sync_config->notify_after_client_reset)
        return config;

    // We cannot initialize the local schema in case of a sync schema migration.
    // Currently, a schema migration involves breaking changes so opening the realm
    // with the new schema results in a crash.
    if (schema_migration_detected)
        return config;

    RealmConfig realm_config = base_config;
    realm_config.sync_config = std::make_shared<SyncConfig>(*sync_config); // deep copy
    realm_config.scheduler = util::Scheduler::make_dummy();

    if (sync_config->notify_after_client_reset) {
        config.notify_after_client_reset = [realm_config](VersionID previous_version, bool did_recover) {
            auto coordinator = _impl::RealmCoordinator::get_coordinator(realm_config);
            ThreadSafeReference active_after = coordinator->get_unbound_realm();
            SharedRealm frozen_before = coordinator->get_realm(realm_config, previous_version);
            REALM_ASSERT(frozen_before);
            REALM_ASSERT(frozen_before->is_frozen());
            realm_config.sync_config->notify_after_client_reset(std::move(frozen_before), std::move(active_after),
                                                                did_recover);
        };
    }
    config.notify_before_client_reset = [config = std::move(realm_config)]() -> VersionID {
        // Opening the Realm live here may make a write if the schema is different
        // than what exists on disk. It is necessary to pass a fully usable Realm
        // to the user here. Note that the schema changes made here will be considered
        // an "offline write" to be recovered if this is recovery mode.
        auto before = Realm::get_shared_realm(config);
        if (auto& notify_before = config.sync_config->notify_before_client_reset) {
            notify_before(config.sync_config->freeze_before_reset_realm ? before->freeze() : before);
        }
        // Note that if the SDK wrote to the Realm (hopefully by requesting a
        // live instance and not opening a secondary one), this may be a
        // different version than what we had before calling the callback.
        before->refresh();
        return before->read_transaction_version();
    };

    return config;
}

void SyncSession::create_sync_session()
{
    if (m_session)
        return;

    util::CheckedLockGuard config_lock(m_config_mutex);

    REALM_ASSERT(m_config.sync_config);
    SyncConfig& sync_config = *m_config.sync_config;
    REALM_ASSERT(sync_config.user);

    std::weak_ptr<SyncSession> weak_self = weak_from_this();

    sync::Session::Config session_config;
    session_config.signed_user_token = sync_config.user->access_token();
    session_config.user_id = sync_config.user->user_id();
    session_config.realm_identifier = sync_config.partition_value;
    session_config.verify_servers_ssl_certificate = sync_config.client_validate_ssl;
    session_config.ssl_trust_certificate_path = sync_config.ssl_trust_certificate_path;
    session_config.ssl_verify_callback = sync_config.ssl_verify_callback;
    session_config.proxy_config = sync_config.proxy_config;
    session_config.simulate_integration_error = sync_config.simulate_integration_error;
    session_config.flx_bootstrap_batch_size_bytes = sync_config.flx_bootstrap_batch_size_bytes;
    session_config.fresh_realm_download = client_reset::is_fresh_path(m_config.path);
    session_config.schema_version = m_config.schema_version;

    if (sync_config.on_sync_client_event_hook) {
        session_config.on_sync_client_event_hook = [hook = sync_config.on_sync_client_event_hook,
                                                    weak_self](const SyncClientHookData& data) {
            return hook(weak_self, data);
        };
    }

    {
        // At this point the sync route was either updated when the first App request was performed, or
        // was populated by a generated value that will be used for first contact. If the generated sync
        // route is not correct, either a redirection will be received or the connection will fail,
        // resulting in an update to both the access token and the location.
        auto [sync_route, verified] = m_sync_manager->sync_route();
        REALM_ASSERT_EX(!sync_route.empty(), "Server URL cannot be empty");

        if (!m_client.decompose_server_url(sync_route, session_config.protocol_envelope,
                                           session_config.server_address, session_config.server_port,
                                           session_config.service_identifier)) {
            throw sync::BadServerUrl(sync_route);
        }
        session_config.server_verified = verified;

        m_server_url = sync_route;
        m_server_url_verified = verified;
    }

    if (sync_config.authorization_header_name) {
        session_config.authorization_header_name = *sync_config.authorization_header_name;
    }
    session_config.custom_http_headers = sync_config.custom_http_headers;

    if (m_client_reset_error) {
        auto client_reset_error = std::exchange(m_client_reset_error, std::nullopt);
        if (client_reset_error->server_requests_action != sync::ProtocolErrorInfo::Action::NoAction) {
            // Use the original sync config, not the updated one from the migration store
            session_config.client_reset_config =
                make_client_reset_config(m_config, m_original_sync_config, std::move(m_client_reset_fresh_copy),
                                         std::move(*client_reset_error), m_previous_schema_version.has_value());
            session_config.schema_version = m_previous_schema_version.value_or(m_config.schema_version);
        }
    }

    session_config.progress_handler = [weak_self](uint_fast64_t downloaded, uint_fast64_t downloadable,
                                                  uint_fast64_t uploaded, uint_fast64_t uploadable,
                                                  uint_fast64_t snapshot_version, double download_estimate,
                                                  double upload_estimate, int64_t query_version) {
        if (auto self = weak_self.lock()) {
            self->handle_progress_update(downloaded, downloadable, uploaded, uploadable, snapshot_version,
                                         download_estimate, upload_estimate, query_version);
        }
    };

    session_config.connection_state_change_listener = [weak_self](sync::ConnectionState state,
                                                                  std::optional<sync::SessionErrorInfo> error) {
        using cs = sync::ConnectionState;
        ConnectionState new_state = [&] {
            switch (state) {
                case cs::disconnected:
                    return ConnectionState::Disconnected;
                case cs::connecting:
                    return ConnectionState::Connecting;
                case cs::connected:
                    return ConnectionState::Connected;
            }
            REALM_UNREACHABLE();
        }();
        // If the OS SyncSession object is destroyed, we ignore any events from the underlying Session as there is
        // nothing useful we can do with them.
        if (auto self = weak_self.lock()) {
            self->update_connection_state(new_state);
            if (error) {
                self->handle_error(std::move(*error));
            }
        }
    };

    m_session = m_client.make_session(m_db, m_flx_subscription_store, m_migration_store, std::move(session_config));
}

void SyncSession::update_connection_state(ConnectionState new_state)
{
    if (new_state == ConnectionState::Connected) {
        util::CheckedLockGuard lock(m_config_mutex);
        m_server_url_verified = true;
    }

    ConnectionState old_state;
    {
        util::CheckedLockGuard lock(m_connection_state_mutex);
        old_state = m_connection_state;
        m_connection_state = new_state;
    }

    // Notify any registered connection callbacks of the state transition
    if (old_state != new_state) {
        m_connection_change_notifier.invoke_callbacks(old_state, new_state);
    }
}

void SyncSession::nonsync_transact_notify(sync::version_type version)
{
    m_progress_notifier.set_local_version(version);

    util::CheckedUniqueLock lock(m_state_mutex);
    switch (m_state) {
        case State::Active:
        case State::WaitingForAccessToken:
            if (m_session) {
                m_session->nonsync_transact_notify(version);
            }
            break;
        case State::Dying:
        case State::Inactive:
        case State::Paused:
            break;
    }
}

void SyncSession::revive_if_needed()
{
    util::CheckedUniqueLock lock(m_state_mutex);
    switch (m_state) {
        case State::Active:
        case State::WaitingForAccessToken:
        case State::Paused:
            return;
        case State::Dying:
        case State::Inactive:
            do_revive(std::move(lock));
            break;
    }
}

void SyncSession::handle_reconnect()
{
    util::CheckedUniqueLock lock(m_state_mutex);
    switch (m_state) {
        case State::Active:
            m_session->cancel_reconnect_delay();
            break;
        case State::Dying:
        case State::Inactive:
        case State::WaitingForAccessToken:
        case State::Paused:
            break;
    }
}

void SyncSession::force_close()
{
    util::CheckedUniqueLock lock(m_state_mutex);
    switch (m_state) {
        case State::Active:
        case State::Dying:
        case State::WaitingForAccessToken:
            become_inactive(std::move(lock));
            break;
        case State::Inactive:
        case State::Paused:
            break;
    }
}

void SyncSession::pause()
{
    util::CheckedUniqueLock lock(m_state_mutex);
    switch (m_state) {
        case State::Active:
        case State::Dying:
        case State::WaitingForAccessToken:
        case State::Inactive:
            become_paused(std::move(lock));
            break;
        case State::Paused:
            break;
    }
}

void SyncSession::resume()
{
    util::CheckedUniqueLock lock(m_state_mutex);
    switch (m_state) {
        case State::Active:
        case State::WaitingForAccessToken:
            return;
        case State::Paused:
        case State::Dying:
        case State::Inactive:
            do_revive(std::move(lock));
            break;
    }
}

void SyncSession::do_revive(util::CheckedUniqueLock&& lock)
{
    auto u = user();
    // If the sync manager has a valid route and the user and it's access token
    // are valid, then revive the session.
    if (!u || !u->access_token_refresh_required()) {
        become_active();
        m_state_mutex.unlock(lock);
        return;
    }

    // Otherwise, either the access token has expired or the location info hasn't
    // been requested since the app was started - request a new access token to
    // refresh both.
    become_waiting_for_access_token();
    // Release the lock for SDKs with a single threaded
    // networking implementation such as our test suite
    // so that the update can trigger a state change from
    // the completion handler.
    m_state_mutex.unlock(lock);
    initiate_access_token_refresh();
}

void SyncSession::close()
{
    util::CheckedUniqueLock lock(m_state_mutex);
    close(std::move(lock));
}

void SyncSession::close(util::CheckedUniqueLock lock)
{
    switch (m_state) {
        case State::Active: {
            switch (config(&SyncConfig::stop_policy)) {
                case SyncSessionStopPolicy::Immediately:
                    become_inactive(std::move(lock));
                    break;
                case SyncSessionStopPolicy::LiveIndefinitely:
                    // Don't do anything; session lives forever.
                    m_state_mutex.unlock(lock);
                    break;
                case SyncSessionStopPolicy::AfterChangesUploaded:
                    // Wait for all pending changes to upload.
                    become_dying(std::move(lock));
                    break;
            }
            break;
        }
        case State::Dying:
            m_state_mutex.unlock(lock);
            break;
        case State::Paused:
        case State::Inactive: {
            // We need to unregister from the sync manager if it still exists so that we don't end up
            // holding the DBRef open after the session is closed. Otherwise we can end up preventing
            // the user from deleting the realm when it's in the paused/inactive state.
            if (m_sync_manager) {
                m_sync_manager->unregister_session(m_db->get_path());
            }
            m_state_mutex.unlock(lock);
            break;
        }
        case State::WaitingForAccessToken:
            // Immediately kill the session.
            become_inactive(std::move(lock));
            break;
    }
}

void SyncSession::shutdown_and_wait()
{
    {
        // Transition immediately to `inactive` state. Calling this function must guarantee that any
        // sync::Session object in SyncSession::m_session that existed prior to the time of invocation
        // must have been destroyed upon return. This allows the caller to follow up with a call to
        // sync::Client::wait_for_session_terminations_or_client_stopped() in order to wait for the
        // Realm file to be closed. This works so long as this SyncSession object remains in the
        // `inactive` state after the invocation of shutdown_and_wait().
        util::CheckedUniqueLock lock(m_state_mutex);
        if (m_state != State::Inactive && m_state != State::Paused) {
            become_inactive(std::move(lock));
        }
    }
    m_client.wait_for_session_terminations();
}

void SyncSession::update_access_token(std::string_view signed_token)
{
    util::CheckedUniqueLock lock(m_state_mutex);
    switch (m_state) {
        case State::Active:
            m_session->refresh(signed_token);
            break;
        case State::WaitingForAccessToken:
            become_active();
            break;
        case State::Paused:
            // token will be pulled from user when the session is unpaused
            return;
        case State::Dying:
        case State::Inactive:
            do_revive(std::move(lock));
            break;
    }
}

void SyncSession::initiate_access_token_refresh()
{
    if (auto session_user = user()) {
        session_user->request_access_token(handle_refresh(shared_from_this(), false));
    }
}

void SyncSession::add_completion_callback(util::UniqueFunction<void(Status)> callback, ProgressDirection direction)
{
    bool is_download = (direction == ProgressDirection::download);

    m_completion_request_counter++;
    m_completion_callbacks.emplace_hint(m_completion_callbacks.end(), m_completion_request_counter,
                                        std::make_pair(direction, std::move(callback)));
    // If the state is inactive then just store the callback and return. The callback will get
    // re-registered with the underlying session if/when the session ever becomes active again.
    if (!m_session) {
        return;
    }

    auto waiter = is_download ? &sync::Session::async_wait_for_download_completion
                              : &sync::Session::async_wait_for_upload_completion;

    (m_session.get()->*waiter)([weak_self = weak_from_this(), id = m_completion_request_counter](Status status) {
        auto self = weak_self.lock();
        if (!self)
            return;
        util::CheckedUniqueLock lock(self->m_state_mutex);
        auto callback_node = self->m_completion_callbacks.extract(id);
        lock.unlock();
        if (callback_node) {
            callback_node.mapped().second(std::move(status));
        }
    });
}

void SyncSession::wait_for_upload_completion(util::UniqueFunction<void(Status)>&& callback)
{
    util::CheckedUniqueLock lock(m_state_mutex);
    add_completion_callback(std::move(callback), ProgressDirection::upload);
}

void SyncSession::wait_for_download_completion(util::UniqueFunction<void(Status)>&& callback)
{
    util::CheckedUniqueLock lock(m_state_mutex);
    add_completion_callback(std::move(callback), ProgressDirection::download);
}

uint64_t SyncSession::register_progress_notifier(std::function<ProgressNotifierCallback>&& notifier,
                                                 ProgressDirection direction, bool is_streaming)
{
    int64_t pending_query_version = 0;
    if (auto sub_store = get_flx_subscription_store()) {
        pending_query_version = sub_store->get_version_info().latest;
    }
    return m_progress_notifier.register_callback(std::move(notifier), direction, is_streaming, pending_query_version);
}

void SyncSession::unregister_progress_notifier(uint64_t token)
{
    m_progress_notifier.unregister_callback(token);
}

uint64_t SyncSession::register_connection_change_callback(std::function<ConnectionStateChangeCallback>&& callback)
{
    return m_connection_change_notifier.add_callback(std::move(callback));
}

void SyncSession::unregister_connection_change_callback(uint64_t token)
{
    m_connection_change_notifier.remove_callback(token);
}

SyncSession::~SyncSession() {}

SyncSession::State SyncSession::state() const
{
    util::CheckedUniqueLock lock(m_state_mutex);
    return m_state;
}

SyncSession::ConnectionState SyncSession::connection_state() const
{
    util::CheckedUniqueLock lock(m_connection_state_mutex);
    return m_connection_state;
}

std::string const& SyncSession::path() const
{
    return m_db->get_path();
}

std::shared_ptr<sync::SubscriptionStore> SyncSession::get_flx_subscription_store()
{
    util::CheckedLockGuard lock(m_state_mutex);
    return m_flx_subscription_store;
}

std::shared_ptr<sync::SubscriptionStore> SyncSession::get_subscription_store_base()
{
    util::CheckedLockGuard lock(m_state_mutex);
    return m_subscription_store_base;
}

sync::SaltedFileIdent SyncSession::get_file_ident() const
{
    auto repl = m_db->get_replication();
    REALM_ASSERT(repl);
    REALM_ASSERT(dynamic_cast<sync::ClientReplication*>(repl));

    sync::SaltedFileIdent ret;
    sync::version_type unused_version;
    sync::SyncProgress unused_progress;
    static_cast<sync::ClientReplication*>(repl)->get_history().get_status(unused_version, ret, unused_progress);
    return ret;
}

std::string SyncSession::get_appservices_connection_id() const
{
    util::CheckedLockGuard lk(m_state_mutex);
    if (!m_session) {
        return {};
    }
    return m_session->get_appservices_connection_id();
}

void SyncSession::update_configuration(SyncConfig new_config)
{
    while (true) {
        util::CheckedUniqueLock state_lock(m_state_mutex);
        if (m_state != State::Inactive && m_state != State::Paused) {
            // Changing the state releases the lock, which means that by the
            // time we reacquire the lock the state may have changed again
            // (either due to one of the callbacks being invoked or another
            // thread coincidentally doing something). We just attempt to keep
            // switching it to inactive until it stays there.
            become_inactive(std::move(state_lock));
            continue;
        }

        util::CheckedUniqueLock config_lock(m_config_mutex);
        REALM_ASSERT(m_state == State::Inactive || m_state == State::Paused);
        REALM_ASSERT(!m_session);
        REALM_ASSERT(m_config.sync_config->user == new_config.user);
        // Since this is used for testing purposes only, just update the current sync_config
        m_config.sync_config = std::make_shared<SyncConfig>(std::move(new_config));
        break;
    }
    revive_if_needed();
}

void SyncSession::apply_sync_config_after_migration_or_rollback()
{
    // Migration state changed - Update the configuration to
    // match the new sync mode.
    util::CheckedLockGuard cfg_lock(m_config_mutex);
    if (!m_migrated_sync_config)
        return;

    m_config.sync_config = m_migrated_sync_config;
    m_migrated_sync_config.reset();
}

void SyncSession::save_sync_config_after_migration_or_rollback()
{
    util::CheckedLockGuard cfg_lock(m_config_mutex);
    m_migrated_sync_config = m_migration_store->convert_sync_config(m_original_sync_config);
}

void SyncSession::update_subscription_store(bool flx_sync_requested, std::optional<sync::SubscriptionSet> new_subs)
{
    util::CheckedUniqueLock lock(m_state_mutex);

    // The session should be closed before updating the FLX subscription store
    REALM_ASSERT(!m_session);

    // If the subscription store exists and switching to PBS, then clear the store
    auto& history = static_cast<sync::ClientReplication&>(*m_db->get_replication());
    if (!flx_sync_requested) {
        if (m_flx_subscription_store) {
            // Empty the subscription store and cancel any pending subscription notification
            // waiters
            auto subscription_store = std::move(m_flx_subscription_store);
            lock.unlock();
            auto tr = m_db->start_write();
            subscription_store->reset(*tr);
            history.set_write_validator_factory(nullptr);
            tr->commit();
        }
        return;
    }

    if (m_flx_subscription_store)
        return; // Using FLX and subscription store already exists

    // Going from PBS -> FLX (or one doesn't exist yet), create a new subscription store
    create_subscription_store();

    std::weak_ptr<sync::SubscriptionStore> weak_sub_mgr(m_flx_subscription_store);

    // If migrated to FLX, create subscriptions in the local realm to cover the existing data.
    // This needs to be done before setting the write validator to avoid NoSubscriptionForWrite errors.
    if (new_subs) {
        auto active_mut_sub = m_flx_subscription_store->get_active().make_mutable_copy();
        active_mut_sub.import(std::move(*new_subs));
        active_mut_sub.set_state(sync::SubscriptionSet::State::Complete);
        active_mut_sub.commit();
    }

    auto tr = m_db->start_write();
    set_write_validator_factory(weak_sub_mgr);
    tr->rollback();
}

void SyncSession::create_subscription_store()
{
    REALM_ASSERT(!m_flx_subscription_store);

    // Create the main subscription store instance when this is first called - this will
    // remain valid afterwards for the life of the SyncSession, but m_flx_subscription_store
    // will be reset when rolling back to PBS after a client FLX migration
    if (!m_subscription_store_base) {
        m_subscription_store_base = sync::SubscriptionStore::create(m_db);
    }

    // m_subscription_store_base is always around for the life of SyncSession, but the
    // m_flx_subscription_store is set when using FLX.
    m_flx_subscription_store = m_subscription_store_base;
}

void SyncSession::set_write_validator_factory(std::weak_ptr<sync::SubscriptionStore> weak_sub_mgr)
{
    auto& history = static_cast<sync::ClientReplication&>(*m_db->get_replication());
    history.set_write_validator_factory(
        [weak_sub_mgr](Transaction& tr) -> util::UniqueFunction<sync::SyncReplication::WriteValidator> {
            auto sub_mgr = weak_sub_mgr.lock();
            REALM_ASSERT_RELEASE(sub_mgr);
            auto latest_sub_tables = sub_mgr->get_tables_for_latest(tr);
            return [tables = std::move(latest_sub_tables)](const Table& table) {
                if (table.get_table_type() != Table::Type::TopLevel) {
                    return;
                }
                auto object_class_name = Group::table_name_to_class_name(table.get_name());
                if (tables.find(object_class_name) == tables.end()) {
                    throw NoSubscriptionForWrite(
                        util::format("Cannot write to class %1 when no flexible sync subscription has been created.",
                                     object_class_name));
                }
            };
        });
}

// Represents a reference to the SyncSession from outside of the sync subsystem.
// We attempt to keep the SyncSession in an active state as long as it has an external reference.
class SyncSession::ExternalReference {
public:
    ExternalReference(std::shared_ptr<SyncSession> session)
        : m_session(std::move(session))
    {
    }

    ~ExternalReference()
    {
        m_session->did_drop_external_reference();
    }

private:
    std::shared_ptr<SyncSession> m_session;
};

std::shared_ptr<SyncSession> SyncSession::external_reference()
{
    util::CheckedLockGuard lock(m_external_reference_mutex);

    if (auto external_reference = m_external_reference.lock())
        return std::shared_ptr<SyncSession>(external_reference, this);

    auto external_reference = std::make_shared<ExternalReference>(shared_from_this());
    m_external_reference = external_reference;
    return std::shared_ptr<SyncSession>(external_reference, this);
}

std::shared_ptr<SyncSession> SyncSession::existing_external_reference()
{
    util::CheckedLockGuard lock(m_external_reference_mutex);

    if (auto external_reference = m_external_reference.lock())
        return std::shared_ptr<SyncSession>(external_reference, this);

    return nullptr;
}

void SyncSession::did_drop_external_reference()
{
    util::CheckedUniqueLock lock1(m_state_mutex);
    {
        util::CheckedLockGuard lock2(m_external_reference_mutex);

        // If the session is being resurrected we should not close the session.
        if (!m_external_reference.expired())
            return;
    }

    close(std::move(lock1));
}

uint64_t SyncProgressNotifier::register_callback(std::function<ProgressNotifierCallback> notifier,
                                                 NotifierType direction, bool is_streaming,
                                                 int64_t pending_query_version)
{
    util::UniqueFunction<void()> invocation;
    uint64_t token_value = 0;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        token_value = m_progress_notifier_token++;
        NotifierPackage package{std::move(notifier), m_local_transaction_version, is_streaming,
                                direction == NotifierType::download, pending_query_version};
        if (!m_current_progress) {
            // Simply register the package, since we have no data yet.
            m_packages.emplace(token_value, std::move(package));
            return token_value;
        }
        bool skip_registration = false;
        invocation = package.create_invocation(*m_current_progress, skip_registration);
        if (skip_registration) {
            token_value = 0;
        }
        else {
            m_packages.emplace(token_value, std::move(package));
        }
    }
    invocation();
    return token_value;
}

void SyncProgressNotifier::unregister_callback(uint64_t token)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_packages.erase(token);
}

void SyncProgressNotifier::unregister_callbacks()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_packages.clear();
    m_current_progress.reset();
    m_local_transaction_version = 0;
}

void SyncProgressNotifier::update(uint64_t downloaded, uint64_t downloadable, uint64_t uploaded, uint64_t uploadable,
                                  uint64_t snapshot_version, double download_estimate, double upload_estimate,
                                  int64_t query_version)
{
    std::vector<util::UniqueFunction<void()>> invocations;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_current_progress = Progress{uploadable,      downloadable,      uploaded,         downloaded,
                                      upload_estimate, download_estimate, snapshot_version, query_version};

        for (auto it = m_packages.begin(); it != m_packages.end();) {
            bool should_delete = false;
            invocations.emplace_back(it->second.create_invocation(*m_current_progress, should_delete));
            it = should_delete ? m_packages.erase(it) : std::next(it);
        }
    }
    // Run the notifiers only after we've released the lock.
    for (auto& invocation : invocations)
        invocation();
}

void SyncProgressNotifier::set_local_version(uint64_t snapshot_version)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_local_transaction_version = snapshot_version;
}

util::UniqueFunction<void()>
SyncProgressNotifier::NotifierPackage::create_invocation(Progress const& current_progress, bool& is_expired)
{
    uint64_t transfered = is_download ? current_progress.downloaded : current_progress.uploaded;
    uint64_t transferable = is_download ? current_progress.downloadable : current_progress.uploadable;
    double estimate = is_download ? current_progress.download_estimate : current_progress.upload_estimate;

    if (!is_streaming) {
        // If the sync client has not yet processed all of the local
        // transactions then the uploadable data is incorrect and we should
        // not invoke the callback
        if (!is_download && snapshot_version > current_progress.snapshot_version)
            return [] {};

        // If this is a non-streaming download progress update and this notifier was
        // created for a later query version (e.g. we're currently downloading
        // subscription set version zero, but subscription set version 1 existed
        // when the notifier was registered), then we want to skip this callback.
        if (is_download && current_progress.query_version < pending_query_version) {
            return [] {};
        }

        // The initial download size we get from the server is the uncompacted
        // size, and so the download may complete before we actually receive
        // that much data. When that happens, transferrable will drop and we
        // need to use the new value instead of the captured one.
        if (!captured_transferable || *captured_transferable > transferable)
            captured_transferable = transferable;
        transferable = *captured_transferable;

        // Since we can adjust the transferrable downwards the estimate for uploads
        // won't be correct since the sync client's view of the estimate is based on
        // the total number of uploadable bytes available rather than the number of
        // bytes this NotifierPackage was waiting to upload.
        if (!is_download) {
            estimate = transferable > 0 ? std::min(transfered / double(transferable), 1.0) : 0.0;
        }
    }

    // A notifier is expired if at least as many bytes have been transferred
    // as were originally considered transferrable.
    is_expired =
        !is_streaming && (transfered >= transferable && (!is_download || !pending_query_version || estimate >= 1.0));
    return [=, notifier = notifier] {
        notifier(transfered, transferable, estimate);
    };
}

uint64_t SyncSession::ConnectionChangeNotifier::add_callback(std::function<ConnectionStateChangeCallback> callback)
{
    std::lock_guard<std::mutex> lock(m_callback_mutex);
    auto token = m_next_token++;
    m_callbacks.push_back({std::move(callback), token});
    return token;
}

void SyncSession::ConnectionChangeNotifier::remove_callback(uint64_t token)
{
    Callback old;
    {
        std::lock_guard<std::mutex> lock(m_callback_mutex);
        auto it = std::find_if(begin(m_callbacks), end(m_callbacks), [=](const auto& c) {
            return c.token == token;
        });
        if (it == end(m_callbacks)) {
            return;
        }

        size_t idx = distance(begin(m_callbacks), it);
        if (m_callback_index != npos) {
            if (m_callback_index >= idx)
                --m_callback_index;
        }
        --m_callback_count;

        old = std::move(*it);
        m_callbacks.erase(it);
    }
}

void SyncSession::ConnectionChangeNotifier::remove_callbacks()
{
    std::lock_guard<std::mutex> lock(m_callback_mutex);
    m_callbacks.clear();
    m_callback_count = -1;
    m_callback_index = -1;
}

void SyncSession::ConnectionChangeNotifier::invoke_callbacks(ConnectionState old_state, ConnectionState new_state)
{
    std::unique_lock lock(m_callback_mutex);
    m_callback_count = m_callbacks.size();
    for (++m_callback_index; m_callback_index < m_callback_count; ++m_callback_index) {
        // acquire a local reference to the callback so that removing the
        // callback from within it can't result in a dangling pointer
        auto cb = m_callbacks[m_callback_index].fn;
        lock.unlock();
        cb(old_state, new_state);
        lock.lock();
    }
    m_callback_index = npos;
}

util::Future<std::string> SyncSession::send_test_command(std::string body)
{
    util::CheckedLockGuard lk(m_state_mutex);
    if (!m_session) {
        return Status{ErrorCodes::RuntimeError, "Session doesn't exist to send test command on"};
    }

    return m_session->send_test_command(std::move(body));
}

void SyncSession::migrate_schema(util::UniqueFunction<void(Status)>&& callback)
{
    util::CheckedUniqueLock lock(m_state_mutex);
    // If the schema migration is already in progress, just wait to complete.
    if (m_schema_migration_in_progress) {
        add_completion_callback(std::move(callback), ProgressDirection::download);
        return;
    }
    m_schema_migration_in_progress = true;

    // Perform the migration:
    //  1. Pause the sync session
    //  2. Once the sync client releases the realm file:
    //      a. Delete all tables (private and public)
    //      b. Reset the subscription store
    //      d. Empty the sync history and adjust cursors
    //      e. Reset file ident (the server flags the old ident as in the case of a client reset)
    // 3. Resume the session (the client asks for a new file ident)
    // See `sync_schema_migration::perform_schema_migration` for more details.

    CompletionCallbacks callbacks;
    std::swap(m_completion_callbacks, callbacks);
    auto guard = util::make_scope_exit([&]() noexcept {
        util::CheckedUniqueLock lock(m_state_mutex);
        if (m_completion_callbacks.empty())
            std::swap(callbacks, m_completion_callbacks);
        else
            m_completion_callbacks.merge(std::move(callbacks));
    });
    m_state_mutex.unlock(lock);

    auto future = pause_async();
    std::move(future).get_async(
        [callback = std::move(callback), weak_session = weak_from_this()](Status status) mutable {
            if (!status.is_ok())
                return callback(status);

            auto session = weak_session.lock();
            if (!session) {
                status = Status(ErrorCodes::InvalidSession, "Sync session was destroyed during schema migration");
                return callback(status);
            }
            sync_schema_migration::perform_schema_migration(*session->m_db);
            {
                util::CheckedUniqueLock lock(session->m_state_mutex);
                session->m_previous_schema_version.reset();
                session->m_schema_migration_in_progress = false;
                session->m_subscription_store_base.reset();
                session->m_flx_subscription_store.reset();
            }
            session->update_subscription_store(true, {});
            session->wait_for_download_completion(std::move(callback));
            session->resume();
        });
}
