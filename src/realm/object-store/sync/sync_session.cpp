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
#include <realm/object-store/sync/impl/sync_metadata.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_user.hpp>

#include <realm/db_options.hpp>
#include <realm/sync/client.hpp>
#include <realm/sync/config.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset_operation.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/util/websocket.hpp>

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

    // when entering from the Dying state the session will still be bound
    if (!m_session) {
        create_sync_session();
        m_session->bind();
    }

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
    m_session->async_wait_for_upload_completion(
        [weak_session = weak_from_this(), current_death_count](std::error_code) {
            if (auto session = weak_session.lock()) {
                util::CheckedUniqueLock lock(session->m_state_mutex);
                if (session->m_state == State::Dying && session->m_death_count == current_death_count) {
                    session->become_inactive(std::move(lock));
                }
            }
        });
    m_state_mutex.unlock(lock);
}

void SyncSession::become_inactive(util::CheckedUniqueLock lock, std::error_code ec)
{
    REALM_ASSERT(m_state != State::Inactive);
    m_state = State::Inactive;

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
    auto& sync_manager = *m_sync_manager;
    m_state_mutex.unlock(lock);
    sync_manager.unregister_session(m_db->get_path());

    // Send notifications after releasing the lock to prevent deadlocks in the callback.
    if (old_state != new_state) {
        m_connection_change_notifier.invoke_callbacks(old_state, connection_state());
    }

    if (!ec)
        ec = make_error_code(util::error::operation_aborted);

    // Inform any queued-up completion handlers that they were cancelled.
    for (auto& [id, callback] : waits)
        callback.second(ec);
}

void SyncSession::become_waiting_for_access_token()
{
    REALM_ASSERT(m_state != State::WaitingForAccessToken);
    m_state = State::WaitingForAccessToken;
}

void SyncSession::handle_bad_auth(const std::shared_ptr<SyncUser>& user, std::error_code error_code,
                                  const std::string& context_message)
{
    // TODO: ideally this would write to the logs as well in case users didn't set up their error handler.
    {
        util::CheckedUniqueLock lock(m_state_mutex);
        cancel_pending_waits(std::move(lock), error_code);
    }
    if (user) {
        user->log_out();
    }

    if (auto error_handler = config(&SyncConfig::error_handler)) {
        auto user_facing_error = SyncError(realm::sync::ProtocolError::bad_authentication, context_message, true);
        error_handler(shared_from_this(), std::move(user_facing_error));
    }
}

util::UniqueFunction<void(util::Optional<app::AppError>)>
SyncSession::handle_refresh(const std::shared_ptr<SyncSession>& session)
{
    return [session](util::Optional<app::AppError> error) {
        auto session_user = session->user();
        if (!session_user) {
            util::CheckedUniqueLock lock(session->m_state_mutex);
            session->cancel_pending_waits(std::move(lock), error ? error->error_code : std::error_code());
        }
        else if (error) {
            if (error->error_code == app::make_client_error_code(app::ClientErrorCode::app_deallocated)) {
                return; // this response came in after the app shut down, ignore it
            }
            else if (error->error_code.category() == app::client_error_category()) {
                // any other client errors other than app_deallocated are considered fatal because
                // there was a problem locally before even sending the request to the server
                // eg. ClientErrorCode::user_not_found, ClientErrorCode::user_not_logged_in
                session->handle_bad_auth(session_user, error->error_code, error->message);
            }
            else if (error->http_status_code &&
                     (*error->http_status_code == 401 || *error->http_status_code == 403)) {
                // A 401 response on a refresh request means that the token cannot be refreshed and we should not
                // retry. This can be because an admin has revoked this user's sessions, the user has been disabled,
                // or the refresh token has expired according to the server's clock.
                session->handle_bad_auth(session_user, error->error_code, "Unable to refresh the user access token.");
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
                if (session->m_state == State::WaitingForAccessToken) {
                    session->become_active();
                }
            }
        }
        else {
            session->update_access_token(session_user->access_token());
        }
    };
}

SyncSession::SyncSession(SyncClient& client, std::shared_ptr<DB> db, SyncConfig config, SyncManager* sync_manager)
    : m_config(std::move(config))
    , m_db(std::move(db))
    , m_flx_subscription_store([this](bool use_flx_sync) -> std::shared_ptr<sync::SubscriptionStore> {
        if (!use_flx_sync) {
            return nullptr;
        }

        return sync::SubscriptionStore::create(m_db, [this](int64_t new_version) {
            util::CheckedLockGuard lk(m_state_mutex);
            if (m_state != State::Active && m_state != State::WaitingForAccessToken) {
                return;
            }
            // There may be no session yet (i.e., waiting to refresh the access token).
            if (m_session) {
                m_session->on_new_flx_sync_subscription(new_version);
            }
        });
    }(m_config.flx_sync_requested))
    , m_client(client)
    , m_sync_manager(sync_manager)
{
    if (m_config.flx_sync_requested) {
        std::weak_ptr<sync::SubscriptionStore> weak_sub_mgr(m_flx_subscription_store);
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
                        throw NoSubscriptionForWrite(util::format(
                            "Cannot write to class %1 when no flexible sync subscription has been created.",
                            object_class_name));
                    }
                };
            });
    }
}

std::shared_ptr<SyncManager> SyncSession::sync_manager() const
{
    util::CheckedLockGuard lk(m_state_mutex);
    REALM_ASSERT(m_sync_manager);
    return m_sync_manager->shared_from_this();
}

void SyncSession::detach_from_sync_manager()
{
    shutdown_and_wait();
    util::CheckedLockGuard lk(m_state_mutex);
    m_sync_manager = nullptr;
}

void SyncSession::update_error_and_mark_file_for_deletion(SyncError& error, ShouldBackup should_backup)
{
    util::CheckedLockGuard config_lock(m_config_mutex);
    // Add a SyncFileActionMetadata marking the Realm as needing to be deleted.
    std::string recovery_path;
    auto original_path = path();
    error.user_info[SyncError::c_original_file_path_key] = original_path;
    if (should_backup == ShouldBackup::yes) {
        recovery_path =
            util::reserve_unique_file_name(m_sync_manager->recovery_directory_path(m_config.recovery_directory),
                                           util::create_timestamped_template("recovered_realm"));
        error.user_info[SyncError::c_recovery_file_path_key] = recovery_path;
    }
    using Action = SyncFileActionMetadata::Action;
    auto action = should_backup == ShouldBackup::yes ? Action::BackUpThenDeleteRealm : Action::DeleteRealm;
    m_sync_manager->perform_metadata_update(
        [action, original_path = std::move(original_path), recovery_path = std::move(recovery_path),
         partition_value = m_config.partition_value, identity = m_config.user->identity()](const auto& manager) {
            manager.make_file_action_metadata(original_path, partition_value, identity, action, recovery_path);
        });
}

void SyncSession::download_fresh_realm(util::Optional<SyncError::ClientResetModeAllowed> allowed_mode)
{
    // first check that recovery will not be prevented
    if (allowed_mode && *allowed_mode == SyncError::ClientResetModeAllowed::RecoveryNotPermitted) {
        auto mode = config(&SyncConfig::client_resync_mode);
        if (mode == ClientResyncMode::Recover) {
            handle_fresh_realm_downloaded(
                nullptr, {"A client reset is required but the server does not permit recovery for this client"},
                allowed_mode);
        }
    }
    DBOptions options;
    options.encryption_key = m_db->get_encryption_key();
    options.allow_file_format_upgrade = false;
    options.enable_async_writes = false;

    std::shared_ptr<DB> db;
    auto fresh_path = ClientResetOperation::get_fresh_path_for(m_db->get_path());
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
    catch (std::exception const& e) {
        // Failed to open the fresh path after attempting to delete it, so we
        // just can't do automatic recovery.
        handle_fresh_realm_downloaded(nullptr, std::string(e.what()), allowed_mode);
        return;
    }

    util::CheckedLockGuard state_lock(m_state_mutex);
    if (m_state != State::Active) {
        return;
    }
    std::shared_ptr<SyncSession> sync_session;
    {
        util::CheckedLockGuard config_lock(m_config_mutex);
        SyncConfig config = m_config;
        config.stop_policy = SyncSessionStopPolicy::Immediately;
        config.client_resync_mode = ClientResyncMode::Manual;
        sync_session = create(m_client, db, config, m_sync_manager);
        auto& history = static_cast<sync::ClientReplication&>(*db->get_replication());
        // the fresh Realm may apply writes to this db after it has outlived its sync session
        // the writes are used to generate a changeset for recovery, but are never committed
        history.set_write_validator_factory({});
    }

    sync_session->assert_mutex_unlocked();
    if (m_flx_subscription_store) {
        sync::SubscriptionSet active = m_flx_subscription_store->get_active();
        auto fresh_sub_store = sync_session->get_flx_subscription_store();
        REALM_ASSERT(fresh_sub_store);
        auto fresh_mut_sub = fresh_sub_store->get_latest().make_mutable_copy();
        fresh_mut_sub.import(active);
        std::move(fresh_mut_sub)
            .commit()
            .get_state_change_notification(sync::SubscriptionSet::State::Complete)
            .get_async([=, weak_self = weak_from_this()](StatusWith<sync::SubscriptionSet::State> s) {
                // Keep the sync session alive while it's downloading, but then close
                // it immediately
                sync_session->close();
                if (auto strong_self = weak_self.lock()) {
                    if (s.is_ok()) {
                        strong_self->handle_fresh_realm_downloaded(db, none, allowed_mode);
                    }
                    else {
                        strong_self->handle_fresh_realm_downloaded(nullptr, s.get_status().reason(), allowed_mode);
                    }
                }
            });
    }
    else { // pbs
        sync_session->wait_for_download_completion([=, weak_self = weak_from_this()](std::error_code ec) {
            // Keep the sync session alive while it's downloading, but then close
            // it immediately
            sync_session->close();
            if (auto strong_self = weak_self.lock()) {
                if (ec) {
                    strong_self->handle_fresh_realm_downloaded(nullptr, ec.message(), allowed_mode);
                }
                else {
                    strong_self->handle_fresh_realm_downloaded(db, none, allowed_mode);
                }
            }
        });
    }
    sync_session->revive_if_needed();
}

void SyncSession::handle_fresh_realm_downloaded(DBRef db, util::Optional<std::string> error_message,
                                                util::Optional<SyncError::ClientResetModeAllowed> allowed_mode)
{
    util::CheckedUniqueLock lock(m_state_mutex);
    if (m_state != State::Active) {
        return;
    }
    // The download can fail for many reasons. For example:
    // - unable to write the fresh copy to the file system
    // - during download of the fresh copy, the fresh copy itself is reset
    // - in FLX mode there was a problem fulfilling the previously active subscription
    if (error_message) {
        lock.unlock();
        if (m_flx_subscription_store) {
            // In DiscardLocal mode, only the active subscription set is preserved
            // this means that we have to remove all other subscriptions including later
            // versioned ones.
            auto mut_sub = m_flx_subscription_store->get_active().make_mutable_copy();
            m_flx_subscription_store->supercede_all_except(mut_sub);
            mut_sub.update_state(sync::SubscriptionSet::State::Error,
                                 util::make_optional<std::string_view>(*error_message));
            std::move(mut_sub).commit();
        }
        const bool is_fatal = true;
        SyncError synthetic(make_error_code(sync::Client::Error::auto_client_reset_failure),
                            util::format("A fatal error occured during client reset: '%1'", error_message), is_fatal);
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
        m_force_client_reset = allowed_mode.value_or(SyncError::ClientResetModeAllowed::RecoveryPermitted);
        m_client_reset_fresh_copy = db;
        CompletionCallbacks callbacks;
        std::swap(m_completion_callbacks, callbacks);
        // always swap back, even if advance_state throws
        auto guard = util::make_scope_exit([&]() noexcept {
            util::CheckedUniqueLock lock(m_state_mutex);
            if (m_completion_callbacks.empty())
                std::swap(callbacks, m_completion_callbacks);
            else
                m_completion_callbacks.merge(std::move(callbacks));
        });
        become_inactive(std::move(lock)); // unlocks the lock
    }
    revive_if_needed();
}

// This method should only be called from within the error handler callback registered upon the underlying
// `m_session`.
void SyncSession::handle_error(SyncError error)
{
    enum class NextStateAfterError { none, inactive, error };
    auto next_state = error.is_fatal ? NextStateAfterError::error : NextStateAfterError::none;
    auto error_code = error.error_code;
    util::Optional<ShouldBackup> delete_file;
    bool log_out_user = false;

    if (error_code == make_error_code(sync::Client::Error::auto_client_reset_failure)) {
        // At this point, automatic recovery has been attempted but it failed.
        // Fallback to a manual reset and let the user try to handle it.
        next_state = NextStateAfterError::inactive;
        delete_file = ShouldBackup::yes;
    }
    else if (error_code.category() == realm::sync::protocol_error_category()) {
        SimplifiedProtocolError simplified =
            get_simplified_error(static_cast<sync::ProtocolError>(error_code.value()));
        if (error.server_requests_client_reset) {
            // we have specific instructions from the server concerning client resets
            switch (*error.server_requests_client_reset) {
                case SyncError::ClientResetModeAllowed::DoNotClientReset:
                    if (simplified == SimplifiedProtocolError::ClientResetRequested) {
                        // Server says not to do a client reset, but the client wants
                        // to do one; make a fatal (non-reset) error instead.
                        simplified = SimplifiedProtocolError::UnexpectedInternalIssue;
                    }
                    break;
                case SyncError::ClientResetModeAllowed::RecoveryPermitted:
                    [[fallthrough]];
                case SyncError::ClientResetModeAllowed::RecoveryNotPermitted:
                    simplified = SimplifiedProtocolError::ClientResetRequested;
                    break;
            }
        }
        switch (simplified) {
            case SimplifiedProtocolError::ConnectionIssue:
                // Not real errors, don't need to be reported to the binding.
                return;
            case SimplifiedProtocolError::UnexpectedInternalIssue:
                break; // fatal: bubble these up to the user below
            case SimplifiedProtocolError::SessionIssue:
                // The SDK doesn't need to be aware of these because they are strictly informational, and do not
                // represent actual errors.
                return;
            case SimplifiedProtocolError::CompensatingWrite:
                break; // not fatal, but should be bubbled up to the user below.
            case SimplifiedProtocolError::BadAuthentication:
                next_state = NextStateAfterError::inactive;
                log_out_user = true;
                break;
            case SimplifiedProtocolError::PermissionDenied:
                next_state = NextStateAfterError::inactive;
                delete_file = ShouldBackup::no;
                break;
            case SimplifiedProtocolError::ClientResetRequested:
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
                        download_fresh_realm(error.server_requests_client_reset);
                        return; // do not propgate the error to the user at this point
                }
                break;
        }
    }
    else if (error_code.category() == realm::sync::client_error_category()) {
        using ClientError = realm::sync::ClientError;
        switch (static_cast<ClientError>(error_code.value())) {
            case ClientError::connection_closed:
            case ClientError::pong_timeout:
                // Not real errors, don't need to be reported to the SDK.
                return;
            case ClientError::bad_changeset:
            case ClientError::bad_changeset_header_syntax:
            case ClientError::bad_changeset_size:
            case ClientError::bad_client_file_ident:
            case ClientError::bad_client_file_ident_salt:
            case ClientError::bad_client_version:
            case ClientError::bad_compression:
            case ClientError::bad_error_code:
            case ClientError::bad_file_ident:
            case ClientError::bad_message_order:
            case ClientError::bad_origin_file_ident:
            case ClientError::bad_progress:
            case ClientError::bad_protocol_from_server:
            case ClientError::bad_request_ident:
            case ClientError::bad_server_version:
            case ClientError::bad_session_ident:
            case ClientError::bad_state_message:
            case ClientError::bad_syntax:
            case ClientError::bad_timestamp:
            case ClientError::client_too_new_for_server:
            case ClientError::client_too_old_for_server:
            case ClientError::connect_timeout:
            case ClientError::limits_exceeded:
            case ClientError::protocol_mismatch:
            case ClientError::ssl_server_cert_rejected:
            case ClientError::missing_protocol_feature:
            case ClientError::unknown_message:
            case ClientError::http_tunnel_failed:
            case ClientError::auto_client_reset_failure:
                // Don't do anything special for these errors.
                // Future functionality may require special-case handling for existing
                // errors, or newly introduced error codes.
                break;
        }
    }
    else {
        // The server replies with '401: unauthorized' if the access token is invalid, expired, revoked, or the user
        // is disabled. In this scenario we attempt an automatic token refresh and if that succeeds continue as
        // normal. If the refresh request also fails with 401 then we need to stop retrying and pass along the error;
        // see handle_refresh().
        if (error_code == util::websocket::make_error_code(util::websocket::Error::bad_response_401_unauthorized)) {
            if (auto u = user()) {
                u->refresh_custom_data(handle_refresh(shared_from_this()));
                return;
            }
        }
        // Unrecognized error code.
        error.is_unrecognized_by_client = true;
    }

    util::CheckedUniqueLock lock(m_state_mutex);
    if (delete_file)
        update_error_and_mark_file_for_deletion(error, *delete_file);

    if (m_state == State::Dying && error.is_fatal) {
        become_inactive(std::move(lock));
        return;
    }

    // Dont't bother invoking m_config.error_handler if the sync is inactive.
    // It does not make sense to call the handler when the session is closed.
    if (m_state == State::Inactive) {
        return;
    }

    switch (next_state) {
        case NextStateAfterError::none:
            if (config(&SyncConfig::cancel_waits_on_nonfatal_error)) {
                cancel_pending_waits(std::move(lock), error.error_code); // unlocks the mutex
            }
            break;
        case NextStateAfterError::inactive: {
            become_inactive(std::move(lock), error.error_code);
            break;
        }
        case NextStateAfterError::error: {
            cancel_pending_waits(std::move(lock), error.error_code);
            break;
        }
    }

    if (log_out_user) {
        if (auto u = user())
            u->log_out();
    }

    if (auto error_handler = config(&SyncConfig::error_handler)) {
        error_handler(shared_from_this(), std::move(error));
    }
}

void SyncSession::cancel_pending_waits(util::CheckedUniqueLock lock, std::error_code error)
{
    CompletionCallbacks callbacks;
    std::swap(callbacks, m_completion_callbacks);
    m_state_mutex.unlock(lock);

    // Inform any queued-up completion handlers that they were cancelled.
    for (auto& [id, callback] : callbacks)
        callback.second(error);
}

void SyncSession::handle_progress_update(uint64_t downloaded, uint64_t downloadable, uint64_t uploaded,
                                         uint64_t uploadable, uint64_t download_version, uint64_t snapshot_version)
{
    m_progress_notifier.update(downloaded, downloadable, uploaded, uploadable, download_version, snapshot_version);
}

static sync::Session::Config::ClientReset make_client_reset_config(SyncConfig& session_config, DBRef&& fresh_copy,
                                                                   bool recovery_is_allowed)
{
    sync::Session::Config::ClientReset config;
    REALM_ASSERT(session_config.client_resync_mode != ClientResyncMode::Manual);
    config.mode = session_config.client_resync_mode;
    config.notify_after_client_reset = [notify = session_config.notify_after_client_reset](
                                           std::string local_path, VersionID previous_version, bool did_recover) {
        REALM_ASSERT(!local_path.empty());
        SharedRealm frozen_before;
        ThreadSafeReference active_after;
        if (auto local_coordinator = RealmCoordinator::get_existing_coordinator(local_path)) {
            auto local_config = local_coordinator->get_config();
            active_after = local_coordinator->get_unbound_realm();
            local_config.scheduler = nullptr;
            frozen_before = local_coordinator->get_realm(local_config, previous_version);
            REALM_ASSERT(frozen_before);
            REALM_ASSERT(frozen_before->is_frozen());
        }
        if (notify) {
            notify(frozen_before, std::move(active_after), did_recover);
        }
    };
    config.notify_before_client_reset = [notify = session_config.notify_before_client_reset](std::string local_path) {
        REALM_ASSERT(!local_path.empty());
        SharedRealm frozen_local;
        Realm::Config local_config;
        if (auto local_coordinator = RealmCoordinator::get_existing_coordinator(local_path)) {
            local_config = local_coordinator->get_config();
            local_config.scheduler = nullptr;
            frozen_local = local_coordinator->get_realm(local_config, VersionID());
            REALM_ASSERT(frozen_local);
            REALM_ASSERT(frozen_local->is_frozen());
        }
        if (notify) {
            notify(frozen_local);
        }
    };
    config.fresh_copy = std::move(fresh_copy);
    config.recovery_is_allowed = recovery_is_allowed;
    return config;
}


void SyncSession::create_sync_session()
{
    if (m_session)
        return;

    util::CheckedLockGuard config_lock(m_config_mutex);

    REALM_ASSERT(m_config.user);

    sync::Session::Config session_config;
    session_config.signed_user_token = m_config.user->access_token();
    session_config.realm_identifier = m_config.partition_value;
    session_config.verify_servers_ssl_certificate = m_config.client_validate_ssl;
    session_config.ssl_trust_certificate_path = m_config.ssl_trust_certificate_path;
    session_config.ssl_verify_callback = m_config.ssl_verify_callback;
    session_config.proxy_config = m_config.proxy_config;
    if (m_config.on_download_message_received_hook) {
        session_config.on_download_message_received_hook =
            [hook = m_config.on_download_message_received_hook, anchor = weak_from_this()](
                const sync::SyncProgress& progress, int64_t query_version, sync::DownloadBatchState batch_state) {
                hook(anchor, progress, query_version, batch_state);
            };
    }
    if (m_config.on_bootstrap_message_processed_hook) {
        session_config.on_bootstrap_message_processed_hook =
            [hook = m_config.on_bootstrap_message_processed_hook,
             anchor = weak_from_this()](const sync::SyncProgress& progress, int64_t query_version,
                                        sync::DownloadBatchState batch_state) -> bool {
            return hook(anchor, progress, query_version, batch_state);
        };
    }

    {
        std::string sync_route = m_sync_manager->sync_route();

        if (!m_client.decompose_server_url(sync_route, session_config.protocol_envelope,
                                           session_config.server_address, session_config.server_port,
                                           session_config.service_identifier)) {
            throw sync::BadServerUrl();
        }
        // FIXME: Java needs the fully resolved URL for proxy support, but we also need it before
        // the session is created. How to resolve this?
        m_server_url = sync_route;
    }

    if (m_config.authorization_header_name) {
        session_config.authorization_header_name = *m_config.authorization_header_name;
    }
    session_config.custom_http_headers = m_config.custom_http_headers;

    if (m_force_client_reset) {
        const bool allowed_to_recover = *m_force_client_reset == SyncError::ClientResetModeAllowed::RecoveryPermitted;
        session_config.client_reset_config =
            make_client_reset_config(m_config, std::move(m_client_reset_fresh_copy), allowed_to_recover);
        m_force_client_reset = util::none;
    }

    m_session = m_client.make_session(m_db, m_flx_subscription_store, std::move(session_config));

    std::weak_ptr<SyncSession> weak_self = weak_from_this();

    // Configure the sync transaction callback.
    auto wrapped_callback = [weak_self](VersionID old_version, VersionID new_version) {
        if (auto self = weak_self.lock()) {
            util::CheckedLockGuard l(self->m_state_mutex);
            if (self->m_sync_transact_callback) {
                self->m_sync_transact_callback(old_version, new_version);
            }
        }
    };
    m_session->set_sync_transact_callback(std::move(wrapped_callback));

    // Set up the wrapped progress handler callback
    m_session->set_progress_handler([weak_self](uint_fast64_t downloaded, uint_fast64_t downloadable,
                                                uint_fast64_t uploaded, uint_fast64_t uploadable,
                                                uint_fast64_t progress_version, uint_fast64_t snapshot_version) {
        if (auto self = weak_self.lock()) {
            self->handle_progress_update(downloaded, downloadable, uploaded, uploadable, progress_version,
                                         snapshot_version);
        }
    });

    // Sets up the connection state listener. This callback is used for both reporting errors as well as changes to
    // the connection state.
    m_session->set_connection_state_change_listener(
        [weak_self](sync::ConnectionState state, util::Optional<sync::Session::ErrorInfo> error) {
            // If the OS SyncSession object is destroyed, we ignore any events from the underlying Session as there is
            // nothing useful we can do with them.
            auto self = weak_self.lock();
            if (!self) {
                return;
            }
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
            util::CheckedUniqueLock lock(self->m_connection_state_mutex);
            auto old_state = self->m_connection_state;
            self->m_connection_state = new_state;
            lock.unlock();

            if (old_state != new_state) {
                self->m_connection_change_notifier.invoke_callbacks(old_state, new_state);
            }

            if (error) {
                SyncError sync_error{error->error_code, std::string(error->message), error->is_fatal(),
                                     error->log_url, std::move(error->compensating_writes)};
                if (error->should_client_reset) {
                    if (*error->should_client_reset) {
                        sync_error.server_requests_client_reset =
                            error->client_reset_recovery_is_disabled
                                ? SyncError::ClientResetModeAllowed::RecoveryNotPermitted
                                : SyncError::ClientResetModeAllowed::RecoveryPermitted;
                    }
                    else {
                        sync_error.server_requests_client_reset = SyncError::ClientResetModeAllowed::DoNotClientReset;
                    }
                }
                self->handle_error(std::move(sync_error));
            }
        });
}

void SyncSession::set_sync_transact_callback(util::UniqueFunction<sync::Session::SyncTransactCallback> callback)
{
    util::CheckedLockGuard l(m_state_mutex);
    m_sync_transact_callback = std::move(callback);
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
            break;
    }
}

void SyncSession::revive_if_needed()
{
    util::CheckedUniqueLock lock(m_state_mutex);
    switch (m_state) {
        case State::Active:
        case State::WaitingForAccessToken:
            return;
        case State::Dying:
        case State::Inactive: {
            // Revive.
            auto u = user();
            if (!u || !u->access_token_refresh_required()) {
                become_active();
                return;
            }

            become_waiting_for_access_token();
            // Release the lock for SDKs with a single threaded
            // networking implementation such as our test suite
            // so that the update can trigger a state change from
            // the completion handler.
            lock.unlock();
            initiate_access_token_refresh();
            break;
        }
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
            break;
    }
}

void SyncSession::log_out()
{
    util::CheckedUniqueLock lock(m_state_mutex);
    switch (m_state) {
        case State::Active:
        case State::Dying:
        case State::WaitingForAccessToken:
            become_inactive(std::move(lock));
            break;
        case State::Inactive:
            break;
    }
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
        case State::Inactive: {
            auto& sync_manager = *m_sync_manager;
            m_state_mutex.unlock(lock);
            sync_manager.unregister_session(m_db->get_path());
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
        // Transition immediately to `inactive` state. Calling this function must gurantee that any
        // sync::Session object in SyncSession::m_session that existed prior to the time of invocation
        // must have been destroyed upon return. This allows the caller to follow up with a call to
        // sync::Client::wait_for_session_terminations_or_client_stopped() in order to wait for the
        // Realm file to be closed. This works so long as this SyncSession object remains in the
        // `inactive` state after the invocation of shutdown_and_wait().
        util::CheckedUniqueLock lock(m_state_mutex);
        if (m_state != State::Inactive) {
            become_inactive(std::move(lock));
        }
    }
    m_client.wait_for_session_terminations();
}

void SyncSession::update_access_token(const std::string& signed_token)
{
    util::CheckedUniqueLock lock(m_state_mutex);
    // We don't expect there to be a session when waiting for access token, but if there is, refresh its token.
    // If not, the latest token will be seeded from SyncUser::access_token() on session creation.
    if (m_session) {
        m_session->refresh(signed_token);
    }
    if (m_state == State::WaitingForAccessToken) {
        become_active();
    }
}

void SyncSession::initiate_access_token_refresh()
{
    if (auto session_user = user()) {
        session_user->refresh_custom_data(handle_refresh(shared_from_this()));
    }
}

void SyncSession::add_completion_callback(util::UniqueFunction<void(std::error_code)> callback,
                                          _impl::SyncProgressNotifier::NotifierType direction)
{
    bool is_download = (direction == _impl::SyncProgressNotifier::NotifierType::download);

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

    (m_session.get()->*waiter)([weak_self = weak_from_this(), id = m_completion_request_counter](std::error_code ec) {
        auto self = weak_self.lock();
        if (!self)
            return;
        util::CheckedUniqueLock lock(self->m_state_mutex);
        auto callback_node = self->m_completion_callbacks.extract(id);
        lock.unlock();
        if (callback_node)
            callback_node.mapped().second(ec);
    });
}

void SyncSession::wait_for_upload_completion(util::UniqueFunction<void(std::error_code)>&& callback)
{
    util::CheckedUniqueLock lock(m_state_mutex);
    add_completion_callback(std::move(callback), ProgressDirection::upload);
}

void SyncSession::wait_for_download_completion(util::UniqueFunction<void(std::error_code)>&& callback)
{
    util::CheckedUniqueLock lock(m_state_mutex);
    add_completion_callback(std::move(callback), ProgressDirection::download);
}

bool SyncSession::wait_for_upload_completion()
{
    util::CheckedUniqueLock lock(m_state_mutex);
    if (m_session) {
        return m_session->wait_for_upload_complete_or_client_stopped();
    }
    return false;
}

bool SyncSession::wait_for_download_completion()
{
    util::CheckedUniqueLock lock(m_state_mutex);
    if (m_session) {
        return m_session->wait_for_download_complete_or_client_stopped();
    }
    return false;
}

uint64_t SyncSession::register_progress_notifier(std::function<ProgressNotifierCallback>&& notifier,
                                                 ProgressDirection direction, bool is_streaming)
{
    return m_progress_notifier.register_callback(std::move(notifier), direction, is_streaming);
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

const std::shared_ptr<sync::SubscriptionStore>& SyncSession::get_flx_subscription_store()
{
    return m_flx_subscription_store;
}

void SyncSession::update_configuration(SyncConfig new_config)
{
    while (true) {
        util::CheckedUniqueLock state_lock(m_state_mutex);
        if (m_state != State::Inactive) {
            // Changing the state releases the lock, which means that by the
            // time we reacquire the lock the state may have changed again
            // (either due to one of the callbacks being invoked or another
            // thread coincidentally doing something). We just attempt to keep
            // switching it to inactive until it stays there.
            become_inactive(std::move(state_lock));
            continue;
        }

        util::CheckedUniqueLock config_lock(m_config_mutex);
        REALM_ASSERT(m_state == State::Inactive);
        REALM_ASSERT(!m_session);
        REALM_ASSERT(m_config.user == new_config.user);
        m_config = std::move(new_config);
        break;
    }
    revive_if_needed();
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
                                                 NotifierType direction, bool is_streaming)
{
    util::UniqueFunction<void()> invocation;
    uint64_t token_value = 0;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        token_value = m_progress_notifier_token++;
        NotifierPackage package{std::move(notifier), util::none, m_local_transaction_version, is_streaming,
                                direction == NotifierType::download};
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

void SyncProgressNotifier::update(uint64_t downloaded, uint64_t downloadable, uint64_t uploaded, uint64_t uploadable,
                                  uint64_t download_version, uint64_t snapshot_version)
{
    // Ignore progress messages from before we first receive a DOWNLOAD message
    if (download_version == 0)
        return;

    std::vector<util::UniqueFunction<void()>> invocations;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_current_progress = Progress{uploadable, downloadable, uploaded, downloaded, snapshot_version};

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
    uint64_t transferred = is_download ? current_progress.downloaded : current_progress.uploaded;
    uint64_t transferrable = is_download ? current_progress.downloadable : current_progress.uploadable;
    if (!is_streaming) {
        // If the sync client has not yet processed all of the local
        // transactions then the uploadable data is incorrect and we should
        // not invoke the callback
        if (!is_download && snapshot_version > current_progress.snapshot_version)
            return [] {};

        // The initial download size we get from the server is the uncompacted
        // size, and so the download may complete before we actually receive
        // that much data. When that happens, transferrable will drop and we
        // need to use the new value instead of the captured one.
        if (!captured_transferrable || *captured_transferrable > transferrable)
            captured_transferrable = transferrable;
        transferrable = *captured_transferrable;
    }

    // A notifier is expired if at least as many bytes have been transferred
    // as were originally considered transferrable.
    is_expired = !is_streaming && transferred >= transferrable;
    return [=, notifier = notifier] {
        notifier(transferred, transferrable);
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
