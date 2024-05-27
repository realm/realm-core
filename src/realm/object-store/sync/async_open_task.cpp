////////////////////////////////////////////////////////////////////////////
//
// Copyright 2019 Realm Inc.
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

#include <realm/object-store/sync/async_open_task.hpp>

#include <realm/sync/subscriptions.hpp>
#include <realm/sync/noinst/sync_schema_migration.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/thread_safe_reference.hpp>

namespace realm {

AsyncOpenTask::AsyncOpenTask(Private, std::shared_ptr<_impl::RealmCoordinator> coordinator,
                             std::shared_ptr<realm::SyncSession> session, bool db_first_open)
    : m_coordinator(coordinator)
    , m_session(session)
    , m_db_first_open(db_first_open)
{
}

void AsyncOpenTask::start(AsyncOpenCallback callback)
{
    util::CheckedUniqueLock lock(m_mutex);
    if (!m_session)
        return;
    auto session = m_session;
    lock.unlock();

    std::shared_ptr<AsyncOpenTask> self(shared_from_this());
    session->wait_for_download_completion([callback = std::move(callback), self, this](Status status) mutable {
        std::shared_ptr<_impl::RealmCoordinator> coordinator;
        {
            util::CheckedLockGuard lock(m_mutex);
            if (!m_session)
                return; // Swallow all events if the task has been cancelled.

            // Hold on to the coordinator until after we've called the callback
            coordinator = std::move(m_coordinator);
        }

        if (!status.is_ok()) {
            self->async_open_complete(std::move(callback), coordinator, status);
            return;
        }

        self->migrate_schema_or_complete(std::move(callback), coordinator);
    });
    session->revive_if_needed();
}

void AsyncOpenTask::cancel()
{
    std::shared_ptr<SyncSession> session;
    {
        util::CheckedLockGuard lock(m_mutex);
        if (!m_session)
            return;

        for (auto token : m_registered_callbacks) {
            m_session->unregister_progress_notifier(token);
        }

        session = std::move(m_session);
        m_coordinator = nullptr;
    }

    // We need to release the mutex before we log the session out as that will invoke the
    // wait_for_download_completion callback which will also attempt to acquire the mutex
    // thus deadlocking.
    if (session) {
        // Does a better way exists for canceling the download?
        session->force_close();
    }
}

uint64_t AsyncOpenTask::register_download_progress_notifier(std::function<ProgressNotifierCallback>&& callback)
{
    util::CheckedLockGuard lock(m_mutex);
    if (m_session) {
        auto token = m_session->register_progress_notifier(std::move(callback),
                                                           SyncSession::ProgressDirection::download, true);
        m_registered_callbacks.emplace_back(token);
        return token;
    }
    return 0;
}

void AsyncOpenTask::unregister_download_progress_notifier(uint64_t token)
{
    util::CheckedLockGuard lock(m_mutex);
    if (m_session)
        m_session->unregister_progress_notifier(token);
}

void AsyncOpenTask::wait_for_bootstrap_or_complete(AsyncOpenCallback&& callback,
                                                   std::shared_ptr<_impl::RealmCoordinator> coordinator,
                                                   Status status)
{
    if (!status.is_ok()) {
        async_open_complete(std::move(callback), coordinator, status);
        return;
    }

    auto config = coordinator->get_config();
    // FlX sync is not used so there is nothing to bootstrap.
    if (!config.sync_config || !config.sync_config->flx_sync_requested) {
        async_open_complete(std::move(callback), coordinator, status);
        return;
    }

    SharedRealm shared_realm;
    try {
        shared_realm = coordinator->get_realm(nullptr, m_db_first_open);
    }
    catch (...) {
        async_open_complete(std::move(callback), coordinator, exception_to_status());
        return;
    }
    const auto subscription_set = shared_realm->get_latest_subscription_set();
    const auto sub_state = subscription_set.state();

    if (sub_state != sync::SubscriptionSet::State::Complete) {
        // We need to wait until subscription initializer completes
        std::shared_ptr<AsyncOpenTask> self(shared_from_this());
        subscription_set.get_state_change_notification(sync::SubscriptionSet::State::Complete)
            .get_async([self, coordinator, callback = std::move(callback)](
                           StatusWith<realm::sync::SubscriptionSet::State> state) mutable {
                self->async_open_complete(std::move(callback), coordinator, state.get_status());
            });
    }
    else {
        async_open_complete(std::move(callback), coordinator, Status::OK());
    }
}

void AsyncOpenTask::async_open_complete(AsyncOpenCallback&& callback,
                                        std::shared_ptr<_impl::RealmCoordinator> coordinator, Status status)
{
    {
        util::CheckedLockGuard lock(m_mutex);
        // 'Cancel' may have been called just before 'async_open_complete' is invoked.
        if (!m_session)
            return;

        for (auto token : m_registered_callbacks) {
            m_session->unregister_progress_notifier(token);
        }
        m_session = nullptr;
    }

    if (status.is_ok()) {
        ThreadSafeReference realm;
        try {
            realm = coordinator->get_unbound_realm();
        }
        catch (...) {
            return callback({}, std::current_exception());
        }
        return callback(std::move(realm), nullptr);
    }
    return callback({}, std::make_exception_ptr(Exception(status)));
}

void AsyncOpenTask::migrate_schema_or_complete(AsyncOpenCallback&& callback,
                                               std::shared_ptr<_impl::RealmCoordinator> coordinator)
{
    util::CheckedUniqueLock lock(m_mutex);
    if (!m_session)
        return;
    auto session = m_session;
    lock.unlock();

    auto pending_migration = [&] {
        auto rt = coordinator->begin_read();
        return _impl::sync_schema_migration::has_pending_migration(*rt);
    }();

    if (!pending_migration) {
        wait_for_bootstrap_or_complete(std::move(callback), coordinator, Status::OK());
        return;
    }

    // Migrate the schema.
    //  * First upload the changes at the old schema version
    //  * Then, pause the session, delete all tables, re-initialize the metadata, and finally restart the session.
    // The lifetime of the task is extended until the bootstrap completes.
    std::shared_ptr<AsyncOpenTask> self(shared_from_this());
    session->wait_for_upload_completion(
        [callback = std::move(callback), coordinator, session, self, this](Status status) mutable {
            {
                util::CheckedLockGuard lock(m_mutex);
                if (!m_session)
                    return; // Swallow all events if the task has been cancelled.
            }

            if (!status.is_ok()) {
                self->async_open_complete(std::move(callback), coordinator, status);
                return;
            }

            auto migration_completed_callback = [callback = std::move(callback), coordinator = std::move(coordinator),
                                                 self](Status status) mutable {
                self->wait_for_bootstrap_or_complete(std::move(callback), coordinator, status);
            };
            SyncSession::Internal::migrate_schema(*session, std::move(migration_completed_callback));
        });
}

} // namespace realm
