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
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/thread_safe_reference.hpp>

namespace realm {

AsyncOpenTask::AsyncOpenTask(std::shared_ptr<_impl::RealmCoordinator> coordinator,
                             std::shared_ptr<realm::SyncSession> session, bool db_first_open)
    : m_coordinator(coordinator)
    , m_session(session)
    , m_db_first_open(db_first_open)
{
}

void AsyncOpenTask::start(AsyncOpenCallback async_open_complete)
{
    util::CheckedUniqueLock lock(m_mutex);
    if (!m_session)
        return;
    auto session = m_session;
    lock.unlock();

    std::shared_ptr<AsyncOpenTask> self(shared_from_this());
    session->wait_for_download_completion([async_open_complete = std::move(async_open_complete), self,
                                           this](Status status) mutable {
        std::shared_ptr<_impl::RealmCoordinator> coordinator;
        {
            util::CheckedLockGuard lock(m_mutex);
            if (!m_session)
                return; // Swallow all events if the task has been cancelled.

            // Hold on to the coordinator until after we've called the callback
            coordinator = std::move(m_coordinator);
        }

        if (!status.is_ok()) {
            self->async_open_complete(std::move(async_open_complete), coordinator, status);
            return;
        }

        auto config = coordinator->get_config();
        if (config.sync_config && config.sync_config->flx_sync_requested &&
            config.sync_config->subscription_initializer) {
            const bool rerun_on_launch = config.sync_config->rerun_init_subscription_on_open;
            self->attach_to_subscription_initializer(std::move(async_open_complete), coordinator, rerun_on_launch);
        }
        else {
            self->async_open_complete(std::move(async_open_complete), coordinator, status);
        }
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

uint64_t
AsyncOpenTask::register_download_progress_notifier(std::function<SyncSession::ProgressNotifierCallback>&& callback)
{
    util::CheckedLockGuard lock(m_mutex);
    if (m_session) {
        auto token = m_session->register_progress_notifier(std::move(callback),
                                                           SyncSession::ProgressDirection::download, false);
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

void AsyncOpenTask::attach_to_subscription_initializer(AsyncOpenCallback&& async_open_callback,
                                                       std::shared_ptr<_impl::RealmCoordinator> coordinator,
                                                       bool rerun_on_launch)
{
    // Attaching the subscription initializer to the latest subscription that was committed.
    // This is going to be enough, for waiting that the subscription committed by init_subscription_initializer has
    // been completed (either if it is the first time that the file is created or if rerun on launch was set to true).
    // If the same Realm file is already opened, there is the possibility that this code may wait on a subscription
    // that was not committed by init_subscription_initializer.

    auto shared_realm = coordinator->get_realm(nullptr, m_db_first_open);
    const auto init_subscription = shared_realm->get_latest_subscription_set();
    const auto sub_state = init_subscription.state();

    if ((sub_state != sync::SubscriptionSet::State::Complete) || (m_db_first_open && rerun_on_launch)) {
        // We need to wait until subscription initializer completes
        std::shared_ptr<AsyncOpenTask> self(shared_from_this());
        init_subscription.get_state_change_notification(sync::SubscriptionSet::State::Complete)
            .get_async([self, coordinator, async_open_callback = std::move(async_open_callback)](
                           StatusWith<realm::sync::SubscriptionSet::State> state) mutable {
                self->async_open_complete(std::move(async_open_callback), coordinator, state.get_status());
            });
    }
    else {
        async_open_complete(std::move(async_open_callback), coordinator, Status::OK());
    }
}

void AsyncOpenTask::async_open_complete(AsyncOpenCallback&& callback,
                                        std::shared_ptr<_impl::RealmCoordinator> coordinator, Status status)
{
    {
        util::CheckedLockGuard lock(m_mutex);
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

} // namespace realm
