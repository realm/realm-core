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
                             std::shared_ptr<realm::SyncSession> session)
    : m_coordinator(coordinator)
    , m_session(session)
{
}

void AsyncOpenTask::start(AsyncOpenCallback async_open_complete, SubscriptionCallback subscription_initializer)
{
    util::CheckedUniqueLock lock(m_mutex);
    if (!m_session)
        return;
    auto session = m_session;
    lock.unlock();

    std::shared_ptr<AsyncOpenTask> self(shared_from_this());
    session->wait_for_download_completion([async_open_complete = std::move(async_open_complete),
                                           subscription_initializer = std::move(subscription_initializer), self,
                                           this](Status status) mutable {
        std::shared_ptr<_impl::RealmCoordinator> coordinator;
        {
            util::CheckedLockGuard lock(m_mutex);
            if (!m_session)
                return; // Swallow all events if the task has been cancelled.

            for (auto token : m_registered_callbacks) {
                m_session->unregister_progress_notifier(token);
            }
            m_session = nullptr;
            // Hold on to the coordinator until after we've called the callback
            coordinator = std::move(m_coordinator);
        }

        if (!status.is_ok())
            self->async_open_complete(std::move(async_open_complete), coordinator, status);

        auto config = coordinator->get_config();
        if (subscription_initializer) {
            self->run_subscription_initializer(std::move(subscription_initializer), std::move(async_open_complete),
                                               coordinator);
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

void AsyncOpenTask::run_subscription_initializer(SubscriptionCallback&& subscription_callback,
                                                 AsyncOpenCallback&& async_open_callback,
                                                 std::shared_ptr<_impl::RealmCoordinator> coordinator)
{
    auto shared_realm = coordinator->get_realm();
    subscription_callback(shared_realm);
    auto committed_subscription = shared_realm->get_active_subscription_set();
    std::shared_ptr<AsyncOpenTask> self(shared_from_this());
    committed_subscription.get_state_change_notification(sync::SubscriptionSet::State::Complete)
        .get_async([self, coordinator, async_open_callback = std::move(async_open_callback)](
                       StatusWith<realm::sync::SubscriptionSet::State> state) mutable {
            self->async_open_complete(std::move(async_open_callback), coordinator, state.get_status());
        });
}

void AsyncOpenTask::async_open_complete(AsyncOpenCallback&& callback,
                                        std::shared_ptr<_impl::RealmCoordinator> coordinator, Status status)
{
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
