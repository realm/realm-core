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

void AsyncOpenTask::start(std::function<void(ThreadSafeReference, std::exception_ptr)> callback)
{
    util::CheckedLockGuard lock(m_mutex);
    if (!m_session)
        return;

    m_session->revive_if_needed();

    std::shared_ptr<AsyncOpenTask> self(shared_from_this());
    m_session->wait_for_download_completion([callback, self, this](std::error_code ec) {
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

        if (ec)
            return callback({}, std::make_exception_ptr(std::system_error(ec)));

        ThreadSafeReference realm;
        try {
            realm = coordinator->get_unbound_realm();
        }
        catch (...) {
            return callback({}, std::current_exception());
        }
        callback(std::move(realm), nullptr);
    });
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
        session->log_out();
    }
}

uint64_t
AsyncOpenTask::register_download_progress_notifier(std::function<SyncSession::ProgressNotifierCallback> callback)
{
    util::CheckedLockGuard lock(m_mutex);
    if (m_session) {
        auto token = m_session->register_progress_notifier(callback, SyncSession::ProgressDirection::download, false);
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

} // namespace realm
