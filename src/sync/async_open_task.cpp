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

#include "impl/realm_coordinator.hpp"
#include "sync/async_open_task.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_session.hpp"

namespace realm {

AsyncOpenTask::AsyncOpenTask(std::shared_ptr<_impl::RealmCoordinator> coordinator, std::shared_ptr<realm::SyncSession> session)
: m_coordinator(coordinator)
, m_session(session)
{
}

void AsyncOpenTask::start(std::function<void(std::shared_ptr<Realm>, std::exception_ptr)> callback)
{
    std::weak_ptr<AsyncOpenTask> weak_self(shared_from_this());
    m_session->wait_for_download_completion([callback, weak_self](std::error_code ec) {
        if (auto self = weak_self.lock()) {
            if (self->m_canceled)
                return; // Swallow all events if the task as been canceled.

            if (ec)
                callback(nullptr, std::make_exception_ptr(std::system_error(ec)));
            else {
                std::shared_ptr<Realm> realm;
                try {
                    realm = self->m_coordinator->get_realm();
                }
                catch (...) {
                    return callback(nullptr, std::current_exception());
                }
                callback(realm, nullptr);
            }
        }
    });
}

void AsyncOpenTask::cancel()
{
    if (m_session) {
        // Does a better way exists for canceling the download?
        m_canceled = true;
        m_session->log_out();
        m_session = nullptr;
        m_coordinator = nullptr;
    }
}

uint64_t AsyncOpenTask::register_download_progress_notifier(std::function<SyncProgressNotifierCallback> callback)
{
    if (m_session) {
        return m_session->register_progress_notifier(callback, realm::SyncSession::NotifierType::download, false);
    }
    else {
        return 0;
    }
}

void AsyncOpenTask::unregister_download_progress_notifier(uint64_t token)
{
    if (m_session)
        m_session->unregister_progress_notifier(token);
}

}
