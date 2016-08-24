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

#include "sync_session.hpp"

#include "sync_client.hpp"

using namespace realm;
using namespace realm::_impl;

SyncSession::SyncSession(std::shared_ptr<SyncClient> client, std::string realm_path)
    : m_client(std::move(client))
    , m_session(m_client->client, std::move(realm_path))
{
}

void SyncSession::set_sync_transact_callback(std::function<sync::Session::SyncTransactCallback> callback)
{
    m_session.set_sync_transact_callback(std::move(callback));
}

void SyncSession::set_error_handler(std::function<sync::Session::ErrorHandler> handler)
{
    m_session.set_error_handler(std::move(handler));
}

void SyncSession::nonsync_transact_notify(sync::Session::version_type version)
{
    if (!m_awaits_user_token) {
        // Fully ready sync session, notify immediately.
        m_session.nonsync_transact_notify(version);
    }
    else {
        m_deferred_commit_notification = version;
    }
}

void SyncSession::refresh_sync_access_token(std::string access_token, util::Optional<std::string> server_url)
{
    if (!server_url && !m_server_url) {
        return;
    }

    if (m_awaits_user_token) {
        m_awaits_user_token = false;

        // Since the sync session was previously unbound, it's safe to do this from the
        // calling thread.
        if (!m_server_url) {
            m_server_url = std::move(server_url);
        }
        m_session.bind(*m_server_url, std::move(access_token));

        if (m_deferred_commit_notification) {
            m_session.nonsync_transact_notify(*m_deferred_commit_notification);
            m_deferred_commit_notification = {};
        }
    }
    else {
        m_session.refresh(std::move(access_token));
    }
}
