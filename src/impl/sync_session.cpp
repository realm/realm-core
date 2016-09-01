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

#include <realm/sync/protocol.hpp>

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

void SyncSession::set_error_handler(std::function<SyncSessionErrorHandler> handler)
{
    auto wrapped_handler = [=](int error_code, std::string message) {
        using Error = realm::sync::Error;

        SyncSessionError error_type;
        // Precondition: error_code is a valid realm::sync::Error raw value.
        Error strong_code = static_cast<Error>(error_code);

        switch (strong_code) {
            // Client errors; all ignored (for now)
            case Error::connection_closed:
            case Error::other_error:
            case Error::unknown_message:
            case Error::bad_syntax:
            case Error::limits_exceeded:
            case Error::wrong_protocol_version:
            case Error::bad_session_ident:
            case Error::reuse_of_session_ident:
            case Error::bound_in_other_session:
            case Error::bad_message_order:
                return;
            // Session errors
            case Error::session_closed:
            case Error::other_session_error:
                // The binding doesn't need to be aware of these because they are strictly informational, and do not
                // represent actual errors.
                return;
            case Error::token_expired:
                error_type = SyncSessionError::SessionTokenExpired;
                break;
            case Error::bad_authentication:
                error_type = SyncSessionError::UserFatal;
                break;
            case Error::illegal_realm_path:
            case Error::no_such_realm:
            case Error::bad_server_file_ident:
            case Error::diverging_histories:
            case Error::bad_changeset:
                error_type = SyncSessionError::SessionFatal;
                break;
            case Error::permission_denied:
                error_type = SyncSessionError::AccessDenied;
                break;
            case Error::bad_client_file_ident:
            case Error::bad_server_version:
            case Error::bad_client_version:
                error_type = SyncSessionError::Debug;
                break;
        }
        handler(error_code, message, error_type);
    };
    m_session.set_error_handler(std::move(wrapped_handler));
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
