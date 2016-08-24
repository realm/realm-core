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

#include <realm/sync/client.hpp>

namespace realm {

namespace _impl {

struct SyncClient;

struct SyncSession {
    SyncSession(std::shared_ptr<SyncClient>, std::string realm_path);

    void set_sync_transact_callback(std::function<sync::Session::SyncTransactCallback>);
    void set_error_handler(std::function<sync::Session::ErrorHandler>);
    void nonsync_transact_notify(sync::Session::version_type);

    void refresh_sync_access_token(std::string access_token, util::Optional<std::string> server_url);

private:
    std::shared_ptr<SyncClient> m_client;
    sync::Session m_session;
    bool m_awaits_user_token = true;
    util::Optional<int_fast64_t> m_deferred_commit_notification;

    // The fully-resolved URL of this Realm, including the server and the path.
    util::Optional<std::string> m_server_url;
};

}
}

#endif // REALM_OS_SYNC_SESSION_HPP
