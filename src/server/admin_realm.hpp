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

#ifndef REALM_JS_ADMIN_REALM_HPP
#define REALM_JS_ADMIN_REALM_HPP

#include "results.hpp"
#include "shared_realm.hpp"
#include "sync/sync_config.hpp"

#include <functional>
#include <memory>

namespace realm {

class SyncUser;
class SyncSession;

class AdminRealmListener : public std::enable_shared_from_this<AdminRealmListener> {
public:
    AdminRealmListener(std::string local_root, std::string server_base_url, std::shared_ptr<SyncUser> user, std::function<SyncBindSessionHandler> bind_callback);
    void start(std::function<void(std::vector<std::string>)> register_callback);

private:
    Realm::Config m_config;
    SharedRealm m_realm;
    Results m_results;
    NotificationToken m_notification_token;
    std::shared_ptr<SyncSession> m_downloading_session;
};

} // namespace realm

#endif // REALM_JS_ADMIN_REALM_HPP
