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

#ifndef REALM_OBJECT_STORE_ADMIN_REALM_HPP
#define REALM_OBJECT_STORE_ADMIN_REALM_HPP

#include "results.hpp"
#include "shared_realm.hpp"

#include <realm/version_id.hpp>

#include <condition_variable>
#include <mutex>
#include <queue>
#include <unordered_map>

namespace realm {

class SyncUser;
class SyncSession;

class AdminRealmListener {
public:
    using RealmInfo = std::pair<std::string, std::string>;
    AdminRealmListener(std::string local_root, std::string server_base_url, std::shared_ptr<SyncUser> user);
    void start(std::function<void(std::vector<RealmInfo>, bool)> register_callback);
    void create_realm(StringData realm_id, StringData realm_name);

private:    
    SharedRealm m_realm;
    Results m_results;
    NotificationToken m_notification_token;
    bool m_first = true;
};

} // namespace realm

#endif // REALM_OBJECT_STORE_ADMIN_REALM_HPP
