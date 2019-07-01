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

#include <memory>

namespace realm {
class SyncUser;
class SyncSession;
namespace sync {
struct ObjectID;
}

class AdminRealmListener : public std::enable_shared_from_this<AdminRealmListener> {
public:
    AdminRealmListener(std::string local_root_dir, SyncConfig sync_config_template);

    void start();

    Realm::Config get_config(StringData virtual_path, StringData id = nullptr) const;

    virtual void register_realm(sync::ObjectID id, StringData virtual_path) = 0;
    virtual void unregister_realm(sync::ObjectID id, StringData virtual_path) = 0;
    virtual void download_complete() = 0;
    virtual void error(std::exception_ptr) = 0;

private:
    Realm::Config m_config;
    const std::string m_local_root_dir;
    const SyncConfig m_sync_config_template;
    Results m_results;
    NotificationToken m_notification_token;
    std::shared_ptr<SyncSession> m_download_session;
};

} // namespace realm

#endif // REALM_JS_ADMIN_REALM_HPP
