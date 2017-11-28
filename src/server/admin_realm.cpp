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

#include "admin_realm.hpp"

#include "event_loop_dispatcher.hpp"
#include "object_store.hpp"
#include "results.hpp"

#include "sync/sync_config.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_user.hpp"
#include "sync/sync_session.hpp"

#include <realm/util/file.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/util/uri.hpp>

#include <stdexcept>
#include <vector>

using namespace realm;
using namespace realm::_impl;

AdminRealmListener::AdminRealmListener(std::string local_root, std::string server_base_url,
                                       std::shared_ptr<SyncUser> user,
                                       std::function<SyncBindSessionHandler> bind_callback)
{
    m_config.cache = false;
    m_config.path = util::File::resolve("realms.realm", local_root);
    m_config.schema_mode = SchemaMode::ReadOnlyAlternative;
    m_config.sync_config = std::make_shared<SyncConfig>(user, server_base_url + "/__admin");
    m_config.sync_config->bind_session_handler = std::move(bind_callback);
}

void AdminRealmListener::start()
{
    if (m_download_session) {
        // If we're already downloading the Realm, don't need to do anything
        return;
    }

    if (auto realm = m_results.get_realm()) {
        // If we've finished downloading the Realm, just re-report all the files listed in it
        auto& table = *ObjectStore::table_for_object_type(realm->read_group(), "RealmFile");
        size_t path_col_ndx = table.get_column_index("path");

        for (size_t i = 0, size = table.size(); i < size; ++i)
            register_realm(table.get_string(path_col_ndx, i));
        return;
    }

    m_download_session = SyncManager::shared().get_session(m_config.path, *m_config.sync_config);
    std::weak_ptr<AdminRealmListener> weak_self = shared_from_this();
    EventLoopDispatcher<void(std::error_code)> download_callback([weak_self, this](std::error_code ec) {
        auto self = weak_self.lock();
        if (!self)
            return;

        auto cleanup = util::make_scope_exit([&]() noexcept { m_download_session.reset(); });
        if (ec) {
            if (ec == util::error::operation_aborted)
                return;
            error(std::make_exception_ptr(std::system_error(ec)));
            return;
        }
        download_complete();

        auto realm = Realm::get_shared_realm(m_config);
        m_results = Results(realm, *ObjectStore::table_for_object_type(realm->read_group(), "RealmFile"));
        m_notification_token = m_results.add_notification_callback([=](CollectionChangeSet const& changes, std::exception_ptr err) {
            auto self = weak_self.lock();
            if (!self)
                return;
            if (err) {
                error(err);
                return;
            }

            auto& table = *ObjectStore::table_for_object_type(realm->read_group(), "RealmFile");
            size_t path_col_ndx = table.get_column_index("path");

            if (changes.empty()) {
                for (size_t i = 0, size = table.size(); i < size; ++i)
                    register_realm(table.get_string(path_col_ndx, i));
            }
            else {
                for (auto i : changes.insertions.as_indexes())
                    register_realm(table.get_string(path_col_ndx, i));
            }
        });
    });
    bool result = m_download_session->wait_for_download_completion(std::move(download_callback));
    REALM_ASSERT_RELEASE(result);
}
