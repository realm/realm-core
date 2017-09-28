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

#include "global_notifier.hpp"

#include "impl/realm_coordinator.hpp"
#include "impl/transact_log_handler.hpp"
#include "object_store.hpp"
#include "results.hpp"
#include "object_schema.hpp"
#include "event_loop_dispatcher.hpp"

#include "sync/sync_config.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_user.hpp"
#include "sync/sync_session.hpp"

#include <realm/util/file.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/util/uri.hpp>
#include <realm/lang_bind_helper.hpp>

#include <regex>
#include <utility>
#include <stdexcept>
#include <vector>

using namespace realm;
using namespace realm::_impl;

AdminRealmListener::AdminRealmListener(std::string local_root, std::string server_base_url, std::shared_ptr<SyncUser> user, std::function<SyncBindSessionHandler> bind_callback)
{
    m_config.cache = false;
    m_config.path = util::File::resolve("realms.realm", local_root);
    m_config.schema_mode = SchemaMode::ReadOnlyAlternative;
    m_config.sync_config = std::make_shared<SyncConfig>();
    m_config.sync_config->stop_policy = SyncSessionStopPolicy::AfterChangesUploaded;
    m_config.sync_config->user = user;
    m_config.sync_config->realm_url = server_base_url + "/__admin";
    m_config.sync_config->bind_session_handler = std::move(bind_callback);
}

void AdminRealmListener::start(std::function<void(std::vector<std::string>)> callback)
{
    m_downloading_session = SyncManager::shared().get_session(m_config.path, *m_config.sync_config);
    EventLoopDispatcher<void(std::error_code)> download_callback([this, callback](std::error_code ec){
        auto cleanup = util::make_scope_exit([&]() noexcept { m_downloading_session.reset(); });
        if (ec) {
            if (ec == util::error::operation_aborted)
                return;
            throw std::system_error(ec);
        }

        m_realm = Realm::get_shared_realm(m_config);
        m_results = Results(m_realm, *ObjectStore::table_for_object_type(m_realm->read_group(), "RealmFile"));
        m_notification_token = m_results.add_notification_callback([this, callback](CollectionChangeSet changes, std::exception_ptr) {
            auto& table = *ObjectStore::table_for_object_type(m_realm->read_group(), "RealmFile");
            size_t path_col_ndx = table.get_column_index("path");
            std::vector<std::string> realms;

            auto add_realm = [&](auto index) {
                std::string realm_path = table.get_string(path_col_ndx, index);
                realms.emplace_back(std::move(realm_path));
            };

            if (changes.empty()) {
                for (size_t i = 0, size = table.size(); i < size; ++i) {
                    add_realm(i);
                }
            }
            else {
                for (auto i : changes.insertions.as_indexes()) {
                    add_realm(i);
                }
            }

            if (realms.size()) {
                callback(std::move(realms));
            }
        });
    });
    bool result = m_downloading_session->wait_for_download_completion(std::move(download_callback));
    REALM_ASSERT_RELEASE(result);
}
