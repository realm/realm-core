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
#include "util/event_loop_signal.hpp"

#include "sync/sync_config.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_user.hpp"
#include "sync/sync_session.hpp"

#include <realm/util/file.hpp>
#include <realm/util/uri.hpp>
#include <realm/lang_bind_helper.hpp>

#include <regex>
#include <utility>
#include <stdexcept>
#include <vector>

using namespace realm;
using namespace realm::_impl;

AdminRealmListener::AdminRealmListener(std::string local_root, std::string server_base_url, std::shared_ptr<SyncUser> user)
{
    Realm::Config config;
    config.cache = false;
    config.path = util::File::resolve("__admin.realm", local_root);
    config.schema_mode = SchemaMode::Additive;
    config.sync_config = std::shared_ptr<SyncConfig>(
        new SyncConfig{user, server_base_url + "/__admin", SyncSessionStopPolicy::AfterChangesUploaded,
           [&](auto, const auto& config, auto session) {
                session->bind_with_admin_token(config.user->refresh_token(), config.realm_url);

           }}
    );
    config.schema = Schema{
        {"RealmFile", {
            {"path", PropertyType::String, Property::IsPrimary{true}, Property::IsIndexed{false}},
            {"creatorId", PropertyType::String, Property::IsPrimary{false}, Property::IsIndexed{false}},
            {"creationDate", PropertyType::Date, Property::IsPrimary{false}, Property::IsIndexed{false}},
            {"syncLabel", PropertyType::String, Property::IsPrimary{false}, Property::IsIndexed{false}}
        }}
    };
    config.schema_version = 2;
    m_realm = Realm::get_shared_realm(std::move(config));
}

void AdminRealmListener::start(std::function<void(std::vector<RealmInfo>)> callback)
{
    m_results = Results(m_realm, *ObjectStore::table_for_object_type(m_realm->read_group(), "RealmFile"));
    m_notification_token = m_results.add_notification_callback([=](CollectionChangeSet changes, std::exception_ptr) {
        auto& table = *ObjectStore::table_for_object_type(m_realm->read_group(), "RealmFile");
        size_t id_col_ndx   = table.get_column_index("creatorId");
        size_t name_col_ndx = table.get_column_index("path");
        std::vector<RealmInfo> realms;

        auto add_realm = [&](auto index) {
            std::string realm_path = table.get_string(name_col_ndx, index);
            realms.emplace_back(table.get_string(id_col_ndx, index), realm_path);
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
}

void AdminRealmListener::create_realm(StringData realm_id, StringData realm_name)
{
    m_realm->begin_transaction();
    auto& table = *ObjectStore::table_for_object_type(m_realm->read_group(), "RealmFile");
    size_t row_ndx      = table.add_empty_row();
    size_t id_col_ndx   = table.get_column_index("creatorId");
    size_t name_col_ndx = table.get_column_index("path");
    table.set_string(id_col_ndx, row_ndx, realm_id);
    table.set_string(name_col_ndx, row_ndx, realm_name);
    m_realm->commit_transaction();
}
