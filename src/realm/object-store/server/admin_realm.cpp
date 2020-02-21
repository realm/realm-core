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

#include "object_store.hpp"
#include "results.hpp"
#include "object_schema.hpp"
#include "util/event_loop_dispatcher.hpp"

#include "sync/sync_config.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_user.hpp"
#include "sync/sync_session.hpp"

#include <realm/util/file.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/util/uri.hpp>
#include <realm/obj.hpp>

#include <stdexcept>
#include <vector>

using namespace realm;
using namespace realm::_impl;

AdminRealmListener::AdminRealmListener(std::string local_root_dir, SyncConfig sync_config_template)
: m_local_root_dir(std::move(local_root_dir))
, m_sync_config_template(std::move(sync_config_template))
{
    m_config.path = util::File::resolve("realms.realm", m_local_root_dir);
    // We explicitly set the schema to avoid a race condition where the Admin Realm may
    // have been created but its schema may not have been uploaded to the server yet.
    m_config.schema_mode = SchemaMode::Additive;
    m_config.schema = Schema{
        {"RealmFile", {
            Property{"path", PropertyType::String, Property::IsPrimary{true}},
        }},
    };
    m_config.schema_version = 0;
    m_config.sync_config = std::make_shared<SyncConfig>(m_sync_config_template);
    m_config.sync_config->reference_realm_url += "/__admin";
}

void AdminRealmListener::start()
{
    if (m_download_session) {
        // If we're already downloading the Realm, don't need to do anything
        return;
    }

    if (auto realm = m_results.get_realm()) {
        // If we've finished downloading the Realm, just re-report all the files listed in it
        auto& group = realm->read_group();
        auto& table = *ObjectStore::table_for_object_type(group, "RealmFile");
        auto path_col_key = table.get_column_key("path");

        for (auto& obj : table) {
            register_realm(table.get_object_id(obj.get_key()), obj.get<String>(path_col_key));
        }
        return;
    }

    std::weak_ptr<AdminRealmListener> weak_self = shared_from_this();

    m_config.sync_config->error_handler = util::EventLoopDispatcher<void(std::shared_ptr<SyncSession>, SyncError)>([weak_self, this](std::shared_ptr<SyncSession>, SyncError e) {
        auto self = weak_self.lock();
        if (!self)
            return;
        error(std::make_exception_ptr(std::system_error(e.error_code)));
        m_download_session.reset();
    });

    util::EventLoopDispatcher<void(std::error_code)> download_callback([weak_self, this](std::error_code ec) {
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
        m_results = Results(realm, ObjectStore::table_for_object_type(realm->read_group(), "RealmFile")).sort({{"path", true}});

        struct Handler {
            bool initial_sent = false;
            std::weak_ptr<AdminRealmListener> weak_self;
            Handler(std::weak_ptr<AdminRealmListener> weak_self) : weak_self(std::move(weak_self)) { }

            void before(CollectionChangeSet const& c)
            {
                if (c.deletions.empty())
                    return;
                auto self = weak_self.lock();
                if (!self)
                    return;

                auto table = self->m_results.get_tableview().get_parent();
                auto path_col_key = table->get_column_key("path");
                for (auto i : c.deletions.as_indexes()) {
                    auto obj = self->m_results.get(i);
                    self->unregister_realm(table->get_object_id(obj.get_key()), obj.get<StringData>(path_col_key));
                }
            }

            void after(CollectionChangeSet const& c)
            {
                if (c.insertions.empty() && initial_sent)
                    return;

                auto self = weak_self.lock();
                if (!self)
                    return;
                if (self->m_results.size() == 0)
                    return;

                auto table = self->m_results.get_tableview().get_parent();
                auto path_col_key = table->get_column_key("path");

                if (!initial_sent) {
                    for (size_t i = 0, size = self->m_results.size(); i < size; ++i) {
                        auto obj = self->m_results.get(i);
                        self->register_realm(table->get_object_id(obj.get_key()), obj.get<StringData>(path_col_key));
                    }
                    initial_sent = true;
                }
                else {
                    for (auto i : c.insertions.as_indexes()) {
                        auto obj = self->m_results.get(i);
                        self->register_realm(table->get_object_id(obj.get_key()), obj.get<StringData>(path_col_key));
                    }
                }
            }

            void error(std::exception_ptr e)
            {
                if (auto self = weak_self.lock())
                    self->error(e);
            }
        };
        m_notification_token = m_results.add_notification_callback(Handler(std::move(weak_self)));
    });
    m_download_session = SyncManager::shared().get_session(m_config.path, *m_config.sync_config);
    m_download_session->wait_for_download_completion(std::move(download_callback));
}

Realm::Config AdminRealmListener::get_config(StringData virtual_path, StringData id) const {
    Realm::Config config;

    std::string file_path = m_local_root_dir + "/realms" + virtual_path.data();
    if (id) {
        file_path += std::string("/") + id.data();
    }
    file_path += + ".realm";
    for (size_t pos = m_local_root_dir.size(); pos != file_path.npos; pos = file_path.find('/', pos + 1)) {
        file_path[pos] = '\0';
        util::try_make_dir(file_path);
        file_path[pos] = '/';
    }

    config.path = std::move(file_path);
    config.sync_config = std::make_unique<SyncConfig>(m_sync_config_template);
    config.sync_config->reference_realm_url += virtual_path.data();
    config.schema_mode = SchemaMode::Additive;
    config.automatic_change_notifications = false;
    return config;
}
