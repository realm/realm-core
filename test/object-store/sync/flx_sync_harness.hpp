////////////////////////////////////////////////////////////////////////////
//
// Copyright 2022 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or utilied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef FLX_SYNC_HARNESS_H
#define FLX_SYNC_HARNESS_H

#include "sync/sync_test_utils.hpp"
#include "util/baas_admin_api.hpp"
#include "util/test_file.hpp"

namespace realm::app {

class FLXSyncTestHarness {
public:
    struct ServerSchema {
        Schema schema;
        std::vector<std::string> queryable_fields;
        bool dev_mode_enabled = false;
    };

    static ServerSchema default_server_schema()
    {
        Schema schema{
            ObjectSchema("TopLevel",
                         {
                             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                             {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
                             {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
                             {"non_queryable_field", PropertyType::String | PropertyType::Nullable},
                         }),
        };

        return ServerSchema{std::move(schema), {"queryable_str_field", "queryable_int_field"}};
    }

    static AppSession make_app_from_server_schema(const std::string& test_name,
                                                  const FLXSyncTestHarness::ServerSchema& server_schema)
    {
        auto server_app_config = minimal_app_config(get_base_url(), test_name, server_schema.schema);
        server_app_config.dev_mode_enabled = server_schema.dev_mode_enabled;
        AppCreateConfig::FLXSyncConfig flx_config;
        flx_config.queryable_fields = server_schema.queryable_fields;

        server_app_config.flx_sync_config = std::move(flx_config);
        return create_app(server_app_config);
    }

    FLXSyncTestHarness(const std::string& test_name, ServerSchema server_schema = default_server_schema())
        : m_app_session(make_app_from_server_schema(test_name, server_schema))
        , m_app_config(get_config(instance_of<SynchronousTestTransport>, m_app_session))
        , m_schema(std::move(server_schema.schema))
    {
    }


    template <typename Func>
    void do_with_new_user(Func&& func)
    {
        auto sync_mgr = make_sync_manager();
        auto creds = create_user_and_log_in(sync_mgr.app());
        func(sync_mgr.app()->current_user());
    }

    template <typename Func>
    void do_with_new_realm(Func&& func, util::Optional<Schema> schema_for_realm = util::none)
    {
        do_with_new_user([&](std::shared_ptr<SyncUser> user) {
            SyncTestFile config(user, schema_for_realm.value_or(schema()), SyncConfig::FLXSyncEnabled{});
            func(Realm::get_shared_realm(config));
        });
    }


    template <typename Func>
    void load_initial_data(Func&& func)
    {
        do_with_new_realm([&](SharedRealm realm) {
            {
                auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
                for (const auto& table : realm->schema()) {
                    Query query_for_table(realm->read_group().get_table(table.table_key));
                    mut_subs.insert_or_assign(query_for_table);
                }
                auto subs = std::move(mut_subs).commit();
                subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
            }
            func(realm);
        });
    }

    TestSyncManager make_sync_manager()
    {
        TestSyncManager::Config smc(m_app_config);
        return TestSyncManager(std::move(smc), {});
    }

    const Schema& schema() const
    {
        return m_schema;
    }

private:
    AppSession m_app_session;
    app::App::Config m_app_config;
    Schema m_schema;
};

} // namespace realm::app

#endif // FLX_SYNC_HARNESS_H
