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

#ifdef REALM_ENABLE_AUTH_TESTS

#include "sync/sync_test_utils.hpp"
#include "util/baas_admin_api.hpp"

namespace realm::app {

class FLXSyncTestHarness {
public:
    struct ServerSchema {
        Schema schema;
        std::vector<std::string> queryable_fields;
        std::vector<AppCreateConfig::FLXSyncRole> default_roles;
        bool dev_mode_enabled = false;
    };

    static ServerSchema default_server_schema()
    {
        Schema schema{
            {"TopLevel",
             {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
              {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
              {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
              {"non_queryable_field", PropertyType::String | PropertyType::Nullable}}},
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
        flx_config.default_roles = server_schema.default_roles;

        server_app_config.flx_sync_config = std::move(flx_config);
        return create_app(server_app_config);
    }

    FLXSyncTestHarness(const std::string& test_name, ServerSchema server_schema = default_server_schema(),
                       std::shared_ptr<GenericNetworkTransport> transport = instance_of<SynchronousTestTransport>)
        : m_test_session(make_app_from_server_schema(test_name, server_schema), std::move(transport))
        , m_schema(std::move(server_schema.schema))
    {
    }


    template <typename Func>
    void do_with_new_user(Func&& func)
    {
        create_user_and_log_in(m_test_session.app());
        func(m_test_session.app()->current_user());
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
        SyncTestFile config(m_test_session.app()->current_user(), schema(), SyncConfig::FLXSyncEnabled{});
        auto realm = Realm::get_shared_realm(config);
        auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
        for (const auto& table : realm->schema()) {
            if (table.table_type != ObjectSchema::ObjectType::TopLevel) {
                continue;
            }
            Query query_for_table(realm->read_group().get_table(table.table_key));
            mut_subs.insert_or_assign(query_for_table);
        }
        auto subs = std::move(mut_subs).commit();
        subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        wait_for_download(*realm);

        realm->begin_transaction();
        func(realm);
        realm->commit_transaction();
        wait_for_upload(*realm);
    }

    const Schema& schema() const
    {
        return m_schema;
    }

    std::shared_ptr<App> app() const noexcept
    {
        return m_test_session.app();
    }

    const TestAppSession& session() const
    {
        return m_test_session;
    }

    SyncTestFile make_test_file() const
    {
        return SyncTestFile(app()->current_user(), schema(), realm::SyncConfig::FLXSyncEnabled{});
    }

private:
    TestAppSession m_test_session;
    Schema m_schema;
};
} // namespace realm::app
#endif // REALM_ENABLE_AUTH_TESTS
#endif // FLX_SYNC_HARNESS_H
