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

#pragma once

#ifdef REALM_ENABLE_AUTH_TESTS

#include <util/sync/baas_admin_api.hpp>
#include <util/sync/sync_test_utils.hpp>

#include <vector>

namespace realm::app {

class FLXSyncTestHarness {
public:
    struct ServerSchema {
        Schema schema;
        std::vector<std::string> queryable_fields;
        std::vector<AppCreateConfig::ServiceRole> service_roles;
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
        auto server_app_config = minimal_app_config(test_name, server_schema.schema);
        server_app_config.dev_mode_enabled = server_schema.dev_mode_enabled;
        AppCreateConfig::FLXSyncConfig flx_config;
        flx_config.queryable_fields = server_schema.queryable_fields;

        server_app_config.flx_sync_config = std::move(flx_config);
        server_app_config.service_roles = server_schema.service_roles;

        return create_app(server_app_config);
    }

    struct Config {
        Config(std::string test_name, ServerSchema server_schema)
            : test_name(std::move(test_name))
            , server_schema(std::move(server_schema))
        {
        }

        std::string test_name;
        ServerSchema server_schema;
        std::shared_ptr<GenericNetworkTransport> transport = instance_of<SynchronousTestTransport>;
        ReconnectMode reconnect_mode = ReconnectMode::testing;
        std::shared_ptr<realm::sync::SyncSocketProvider> custom_socket_provider = nullptr;
    };

    explicit FLXSyncTestHarness(Config&& config)
        : m_test_session(make_app_from_server_schema(config.test_name, config.server_schema), config.transport, true,
                         config.reconnect_mode, config.custom_socket_provider)
        , m_schema(std::move(config.server_schema.schema))
    {
    }
    FLXSyncTestHarness(const std::string& test_name, ServerSchema server_schema = default_server_schema(),
                       std::shared_ptr<GenericNetworkTransport> transport = instance_of<SynchronousTestTransport>,
                       std::shared_ptr<realm::sync::SyncSocketProvider> custom_socket_provider = nullptr)
        : m_test_session(make_app_from_server_schema(test_name, server_schema), std::move(transport), true,
                         realm::ReconnectMode::normal, custom_socket_provider)
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
        subscribe_to_all_and_bootstrap(*realm);

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
