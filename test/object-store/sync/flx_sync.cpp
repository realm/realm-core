////////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
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

#if REALM_ENABLE_AUTH_TESTS && REALM_ENABLE_FLX_SYNC

#include <catch2/catch.hpp>

#include "sync_test_utils.hpp"

#include "util/baas_admin_api.hpp"
#include "util/test_file.hpp"

#include "realm/object-store/impl/object_accessor_impl.hpp"

namespace realm::app {

class FLXSyncTestHarness {
public:
    struct ServerSchema {
        Schema schema;
        std::vector<std::string> queryable_fields;
    };

    static ServerSchema default_server_schema();

    FLXSyncTestHarness(const std::string& test_name, ServerSchema server_schema = default_server_schema());

    template <typename Func>
    void do_with_new_realm(Func&& func)
    {
        auto sync_mgr = make_sync_manager();
        auto creds = create_user_and_log_in(sync_mgr.app());

        SyncTestFile config(sync_mgr.app()->current_user(), schema(), SyncConfig::FLXSyncEnabled{});
        func(Realm::get_shared_realm(config));
    }

    template <typename Func>
    void load_initial_data(Func&& func)
    {
        do_with_new_realm([&](SharedRealm realm) {
            {
                auto subs = realm->get_latest_subscription_set().make_mutable_copy();
                for (const auto& table : realm->schema()) {
                    Query query_for_table(realm->read_group().get_table(table.table_key));
                    subs.insert_or_assign(query_for_table);
                }
                subs.commit();
                subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
            }
            func(realm);
        });
    }

    TestSyncManager make_sync_manager();

    const Schema& schema() const
    {
        return m_schema;
    }

private:
    AppSession m_app_session;
    app::App::Config m_app_config;
    Schema m_schema;
};

FLXSyncTestHarness::ServerSchema FLXSyncTestHarness::default_server_schema()
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

AppSession make_app_from_server_schema(const std::string& test_name,
                                       const FLXSyncTestHarness::ServerSchema& server_schema)
{
    auto server_app_config = minimal_app_config(get_base_url(), test_name, server_schema.schema);
    AppCreateConfig::FLXSyncConfig flx_config;
    for (const auto& table : server_schema.schema) {
        flx_config.queryable_fields[table.name] = server_schema.queryable_fields;
    }

    server_app_config.flx_sync_config = std::move(flx_config);
    return create_app(server_app_config);
}


FLXSyncTestHarness::FLXSyncTestHarness(const std::string& test_name, ServerSchema server_schema)
    : m_app_session(make_app_from_server_schema(test_name, server_schema))
    , m_app_config(get_config(instance_of<SynchronousTestTransport>, m_app_session))
    , m_schema(std::move(server_schema.schema))
{
}

TestSyncManager FLXSyncTestHarness::make_sync_manager()
{
    TestSyncManager::Config smc(m_app_config);
    return TestSyncManager(std::move(smc), {});
}

TEST_CASE("flx: connect to FLX-enabled app", "[sync][flx][app]") {
    FLXSyncTestHarness harness("basic_flx_connect");

    auto foo_obj_id = ObjectId::gen();
    auto bar_obj_id = ObjectId::gen();
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        realm->begin_transaction();
        Object::create(c, realm, "TopLevel",
                       util::Any(AnyDict{{"_id", foo_obj_id},
                                         {"queryable_str_field", std::string{"foo"}},
                                         {"queryable_int_field", static_cast<int64_t>(5)},
                                         {"non_queryable_field", std::string{"non queryable 1"}}}));
        Object::create(c, realm, "TopLevel",
                       util::Any(AnyDict{{"_id", bar_obj_id},
                                         {"queryable_str_field", std::string{"bar"}},
                                         {"queryable_int_field", static_cast<int64_t>(10)},
                                         {"non_queryable_field", std::string{"non queryable 2"}}}));

        realm->commit_transaction();
        wait_for_upload(*realm);
    });
    harness.do_with_new_realm([&](SharedRealm realm) {
        auto table = realm->read_group().get_table("class_TopLevel");
        Query new_query_a(table);
        auto col_key = table->get_column_key("queryable_str_field");
        new_query_a.equal(col_key, "foo");
        {
            auto new_subs = realm->get_latest_subscription_set().make_mutable_copy();
            new_query_a.equal(col_key, "foo");
            new_subs.insert_or_assign(new_query_a);
            new_subs.commit();
            new_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        {
            realm->refresh();
            Results results(realm, new_query_a);
            CHECK(results.size() == 1);
            auto obj = results.get<Obj>(0);
            CHECK(obj.is_valid());
            CHECK(obj.get<ObjectId>("_id") == foo_obj_id);
        }
    });
}

TEST_CASE("flx: query on non-queryable field results in query error message", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_bad_query");

    harness.do_with_new_realm([&](SharedRealm realm) {
        auto table = realm->read_group().get_table("class_TopLevel");
        auto bad_col_key = table->get_column_key("non_queryable_field");
        auto good_col_key = table->get_column_key("queryable_str_field");

        Query new_query_a(table);
        auto new_subs = realm->get_latest_subscription_set().make_mutable_copy();
        new_query_a.equal(bad_col_key, "bar");
        new_subs.insert_or_assign(new_query_a);
        new_subs.commit();
        auto sub_res =
            std::move(new_subs).get_state_change_notification(sync::SubscriptionSet::State::Complete).get_no_throw();
        CHECK(!sub_res.is_ok());
        CHECK(sub_res.get_status().reason() ==
              "Client provided query with bad syntax: invalid match expression for table "
              "\"TopLevel\": key \"non_queryable_field\" is not a queryable field");

        CHECK(realm->get_active_subscription_set().version() == 0);
        CHECK(realm->get_latest_subscription_set().version() == 1);

        Query new_query_b(table);
        new_query_b.equal(good_col_key, "foo");
        new_subs = realm->get_active_subscription_set().make_mutable_copy();
        new_subs.insert_or_assign(new_query_b);
        new_subs.commit();
        std::move(new_subs).get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

        CHECK(realm->get_active_subscription_set().version() == 2);
        CHECK(realm->get_latest_subscription_set().version() == 2);
    });
}

TEST_CASE("flx: no subscription store created for PBS app", "[sync][flx][app]") {
    const std::string base_url = get_base_url();

    Schema schema{
        ObjectSchema("TopLevel",
                     {
                         {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                     }),
    };

    auto server_app_config = minimal_app_config(base_url, "flx_connect_as_pbs", schema);
    auto app_session = create_app(server_app_config);
    auto app_config = get_config(instance_of<SynchronousTestTransport>, app_session);

    TestSyncManager::Config smc(app_config);
    TestSyncManager sync_manager(std::move(smc), {});
    auto app = sync_manager.app();

    auto creds = create_user_and_log_in(app);
    auto user = app->current_user();

    SyncTestFile config(app, bson::Bson{}, schema);

    auto realm = Realm::get_shared_realm(config);
    CHECK(!wait_for_download(*realm));
    CHECK(!wait_for_upload(*realm));

    CHECK(!realm->sync_session()->has_flx_subscription_store());
}

TEST_CASE("flx: connect to FLX as PBS returns an error", "[sync][flx][app]") {
    FLXSyncTestHarness harness("connect_to_flx_as_pbs");

    auto tsm = harness.make_sync_manager();
    create_user_and_log_in(tsm.app());
    SyncTestFile config(tsm.app(), bson::Bson{}, harness.schema());
    std::mutex sync_error_mutex;
    util::Optional<SyncError> sync_error;
    config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) mutable {
        std::lock_guard<std::mutex> lk(sync_error_mutex);
        sync_error = std::move(error);
    };
    auto realm = Realm::get_shared_realm(config);
    timed_wait_for([&] {
        std::lock_guard<std::mutex> lk(sync_error_mutex);
        return static_cast<bool>(sync_error);
    });

    CHECK(sync_error->error_code == make_error_code(sync::ProtocolError::switch_to_flx_sync));
}

TEST_CASE("flx: connect to PBS as FLX returns an error", "[sync][flx][app]") {
    const std::string base_url = get_base_url();

    Schema schema{
        ObjectSchema("TopLevel",
                     {
                         {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                     }),
    };

    auto server_app_config = minimal_app_config(base_url, "flx_connect_as_pbs", schema);
    auto app_session = create_app(server_app_config);
    auto app_config = get_config(instance_of<SynchronousTestTransport>, app_session);

    TestSyncManager::Config smc(app_config);
    TestSyncManager sync_manager(std::move(smc), {});
    auto app = sync_manager.app();

    auto creds = create_user_and_log_in(app);
    auto user = app->current_user();

    SyncTestFile config(user, schema, SyncConfig::FLXSyncEnabled{});

    std::mutex sync_error_mutex;
    util::Optional<SyncError> sync_error;
    config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) mutable {
        std::lock_guard<std::mutex> lk(sync_error_mutex);
        sync_error = std::move(error);
    };
    auto realm = Realm::get_shared_realm(config);
    auto latest_subs = realm->get_latest_subscription_set().make_mutable_copy();
    auto table = realm->read_group().get_table("class_TopLevel");
    Query new_query_a(table);
    new_query_a.equal(table->get_column_key("_id"), ObjectId::gen());
    latest_subs.insert_or_assign(std::move(new_query_a));
    latest_subs.commit();

    timed_wait_for([&] {
        std::lock_guard<std::mutex> lk(sync_error_mutex);
        return static_cast<bool>(sync_error);
    });

    CHECK(sync_error->error_code == make_error_code(sync::ProtocolError::switch_to_pbs));
}
} // namespace realm::app

#endif
