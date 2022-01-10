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
#include "realm/sync/protocol.hpp"

namespace realm::app {

class FLXSyncTestHarness {
public:
    struct ServerSchema {
        Schema schema;
        std::vector<std::string> queryable_fields;
        bool dev_mode_enabled = false;
    };

    static ServerSchema default_server_schema();

    FLXSyncTestHarness(const std::string& test_name, ServerSchema server_schema = default_server_schema());

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
    server_app_config.dev_mode_enabled = server_schema.dev_mode_enabled;
    AppCreateConfig::FLXSyncConfig flx_config;
    flx_config.queryable_fields = server_schema.queryable_fields;

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
            auto subs = std::move(new_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
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

TEST_CASE("flx: uploading an object that is out-of-view results in a client reset", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_bad_query");

    // TODO(RCORE-912) When DiscardLocal is supported with FLX sync we should remove this check in favor of the
    // tests for DiscardLocal.
    SECTION("disallow discardlocal") {
        harness.do_with_new_user([&](auto user) {
            SyncTestFile config(user, harness.schema(), SyncConfig::FLXSyncEnabled{});
            config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

            CHECK_THROWS_AS(Realm::get_shared_realm(config), std::logic_error);
        });
    }

    auto make_error_handler = [] {
        auto [error_promise, error_future] = util::make_promise_future<SyncError>();
        auto shared_promise = std::make_shared<decltype(error_promise)>(std::move(error_promise));
        auto fn = [error_promise = std::move(shared_promise)](std::shared_ptr<SyncSession>, SyncError err) {
            error_promise->emplace_value(std::move(err));
        };

        return std::make_pair(std::move(error_future), std::move(fn));
    };

    SECTION("client reset before setting a query") {
        harness.do_with_new_user([&](auto user) {
            SyncTestFile config(user, harness.schema(), SyncConfig::FLXSyncEnabled{});
            auto&& [error_future, err_handler] = make_error_handler();
            config.sync_config->error_handler = err_handler;

            auto realm = Realm::get_shared_realm(config);
            CppContext c(realm);
            realm->begin_transaction();
            Object::create(c, realm, "TopLevel",
                           util::Any(AnyDict{{"_id", ObjectId::gen()},
                                             {"queryable_str_field", std::string{"foo"}},
                                             {"queryable_int_field", static_cast<int64_t>(5)},
                                             {"non_queryable_field", std::string{"non queryable 1"}}}));
            realm->commit_transaction();

            auto sync_error = std::move(error_future).get();
            CHECK(sync_error.error_code == sync::make_error_code(sync::ProtocolError::write_not_allowed));
            CHECK(sync_error.is_session_level_protocol_error());
            CHECK(sync_error.is_client_reset_requested());
        });
    }

    SECTION("client reset after setting a query") {
        harness.do_with_new_user([&](auto user) {
            SyncTestFile config(user, harness.schema(), SyncConfig::FLXSyncEnabled{});
            auto&& [error_future, err_handler] = make_error_handler();
            config.sync_config->error_handler = err_handler;

            auto realm = Realm::get_shared_realm(config);
            auto table = realm->read_group().get_table("class_TopLevel");
            auto queryable_str_field = table->get_column_key("queryable_str_field");
            auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
            new_query.insert_or_assign(Query(table).equal(queryable_str_field, "foo"));
            std::move(new_query).commit();

            CppContext c(realm);
            realm->begin_transaction();
            Object::create(c, realm, "TopLevel",
                           util::Any(AnyDict{{"_id", ObjectId::gen()},
                                             {"queryable_str_field", std::string{"foo"}},
                                             {"queryable_int_field", static_cast<int64_t>(5)},
                                             {"non_queryable_field", std::string{"non queryable 1"}}}));
            Object::create(c, realm, "TopLevel",
                           util::Any(AnyDict{{"_id", ObjectId::gen()},
                                             {"queryable_str_field", std::string{"bar"}},
                                             {"queryable_int_field", static_cast<int64_t>(10)},
                                             {"non_queryable_field", std::string{"non queryable 2"}}}));
            realm->commit_transaction();

            auto sync_error = std::move(error_future).get();
            CHECK(sync_error.error_code == sync::make_error_code(sync::ProtocolError::write_not_allowed));
            CHECK(sync_error.is_session_level_protocol_error());
            CHECK(sync_error.is_client_reset_requested());
        });
    }
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
        auto subs = std::move(new_subs).commit();
        auto sub_res = subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get_no_throw();
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
        subs = std::move(new_subs).commit();
        subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

        CHECK(realm->get_active_subscription_set().version() == 2);
        CHECK(realm->get_latest_subscription_set().version() == 2);
    });
}

TEST_CASE("flx: dev mode uploads schema before query change", "[sync][flx][app]") {
    FLXSyncTestHarness::ServerSchema server_schema;
    auto default_schema = FLXSyncTestHarness::default_server_schema();
    server_schema.queryable_fields = default_schema.queryable_fields;
    server_schema.dev_mode_enabled = true;
    server_schema.schema = Schema{};

    FLXSyncTestHarness harness("flx_dev_mode", server_schema);
    auto foo_obj_id = ObjectId::gen();
    auto bar_obj_id = ObjectId::gen();
    harness.do_with_new_realm(
        [&](SharedRealm realm) {
            auto table = realm->read_group().get_table("class_TopLevel");
            // auto queryable_str_field = table->get_column_key("queryable_str_field");
            // auto queryable_int_field = table->get_column_key("queryable_int_field");
            auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
            new_query.insert_or_assign(Query(table));
            std::move(new_query).commit();

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
        },
        default_schema.schema);

    harness.do_with_new_realm(
        [&](SharedRealm realm) {
            auto table = realm->read_group().get_table("class_TopLevel");
            auto queryable_int_field = table->get_column_key("queryable_int_field");
            auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
            new_query.insert_or_assign(Query(table).greater_equal(queryable_int_field, int64_t(5)));
            auto subs = std::move(new_query).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
            wait_for_download(*realm);
            Results results(realm, table);

            realm->refresh();
            CHECK(results.size() == 2);
            CHECK(table->get_object_with_primary_key({foo_obj_id}).is_valid());
            CHECK(table->get_object_with_primary_key({bar_obj_id}).is_valid());
        },
        default_schema.schema);
}

TEST_CASE("flx: writes work offline", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_offline_writes");

    harness.do_with_new_realm([&](SharedRealm realm) {
        auto sync_session = realm->sync_session();
        auto table = realm->read_group().get_table("class_TopLevel");
        auto queryable_str_field = table->get_column_key("queryable_str_field");
        auto queryable_int_field = table->get_column_key("queryable_int_field");
        auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
        new_query.insert_or_assign(Query(table));
        std::move(new_query).commit();

        auto foo_obj_id = ObjectId::gen();
        auto bar_obj_id = ObjectId::gen();

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
        sync_session->close();
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).equal(queryable_str_field, "foo"));
            std::move(mut_subs).commit();
        }

        {
            Results results(realm, table);
            realm->begin_transaction();
            auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
            foo_obj.set<int64_t>(queryable_int_field, 15);
            realm->commit_transaction();
        }

        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).greater_equal(queryable_int_field, static_cast<int64_t>(10)));
            std::move(mut_subs).commit();
        }

        Results results(realm, table);
        realm->begin_transaction();
        auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
        foo_obj.set<int64_t>(queryable_int_field, 0);
        realm->commit_transaction();

        sync_session->revive_if_needed();
        wait_for_upload(*realm);

        realm->refresh();
        CHECK(results.size() == 2);
        CHECK(table->get_object_with_primary_key({foo_obj_id}).is_valid());
        CHECK(table->get_object_with_primary_key({bar_obj_id}).is_valid());
    });
}

TEST_CASE("flx: writes work without waiting for sync", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_offline_writes");

    harness.do_with_new_realm([&](SharedRealm realm) {
        auto table = realm->read_group().get_table("class_TopLevel");
        auto queryable_str_field = table->get_column_key("queryable_str_field");
        auto queryable_int_field = table->get_column_key("queryable_int_field");
        auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
        new_query.insert_or_assign(Query(table));
        std::move(new_query).commit();

        auto foo_obj_id = ObjectId::gen();
        auto bar_obj_id = ObjectId::gen();

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
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).equal(queryable_str_field, "foo"));
            std::move(mut_subs).commit();
        }

        {
            Results results(realm, table);
            realm->begin_transaction();
            auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
            foo_obj.set<int64_t>(queryable_int_field, 15);
            realm->commit_transaction();
        }

        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).greater_equal(queryable_int_field, static_cast<int64_t>(10)));
            std::move(mut_subs).commit();
        }

        Results results(realm, table);
        realm->begin_transaction();
        auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
        foo_obj.set<int64_t>(queryable_int_field, 0);
        realm->commit_transaction();

        wait_for_upload(*realm);

        realm->refresh();
        CHECK(results.size() == 2);
        CHECK(table->get_object_with_primary_key({foo_obj_id}).is_valid());
        CHECK(table->get_object_with_primary_key({bar_obj_id}).is_valid());
    });
}


TEST_CASE("flx: subscriptions persist after closing/reopening", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_bad_query");

    auto sync_mgr = harness.make_sync_manager();
    auto creds = create_user_and_log_in(sync_mgr.app());

    SyncTestFile config(sync_mgr.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
    config.persist();

    auto orig_realm = Realm::get_shared_realm(config);
    auto mut_subs = orig_realm->get_latest_subscription_set().make_mutable_copy();
    mut_subs.insert_or_assign(Query(orig_realm->read_group().get_table("class_TopLevel")));
    std::move(mut_subs).commit();
    orig_realm->close();

    auto new_realm = Realm::get_shared_realm(config);
    auto latest_subs = new_realm->get_latest_subscription_set();
    CHECK(latest_subs.size() == 1);
    latest_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
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

TEST_CASE("flx: connect to FLX with partition value returns an error", "[sync][flx][app]") {
    FLXSyncTestHarness harness("connect_to_flx_as_pbs");

    auto tsm = harness.make_sync_manager();
    create_user_and_log_in(tsm.app());
    SyncTestFile config(tsm.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
    config.sync_config->partition_value = "\"foobar\"";

    CHECK_THROWS_AS(Realm::get_shared_realm(config), std::logic_error);
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
    std::move(latest_subs).commit();

    timed_wait_for([&] {
        std::lock_guard<std::mutex> lk(sync_error_mutex);
        return static_cast<bool>(sync_error);
    });

    CHECK(sync_error->error_code == make_error_code(sync::ProtocolError::switch_to_pbs));
}
} // namespace realm::app

#endif
