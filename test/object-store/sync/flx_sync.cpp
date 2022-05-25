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

#if REALM_ENABLE_AUTH_TESTS

#include <catch2/catch.hpp>

#include "flx_sync_harness.hpp"
#include "realm/object-store/impl/object_accessor_impl.hpp"
#include "realm/object-store/impl/realm_coordinator.hpp"
#include "realm/object-store/schema.hpp"
#include "realm/object-store/sync/generic_network_transport.hpp"
#include "realm/object-store/sync/sync_session.hpp"
#include "realm/object_id.hpp"
#include "realm/sync/client_base.hpp"
#include "realm/sync/config.hpp"
#include "realm/sync/noinst/client_history_impl.hpp"
#include "realm/sync/noinst/pending_bootstrap_store.hpp"
#include "realm/sync/protocol.hpp"
#include "realm/sync/subscriptions.hpp"
#include "realm/util/future.hpp"
#include "realm/util/logger.hpp"
#include "util/test_file.hpp"
#include <realm/sync/noinst/server/access_token.hpp>

#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace realm::app {

namespace {
const Schema g_minimal_schema{
    {"TopLevel",
     {
         {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
     }},
};

const Schema g_large_array_schema{
    ObjectSchema("TopLevel",
                 {
                     {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                     {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
                     {"list_of_strings", PropertyType::Array | PropertyType::String},
                 }),
};

// Populates a FLXSyncTestHarness with the g_large_array_schema with objects that are large enough that
// they are guaranteed to fill multiple bootstrap download messages. Currently this means generating 5
// objects each with 1024 array entries of 1024 bytes each.
//
// Returns a list of the _id values for the objects created.
std::vector<ObjectId> fill_large_array_schema(FLXSyncTestHarness& harness)
{
    std::vector<ObjectId> ret;
    REQUIRE(harness.schema() == g_large_array_schema);
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        for (int i = 0; i < 5; ++i) {
            auto id = ObjectId::gen();
            auto obj = Object::create(c, realm, "TopLevel",
                                      util::Any(AnyDict{{"_id", id},
                                                        {"list_of_strings", AnyVector{}},
                                                        {"queryable_int_field", static_cast<int64_t>(i * 5)}}));
            List str_list(obj, realm->schema().find("TopLevel")->property_for_name("list_of_strings"));
            for (int j = 0; j < 1024; ++j) {
                str_list.add(c, util::Any(std::string(1024, 'a' + (j % 26))));
            }

            ret.push_back(id);
        }
    });
    return ret;
}
} // namespace

TEST_CASE("flx: connect to FLX-enabled app", "[sync][flx][app]") {
    FLXSyncTestHarness harness("basic_flx_connect");

    auto foo_obj_id = ObjectId::gen();
    auto bar_obj_id = ObjectId::gen();
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
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
    });

    harness.do_with_new_realm([&](SharedRealm realm) {
        wait_for_download(*realm);
        {
            auto empty_subs = realm->get_latest_subscription_set();
            CHECK(empty_subs.size() == 0);
            CHECK(empty_subs.version() == 0);
            empty_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        auto table = realm->read_group().get_table("class_TopLevel");
        auto col_key = table->get_column_key("queryable_str_field");
        Query query_foo(table);
        query_foo.equal(col_key, "foo");
        {
            auto new_subs = realm->get_latest_subscription_set().make_mutable_copy();
            new_subs.insert_or_assign(query_foo);
            auto subs = std::move(new_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        wait_for_download(*realm);
        {
            realm->refresh();
            Results results(realm, table);
            CHECK(results.size() == 1);
            auto obj = results.get<Obj>(0);
            CHECK(obj.is_valid());
            CHECK(obj.get<ObjectId>("_id") == foo_obj_id);
        }

        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            Query new_query_bar(table);
            new_query_bar.equal(col_key, "bar");
            mut_subs.insert_or_assign(new_query_bar);
            auto subs = std::move(mut_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        {
            realm->refresh();
            Results results(realm, Query(table));
            CHECK(results.size() == 2);
        }

        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            auto it = mut_subs.find(query_foo);
            CHECK(it != mut_subs.end());
            mut_subs.erase(it);
            Query new_query_bar(table);
            new_query_bar.equal(col_key, "bar");
            mut_subs.insert_or_assign(new_query_bar);
            auto subs = std::move(mut_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        {
            realm->refresh();
            Results results(realm, Query(table));
            CHECK(results.size() == 1);
            auto obj = results.get<Obj>(0);
            CHECK(obj.is_valid());
            CHECK(obj.get<ObjectId>("_id") == bar_obj_id);
        }

        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            auto subs = std::move(mut_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        {
            realm->refresh();
            Results results(realm, table);
            CHECK(results.size() == 0);
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

            REQUIRE_THROW_LOGIC_ERROR_WITH_CODE(Realm::get_shared_realm(config), ErrorCodes::IllegalCombination);
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
            CHECK(sync_error.get_system_error() == sync::make_error_code(sync::ProtocolError::write_not_allowed));
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
            CHECK(sync_error.get_system_error() == sync::make_error_code(sync::ProtocolError::write_not_allowed));
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
        if (sub_res.get_status().reason().find("Client provided query with bad syntax:") == std::string::npos ||
            sub_res.get_status().reason().find(
                "\"TopLevel\": key \"non_queryable_field\" is not a queryable field") == std::string::npos) {
            FAIL(sub_res.get_status().reason());
        }

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

TEST_CASE("flx: interrupted bootstrap restarts/recovers on reconnect", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_bootstrap_batching", {g_large_array_schema, {"queryable_int_field"}});

    std::vector<ObjectId> obj_ids_at_end = fill_large_array_schema(harness);
    SyncTestFile interrupted_realm_config(harness.app()->current_user(), harness.schema(),
                                          SyncConfig::FLXSyncEnabled{});
    interrupted_realm_config.cache = false;

    {
        auto [interrupted_promise, interrupted] = util::make_promise_future<void>();
        Realm::Config config = interrupted_realm_config;
        config.sync_config = std::make_shared<SyncConfig>(*interrupted_realm_config.sync_config);
        auto shared_promise = std::make_shared<util::Promise<void>>(std::move(interrupted_promise));
        config.sync_config->on_download_message_received_hook = [promise = std::move(shared_promise)](
                                                                    std::weak_ptr<SyncSession> weak_session,
                                                                    const sync::SyncProgress&, int64_t query_version,
                                                                    sync::DownloadBatchState batch_state) mutable {
            auto session = weak_session.lock();
            if (!session) {
                return;
            }

            auto latest_subs = session->get_flx_subscription_store()->get_latest();
            if (latest_subs.version() == 1 && latest_subs.state() == sync::SubscriptionSet::State::Bootstrapping) {
                REQUIRE(query_version == 1);
                REQUIRE(batch_state == sync::DownloadBatchState::MoreToCome);
                session->close();
                promise->emplace_value();
            }
        };

        auto realm = Realm::get_shared_realm(config);
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            auto table = realm->read_group().get_table("class_TopLevel");
            mut_subs.insert_or_assign(Query(table));
            std::move(mut_subs).commit();
        }

        interrupted.get();
        realm->sync_session()->shutdown_and_wait();
        realm->close();
    }

    _impl::RealmCoordinator::clear_all_caches();

    {
        auto realm = DB::create(sync::make_client_replication(), interrupted_realm_config.path);
        auto sub_store = sync::SubscriptionStore::create(realm, [](int64_t) {});
        REQUIRE(sub_store->get_active_and_latest_versions() == std::pair<int64_t, int64_t>{0, 1});
        auto latest_subs = sub_store->get_latest();
        REQUIRE(latest_subs.state() == sync::SubscriptionSet::State::Bootstrapping);
        REQUIRE(latest_subs.size() == 1);
        REQUIRE(latest_subs.at(0).object_class_name() == "TopLevel");
    }

    auto realm = Realm::get_shared_realm(interrupted_realm_config);
    auto table = realm->read_group().get_table("class_TopLevel");
    realm->get_latest_subscription_set().get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
    wait_for_upload(*realm);
    wait_for_download(*realm);

    realm->refresh();
    REQUIRE(table->size() == obj_ids_at_end.size());
    for (auto& id : obj_ids_at_end) {
        REQUIRE(table->find_primary_key(Mixed{id}));
    }

    auto active_subs = realm->get_active_subscription_set();
    auto latest_subs = realm->get_latest_subscription_set();
    REQUIRE(active_subs.version() == latest_subs.version());
    REQUIRE(active_subs.version() == int64_t(1));
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
        wait_for_download(*realm);
        sync_session->close();

        // Make it so the subscriptions only match the "foo" object
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).equal(queryable_str_field, "foo"));
            std::move(mut_subs).commit();
        }

        // Make foo so that it will match the next subscription update. This checks whether you can do
        // multiple subscription set updates offline and that the last one eventually takes effect when
        // you come back online and fully synchronize.
        {
            Results results(realm, table);
            realm->begin_transaction();
            auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
            foo_obj.set<int64_t>(queryable_int_field, 15);
            realm->commit_transaction();
        }

        // Update our subscriptions so that both foo/bar will be included
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).greater_equal(queryable_int_field, static_cast<int64_t>(10)));
            std::move(mut_subs).commit();
        }

        // Make foo out of view for the current subscription.
        {
            Results results(realm, table);
            realm->begin_transaction();
            auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
            foo_obj.set<int64_t>(queryable_int_field, 0);
            realm->commit_transaction();
        }

        sync_session->revive_if_needed();
        wait_for_upload(*realm);
        wait_for_download(*realm);

        realm->refresh();
        Results results(realm, table);
        CHECK(results.size() == 1);
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

        // Make it so the subscriptions only match the "foo" object
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).equal(queryable_str_field, "foo"));
            std::move(mut_subs).commit();
        }

        // Make foo so that it will match the next subscription update. This checks whether you can do
        // multiple subscription set updates without waiting and that the last one eventually takes effect when
        // you fully synchronize.
        {
            Results results(realm, table);
            realm->begin_transaction();
            auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
            foo_obj.set<int64_t>(queryable_int_field, 15);
            realm->commit_transaction();
        }

        // Update our subscriptions so that both foo/bar will be included
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).greater_equal(queryable_int_field, static_cast<int64_t>(10)));
            std::move(mut_subs).commit();
        }

        // Make foo out-of-view for the current subscription.
        {
            Results results(realm, table);
            realm->begin_transaction();
            auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
            foo_obj.set<int64_t>(queryable_int_field, 0);
            realm->commit_transaction();
        }

        wait_for_upload(*realm);
        wait_for_download(*realm);

        realm->refresh();
        Results results(realm, table);
        CHECK(results.size() == 1);
        CHECK(table->get_object_with_primary_key({bar_obj_id}).is_valid());
    });
}

TEST_CASE("flx: subscriptions persist after closing/reopening", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_bad_query");
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    {
        auto orig_realm = Realm::get_shared_realm(config);
        auto mut_subs = orig_realm->get_latest_subscription_set().make_mutable_copy();
        mut_subs.insert_or_assign(Query(orig_realm->read_group().get_table("class_TopLevel")));
        std::move(mut_subs).commit();
        orig_realm->close();
    }

    {
        auto new_realm = Realm::get_shared_realm(config);
        auto latest_subs = new_realm->get_latest_subscription_set();
        CHECK(latest_subs.size() == 1);
        latest_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
    }
}

TEST_CASE("flx: no subscription store created for PBS app", "[sync][flx][app]") {
    const std::string base_url = get_base_url();
    auto server_app_config = minimal_app_config(base_url, "flx_connect_as_pbs", g_minimal_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config(session.app(), bson::Bson{}, g_minimal_schema);

    auto realm = Realm::get_shared_realm(config);
    CHECK(!wait_for_download(*realm));
    CHECK(!wait_for_upload(*realm));

    CHECK(!realm->sync_session()->get_flx_subscription_store());
}

TEST_CASE("flx: connect to FLX as PBS returns an error", "[sync][flx][app]") {
    FLXSyncTestHarness harness("connect_to_flx_as_pbs");
    SyncTestFile config(harness.app(), bson::Bson{}, harness.schema());
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

    CHECK(sync_error->get_system_error() == make_error_code(sync::ProtocolError::switch_to_flx_sync));
}

TEST_CASE("flx: connect to FLX with partition value returns an error", "[sync][flx][app]") {
    FLXSyncTestHarness harness("connect_to_flx_as_pbs");
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
    config.sync_config->partition_value = "\"foobar\"";

    REQUIRE_THROW_LOGIC_ERROR_WITH_CODE(Realm::get_shared_realm(config), ErrorCodes::IllegalCombination);
}

TEST_CASE("flx: connect to PBS as FLX returns an error", "[sync][flx][app]") {
    const std::string base_url = get_base_url();

    auto server_app_config = minimal_app_config(base_url, "flx_connect_as_pbs", g_minimal_schema);
    TestAppSession session(create_app(server_app_config));
    auto app = session.app();
    auto user = app->current_user();

    SyncTestFile config(user, g_minimal_schema, SyncConfig::FLXSyncEnabled{});

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

    CHECK(sync_error->get_system_error() == make_error_code(sync::ProtocolError::switch_to_pbs));
}

TEST_CASE("flx: commit subscription while refreshing the access token", "[sync][flx][app]") {
    class HookedTransport : public SynchronousTestTransport {
    public:
        void send_request_to_server(Request&& request,
                                    util::UniqueFunction<void(const Response&)>&& completion_block) override
        {
            if (request_hook) {
                request_hook(request);
            }
            SynchronousTestTransport::send_request_to_server(std::move(request), [&](const Response& response) {
                completion_block(response);
            });
        }
        util::UniqueFunction<void(Request&)> request_hook;
    };

    auto transport = std::make_shared<HookedTransport>();
    FLXSyncTestHarness harness("flx_wait_access_token2", FLXSyncTestHarness::default_server_schema(), transport);
    auto app = harness.app();
    std::shared_ptr<SyncUser> user = app->current_user();
    REQUIRE(user);
    REQUIRE(!user->access_token_refresh_required());
    // Set a bad access token, with an expired time. This will trigger a refresh initiated by the client.
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    using namespace std::chrono_literals;
    auto expires = std::chrono::system_clock::to_time_t(now - 30s);
    user->update_access_token(encode_fake_jwt("fake_access_token", expires));
    REQUIRE(user->access_token_refresh_required());

    bool seen_waiting_for_access_token = false;
    // Commit a subcription set while there is no sync session.
    // A session is created when the access token is refreshed.
    transport->request_hook = [&](Request&) {
        auto user = app->current_user();
        REQUIRE(user);
        for (auto& session : user->all_sessions()) {
            if (session->state() == SyncSession::State::WaitingForAccessToken) {
                REQUIRE(!seen_waiting_for_access_token);
                seen_waiting_for_access_token = true;

                auto store = session->get_flx_subscription_store();
                REQUIRE(store);
                auto mut_subs = store->get_latest().make_mutable_copy();
                std::move(mut_subs).commit();
            }
        }
    };
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
    // This triggers the token refresh.
    auto r = Realm::get_shared_realm(config);
    REQUIRE(seen_waiting_for_access_token);
}

TEST_CASE("flx: bootstrap batching prevents orphan documents", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_bootstrap_batching", {g_large_array_schema, {"queryable_int_field"}});

    std::vector<ObjectId> obj_ids_at_end = fill_large_array_schema(harness);
    SyncTestFile interrupted_realm_config(harness.app()->current_user(), harness.schema(),
                                          SyncConfig::FLXSyncEnabled{});
    interrupted_realm_config.cache = false;

    auto check_interrupted_state = [&](const DBRef& realm) {
        auto tr = realm->start_read();
        auto top_level = tr->get_table("class_TopLevel");
        REQUIRE(top_level);
        REQUIRE(top_level->is_empty());

        auto sub_store = sync::SubscriptionStore::create(realm, [](int64_t) {});
        REQUIRE(sub_store->get_active_and_latest_versions() == std::pair<int64_t, int64_t>{0, 1});
        auto latest_subs = sub_store->get_latest();
        REQUIRE(latest_subs.state() == sync::SubscriptionSet::State::Bootstrapping);
        REQUIRE(latest_subs.size() == 1);
        REQUIRE(latest_subs.at(0).object_class_name() == "TopLevel");
    };

    auto mutate_realm = [&] {
        harness.load_initial_data([&](SharedRealm realm) {
            auto table = realm->read_group().get_table("class_TopLevel");
            realm->refresh();
            Results res(realm, Query(table).greater(table->get_column_key("queryable_int_field"), int64_t(10)));
            REQUIRE(res.size() == 2);
            res.clear();
        });
    };

    SECTION("interrupted before final bootstrap message") {
        {
            auto [interrupted_promise, interrupted] = util::make_promise_future<void>();
            Realm::Config config = interrupted_realm_config;
            config.sync_config = std::make_shared<SyncConfig>(*interrupted_realm_config.sync_config);
            auto shared_promise = std::make_shared<util::Promise<void>>(std::move(interrupted_promise));
            config.sync_config->on_bootstrap_message_processed_hook =
                [promise = std::move(shared_promise)](std::weak_ptr<SyncSession> weak_session,
                                                      const sync::SyncProgress&, int64_t query_version,
                                                      sync::DownloadBatchState batch_state) mutable {
                    auto session = weak_session.lock();
                    if (!session) {
                        return true;
                    }

                    if (query_version == 1 && batch_state == sync::DownloadBatchState::MoreToCome) {
                        session->close();
                        promise->emplace_value();
                        return false;
                    }
                    return true;
                };
            auto realm = Realm::get_shared_realm(config);
            {
                auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
                auto table = realm->read_group().get_table("class_TopLevel");
                mut_subs.insert_or_assign(Query(table));
                std::move(mut_subs).commit();
            }

            interrupted.get();
            realm->sync_session()->shutdown_and_wait();
            realm->close();
        }

        _impl::RealmCoordinator::clear_all_caches();

        // Open up the realm without the sync client attached and verify that the realm got interrupted in the state
        // we expected it to be in.
        {
            auto realm = DB::create(sync::make_client_replication(), interrupted_realm_config.path);
            util::StderrLogger logger;
            sync::PendingBootstrapStore bootstrap_store(realm, &logger);
            REQUIRE(bootstrap_store.has_pending());
            auto pending_batch = bootstrap_store.peek_pending(1024 * 1024 * 16);
            REQUIRE(pending_batch.query_version == 1);
            REQUIRE(!pending_batch.progress);
            REQUIRE(pending_batch.remaining == 0);
            REQUIRE(pending_batch.changesets.size() == 1);

            check_interrupted_state(realm);
        }

        // Now we'll open a different realm and make some changes that would leave orphan objects on the client
        // if the bootstrap batches weren't being cached until lastInBatch were true.
        mutate_realm();

        // Finally re-open the realm whose bootstrap we interrupted and just wait for it to finish downloading.
        auto realm = Realm::get_shared_realm(interrupted_realm_config);
        auto table = realm->read_group().get_table("class_TopLevel");
        realm->get_latest_subscription_set()
            .get_state_change_notification(sync::SubscriptionSet::State::Complete)
            .get();
        wait_for_upload(*realm);
        wait_for_download(*realm);

        realm->refresh();
        auto expected_obj_ids = util::Span<ObjectId>(obj_ids_at_end).sub_span(0, 3);

        REQUIRE(table->size() == expected_obj_ids.size());
        for (auto& id : expected_obj_ids) {
            REQUIRE(table->find_primary_key(Mixed{id}));
        }
    }

    SECTION("interrupted after final bootstrap message before processing") {
        {
            auto [interrupted_promise, interrupted] = util::make_promise_future<void>();
            Realm::Config config = interrupted_realm_config;
            config.sync_config = std::make_shared<SyncConfig>(*interrupted_realm_config.sync_config);
            auto shared_promise = std::make_shared<util::Promise<void>>(std::move(interrupted_promise));
            config.sync_config->on_bootstrap_message_processed_hook =
                [promise = std::move(shared_promise)](std::weak_ptr<SyncSession> weak_session,
                                                      const sync::SyncProgress, int64_t query_version,
                                                      sync::DownloadBatchState batch_state) mutable {
                    auto session = weak_session.lock();
                    if (!session) {
                        return true;
                    }

                    if (query_version == 1 && batch_state == sync::DownloadBatchState::LastInBatch) {
                        session->close();
                        promise->emplace_value();
                        return false;
                    }
                    return true;
                };
            auto realm = Realm::get_shared_realm(config);
            {
                auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
                auto table = realm->read_group().get_table("class_TopLevel");
                mut_subs.insert_or_assign(Query(table));
                std::move(mut_subs).commit();
            }

            interrupted.get();
            realm->sync_session()->shutdown_and_wait();
            realm->close();
        }

        _impl::RealmCoordinator::clear_all_caches();

        // Open up the realm without the sync client attached and verify that the realm got interrupted in the state
        // we expected it to be in.
        {
            auto realm = DB::create(sync::make_client_replication(), interrupted_realm_config.path);
            util::StderrLogger logger;
            sync::PendingBootstrapStore bootstrap_store(realm, &logger);
            REQUIRE(bootstrap_store.has_pending());
            auto pending_batch = bootstrap_store.peek_pending(1024 * 1024 * 16);
            REQUIRE(pending_batch.query_version == 1);
            REQUIRE(static_cast<bool>(pending_batch.progress));
            REQUIRE(pending_batch.remaining == 0);
            REQUIRE(pending_batch.changesets.size() == 3);

            check_interrupted_state(realm);
        }

        // Now we'll open a different realm and make some changes that would leave orphan objects on the client
        // if the bootstrap batches weren't being cached until lastInBatch were true.
        mutate_realm();

        auto [saw_valid_state_promise, saw_valid_state_future] = util::make_promise_future<void>();
        auto shared_saw_valid_state_promise =
            std::make_shared<decltype(saw_valid_state_promise)>(std::move(saw_valid_state_promise));
        SharedRealm realm;
        // This hook will let us check what the state of the realm is before it's integrated any new download
        // messages from the server. This should be the full 5 object bootstrap that was received before we
        // called mutate_realm().
        interrupted_realm_config.sync_config->on_download_message_received_hook =
            [&, promise = std::move(shared_saw_valid_state_promise)](std::weak_ptr<SyncSession> weak_session,
                                                                     const sync::SyncProgress&, int64_t query_version,
                                                                     sync::DownloadBatchState batch_state) {
                auto session = weak_session.lock();
                if (!session) {
                    return;
                }

                if (query_version != 1 || batch_state != sync::DownloadBatchState::LastInBatch) {
                    return;
                }

                auto latest_sub_set = session->get_flx_subscription_store()->get_latest();
                auto active_sub_set = session->get_flx_subscription_store()->get_active();
                REQUIRE(latest_sub_set.version() == active_sub_set.version());
                REQUIRE(active_sub_set.state() == sync::SubscriptionSet::State::Complete);

                auto db = SyncSession::OnlyForTesting::get_db(*session);
                auto tr = db->start_read();

                auto table = tr->get_table("class_TopLevel");
                REQUIRE(table->size() == obj_ids_at_end.size());
                for (auto& id : obj_ids_at_end) {
                    REQUIRE(table->find_primary_key(Mixed{id}));
                }

                promise->emplace_value();
            };

        // Finally re-open the realm whose bootstrap we interrupted and just wait for it to finish downloading.
        realm = Realm::get_shared_realm(interrupted_realm_config);
        saw_valid_state_future.get();
        auto table = realm->read_group().get_table("class_TopLevel");
        realm->get_latest_subscription_set()
            .get_state_change_notification(sync::SubscriptionSet::State::Complete)
            .get();
        wait_for_upload(*realm);
        wait_for_download(*realm);

        realm->refresh();
        auto expected_obj_ids = util::Span<ObjectId>(obj_ids_at_end).sub_span(0, 3);

        // After we've downloaded all the mutations there should only by 3 objects left.
        REQUIRE(table->size() == expected_obj_ids.size());
        for (auto& id : expected_obj_ids) {
            REQUIRE(table->find_primary_key(Mixed{id}));
        }
    }
}

} // namespace realm::app

#endif // REALM_ENABLE_AUTH_TESTS
