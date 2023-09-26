////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
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

#if REALM_ENABLE_SYNC
#if REALM_ENABLE_AUTH_TESTS

#include <util/sync/baas_admin_api.hpp>
#include <util/sync/flx_sync_harness.hpp>
#include <util/sync/sync_test_utils.hpp>

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/mongo_database.hpp>

#include <realm/util/future.hpp>

#include <catch2/catch_all.hpp>

#include <any>

using namespace std::string_literals;

namespace realm::app {

namespace {

// TODO: to be removed once the feature flag is removed.
void set_schema_versioning_feature_flag(const std::string& app_id, std::shared_ptr<SyncUser> user)
{
    auto remote_client = user->mongo_client("BackingDB");
    auto db = remote_client.db("app");
    auto settings = db["settings"];

    bson::BsonDocument filter_doc{{"_id", ObjectId(app_id)}};
    bson::BsonDocument update_doc{{"$addToSet", bson::BsonDocument{{"features.enabled", "enable_schema_versioning"}}}};
    settings.update_one(filter_doc, update_doc, false,
                        [&](app::MongoCollection::UpdateResult result, util::Optional<app::AppError> error) {
                            REQUIRE_FALSE(error);
                            CHECK(result.modified_count == 1);
                        });
}

} // namespace

TEST_CASE("flx: new schema version", "[sync][flx][baas]") {
    Schema schema_v0{
        {"TopLevel",
         {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
          {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
          {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
          {"non_queryable_field", PropertyType::String | PropertyType::Nullable}}},
        {"TopLevel2",
         {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
          {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
          {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
          {"non_queryable_field", PropertyType::String | PropertyType::Nullable}}},
    };
    FLXSyncTestHarness harness("flx_schema_versions", {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    const AppSession& app_session = harness.session().app_session();
    //set_schema_versioning_feature_flag(app_session.server_app_id, harness.app()->current_user());

    {
        config.schema_version = 0;
        auto orig_realm = Realm::get_shared_realm(config);

        wait_for_download(*orig_realm);

        auto table = orig_realm->read_group().get_table("class_TopLevel");
        auto queryable_str_field = table->get_column_key("queryable_str_field");
        auto new_subs = orig_realm->get_latest_subscription_set().make_mutable_copy();
        new_subs.insert_or_assign(Query(table).not_equal(queryable_str_field, ""));
        auto subs = new_subs.commit();
        subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

        wait_for_upload(*orig_realm);

        orig_realm->sync_session()->pause();

        orig_realm->begin_transaction();
        CppContext c(orig_realm);
        Object::create(c, orig_realm, "TopLevel",
                       std::any(AnyDict{{"_id", ObjectId::gen()},
                                        {"queryable_str_field", "biz"s},
                                        {"queryable_int_field", static_cast<int64_t>(15)},
                                        {"non_queryable_field", "non queryable 3"s}}));
        orig_realm->commit_transaction();
        orig_realm->close();
    }

    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", ObjectId::gen()},
                                        {"queryable_str_field", "foo"s},
                                        {"queryable_int_field", static_cast<int64_t>(5)},
                                        {"non_queryable_field", "non queryable 1"s}}));
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", ObjectId::gen()},
                                        {"queryable_str_field", "bar"s},
                                        {"queryable_int_field", static_cast<int64_t>(10)},
                                        {"non_queryable_field", "non queryable 2"s}}));
    });

    Schema schema_v1{{"TopLevel",
                      {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                       {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
                       {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
                       {"non_queryable_field", PropertyType::String | PropertyType::Nullable}}}};
    
    set_schema_versioning_feature_flag(app_session.server_app_id, harness.app()->current_user());
    app_session.admin_api.update_schema(app_session.server_app_id, schema_v1);

    {
        // Bump the schema version
        config.schema_version = 1;
        config.schema = schema_v1;
        config.sync_config->subscription_initializer = [](std::shared_ptr<Realm> realm) mutable {
            REQUIRE(realm);
            auto table = realm->read_group().get_table("class_TopLevel");
            Query query(table);
            auto subs = realm->get_latest_subscription_set().make_mutable_copy();
            subs.insert_or_assign(query);
            subs.commit();
        };

        std::mutex mutex;

        auto async_open_realm = [&](const Realm::Config& config) {
            ThreadSafeReference realm_ref;
            std::exception_ptr error;
            auto task = Realm::get_synchronized_realm(config);
            task->start([&](ThreadSafeReference&& ref, std::exception_ptr e) {
                std::lock_guard lock(mutex);
                realm_ref = std::move(ref);
                error = e;
            });
            util::EventLoop::main().run_until([&] {
                std::lock_guard lock(mutex);
                return realm_ref || error;
            });
            return std::pair(std::move(realm_ref), error);
        };

        auto [ref, error] = async_open_realm(config);
        REQUIRE(ref);
        REQUIRE_FALSE(error);

        auto realm = Realm::get_shared_realm(std::move(ref));
        auto table = realm->read_group().get_table("class_TopLevel");
        // REQUIRE(!table->get_column_key("queryable_str_field"));
        REQUIRE(table);
        auto table2 = realm->read_group().get_table("class_TopLevel2");
        REQUIRE(!table2);

        realm->begin_transaction();
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", ObjectId::gen()},
                                        {"queryable_int_field", static_cast<int64_t>(15)},
                                        {"non_queryable_field", "non queryable 4"s}}));
        realm->commit_transaction();

        wait_for_upload(*realm);
        wait_for_download(*realm);

        wait_for_advance(*realm);
        Results results(realm, table);
        CHECK(results.size() == 4);
    }
}

} // namespace realm::app

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC