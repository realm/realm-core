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

#include <util/crypt_key.hpp>
#include <util/sync/baas_admin_api.hpp>
#include <util/sync/flx_sync_harness.hpp>
#include <util/sync/sync_test_utils.hpp>

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/mongo_database.hpp>

#include <realm/sync/noinst/client_history_impl.hpp>

#include <realm/util/future.hpp>

#include <catch2/catch_all.hpp>

#include <any>

using namespace std::string_literals;
using namespace realm::sync;

namespace realm::app {

namespace {

void create_schema(const AppSession& app_session, std::shared_ptr<SyncUser> user, Schema target_schema,
                   int64_t target_schema_version)
{
    auto create_config = app_session.config;
    create_config.schema = target_schema;
    app_session.admin_api.create_schema(app_session.server_app_id, create_config);

    auto remote_client = user->mongo_client("BackingDB");
    auto db = remote_client.db("app");
    auto settings = db["schema_history"];

    timed_sleeping_wait_for(
        [&] {
            bson::BsonDocument filter_doc{{"app_id", ObjectId(app_session.server_app_id)},
                                          {"version", target_schema_version - 1}};
            bool found = false;
            settings.find_one(filter_doc,
                              [&](util::Optional<bson::BsonDocument> document, util::Optional<app::AppError> error) {
                                  REQUIRE_FALSE(error);
                                  found = document.has_value();
                              });
            return found;
        },
        std::chrono::minutes(5), std::chrono::milliseconds(500));
}

std::pair<SharedRealm, std::exception_ptr> async_open_realm(const Realm::Config& config)
{
    std::mutex mutex;
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
    auto realm = error ? nullptr : Realm::get_shared_realm(std::move(realm_ref));
    return std::pair(realm, error);
}

std::vector<ObjectSchema> get_schema_v0()
{
    return {
        {"TopLevel",
         {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
          {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
          {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
          {"non_queryable_field", PropertyType::String | PropertyType::Nullable},
          {"non_queryable_field2", PropertyType::String}}},
        {"TopLevel2",
         {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
          {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
          {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
          {"non_queryable_field", PropertyType::String | PropertyType::Nullable}}},
        {"TopLevel3",
         {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}}, {"queryable_int_field", PropertyType::Int}}},
    };
}

auto get_subscription_initializer_callback_for_schema_v0()
{
    return [](std::shared_ptr<Realm> realm) mutable {
        REQUIRE(realm);
        auto table = realm->read_group().get_table("class_TopLevel");
        auto col_key = table->get_column_key("queryable_int_field");
        auto query = Query(table).greater_equal(col_key, int64_t(0));
        auto table2 = realm->read_group().get_table("class_TopLevel2");
        Query query2(table2);
        table = realm->read_group().get_table("class_TopLevel3");
        col_key = table->get_column_key("queryable_int_field");
        auto query3 = Query(table).greater_equal(col_key, int64_t(0));
        auto subs = realm->get_latest_subscription_set().make_mutable_copy();
        subs.clear();
        subs.insert_or_assign(query);
        subs.insert_or_assign(query2);
        subs.insert_or_assign(query3);
        subs.commit();
    };
}

// The following breaking changes are applied to schema at v0:
//  * Table 'TopLevel2' is removed
//  * Field 'queryable_str_field' in table 'TopLevel' is removed (the user does not query on it)
//  * Field 'non_queryable_field' in table 'TopLevel' is marked required
//  * Field 'non_queryable_field2' in table 'TopLevel' is marked optional
//  * Filed 'queryable_int_field' in table 'TopLevel3' is removed (the user queries on it)
std::vector<ObjectSchema> get_schema_v1()
{
    return {
        {"TopLevel",
         {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
          {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
          {"non_queryable_field", PropertyType::String},
          {"non_queryable_field2", PropertyType::String | PropertyType::Nullable}}},
        {"TopLevel3", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}}}},
    };
}

auto get_subscription_initializer_callback_for_schema_v1()
{
    return [](std::shared_ptr<Realm> realm) mutable {
        REQUIRE(realm);
        auto table = realm->read_group().get_table("class_TopLevel");
        Query query(table);
        table = realm->read_group().get_table("class_TopLevel3");
        Query query2(table);
        auto subs = realm->get_latest_subscription_set().make_mutable_copy();
        subs.clear();
        subs.insert_or_assign(query);
        subs.insert_or_assign(query2);
        subs.commit();
    };
}

// The following breaking changes are applied to schema at v1:
//  * Field 'queryable_int_field' in table 'TopLevel' is marked required
std::vector<ObjectSchema> get_schema_v2()
{
    return {
        {"TopLevel",
         {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
          {"queryable_int_field", PropertyType::Int},
          {"non_queryable_field", PropertyType::String},
          {"non_queryable_field2", PropertyType::String | PropertyType::Nullable}}},
        {"TopLevel3", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}}}},
    };
}

auto get_subscription_initializer_callback_for_schema_v2()
{
    return [](std::shared_ptr<Realm> realm) mutable {
        REQUIRE(realm);
        auto table = realm->read_group().get_table("class_TopLevel");
        auto col_key = table->get_column_key("queryable_int_field");
        auto query = Query(table).greater_equal(col_key, int64_t(5));
        table = realm->read_group().get_table("class_TopLevel3");
        Query query2(table);
        auto subs = realm->get_latest_subscription_set().make_mutable_copy();
        subs.clear();
        subs.insert_or_assign(query);
        subs.insert_or_assign(query2);
        subs.commit();
    };
}

// Sort 'computed_properties' and 'persisted_properties'.
ObjectSchema sort_schema_properties(const ObjectSchema& schema)
{
    ObjectSchema target_schema = schema;
    auto predicate = [](const Property& a, const Property& b) {
        return a.name < b.name;
    };
    std::vector<Property> persisted_properties = schema.persisted_properties;
    std::sort(std::begin(persisted_properties), std::end(persisted_properties), predicate);
    target_schema.persisted_properties = persisted_properties;
    std::vector<Property> computed_properties = schema.computed_properties;
    std::sort(std::begin(computed_properties), std::end(computed_properties), predicate);
    target_schema.computed_properties = computed_properties;
    return target_schema;
}

// Check realm's schema and target_schema match.
void check_realm_schema(SharedRealm& realm, const std::vector<ObjectSchema>& target_schema,
                        uint64_t target_schema_version)
{
    auto realm_schema = ObjectStore::schema_from_group(realm->read_group());
    CHECK(realm->schema_version() == target_schema_version);
    CHECK(realm_schema.size() == target_schema.size());

    for (auto& object : target_schema) {
        auto it = realm_schema.find(object);
        CHECK(it != realm_schema.end());
        auto target_object_schema = sort_schema_properties(object);
        auto realm_object_schema = sort_schema_properties(*it);
        CHECK(target_object_schema == realm_object_schema);
    }
}

} // namespace

TEST_CASE("Sync schema migrations don't not work with sync open", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    // First open the realm at schema version 0.
    {
        auto realm = Realm::get_shared_realm(config);
        subscribe_to_all_and_bootstrap(*realm);
        wait_for_upload(*realm);
    }

    const AppSession& app_session = harness.session().app_session();

    // Bump the schema version.
    config.schema_version = 1;
    auto schema_v1 = schema_v0;

    SECTION("Breaking change detected by client") {
        // Make field 'non_queryable_field2' of table 'TopLevel' optional.
        schema_v1[0].persisted_properties.back() = {"non_queryable_field2",
                                                    PropertyType::String | PropertyType::Nullable};
        config.schema = schema_v1;
        create_schema(app_session, harness.app()->current_user(), *config.schema, config.schema_version);

        REQUIRE_THROWS_AS(Realm::get_shared_realm(config), InvalidAdditiveSchemaChangeException);
    }

    SECTION("Breaking change detected by server") {
        // Remove table 'TopLevel2'.
        schema_v1.pop_back();
        config.schema = schema_v1;
        create_schema(app_session, harness.app()->current_user(), *config.schema, config.schema_version);

        config.sync_config->on_sync_client_event_hook = [&](std::weak_ptr<SyncSession> weak_session,
                                                            const SyncClientHookData& data) mutable {
            if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
                return SyncClientHookAction::NoAction;
            }
            auto session = weak_session.lock();
            REQUIRE(session);

            auto error_code = sync::ProtocolError(data.error_info->raw_error_code);
            if (error_code == sync::ProtocolError::initial_sync_not_completed) {
                return SyncClientHookAction::NoAction;
            }
            CHECK(error_code == sync::ProtocolError::schema_version_changed);
            return SyncClientHookAction::NoAction;
        };
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);
        wait_for_upload(*realm);

        auto table = realm->read_group().get_table("class_TopLevel2");
        // Migration did not succeed because table 'TopLevel2' still exists (but there is no error).
        CHECK(table);
    }
}

TEST_CASE("Cannot migrate schema to unknown version", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();

    SECTION("Fresh realm") {
        SECTION("No schema versions") {
        }

        SECTION("Schema versions") {
            create_schema(app_session, harness.app()->current_user(), schema_v1, 1);
        }
    }

    SECTION("Existing realm") {
        // First open the realm at schema version 0.
        {
            auto realm = Realm::get_shared_realm(config);
            subscribe_to_all_and_bootstrap(*realm);
            wait_for_upload(*realm);
        }

        SECTION("No schema versions") {
        }

        SECTION("Schema versions") {
            create_schema(app_session, harness.app()->current_user(), schema_v1, 1);
        }
    }

    // Bump the schema to a version the server does not know about.
    config.schema_version = 42;
    config.schema = schema_v0;
    config.sync_config->error_handler = nullptr;
    config.sync_config->on_sync_client_event_hook = [](std::weak_ptr<SyncSession> weak_session,
                                                       const SyncClientHookData& data) mutable {
        if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
            return SyncClientHookAction::NoAction;
        }
        auto session = weak_session.lock();
        REQUIRE(session);

        auto error_code = sync::ProtocolError(data.error_info->raw_error_code);
        if (error_code == sync::ProtocolError::initial_sync_not_completed) {
            return SyncClientHookAction::NoAction;
        }

        CHECK(error_code == sync::ProtocolError::bad_schema_version);
        session->force_close();
        return SyncClientHookAction::NoAction;
    };

    auto [realm, error] = async_open_realm(config);
    REQUIRE_FALSE(realm);
    REQUIRE(error);

    // Update schema version to 0 and try again (the version now matches the actual schema).
    config.schema_version = 0;
    config.sync_config->on_sync_client_event_hook = nullptr;
    std::tie(realm, error) = async_open_realm(config);
    REQUIRE(realm);
    REQUIRE_FALSE(error);
    check_realm_schema(realm, schema_v0, 0);
}

TEST_CASE("Schema version mismatch between client and server", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, harness.app()->current_user(), schema_v1, 1);

    bool schema_migration_expected = false;

    SECTION("Fresh realm") {
        schema_migration_expected = false;
    }

    SECTION("Existing realm") {
        auto realm = Realm::get_shared_realm(config);
        subscribe_to_all_and_bootstrap(*realm);
        wait_for_upload(*realm);

        schema_migration_expected = true;

        SECTION("Realm already on the latest schema version") {
            DBOptions options;
            options.encryption_key = test_util::crypt_key();
            auto db = DB::create(sync::make_client_replication(), config.path, options);
            auto tr = db->start_write();
            ObjectStore::set_schema_version(*tr, 1);
            tr->commit();
            auto schema_version = ObjectStore::get_schema_version(*db->start_read());
            CHECK(schema_version == 1);
        }
        SECTION("Open realm with the lastest schema version for the first time") {
        }
    }

    config.schema_version = 1;
    config.schema = schema_v0;

    auto schema_migration_required = false;
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
    config.sync_config->error_handler = nullptr;
    config.sync_config->on_sync_client_event_hook =
        [&schema_migration_required](std::weak_ptr<SyncSession> weak_session,
                                     const SyncClientHookData& data) mutable {
            if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
                return SyncClientHookAction::NoAction;
            }
            auto session = weak_session.lock();
            REQUIRE(session);

            auto error_code = sync::ProtocolError(data.error_info->raw_error_code);
            if (error_code != sync::ProtocolError::schema_version_changed) {
                return SyncClientHookAction::NoAction;
            }
            schema_migration_required = true;
            return SyncClientHookAction::NoAction;
        };

    auto [realm, error] = async_open_realm(config);
    REQUIRE_FALSE(realm);
    REQUIRE(error);
    REQUIRE(schema_migration_expected == schema_migration_required);
}

TEST_CASE("Fresh realm does not require schema migration", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, harness.app()->current_user(), schema_v1, 1);

    config.schema_version = 1;
    config.schema = schema_v1;
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();
    config.sync_config->on_sync_client_event_hook = [](std::weak_ptr<SyncSession> weak_session,
                                                       const SyncClientHookData& data) mutable {
        if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
            return SyncClientHookAction::NoAction;
        }
        auto session = weak_session.lock();
        REQUIRE(session);

        auto error_code = sync::ProtocolError(data.error_info->raw_error_code);
        CHECK(error_code == sync::ProtocolError::initial_sync_not_completed);
        return SyncClientHookAction::NoAction;
    };

    auto [realm, error] = async_open_realm(config);
    REQUIRE(realm);
    REQUIRE_FALSE(error);
    check_realm_schema(realm, schema_v1, 1);
}

TEST_CASE("Upgrade schema version (with recovery) then downgrade", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    {
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);
        wait_for_upload(*realm);
        check_realm_schema(realm, schema_v0, 0);

        realm->sync_session()->pause();

        // Subscription to recover when upgrading the schema.
        auto subs = realm->get_latest_subscription_set().make_mutable_copy();
        CHECK(subs.erase_by_class_name("TopLevel2"));
        auto table = realm->read_group().get_table("class_TopLevel2");
        auto col_key = table->get_column_key("queryable_int_field");
        auto query = Query(table).greater_equal(col_key, int64_t(0));
        subs.insert_or_assign(query);
        subs.commit();

        // Object to recover when upgrading the schema.
        realm->begin_transaction();
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", ObjectId::gen()},
                                        {"queryable_str_field", "biz"s},
                                        {"queryable_int_field", static_cast<int64_t>(15)},
                                        {"non_queryable_field2", "non queryable 33"s}}));
        realm->commit_transaction();
        // This server drops this object because the client is querying on a removed field.
        realm->begin_transaction();
        Object::create(
            c, realm, "TopLevel3",
            std::any(AnyDict{{"_id", ObjectId::gen()}, {"queryable_int_field", static_cast<int64_t>(42)}}));

        realm->close();
    }

    auto obj3_id = ObjectId::gen();
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", ObjectId::gen()},
                                        {"queryable_str_field", "foo"s},
                                        {"queryable_int_field", static_cast<int64_t>(5)},
                                        {"non_queryable_field", "non queryable 1"s},
                                        {"non_queryable_field2", "non queryable 11"s}}));
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", ObjectId::gen()},
                                        {"queryable_str_field", "bar"s},
                                        {"queryable_int_field", static_cast<int64_t>(10)},
                                        {"non_queryable_field", "non queryable 2"s},
                                        {"non_queryable_field2", "non queryable 22"s}}));
        Object::create(c, realm, "TopLevel2",
                       std::any(AnyDict{{"_id", ObjectId::gen()},
                                        {"queryable_str_field", "foo2"s},
                                        {"queryable_int_field", static_cast<int64_t>(10)},
                                        {"non_queryable_field", "non queryable 2"s}}));
        Object::create(c, realm, "TopLevel3",
                       std::any(AnyDict{{"_id", obj3_id}, {"queryable_int_field", static_cast<int64_t>(10000)}}));
    });

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, harness.app()->current_user(), schema_v1, 1);
    auto schema_v2 = get_schema_v2();
    create_schema(app_session, harness.app()->current_user(), schema_v2, 2);

    // First schema upgrade.
    {
        // Upgrade the schema version
        config.schema_version = 1;
        config.schema = schema_v1;
        config.sync_config->subscription_initializer = nullptr;

        // Cannot migrate the schema without setting the subscription initializer callback.
        auto [realm, error] = async_open_realm(config);
        REQUIRE_FALSE(realm);
        REQUIRE(error);

        // Retry migration with subscription initializer callback set.
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();
        std::tie(realm, error) = async_open_realm(config);
        REQUIRE(realm);
        REQUIRE_FALSE(error);
        check_realm_schema(realm, schema_v1, 1);

        auto table = realm->read_group().get_table("class_TopLevel");
        CHECK(table->size() == 3);
        table = realm->read_group().get_table("class_TopLevel3");
        CHECK(table->size() == 1);
        CHECK(table->get_object_with_primary_key(obj3_id));

        realm->begin_transaction();
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", ObjectId::gen()},
                                        {"queryable_int_field", static_cast<int64_t>(15)},
                                        {"non_queryable_field", "non queryable 4"s},
                                        {"non_queryable_field2", "non queryable 44"s}}));
        realm->commit_transaction();

        wait_for_upload(*realm);
        wait_for_download(*realm);
    }

    // Second schema upgrade.
    {
        config.schema_version = 2;
        config.schema = schema_v2;
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v2();

        auto [realm, error] = async_open_realm(config);
        REQUIRE(realm);
        REQUIRE_FALSE(error);
        check_realm_schema(realm, schema_v2, 2);

        auto table = realm->read_group().get_table("class_TopLevel");
        CHECK(table->size() == 4);
        table = realm->read_group().get_table("class_TopLevel3");
        CHECK(table->size() == 1);
        CHECK(table->get_object_with_primary_key(obj3_id));
    }

    // First schema downgrade.
    {
        config.schema_version = 1;
        config.schema = schema_v1;
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();

        auto [realm, error] = async_open_realm(config);
        REQUIRE(realm);
        REQUIRE_FALSE(error);
        check_realm_schema(realm, schema_v1, 1);

        auto table = realm->read_group().get_table("class_TopLevel");
        CHECK(table->size() == 4);
        table = realm->read_group().get_table("class_TopLevel3");
        CHECK(table->size() == 1);
        CHECK(table->get_object_with_primary_key(obj3_id));
    }

    // Second schema downgrade.
    {
        config.schema_version = 0;
        config.schema = schema_v0;
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();

        auto [realm, error] = async_open_realm(config);
        REQUIRE(realm);
        REQUIRE_FALSE(error);
        check_realm_schema(realm, schema_v0, 0);

        auto table = realm->read_group().get_table("class_TopLevel");
        CHECK(table->size() == 4);
        table = realm->read_group().get_table("class_TopLevel2");
        CHECK(table->is_empty());
        auto table3 = realm->read_group().get_table("class_TopLevel3");
        CHECK(table3->is_empty());

        // The existing subscription for 'TopLevel3' is on a removed field (in version 1), so data cannot be sync'd.
        // Update subscription so data can be sync'd.
        auto subs = realm->get_latest_subscription_set().make_mutable_copy();
        CHECK(subs.erase_by_class_name("TopLevel3"));
        subs.insert_or_assign(Query(table3));
        auto new_subs = subs.commit();
        new_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        realm->refresh();
        CHECK(table3->size() == 1);
        CHECK(table3->get_object_with_primary_key(obj3_id));
    }
}

TEST_CASE("An interrupted schema migration can recover on the next session",
          "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    {
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);
        wait_for_upload(*realm);
        check_realm_schema(realm, schema_v0, 0);
    }

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, harness.app()->current_user(), schema_v1, 1);

    config.schema_version = 1;
    config.schema = schema_v1;
    auto bad_schema_version_count = 0;
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();
    config.sync_config->on_sync_client_event_hook =
        [&bad_schema_version_count](std::weak_ptr<SyncSession> weak_session, const SyncClientHookData& data) mutable {
            if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
                return SyncClientHookAction::NoAction;
            }
            auto session = weak_session.lock();
            REQUIRE(session);

            auto error_code = sync::ProtocolError(data.error_info->raw_error_code);
            if (error_code == sync::ProtocolError::initial_sync_not_completed) {
                return SyncClientHookAction::NoAction;
            }

            CHECK(error_code == sync::ProtocolError::schema_version_changed);
            // Pause and resume the session the first time the a schema migration is required.
            if (++bad_schema_version_count == 1) {
                session->force_close();
            }
            return SyncClientHookAction::NoAction;
        };

    auto [realm, error] = async_open_realm(config);
    REQUIRE_FALSE(realm);
    REQUIRE(error);

    // Retry the migration.
    std::tie(realm, error) = async_open_realm(config);
    REQUIRE(realm);
    REQUIRE_FALSE(error);
    REQUIRE(bad_schema_version_count == 2);
    check_realm_schema(realm, schema_v1, 1);
}

TEST_CASE("Migrate to new schema version with a schema subset", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    {
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);
        wait_for_upload(*realm);
        check_realm_schema(realm, schema_v0, 0);
    }

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, harness.app()->current_user(), schema_v1, 1);

    config.schema_version = 1;
    auto schema_subset = schema_v1;
    // One of the columns in 'TopLevel' is not needed by the user.
    schema_subset[0].persisted_properties.pop_back();
    config.schema = schema_subset;
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();

    auto [realm, error] = async_open_realm(config);
    REQUIRE(realm);
    REQUIRE_FALSE(error);
    check_realm_schema(realm, schema_v1, 1);
}

TEST_CASE("Client reset during schema migration", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    {
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);
        wait_for_upload(*realm);
        check_realm_schema(realm, schema_v0, 0);

        realm->sync_session()->pause();

        realm->begin_transaction();
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", ObjectId::gen()},
                                        {"queryable_str_field", "foo"s},
                                        {"queryable_int_field", static_cast<int64_t>(15)},
                                        {"non_queryable_field2", "non queryable 11"s}}));
        Object::create(
            c, realm, "TopLevel3",
            std::any(AnyDict{{"_id", ObjectId::gen()}, {"queryable_int_field", static_cast<int64_t>(42)}}));
        realm->commit_transaction();
        realm->close();
    }

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, harness.app()->current_user(), schema_v1, 1);

    config.schema_version = 1;
    config.schema = schema_v1;
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();
    config.sync_config->client_resync_mode = ClientResyncMode::Recover;
    config.sync_config->on_sync_client_event_hook = [&harness, schema_version_changed_count =
                                                                   0](std::weak_ptr<SyncSession> weak_session,
                                                                      const SyncClientHookData& data) mutable {
        if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
            return SyncClientHookAction::NoAction;
        }
        auto session = weak_session.lock();
        REQUIRE(session);

        auto error_code = sync::ProtocolError(data.error_info->raw_error_code);
        if (error_code == sync::ProtocolError::initial_sync_not_completed) {
            return SyncClientHookAction::NoAction;
        }

        if (error_code == sync::ProtocolError::schema_version_changed) {
            ++schema_version_changed_count;
            if (schema_version_changed_count == 1) {
                reset_utils::trigger_client_reset(harness.session().app_session());
            }
        }

        return SyncClientHookAction::NoAction;
    };
    size_t before_reset_count = 0;
    size_t after_reset_count = 0;
    config.sync_config->notify_before_client_reset = [&before_reset_count](SharedRealm) {
        ++before_reset_count;
    };
    config.sync_config->notify_after_client_reset = [&after_reset_count](SharedRealm, ThreadSafeReference, bool) {
        ++after_reset_count;
    };

    auto [realm, error] = async_open_realm(config);
    REQUIRE(realm);
    REQUIRE_FALSE(error);
    REQUIRE(before_reset_count == 0);
    REQUIRE(after_reset_count == 0);
    check_realm_schema(realm, schema_v1, 1);

    auto table = realm->read_group().get_table("class_TopLevel");
    CHECK(table->size() == 1);
    table = realm->read_group().get_table("class_TopLevel3");
    CHECK(table->is_empty());
}

TEST_CASE("Migrate to new schema version after migration to intermediate version is interrupted",
          "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    {
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);
        wait_for_upload(*realm);
        check_realm_schema(realm, schema_v0, 0);

        realm->sync_session()->pause();

        realm->begin_transaction();
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", ObjectId::gen()},
                                        {"queryable_str_field", "foo"s},
                                        {"queryable_int_field", static_cast<int64_t>(15)},
                                        {"non_queryable_field2", "non queryable 11"s}}));
        Object::create(
            c, realm, "TopLevel3",
            std::any(AnyDict{{"_id", ObjectId::gen()}, {"queryable_int_field", static_cast<int64_t>(42)}}));
        realm->commit_transaction();
        realm->close();
    }

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, harness.app()->current_user(), schema_v1, 1);
    auto schema_v2 = get_schema_v2();
    create_schema(app_session, harness.app()->current_user(), schema_v2, 2);

    config.schema_version = 1;
    config.schema = schema_v1;
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();
    config.sync_config->client_resync_mode = ClientResyncMode::Recover;
    config.sync_config->on_sync_client_event_hook = [](std::weak_ptr<SyncSession> weak_session,
                                                       const SyncClientHookData& data) mutable {
        if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
            return SyncClientHookAction::NoAction;
        }
        auto session = weak_session.lock();
        REQUIRE(session);

        auto error_code = sync::ProtocolError(data.error_info->raw_error_code);
        if (error_code != sync::ProtocolError::schema_version_changed) {
            return SyncClientHookAction::NoAction;
        }
        // Close the session once the first migration is requested by the server.
        session->force_close();
        return SyncClientHookAction::NoAction;
    };

    // Migration to v1 is interrupted.
    auto [realm, error] = async_open_realm(config);
    REQUIRE_FALSE(realm);
    REQUIRE(error);

    // Migrate to v2.
    config.schema_version = 2;
    config.schema = schema_v2;
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v2();
    config.sync_config->on_sync_client_event_hook = nullptr;
    std::tie(realm, error) = async_open_realm(config);
    REQUIRE(realm);
    REQUIRE_FALSE(error);
    check_realm_schema(realm, schema_v2, 2);

    auto table = realm->read_group().get_table("class_TopLevel");
    CHECK(table->size() == 1);
    table = realm->read_group().get_table("class_TopLevel3");
    CHECK(table->is_empty());
}

} // namespace realm::app

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC