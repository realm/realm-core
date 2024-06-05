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

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/mongo_database.hpp>

#include <realm/sync/noinst/client_history_impl.hpp>

#include <catch2/catch_all.hpp>

#include <any>

using namespace std::string_literals;
using namespace realm::sync;

namespace realm::app {

namespace {

void create_schema(const AppSession& app_session, Schema target_schema, int64_t target_schema_version)
{
    auto create_config = app_session.config;
    create_config.schema = target_schema;
    app_session.admin_api.create_schema(app_session.server_app_id, create_config);

    timed_sleeping_wait_for(
        [&] {
            auto versions = app_session.admin_api.get_schema_versions(app_session.server_app_id);
            return std::any_of(versions.begin(), versions.end(), [&](const AdminAPISession::SchemaVersionInfo& info) {
                return info.version_major == target_schema_version;
            });
        },
        std::chrono::minutes(5), std::chrono::seconds(1));

    // FIXME: There is a delay on the server between the schema being created and actually ready to use. This is due
    // to resource pool key cache keys using second precision (BAAS-18361). So we wait for a couple of seconds so the
    // app is refreshed.
    const auto wait_start = std::chrono::steady_clock::now();
    using namespace std::chrono_literals;
    util::EventLoop::main().run_until([&]() -> bool {
        return std::chrono::steady_clock::now() - wait_start >= 2s;
    });
}

std::pair<SharedRealm, std::exception_ptr> async_open_realm(const Realm::Config& config)
{
    auto task = Realm::get_synchronized_realm(config);
    ThreadSafeReference tsr;
    SharedRealm realm;
    std::exception_ptr err = nullptr;
    auto pf = util::make_promise_future<void>();
    task->start([&tsr, &err, promise = util::CopyablePromiseHolder(std::move(pf.promise))](
                    ThreadSafeReference&& ref, std::exception_ptr e) mutable {
        tsr = std::move(ref);
        err = e;
        promise.get_promise().emplace_value();
    });
    pf.future.get();
    realm = err ? nullptr : Realm::get_shared_realm(std::move(tsr));
    return std::pair(realm, err);
}

std::vector<ObjectSchema> get_schema_v0()
{
    return {
        {"Embedded", ObjectSchema::ObjectType::Embedded, {{"str_field", PropertyType::String}}},
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
         {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
          {"queryable_int_field", PropertyType::Int},
          {"link", PropertyType::Object | PropertyType::Nullable, "TopLevel"},
          {"embedded_link", PropertyType::Object | PropertyType::Nullable, "Embedded"}}},
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
        {"Embedded", ObjectSchema::ObjectType::Embedded, {{"str_field", PropertyType::String}}},
        {"TopLevel",
         {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
          {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
          {"non_queryable_field", PropertyType::String},
          {"non_queryable_field2", PropertyType::String | PropertyType::Nullable}}},
        {"TopLevel3",
         {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
          {"link", PropertyType::Object | PropertyType::Nullable, "TopLevel"},
          {"embedded_link", PropertyType::Object | PropertyType::Nullable, "Embedded"}}},
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
        {"Embedded", ObjectSchema::ObjectType::Embedded, {{"str_field", PropertyType::String}}},
        {"TopLevel",
         {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
          {"queryable_int_field", PropertyType::Int},
          {"non_queryable_field", PropertyType::String},
          {"non_queryable_field2", PropertyType::String | PropertyType::Nullable}}},
        {"TopLevel3",
         {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
          {"link", PropertyType::Object | PropertyType::Nullable, "TopLevel"},
          {"embedded_link", PropertyType::Object | PropertyType::Nullable, "Embedded"}}},
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
void check_realm_schema(const std::string& path, const std::vector<ObjectSchema>& target_schema,
                        uint64_t target_schema_version)
{
    DBOptions options;
    options.encryption_key = test_util::crypt_key();
    auto db = DB::create(sync::make_client_replication(), path, options);
    auto realm_schema = ObjectStore::schema_from_group(*db->start_read());
    auto realm_schema_version = ObjectStore::get_schema_version(*db->start_read());
    CHECK(realm_schema_version == target_schema_version);
    CHECK(realm_schema.size() == target_schema.size());

    for (auto& object : target_schema) {
        auto it = realm_schema.find(object);
        CHECK(it != realm_schema.end());
        auto target_object_schema = sort_schema_properties(object);
        auto realm_object_schema = sort_schema_properties(*it);
        CHECK(target_object_schema == realm_object_schema);
    }
}

auto make_error_handler()
{
    auto [error_promise, error_future] = util::make_promise_future<SyncError>();
    auto shared_promise = std::make_shared<decltype(error_promise)>(std::move(error_promise));
    auto fn = [error_promise = std::move(shared_promise)](std::shared_ptr<SyncSession>, SyncError err) {
        error_promise->emplace_value(std::move(err));
    };
    return std::make_pair(std::move(error_future), std::move(fn));
}

} // namespace

TEST_CASE("Sync schema migrations don't work with sync open", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    auto config = harness.make_test_file();

    // First open the realm at schema version 0.
    {
        auto realm = Realm::get_shared_realm(config);
        subscribe_to_all_and_bootstrap(*realm);
        wait_for_upload(*realm);
        check_realm_schema(config.path, schema_v0, 0);
    }

    const AppSession& app_session = harness.session().app_session();

    // Bump the schema version.
    config.schema_version = 1;
    auto schema_v1 = schema_v0;

    SECTION("Breaking change detected by client") {
        // Make field 'non_queryable_field2' of table 'TopLevel' optional.
        schema_v1[1].persisted_properties.back() = {"non_queryable_field2",
                                                    PropertyType::String | PropertyType::Nullable};
        config.schema = schema_v1;
        create_schema(app_session, *config.schema, config.schema_version);

        REQUIRE_THROWS_AS(Realm::get_shared_realm(config), InvalidAdditiveSchemaChangeException);
        check_realm_schema(config.path, schema_v0, 0);
    }

    SECTION("Breaking change detected by server") {
        // Remove table 'TopLevel2'.
        schema_v1.erase(schema_v1.begin() + 2);
        config.schema = schema_v1;
        create_schema(app_session, *config.schema, config.schema_version);

        config.sync_config->on_sync_client_event_hook = [&](std::weak_ptr<SyncSession>,
                                                            const SyncClientHookData& data) mutable {
            if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
                return SyncClientHookAction::NoAction;
            }

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
        check_realm_schema(config.path, schema_v0, 1);
    }
}

TEST_CASE("Cannot migrate schema to unknown version", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    auto config = harness.make_test_file();

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();

    uint64_t target_schema_version = 0;
    std::vector<ObjectSchema> target_schema;

    SECTION("Fresh realm") {
        target_schema_version = -1;
        target_schema = {};

        SECTION("No schema versions") {
        }

        SECTION("Schema versions") {
            create_schema(app_session, schema_v1, 1);
        }
    }

    SECTION("Existing realm") {
        auto schema_version = GENERATE(0, 42);

        // First open the realm at schema version 0.
        {
            auto realm = Realm::get_shared_realm(config);
            subscribe_to_all_and_bootstrap(*realm);
            wait_for_upload(*realm);
        }

        // Then set the right schema version.
        DBOptions options;
        options.encryption_key = test_util::crypt_key();
        auto db = DB::create(sync::make_client_replication(), config.path, options);
        auto tr = db->start_write();
        ObjectStore::set_schema_version(*tr, schema_version);
        tr->commit();

        target_schema_version = schema_version;
        target_schema = schema_v0;

        SECTION(util::format("No schema versions | Realm schema: %1", schema_version)) {
        }

        SECTION(util::format("Schema versions | Realm schema: %1", schema_version)) {
            create_schema(app_session, schema_v1, 1);
        }
    }

    // Bump the schema to a version the server does not know about.
    config.schema_version = 42;
    config.schema = schema_v0;
    auto&& [error_future, err_handler] = make_error_handler();
    config.sync_config->error_handler = err_handler;

    {
        auto [realm, error] = async_open_realm(config);
        REQUIRE_FALSE(realm);
        REQUIRE(error);
        REQUIRE_THROWS_CONTAINING(std::rethrow_exception(error), "Client provided invalid schema version");
        error_future.get();
        check_realm_schema(config.path, target_schema, target_schema_version);
    }

    // Update schema version to 0 and try again (the version now matches the actual schema).
    config.schema_version = 0;
    config.sync_config->error_handler = nullptr;
    auto [realm, error] = async_open_realm(config);
    REQUIRE(realm);
    REQUIRE_FALSE(error);
    check_realm_schema(config.path, schema_v0, 0);
}

TEST_CASE("Schema version mismatch between client and server", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    auto config = harness.make_test_file();

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, schema_v1, 1);

    {
        auto realm = Realm::get_shared_realm(config);
        subscribe_to_all_and_bootstrap(*realm);
        wait_for_upload(*realm);

        realm->sync_session()->shutdown_and_wait();
        check_realm_schema(config.path, schema_v0, 0);
    }
    _impl::RealmCoordinator::assert_no_open_realms();

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

    config.schema_version = 1;
    config.schema = schema_v0;

    auto schema_migration_required = false;
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
    config.sync_config->error_handler = nullptr;
    config.sync_config->on_sync_client_event_hook =
        [&schema_migration_required](std::weak_ptr<SyncSession>, const SyncClientHookData& data) mutable {
            if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
                return SyncClientHookAction::NoAction;
            }
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
    REQUIRE_THROWS_CONTAINING(std::rethrow_exception(error),
                              "The following changes cannot be made in additive-only schema mode");
    REQUIRE(schema_migration_required);
    // Applying the new schema (and version) fails, therefore the schema is unversioned (the metadata table is removed
    // during migration). There is a schema though because the server schema is already applied by the time the client
    // applies the mismatch schema.
    check_realm_schema(config.path, schema_v1, ObjectStore::NotVersioned);
    wait_for_sessions_to_close(harness.session());
}

TEST_CASE("Fresh realm does not require schema migration", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    auto config = harness.make_test_file();

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, schema_v1, 1);

    config.schema_version = 1;
    config.schema = schema_v1;
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();
    config.sync_config->on_sync_client_event_hook = [](std::weak_ptr<SyncSession>,
                                                       const SyncClientHookData& data) mutable {
        if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
            return SyncClientHookAction::NoAction;
        }
        auto error_code = sync::ProtocolError(data.error_info->raw_error_code);
        CHECK(error_code == sync::ProtocolError::initial_sync_not_completed);
        return SyncClientHookAction::NoAction;
    };

    auto [realm, error] = async_open_realm(config);
    REQUIRE(realm);
    REQUIRE_FALSE(error);
    check_realm_schema(config.path, schema_v1, 1);
}

TEST_CASE("Upgrade schema version (with recovery) then downgrade", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    auto config = harness.make_test_file();

    {
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);
        wait_for_upload(*realm);
        check_realm_schema(config.path, schema_v0, 0);

        realm->sync_session()->shutdown_and_wait();

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
        // The server filters out this object because the schema version the client migrates to removes the queryable
        // field.
        realm->begin_transaction();
        Object::create(
            c, realm, "TopLevel3",
            std::any(AnyDict{{"_id", ObjectId::gen()}, {"queryable_int_field", static_cast<int64_t>(42)}}));
        realm->commit_transaction();
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
    create_schema(app_session, schema_v1, 1);
    auto schema_v2 = get_schema_v2();
    create_schema(app_session, schema_v2, 2);

    // First schema upgrade.
    {
        // Upgrade the schema version
        config.schema_version = 1;
        config.schema = schema_v1;
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();
        auto [realm, error] = async_open_realm(config);
        REQUIRE(realm);
        REQUIRE_FALSE(error);
        check_realm_schema(config.path, schema_v1, 1);

        auto table = realm->read_group().get_table("class_TopLevel");
        CHECK(table->size() == 3);
        table = realm->read_group().get_table("class_TopLevel2");
        CHECK(!table);
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
        check_realm_schema(config.path, schema_v2, 2);

        auto table = realm->read_group().get_table("class_TopLevel");
        CHECK(table->size() == 4);
        table = realm->read_group().get_table("class_TopLevel2");
        CHECK(!table);
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
        check_realm_schema(config.path, schema_v1, 1);

        auto table = realm->read_group().get_table("class_TopLevel");
        CHECK(table->size() == 4);
        table = realm->read_group().get_table("class_TopLevel2");
        CHECK(!table);
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
        check_realm_schema(config.path, schema_v0, 0);

        auto table = realm->read_group().get_table("class_TopLevel");
        CHECK(table->size() == 4);
        table = realm->read_group().get_table("class_TopLevel2");
        CHECK(table->is_empty());
        auto table3 = realm->read_group().get_table("class_TopLevel3");
        CHECK(table3->is_empty());

        // The subscription for 'TopLevel3' is on a removed field (i.e, the field does not exist in the previous
        // schema version used), so data cannot be synced.
        // Update subscription so data can be synced.
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
    auto config = harness.make_test_file();

    {
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);
        wait_for_upload(*realm);
        check_realm_schema(config.path, schema_v0, 0);
    }
    _impl::RealmCoordinator::assert_no_open_realms();

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, schema_v1, 1);

    config.schema_version = 1;
    config.schema = schema_v1;
    auto schema_version_changed_count = 0;
    std::shared_ptr<AsyncOpenTask> task;
    auto [promise, future] = util::make_promise_future<void>();
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();
    config.sync_config->on_sync_client_event_hook =
        [&schema_version_changed_count, &task, promise = util::CopyablePromiseHolder<void>(std::move(promise))](
            std::weak_ptr<SyncSession>, const SyncClientHookData& data) mutable {
            if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
                return SyncClientHookAction::NoAction;
            }

            auto error_code = sync::ProtocolError(data.error_info->raw_error_code);
            if (error_code == sync::ProtocolError::initial_sync_not_completed) {
                return SyncClientHookAction::NoAction;
            }

            CHECK(error_code == sync::ProtocolError::schema_version_changed);
            // Cancel the async open task (the sync session closes too) the first time a schema migration is required.
            if (++schema_version_changed_count == 1) {
                task->cancel();
                promise.get_promise().emplace_value();
            }
            return SyncClientHookAction::NoAction;
        };

    {
        task = Realm::get_synchronized_realm(config);
        task->start([](ThreadSafeReference, std::exception_ptr) {
            FAIL();
        });
        future.get();
        task.reset();
        check_realm_schema(config.path, schema_v0, 0);
    }

    // Retry the migration.
    auto [realm, error] = async_open_realm(config);
    REQUIRE(realm);
    REQUIRE_FALSE(error);
    REQUIRE(schema_version_changed_count == 2);
    check_realm_schema(config.path, schema_v1, 1);
}

TEST_CASE("Migrate to new schema version with a schema subset", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    auto config = harness.make_test_file();

    {
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);
        wait_for_upload(*realm);
        check_realm_schema(config.path, schema_v0, 0);
    }

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, schema_v1, 1);

    config.schema_version = 1;
    auto schema_subset = schema_v1;
    // One of the columns in 'TopLevel' is not needed by the user.
    schema_subset[0].persisted_properties.pop_back();
    config.schema = schema_subset;
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();

    auto [realm, error] = async_open_realm(config);
    REQUIRE(realm);
    REQUIRE_FALSE(error);
    check_realm_schema(config.path, schema_v1, 1);
}

TEST_CASE("Client reset during schema migration", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    auto config = harness.make_test_file();

    {
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);
        wait_for_upload(*realm);
        check_realm_schema(config.path, schema_v0, 0);

        realm->sync_session()->shutdown_and_wait();

        realm->begin_transaction();
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", ObjectId::gen()},
                                        {"queryable_str_field", "foo"s},
                                        {"queryable_int_field", static_cast<int64_t>(15)},
                                        {"non_queryable_field2", "non queryable 11"s}}));
        // The server filters out this object because the schema version the client migrates to removes the queryable
        // field.
        Object::create(
            c, realm, "TopLevel3",
            std::any(AnyDict{{"_id", ObjectId::gen()}, {"queryable_int_field", static_cast<int64_t>(42)}}));
        realm->commit_transaction();
    }
    _impl::RealmCoordinator::assert_no_open_realms();

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, schema_v1, 1);

    config.schema_version = 1;
    config.schema = schema_v1;
    auto schema_version_changed_count = 0;
    bool once = false;
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();
    config.sync_config->client_resync_mode = ClientResyncMode::Recover;
    config.sync_config->on_sync_client_event_hook = [&harness, &schema_version_changed_count,
                                                     &once](std::weak_ptr<SyncSession> weak_session,
                                                            const SyncClientHookData& data) mutable {
        if (schema_version_changed_count == 1 && data.event == SyncClientHookEvent::DownloadMessageReceived &&
            !once) {
            once = true;
            return SyncClientHookAction::SuspendWithRetryableError;
        }
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
            if (++schema_version_changed_count == 1) {
                reset_utils::trigger_client_reset(harness.session().app_session(), *session);
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
    check_realm_schema(config.path, schema_v1, 1);

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
    auto config = harness.make_test_file();

    {
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);
        wait_for_upload(*realm);
        check_realm_schema(config.path, schema_v0, 0);

        realm->sync_session()->shutdown_and_wait();

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
    _impl::RealmCoordinator::assert_no_open_realms();

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, schema_v1, 1);
    auto schema_v2 = get_schema_v2();
    create_schema(app_session, schema_v2, 2);

    config.schema_version = 1;
    config.schema = schema_v1;
    auto schema_version_changed_count = 0;
    std::shared_ptr<AsyncOpenTask> task;
    auto [promise, future] = util::make_promise_future<void>();
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();
    config.sync_config->on_sync_client_event_hook =
        [&schema_version_changed_count, &task, promise = util::CopyablePromiseHolder<void>(std::move(promise))](
            std::weak_ptr<SyncSession>, const SyncClientHookData& data) mutable {
            if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
                return SyncClientHookAction::NoAction;
            }

            auto error_code = sync::ProtocolError(data.error_info->raw_error_code);
            if (error_code == sync::ProtocolError::initial_sync_not_completed) {
                return SyncClientHookAction::NoAction;
            }

            CHECK(error_code == sync::ProtocolError::schema_version_changed);
            // Cancel the async open task (the sync session closes too) the first time a schema migration is required.
            if (++schema_version_changed_count == 1) {
                task->cancel();
                promise.get_promise().emplace_value();
            }
            return SyncClientHookAction::NoAction;
        };

    {
        task = Realm::get_synchronized_realm(config);
        task->start([](ThreadSafeReference, std::exception_ptr) {
            FAIL();
        });
        future.get();
        task.reset();
        check_realm_schema(config.path, schema_v0, 0);
    }

    // Migrate to v2.
    config.schema_version = 2;
    config.schema = schema_v2;
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v2();
    auto [realm, error] = async_open_realm(config);
    REQUIRE(realm);
    REQUIRE_FALSE(error);
    REQUIRE(schema_version_changed_count == 2);
    check_realm_schema(config.path, schema_v2, 2);

    auto table = realm->read_group().get_table("class_TopLevel");
    CHECK(table->size() == 1);
    table = realm->read_group().get_table("class_TopLevel3");
    CHECK(table->is_empty());
}

TEST_CASE("Send schema version zero if no schema is used to open the realm",
          "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    auto config = harness.make_test_file();

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, schema_v1, 1);

    config.schema = {};
    config.schema_version = -1; // override the schema version set by SyncTestFile constructor
    auto [realm, error] = async_open_realm(config);
    REQUIRE(realm);
    REQUIRE_FALSE(error);
    // The schema is received from the server, but it is unversioned.
    check_realm_schema(config.path, schema_v0, ObjectStore::NotVersioned);
}

TEST_CASE("Allow resetting the schema version to zero after bad schema version error",
          "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    auto config = harness.make_test_file();
    config.schema_version = 42;

    SECTION("Fresh realm") {
    }

    SECTION("Existing realm") {
        DBOptions options;
        options.encryption_key = test_util::crypt_key();
        auto db = DB::create(sync::make_client_replication(), config.path, options);
        auto tr = db->start_write();
        ObjectStore::set_schema_version(*tr, config.schema_version);
        tr->commit();
        auto schema_version = ObjectStore::get_schema_version(*db->start_read());
        CHECK(schema_version == 42);
    }

    {
        auto&& [error_future, err_handler] = make_error_handler();
        config.sync_config->error_handler = err_handler;
        auto realm = Realm::get_shared_realm(config);
        auto error = error_future.get();
        REQUIRE(error.status == ErrorCodes::SyncSchemaMigrationError);
        REQUIRE_THAT(error.status.reason(),
                     Catch::Matchers::ContainsSubstring("Client provided invalid schema version"));
        check_realm_schema(config.path, schema_v0, 42);
    }

    config.schema_version = 0;
    config.sync_config->error_handler = nullptr;
    auto realm = Realm::get_shared_realm(config);
    wait_for_download(*realm);
    check_realm_schema(config.path, schema_v0, 0);
}

TEST_CASE("Client reset and schema migration", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    auto config = harness.make_test_file();

    {
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);
        wait_for_upload(*realm);
        check_realm_schema(config.path, schema_v0, 0);

        realm->sync_session()->shutdown_and_wait();

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

        // Trigger a client reset.
        reset_utils::trigger_client_reset(harness.session().app_session(), *realm->sync_session());
    }
    _impl::RealmCoordinator::assert_no_open_realms();

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, schema_v1, 1);

    config.schema_version = 1;
    config.schema = schema_v1;
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();
    config.sync_config->client_resync_mode = ClientResyncMode::Recover;
    config.sync_config->on_sync_client_event_hook = [](std::weak_ptr<SyncSession>,
                                                       const SyncClientHookData& data) mutable {
        if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
            return SyncClientHookAction::NoAction;
        }

        auto error_code = sync::ProtocolError(data.error_info->raw_error_code);
        if (error_code == sync::ProtocolError::initial_sync_not_completed) {
            return SyncClientHookAction::NoAction;
        }
        CHECK((error_code == sync::ProtocolError::schema_version_changed ||
               error_code == sync::ProtocolError::bad_client_file_ident));
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
    check_realm_schema(config.path, schema_v1, 1);

    auto table = realm->read_group().get_table("class_TopLevel");
    CHECK(table->size() == 1);
    table = realm->read_group().get_table("class_TopLevel3");
    CHECK(table->is_empty());
}

TEST_CASE("Multiple async open tasks trigger a schema migration", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    auto config = harness.make_test_file();
    config.sync_config->rerun_init_subscription_on_open = true;

    {
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);
        wait_for_upload(*realm);
        check_realm_schema(config.path, schema_v0, 0);

        realm->sync_session()->shutdown_and_wait();

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
        // The server filters out this object because the schema version the client migrates to removes the queryable
        // field.
        realm->begin_transaction();
        Object::create(
            c, realm, "TopLevel3",
            std::any(AnyDict{{"_id", ObjectId::gen()}, {"queryable_int_field", static_cast<int64_t>(42)}}));
        realm->commit_transaction();
        realm->close();
    }

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, schema_v1, 1);

    // Upgrade the schema version
    config.schema_version = 1;
    config.schema = schema_v1;
    config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v1();

    auto task1 = Realm::get_synchronized_realm(config);
    auto task2 = Realm::get_synchronized_realm(config);

    auto open_task1_pf = util::make_promise_future<SharedRealm>();
    auto open_task2_pf = util::make_promise_future<SharedRealm>();
    auto open_callback1 = [promise_holder = util::CopyablePromiseHolder(std::move(open_task1_pf.promise))](
                              ThreadSafeReference ref, std::exception_ptr err) mutable {
        REQUIRE_FALSE(err);
        auto realm = Realm::get_shared_realm(std::move(ref));
        REQUIRE(realm);
        promise_holder.get_promise().emplace_value(realm);
    };
    auto open_callback2 = [promise_holder = util::CopyablePromiseHolder(std::move(open_task2_pf.promise))](
                              ThreadSafeReference ref, std::exception_ptr err) mutable {
        REQUIRE_FALSE(err);
        auto realm = Realm::get_shared_realm(std::move(ref));
        REQUIRE(realm);
        promise_holder.get_promise().emplace_value(realm);
    };

    task1->start(open_callback1);
    task2->start(open_callback2);

    auto realm1 = open_task1_pf.future.get();
    auto realm2 = open_task2_pf.future.get();

    auto verify_realm = [&](SharedRealm realm) {
        check_realm_schema(config.path, schema_v1, 1);

        auto table = realm->read_group().get_table("class_TopLevel");
        CHECK(table->size() == 1);
        table = realm->read_group().get_table("class_TopLevel2");
        CHECK(!table);
        table = realm->read_group().get_table("class_TopLevel3");
        CHECK(table->is_empty());
    };

    verify_realm(realm1);
    verify_realm(realm2);
}

TEST_CASE("Upgrade schema version with no subscription initializer", "[sync][flx][flx schema migration][baas]") {
    auto schema_v0 = get_schema_v0();
    FLXSyncTestHarness harness("flx_sync_schema_migration",
                               {schema_v0, {"queryable_str_field", "queryable_int_field"}});
    auto config = harness.make_test_file();

    {
        config.sync_config->subscription_initializer = get_subscription_initializer_callback_for_schema_v0();
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);
        wait_for_upload(*realm);
        check_realm_schema(config.path, schema_v0, 0);

        realm->sync_session()->shutdown_and_wait();

        // Object to recover when upgrading the schema.
        realm->begin_transaction();
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", ObjectId::gen()},
                                        {"queryable_str_field", "biz"s},
                                        {"queryable_int_field", static_cast<int64_t>(15)},
                                        {"non_queryable_field2", "non queryable 33"s}}));
        realm->commit_transaction();
        realm->close();
    }

    const AppSession& app_session = harness.session().app_session();
    auto schema_v1 = get_schema_v1();
    create_schema(app_session, schema_v1, 1);

    {
        // Upgrade the schema version
        config.schema_version = 1;
        config.schema = schema_v1;
        config.sync_config->subscription_initializer = nullptr;
        auto [realm, error] = async_open_realm(config);
        REQUIRE(realm);
        REQUIRE_FALSE(error);
        check_realm_schema(config.path, schema_v1, 1);

        auto table = realm->read_group().get_table("class_TopLevel");
        CHECK(table->is_empty());
        table = realm->read_group().get_table("class_TopLevel2");
        CHECK(!table);
        table = realm->read_group().get_table("class_TopLevel3");
        CHECK(table->is_empty());
    }
}

} // namespace realm::app

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC
