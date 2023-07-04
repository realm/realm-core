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

#include <util/collection_fixtures.hpp>
#include <util/baas_admin_api.hpp>
#include <util/baas_test_utils.hpp>
#include <util/sync_test_utils.hpp>
#include <util/test_file.hpp>
#include <util/test_utils.hpp>

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/util/logger.hpp>

#include <external/json/json.hpp>
#include <external/mpark/variant.hpp>

#include <chrono>
#include <mutex>

using namespace realm;
using namespace realm::app;
using util::any_cast;
using util::Optional;


// MARK: - Call Function Tests

TEST_CASE("app: call function", "[sync][app][baas][function][new]") {
    TestAppSession session;
    auto app = session.app();

    bson::BsonArray toSum(5);
    std::iota(toSum.begin(), toSum.end(), static_cast<int64_t>(1));
    const auto checkFn = [](Optional<int64_t>&& sum, Optional<AppError>&& error) {
        REQUIRE(!error);
        CHECK(*sum == 15);
    };
    app->call_function<int64_t>("sumFunc", toSum, checkFn);
    app->call_function<int64_t>(app->sync_manager()->get_current_user(), "sumFunc", toSum, checkFn);
}

// MARK: - Push Notifications Tests

TEST_CASE("app: push notifications", "[sync][app][baas][notifications][new]") {
    TestAppSession session;
    auto app = session.app();
    std::shared_ptr<SyncUser> sync_user = app->current_user();

    SECTION("register") {
        bool processed;

        app->push_notification_client("gcm").register_device("hello", sync_user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
            processed = true;
        });

        CHECK(processed);
    }
    /*
        // FIXME: It seems this test fails when the two register_device calls are invoked too quickly,
        // The error returned will be 'Device not found' on the second register_device call.
        SECTION("register twice") {
            // registering the same device twice should not result in an error
            bool processed;

            app->push_notification_client("gcm").register_device("hello",
                                                                 sync_user,
                                                                 [&](Optional<AppError> error) {
                REQUIRE_FALSE(error);
            });

            app->push_notification_client("gcm").register_device("hello",
                                                                 sync_user,
                                                                 [&](Optional<AppError> error) {
                REQUIRE_FALSE(error);
                processed = true;
            });

            CHECK(processed);
        }
    */
    SECTION("deregister") {
        bool processed;

        app->push_notification_client("gcm").deregister_device(sync_user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("register with unavailable service") {
        bool processed;

        app->push_notification_client("gcm_blah").register_device("hello", sync_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->reason() == "service not found: 'gcm_blah'");
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("register with logged out user") {
        bool processed;

        app->log_out([=](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        app->push_notification_client("gcm").register_device("hello", sync_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            processed = true;
        });

        app->push_notification_client("gcm").register_device("hello", nullptr, [&](Optional<AppError> error) {
            REQUIRE(error);
            processed = true;
        });

        CHECK(processed);
    }
}

// MARK: - Sync Tests

TEST_CASE("app: mixed lists with object links", "[sync][pbs][app][baas][new]") {
    std::string base_url = get_base_url();
    const std::string valid_pk_name = "_id";
    REQUIRE(!base_url.empty());

    Schema schema{
        {"TopLevel",
         {
             {valid_pk_name, PropertyType::ObjectId, Property::IsPrimary{true}},
             {"mixed_array", PropertyType::Mixed | PropertyType::Array | PropertyType::Nullable},
         }},
        {"Target",
         {
             {valid_pk_name, PropertyType::ObjectId, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
         }},
    };

    auto server_app_config = minimal_app_config(base_url, "set_new_embedded_object", schema);
    auto app_session = create_app(server_app_config);
    auto partition = random_string(100);

    auto obj_id = ObjectId::gen();
    auto target_id = ObjectId::gen();
    auto mixed_list_values = AnyVector{
        Mixed{int64_t(1234)},
        Mixed{},
        Mixed{target_id},
    };
    {
        TestAppSession test_session(app_session, nullptr, DeleteApp{false});
        SyncTestFile config(test_session.app(), partition, schema);
        auto realm = Realm::get_shared_realm(config);

        CppContext c(realm);
        realm->begin_transaction();
        auto target_obj = Object::create(
            c, realm, "Target", std::any(AnyDict{{valid_pk_name, target_id}, {"value", static_cast<int64_t>(1234)}}));
        mixed_list_values.push_back(Mixed(target_obj.obj().get_link()));

        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{
                           {valid_pk_name, obj_id},
                           {"mixed_array", mixed_list_values},
                       }),
                       CreatePolicy::ForceCreate);
        realm->commit_transaction();
        CHECK(!wait_for_upload(*realm));
    }

    {
        TestAppSession test_session(app_session);
        SyncTestFile config(test_session.app(), partition, schema);
        auto realm = Realm::get_shared_realm(config);

        CHECK(!wait_for_download(*realm));
        CppContext c(realm);
        auto obj = Object::get_for_primary_key(c, realm, "TopLevel", std::any{obj_id});
        auto list = util::any_cast<List&&>(obj.get_property_value<std::any>(c, "mixed_array"));
        for (size_t idx = 0; idx < list.size(); ++idx) {
            Mixed mixed = list.get_any(idx);
            if (idx == 3) {
                CHECK(mixed.is_type(type_TypedLink));
                auto link = mixed.get<ObjLink>();
                auto link_table = realm->read_group().get_table(link.get_table_key());
                CHECK(link_table->get_name() == "class_Target");
                auto link_obj = link_table->get_object(link.get_obj_key());
                CHECK(link_obj.get_primary_key() == target_id);
            }
            else {
                CHECK(mixed == util::any_cast<Mixed>(mixed_list_values[idx]));
            }
        }
    }
}

TEST_CASE("app: roundtrip values", "[sync][pbs][app][baas][new]") {
    std::string base_url = get_base_url();
    const std::string valid_pk_name = "_id";
    REQUIRE(!base_url.empty());

    Schema schema{
        {"TopLevel",
         {
             {valid_pk_name, PropertyType::ObjectId, Property::IsPrimary{true}},
             {"decimal", PropertyType::Decimal | PropertyType::Nullable},
         }},
    };

    auto server_app_config = minimal_app_config(base_url, "roundtrip_values", schema);
    auto app_session = create_app(server_app_config);
    auto partition = random_string(100);

    Decimal128 large_significand = Decimal128(70) / Decimal128(1.09);
    auto obj_id = ObjectId::gen();
    {
        TestAppSession test_session(app_session, nullptr, DeleteApp{false});
        SyncTestFile config(test_session.app(), partition, schema);
        auto realm = Realm::get_shared_realm(config);

        CppContext c(realm);
        realm->begin_transaction();
        Object::create(c, realm, "TopLevel",
                       util::Any(AnyDict{
                           {valid_pk_name, obj_id},
                           {"decimal", large_significand},
                       }),
                       CreatePolicy::ForceCreate);
        realm->commit_transaction();
        CHECK(!wait_for_upload(*realm, std::chrono::seconds(600)));
    }

    {
        TestAppSession test_session(app_session);
        SyncTestFile config(test_session.app(), partition, schema);
        auto realm = Realm::get_shared_realm(config);

        CHECK(!wait_for_download(*realm));
        CppContext c(realm);
        auto obj = Object::get_for_primary_key(c, realm, "TopLevel", util::Any{obj_id});
        auto val = obj.get_column_value<Decimal128>("decimal");
        CHECK(val == large_significand);
    }
}

TEST_CASE("app: set new embedded object", "[sync][pbs][app][baas][embedded][new]") {
    std::string base_url = get_base_url();
    const std::string valid_pk_name = "_id";
    REQUIRE(!base_url.empty());

    Schema schema{
        {"TopLevel",
         {
             {valid_pk_name, PropertyType::ObjectId, Property::IsPrimary{true}},
             {"array_of_objs", PropertyType::Object | PropertyType::Array, "TopLevel_array_of_objs"},
             {"embedded_obj", PropertyType::Object | PropertyType::Nullable, "TopLevel_embedded_obj"},
             {"embedded_dict", PropertyType::Object | PropertyType::Dictionary | PropertyType::Nullable,
              "TopLevel_embedded_dict"},
         }},
        {"TopLevel_array_of_objs",
         ObjectSchema::ObjectType::Embedded,
         {
             {"array", PropertyType::Int | PropertyType::Array},
         }},
        {"TopLevel_embedded_obj",
         ObjectSchema::ObjectType::Embedded,
         {
             {"array", PropertyType::Int | PropertyType::Array},
         }},
        {"TopLevel_embedded_dict",
         ObjectSchema::ObjectType::Embedded,
         {
             {"array", PropertyType::Int | PropertyType::Array},
         }},
    };

    auto server_app_config = minimal_app_config(base_url, "set_new_embedded_object", schema);
    TestAppSession test_session(create_app(server_app_config));
    auto partition = random_string(100);

    auto array_of_objs_id = ObjectId::gen();
    auto embedded_obj_id = ObjectId::gen();
    auto dict_obj_id = ObjectId::gen();

    {
        SyncTestFile config(test_session.app(), partition, schema);
        auto realm = Realm::get_shared_realm(config);

        CppContext c(realm);
        realm->begin_transaction();
        auto array_of_objs =
            Object::create(c, realm, "TopLevel",
                           std::any(AnyDict{
                               {valid_pk_name, array_of_objs_id},
                               {"array_of_objs", AnyVector{AnyDict{{"array", AnyVector{INT64_C(1), INT64_C(2)}}}}},
                           }),
                           CreatePolicy::ForceCreate);

        auto embedded_obj =
            Object::create(c, realm, "TopLevel",
                           std::any(AnyDict{
                               {valid_pk_name, embedded_obj_id},
                               {"embedded_obj", AnyDict{{"array", AnyVector{INT64_C(1), INT64_C(2)}}}},
                           }),
                           CreatePolicy::ForceCreate);

        auto dict_obj = Object::create(
            c, realm, "TopLevel",
            std::any(AnyDict{
                {valid_pk_name, dict_obj_id},
                {"embedded_dict", AnyDict{{"foo", AnyDict{{"array", AnyVector{INT64_C(1), INT64_C(2)}}}}}},
            }),
            CreatePolicy::ForceCreate);

        realm->commit_transaction();
        {
            realm->begin_transaction();
            embedded_obj.set_property_value(c, "embedded_obj",
                                            std::any(AnyDict{{
                                                "array",
                                                AnyVector{INT64_C(3), INT64_C(4)},
                                            }}),
                                            CreatePolicy::UpdateAll);
            realm->commit_transaction();
        }

        {
            realm->begin_transaction();
            List array(array_of_objs, array_of_objs.get_object_schema().property_for_name("array_of_objs"));
            CppContext c2(realm, &array.get_object_schema());
            array.set(c2, 0, std::any{AnyDict{{"array", AnyVector{INT64_C(5), INT64_C(6)}}}});
            realm->commit_transaction();
        }

        {
            realm->begin_transaction();
            object_store::Dictionary dict(dict_obj, dict_obj.get_object_schema().property_for_name("embedded_dict"));
            CppContext c2(realm, &dict.get_object_schema());
            dict.insert(c2, "foo", std::any{AnyDict{{"array", AnyVector{INT64_C(7), INT64_C(8)}}}});
            realm->commit_transaction();
        }
        CHECK(!wait_for_upload(*realm));
    }

    {
        SyncTestFile config(test_session.app(), partition, schema);
        auto realm = Realm::get_shared_realm(config);

        CHECK(!wait_for_download(*realm));
        CppContext c(realm);
        {
            auto obj = Object::get_for_primary_key(c, realm, "TopLevel", std::any{embedded_obj_id});
            auto embedded_obj = util::any_cast<Object&&>(obj.get_property_value<std::any>(c, "embedded_obj"));
            auto array_list = util::any_cast<List&&>(embedded_obj.get_property_value<std::any>(c, "array"));
            CHECK(array_list.size() == 2);
            CHECK(array_list.get<int64_t>(0) == int64_t(3));
            CHECK(array_list.get<int64_t>(1) == int64_t(4));
        }

        {
            auto obj = Object::get_for_primary_key(c, realm, "TopLevel", std::any{array_of_objs_id});
            auto embedded_list = util::any_cast<List&&>(obj.get_property_value<std::any>(c, "array_of_objs"));
            CppContext c2(realm, &embedded_list.get_object_schema());
            auto embedded_array_obj = util::any_cast<Object&&>(embedded_list.get(c2, 0));
            auto array_list = util::any_cast<List&&>(embedded_array_obj.get_property_value<std::any>(c2, "array"));
            CHECK(array_list.size() == 2);
            CHECK(array_list.get<int64_t>(0) == int64_t(5));
            CHECK(array_list.get<int64_t>(1) == int64_t(6));
        }

        {
            auto obj = Object::get_for_primary_key(c, realm, "TopLevel", std::any{dict_obj_id});
            object_store::Dictionary dict(obj, obj.get_object_schema().property_for_name("embedded_dict"));
            CppContext c2(realm, &dict.get_object_schema());
            auto embedded_obj = util::any_cast<Object&&>(dict.get(c2, "foo"));
            auto array_list = util::any_cast<List&&>(embedded_obj.get_property_value<std::any>(c2, "array"));
            CHECK(array_list.size() == 2);
            CHECK(array_list.get<int64_t>(0) == int64_t(7));
            CHECK(array_list.get<int64_t>(1) == int64_t(8));
        }
    }
}

TEST_CASE("app: sync integration", "[sync][pbs][app][baas][new]") {
    const auto schema = default_app_config("").schema;

    auto get_dogs = [](SharedRealm r) -> Results {
        wait_for_upload(*r, std::chrono::seconds(10));
        wait_for_download(*r, std::chrono::seconds(10));
        return Results(r, r->read_group().get_table("class_Dog"));
    };

    auto create_one_dog = [](SharedRealm r) {
        r->begin_transaction();
        CppContext c;
        Object::create(c, r, "Dog",
                       std::any(AnyDict{{"_id", std::any(ObjectId::gen())},
                                        {"breed", std::string("bulldog")},
                                        {"name", std::string("fido")}}),
                       CreatePolicy::ForceCreate);
        r->commit_transaction();
    };

    TestAppSession session;
    auto app = session.app();
    const auto partition = random_string(100);

    // MARK: Add Objects -
    SECTION("Add Objects") {
        {
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);

            REQUIRE(get_dogs(r).size() == 0);
            create_one_dog(r);
            REQUIRE(get_dogs(r).size() == 1);
        }

        {
            create_user_and_log_in(app);
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);
            Results dogs = get_dogs(r);
            REQUIRE(dogs.size() == 1);
            REQUIRE(dogs.get(0).get<String>("breed") == "bulldog");
            REQUIRE(dogs.get(0).get<String>("name") == "fido");
        }
    }

    SECTION("MemOnly durability") {
        {
            SyncTestFile config(app, partition, schema);
            config.in_memory = true;
            config.encryption_key = std::vector<char>();

            REQUIRE(config.options().durability == DBOptions::Durability::MemOnly);
            auto r = Realm::get_shared_realm(config);

            REQUIRE(get_dogs(r).size() == 0);
            create_one_dog(r);
            REQUIRE(get_dogs(r).size() == 1);
        }

        {
            create_user_and_log_in(app);
            SyncTestFile config(app, partition, schema);
            config.in_memory = true;
            config.encryption_key = std::vector<char>();
            auto r = Realm::get_shared_realm(config);
            Results dogs = get_dogs(r);
            REQUIRE(dogs.size() == 1);
            REQUIRE(dogs.get(0).get<String>("breed") == "bulldog");
            REQUIRE(dogs.get(0).get<String>("name") == "fido");
        }
    }

    {
        std::unique_ptr<realm::AppSession> app_session;
        std::string base_file_path = util::make_temp_dir() + random_string(10);
        auto redir_transport = std::make_shared<HookedTransport>();
        AutoVerifiedEmailCredentials creds;

        auto app_config = get_config(redir_transport, session.app_session());
        set_app_config_defaults(app_config, redir_transport);

        util::try_make_dir(base_file_path);
        SyncClientConfig sc_config;
        sc_config.base_file_path = base_file_path;
        sc_config.metadata_mode = realm::SyncManager::MetadataMode::NoEncryption;

        // initialize app and sync client
        auto redir_app = app::App::get_uncached_app(app_config, sc_config);

        SECTION("Test server in maintenance") {
            redir_transport->request_hook = [&](const Request&) {
                nlohmann::json maintenance_error = {{"error_code", "MaintenanceInProgress"},
                                                    {"error", "This service is currently undergoing maintenance"},
                                                    {"link", "https://link.to/server_logs"}};
                redir_transport->simulated_response = {
                    500, 0, {{"Content-Type", "application/json"}}, maintenance_error.dump()};
            };

            redir_app->log_in_with_credentials(
                realm::app::AppCredentials::username_password(creds.email, creds.password),
                [&](std::shared_ptr<realm::SyncUser> user, util::Optional<app::AppError> error) {
                    REQUIRE(!user);
                    REQUIRE(error);
                    REQUIRE(error->is_service_error());
                    REQUIRE(error->code() == ErrorCodes::MaintenanceInProgress);
                    REQUIRE(error->reason() == "This service is currently undergoing maintenance");
                    REQUIRE(error->link_to_server_logs == "https://link.to/server_logs");
                    REQUIRE(*error->additional_status_code == 500);
                });
        }
    }

    SECTION("large write transactions which would be too large if batched") {
        SyncTestFile config(app, partition, schema);

        std::mutex mutex;
        bool done = false;
        auto r = Realm::get_shared_realm(config);
        r->sync_session()->pause();

        // Create 26 MB worth of dogs in 26 transactions, which should work but
        // will result in an error from the server if the changesets are batched
        // for upload.
        CppContext c;
        for (auto i = 'a'; i < 'z'; ++i) {
            r->begin_transaction();
            Object::create(c, r, "Dog",
                           std::any(AnyDict{{"_id", std::any(ObjectId::gen())},
                                            {"breed", std::string("bulldog")},
                                            {"name", random_string(1024 * 1024)}}),
                           CreatePolicy::ForceCreate);
            r->commit_transaction();
        }
        r->sync_session()->wait_for_upload_completion([&](Status ec) {
            std::lock_guard lk(mutex);
            REQUIRE(!ec.get_std_error_code());
            done = true;
        });
        r->sync_session()->resume();

        // If we haven't gotten an error in more than 5 minutes, then something has gone wrong
        // and we should fail the test.
        timed_wait_for(
            [&] {
                std::lock_guard lk(mutex);
                return done;
            },
            std::chrono::minutes(5));
    }

    SECTION("too large sync message error handling") {
        SyncTestFile config(app, partition, schema);

        auto pf = util::make_promise_future<SyncError>();
        config.sync_config->error_handler =
            [sp = util::CopyablePromiseHolder(std::move(pf.promise))](auto, SyncError error) mutable {
                sp.get_promise().emplace_value(std::move(error));
            };
        auto r = Realm::get_shared_realm(config);

        // Create 26 MB worth of dogs in a single transaction - this should all get put into one changeset
        // and get uploaded at once, which for now is an error on the server.
        r->begin_transaction();
        CppContext c;
        for (auto i = 'a'; i < 'z'; ++i) {
            Object::create(c, r, "Dog",
                           std::any(AnyDict{{"_id", std::any(ObjectId::gen())},
                                            {"breed", std::string("bulldog")},
                                            {"name", random_string(1024 * 1024)}}),
                           CreatePolicy::ForceCreate);
        }
        r->commit_transaction();

        auto error = wait_for_future(std::move(pf.future), std::chrono::minutes(5)).get();
        REQUIRE(error.get_system_error() == make_error_code(sync::ProtocolError::limits_exceeded));
        REQUIRE_THAT(std::string(error.reason()),
                     Catch::Matchers::ContainsSubstring("Sync websocket closed because the server received a message "
                                                        "that was too large: read limited at 16777217 bytes"));
        REQUIRE(error.is_client_reset_requested());
        REQUIRE(error.server_requests_action == sync::ProtocolErrorInfo::Action::ClientReset);
    }

    SECTION("freezing realm does not resume session") {
        SyncTestFile config(app, partition, schema);
        auto realm = Realm::get_shared_realm(config);
        wait_for_download(*realm);

        auto state = realm->sync_session()->state();
        REQUIRE(state == SyncSession::State::Active);

        realm->sync_session()->pause();
        state = realm->sync_session()->state();
        REQUIRE(state == SyncSession::State::Paused);

        realm->read_group();

        {
            auto frozen = realm->freeze();
            REQUIRE(realm->sync_session() == realm->sync_session());
            REQUIRE(realm->sync_session()->state() == SyncSession::State::Paused);
        }

        {
            auto frozen = Realm::get_frozen_realm(config, realm->read_transaction_version());
            REQUIRE(realm->sync_session() == realm->sync_session());
            REQUIRE(realm->sync_session()->state() == SyncSession::State::Paused);
        }
    }

    SECTION("pausing a session does not hold the DB open") {
        SyncTestFile config(app, partition, schema);
        DBRef dbref;
        std::shared_ptr<SyncSession> sync_sess_ext_ref;
        {
            auto realm = Realm::get_shared_realm(config);
            wait_for_download(*realm);

            auto state = realm->sync_session()->state();
            REQUIRE(state == SyncSession::State::Active);

            sync_sess_ext_ref = realm->sync_session()->external_reference();
            dbref = TestHelper::get_db(*realm);
            // One ref each for the
            // - RealmCoordinator
            // - SyncSession
            // - SessionWrapper
            // - local dbref
            REQUIRE(dbref.use_count() >= 4);

            realm->sync_session()->pause();
            state = realm->sync_session()->state();
            REQUIRE(state == SyncSession::State::Paused);
        }

        // Closing the realm should leave one ref for the SyncSession and one for the local dbref.
        REQUIRE_THAT(
            [&] {
                return dbref.use_count() < 4;
            },
            ReturnsTrueWithinTimeLimit{});

        // Releasing the external reference should leave one ref (the local dbref) only.
        sync_sess_ext_ref.reset();
        REQUIRE_THAT(
            [&] {
                return dbref.use_count() == 1;
            },
            ReturnsTrueWithinTimeLimit{});
    }

    SECTION("validation") {
        SyncTestFile config(app, partition, schema);

        SECTION("invalid partition error handling") {
            config.sync_config->partition_value = "not a bson serialized string";
            std::atomic<bool> error_did_occur = false;
            config.sync_config->error_handler = [&error_did_occur](std::shared_ptr<SyncSession>, SyncError error) {
                CHECK(error.reason().find("Illegal Realm path (BIND): serialized partition 'not a bson serialized "
                                          "string' is invalid") != std::string::npos);
                error_did_occur.store(true);
            };
            auto r = Realm::get_shared_realm(config);
            auto session = app->current_user()->session_for_on_disk_path(r->config().path);
            timed_wait_for([&] {
                return error_did_occur.load();
            });
            REQUIRE(error_did_occur.load());
        }

        SECTION("invalid pk schema error handling") {
            const std::string invalid_pk_name = "my_primary_key";
            auto it = config.schema->find("Dog");
            REQUIRE(it != config.schema->end());
            REQUIRE(it->primary_key_property());
            REQUIRE(it->primary_key_property()->name == "_id");
            it->primary_key_property()->name = invalid_pk_name;
            it->primary_key = invalid_pk_name;
            REQUIRE_THROWS_CONTAINING(Realm::get_shared_realm(config),
                                      "The primary key property on a synchronized Realm must be named '_id' but "
                                      "found 'my_primary_key' for type 'Dog'");
        }

        SECTION("missing pk schema error handling") {
            auto it = config.schema->find("Dog");
            REQUIRE(it != config.schema->end());
            REQUIRE(it->primary_key_property());
            it->primary_key_property()->is_primary = false;
            it->primary_key = "";
            REQUIRE(!it->primary_key_property());
            REQUIRE_THROWS_CONTAINING(Realm::get_shared_realm(config),
                                      "There must be a primary key property named '_id' on a synchronized "
                                      "Realm but none was found for type 'Dog'");
        }
    }
}

namespace cf = realm::collection_fixtures;
TEMPLATE_TEST_CASE("app: collections of links integration", "[sync][pbs][app][baas][collections][new]",
                   cf::ListOfObjects, cf::ListOfMixedLinks, cf::SetOfObjects, cf::SetOfMixedLinks,
                   cf::DictionaryOfObjects, cf::DictionaryOfMixedLinks)
{
    std::string base_url = get_base_url();
    const std::string valid_pk_name = "_id";
    REQUIRE(!base_url.empty());
    const auto partition = random_string(100);
    TestType test_type("collection", "dest");
    Schema schema = {{"source",
                      {{valid_pk_name, PropertyType::Int | PropertyType::Nullable, true},
                       {"realm_id", PropertyType::String | PropertyType::Nullable},
                       test_type.property()}},
                     {"dest",
                      {
                          {valid_pk_name, PropertyType::Int | PropertyType::Nullable, true},
                          {"realm_id", PropertyType::String | PropertyType::Nullable},
                      }}};
    auto server_app_config = minimal_app_config(base_url, "collections_of_links", schema);
    TestAppSession test_session(create_app(server_app_config));

    auto wait_for_num_objects_to_equal = [](realm::SharedRealm r, const std::string& table_name, size_t count) {
        timed_sleeping_wait_for([&]() -> bool {
            r->refresh();
            TableRef dest = r->read_group().get_table(table_name);
            size_t cur_count = dest->size();
            return cur_count == count;
        });
    };
    auto wait_for_num_outgoing_links_to_equal = [&](realm::SharedRealm r, Obj obj, size_t count) {
        timed_sleeping_wait_for([&]() -> bool {
            r->refresh();
            return test_type.size_of_collection(obj) == count;
        });
    };

    CppContext c;
    auto create_one_source_object = [&](realm::SharedRealm r, int64_t val, std::vector<ObjLink> links = {}) {
        r->begin_transaction();
        auto object = Object::create(
            c, r, "source",
            std::any(realm::AnyDict{{valid_pk_name, std::any(val)}, {"realm_id", std::string(partition)}}),
            CreatePolicy::ForceCreate);

        for (auto link : links) {
            test_type.add_link(object.obj(), link);
        }
        r->commit_transaction();
    };

    auto create_one_dest_object = [&](realm::SharedRealm r, int64_t val) -> ObjLink {
        r->begin_transaction();
        auto obj = Object::create(
            c, r, "dest",
            std::any(realm::AnyDict{{valid_pk_name, std::any(val)}, {"realm_id", std::string(partition)}}),
            CreatePolicy::ForceCreate);
        r->commit_transaction();
        return ObjLink{obj.obj().get_table()->get_key(), obj.obj().get_key()};
    };

    auto require_links_to_match_ids = [&](std::vector<Obj> links, std::vector<int64_t> expected) {
        std::vector<int64_t> actual;
        for (auto obj : links) {
            actual.push_back(obj.get<Int>(valid_pk_name));
        }
        std::sort(actual.begin(), actual.end());
        std::sort(expected.begin(), expected.end());
        REQUIRE(actual == expected);
    };

    SECTION("integration testing") {
        auto app = test_session.app();
        SyncTestFile config1(app, partition, schema); // uses the current user created above
        auto r1 = realm::Realm::get_shared_realm(config1);
        Results r1_source_objs = realm::Results(r1, r1->read_group().get_table("class_source"));

        create_user_and_log_in(app);
        SyncTestFile config2(app, partition, schema); // uses the user created above
        auto r2 = realm::Realm::get_shared_realm(config2);
        Results r2_source_objs = realm::Results(r2, r2->read_group().get_table("class_source"));

        constexpr int64_t source_pk = 0;
        constexpr int64_t dest_pk_1 = 1;
        constexpr int64_t dest_pk_2 = 2;
        constexpr int64_t dest_pk_3 = 3;
        { // add a container collection with three valid links
            REQUIRE(r1_source_objs.size() == 0);
            ObjLink dest1 = create_one_dest_object(r1, dest_pk_1);
            ObjLink dest2 = create_one_dest_object(r1, dest_pk_2);
            ObjLink dest3 = create_one_dest_object(r1, dest_pk_3);
            create_one_source_object(r1, source_pk, {dest1, dest2, dest3});
            REQUIRE(r1_source_objs.size() == 1);
            REQUIRE(r1_source_objs.get(0).get<Int>(valid_pk_name) == source_pk);
            REQUIRE(r1_source_objs.get(0).get<String>("realm_id") == partition);
            require_links_to_match_ids(test_type.get_links(r1_source_objs.get(0)), {dest_pk_1, dest_pk_2, dest_pk_3});
        }

        size_t expected_coll_size = 3;
        std::vector<int64_t> remaining_dest_object_ids;
        { // erase one of the destination objects
            wait_for_num_objects_to_equal(r2, "class_source", 1);
            wait_for_num_objects_to_equal(r2, "class_dest", 3);
            REQUIRE(r2_source_objs.size() == 1);
            REQUIRE(r2_source_objs.get(0).get<Int>(valid_pk_name) == source_pk);
            REQUIRE(test_type.size_of_collection(r2_source_objs.get(0)) == 3);
            auto linked_objects = test_type.get_links(r2_source_objs.get(0));
            require_links_to_match_ids(linked_objects, {dest_pk_1, dest_pk_2, dest_pk_3});
            r2->begin_transaction();
            linked_objects[0].remove();
            r2->commit_transaction();
            remaining_dest_object_ids = {linked_objects[1].template get<Int>(valid_pk_name),
                                         linked_objects[2].template get<Int>(valid_pk_name)};
            expected_coll_size = test_type.will_erase_removed_object_links() ? 2 : 3;
            REQUIRE(test_type.size_of_collection(r2_source_objs.get(0)) == expected_coll_size);
        }

        { // remove a link from the collection
            wait_for_num_objects_to_equal(r1, "class_dest", 2);
            REQUIRE(r1_source_objs.size() == 1);
            REQUIRE(test_type.size_of_collection(r1_source_objs.get(0)) == expected_coll_size);
            auto linked_objects = test_type.get_links(r1_source_objs.get(0));
            require_links_to_match_ids(linked_objects, remaining_dest_object_ids);
            r1->begin_transaction();
            test_type.remove_link(r1_source_objs.get(0),
                                  ObjLink{linked_objects[0].get_table()->get_key(), linked_objects[0].get_key()});
            r1->commit_transaction();
            --expected_coll_size;
            remaining_dest_object_ids = {linked_objects[1].template get<Int>(valid_pk_name)};
            REQUIRE(test_type.size_of_collection(r1_source_objs.get(0)) == expected_coll_size);
        }

        { // clear the collection
            REQUIRE(r2_source_objs.size() == 1);
            REQUIRE(r2_source_objs.get(0).get<Int>(valid_pk_name) == source_pk);
            wait_for_num_outgoing_links_to_equal(r2, r2_source_objs.get(0), expected_coll_size);
            auto linked_objects = test_type.get_links(r2_source_objs.get(0));
            require_links_to_match_ids(linked_objects, remaining_dest_object_ids);
            r2->begin_transaction();
            test_type.clear_collection(r2_source_objs.get(0));
            r2->commit_transaction();
            expected_coll_size = 0;
            REQUIRE(test_type.size_of_collection(r2_source_objs.get(0)) == expected_coll_size);
        }

        { // expect an empty collection
            REQUIRE(r1_source_objs.size() == 1);
            wait_for_num_outgoing_links_to_equal(r1, r1_source_objs.get(0), expected_coll_size);
        }
    }
}

TEMPLATE_TEST_CASE("app: partition types", "[sync][pbs][app][baas][partition][new]", cf::Int, cf::String, cf::OID,
                   cf::UUID, cf::BoxedOptional<cf::Int>, cf::UnboxedOptional<cf::String>, cf::BoxedOptional<cf::OID>,
                   cf::BoxedOptional<cf::UUID>)
{
    std::string base_url = get_base_url();
    const std::string valid_pk_name = "_id";
    const std::string partition_key_col_name = "partition_key_prop";
    const std::string table_name = "class_partition_test_type";
    REQUIRE(!base_url.empty());
    auto partition_property = Property(partition_key_col_name, TestType::property_type);
    Schema schema = {{Group::table_name_to_class_name(table_name),
                      {
                          {valid_pk_name, PropertyType::Int, true},
                          partition_property,
                      }}};
    auto server_app_config = minimal_app_config(base_url, "partition_types_app_name", schema);
    server_app_config.partition_key = partition_property;
    TestAppSession test_session(create_app(server_app_config));
    auto app = test_session.app();

    auto wait_for_num_objects_to_equal = [](realm::SharedRealm r, const std::string& table_name, size_t count) {
        timed_sleeping_wait_for([&]() -> bool {
            r->refresh();
            TableRef dest = r->read_group().get_table(table_name);
            size_t cur_count = dest->size();
            return cur_count == count;
        });
    };
    using T = typename TestType::Type;
    CppContext c;
    auto create_object = [&](realm::SharedRealm r, int64_t val, std::any partition) {
        r->begin_transaction();
        auto object = Object::create(
            c, r, Group::table_name_to_class_name(table_name),
            std::any(realm::AnyDict{{valid_pk_name, std::any(val)}, {partition_key_col_name, partition}}),
            CreatePolicy::ForceCreate);
        r->commit_transaction();
    };

    auto get_bson = [](T val) -> bson::Bson {
        if constexpr (std::is_same_v<T, StringData>) {
            return val.is_null() ? bson::Bson(util::none) : bson::Bson(val);
        }
        else if constexpr (TestType::is_optional) {
            return val ? bson::Bson(*val) : bson::Bson(util::none);
        }
        else {
            return bson::Bson(val);
        }
    };

    SECTION("can round trip an object") {
        auto values = TestType::values();
        auto user1 = app->current_user();
        create_user_and_log_in(app);
        auto user2 = app->current_user();
        REQUIRE(user1);
        REQUIRE(user2);
        REQUIRE(user1 != user2);
        for (T partition_value : values) {
            SyncTestFile config1(user1, get_bson(partition_value), schema); // uses the current user created above
            auto r1 = realm::Realm::get_shared_realm(config1);
            Results r1_source_objs = realm::Results(r1, r1->read_group().get_table(table_name));

            SyncTestFile config2(user2, get_bson(partition_value), schema); // uses the user created above
            auto r2 = realm::Realm::get_shared_realm(config2);
            Results r2_source_objs = realm::Results(r2, r2->read_group().get_table(table_name));

            const int64_t pk_value = random_int();
            {
                REQUIRE(r1_source_objs.size() == 0);
                create_object(r1, pk_value, TestType::to_any(partition_value));
                REQUIRE(r1_source_objs.size() == 1);
                REQUIRE(r1_source_objs.get(0).get<T>(partition_key_col_name) == partition_value);
                REQUIRE(r1_source_objs.get(0).get<Int>(valid_pk_name) == pk_value);
            }
            {
                wait_for_num_objects_to_equal(r2, table_name, 1);
                REQUIRE(r2_source_objs.size() == 1);
                REQUIRE(r2_source_objs.size() == 1);
                REQUIRE(r2_source_objs.get(0).get<T>(partition_key_col_name) == partition_value);
                REQUIRE(r2_source_objs.get(0).get<Int>(valid_pk_name) == pk_value);
            }
        }
    }
}

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC
