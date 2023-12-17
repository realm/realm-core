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

#include "collection_fixtures.hpp"
#include "util/sync/baas_admin_api.hpp"
#include "util/sync/sync_test_utils.hpp"
#include "util/unit_test_transport.hpp"

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/app_utils.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/mongo_database.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/object-store/util/uuid.hpp>
#include <realm/sync/network/default_socket.hpp>
#include <realm/sync/network/websocket.hpp>
#include <realm/sync/noinst/server/access_token.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/overload.hpp>
#include <realm/util/platform_info.hpp>
#include <realm/util/uri.hpp>

#include <catch2/catch_all.hpp>
#include <external/json/json.hpp>
#include <external/mpark/variant.hpp>

#include <condition_variable>
#include <future>
#include <iostream>
#include <list>
#include <mutex>

using namespace realm;
using namespace realm::app;
using util::any_cast;
using util::Optional;

using namespace std::string_view_literals;
using namespace std::literals::string_literals;

#if REALM_ENABLE_AUTH_TESTS && !defined(REALM_MONGODB_ENDPOINT)
static_assert(false, "These tests require a MongoDB instance")
#endif

    namespace realm
{
    class TestHelper {
    public:
        static DBRef get_db(Realm& realm)
        {
            return Realm::Internal::get_db(realm);
        }
    };
} // namespace realm

// MARK: - Sync Tests

#if REALM_ENABLE_AUTH_TESTS

TEST_CASE("app: mixed lists with object links", "[sync][pbs][app][links][baas]") {
    const std::string valid_pk_name = "_id";

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

    auto server_app_config = minimal_app_config("set_new_embedded_object", schema);
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
        TestAppSession test_session({app_session, nullptr, DeleteApp{false}});
        SyncTestFile config(test_session.app(), partition, schema);
        auto realm = Realm::get_shared_realm(config);

        CppContext c(realm);
        realm->begin_transaction();
        auto target_obj = Object::create(
            c, realm, "Target", std::any(AnyDict{{valid_pk_name, target_id}, {"value", static_cast<int64_t>(1234)}}));
        mixed_list_values.push_back(Mixed(target_obj.get_obj().get_link()));

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

TEST_CASE("app: roundtrip values", "[sync][pbs][app][baas]") {
    const std::string valid_pk_name = "_id";

    Schema schema{
        {"TopLevel",
         {
             {valid_pk_name, PropertyType::ObjectId, Property::IsPrimary{true}},
             {"decimal", PropertyType::Decimal | PropertyType::Nullable},
         }},
    };

    auto server_app_config = minimal_app_config("roundtrip_values", schema);
    auto app_session = create_app(server_app_config);
    auto partition = random_string(100);

    Decimal128 large_significand = Decimal128(70) / Decimal128(1.09);
    auto obj_id = ObjectId::gen();
    {
        TestAppSession test_session({app_session, nullptr, DeleteApp{false}});
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

TEST_CASE("app: upgrade from local to synced realm", "[sync][pbs][app][upgrade][baas]") {
    const std::string valid_pk_name = "_id";

    Schema schema{
        {"origin",
         {{valid_pk_name, PropertyType::Int, Property::IsPrimary{true}},
          {"link", PropertyType::Object | PropertyType::Nullable, "target"},
          {"embedded_link", PropertyType::Object | PropertyType::Nullable, "embedded"}}},
        {"target",
         {{valid_pk_name, PropertyType::String, Property::IsPrimary{true}},
          {"value", PropertyType::Int},
          {"name", PropertyType::String}}},
        {"other_origin",
         {{valid_pk_name, PropertyType::ObjectId, Property::IsPrimary{true}},
          {"array", PropertyType::Array | PropertyType::Object, "other_target"}}},
        {"other_target",
         {{valid_pk_name, PropertyType::UUID, Property::IsPrimary{true}}, {"value", PropertyType::Int}}},
        {"embedded", ObjectSchema::ObjectType::Embedded, {{"name", PropertyType::String | PropertyType::Nullable}}},
    };

    /*             Create local realm             */
    TestFile local_config;
    local_config.schema = schema;
    auto local_realm = Realm::get_shared_realm(local_config);
    {
        auto origin = local_realm->read_group().get_table("class_origin");
        auto target = local_realm->read_group().get_table("class_target");
        auto other_origin = local_realm->read_group().get_table("class_other_origin");
        auto other_target = local_realm->read_group().get_table("class_other_target");

        local_realm->begin_transaction();
        auto o = target->create_object_with_primary_key("Foo").set("name", "Egon");
        // 'embedded_link' property is null.
        origin->create_object_with_primary_key(47).set("link", o.get_key());
        // 'embedded_link' property is not null.
        auto obj = origin->create_object_with_primary_key(42);
        auto col_key = origin->get_column_key("embedded_link");
        obj.create_and_set_linked_object(col_key);
        other_target->create_object_with_primary_key(UUID("3b241101-e2bb-4255-8caf-4136c566a961"));
        other_origin->create_object_with_primary_key(ObjectId::gen());
        local_realm->commit_transaction();
    }

    /* Create a synced realm and upload some data */
    auto server_app_config = minimal_app_config("upgrade_from_local", schema);
    TestAppSession test_session(create_app(server_app_config));
    auto partition = random_string(100);
    auto user1 = test_session.app()->current_user();
    SyncTestFile config1(user1, partition, schema);

    auto r1 = Realm::get_shared_realm(config1);

    auto origin = r1->read_group().get_table("class_origin");
    auto target = r1->read_group().get_table("class_target");
    auto other_origin = r1->read_group().get_table("class_other_origin");
    auto other_target = r1->read_group().get_table("class_other_target");

    r1->begin_transaction();
    auto o = target->create_object_with_primary_key("Baa").set("name", "Børge");
    origin->create_object_with_primary_key(47).set("link", o.get_key());
    other_target->create_object_with_primary_key(UUID("01234567-89ab-cdef-edcb-a98765432101"));
    other_origin->create_object_with_primary_key(ObjectId::gen());
    r1->commit_transaction();
    CHECK(!wait_for_upload(*r1));

    /* Copy local realm data over in a synced one*/
    create_user_and_log_in(test_session.app());
    auto user2 = test_session.app()->current_user();
    REQUIRE(user1 != user2);

    SyncTestFile config2(user1, partition, schema);

    SharedRealm r2;
    SECTION("Copy before connecting to server") {
        local_realm->convert(config2);
        r2 = Realm::get_shared_realm(config2);
    }

    SECTION("Open synced realm first") {
        r2 = Realm::get_shared_realm(config2);
        CHECK(!wait_for_download(*r2));
        local_realm->convert(config2);
        CHECK(!wait_for_upload(*r2));
    }

    CHECK(!wait_for_download(*r2));
    advance_and_notify(*r2);
    Group& g = r2->read_group();
    // g.to_json(std::cout);
    REQUIRE(g.get_table("class_origin")->size() == 2);
    REQUIRE(g.get_table("class_target")->size() == 2);
    REQUIRE(g.get_table("class_other_origin")->size() == 2);
    REQUIRE(g.get_table("class_other_target")->size() == 2);

    CHECK(!wait_for_upload(*r2));
    CHECK(!wait_for_download(*r1));
    advance_and_notify(*r1);
    // r1->read_group().to_json(std::cout);
}

TEST_CASE("app: set new embedded object", "[sync][pbs][app][baas]") {
    const std::string valid_pk_name = "_id";

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

    auto server_app_config = minimal_app_config("set_new_embedded_object", schema);
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

TEST_CASE("app: make distributable client file", "[sync][pbs][app][baas]") {
    TestAppSession session;
    auto app = session.app();

    auto schema = get_default_schema();
    SyncTestFile original_config(app, bson::Bson("foo"), schema);
    create_user_and_log_in(app);
    SyncTestFile target_config(app, bson::Bson("foo"), schema);

    // Create realm file without client file id
    {
        auto realm = Realm::get_shared_realm(original_config);

        // Write some data
        realm->begin_transaction();
        CppContext c;
        Object::create(c, realm, "Person",
                       std::any(realm::AnyDict{{"_id", std::any(ObjectId::gen())},
                                               {"age", INT64_C(64)},
                                               {"firstName", std::string("Paul")},
                                               {"lastName", std::string("McCartney")}}));
        realm->commit_transaction();
        wait_for_upload(*realm);
        wait_for_download(*realm);

        realm->convert(target_config);

        // Write some additional data
        realm->begin_transaction();
        Object::create(c, realm, "Dog",
                       std::any(realm::AnyDict{{"_id", std::any(ObjectId::gen())},
                                               {"breed", std::string("stabyhoun")},
                                               {"name", std::string("albert")},
                                               {"realm_id", std::string("foo")}}));
        realm->commit_transaction();
        wait_for_upload(*realm);
    }
    // Starting a new session based on the copy
    {
        auto realm = Realm::get_shared_realm(target_config);
        REQUIRE(realm->read_group().get_table("class_Person")->size() == 1);
        REQUIRE(realm->read_group().get_table("class_Dog")->size() == 0);

        // Should be able to download the object created in the source Realm
        // after writing the copy
        wait_for_download(*realm);
        realm->refresh();
        REQUIRE(realm->read_group().get_table("class_Person")->size() == 1);
        REQUIRE(realm->read_group().get_table("class_Dog")->size() == 1);

        // Check that we can continue committing to this realm
        realm->begin_transaction();
        CppContext c;
        Object::create(c, realm, "Dog",
                       std::any(realm::AnyDict{{"_id", std::any(ObjectId::gen())},
                                               {"breed", std::string("bulldog")},
                                               {"name", std::string("fido")},
                                               {"realm_id", std::string("foo")}}));
        realm->commit_transaction();
        wait_for_upload(*realm);
    }
    // Original Realm should be able to read the object which was written to the copy
    {
        auto realm = Realm::get_shared_realm(original_config);
        REQUIRE(realm->read_group().get_table("class_Person")->size() == 1);
        REQUIRE(realm->read_group().get_table("class_Dog")->size() == 1);

        wait_for_download(*realm);
        realm->refresh();
        REQUIRE(realm->read_group().get_table("class_Person")->size() == 1);
        REQUIRE(realm->read_group().get_table("class_Dog")->size() == 2);
    }
}

TEST_CASE("app: sync integration", "[sync][pbs][app][baas]") {
    auto logger = util::Logger::get_default_logger();

    const auto schema = get_default_schema();

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

    // MARK: Expired Session Refresh -
    SECTION("Invalid Access Token is Refreshed") {
        {
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);
            REQUIRE(get_dogs(r).size() == 0);
            create_one_dog(r);
            REQUIRE(get_dogs(r).size() == 1);
        }

        {
            create_user_and_log_in(app);
            auto user = app->current_user();
            // set a bad access token. this will trigger a refresh when the sync session opens
            user->update_access_token(encode_fake_jwt("fake_access_token"));

            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);
            Results dogs = get_dogs(r);
            REQUIRE(dogs.size() == 1);
            REQUIRE(dogs.get(0).get<String>("breed") == "bulldog");
            REQUIRE(dogs.get(0).get<String>("name") == "fido");
        }
    }

    class HookedTransport : public SynchronousTestTransport {
    public:
        void send_request_to_server(const Request& request,
                                    util::UniqueFunction<void(const Response&)>&& completion) override
        {
            if (request_hook) {
                request_hook(request);
            }
            if (simulated_response) {
                return completion(*simulated_response);
            }
            SynchronousTestTransport::send_request_to_server(request, [&](const Response& response) mutable {
                if (response_hook) {
                    response_hook(request, response);
                }
                completion(response);
            });
        }
        // Optional handler for the request and response before it is returned to completion
        std::function<void(const Request&, const Response&)> response_hook;
        // Optional handler for the request before it is sent to the server
        std::function<void(const Request&)> request_hook;
        // Optional Response object to return immediately instead of communicating with the server
        std::optional<Response> simulated_response;
    };

    struct HookedSocketProvider : public sync::websocket::DefaultSocketProvider {
        HookedSocketProvider(const std::shared_ptr<util::Logger>& logger, const std::string user_agent,
                             AutoStart auto_start = AutoStart{true})
            : DefaultSocketProvider(logger, user_agent, nullptr, auto_start)
        {
        }

        std::unique_ptr<sync::WebSocketInterface> connect(std::unique_ptr<sync::WebSocketObserver> observer,
                                                          sync::WebSocketEndpoint&& endpoint) override
        {
            int status_code = 101;
            std::string body;
            bool use_simulated_response = websocket_connect_func && websocket_connect_func(status_code, body);

            auto websocket = DefaultSocketProvider::connect(std::move(observer), std::move(endpoint));
            if (use_simulated_response) {
                auto default_websocket = static_cast<sync::websocket::DefaultWebSocket*>(websocket.get());
                if (default_websocket)
                    default_websocket->force_handshake_response_for_testing(status_code, body);
            }
            return websocket;
        }

        std::function<bool(int&, std::string&)> websocket_connect_func;
    };

    {
        std::unique_ptr<realm::AppSession> app_session;
        std::string base_file_path = util::make_temp_dir() + random_string(10);
        auto redir_transport = std::make_shared<HookedTransport>();
        AutoVerifiedEmailCredentials creds;

        auto app_config = get_config(redir_transport, session.app_session());
        set_app_config_defaults(app_config, redir_transport);

        util::try_make_dir(base_file_path);
        SyncClientConfig sc_config;
        RealmBackingStoreConfig bsc;
        bsc.base_file_path = base_file_path;
        bsc.metadata_mode = realm::app::RealmBackingStoreConfig::MetadataMode::NoEncryption;
        auto factory = [bsc](app::SharedApp app) {
            return std::make_shared<RealmBackingStore>(app, bsc);
        };
        // initialize app and sync client
        auto redir_app = app::App::get_app(app::App::CacheMode::Disabled, app_config, sc_config, factory);

        SECTION("Test invalid redirect response") {
            int request_count = 0;
            redir_transport->request_hook = [&](const Request& request) {
                if (request_count == 0) {
                    logger->trace("request.url (%1): %2", request_count, request.url);
                    redir_transport->simulated_response = {
                        301, 0, {{"Content-Type", "application/json"}}, "Some body data"};
                    request_count++;
                }
                else if (request_count == 1) {
                    logger->trace("request.url (%1): %2", request_count, request.url);
                    redir_transport->simulated_response = {
                        301, 0, {{"Location", ""}, {"Content-Type", "application/json"}}, "Some body data"};
                    request_count++;
                }
            };

            // This will fail due to no Location header
            redir_app->provider_client<app::App::UsernamePasswordProviderClient>().register_email(
                creds.email, creds.password, [&](util::Optional<app::AppError> error) {
                    REQUIRE(error);
                    REQUIRE(error->is_client_error());
                    REQUIRE(error->code() == ErrorCodes::ClientRedirectError);
                    REQUIRE(error->reason() == "Redirect response missing location header");
                });

            // This will fail due to empty Location header
            redir_app->provider_client<app::App::UsernamePasswordProviderClient>().register_email(
                creds.email, creds.password, [&](util::Optional<app::AppError> error) {
                    REQUIRE(error);
                    REQUIRE(error->is_client_error());
                    REQUIRE(error->code() == ErrorCodes::ClientRedirectError);
                    REQUIRE(error->reason() == "Redirect response missing location header");
                });
        }

        SECTION("Test redirect response") {
            int request_count = 0;
            // redirect URL is localhost or 127.0.0.1 depending on what the initial value is
            std::string original_host = "localhost:9090";
            std::string redirect_scheme = "http://";
            std::string redirect_host = "127.0.0.1:9090";
            std::string redirect_url = "http://127.0.0.1:9090";
            redir_transport->request_hook = [&](const Request& request) {
                logger->trace("Received request[%1]: %2", request_count, request.url);
                if (request_count == 0) {
                    // First request should be to location
                    REQUIRE(request.url.find("/location") != std::string::npos);
                    if (request.url.find("https://") != std::string::npos) {
                        redirect_scheme = "https://";
                    }
                    // using local baas
                    if (request.url.find("127.0.0.1:9090") != std::string::npos) {
                        redirect_host = "localhost:9090";
                        original_host = "127.0.0.1:9090";
                    }
                    // using baas docker - can't test redirect
                    else if (request.url.find("mongodb-realm:9090") != std::string::npos) {
                        redirect_host = "mongodb-realm:9090";
                        original_host = "mongodb-realm:9090";
                    }

                    redirect_url = redirect_scheme + redirect_host;
                    logger->trace("redirect_url (%1): %2", request_count, redirect_url);
                    request_count++;
                }
                else if (request_count == 1) {
                    logger->trace("request.url (%1): %2", request_count, request.url);
                    REQUIRE(!request.redirect_count);
                    redir_transport->simulated_response = {
                        301,
                        0,
                        {{"Location", "http://somehost:9090"}, {"Content-Type", "application/json"}},
                        "Some body data"};
                    request_count++;
                }
                else if (request_count == 2) {
                    logger->trace("request.url (%1): %2", request_count, request.url);
                    REQUIRE(request.url.find("somehost:9090") != std::string::npos);
                    redir_transport->simulated_response = {
                        308, 0, {{"Location", redirect_url}, {"Content-Type", "application/json"}}, "Some body data"};
                    request_count++;
                }
                else if (request_count == 3) {
                    logger->trace("request.url (%1): %2", request_count, request.url);
                    REQUIRE(request.url.find(redirect_url) != std::string::npos);
                    redir_transport->simulated_response = {
                        301,
                        0,
                        {{"Location", redirect_scheme + original_host}, {"Content-Type", "application/json"}},
                        "Some body data"};
                    request_count++;
                }
                else if (request_count == 4) {
                    logger->trace("request.url (%1): %2", request_count, request.url);
                    REQUIRE(request.url.find(redirect_scheme + original_host) != std::string::npos);
                    // Let the init_app_metadata request go through
                    redir_transport->simulated_response.reset();
                    request_count++;
                }
                else if (request_count == 5) {
                    // This is the original request after the init app metadata
                    logger->trace("request.url (%1): %2", request_count, request.url);
                    auto backing_store = redir_app->backing_store();
                    REQUIRE(backing_store);
                    auto app_metadata = backing_store->app_metadata();
                    REQUIRE(app_metadata);
                    logger->trace("Deployment model: %1", app_metadata->deployment_model);
                    logger->trace("Location: %1", app_metadata->location);
                    logger->trace("Hostname: %1", app_metadata->hostname);
                    logger->trace("WS Hostname: %1", app_metadata->ws_hostname);
                    REQUIRE(app_metadata->hostname.find(original_host) != std::string::npos);
                    REQUIRE(request.url.find(redirect_scheme + original_host) != std::string::npos);
                    redir_transport->simulated_response.reset();
                    // Validate the retry count tracked in the original message
                    REQUIRE(request.redirect_count == 3);
                    request_count++;
                }
            };

            // This will be successful after a couple of retries due to the redirect response
            redir_app->provider_client<app::App::UsernamePasswordProviderClient>().register_email(
                creds.email, creds.password, [&](util::Optional<app::AppError> error) {
                    REQUIRE(!error);
                });
        }
        SECTION("Test too many redirects") {
            int request_count = 0;
            redir_transport->request_hook = [&](const Request& request) {
                logger->trace("request.url (%1): %2", request_count, request.url);
                REQUIRE(request_count <= 21);
                redir_transport->simulated_response = {
                    request_count % 2 == 1 ? 308 : 301,
                    0,
                    {{"Location", "http://somehost:9090"}, {"Content-Type", "application/json"}},
                    "Some body data"};
                request_count++;
            };

            redir_app->log_in_with_credentials(
                realm::app::AppCredentials::username_password(creds.email, creds.password),
                [&](std::shared_ptr<realm::SyncUser> user, util::Optional<app::AppError> error) {
                    REQUIRE(!user);
                    REQUIRE(error);
                    REQUIRE(error->is_client_error());
                    REQUIRE(error->code() == ErrorCodes::ClientTooManyRedirects);
                    REQUIRE(error->reason() == "number of redirections exceeded 20");
                });
        }
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
    SECTION("Test app redirect with no metadata") {
        std::unique_ptr<realm::AppSession> app_session;
        std::string base_file_path = util::make_temp_dir() + random_string(10);
        auto redir_transport = std::make_shared<HookedTransport>();
        AutoVerifiedEmailCredentials creds, creds2;

        auto app_config = get_config(redir_transport, session.app_session());
        set_app_config_defaults(app_config, redir_transport);

        util::try_make_dir(base_file_path);
        SyncClientConfig sc_config;
        RealmBackingStoreConfig bsc;
        bsc.base_file_path = base_file_path;
        bsc.metadata_mode = realm::app::RealmBackingStoreConfig::MetadataMode::NoMetadata;
        auto factory = [&bsc](SharedApp app) {
            return std::make_shared<RealmBackingStore>(app, bsc);
        };
        // initialize app and sync client
        auto redir_app = app::App::get_app(app::App::CacheMode::Disabled, app_config, sc_config, factory);

        int request_count = 0;
        // redirect URL is localhost or 127.0.0.1 depending on what the initial value is
        std::string original_host = "localhost:9090";
        std::string original_scheme = "http://";
        std::string websocket_url = "ws://some-websocket:9090";
        std::string original_url;
        redir_transport->request_hook = [&](const Request& request) {
            logger->trace("request.url (%1): %2", request_count, request.url);
            if (request_count == 0) {
                // First request should be to location
                REQUIRE(request.url.find("/location") != std::string::npos);
                if (request.url.find("https://") != std::string::npos) {
                    original_scheme = "https://";
                }
                // using local baas
                if (request.url.find("127.0.0.1:9090") != std::string::npos) {
                    original_host = "127.0.0.1:9090";
                }
                // using baas docker
                else if (request.url.find("mongodb-realm:9090") != std::string::npos) {
                    original_host = "mongodb-realm:9090";
                }
                original_url = original_scheme + original_host;
                logger->trace("original_url (%1): %2", request_count, original_url);
            }
            else if (request_count == 1) {
                REQUIRE(!request.redirect_count);
                redir_transport->simulated_response = {
                    308,
                    0,
                    {{"Location", "http://somehost:9090"}, {"Content-Type", "application/json"}},
                    "Some body data"};
            }
            else if (request_count == 2) {
                REQUIRE(request.url.find("http://somehost:9090") != std::string::npos);
                REQUIRE(request.url.find("location") != std::string::npos);
                // app hostname will be updated via the metadata info
                redir_transport->simulated_response = {
                    static_cast<int>(sync::HTTPStatus::Ok),
                    0,
                    {{"Content-Type", "application/json"}},
                    util::format("{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":\"%1\",\"ws_"
                                 "hostname\":\"%2\"}",
                                 original_url, websocket_url)};
            }
            else {
                REQUIRE(request.url.find(original_url) != std::string::npos);
                redir_transport->simulated_response.reset();
            }
            request_count++;
        };

        // This will be successful after a couple of retries due to the redirect response
        redir_app->provider_client<app::App::UsernamePasswordProviderClient>().register_email(
            creds.email, creds.password, [&](util::Optional<app::AppError> error) {
                REQUIRE(!error);
            });
        REQUIRE(!redir_app->backing_store()->app_metadata()); // no stored app metadata
        REQUIRE(redir_app->sync_manager()->sync_route().find(websocket_url) != std::string::npos);

        // Register another email address and verify location data isn't requested again
        request_count = 0;
        redir_transport->request_hook = [&](const Request& request) {
            logger->trace("request.url (%1): %2", request_count, request.url);
            redir_transport->simulated_response.reset();
            REQUIRE(request.url.find("location") == std::string::npos);
            request_count++;
        };

        redir_app->provider_client<app::App::UsernamePasswordProviderClient>().register_email(
            creds2.email, creds2.password, [&](util::Optional<app::AppError> error) {
                REQUIRE(!error);
            });
    }

    SECTION("Test websocket redirect with existing session") {
        std::string original_host = "localhost:9090";
        std::string redirect_scheme = "http://";
        std::string websocket_scheme = "ws://";
        std::string redirect_host = "127.0.0.1:9090";
        std::string redirect_url = "http://127.0.0.1:9090";

        auto redir_transport = std::make_shared<HookedTransport>();
        auto redir_provider = std::make_shared<HookedSocketProvider>(logger, "");
        std::mutex logout_mutex;
        std::condition_variable logout_cv;
        bool logged_out = false;

        // Use the transport to grab the current url so it can be converted
        redir_transport->request_hook = [&](const Request& request) {
            if (request.url.find("https://") != std::string::npos) {
                redirect_scheme = "https://";
                websocket_scheme = "wss://";
            }
            // using local baas
            if (request.url.find("127.0.0.1:9090") != std::string::npos) {
                redirect_host = "localhost:9090";
                original_host = "127.0.0.1:9090";
            }
            // using baas docker - can't test redirect
            else if (request.url.find("mongodb-realm:9090") != std::string::npos) {
                redirect_host = "mongodb-realm:9090";
                original_host = "mongodb-realm:9090";
            }

            redirect_url = redirect_scheme + redirect_host;
            logger->trace("redirect_url: %1", redirect_url);
        };

        auto server_app_config = minimal_app_config("websocket_redirect", schema);
        TestAppSession test_session({create_app(server_app_config), redir_transport, DeleteApp{true},
                                     realm::ReconnectMode::normal, redir_provider});
        auto partition = random_string(100);
        auto user1 = test_session.app()->current_user();
        SyncTestFile r_config(user1, partition, schema);
        // Override the default
        r_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
            if (error.status == ErrorCodes::AuthError) {
                util::format(std::cerr, "Websocket redirect test: User logged out\n");
                std::unique_lock lk(logout_mutex);
                logged_out = true;
                logout_cv.notify_one();
                return;
            }
            util::format(std::cerr, "An unexpected sync error was caught by the default SyncTestFile handler: '%1'\n",
                         error.status);
            abort();
        };

        auto r = Realm::get_shared_realm(r_config);

        REQUIRE(!wait_for_download(*r));

        SECTION("Valid websocket redirect") {
            auto sync_manager = test_session.app()->sync_manager();
            auto sync_session = sync_manager->get_existing_session(r->config().path);
            sync_session->pause();

            int connect_count = 0;
            redir_provider->websocket_connect_func = [&connect_count](int& status_code, std::string& body) {
                if (connect_count++ > 0)
                    return false;

                status_code = static_cast<int>(sync::HTTPStatus::PermanentRedirect);
                body = "";
                return true;
            };
            int request_count = 0;
            redir_transport->request_hook = [&](const Request& request) {
                logger->trace("request.url (%1): %2", request_count, request.url);
                if (request_count++ == 0) {
                    // First request should be a location request against the original URL
                    REQUIRE(request.url.find(original_host) != std::string::npos);
                    REQUIRE(request.url.find("/location") != std::string::npos);
                    REQUIRE(request.redirect_count == 0);
                    redir_transport->simulated_response = {
                        static_cast<int>(sync::HTTPStatus::PermanentRedirect),
                        0,
                        {{"Location", redirect_url}, {"Content-Type", "application/json"}},
                        "Some body data"};
                }
                else if (request.url.find("/location") != std::string::npos) {
                    redir_transport->simulated_response = {
                        static_cast<int>(sync::HTTPStatus::Ok),
                        0,
                        {{"Content-Type", "application/json"}},
                        util::format(
                            "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":\"%2%1\",\"ws_"
                            "hostname\":\"%3%1\"}",
                            redirect_host, redirect_scheme, websocket_scheme)};
                }
                else {
                    redir_transport->simulated_response.reset();
                }
            };

            SyncManager::OnlyForTesting::voluntary_disconnect_all_connections(*sync_manager);
            sync_session->resume();
            REQUIRE(!wait_for_download(*r));
            REQUIRE(user1->is_logged_in());

            // Verify session is using the updated server url from the redirect
            auto server_url = sync_session->full_realm_url();
            logger->trace("FULL_REALM_URL: %1", server_url);
            REQUIRE((server_url && server_url->find(redirect_host) != std::string::npos));
        }
        SECTION("Websocket redirect logs out user") {
            auto sync_manager = test_session.app()->sync_manager();
            auto sync_session = sync_manager->get_existing_session(r->config().path);
            sync_session->pause();

            int connect_count = 0;
            redir_provider->websocket_connect_func = [&connect_count](int& status_code, std::string& body) {
                if (connect_count++ > 0)
                    return false;

                status_code = static_cast<int>(sync::HTTPStatus::MovedPermanently);
                body = "";
                return true;
            };
            int request_count = 0;
            redir_transport->request_hook = [&](const Request& request) {
                logger->trace("request.url (%1): %2", request_count, request.url);
                if (request_count++ == 0) {
                    // First request should be a location request against the original URL
                    REQUIRE(request.url.find(original_host) != std::string::npos);
                    REQUIRE(request.url.find("/location") != std::string::npos);
                    REQUIRE(request.redirect_count == 0);
                    redir_transport->simulated_response = {
                        static_cast<int>(sync::HTTPStatus::MovedPermanently),
                        0,
                        {{"Location", redirect_url}, {"Content-Type", "application/json"}},
                        "Some body data"};
                }
                else if (request.url.find("/location") != std::string::npos) {
                    redir_transport->simulated_response = {
                        static_cast<int>(sync::HTTPStatus::Ok),
                        0,
                        {{"Content-Type", "application/json"}},
                        util::format(
                            "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":\"%2%1\",\"ws_"
                            "hostname\":\"%3%1\"}",
                            redirect_host, redirect_scheme, websocket_scheme)};
                }
                else if (request.url.find("auth/session") != std::string::npos) {
                    redir_transport->simulated_response = {static_cast<int>(sync::HTTPStatus::Unauthorized),
                                                           0,
                                                           {{"Content-Type", "application/json"}},
                                                           ""};
                }
                else {
                    redir_transport->simulated_response.reset();
                }
            };

            SyncManager::OnlyForTesting::voluntary_disconnect_all_connections(*sync_manager);
            sync_session->resume();
            REQUIRE(wait_for_download(*r));
            std::unique_lock lk(logout_mutex);
            auto result = logout_cv.wait_for(lk, std::chrono::seconds(15), [&]() {
                return logged_out;
            });
            REQUIRE(result);
            REQUIRE(!user1->is_logged_in());
        }
        SECTION("Too many websocket redirects logs out user") {
            auto sync_manager = test_session.app()->sync_manager();
            auto sync_session = sync_manager->get_existing_session(r->config().path);
            sync_session->pause();

            int connect_count = 0;
            redir_provider->websocket_connect_func = [&connect_count](int& status_code, std::string& body) {
                if (connect_count++ > 0)
                    return false;

                status_code = static_cast<int>(sync::HTTPStatus::MovedPermanently);
                body = "";
                return true;
            };
            int request_count = 0;
            const int max_http_redirects = 20; // from app.cpp in object-store
            redir_transport->request_hook = [&](const Request& request) {
                logger->trace("request.url (%1): %2", request_count, request.url);
                if (request_count++ == 0) {
                    // First request should be a location request against the original URL
                    REQUIRE(request.url.find(original_host) != std::string::npos);
                    REQUIRE(request.url.find("/location") != std::string::npos);
                    REQUIRE(request.redirect_count == 0);
                }
                if (request.url.find("/location") != std::string::npos) {
                    // Keep returning the redirected response
                    REQUIRE(request.redirect_count < max_http_redirects);
                    redir_transport->simulated_response = {
                        static_cast<int>(sync::HTTPStatus::MovedPermanently),
                        0,
                        {{"Location", redirect_url}, {"Content-Type", "application/json"}},
                        "Some body data"};
                }
                else {
                    // should not get any other types of requests during the test - the log out is local
                    REQUIRE(false);
                }
            };

            SyncManager::OnlyForTesting::voluntary_disconnect_all_connections(*sync_manager);
            sync_session->resume();
            REQUIRE(wait_for_download(*r));
            std::unique_lock lk(logout_mutex);
            auto result = logout_cv.wait_for(lk, std::chrono::seconds(15), [&]() {
                return logged_out;
            });
            REQUIRE(result);
            REQUIRE(!user1->is_logged_in());
        }
    }

    SECTION("Fast clock on client") {
        {
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);

            REQUIRE(get_dogs(r).size() == 0);
            create_one_dog(r);
            REQUIRE(get_dogs(r).size() == 1);
        }

        auto transport = std::make_shared<HookedTransport>();
        TestAppSession hooked_session({session.app_session(), transport, DeleteApp{false}});
        auto app = hooked_session.app();
        std::shared_ptr<SyncUser> user = app->current_user();
        REQUIRE(user);
        REQUIRE(!user->access_token_refresh_required());
        // Make the SyncUser behave as if the client clock is 31 minutes fast, so the token looks expired locally
        // (access tokens have an lifetime of 30 minutes today).
        user->set_seconds_to_adjust_time_for_testing(31 * 60);
        REQUIRE(user->access_token_refresh_required());

        // This assumes that we make an http request for the new token while
        // already in the WaitingForAccessToken state.
        bool seen_waiting_for_access_token = false;
        transport->request_hook = [&](const Request&) {
            auto user = app->current_user();
            REQUIRE(user);
            for (auto& session : app->sync_manager()->get_all_sessions_for(*user)) {
                // Prior to the fix for #4941, this callback would be called from an infinite loop, always in the
                // WaitingForAccessToken state.
                if (session->state() == SyncSession::State::WaitingForAccessToken) {
                    REQUIRE(!seen_waiting_for_access_token);
                    seen_waiting_for_access_token = true;
                }
            }
            return true;
        };
        SyncTestFile config(app, partition, schema);
        auto r = Realm::get_shared_realm(config);
        REQUIRE(seen_waiting_for_access_token);
        Results dogs = get_dogs(r);
        REQUIRE(dogs.size() == 1);
        REQUIRE(dogs.get(0).get<String>("breed") == "bulldog");
        REQUIRE(dogs.get(0).get<String>("name") == "fido");
    }

    SECTION("Expired Tokens") {
        sync::AccessToken token;
        {
            std::shared_ptr<SyncUser> user = app->current_user();
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);

            REQUIRE(get_dogs(r).size() == 0);
            create_one_dog(r);

            REQUIRE(get_dogs(r).size() == 1);
            sync::AccessToken::ParseError error_state = realm::sync::AccessToken::ParseError::none;
            sync::AccessToken::parse(user->access_token(), token, error_state, nullptr);
            REQUIRE(error_state == sync::AccessToken::ParseError::none);
            REQUIRE(token.timestamp);
            REQUIRE(token.expires);
            REQUIRE(token.timestamp < token.expires);
            std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
            using namespace std::chrono_literals;
            token.expires = std::chrono::system_clock::to_time_t(now - 30s);
            REQUIRE(token.expired(now));
        }

        auto transport = std::make_shared<HookedTransport>();
        TestAppSession hooked_session({session.app_session(), transport, DeleteApp{false}});
        auto app = hooked_session.app();
        std::shared_ptr<SyncUser> user = app->current_user();
        REQUIRE(user);
        REQUIRE(!user->access_token_refresh_required());
        // Set a bad access token, with an expired time. This will trigger a refresh initiated by the client.
        user->update_access_token(encode_fake_jwt("fake_access_token", token.expires, token.timestamp));
        REQUIRE(user->access_token_refresh_required());

        SECTION("Expired Access Token is Refreshed") {
            // This assumes that we make an http request for the new token while
            // already in the WaitingForAccessToken state.
            bool seen_waiting_for_access_token = false;
            transport->request_hook = [&](const Request&) {
                auto user = app->current_user();
                REQUIRE(user);
                for (auto& session : app->sync_manager()->get_all_sessions_for(*user)) {
                    if (session->state() == SyncSession::State::WaitingForAccessToken) {
                        REQUIRE(!seen_waiting_for_access_token);
                        seen_waiting_for_access_token = true;
                    }
                }
            };
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);
            REQUIRE(seen_waiting_for_access_token);
            Results dogs = get_dogs(r);
            REQUIRE(dogs.size() == 1);
            REQUIRE(dogs.get(0).get<String>("breed") == "bulldog");
            REQUIRE(dogs.get(0).get<String>("name") == "fido");
        }

        SECTION("User is logged out if the refresh request is denied") {
            REQUIRE(user->is_logged_in());
            transport->response_hook = [&](const Request& request, const Response& response) {
                auto user = app->current_user();
                REQUIRE(user);
                // simulate the server denying the refresh
                if (request.url.find("/session") != std::string::npos) {
                    auto& response_ref = const_cast<Response&>(response);
                    response_ref.http_status_code = 401;
                    response_ref.body = "fake: refresh token could not be refreshed";
                }
            };
            SyncTestFile config(app, partition, schema);
            std::atomic<bool> sync_error_handler_called{false};
            config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                sync_error_handler_called.store(true);
                REQUIRE(error.status.code() == ErrorCodes::AuthError);
                REQUIRE_THAT(std::string{error.status.reason()},
                             Catch::Matchers::StartsWith("Unable to refresh the user access token"));
            };
            auto r = Realm::get_shared_realm(config);
            timed_wait_for([&] {
                return sync_error_handler_called.load();
            });
            // the failed refresh logs out the user
            REQUIRE(!user->is_logged_in());
        }

        SECTION("User is left logged out if logged out while the refresh is in progress") {
            REQUIRE(user->is_logged_in());
            transport->request_hook = [&](const Request&) {
                user->log_out();
            };
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);
            REQUIRE_FALSE(user->is_logged_in());
            REQUIRE(user->state() == SyncUser::State::LoggedOut);
        }

        SECTION("Requests that receive an error are retried on a backoff") {
            using namespace std::chrono;
            std::vector<time_point<steady_clock>> response_times;
            std::atomic<bool> did_receive_valid_token{false};
            constexpr size_t num_error_responses = 6;

            transport->response_hook = [&](const Request& request, const Response& response) {
                // simulate the server experiencing an internal server error
                if (request.url.find("/session") != std::string::npos) {
                    if (response_times.size() >= num_error_responses) {
                        did_receive_valid_token.store(true);
                        return;
                    }
                    auto& response_ref = const_cast<Response&>(response);
                    response_ref.http_status_code = 500;
                }
            };
            transport->request_hook = [&](const Request& request) {
                if (!did_receive_valid_token.load() && request.url.find("/session") != std::string::npos) {
                    response_times.push_back(steady_clock::now());
                }
            };
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);
            create_one_dog(r);
            timed_wait_for(
                [&] {
                    return did_receive_valid_token.load();
                },
                30s);
            REQUIRE(user->is_logged_in());
            REQUIRE(response_times.size() >= num_error_responses);
            std::vector<uint64_t> delay_times;
            for (size_t i = 1; i < response_times.size(); ++i) {
                delay_times.push_back(duration_cast<milliseconds>(response_times[i] - response_times[i - 1]).count());
            }

            // sync delays start at 1000ms minus a random number of up to 25%.
            // the subsequent delay is double the previous one minus a random 25% again.
            // this calculation happens in Connection::initiate_reconnect_wait()
            bool increasing_delay = true;
            for (size_t i = 1; i < delay_times.size(); ++i) {
                if (delay_times[i - 1] >= delay_times[i]) {
                    increasing_delay = false;
                }
            }
            // fail if the first delay isn't longer than half a second
            if (delay_times.size() <= 1 || delay_times[1] < 500) {
                increasing_delay = false;
            }
            if (!increasing_delay) {
                std::cerr << "delay times are not increasing: ";
                for (auto& delay : delay_times) {
                    std::cerr << delay << ", ";
                }
                std::cerr << std::endl;
            }
            REQUIRE(increasing_delay);
        }
    }

    SECTION("Invalid refresh token") {
        auto& app_session = session.app_session();
        std::mutex mtx;
        auto verify_error_on_sync_with_invalid_refresh_token = [&](std::shared_ptr<SyncUser> user,
                                                                   Realm::Config config) {
            REQUIRE(user);
            REQUIRE(app_session.admin_api.verify_access_token(user->access_token(), app_session.server_app_id));

            // requesting a new access token fails because the refresh token used for this request is revoked
            user->refresh_custom_data([&](Optional<AppError> error) {
                REQUIRE(error);
                REQUIRE(error->additional_status_code == 401);
                REQUIRE(error->code() == ErrorCodes::InvalidSession);
            });

            // Set a bad access token. This will force a request for a new access token when the sync session opens
            // this is only necessary because the server doesn't actually revoke previously issued access tokens
            // instead allowing their session to time out as normal. So this simulates the access token expiring.
            // see:
            // https://github.com/10gen/baas/blob/05837cc3753218dfaf89229c6930277ef1616402/api/common/auth.go#L1380-L1386
            user->update_access_token(encode_fake_jwt("fake_access_token"));
            REQUIRE(!app_session.admin_api.verify_access_token(user->access_token(), app_session.server_app_id));

            auto [sync_error_promise, sync_error] = util::make_promise_future<SyncError>();
            config.sync_config->error_handler =
                [promise = util::CopyablePromiseHolder(std::move(sync_error_promise))](std::shared_ptr<SyncSession>,
                                                                                       SyncError error) mutable {
                    promise.get_promise().emplace_value(std::move(error));
                };

            auto transport = static_cast<SynchronousTestTransport*>(session.transport());
            transport->block(); // don't let the token refresh happen until we're ready for it
            auto r = Realm::get_shared_realm(config);
            auto session = app->sync_manager()->get_existing_session(config.path);
            REQUIRE(user->is_logged_in());
            REQUIRE(!sync_error.is_ready());
            {
                std::atomic<bool> called{false};
                session->wait_for_upload_completion([&](Status stat) {
                    std::lock_guard<std::mutex> lock(mtx);
                    called.store(true);
                    REQUIRE(stat.code() == ErrorCodes::InvalidSession);
                });
                transport->unblock();
                timed_wait_for([&] {
                    return called.load();
                });
                std::lock_guard<std::mutex> lock(mtx);
                REQUIRE(called);
            }

            auto sync_error_res = wait_for_future(std::move(sync_error)).get();
            REQUIRE(sync_error_res.status == ErrorCodes::AuthError);
            REQUIRE_THAT(std::string{sync_error_res.status.reason()},
                         Catch::Matchers::StartsWith("Unable to refresh the user access token"));

            // the failed refresh logs out the user
            std::lock_guard<std::mutex> lock(mtx);
            REQUIRE(!user->is_logged_in());
        };

        SECTION("Disabled user results in a sync error") {
            auto creds = create_user_and_log_in(app);
            SyncTestFile config(app, partition, schema);
            auto user = app->current_user();
            REQUIRE(user);
            REQUIRE(app_session.admin_api.verify_access_token(user->access_token(), app_session.server_app_id));
            app_session.admin_api.disable_user_sessions(app->current_user()->user_id(), app_session.server_app_id);

            verify_error_on_sync_with_invalid_refresh_token(user, config);

            // logging in again doesn't fix things while the account is disabled
            auto error = failed_log_in(app, creds);
            REQUIRE(error.code() == ErrorCodes::UserDisabled);

            // admin enables user sessions again which should allow the session to continue
            app_session.admin_api.enable_user_sessions(user->user_id(), app_session.server_app_id);

            // logging in now works properly
            log_in(app, creds);

            // still referencing the same user
            REQUIRE(user == app->current_user());
            REQUIRE(user->is_logged_in());

            {
                // check that there are no errors initiating a session now by making sure upload/download succeeds
                auto r = Realm::get_shared_realm(config);
                Results dogs = get_dogs(r);
            }
        }

        SECTION("Revoked refresh token results in a sync error") {
            auto creds = create_user_and_log_in(app);
            SyncTestFile config(app, partition, schema);
            auto user = app->current_user();
            REQUIRE(app_session.admin_api.verify_access_token(user->access_token(), app_session.server_app_id));
            app_session.admin_api.revoke_user_sessions(user->user_id(), app_session.server_app_id);
            // revoking a user session only affects the refresh token, so the access token should still continue to
            // work.
            REQUIRE(app_session.admin_api.verify_access_token(user->access_token(), app_session.server_app_id));

            verify_error_on_sync_with_invalid_refresh_token(user, config);

            // logging in again succeeds and generates a new and valid refresh token
            log_in(app, creds);

            // still referencing the same user and now the user is logged in
            REQUIRE(user == app->current_user());
            REQUIRE(user->is_logged_in());

            // new requests for an access token succeed again
            user->refresh_custom_data([&](Optional<AppError> error) {
                REQUIRE_FALSE(error);
            });

            {
                // check that there are no errors initiating a new sync session by making sure upload/download
                // succeeds
                auto r = Realm::get_shared_realm(config);
                Results dogs = get_dogs(r);
            }
        }

        SECTION("Revoked refresh token on an anonymous user results in a sync error") {
            app->current_user()->log_out();
            auto anon_user = log_in(app);
            REQUIRE(app->current_user() == anon_user);
            SyncTestFile config(app, partition, schema);
            REQUIRE(app_session.admin_api.verify_access_token(anon_user->access_token(), app_session.server_app_id));
            app_session.admin_api.revoke_user_sessions(anon_user->user_id(), app_session.server_app_id);
            // revoking a user session only affects the refresh token, so the access token should still continue to
            // work.
            REQUIRE(app_session.admin_api.verify_access_token(anon_user->access_token(), app_session.server_app_id));

            verify_error_on_sync_with_invalid_refresh_token(anon_user, config);

            // the user has been logged out, and current user is reset
            REQUIRE(!app->current_user());
            REQUIRE(!anon_user->is_logged_in());
            REQUIRE(anon_user->state() == SyncUser::State::Removed);

            // new requests for an access token do not work for anon users
            anon_user->refresh_custom_data([&](Optional<AppError> error) {
                REQUIRE(error);
                REQUIRE(error->reason() ==
                        util::format("Cannot initiate a refresh on user '%1' because the user has been removed",
                                     anon_user->user_id()));
            });

            REQUIRE_EXCEPTION(
                Realm::get_shared_realm(config), ClientUserNotFound,
                util::format("Cannot start a sync session for user '%1' because this user has been removed.",
                             anon_user->user_id()));
        }

        SECTION("Opening a Realm with a removed email user results produces an exception") {
            auto creds = create_user_and_log_in(app);
            auto email_user = app->current_user();
            const std::string user_id = email_user->user_id();
            REQUIRE(email_user);
            SyncTestFile config(app, partition, schema);
            REQUIRE(email_user->is_logged_in());
            {
                // sync works on a valid user
                auto r = Realm::get_shared_realm(config);
                Results dogs = get_dogs(r);
            }
            app->backing_store()->remove_user(user_id);
            REQUIRE_FALSE(email_user->is_logged_in());
            REQUIRE(email_user->state() == SyncUser::State::Removed);

            // should not be able to open a synced Realm with an invalid user
            REQUIRE_EXCEPTION(
                Realm::get_shared_realm(config), ClientUserNotFound,
                util::format("Cannot start a sync session for user '%1' because this user has been removed.",
                             user_id));

            std::shared_ptr<SyncUser> new_user_instance = log_in(app, creds);
            // the previous instance is still invalid
            REQUIRE_FALSE(email_user->is_logged_in());
            REQUIRE(email_user->state() == SyncUser::State::Removed);
            // but the new instance will work and has the same server issued ident
            REQUIRE(new_user_instance);
            REQUIRE(new_user_instance->is_logged_in());
            REQUIRE(new_user_instance->user_id() == user_id);
            {
                // sync works again if the same user is logged back in
                config.sync_config->user = new_user_instance;
                auto r = Realm::get_shared_realm(config);
                Results dogs = get_dogs(r);
            }
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
        r->sync_session()->wait_for_upload_completion([&](Status status) {
            std::lock_guard lk(mutex);
            REQUIRE(status.is_ok());
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

#if defined(TEST_TIMEOUT_EXTRA) && TEST_TIMEOUT_EXTRA > 0
        // It may take 30 minutes to transfer 16MB at 10KB/s
        auto delay = std::chrono::minutes(35);
#else
        auto delay = std::chrono::minutes(5);
#endif

        auto error = wait_for_future(std::move(pf.future), delay).get();
        REQUIRE(error.status == ErrorCodes::LimitExceeded);
        REQUIRE(error.status.reason() ==
                "Sync websocket closed because the server received a message that was too large: "
                "read limited at 16777217 bytes");
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
                CHECK(error.status.reason().find(
                          "Illegal Realm path (BIND): serialized partition 'not a bson serialized "
                          "string' is invalid") != std::string::npos);
                error_did_occur.store(true);
            };
            auto r = Realm::get_shared_realm(config);
            auto session = app->sync_manager()->get_existing_session(r->config().path);
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
TEMPLATE_TEST_CASE("app: collections of links integration", "[sync][pbs][app][collections][baas]", cf::ListOfObjects,
                   cf::ListOfMixedLinks, cf::SetOfObjects, cf::SetOfMixedLinks, cf::DictionaryOfObjects,
                   cf::DictionaryOfMixedLinks)
{
    const std::string valid_pk_name = "_id";
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
    auto server_app_config = minimal_app_config("collections_of_links", schema);
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
            auto& obj = object.get_obj();
            test_type.add_link(obj, link);
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
        return ObjLink{obj.get_obj().get_table()->get_key(), obj.get_obj().get_key()};
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
            auto obj = r1_source_objs.get(0);
            test_type.remove_link(obj,
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

TEMPLATE_TEST_CASE("app: partition types", "[sync][pbs][app][partition][baas]", cf::Int, cf::String, cf::OID,
                   cf::UUID, cf::BoxedOptional<cf::Int>, cf::UnboxedOptional<cf::String>, cf::BoxedOptional<cf::OID>,
                   cf::BoxedOptional<cf::UUID>)
{
    const std::string valid_pk_name = "_id";
    const std::string partition_key_col_name = "partition_key_prop";
    const std::string table_name = "class_partition_test_type";
    auto partition_property = Property(partition_key_col_name, TestType::property_type);
    Schema schema = {{Group::table_name_to_class_name(table_name),
                      {
                          {valid_pk_name, PropertyType::Int, true},
                          partition_property,
                      }}};
    auto server_app_config = minimal_app_config("partition_types_app_name", schema);
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

TEST_CASE("app: full-text compatible with sync", "[sync][app][baas]") {
    const std::string valid_pk_name = "_id";

    Schema schema{
        {"TopLevel",
         {
             {valid_pk_name, PropertyType::ObjectId, Property::IsPrimary{true}},
             {"full_text", Property::IsFulltextIndexed{true}},
         }},
    };

    auto server_app_config = minimal_app_config("full_text", schema);
    auto app_session = create_app(server_app_config);
    const auto partition = random_string(100);
    TestAppSession test_session({app_session, nullptr});
    SyncTestFile config(test_session.app(), partition, schema);
    SharedRealm realm;
    SECTION("sync open") {
        INFO("realm opened without async open");
        realm = Realm::get_shared_realm(config);
    }
    SECTION("async open") {
        INFO("realm opened with async open");
        auto async_open_task = Realm::get_synchronized_realm(config);

        auto [realm_promise, realm_future] = util::make_promise_future<ThreadSafeReference>();
        async_open_task->start(
            [promise = std::move(realm_promise)](ThreadSafeReference ref, std::exception_ptr ouch) mutable {
                if (ouch) {
                    try {
                        std::rethrow_exception(ouch);
                    }
                    catch (...) {
                        promise.set_error(exception_to_status());
                    }
                }
                else {
                    promise.emplace_value(std::move(ref));
                }
            });

        realm = Realm::get_shared_realm(std::move(realm_future.get()));
    }

    CppContext c(realm);
    auto obj_id_1 = ObjectId::gen();
    auto obj_id_2 = ObjectId::gen();
    realm->begin_transaction();
    Object::create(c, realm, "TopLevel", std::any(AnyDict{{"_id", obj_id_1}, {"full_text", "Hello, world!"s}}));
    Object::create(c, realm, "TopLevel", std::any(AnyDict{{"_id", obj_id_2}, {"full_text", "Hello, everyone!"s}}));
    realm->commit_transaction();

    auto table = realm->read_group().get_table("class_TopLevel");
    REQUIRE(table->search_index_type(table->get_column_key("full_text")) == IndexType::Fulltext);
    Results world_results(realm, Query(table).fulltext(table->get_column_key("full_text"), "world"));
    REQUIRE(world_results.size() == 1);
    REQUIRE(world_results.get<Obj>(0).get_primary_key() == Mixed{obj_id_1});
}

#endif // REALM_ENABLE_AUTH_TESTS

namespace {
class AsyncMockNetworkTransport {
public:
    AsyncMockNetworkTransport()
        : transport_thread(&AsyncMockNetworkTransport::worker_routine, this)
    {
    }

    void add_work_item(Response&& response, util::UniqueFunction<void(const Response&)>&& completion)
    {
        std::lock_guard<std::mutex> lk(transport_work_mutex);
        transport_work.push_front(ResponseWorkItem{std::move(response), std::move(completion)});
        transport_work_cond.notify_one();
    }

    void add_work_item(util::UniqueFunction<void()> cb)
    {
        std::lock_guard<std::mutex> lk(transport_work_mutex);
        transport_work.push_front(std::move(cb));
        transport_work_cond.notify_one();
    }

    void mark_complete()
    {
        std::unique_lock<std::mutex> lk(transport_work_mutex);
        test_complete = true;
        transport_work_cond.notify_one();
        lk.unlock();
        transport_thread.join();
    }

private:
    struct ResponseWorkItem {
        Response response;
        util::UniqueFunction<void(const Response&)> completion;
    };

    void worker_routine()
    {
        std::unique_lock<std::mutex> lk(transport_work_mutex);
        for (;;) {
            transport_work_cond.wait(lk, [&] {
                return test_complete || !transport_work.empty();
            });

            if (!transport_work.empty()) {
                auto work_item = std::move(transport_work.back());
                transport_work.pop_back();
                lk.unlock();

                mpark::visit(util::overload{[](ResponseWorkItem& work_item) {
                                                work_item.completion(std::move(work_item.response));
                                            },
                                            [](util::UniqueFunction<void()>& cb) {
                                                cb();
                                            }},
                             work_item);

                lk.lock();
                continue;
            }

            if (test_complete) {
                return;
            }
        }
    }

    std::mutex transport_work_mutex;
    std::condition_variable transport_work_cond;
    bool test_complete = false;
    std::list<mpark::variant<ResponseWorkItem, util::UniqueFunction<void()>>> transport_work;
    JoiningThread transport_thread;
};

} // namespace

#if 0
TEST_CASE("app: app cannot get deallocated during log in", "[sync][app]") {
    AsyncMockNetworkTransport mock_transport_worker;
    enum class TestState { unknown, location, login, app_deallocated, profile };
    struct TestStateBundle {
        void advance_to(TestState new_state)
        {
            std::lock_guard<std::mutex> lk(mutex);
            state = new_state;
            cond.notify_one();
        }

        TestState get() const
        {
            std::lock_guard<std::mutex> lk(mutex);
            return state;
        }

        void wait_for(TestState new_state)
        {
            std::unique_lock<std::mutex> lk(mutex);
            cond.wait(lk, [&] {
                return state == new_state;
            });
        }

        mutable std::mutex mutex;
        std::condition_variable cond;

        TestState state = TestState::unknown;
    } state;
    struct transport : public GenericNetworkTransport {
        transport(AsyncMockNetworkTransport& worker, TestStateBundle& state)
            : mock_transport_worker(worker)
            , state(state)
        {
        }

        void send_request_to_server(const Request& request, util::UniqueFunction<void(const Response&)>&& completion) override
        {
            if (request.url.find("/login") != std::string::npos) {
                state.advance_to(TestState::login);
                state.wait_for(TestState::app_deallocated);
                mock_transport_worker.add_work_item(
                    Response{200, 0, {}, user_json(encode_fake_jwt("access token")).dump()},
                    std::move(completion));
            }
            else if (request.url.find("/profile") != std::string::npos) {
                state.advance_to(TestState::profile);
                mock_transport_worker.add_work_item(Response{200, 0, {}, user_profile_json().dump()},
                                                    std::move(completion));
            }
            else if (request.url.find("/location") != std::string::npos) {
                CHECK(request.method == HttpMethod::get);
                state.advance_to(TestState::location);
                mock_transport_worker.add_work_item(
                    Response{200,
                             0,
                             {},
                             "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":"
                             "\"http://localhost:9090\",\"ws_hostname\":\"ws://localhost:9090\"}"},
                    std::move(completion));
            }
        }

        AsyncMockNetworkTransport& mock_transport_worker;
        TestStateBundle& state;
    };

    auto [cur_user_promise, cur_user_future] = util::make_promise_future<std::shared_ptr<SyncUser>>();
    auto transporter = std::make_shared<transport>(mock_transport_worker, state);

    {
        TestSyncManager sync_manager(get_config(transporter));
        auto app = sync_manager.app();

        app->log_in_with_credentials(AppCredentials::anonymous(),
                                     [promise = std::move(cur_user_promise)](std::shared_ptr<SyncUser> user,
                                                                             util::Optional<AppError> error) mutable {
                                         REQUIRE_FALSE(error);
                                         promise.emplace_value(std::move(user));
                                     });
    }

    // At this point the test does not hold any reference to `app`.
    state.advance_to(TestState::app_deallocated);
    auto cur_user = std::move(cur_user_future).get();
    CHECK(cur_user);

    mock_transport_worker.mark_complete();
}
#endif

TEST_CASE("app: user logs out while profile is fetched", "[sync][app][user]") {
    AsyncMockNetworkTransport mock_transport_worker;
    enum class TestState { unknown, location, login, profile };
    struct TestStateBundle {
        void advance_to(TestState new_state)
        {
            std::lock_guard<std::mutex> lk(mutex);
            state = new_state;
            cond.notify_one();
        }

        TestState get() const
        {
            std::lock_guard<std::mutex> lk(mutex);
            return state;
        }

        void wait_for(TestState new_state)
        {
            std::unique_lock<std::mutex> lk(mutex);
            cond.wait(lk, [&] {
                return state == new_state;
            });
        }

        mutable std::mutex mutex;
        std::condition_variable cond;

        TestState state = TestState::unknown;
    } state;
    struct transport : public GenericNetworkTransport {
        transport(AsyncMockNetworkTransport& worker, TestStateBundle& state,
                  std::shared_ptr<SyncUser>& logged_in_user)
            : mock_transport_worker(worker)
            , state(state)
            , logged_in_user(logged_in_user)
        {
        }

        void send_request_to_server(const Request& request,
                                    util::UniqueFunction<void(const Response&)>&& completion) override
        {
            if (request.url.find("/login") != std::string::npos) {
                state.advance_to(TestState::login);
                mock_transport_worker.add_work_item(
                    Response{200, 0, {}, user_json(encode_fake_jwt("access token")).dump()}, std::move(completion));
            }
            else if (request.url.find("/profile") != std::string::npos) {
                logged_in_user->log_out();
                state.advance_to(TestState::profile);
                mock_transport_worker.add_work_item(Response{200, 0, {}, user_profile_json().dump()},
                                                    std::move(completion));
            }
            else if (request.url.find("/location") != std::string::npos) {
                CHECK(request.method == HttpMethod::get);
                state.advance_to(TestState::location);
                mock_transport_worker.add_work_item(
                    Response{200,
                             0,
                             {},
                             "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":"
                             "\"http://localhost:9090\",\"ws_hostname\":\"ws://localhost:9090\"}"},
                    std::move(completion));
            }
        }

        AsyncMockNetworkTransport& mock_transport_worker;
        TestStateBundle& state;
        std::shared_ptr<SyncUser>& logged_in_user;
    };

    std::shared_ptr<SyncUser> logged_in_user;
    auto transporter = std::make_shared<transport>(mock_transport_worker, state, logged_in_user);
    OfflineAppSession tas({transporter});
    auto app = tas.app();

    logged_in_user = app->backing_store()->get_user("userid", good_access_token, good_access_token, dummy_device_id);
    auto custom_credentials = AppCredentials::facebook("a_token");
    auto [cur_user_promise, cur_user_future] = util::make_promise_future<std::shared_ptr<SyncUser>>();

    app->link_user(logged_in_user, custom_credentials,
                   [promise = std::move(cur_user_promise)](std::shared_ptr<SyncUser> user,
                                                           util::Optional<AppError> error) mutable {
                       REQUIRE_FALSE(error);
                       promise.emplace_value(std::move(user));
                   });

    auto cur_user = std::move(cur_user_future).get();
    CHECK(state.get() == TestState::profile);
    CHECK(cur_user);
    CHECK(cur_user == logged_in_user);

    mock_transport_worker.mark_complete();
}

TEST_CASE("app: app destroyed during token refresh", "[sync][app][user][token]") {
    AsyncMockNetworkTransport mock_transport_worker;
    enum class TestState { unknown, location, login, profile_1, profile_2, refresh_1, refresh_2, refresh_3 };
    struct TestStateBundle {
        void advance_to(TestState new_state)
        {
            std::lock_guard<std::mutex> lk(mutex);
            state = new_state;
            cond.notify_one();
        }

        TestState get() const
        {
            std::lock_guard<std::mutex> lk(mutex);
            return state;
        }

        void wait_for(TestState new_state)
        {
            std::unique_lock lk(mutex);
            bool failed = !cond.wait_for(lk, std::chrono::seconds(5), [&] {
                return state == new_state;
            });
            if (failed) {
                throw std::runtime_error("wait timed out");
            }
        }

        mutable std::mutex mutex;
        std::condition_variable cond;

        TestState state = TestState::unknown;
    } state;
    struct transport : public GenericNetworkTransport {
        transport(AsyncMockNetworkTransport& worker, TestStateBundle& state)
            : mock_transport_worker(worker)
            , state(state)
        {
        }

        void send_request_to_server(const Request& request,
                                    util::UniqueFunction<void(const Response&)>&& completion) override
        {
            if (request.url.find("/login") != std::string::npos) {
                CHECK(state.get() == TestState::location);
                state.advance_to(TestState::login);
                mock_transport_worker.add_work_item(
                    Response{200, 0, {}, user_json(encode_fake_jwt("access token 1")).dump()}, std::move(completion));
            }
            else if (request.url.find("/profile") != std::string::npos) {
                // simulated bad token request
                auto cur_state = state.get();
                CHECK((cur_state == TestState::refresh_1 || cur_state == TestState::login));
                if (cur_state == TestState::refresh_1) {
                    state.advance_to(TestState::profile_2);
                    mock_transport_worker.add_work_item(Response{200, 0, {}, user_profile_json().dump()},
                                                        std::move(completion));
                }
                else if (cur_state == TestState::login) {
                    state.advance_to(TestState::profile_1);
                    mock_transport_worker.add_work_item(Response{401, 0, {}}, std::move(completion));
                }
            }
            else if (request.url.find("/session") != std::string::npos && request.method == HttpMethod::post) {
                if (state.get() == TestState::profile_1) {
                    state.advance_to(TestState::refresh_1);
                    nlohmann::json json{{"access_token", encode_fake_jwt("access token 1")}};
                    mock_transport_worker.add_work_item(Response{200, 0, {}, json.dump()}, std::move(completion));
                }
                else if (state.get() == TestState::profile_2) {
                    state.advance_to(TestState::refresh_2);
                    mock_transport_worker.add_work_item(Response{200, 0, {}, "{\"error\":\"too bad, buddy!\"}"},
                                                        std::move(completion));
                }
                else {
                    CHECK(state.get() == TestState::refresh_2);
                    state.advance_to(TestState::refresh_3);
                    nlohmann::json json{{"access_token", encode_fake_jwt("access token 2")}};
                    mock_transport_worker.add_work_item(Response{200, 0, {}, json.dump()}, std::move(completion));
                }
            }
            else if (request.url.find("/location") != std::string::npos) {
                CHECK(request.method == HttpMethod::get);
                CHECK(state.get() == TestState::unknown);
                state.advance_to(TestState::location);
                mock_transport_worker.add_work_item(
                    Response{200,
                             0,
                             {},
                             "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":"
                             "\"http://localhost:9090\",\"ws_hostname\":\"ws://localhost:9090\"}"},
                    std::move(completion));
            }
        }

        AsyncMockNetworkTransport& mock_transport_worker;
        TestStateBundle& state;
    };
    TestSyncManager sync_manager(get_config(std::make_shared<transport>(mock_transport_worker, state)));
    auto app = sync_manager.app();

    {
        auto [cur_user_promise, cur_user_future] = util::make_promise_future<std::shared_ptr<SyncUser>>();
        app->log_in_with_credentials(AppCredentials::anonymous(),
                                     [promise = std::move(cur_user_promise)](std::shared_ptr<SyncUser> user,
                                                                             util::Optional<AppError> error) mutable {
                                         REQUIRE_FALSE(error);
                                         promise.emplace_value(std::move(user));
                                     });

        auto cur_user = std::move(cur_user_future).get();
        CHECK(cur_user);

        SyncTestFile config(app->current_user(), bson::Bson("foo"));
        // Ignore websocket errors, since sometimes a websocket connection gets started during the test
        config.sync_config->error_handler = [](std::shared_ptr<SyncSession> session, SyncError error) mutable {
            // Ignore these errors, since there's not really an app out there...
            // Primarily make sure we don't crash unexpectedly
            std::vector<const char*> expected_errors = {"Bad WebSocket", "Connection Failed", "user has been removed",
                                                        "Connection refused", "The user is not logged in"};
            auto expected =
                std::find_if(expected_errors.begin(), expected_errors.end(), [error](const char* err_msg) {
                    return error.status.reason().find(err_msg) != std::string::npos;
                });
            if (expected != expected_errors.end()) {
                util::format(std::cerr,
                             "An expected possible WebSocket error was caught during test: 'app destroyed during "
                             "token refresh': '%1' for '%2'",
                             error.status, session->path());
            }
            else {
                std::string err_msg(util::format("An unexpected sync error was caught during test: 'app destroyed "
                                                 "during token refresh': '%1' for '%2'",
                                                 error.status, session->path()));
                std::cerr << err_msg << std::endl;
                throw std::runtime_error(err_msg);
            }
        };
        auto r = Realm::get_shared_realm(config);
        auto session = r->sync_session();
        mock_transport_worker.add_work_item([session] {
            session->initiate_access_token_refresh();
        });
    }
    for (const auto& user : app->all_users()) {
        user->log_out();
    }

    timed_wait_for([&] {
        return !app->sync_manager()->has_existing_sessions();
    });

    mock_transport_worker.mark_complete();
}

TEST_CASE("app: metadata is persisted between sessions", "[sync][app][metadata]") {
    static const auto test_hostname = "proto://host:1234";
    static const auto test_ws_hostname = "wsproto://host:1234";

    struct transport : UnitTestTransport {
        void send_request_to_server(const Request& request,
                                    util::UniqueFunction<void(const Response&)>&& completion) override
        {
            if (request.url.find("/location") != std::string::npos) {
                CHECK(request.method == HttpMethod::get);
                completion({200,
                            0,
                            {},
                            nlohmann::json({{"deployment_model", "LOCAL"},
                                            {"location", "IE"},
                                            {"hostname", test_hostname},
                                            {"ws_hostname", test_ws_hostname}})
                                .dump()});
            }
            else if (request.url.find("functions/call") != std::string::npos) {
                REQUIRE(request.url.rfind(test_hostname, 0) != std::string::npos);
            }
            else {
                UnitTestTransport::send_request_to_server(request, std::move(completion));
            }
        }
    };

    TestSyncManager::Config config = get_config(instance_of<transport>);
    config.base_path = util::make_temp_dir();
    config.should_teardown_test_directory = false;
    config.metadata_mode = SyncManager::MetadataMode::NoEncryption;

    {
        TestSyncManager sync_manager(config, {});
        auto app = sync_manager.app();
        app->log_in_with_credentials(AppCredentials::anonymous(), [](auto, auto error) {
            REQUIRE_FALSE(error);
        });
        REQUIRE(app->sync_manager()->sync_route().rfind(test_ws_hostname, 0) != std::string::npos);
    }

    App::clear_cached_apps();
    config.override_sync_route = false;
    config.should_teardown_test_directory = true;
    {
        TestSyncManager sync_manager(config);
        auto app = sync_manager.app();
        REQUIRE(app->sync_manager()->sync_route().rfind(test_ws_hostname, 0) != std::string::npos);
        app->call_function("function", {}, [](auto error, auto) {
            REQUIRE_FALSE(error);
        });
    }
}
