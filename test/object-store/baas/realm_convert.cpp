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

#include <util/sync_test_utils.hpp>
#include <util/baas_admin_api.hpp>

#include <catch2/catch_all.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/sync/app.hpp>

using namespace realm;

TEST_CASE("app: upgrade from local to synced realm", "[sync][pbs][app][baas][convert]") {
    std::string base_url = get_base_url();
    const std::string valid_pk_name = "_id";
    REQUIRE(!base_url.empty());

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
    auto server_app_config = minimal_app_config(base_url, "upgrade_from_local", schema);
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

TEST_CASE("app: make distributable client file", "[sync][pbs][app][baas][convert]") {
    TestAppSession session;
    auto app = session.app();

    auto schema = default_app_config("").schema;
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

        // Make a copy of the source realm using the target config
        // realm instance still points to the original Realm and a copy is
        // located at the path in target_config
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
        // Open the copied realm (before adding the Dog object)
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

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC
