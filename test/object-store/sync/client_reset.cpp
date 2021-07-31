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

#include <catch2/catch.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_database.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/sync_session.hpp>

#include "sync/sync_test_utils.hpp"
#include "util/baas_admin_api.hpp"
#include "util/event_loop.hpp"
#include "util/test_utils.hpp"
#include "util/test_file.hpp"

#include <iostream>

#if REALM_ENABLE_AUTH_TESTS
namespace realm {

TEST_CASE("app: client reset integration", "[sync][app][client reset]") {
    std::unique_ptr<app::GenericNetworkTransport> (*factory)() = [] {
        return std::unique_ptr<app::GenericNetworkTransport>(new IntTestTransport);
    };
    std::string base_url = get_base_url();
    const std::string valid_pk_name = "_id";
    REQUIRE(!base_url.empty());
    const std::string partition = "foo";

    Schema schema = {{"source",
                      {
                          {valid_pk_name, PropertyType::ObjectId | PropertyType::Nullable, true},
                          {"source_int", PropertyType::Int},
                          {"realm_id", PropertyType::String | PropertyType::Nullable},
                      }},
                     {"dest",
                      {
                          {valid_pk_name, PropertyType::ObjectId | PropertyType::Nullable, true},
                          {"dest_int", PropertyType::Int},
                          {"realm_id", PropertyType::String | PropertyType::Nullable},
                      }}};

    AppCreateConfig app_create_config = default_app_config(base_url);
    app_create_config.schema = schema;
    auto app_session = create_app(app_create_config);

    auto app_config = app::App::Config{app_session.client_app_id,
                                       factory,
                                       base_url,
                                       util::none,
                                       util::Optional<std::string>("A Local App Version"),
                                       util::none,
                                       "Object Store Platform Tests",
                                       "Object Store Platform Version Blah",
                                       "An sdk version"};

    auto base_path = util::make_temp_dir() + app_config.app_id;
    util::try_remove_dir_recursive(base_path);
    util::try_make_dir(base_path);

    auto setup_and_get_config = [&base_path, &schema, &partition](std::shared_ptr<app::App> app,
                                                                  std::string local_path) -> realm::Realm::Config {
        realm::Realm::Config config;
        config.sync_config = std::make_shared<realm::SyncConfig>(app->current_user(), bson::Bson(partition));
        config.sync_config->client_resync_mode = ClientResyncMode::Manual;
        config.sync_config->error_handler = [](std::shared_ptr<SyncSession>, SyncError error) {
            std::cerr << error.message << std::endl;
        };
        config.schema_version = 1;
        config.path = base_path + "/" + local_path;
        config.schema = schema;
        return config;
    };

    //    auto get_source_objects = [&](realm::SharedRealm r, std::shared_ptr<SyncSession> session) -> Results {
    //        wait_for_sync_changes(session);
    //        return realm::Results(r, r->read_group().get_table("class_source"));
    //    };
    //    CppContext c;
    //    int64_t counter = 0;
    //    auto create_one_source_object = [&](realm::SharedRealm r) {
    //        r->begin_transaction();
    //        auto object = Object::create(c, r, "source",
    //                                     util::Any(realm::AnyDict{{valid_pk_name, util::Any(ObjectId::gen())},
    //                                                              {"source_int", counter++},
    //                                                              {"realm_id", std::string(partition)}}),
    //                                     CreatePolicy::ForceCreate);
    //
    //        r->commit_transaction();
    //    };
    //
    //    auto create_one_dest_object = [&](realm::SharedRealm r) -> ObjLink {
    //        r->begin_transaction();
    //        auto obj = Object::create(c, r, "dest",
    //                                  util::Any(realm::AnyDict{{valid_pk_name, util::Any(ObjectId::gen())},
    //                                                           {"dest_int", counter++},
    //                                                           {"realm_id", std::string(partition)}}),
    //                                  CreatePolicy::ForceCreate);
    //        r->commit_transaction();
    //        return ObjLink{obj.obj().get_table()->get_key(), obj.obj().get_key()};
    //    };
    //
    //    auto require_links_to_match_ids = [&](std::vector<Obj> links, std::vector<int64_t> expected) {
    //        std::vector<int64_t> actual;
    //        for (auto obj : links) {
    //            actual.push_back(obj.get<Int>("dest_int"));
    //        }
    //        std::sort(actual.begin(), actual.end());
    //        std::sort(expected.begin(), expected.end());
    //        REQUIRE(actual == expected);
    //    };


    SECTION("manual client reset should trigger the error callback") {
        TestSyncManager sync_manager(TestSyncManager::Config(app_config), {});
        auto app = sync_manager.app();

        create_user_and_login(app);
        auto config = setup_and_get_config(app, "r1.realm");
        config.sync_config->client_resync_mode = ClientResyncMode::Manual;
        std::atomic<bool> called{false};
        config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
            REQUIRE(error.is_client_reset_requested());
            called = true;
        };
        auto r1 = realm::Realm::get_shared_realm(config);
        auto session1 = app->current_user()->session_for_on_disk_path(r1->config().path);

        REQUIRE(app_session.admin_api.is_sync_enabled(app_session.server_app_id));
        app_session.admin_api.disable_sync(app_session.server_app_id);
        REQUIRE(!app_session.admin_api.is_sync_enabled(app_session.server_app_id));
        app_session.admin_api.enable_sync(app_session.server_app_id);
        REQUIRE(app_session.admin_api.is_sync_enabled(app_session.server_app_id));

        util::EventLoop::main().run_until([&] {
            return called.load();
        });
    }
}

} // namespace realm

#endif // REALM_ENABLE_AUTH_TESTS
