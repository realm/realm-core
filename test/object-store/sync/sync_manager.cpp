////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
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

#include <util/event_loop.hpp>
#include <util/test_path.hpp>
#include <util/test_utils.hpp>
#include <util/sync/session_util.hpp>
#include <util/sync/sync_test_utils.hpp>

#include <realm/object-store/property.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_user.hpp>

#include <realm/sync/config.hpp>

#include <realm/util/logger.hpp>
#include <realm/util/optional.hpp>
#include <realm/util/scope_exit.hpp>

using namespace realm;
using namespace realm::util;
using File = realm::util::File;
using MetadataMode = app::AppConfig::MetadataMode;

static const auto base_path =
    fs::path{util::make_temp_dir()}.make_preferred() / "realm_objectstore_sync_manager.test-dir";
static const std::string dummy_device_id = "123400000000000000000000";

#if REALM_APP_SERVICES
TEST_CASE("App: path_for_realm API", "[sync][app][file]") {
    const std::string raw_url = "realms://realm.example.org/a/b/~/123456/xyz";

    SECTION("should work properly") {
        OfflineAppSession oas;
        auto user = oas.make_user();
        auto base_path =
            fs::path{oas.base_file_path()}.make_preferred() / "mongodb-realm" / "app_id" / user->user_id();
        const auto expected = base_path / "realms%3A%2F%2Frealm.example.org%2Fa%2Fb%2F%7E%2F123456%2Fxyz.realm";
        SyncConfig sync_config(user, bson::Bson{});
        REQUIRE(oas.app()->path_for_realm(sync_config, raw_url) == expected);
        // This API should also generate the directory if it doesn't already exist.
        REQUIRE_DIR_PATH_EXISTS(base_path);
    }

    SECTION("should produce the expected path for all partition key types") {
        OfflineAppSession oas;
        auto user = oas.make_user();
        auto base_path =
            fs::path{oas.base_file_path()}.make_preferred() / "mongodb-realm" / "app_id" / user->user_id();
        auto app = oas.app();
        // Directory should not be created until we get the path
        REQUIRE_DIR_PATH_DOES_NOT_EXIST(base_path);

        SECTION("string") {
            const bson::Bson partition("string-partition-value&^#");
            SyncConfig config(user, partition);
            REQUIRE(app->path_for_realm(config) == base_path / "s_string-partition-value%26%5E%23.realm");
        }

        SECTION("string which exceeds the file system path length limit") {
            const std::string name_too_long(500, 'b');
            REQUIRE(name_too_long.length() == 500);
            const bson::Bson partition(name_too_long);
            SyncConfig config(user, partition);

            // Note: does not include `identity` as that's in the hashed part
            auto base_path = fs::path{oas.base_file_path()}.make_preferred() / "mongodb-realm" / "app_id";
            const std::string expected_suffix = ".realm";
            std::string actual = oas.app()->path_for_realm(config);
            size_t expected_length = base_path.string().length() + 1 + 64 + expected_suffix.length();
            REQUIRE(actual.length() == expected_length);
            REQUIRE(StringData(actual).begins_with(base_path.string()));
            REQUIRE(StringData(actual).ends_with(expected_suffix));
        }

        SECTION("int32") {
            const bson::Bson partition(int32_t(-25));
            SyncConfig config(user, partition);
            REQUIRE(app->path_for_realm(config) == base_path / "i_-25.realm");
        }

        SECTION("int64") {
            const bson::Bson partition(int64_t(1.15e18)); // > 32 bits
            SyncConfig config(user, partition);
            REQUIRE(app->path_for_realm(config) == base_path / "l_1150000000000000000.realm");
        }

        SECTION("UUID") {
            const bson::Bson partition(UUID("3b241101-e2bb-4255-8caf-4136c566a961"));
            SyncConfig config(user, partition);
            REQUIRE(app->path_for_realm(config) == base_path / "u_3b241101-e2bb-4255-8caf-4136c566a961.realm");
        }

        SECTION("ObjectId") {
            const bson::Bson partition(ObjectId("0123456789abcdefffffffff"));
            SyncConfig config(user, partition);
            REQUIRE(app->path_for_realm(config) == base_path / "o_0123456789abcdefffffffff.realm");
        }

        SECTION("Null") {
            const bson::Bson partition;
            REQUIRE(partition.type() == bson::Bson::Type::Null);
            SyncConfig config(user, partition);
            REQUIRE(app->path_for_realm(config) == base_path / "null.realm");
        }

        SECTION("Flexible sync") {
            SyncConfig config(user, SyncConfig::FLXSyncEnabled{});
            REQUIRE(app->path_for_realm(config) == base_path / "flx_sync_default.realm");
        }

        SECTION("Custom filename for Flexible Sync") {
            SyncConfig config(user, SyncConfig::FLXSyncEnabled{});
            REQUIRE(app->path_for_realm(config, util::make_optional<std::string>("custom")) ==
                    base_path / "custom.realm");
        }

        SECTION("Custom filename with type will still append .realm") {
            SyncConfig config(user, SyncConfig::FLXSyncEnabled{});
            REQUIRE(app->path_for_realm(config, util::make_optional<std::string>("custom.foo")) ==
                    base_path / "custom.foo.realm");
        }

        SECTION("Custom filename for Flexible Sync including .realm") {
            SyncConfig config(user, SyncConfig::FLXSyncEnabled{});
            REQUIRE(app->path_for_realm(config, util::make_optional<std::string>("custom.realm")) ==
                    base_path / "custom.realm");
        }

        SECTION("Custom filename for Flexible Sync with an existing path") {
            SyncConfig config(user, SyncConfig::FLXSyncEnabled{});
            std::string path = app->path_for_realm(config, util::make_optional<std::string>("custom.realm"));
            realm::test_util::TestPathGuard guard(path);
            realm::util::File(path, File::mode_Write).write(0, "test");
            REQUIRE(app->path_for_realm(config, util::make_optional<std::string>("custom.realm")) ==
                    base_path / "custom.realm");
        }

        // Should now exist after getting the path
        REQUIRE_DIR_PATH_EXISTS(base_path);
    }
}

#endif // REALM_APP_SERVICES

TEST_CASE("SyncManager: set_session_multiplexing", "[sync][sync manager]") {
    TestSyncManager::Config tsm_config;
    tsm_config.start_sync_client = false;
    TestSyncManager tsm(tsm_config);
    bool sync_multiplexing_allowed = GENERATE(true, false);
    auto sync_manager = tsm.sync_manager();
    sync_manager->set_session_multiplexing(sync_multiplexing_allowed);

    auto user_1 = tsm.fake_user("user-name-1");
    auto user_2 = tsm.fake_user("user-name-2");

    SyncTestFile file_1(user_1, "partition1", util::none);
    SyncTestFile file_2(user_1, "partition2", util::none);
    SyncTestFile file_3(user_2, "partition3", util::none);

    auto realm_1 = Realm::get_shared_realm(file_1);
    auto realm_2 = Realm::get_shared_realm(file_2);
    auto realm_3 = Realm::get_shared_realm(file_3);

    wait_for_download(*realm_1);
    wait_for_download(*realm_2);
    wait_for_download(*realm_3);

    if (sync_multiplexing_allowed) {
        REQUIRE(conn_id_for_realm(realm_1) == conn_id_for_realm(realm_2));
        REQUIRE(conn_id_for_realm(realm_2) != conn_id_for_realm(realm_3));
    }
    else {
        REQUIRE(conn_id_for_realm(realm_1) != conn_id_for_realm(realm_2));
        REQUIRE(conn_id_for_realm(realm_2) != conn_id_for_realm(realm_3));
        REQUIRE(conn_id_for_realm(realm_1) != conn_id_for_realm(realm_3));
    }
}

TEST_CASE("SyncManager: has_existing_sessions", "[sync][sync manager][active sessions]") {
    TestSyncManager tsm({}, {false});
    auto sync_manager = tsm.sync_manager();

    SECTION("no active sessions") {
        REQUIRE(!sync_manager->has_existing_sessions());
    }

    auto schema = Schema{
        {"object",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
         }},
    };

    std::atomic<bool> error_handler_invoked(false);
    Realm::Config config;
    auto user = tsm.fake_user("user-name");
    auto create_session = [&](SyncSessionStopPolicy stop_policy) {
        std::shared_ptr<SyncSession> session = sync_session(
            user, "/test-dying-state",
            [&](auto, auto) {
                error_handler_invoked = true;
            },
            stop_policy, nullptr, schema, &config);
        EventLoop::main().run_until([&] {
            return sessions_are_active(*session);
        });
        return session;
    };

    SECTION("active sessions") {
        {
            auto session = create_session(SyncSessionStopPolicy::Immediately);
            REQUIRE(sync_manager->has_existing_sessions());
            session->close();
        }
        EventLoop::main().run_until([&] {
            return !sync_manager->has_existing_sessions();
        });
        REQUIRE(!sync_manager->has_existing_sessions());
    }
}
