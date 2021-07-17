////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
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

#include "collection_fixtures.hpp"
#include "sync/session/session_util.hpp"

#include <realm/object-store/feature_checks.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/schema.hpp>

#include "util/event_loop.hpp"
#include "util/index_helpers.hpp"
#include "util/test_utils.hpp"

#include <realm/util/time.hpp>
#include <realm/util/scope_exit.hpp>

#include <atomic>
#include <chrono>
#include <fstream>
#ifndef _WIN32
#include <unistd.h>
#endif

using namespace realm;
using namespace realm::util;

static const std::string dummy_auth_url = "https://realm.example.org";
static const std::string dummy_device_id = "123400000000000000000000";

TEST_CASE("SyncSession: management by SyncUser", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    TestSyncManager init_sync_manager;
    auto& server = init_sync_manager.sync_server();
    auto app = init_sync_manager.app();
    const std::string realm_base_url = server.base_url();

    SECTION("a SyncUser can properly retrieve its owned sessions") {
        std::string path_1;
        std::string path_2;
        auto user =
            app->sync_manager()->get_user("user1a", ENCODE_FAKE_JWT("fake_refresh_token"),
                                          ENCODE_FAKE_JWT("fake_access_token"), dummy_auth_url, dummy_device_id);
        auto session1 = sync_session(
            user, "/test1a-1", [](auto, auto) {}, SyncSessionStopPolicy::AfterChangesUploaded, &path_1);
        auto session2 = sync_session(
            user, "/test1a-2", [](auto, auto) {}, SyncSessionStopPolicy::AfterChangesUploaded, &path_2);
        EventLoop::main().run_until([&] {
            return sessions_are_active(*session1, *session2);
        });

        // Check the sessions on the SyncUser.
        REQUIRE(user->all_sessions().size() == 2);
        auto s1 = user->session_for_on_disk_path(path_1);
        REQUIRE(s1);
        CHECK(s1->config().partition_value == "/test1a-1");
        auto s2 = user->session_for_on_disk_path(path_2);
        REQUIRE(s2);
        CHECK(s2->config().partition_value == "/test1a-2");
    }

    SECTION("a SyncUser properly unbinds its sessions upon logging out") {
        auto user =
            app->sync_manager()->get_user("user1b", ENCODE_FAKE_JWT("fake_refresh_token"),
                                          ENCODE_FAKE_JWT("fake_access_token"), dummy_auth_url, dummy_device_id);
        auto session1 = sync_session(user, "/test1b-1", [](auto, auto) {});
        auto session2 = sync_session(user, "/test1b-2", [](auto, auto) {});
        EventLoop::main().run_until([&] {
            return sessions_are_active(*session1, *session2);
        });

        // Log the user out.
        user->log_out();
        // The sessions should log themselves out.
        EventLoop::main().run_until([&] {
            return sessions_are_inactive(*session1, *session2);
        });
        CHECK(user->all_sessions().size() == 0);
    }

    SECTION("a SyncUser defers binding new sessions until it is logged in") {
        const std::string user_id = "user1c";
        auto user =
            app->sync_manager()->get_user(user_id, ENCODE_FAKE_JWT("fake_refresh_token"),
                                          ENCODE_FAKE_JWT("fake_access_token"), dummy_auth_url, dummy_device_id);
        user->log_out();
        REQUIRE(user->state() == SyncUser::State::LoggedOut);
        auto session1 = sync_session(user, "/test1c-1", [](auto, auto) {});
        auto session2 = sync_session(user, "/test1c-2", [](auto, auto) {});
        // Run the runloop many iterations to see if the sessions spuriously bind.
        spin_runloop();
        REQUIRE(sessions_are_inactive(*session1));
        REQUIRE(sessions_are_inactive(*session2));
        REQUIRE(user->all_sessions().size() == 0);
        // Log the user back in via the sync manager.
        user = app->sync_manager()->get_user(user_id, ENCODE_FAKE_JWT("fake_refresh_token"),
                                             ENCODE_FAKE_JWT("fake_access_token"), dummy_auth_url, dummy_device_id);
        EventLoop::main().run_until([&] {
            return sessions_are_active(*session1, *session2);
        });
        REQUIRE(user->all_sessions().size() == 2);
    }

    SECTION("a SyncUser properly rebinds existing sessions upon logging back in") {
        const std::string user_id = "user1d";
        auto user =
            app->sync_manager()->get_user(user_id, ENCODE_FAKE_JWT("fake_refresh_token"),
                                          ENCODE_FAKE_JWT("fake_access_token"), dummy_auth_url, dummy_device_id);
        auto session1 = sync_session(user, "/test1d-1", [](auto, auto) {});
        auto session2 = sync_session(user, "/test1d-2", [](auto, auto) {});
        // Make sure the sessions are bound.
        EventLoop::main().run_until([&] {
            return sessions_are_active(*session1, *session2);
        });
        REQUIRE(user->all_sessions().size() == 2);
        // Log the user out.
        user->log_out();
        REQUIRE(user->state() == SyncUser::State::LoggedOut);
        // Run the runloop many iterations to see if the sessions spuriously rebind.
        spin_runloop();
        REQUIRE(sessions_are_inactive(*session1));
        REQUIRE(sessions_are_inactive(*session2));
        REQUIRE(user->all_sessions().size() == 0);
        // Log the user back in via the sync manager.
        user = app->sync_manager()->get_user(user_id, ENCODE_FAKE_JWT("fake_refresh_token"),
                                             ENCODE_FAKE_JWT("fake_access_token"), dummy_auth_url, dummy_device_id);
        EventLoop::main().run_until([&] {
            return sessions_are_active(*session1, *session2);
        });
        REQUIRE(user->all_sessions().size() == 2);
    }

    SECTION("sessions that were destroyed can be properly recreated when requested again") {
        const std::string path = "/test1e";
        std::weak_ptr<SyncSession> weak_session;
        std::string on_disk_path;
        util::Optional<SyncConfig> config;
        auto user =
            app->sync_manager()->get_user("user1e", ENCODE_FAKE_JWT("fake_refresh_token"),
                                          ENCODE_FAKE_JWT("fake_access_token"), dummy_auth_url, dummy_device_id);
        {
            // Create the session within a nested scope, so we can control its lifetime.
            auto session = sync_session(
                user, path, [](auto, auto) {}, SyncSessionStopPolicy::Immediately, &on_disk_path);
            weak_session = session;
            config = session->config();
            REQUIRE(on_disk_path.size() > 0);
            REQUIRE(weak_session.lock());
        }
        // Wait for the session to die. It may not happen immediately if a progress or error handler
        // is called on a background thread and keeps the session alive past the scope of the above block.
        EventLoop::main().run_until([&] {
            return weak_session.expired();
        });

        // The next time we request it, it'll be created anew.
        // The call to `get_session()` should result in `SyncUser::register_session()` being called.
        auto session = app->sync_manager()->get_session(on_disk_path, *config);
        CHECK(session);
        session = user->session_for_on_disk_path(on_disk_path);
        CHECK(session);
    }

    SECTION("a user can create multiple sessions for the same URL") {
        auto user =
            app->sync_manager()->get_user("user", ENCODE_FAKE_JWT("fake_refresh_token"),
                                          ENCODE_FAKE_JWT("fake_access_token"), dummy_auth_url, dummy_device_id);
        auto create_session = [&]() {
            // Note that this should put the sessions at different paths.
            return sync_session(
                user, "/test", [](auto, auto) {}, SyncSessionStopPolicy::Immediately);
        };
        REQUIRE(create_session());
        REQUIRE(create_session());
    }
}

TEST_CASE("sync: log-in", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    // Disable file-related functionality and metadata functionality for testing purposes.
    TestSyncManager init_sync_manager;
    auto app = init_sync_manager.app();
    auto user = app->sync_manager()->get_user("user", ENCODE_FAKE_JWT("fake_refresh_token"),
                                              ENCODE_FAKE_JWT("fake_access_token"), dummy_auth_url, dummy_device_id);

    SECTION("Can log in") {
        std::atomic<int> error_count(0);
        auto session = sync_session(user, "/test", [&](auto, auto) {
            ++error_count;
        });

        std::atomic<bool> download_did_complete(false);
        session->wait_for_download_completion([&](auto) {
            download_did_complete = true;
        });
        EventLoop::main().run_until([&] {
            return download_did_complete.load() || error_count > 0;
        });
        CHECK(error_count == 0);
    }

    // TODO: write a test that logs out a Realm with multiple sessions, then logs it back in?
    // TODO: write tests that check that a Session properly handles various types of errors reported via its callback.
}

TEST_CASE("SyncSession: close() API", "[sync]") {
    TestSyncManager init_sync_manager;
    auto app = init_sync_manager.app();
    auto user = app->sync_manager()->get_user("close-api-tests-user", ENCODE_FAKE_JWT("fake_refresh_token"),
                                              ENCODE_FAKE_JWT("fake_access_token"), "https://realm.example.org",
                                              dummy_device_id);

    SECTION("Behaves properly when called on session in the 'active' or 'inactive' state") {
        auto session = sync_session(
            user, "/test-close-for-active", [](auto, auto) {}, SyncSessionStopPolicy::AfterChangesUploaded);
        EventLoop::main().run_until([&] {
            return sessions_are_active(*session);
        });
        REQUIRE(sessions_are_active(*session));
        session->close();
        EventLoop::main().run_until([&] {
            return sessions_are_inactive(*session);
        });
        REQUIRE(sessions_are_inactive(*session));
        // Try closing the session again. This should be a no-op.
        session->close();
        REQUIRE(sessions_are_inactive(*session));
    }
}

TEST_CASE("SyncSession: shutdown_and_wait() API", "[sync]") {
    TestSyncManager init_sync_manager;
    auto app = init_sync_manager.app();
    auto user = app->sync_manager()->get_user("close-api-tests-user", ENCODE_FAKE_JWT("fake_refresh_token"),
                                              ENCODE_FAKE_JWT("fake_access_token"), "https://realm.example.org",
                                              dummy_device_id);

    SECTION("Behaves properly when called on session in the 'active' or 'inactive' state") {
        auto session = sync_session(
            user, "/test-close-for-active", [](auto, auto) {}, SyncSessionStopPolicy::AfterChangesUploaded);
        EventLoop::main().run_until([&] {
            return sessions_are_active(*session);
        });
        REQUIRE(sessions_are_active(*session));
        session->shutdown_and_wait();
        session->close();
        EventLoop::main().run_until([&] {
            return sessions_are_inactive(*session);
        });
        REQUIRE(sessions_are_inactive(*session));
        // Try closing the session again. This should be a no-op.
        session->close();
        REQUIRE(sessions_are_inactive(*session));
    }
}

TEST_CASE("SyncSession: update_configuration()", "[sync]") {
    TestSyncManager init_sync_manager({}, {false});
    auto app = init_sync_manager.app();
    auto user = app->sync_manager()->get_user("userid", ENCODE_FAKE_JWT("fake_refresh_token"),
                                              ENCODE_FAKE_JWT("fake_access_token"), dummy_auth_url, dummy_device_id);
    auto session = sync_session(
        user, "/update_configuration", [](auto, auto) {}, SyncSessionStopPolicy::AfterChangesUploaded);

    SECTION("updates reported configuration") {
        auto config = session->config();
        REQUIRE(config.client_validate_ssl);
        config.client_validate_ssl = false;
        session->update_configuration(std::move(config));
        REQUIRE_FALSE(session->config().client_validate_ssl);
    }

    SECTION("handles reconnects while it's trying to deactivate session") {
        bool wait_called = false;
        session->wait_for_download_completion([&](std::error_code ec) {
            REQUIRE(ec == util::error::operation_aborted);
            REQUIRE(session->config().client_validate_ssl);
            REQUIRE(session->state() == SyncSession::PublicState::Inactive);

            wait_called = true;
            session->revive_if_needed();

            REQUIRE(session->state() != SyncSession::PublicState::Inactive);
        });

        auto config = session->config();
        config.client_validate_ssl = false;
        session->update_configuration(std::move(config));
        REQUIRE(wait_called);
    }
}

TEST_CASE("sync: error handling", "[sync]") {
    using ProtocolError = realm::sync::ProtocolError;
    TestSyncManager init_sync_manager;
    auto app = init_sync_manager.app();
    // Create a valid session.
    std::function<void(std::shared_ptr<SyncSession>, SyncError)> error_handler = [](auto, auto) {};
    const std::string user_id = "user1d";
    std::string on_disk_path;
    auto user = app->sync_manager()->get_user(user_id, ENCODE_FAKE_JWT("fake_refresh_token"),
                                              ENCODE_FAKE_JWT("fake_access_token"), "https://realm.example.org",
                                              dummy_device_id);
    auto session = sync_session(
        user, "/test1e",
        [&](auto session, SyncError error) {
            error_handler(std::move(session), std::move(error));
        },
        SyncSessionStopPolicy::AfterChangesUploaded, &on_disk_path);
    // Make sure the sessions are bound.
    EventLoop::main().run_until([&] {
        return sessions_are_active(*session);
    });

    SECTION("Doesn't treat unknown system errors as being fatal") {
        std::error_code code = std::error_code{EBADF, std::generic_category()};
        SyncSession::OnlyForTesting::handle_error(*session, {code, "Not a real error message", false});
        CHECK(!sessions_are_inactive(*session));
    }

    SECTION("Properly handles a client reset error") {
        int code = 0;
        util::Optional<SyncError> final_error;
        error_handler = [&](auto, SyncError error) {
            final_error = std::move(error);
        };

        SECTION("for bad_server_file_ident") {
            code = static_cast<int>(ProtocolError::bad_server_file_ident);
        }

        SECTION("for bad_client_file_ident") {
            code = static_cast<int>(ProtocolError::bad_client_file_ident);
        }

        SECTION("for bad_server_version") {
            code = static_cast<int>(ProtocolError::bad_server_version);
        }

        SECTION("for diverging_histories") {
            code = static_cast<int>(ProtocolError::diverging_histories);
        }

        SyncError initial_error{std::error_code{code, realm::sync::protocol_error_category()},
                                "Something bad happened", false};
        std::time_t just_before_raw = std::time(nullptr);
        SyncSession::OnlyForTesting::handle_error(*session, std::move(initial_error));
        REQUIRE(session->state() == SyncSession::PublicState::Inactive);
        std::time_t just_after_raw = std::time(nullptr);
        auto just_before = util::localtime(just_before_raw);
        auto just_after = util::localtime(just_after_raw);
        // At this point final_error should be populated.
        CHECK(bool(final_error));
        CHECK(final_error->is_client_reset_requested());
        // The original file path should be present.
        CHECK(final_error->user_info[SyncError::c_original_file_path_key] == on_disk_path);
        // The path to the recovery file should be present, and should contain all necessary components.
        std::string recovery_path = final_error->user_info[SyncError::c_recovery_file_path_key];
        auto idx = recovery_path.find("recovered_realm");
        CHECK(idx != std::string::npos);
        idx = recovery_path.find(app->sync_manager()->recovery_directory_path());
        CHECK(idx != std::string::npos);
        if (just_before.tm_year == just_after.tm_year) {
            idx = recovery_path.find(util::format_local_time(just_after_raw, "%Y"));
            CHECK(idx != std::string::npos);
        }
        if (just_before.tm_mon == just_after.tm_mon) {
            idx = recovery_path.find(util::format_local_time(just_after_raw, "%m"));
            CHECK(idx != std::string::npos);
        }
        if (just_before.tm_yday == just_after.tm_yday) {
            idx = recovery_path.find(util::format_local_time(just_after_raw, "%d"));
            CHECK(idx != std::string::npos);
        }
    }
}

struct RegularUser {
    static auto user(std::shared_ptr<SyncManager> sync_manager)
    {
        return sync_manager->get_user("user-dying-state", ENCODE_FAKE_JWT("fake_refresh_token"),
                                      ENCODE_FAKE_JWT("fake_access_token"), dummy_auth_url, dummy_device_id);
    }
};

TEMPLATE_TEST_CASE("sync: stop policy behavior", "[sync]", RegularUser)
{
    using ProtocolError = realm::sync::ProtocolError;
    const std::string dummy_auth_url = "https://realm.example.org";
    if (!EventLoop::has_implementation())
        return;

    // Server is initially stopped so we can control when the session exits the dying state.
    TestSyncManager init_sync_manager({}, {false});
    auto& server = init_sync_manager.sync_server();
    auto sync_manager = init_sync_manager.app()->sync_manager();
    auto schema = Schema{
        {"object",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
         }},
    };

    std::atomic<bool> error_handler_invoked(false);
    Realm::Config config;
    auto user = TestType::user(sync_manager);

    auto create_session = [&](SyncSessionStopPolicy stop_policy) {
        auto session = sync_session(
            user, "/test-dying-state",
            [&](auto, auto) {
                error_handler_invoked = true;
            },
            stop_policy, nullptr, schema, &config);
        EventLoop::main().run_until([&] {
            return sessions_are_active(*session);
        });

        // Add an object so there's something to upload
        auto r = Realm::get_shared_realm(config);
        TableRef table = ObjectStore::table_for_object_type(r->read_group(), "object");
        r->begin_transaction();
        table->create_object_with_primary_key(0);
        r->commit_transaction();

        return session;
    };

    SECTION("Immediately") {
        SECTION("transitions directly to Inactive even with the server stopped") {
            auto session = create_session(SyncSessionStopPolicy::Immediately);
            session->close();
            REQUIRE(sessions_are_inactive(*session));
        }
    }

    SECTION("AfterChangesUploaded") {
        auto session = create_session(SyncSessionStopPolicy::AfterChangesUploaded);
        // Now close the session, causing the state to transition to Dying.
        // (it should remain stuck there until we start the server)
        session->close();
        REQUIRE(session->state() == SyncSession::PublicState::Dying);

        SECTION("transitions to Inactive once the server is started") {
            server.start();
            EventLoop::main().run_until([&] {
                return sessions_are_inactive(*session);
            });
        }

        SECTION("transitions back to Active if the session is revived") {
            auto session2 = sync_manager->get_session(config.path, *config.sync_config);
            REQUIRE(session->state() == SyncSession::PublicState::Active);
            REQUIRE(session2 == session);
        }

        SECTION("transitions to Inactive if a fatal error occurs") {
            std::error_code code =
                std::error_code{static_cast<int>(ProtocolError::bad_syntax), realm::sync::protocol_error_category()};
            SyncSession::OnlyForTesting::handle_error(*session, {code, "Not a real error message", true});
            CHECK(sessions_are_inactive(*session));
            // The session shouldn't report fatal errors when in the dying state.
            CHECK(!error_handler_invoked);
        }

        SECTION("ignores non-fatal errors and does not transition to Inactive") {
            // Fire a simulated *non-fatal* error.
            std::error_code code =
                std::error_code{static_cast<int>(ProtocolError::other_error), realm::sync::protocol_error_category()};
            SyncSession::OnlyForTesting::handle_error(*session, {code, "Not a real error message", false});
            REQUIRE(session->state() == SyncSession::PublicState::Dying);
            CHECK(!error_handler_invoked);
        }
    }

    SECTION("can change to Immediately after opening the session") {
        auto session = create_session(SyncSessionStopPolicy::AfterChangesUploaded);
        REQUIRE(session->state() == SyncSession::PublicState::Active);

        auto config = session->config();
        config.stop_policy = SyncSessionStopPolicy::Immediately;
        session->update_configuration(std::move(config));

        session->close();
        REQUIRE(sessions_are_inactive(*session));
    }
}

TEST_CASE("sync: encrypt local realm file", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    // Disable file-related functionality and metadata functionality for testing purposes.
    TestSyncManager init_sync_manager;
    auto sync_manager = init_sync_manager.app()->sync_manager();
    std::array<char, 64> encryption_key;
    encryption_key.fill(12);

    SECTION("open a session with realm file encryption and then open the same file directly") {
        SyncTestFile config(init_sync_manager.app(), "encrypted_realm");
        std::copy_n(encryption_key.begin(), encryption_key.size(), std::back_inserter(config.encryption_key));
        config.sync_config->realm_encryption_key = encryption_key;

        // open a session and wait for it to fully download to its local realm file
        {
            std::atomic<bool> handler_called(false);
            auto session = sync_manager->get_session(config.path, *config.sync_config);
            EventLoop::main().run_until([&] {
                return sessions_are_active(*session);
            });
            session->wait_for_download_completion([&](auto) {
                handler_called = true;
            });
            EventLoop::main().run_until([&] {
                return handler_called == true;
            });
            session->close();
            EventLoop::main().run_until([&] {
                return sessions_are_inactive(*session);
            });
        }

        // open a Realm with the same config, if the session didn't use the encryption key this should fail
        {
            Realm::get_shared_realm(config);
        }
    }

    SECTION("errors if encryption keys are different") {
        {
            SyncTestFile config(init_sync_manager.app(), "encrypted_realm");
            config.sync_config->realm_encryption_key = encryption_key;

            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }

        {
            SyncTestFile config(init_sync_manager.app(), "encrypted_realm");
            std::copy_n(encryption_key.begin(), encryption_key.size(), std::back_inserter(config.encryption_key));

            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }

        {
            SyncTestFile config(init_sync_manager.app(), "encrypted_realm");
            config.sync_config->realm_encryption_key = encryption_key;
            config.encryption_key.push_back(9);

            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }
    }
}

TEST_CASE("sync: non-synced metadata table doesn't result in non-additive schema changes", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    // Disable file-related functionality and metadata functionality for testing purposes.
    TestSyncManager init_sync_manager;

    // Create a synced Realm containing a class with two properties.
    {
        SyncTestFile config1(init_sync_manager.app(), "schema-version-test");
        config1.schema_version = 1;
        config1.schema = Schema{
            {"object",
             {{"_id", PropertyType::Int, Property::IsPrimary{true}},
              {"property1", PropertyType::Int},
              {"property2", PropertyType::Int}}},
        };

        auto realm1 = Realm::get_shared_realm(config1);
        wait_for_upload(*realm1);
    }

    // Download the existing Realm into a second local file without specifying a schema,
    // mirroring how `openAsync` works.
    SyncTestFile config2(init_sync_manager.app(), "schema-version-test");
    config2.schema_version = 1;
    {
        auto realm2 = Realm::get_shared_realm(config2);
        wait_for_download(*realm2);
    }

    // Open the just-downloaded Realm while specifying a schema that contains a class with
    // only a single property. This should not result in us trying to remove `property2`,
    // and will throw an exception if it does.
    {
        SyncTestFile config3(init_sync_manager.app(), "schema-version-test");
        config3.path = config2.path;
        config3.schema_version = 1;
        config3.schema = Schema{
            {"object", {{"_id", PropertyType::Int, Property::IsPrimary{true}}, {"property1", PropertyType::Int}}},
        };

        auto realm3 = Realm::get_shared_realm(config3);
    }
}


TEST_CASE("sync: stable IDs", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    // Disable file-related functionality and metadata functionality for testing purposes.
    TestSyncManager init_sync_manager;

    SECTION("ID column isn't visible in schema read from Group") {
        SyncTestFile config(init_sync_manager.app(), "schema-test");
        config.schema_version = 1;
        config.schema = Schema{
            {"object", {{"_id", PropertyType::Int, Property::IsPrimary{true}}, {"value", PropertyType::Int}}},
        };

        auto realm = Realm::get_shared_realm(config);

        ObjectSchema object_schema(realm->read_group(), "object", TableKey());
        REQUIRE(object_schema == *config.schema->find("object"));
    }
}

#if 0 // Not possible to open core-5 format realms in read-only mode
TEST_CASE("sync: Migration from Sync 1.x to Sync 2.x", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    // Disable file-related functionality and metadata functionality for testing purposes.
    TestSyncManager init_sync_manager;


    SyncTestFile config(init_sync_manager.sync_server(), "migration-test");
    config.schema_version = 1;

    {
        std::ifstream src("sync-1.x.realm", std::ios::binary);
        REQUIRE(src.good());
        std::ofstream dst(config.path, std::ios::binary);

        dst << src.rdbuf();
    }

    auto check = [&](auto f) {
        bool exception_thrown = false;
        try {
            f();
        } catch (RealmFileException const& ex) {
            REQUIRE(ex.kind() == RealmFileException::Kind::IncompatibleSyncedRealm);
            exception_thrown = true;

            SECTION("We should be able to open and read from the recovered Realm file") {
                Realm::Config config;
                config.path = ex.path();
                config.schema_mode = SchemaMode::Immutable;
                auto recovered_realm = Realm::get_shared_realm(config);

                TableRef table = ObjectStore::table_for_object_type(recovered_realm->read_group(), "object");
                REQUIRE(table);
                REQUIRE(table->size() == 2);
            }

            SECTION("We should be able to successfully open the Realm after the recovery") {
                auto result = f();
                REQUIRE(result);
            }
        }
        REQUIRE(exception_thrown);
    };

    SECTION("Realm::get_shared_realm allows recovery from Sync 1.x to Sync 2.x migration") {
        check([&]{
            return Realm::get_shared_realm(config);
        });
    }

    SECTION("SyncManager::get_session allows recovery from Sync 1.x to Sync 2.x migration") {
        check([&]{
            return SyncManager::shared().get_session(config.path, *config.sync_config);
        });
    }

    SECTION("Realm::get_synchronized_realm allows recovery from Sync 1.x to Sync 2.x migration") {
        check([&]{
            return Realm::get_synchronized_realm(config);
        });
    }
}
#endif

namespace {

TableRef get_table(Realm& realm, StringData object_type)
{
    return ObjectStore::table_for_object_type(realm.read_group(), object_type);
}

Obj create_object(Realm& realm, StringData object_type, util::Optional<int64_t> primary_key = util::none)
{
    auto table = get_table(realm, object_type);
    REQUIRE(table);
    static int64_t pk = 0;
    return table->create_object_with_primary_key(primary_key ? *primary_key : pk++);
}

SharedRealm trigger_client_reset(std::function<void(Realm&)> local, std::function<void(Realm&)> remote,
                                 realm::Realm::Config local_config, realm::Realm::Config remote_config,
                                 TestSyncManager& test_sync_manager)
{
    using namespace std::literals::chrono_literals;
    auto& server = test_sync_manager.sync_server();
    auto sync_manager = test_sync_manager.app()->sync_manager();

    auto realm = Realm::get_shared_realm(local_config);
    auto session = sync_manager->get_session(realm->config().path, *realm->config().sync_config);
    {
        realm->begin_transaction();

        auto obj = create_object(*realm, "object");
        auto col = obj.get_table()->get_column_key("value");
        obj.set(col, 1);
        obj.set(col, 2);
        obj.set(col, 3);
        realm->commit_transaction();

        wait_for_upload(*realm);
        session->log_out();

        // Make a change while offline so that log compaction will cause a
        // client reset
        realm->begin_transaction();
        obj.set(col, 4);
        local(*realm);
        realm->commit_transaction();
    }

    // Make writes from another client while advancing the time so that
    // the server performs log compaction
    {
        auto realm2 = Realm::get_shared_realm(remote_config);

        for (int i = 0; i < 2; ++i) {
            wait_for_download(*realm2);
            realm2->begin_transaction();
            auto table = get_table(*realm2, "object");
            auto col = table->get_column_key("value");
            table->begin()->set(col, i + 5);
            realm2->commit_transaction();
            wait_for_upload(*realm2);
            server.advance_clock(10s);
        }

        realm2->begin_transaction();
        remote(*realm2);
        realm2->commit_transaction();
        wait_for_upload(*realm2);
        server.advance_clock(10s);
        realm2->close();
    }

    // Resuming sync on the first realm should now result in a client reset
    session->revive_if_needed();
    return realm;
}

} // namespace

TEST_CASE("sync: client reset", "[client reset]") {
    if (!EventLoop::has_implementation())
        return;

    TestSyncManager init_sync_manager;
    SyncTestFile config(init_sync_manager.app(), "default");
    config.schema = Schema{
        {"object",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
         }},
        {"link target",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
         }},
        {"pk link target",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
         }},
        {"link origin",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"link", PropertyType::Object | PropertyType::Nullable, "link target"},
             {"pk link", PropertyType::Object | PropertyType::Nullable, "pk link target"},
             {"list", PropertyType::Object | PropertyType::Array, "link target"},
             {"pk list", PropertyType::Object | PropertyType::Array, "pk link target"},
         }},
    };
    SyncTestFile config2(init_sync_manager.app(), "default");

    auto setup = [&](auto fn) {
        auto realm = Realm::get_shared_realm(config);
        realm->begin_transaction();
        fn(*realm);
        realm->commit_transaction();
        wait_for_upload(*realm);
    };

    auto trigger_client_reset = [&](auto local, auto remote) -> std::shared_ptr<Realm> {
        return ::trigger_client_reset(local, remote, config, config2, init_sync_manager);
    };

    SECTION("should trigger error callback when mode is manual") {
        config.sync_config->client_resync_mode = ClientResyncMode::Manual;
        std::atomic<bool> called{false};
        config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
            REQUIRE(error.is_client_reset_requested());
            called = true;
        };

        auto realm = trigger_client_reset([](auto&) {}, [](auto&) {});

        EventLoop::main().run_until([&] {
            return called.load();
        });
    }

    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
    config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError) {
        FAIL("Error handler should not have been called");
    };

    SECTION("should discard local changeset when mode is discard") {
        config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

        auto realm = trigger_client_reset([](auto&) {}, [](auto&) {});
        wait_for_download(*realm);
        REQUIRE_THROWS(realm->refresh());
        CHECK(ObjectStore::table_for_object_type(realm->read_group(), "object")->begin()->get<Int>("value") == 4);
        realm->close();
        SharedRealm r_after;
        REQUIRE_NOTHROW(r_after = Realm::get_shared_realm(config));
        CHECK(ObjectStore::table_for_object_type(r_after->read_group(), "object")->begin()->get<Int>("value") == 6);
    }

    SECTION("should honor encryption key for downloaded Realm") {
        config.encryption_key.resize(64, 'a');
        config.sync_config->realm_encryption_key = std::array<char, 64>();
        config.sync_config->realm_encryption_key->fill('a');
        config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

        auto realm = trigger_client_reset([](auto&) {}, [](auto&) {});
        wait_for_download(*realm);
        realm->close();
        SharedRealm r_after;
        REQUIRE_NOTHROW(r_after = Realm::get_shared_realm(config));
        CHECK(ObjectStore::table_for_object_type(r_after->read_group(), "object")->begin()->get<Int>("value") == 6);
    }

    SECTION("add table in discarded transaction") {
        setup([&](auto& realm) {
            auto table = ObjectStore::table_for_object_type(realm.read_group(), "object2");
            REQUIRE(!table);
        });

        auto realm = trigger_client_reset(
            [&](auto& realm) {
                realm.update_schema(
                    {
                        {"object2",
                         {
                             {"_id", PropertyType::Int, Property::IsPrimary{true}},
                             {"value2", PropertyType::Int},
                         }},
                    },
                    0, nullptr, nullptr, true);
                ::create_object(realm, "object2");
            },
            [](auto&) {});
        wait_for_download(*realm);

        // test local realm that changes were persisted
        REQUIRE_THROWS(realm->refresh());
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object2");
        REQUIRE(table);
        REQUIRE(table->size() == 1);
        // test reset realm that changes were overwritten
        realm = Realm::get_shared_realm(config);
        table = ObjectStore::table_for_object_type(realm->read_group(), "object2");
        REQUIRE(!table);
    }

    SECTION("add column in discarded transaction") {
        auto realm = trigger_client_reset(
            [](auto& realm) {
                realm.update_schema(
                    {
                        {"object",
                         {
                             {"_id", PropertyType::Int, Property::IsPrimary{true}},
                             {"value2", PropertyType::Int},
                         }},
                    },
                    0, nullptr, nullptr, true);
                ObjectStore::table_for_object_type(realm.read_group(), "object")->begin()->set("value2", 123);
            },
            [](auto&) {});
        wait_for_download(*realm);
        // test local realm that changes were persisted
        REQUIRE_THROWS(realm->refresh());
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object");
        REQUIRE(table->get_column_count() == 3);
        REQUIRE(table->begin()->get<Int>("value2") == 123);
        REQUIRE_THROWS(realm->refresh());
        // test resync'd realm that changes were overwritten
        realm = Realm::get_shared_realm(config);
        table = ObjectStore::table_for_object_type(realm->read_group(), "object");
        REQUIRE(table);
        REQUIRE(table->get_column_count() == 2);
        REQUIRE(!bool(table->get_column_key("value2")));
    }

    SECTION("seamless loss") {
        config.cache = false;
        config.automatic_change_notifications = false;
        config.sync_config->client_resync_mode = ClientResyncMode::SeamlessLoss;

        Results results;
        Object object;
        CollectionChangeSet object_changes, results_changes;
        NotificationToken object_token, results_token;
        auto setup_listeners = [&](SharedRealm realm) {
            results = Results(realm, ObjectStore::table_for_object_type(realm->read_group(), "object"))
                          .sort({{{"value", true}}});
            if (results.size() >= 1) {
                REQUIRE(results.get<Obj>(0).get<Int>("value") == 4);

                auto obj = *ObjectStore::table_for_object_type(realm->read_group(), "object")->begin();
                REQUIRE(obj.get<Int>("value") == 4);
                object = Object(realm, obj);
                object_token =
                    object.add_notification_callback([&](CollectionChangeSet changes, std::exception_ptr err) {
                        REQUIRE_FALSE(err);
                        object_changes = std::move(changes);
                    });
            }
            results_token =
                results.add_notification_callback([&](CollectionChangeSet changes, std::exception_ptr err) {
                    REQUIRE_FALSE(err);
                    results_changes = std::move(changes);
                });
        };

        SECTION("modify") {
            auto realm = trigger_client_reset([](auto&) {}, [](auto&) {});
            setup_listeners(realm);

            REQUIRE_NOTHROW(advance_and_notify(*realm));
            CHECK(results.size() == 1);
            CHECK(results.get<Obj>(0).get<Int>("value") == 4);

            wait_for_upload(*realm);
            wait_for_download(*realm);
            REQUIRE_NOTHROW(advance_and_notify(*realm));

            CHECK(results.size() == 1);
            CHECK(results.get<Obj>(0).get<Int>("value") == 6);
            CHECK(object.obj().get<Int>("value") == 6);
            REQUIRE_INDICES(results_changes.modifications, 0);
            REQUIRE_INDICES(results_changes.insertions);
            REQUIRE_INDICES(results_changes.deletions);
            REQUIRE_INDICES(object_changes.modifications, 0);
            REQUIRE_INDICES(object_changes.insertions);
            REQUIRE_INDICES(object_changes.deletions);
        }

        SECTION("delete and insert new") {
            constexpr int64_t new_value = 42;
            auto realm = trigger_client_reset([](auto&) {},
                                              [&](auto& remote) {
                                                  auto table = get_table(remote, "object");
                                                  REQUIRE(table);
                                                  REQUIRE(table->size() == 1);
                                                  table->clear();
                                                  auto obj = create_object(remote, "object");
                                                  auto col = obj.get_table()->get_column_key("value");
                                                  obj.set(col, new_value);
                                              });
            setup_listeners(realm);

            REQUIRE_NOTHROW(advance_and_notify(*realm));
            CHECK(results.size() == 1);
            CHECK(results.get<Obj>(0).get<Int>("value") == 4);

            wait_for_upload(*realm);
            wait_for_download(*realm);
            REQUIRE_NOTHROW(advance_and_notify(*realm));

            CHECK(results.size() == 1);
            CHECK(results.get<Obj>(0).get<Int>("value") == new_value);
            CHECK(!object.is_valid());
            REQUIRE_INDICES(results_changes.modifications);
            REQUIRE_INDICES(results_changes.insertions, 0);
            REQUIRE_INDICES(results_changes.deletions, 0);
            REQUIRE_INDICES(object_changes.modifications);
            REQUIRE_INDICES(object_changes.insertions);
            REQUIRE_INDICES(object_changes.deletions, 0);
        }

        SECTION("delete and insert same pk is reported as modification") {
            constexpr int64_t new_value = 42;
            auto realm = trigger_client_reset([](auto&) {},
                                              [&](auto& remote) {
                                                  auto table = get_table(remote, "object");
                                                  REQUIRE(table);
                                                  REQUIRE(table->size() == 1);
                                                  Mixed orig_pk = table->begin()->get_primary_key();
                                                  table->clear();
                                                  auto obj = create_object(remote, "object", {orig_pk.get_int()});
                                                  REQUIRE(obj.get_primary_key() == orig_pk);
                                                  auto col = obj.get_table()->get_column_key("value");
                                                  obj.set(col, new_value);
                                              });
            setup_listeners(realm);

            REQUIRE_NOTHROW(advance_and_notify(*realm));
            CHECK(results.size() == 1);
            CHECK(results.get<Obj>(0).get<Int>("value") == 4);

            wait_for_upload(*realm);
            wait_for_download(*realm);
            REQUIRE_NOTHROW(advance_and_notify(*realm));

            CHECK(results.size() == 1);
            CHECK(results.get<Obj>(0).get<Int>("value") == new_value);
            CHECK(object.is_valid());
            CHECK(object.obj().get<Int>("value") == new_value);
            REQUIRE_INDICES(results_changes.modifications, 0);
            REQUIRE_INDICES(results_changes.insertions);
            REQUIRE_INDICES(results_changes.deletions);
            REQUIRE_INDICES(object_changes.modifications, 0);
            REQUIRE_INDICES(object_changes.insertions);
            REQUIRE_INDICES(object_changes.deletions);
        }

        SECTION("insert in discarded transaction is deleted") {
            constexpr int64_t new_value = 42;
            auto realm = trigger_client_reset(
                [&](auto& local) {
                    auto table = get_table(local, "object");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    auto obj = create_object(local, "object");
                    auto col = obj.get_table()->get_column_key("value");
                    REQUIRE(table->size() == 2);
                    obj.set(col, new_value);
                },
                [&](auto&) {});
            setup_listeners(realm);

            REQUIRE_NOTHROW(advance_and_notify(*realm));
            CHECK(results.size() == 2);

            wait_for_upload(*realm);
            wait_for_download(*realm);
            REQUIRE_NOTHROW(advance_and_notify(*realm));

            CHECK(results.size() == 1);
            CHECK(results.get<Obj>(0).get<Int>("value") == 6);
            CHECK(object.is_valid());
            CHECK(object.obj().get<Int>("value") == 6);
            REQUIRE_INDICES(results_changes.modifications, 0);
            REQUIRE_INDICES(results_changes.insertions);
            REQUIRE_INDICES(results_changes.deletions, 1);
            REQUIRE_INDICES(object_changes.modifications, 0);
            REQUIRE_INDICES(object_changes.insertions);
            REQUIRE_INDICES(object_changes.deletions);
        }

        SECTION("delete in discarded transaction is recovered") {
            auto realm = trigger_client_reset(
                [&](auto& local) {
                    auto table = get_table(local, "object");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    table->clear();
                    REQUIRE(table->size() == 0);
                },
                [&](auto&) {});
            setup_listeners(realm);

            REQUIRE_NOTHROW(advance_and_notify(*realm));
            CHECK(results.size() == 0);

            wait_for_upload(*realm);
            wait_for_download(*realm);
            REQUIRE_NOTHROW(advance_and_notify(*realm));

            CHECK(results.size() == 1);
            CHECK(results.get<Obj>(0).get<Int>("value") == 6);
            CHECK(!object.is_valid());
            REQUIRE_INDICES(results_changes.modifications);
            REQUIRE_INDICES(results_changes.insertions, 0);
            REQUIRE_INDICES(results_changes.deletions);
        }

        SECTION("extra local table is removed") {
            auto realm = trigger_client_reset(
                [](auto& realm) {
                    realm.update_schema(
                        {
                            {"object2",
                             {
                                 {"_id", PropertyType::Int | PropertyType::Nullable, Property::IsPrimary{true}},
                             }},
                        },
                        0, nullptr, nullptr, true);
                    auto table = ObjectStore::table_for_object_type(realm.read_group(), "object2");
                    table->create_object_with_primary_key(Mixed());
                    table->create_object_with_primary_key(Mixed(1));
                },
                [](auto&) {});
            wait_for_download(*realm);
            REQUIRE_THROWS_CONTAINING(realm->refresh(),
                                      "Unsupported schema changes were made by another client or process");
        }

        SECTION("extra local column is removed") {
            auto realm = trigger_client_reset(
                [](auto& realm) {
                    realm.update_schema(
                        {
                            {"object",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"value2", PropertyType::Int},
                                 {"array", PropertyType::Int | PropertyType::Array},
                                 {"link", PropertyType::Object | PropertyType::Nullable, "object"},
                             }},
                        },
                        0, nullptr, nullptr, true);
                    auto table = ObjectStore::table_for_object_type(realm.read_group(), "object");
                    table->begin()->set(table->get_column_key("value2"), 123);
                },
                [](auto&) {});
            wait_for_download(*realm);
            REQUIRE_THROWS_CONTAINING(realm->refresh(),
                                      "Unsupported schema changes were made by another client or process");
        }

        SECTION("compatible schema changes in both remote and local transactions") {
            auto realm = trigger_client_reset(
                [](auto& realm) {
                    realm.update_schema(
                        {
                            {"object",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"value2", PropertyType::Int},
                             }},
                            {"object2",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"link", PropertyType::Object | PropertyType::Nullable, "object"},
                             }},
                        },
                        0, nullptr, nullptr, true);
                },
                [](auto& realm) {
                    realm.update_schema(
                        {
                            {"object",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"value2", PropertyType::Int},
                             }},
                            {"object2",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"link", PropertyType::Object | PropertyType::Nullable, "object"},
                             }},
                        },
                        0, nullptr, nullptr, true);
                });
            wait_for_download(*realm);
            REQUIRE_NOTHROW(realm->refresh());
            auto table = ObjectStore::table_for_object_type(realm->read_group(), "object2");
            REQUIRE(table->get_column_count() == 2);
            REQUIRE(bool(table->get_column_key("link")));
        }

        SECTION("incompatible schema changes in remote and local transactions") {
            auto realm = trigger_client_reset(
                [](auto& realm) {
                    realm.update_schema(
                        {
                            {"object",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"value2", PropertyType::Float},
                             }},
                        },
                        0, nullptr, nullptr, true);
                },
                [](auto& realm) {
                    realm.update_schema(
                        {
                            {"object",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"value2", PropertyType::Int},
                             }},
                        },
                        0, nullptr, nullptr, true);
                });
            wait_for_download(*realm);
            REQUIRE_THROWS_WITH(
                realm->refresh(),
                Catch::Matchers::Contains("Property 'object.value2' has been changed from 'float' to 'int'"));
        }

        SECTION("list operations") {
            ObjKey k0, k1, k2;
            setup([&](auto& realm) {
                k0 = create_object(realm, "link target").set("value", 1).get_key();
                k1 = create_object(realm, "link target").set("value", 2).get_key();
                k2 = create_object(realm, "link target").set("value", 3).get_key();
                Obj o = create_object(realm, "link origin");
                auto list = o.get_linklist(o.get_table()->get_column_key("list"));
                list.add(k0);
                list.add(k1);
                list.add(k2);
            });
            auto check_links = [&](auto& realm) {
                auto table = get_table(*realm, "link origin");
                REQUIRE(table->size() == 1);
                auto list = table->begin()->get_linklist(table->get_column_key("list"));
                REQUIRE(list.size() == 3);
                REQUIRE(list.get_object(0).template get<Int>("value") == 1);
                REQUIRE(list.get_object(1).template get<Int>("value") == 2);
                REQUIRE(list.get_object(2).template get<Int>("value") == 3);
            };

            SECTION("list insertions in local transaction") {
                auto realm = trigger_client_reset(
                    [&](auto& realm) {
                        auto table = get_table(realm, "link origin");
                        auto list = table->begin()->get_linklist(table->get_column_key("list"));
                        list.add(k0);
                        list.insert(0, k2);
                        list.insert(0, k1);
                    },
                    [](auto&) {});
                wait_for_download(*realm);
                REQUIRE_NOTHROW(realm->refresh());
                check_links(realm);
            }

            SECTION("list deletions in local transaction") {
                auto realm = trigger_client_reset(
                    [&](auto& realm) {
                        auto table = get_table(realm, "link origin");
                        auto list = table->begin()->get_linklist(table->get_column_key("list"));
                        list.remove(1);
                    },
                    [](auto&) {});
                wait_for_download(*realm);
                REQUIRE_NOTHROW(realm->refresh());
                check_links(realm);
            }

            SECTION("list clear in local transaction") {
                auto realm = trigger_client_reset(
                    [&](auto& realm) {
                        auto table = get_table(realm, "link origin");
                        auto list = table->begin()->get_linklist(table->get_column_key("list"));
                        list.clear();
                    },
                    [&](auto&) {});
                wait_for_download(*realm);
                REQUIRE_NOTHROW(realm->refresh());
                check_links(realm);
            }
        }

        SECTION("conflicting primary key creations") {
            auto realm = trigger_client_reset(
                [&](auto& realm) {
                    auto table = get_table(realm, "object");
                    table->clear();
                    table->create_object_with_primary_key(1).set("value", 4);
                    table->create_object_with_primary_key(2).set("value", 5);
                    table->create_object_with_primary_key(3).set("value", 6);
                },
                [&](auto& realm) {
                    auto table = get_table(realm, "object");
                    table->clear();
                    table->create_object_with_primary_key(1).set("value", 4);
                    table->create_object_with_primary_key(2).set("value", 7);
                    table->create_object_with_primary_key(5).set("value", 8);
                });
            setup_listeners(realm);

            REQUIRE_NOTHROW(advance_and_notify(*realm));
            CHECK(results.size() == 3);
            CHECK(results.get<Obj>(0).get<Int>("value") == 4);

            wait_for_upload(*realm);
            wait_for_download(*realm);
            REQUIRE_NOTHROW(advance_and_notify(*realm));

            CHECK(results.size() == 3);
            // here we rely on results being sorted by "value"
            CHECK(results.get<Obj>(0).get<Int>("_id") == 1);
            CHECK(results.get<Obj>(0).get<Int>("value") == 4);
            CHECK(results.get<Obj>(1).get<Int>("_id") == 2);
            CHECK(results.get<Obj>(1).get<Int>("value") == 7);
            CHECK(results.get<Obj>(2).get<Int>("_id") == 5);
            CHECK(results.get<Obj>(2).get<Int>("value") == 8);

            CHECK(object.is_valid());
            REQUIRE_INDICES(results_changes.modifications, 1);
            REQUIRE_INDICES(results_changes.insertions, 2);
            REQUIRE_INDICES(results_changes.deletions, 2);
            REQUIRE_INDICES(object_changes.modifications);
            REQUIRE_INDICES(object_changes.insertions);
            REQUIRE_INDICES(object_changes.deletions);
        }

        auto get_key_for_object_with_value = [&](TableRef table, int64_t value) -> ObjKey {
            REQUIRE(table);
            auto target = std::find_if(table->begin(), table->end(), [&](auto it) -> bool {
                return it.template get<Int>("value") == value;
            });
            if (target == table->end()) {
                return {};
            }
            return target->get_key();
        };

        SECTION("link to remotely deleted object") {
            setup([&](auto& realm) {
                auto k0 = create_object(realm, "link target").set("value", 1).get_key();
                create_object(realm, "link target").set("value", 2);
                create_object(realm, "link target").set("value", 3);

                Obj o = create_object(realm, "link origin");
                o.set("link", k0);
            });

            auto realm = trigger_client_reset(
                [&](auto& realm) {
                    auto target_table = get_table(realm, "link target");
                    auto key_of_second_target = get_key_for_object_with_value(target_table, 2);
                    REQUIRE(key_of_second_target);
                    auto table = get_table(realm, "link origin");
                    table->begin()->set("link", key_of_second_target);
                },
                [&](auto& realm) {
                    auto table = get_table(realm, "link target");
                    auto key_of_second_target = get_key_for_object_with_value(table, 2);
                    table->remove_object(key_of_second_target);
                });
            wait_for_download(*realm);
            REQUIRE_NOTHROW(realm->refresh());

            auto origin = get_table(*realm, "link origin");
            auto target = get_table(*realm, "link target");
            REQUIRE(origin->size() == 1);
            REQUIRE(target->size() == 2);
            REQUIRE(get_key_for_object_with_value(target, 1));
            REQUIRE(get_key_for_object_with_value(target, 3));
            auto key = origin->begin()->get<ObjKey>("link");
            auto obj = target->get_object(key);
            REQUIRE(obj.get<Int>("value") == 1);
        }

        SECTION("add remotely deleted object to list") {
            ObjKey k0, k1, k2;
            setup([&](auto& realm) {
                k0 = create_object(realm, "link target").set("value", 1).get_key();
                k1 = create_object(realm, "link target").set("value", 2).get_key();
                k2 = create_object(realm, "link target").set("value", 3).get_key();

                Obj o = create_object(realm, "link origin");
                o.get_linklist("list").add(k0);
            });

            auto realm = trigger_client_reset(
                [&](auto& realm) {
                    auto key = get_key_for_object_with_value(get_table(realm, "link target"), 2);
                    auto table = get_table(realm, "link origin");
                    auto list = table->begin()->get_linklist("list");
                    list.add(key);
                },
                [&](auto& realm) {
                    auto table = get_table(realm, "link target");
                    auto key = get_key_for_object_with_value(table, 2);
                    REQUIRE(key);
                    table->remove_object(key);
                });
            wait_for_download(*realm);
            REQUIRE_NOTHROW(realm->refresh());

            auto table = get_table(*realm, "link origin");
            auto target_table = get_table(*realm, "link target");
            REQUIRE(table->size() == 1);
            REQUIRE(target_table->size() == 2);
            REQUIRE(get_key_for_object_with_value(target_table, 1));
            REQUIRE(get_key_for_object_with_value(target_table, 3));
            auto list = table->begin()->get_linklist("list");
            REQUIRE(list.size() == 1);
            REQUIRE(list.get_object(0).get<Int>("value") == 1);
        }
    }
}

namespace cf = realm::collection_fixtures;
TEMPLATE_TEST_CASE("client reset types", "[client reset]", cf::MixedVal, cf::Int, cf::Bool, cf::Float, cf::Double,
                   cf::String, cf::Binary, cf::Date, cf::OID, cf::Decimal, cf::UUID, cf::BoxedOptional<cf::Int>,
                   cf::BoxedOptional<cf::Bool>, cf::BoxedOptional<cf::Float>, cf::BoxedOptional<cf::Double>,
                   cf::BoxedOptional<cf::OID>, cf::BoxedOptional<cf::UUID>, cf::UnboxedOptional<cf::String>,
                   cf::UnboxedOptional<cf::Binary>, cf::UnboxedOptional<cf::Date>, cf::UnboxedOptional<cf::Decimal>)
{
    auto values = TestType::values();
    using T = typename TestType::Type;
    using W = typename TestType::Wrapped;
    using Boxed = typename TestType::Boxed;

    if (!EventLoop::has_implementation())
        return;

    TestSyncManager init_sync_manager;
    SyncTestFile config(init_sync_manager.app(), "default");
    config.cache = false;
    config.automatic_change_notifications = false;
    config.schema = Schema{
        {"object",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
         }},
        {"test type",
         {{"_id", PropertyType::Int, Property::IsPrimary{true}},
          //{"value", TestType::property_type()},
          {"list", PropertyType::Array | TestType::property_type()},
          {"dictionary", PropertyType::Dictionary | TestType::property_type()},
          {"set", PropertyType::Set | TestType::property_type()}}},
    };

    SyncTestFile config2(init_sync_manager.app(), "default");

    auto setup = [&](auto fn) {
        auto realm = Realm::get_shared_realm(config);
        realm->begin_transaction();
        fn(*realm);
        realm->commit_transaction();
        wait_for_upload(*realm);
    };

    auto trigger_client_reset = [&](auto local, auto remote) -> std::shared_ptr<Realm> {
        return ::trigger_client_reset(local, remote, config, config2, init_sync_manager);
    };

    config.cache = false;
    config.automatic_change_notifications = false;
    config.sync_config->client_resync_mode = ClientResyncMode::SeamlessLoss;

    Results results;
    Object object;
    CollectionChangeSet object_changes, results_changes;
    NotificationToken object_token, results_token;
    auto setup_listeners = [&](SharedRealm realm) {
        results = Results(realm, ObjectStore::table_for_object_type(realm->read_group(), "test type"))
                      .sort({{{"_id", true}}});
        if (results.size() >= 1) {
            auto obj = *ObjectStore::table_for_object_type(realm->read_group(), "test type")->begin();
            object = Object(realm, obj);
            object_token = object.add_notification_callback([&](CollectionChangeSet changes, std::exception_ptr err) {
                REQUIRE_FALSE(err);
                object_changes = std::move(changes);
            });
        }
        results_token = results.add_notification_callback([&](CollectionChangeSet changes, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            results_changes = std::move(changes);
        });
    };

    auto check_list = [&](Obj obj, std::vector<T> expected) {
        ColKey col = obj.get_table()->get_column_key("list");
        auto actual = obj.get_list_values<T>(col);
        REQUIRE(actual == expected);
    };

    auto check_dictionary = [&](Obj obj, std::vector<std::pair<std::string, Mixed>> expected) {
        ColKey col = obj.get_table()->get_column_key("dictionary");
        Dictionary dict = obj.get_dictionary(col);
        REQUIRE(dict.size() == expected.size());
        for (auto pair : expected) {
            auto it = dict.find(pair.first);
            REQUIRE(it != dict.end());
            REQUIRE((*it).second == pair.second);
        }
    };

    auto check_set = [&](Obj obj, std::vector<Mixed> expected) {
        ColKey col = obj.get_table()->get_column_key("set");
        SetBasePtr set = obj.get_setbase_ptr(col);
        REQUIRE(set->size() == expected.size());
        for (auto value : expected) {
            auto ndx = set->find_any(value);
            REQUIRE(ndx != realm::not_found);
        }
    };

    SECTION("lists") {
        REQUIRE(values.size() >= 2);
        REQUIRE(values[0] != values[1]);
        int64_t pk_val = 0;
        setup([&](auto& realm) {
            auto table = get_table(realm, "test type");
            REQUIRE(table);
            auto obj = table->create_object_with_primary_key(pk_val);
            ColKey col = table->get_column_key("list");
            obj.template set_list_values<T>(col, {values[0]});
        });

        auto reset_list = [&](std::vector<T> local_state, std::vector<T> remote_state) {
            auto realm = trigger_client_reset(
                [&](auto& local_realm) {
                    auto table = get_table(local_realm, "test type");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    ColKey col = table->get_column_key("list");
                    table->begin()->template set_list_values<T>(col, local_state);
                },
                [&](auto& remote_realm) {
                    auto table = get_table(remote_realm, "test type");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    ColKey col = table->get_column_key("list");
                    table->begin()->template set_list_values<T>(col, remote_state);
                });
            setup_listeners(realm);

            REQUIRE_NOTHROW(advance_and_notify(*realm));
            CHECK(results.size() == 1);
            CHECK(results.get<Obj>(0).get<Int>("_id") == pk_val);
            CHECK(object.is_valid());
            check_list(results.get<Obj>(0), local_state);
            check_list(object.obj(), local_state);

            wait_for_upload(*realm);
            wait_for_download(*realm);
            REQUIRE_NOTHROW(advance_and_notify(*realm));

            CHECK(results.size() == 1);
            CHECK(object.is_valid());
            check_list(results.get<Obj>(0), remote_state);
            check_list(object.obj(), remote_state);
            if (local_state == remote_state) {
                REQUIRE_INDICES(results_changes.modifications);
                REQUIRE_INDICES(object_changes.modifications);
            }
            else {
                REQUIRE_INDICES(results_changes.modifications, 0);
                REQUIRE_INDICES(object_changes.modifications, 0);
            }
            REQUIRE_INDICES(results_changes.insertions);
            REQUIRE_INDICES(results_changes.deletions);
            REQUIRE_INDICES(object_changes.insertions);
            REQUIRE_INDICES(object_changes.deletions);
        };

        SECTION("modify") {
            reset_list({values[0]}, {values[1]});
        }
        SECTION("modify opposite") {
            reset_list({values[1]}, {values[0]});
        }
        SECTION("empty remote") {
            reset_list({values[1], values[0], values[1]}, {});
        }
        SECTION("empty local") {
            reset_list({}, {values[0], values[1]});
        }
        SECTION("empty both") {
            reset_list({}, {});
        }
        SECTION("equal suffix") {
            reset_list({values[0], values[0], values[1]}, {values[0], values[1]});
        }
        SECTION("equal prefix") {
            reset_list({values[0]}, {values[0], values[1], values[1]});
        }
        SECTION("equal lists") {
            reset_list({values[0]}, {values[0]});
        }
        SECTION("equal middle") {
            reset_list({values[0], values[1], values[0]}, {values[1], values[1], values[1]});
        }
    }

    SECTION("dictionary") {
        REQUIRE(values.size() >= 2);
        REQUIRE(values[0] != values[1]);
        int64_t pk_val = 0;
        std::string dict_key = "hello";
        setup([&](auto& realm) {
            auto table = get_table(realm, "test type");
            REQUIRE(table);
            auto obj = table->create_object_with_primary_key(pk_val);
            ColKey col = table->get_column_key("dictionary");
            Dictionary dict = obj.get_dictionary(col);
            dict.insert(dict_key, Mixed{values[0]});
        });

        auto reset_dictionary = [&](std::vector<std::pair<std::string, Mixed>> local_state,
                                    std::vector<std::pair<std::string, Mixed>> remote_state) {
            auto realm = trigger_client_reset(
                [&](auto& local_realm) {
                    auto table = get_table(local_realm, "test type");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    ColKey col = table->get_column_key("dictionary");
                    Dictionary dict = table->begin()->get_dictionary(col);
                    for (auto pair : local_state) {
                        dict.insert(pair.first, pair.second);
                    }
                    for (auto it = dict.begin(); it != dict.end(); ++it) {
                        auto found = std::any_of(local_state.begin(), local_state.end(), [&](auto pair) {
                            return Mixed{pair.first} == (*it).first && Mixed{pair.second} == (*it).second;
                        });
                        if (!found) {
                            dict.erase(it);
                        }
                    }
                },
                [&](auto& remote_realm) {
                    auto table = get_table(remote_realm, "test type");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    ColKey col = table->get_column_key("dictionary");
                    Dictionary dict = table->begin()->get_dictionary(col);
                    for (auto pair : remote_state) {
                        dict.insert(pair.first, pair.second);
                    }
                    for (auto it = dict.begin(); it != dict.end(); ++it) {
                        auto found = std::any_of(remote_state.begin(), remote_state.end(), [&](auto pair) {
                            return Mixed{pair.first} == (*it).first && Mixed{pair.second} == (*it).second;
                        });
                        if (!found) {
                            dict.erase(it);
                        }
                    }
                });
            setup_listeners(realm);

            REQUIRE_NOTHROW(advance_and_notify(*realm));
            CHECK(results.size() == 1);
            CHECK(results.get<Obj>(0).get<Int>("_id") == pk_val);
            CHECK(object.is_valid());
            check_dictionary(results.get<Obj>(0), local_state);
            check_dictionary(object.obj(), local_state);

            wait_for_upload(*realm);
            wait_for_download(*realm);
            REQUIRE_NOTHROW(advance_and_notify(*realm));

            CHECK(results.size() == 1);
            CHECK(object.is_valid());
            check_dictionary(results.get<Obj>(0), remote_state);
            check_dictionary(object.obj(), remote_state);
            if (local_state == remote_state) {
                REQUIRE_INDICES(results_changes.modifications);
                REQUIRE_INDICES(object_changes.modifications);
            }
            else {
                REQUIRE_INDICES(results_changes.modifications, 0);
                REQUIRE_INDICES(object_changes.modifications, 0);
            }
            REQUIRE_INDICES(results_changes.insertions);
            REQUIRE_INDICES(results_changes.deletions);
            REQUIRE_INDICES(object_changes.insertions);
            REQUIRE_INDICES(object_changes.deletions);
        };

        SECTION("modify") {
            reset_dictionary({{dict_key, Mixed{values[0]}}}, {{dict_key, Mixed{values[1]}}});
        }
        SECTION("modify opposite") {
            reset_dictionary({{dict_key, Mixed{values[1]}}}, {{dict_key, Mixed{values[0]}}});
        }
        SECTION("empty remote") {
            reset_dictionary({{dict_key, Mixed{values[1]}}}, {});
        }
        SECTION("empty local") {
            reset_dictionary({}, {{dict_key, Mixed{values[1]}}});
        }
        SECTION("extra values on remote") {
            reset_dictionary({{dict_key, Mixed{values[0]}}}, {{dict_key, Mixed{values[0]}},
                                                              {"world", Mixed{values[1]}},
                                                              {"foo", Mixed{values[1]}},
                                                              {"aaa", Mixed{values[0]}}});
        }
    }

    SECTION("set") {
        int64_t pk_val = 0;

        auto reset_set = [&](std::vector<Mixed> local_state, std::vector<Mixed> remote_state) {
            auto realm = trigger_client_reset(
                [&local_state](auto& local_realm) {
                    auto table = get_table(local_realm, "test type");
                    REQUIRE(table);
                    ColKey col = table->get_column_key("set");
                    SetBasePtr set = table->begin()->get_setbase_ptr(col);
                    for (size_t i = set->size(); i > 0; --i) {
                        Mixed si = set->get_any(i - 1);
                        if (std::find(local_state.begin(), local_state.end(), si) == local_state.end()) {
                            set->erase_any(si);
                        }
                    }
                    for (auto e : local_state) {
                        set->insert_any(e);
                    }
                },
                [&remote_state](auto& remote_realm) {
                    auto table = get_table(remote_realm, "test type");
                    REQUIRE(table);
                    ColKey col = table->get_column_key("set");
                    SetBasePtr set = table->begin()->get_setbase_ptr(col);
                    for (size_t i = set->size(); i > 0; --i) {
                        Mixed si = set->get_any(i - 1);
                        if (std::find(remote_state.begin(), remote_state.end(), si) == remote_state.end()) {
                            set->erase_any(si);
                        }
                    }
                    for (auto e : remote_state) {
                        set->insert_any(e);
                    }
                });
            setup_listeners(realm);

            REQUIRE_NOTHROW(advance_and_notify(*realm));
            CHECK(results.size() == 1);
            CHECK(results.get<Obj>(0).get<Int>("_id") == pk_val);
            CHECK(object.is_valid());
            check_set(results.get<Obj>(0), local_state);
            check_set(object.obj(), local_state);

            wait_for_upload(*realm);
            wait_for_download(*realm);
            REQUIRE_NOTHROW(advance_and_notify(*realm));

            CHECK(results.size() == 1);
            CHECK(object.is_valid());
            check_set(results.get<Obj>(0), remote_state);
            check_set(object.obj(), remote_state);
            if (local_state == remote_state) {
                REQUIRE_INDICES(results_changes.modifications);
                REQUIRE_INDICES(object_changes.modifications);
            }
            else {
                REQUIRE_INDICES(results_changes.modifications, 0);
                REQUIRE_INDICES(object_changes.modifications, 0);
            }
            REQUIRE_INDICES(results_changes.insertions);
            REQUIRE_INDICES(results_changes.deletions);
            REQUIRE_INDICES(object_changes.insertions);
            REQUIRE_INDICES(object_changes.deletions);
        };

        REQUIRE(values.size() >= 2);
        REQUIRE(values[0] != values[1]);
        setup([&](auto& realm) {
            auto table = get_table(realm, "test type");
            REQUIRE(table);
            auto obj = table->create_object_with_primary_key(pk_val);
            ColKey col = table->get_column_key("set");
            SetBasePtr set = obj.get_setbase_ptr(col);
            set->insert_any(Mixed{values[0]});
        });

        SECTION("modify") {
            reset_set({{values[0]}}, {{values[1]}});
        }
        SECTION("modify opposite") {
            reset_set({{values[1]}}, {{values[0]}});
        }
        SECTION("empty remote") {
            reset_set({{values[1]}, {values[0]}}, {});
        }
        SECTION("empty local") {
            reset_set({}, {{values[0]}, {values[1]}});
        }
        SECTION("empty both") {
            reset_set({}, {});
        }
        SECTION("equal suffix") {
            reset_set({{values[0]}, {values[1]}}, {{values[1]}});
        }
        SECTION("equal prefix") {
            reset_set({{values[0]}}, {{values[1]}, {values[0]}});
        }
        SECTION("equal lists") {
            reset_set({{values[0]}, {values[1]}}, {{values[0]}, {values[1]}});
        }
    }
}
