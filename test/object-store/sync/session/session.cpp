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

#include "catch2/catch.hpp"

#include "sync/session/session_util.hpp"

#include "feature_checks.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"
#include "property.hpp"
#include "schema.hpp"

#include "util/event_loop.hpp"
#include "util/test_utils.hpp"

#include <realm/util/time.hpp>
#include <realm/util/scope_exit.hpp>

#include <atomic>
#include <chrono>
#include <fstream>
#include <unistd.h>

using namespace realm;
using namespace realm::util;

static const std::string dummy_auth_url = "https://realm.example.org";

TEST_CASE("SyncSession: management by SyncUser", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    SyncServer server;
    TestSyncManager init_sync_manager;
    const std::string realm_base_url = server.base_url();

    SECTION("a SyncUser can properly retrieve its owned sessions") {
        std::string path_1;
        std::string path_2;
        auto user = SyncManager::shared().get_user({ "user1a", dummy_auth_url }, "not_a_real_token");
        auto session1 = sync_session(server, user, "/test1a-1",
                                     [](const auto&, const auto&) { return s_test_token; },
                                     [](auto, auto) { },
                                     SyncSessionStopPolicy::AfterChangesUploaded,
                                     &path_1);
        auto session2 = sync_session(server, user, "/test1a-2",
                                     [](const auto&, const auto&) { return s_test_token; },
                                     [](auto, auto) { },
                                     SyncSessionStopPolicy::AfterChangesUploaded,
                                     &path_2);
        EventLoop::main().run_until([&] { return sessions_are_active(*session1, *session2); });

        // Check the sessions on the SyncUser.
        REQUIRE(user->all_sessions().size() == 2);
        auto s1 = user->session_for_on_disk_path(path_1);
        REQUIRE(s1);
        CHECK(s1->config().realm_url() == realm_base_url + "/test1a-1");
        auto s2 = user->session_for_on_disk_path(path_2);
        REQUIRE(s2);
        CHECK(s2->config().realm_url() == realm_base_url + "/test1a-2");
    }

    SECTION("a SyncUser properly unbinds its sessions upon logging out") {
        auto user = SyncManager::shared().get_user({ "user1b", dummy_auth_url }, "not_a_real_token");
        auto session1 = sync_session(server, user, "/test1b-1",
                                     [](const auto&, const auto&) { return s_test_token; },
                                     [](auto, auto) { });
        auto session2 = sync_session(server, user, "/test1b-2",
                                     [](const auto&, const auto&) { return s_test_token; },
                                     [](auto, auto) { });
        EventLoop::main().run_until([&] { return sessions_are_active(*session1, *session2); });

        // Log the user out.
        user->log_out();
        // The sessions should log themselves out.
        EventLoop::main().run_until([&] { return sessions_are_inactive(*session1, *session2); });
        CHECK(user->all_sessions().size() == 0);
    }

    SECTION("a SyncUser defers binding new sessions until it is logged in") {
        const std::string user_id = "user1c";
        auto user = SyncManager::shared().get_user({ user_id, dummy_auth_url }, "not_a_real_token");
        user->log_out();
        REQUIRE(user->state() == SyncUser::State::LoggedOut);
        auto session1 = sync_session(server, user, "/test1c-1",
                                     [](const auto&, const auto&) { return s_test_token; },
                                     [](auto, auto) { });
        auto session2 = sync_session(server, user, "/test1c-2",
                                     [](const auto&, const auto&) { return s_test_token; },
                                     [](auto, auto) { });
        // Run the runloop many iterations to see if the sessions spuriously bind.
        spin_runloop();
        REQUIRE(sessions_are_inactive(*session1));
        REQUIRE(sessions_are_inactive(*session2));
        REQUIRE(user->all_sessions().size() == 0);
        // Log the user back in via the sync manager.
        user = SyncManager::shared().get_user({ user_id, dummy_auth_url }, "not_a_real_token_either");
        EventLoop::main().run_until([&] { return sessions_are_active(*session1, *session2); });
        REQUIRE(user->all_sessions().size() == 2);
    }

    SECTION("a SyncUser properly rebinds existing sessions upon logging back in") {
        const std::string user_id = "user1d";
        auto user = SyncManager::shared().get_user({ user_id, dummy_auth_url }, "not_a_real_token");
        auto session1 = sync_session(server, user, "/test1d-1",
                                     [](const auto&, const auto&) { return s_test_token; },
                                     [](auto, auto) { });
        auto session2 = sync_session(server, user, "/test1d-2",
                                     [](const auto&, const auto&) { return s_test_token; },
                                     [](auto, auto) { });
        // Make sure the sessions are bound.
        EventLoop::main().run_until([&] { return sessions_are_active(*session1, *session2); });
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
        user = SyncManager::shared().get_user({ user_id, dummy_auth_url }, "not_a_real_token_either");
        EventLoop::main().run_until([&] { return sessions_are_active(*session1, *session2); });
        REQUIRE(user->all_sessions().size() == 2);
    }

    SECTION("sessions that were destroyed can be properly recreated when requested again") {
        const std::string path = "/test1e";
        std::weak_ptr<SyncSession> weak_session;
        std::string on_disk_path;
        util::Optional<SyncConfig> config;
        auto user = SyncManager::shared().get_user({ "user1e", dummy_auth_url }, "not_a_real_token");
        {
            // Create the session within a nested scope, so we can control its lifetime.
            auto session = sync_session(server, user, path,
                                        [](const auto&, const auto&) { return s_test_token; },
                                        [](auto, auto) { },
                                        SyncSessionStopPolicy::Immediately,
                                        &on_disk_path);
            weak_session = session;
            config = session->config();
            REQUIRE(on_disk_path.size() > 0);
            REQUIRE(weak_session.lock());
        }
        // Wait for the session to die. It may not happen immediately if a progress or error handler
        // is called on a background thread and keeps the session alive past the scope of the above block.
        EventLoop::main().run_until([&] { return weak_session.expired(); });

        // The next time we request it, it'll be created anew.
        // The call to `get_session()` should result in `SyncUser::register_session()` being called.
        auto session = SyncManager::shared().get_session(on_disk_path, *config);
        CHECK(session);
        session = user->session_for_on_disk_path(on_disk_path);
        CHECK(session);
    }

    SECTION("a user can create multiple sessions for the same URL") {
        auto user = SyncManager::shared().get_user({ "user", dummy_auth_url }, "not_a_real_token");
        auto create_session = [&]() {
            // Note that this should put the sessions at different paths.
            return sync_session(server, user, "/test",
                                [](const auto&, const auto&) { return s_test_token; },
                                [](auto, auto) { },
                                SyncSessionStopPolicy::Immediately);
        };
        REQUIRE(create_session());
        REQUIRE(create_session());
    }
}

TEST_CASE("sync: log-in", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    SyncServer server;
    // Disable file-related functionality and metadata functionality for testing purposes.
    TestSyncManager init_sync_manager;
    auto user = SyncManager::shared().get_user({ "user", dummy_auth_url }, "not_a_real_token");

    SECTION("Can log in") {
        std::atomic<int> error_count(0);
        auto session = sync_session(server, user, "/test",
                                    [](const auto&, const auto&) { return s_test_token; },
                                    [&](auto, auto) { ++error_count; });

        std::atomic<bool> download_did_complete(false);
        session->wait_for_download_completion([&](auto) { download_did_complete = true; });
        EventLoop::main().run_until([&] { return download_did_complete.load() || error_count > 0; });
        CHECK(error_count == 0);
    }

    // FIXME: What should we do if we get an invalid access token? Presumably we want to retry at some point.
    // What should drive that?
    SECTION("Session is invalid after invalid token") {
        std::atomic<int> error_count(0);
        auto session = sync_session(server, user, "/test",
                                    [](const auto&, const auto&) { return "this is not a valid access token"; },
                                    [&](auto, auto) { ++error_count; });

        EventLoop::main().run_until([&] { return error_count > 0; });
    }

    // TODO: write a test that logs out a Realm with multiple sessions, then logs it back in?
    // TODO: write tests that check that a Session properly handles various types of errors reported via its callback.
}

TEST_CASE("sync: token refreshing", "[sync]") {
    using PublicState = realm::SyncSession::PublicState;
    using ProtocolError = realm::sync::ProtocolError;
    if (!EventLoop::has_implementation())
        return;

    SyncServer server;
    // Disable file-related functionality and metadata functionality for testing purposes.
    TestSyncManager init_sync_manager;
    auto user = SyncManager::shared().get_user({ "user-token-refreshing", dummy_auth_url }, "not_a_real_token");

    SECTION("Can preemptively refresh token while session is active.") {
        auto session = sync_session(server, user, "/test-token-refreshing",
                                    [](const auto&, const auto&) { return s_test_token; },
                                    [](auto, auto) { },
                                    SyncSessionStopPolicy::AfterChangesUploaded);
        EventLoop::main().run_until([&] { return sessions_are_active(*session); });

        REQUIRE(session->state() == PublicState::Active);
        session->refresh_access_token(s_test_token, none);
        REQUIRE(session->state() == PublicState::Active);
    }

    SECTION("Can refresh token when expired while session is active.") {
        std::atomic<bool> bind_function_called(false);
        auto session = sync_session(server, user, "/test-token-refreshing",
                                    [&](const auto&, const auto&) {
                                        bind_function_called = true;
                                        return s_test_token;
                                    },
                                    [](auto, SyncError err) {
                                        printf("DEBUG: test received an error: %s\n", err.message.c_str());
                                    },
                                    SyncSessionStopPolicy::AfterChangesUploaded);
        EventLoop::main().run_until([&] { return sessions_are_active(*session); });
        bind_function_called = false;

        // Simulate the "token expired" error, which should cause the object store
        // to request another token from the binding.
        std::error_code code = std::error_code{static_cast<int>(ProtocolError::token_expired), realm::sync::protocol_error_category()};
        SyncSession::OnlyForTesting::handle_error(*session, {code, "Too many pugs in the office.", false});
        REQUIRE(bind_function_called == true);
        REQUIRE(session->state() == PublicState::Active);
    }
}

TEST_CASE("SyncSession: close() API", "[sync]") {
    using PublicState = realm::SyncSession::PublicState;
    SyncServer server;

    auto user = SyncManager::shared().get_user({ "close-api-tests-user", "https://realm.example.org" }, "not_a_real_token");

    SECTION("Behaves properly when called on session in the 'waiting for token' state for Immediate") {
        std::atomic<bool> bind_function_called(false);
        // Make a session that won't leave the 'waiting for token' state.
        auto server_path = "/test-close-for-waiting-token";
        auto session = sync_session_with_bind_handler(server, user, server_path,
                                                      [&](const auto&, const auto&, std::shared_ptr<SyncSession>) {
                                                          bind_function_called = true;
                                                      },
                                                      [&](auto, auto) { },
                                                      SyncSessionStopPolicy::Immediately);
        REQUIRE(session);
        EventLoop::main().run_until([&] { return bind_function_called == true; });
        REQUIRE(session->state() == PublicState::WaitingForAccessToken);
        session->close();
        REQUIRE(sessions_are_inactive(*session));
        // Test trying to call bind on the session after it's been closed. Should be a no-op.
        session->refresh_access_token(s_test_token, server.base_url() + server_path);
        REQUIRE(sessions_are_inactive(*session));
    }

    SECTION("Behaves properly when called on session in the 'active' or 'inactive' state") {
        auto session = sync_session(server, user, "/test-close-for-active",
                                    [](const auto&, const auto&) { return s_test_token; },
                                    [](auto, auto) { },
                                    SyncSessionStopPolicy::AfterChangesUploaded);
        EventLoop::main().run_until([&] { return sessions_are_active(*session); });
        REQUIRE(sessions_are_active(*session));
        session->close();
        EventLoop::main().run_until([&] { return sessions_are_inactive(*session); });
        REQUIRE(sessions_are_inactive(*session));
        // Try closing the session again. This should be a no-op.
        session->close();
        REQUIRE(sessions_are_inactive(*session));
    }
}

TEST_CASE("SyncSession: update_configuration()", "[sync]") {
    TestSyncManager init_sync_manager;

    SyncServer server{false};

    auto user = SyncManager::shared().get_user({"userid", dummy_auth_url}, "not_a_real_token");
    auto session =  sync_session_with_bind_handler(server, user, "/update_configuration",
                                                   [](const auto&, const auto&, auto) { },
                                                   [](auto, auto) { },
                                                   SyncSessionStopPolicy::AfterChangesUploaded);

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
    SyncServer server;
    TestSyncManager init_sync_manager;

    // Create a valid session.
    std::function<void(std::shared_ptr<SyncSession>, SyncError)> error_handler = [](auto, auto) { };
    const std::string user_id = "user1d";
    std::string on_disk_path;
    auto user = SyncManager::shared().get_user({ user_id, "https://realm.example.org" }, "not_a_real_token");
    auto session = sync_session(server, user, "/test1e",
                                [](const auto&, const auto&) { return s_test_token; },
                                [&](auto session, SyncError error) {
                                    error_handler(std::move(session), std::move(error));
                                },
                                SyncSessionStopPolicy::AfterChangesUploaded,
                                &on_disk_path);
    // Make sure the sessions are bound.
    EventLoop::main().run_until([&] { return sessions_are_active(*session); });

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

        SyncError initial_error{std::error_code{code, realm::sync::protocol_error_category()}, "Something bad happened", false};
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
        idx = recovery_path.find(SyncManager::shared().recovery_directory_path());
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

struct AdminTokenUser {
    static auto user() { return SyncManager::shared().get_admin_token_user(dummy_auth_url, "not_a_real_token"); }
};
struct RegularUser {
    static auto user() { return SyncManager::shared().get_user({"user-dying-state", dummy_auth_url}, "not_a_real_token"); }
};

TEMPLATE_TEST_CASE("sync: stop policy behavior", "[sync]", RegularUser, AdminTokenUser) {
    using ProtocolError = realm::sync::ProtocolError;
    const std::string dummy_auth_url = "https://realm.example.org";
    if (!EventLoop::has_implementation())
        return;

    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });

    // Server is initially stopped so we can control when the session exits the dying state.
    SyncServer server(false);
    auto schema = Schema{
        {"object", {
            {"value", PropertyType::Int},
        }},
    };

    std::atomic<bool> error_handler_invoked(false);
    Realm::Config config;
    auto user = TestType::user();

    auto create_session = [&](SyncSessionStopPolicy stop_policy) {
        auto session = sync_session(server, user, "/test-dying-state",
                                    [](const auto&, const auto&) { return s_test_token; },
                                    [&](auto, auto) { error_handler_invoked = true; },
                                    stop_policy, nullptr, schema, &config);
        EventLoop::main().run_until([&] { return sessions_are_active(*session); });

        // Add an object so there's something to upload
        auto r = Realm::get_shared_realm(config);
        TableRef table = ObjectStore::table_for_object_type(r->read_group(), "object");
        r->begin_transaction();
        table->create_object();
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
            EventLoop::main().run_until([&] { return sessions_are_inactive(*session); });
        }

        SECTION("transitions back to Active if the session is revived") {
            auto session2 = SyncManager::shared().get_session(config.path, *config.sync_config);
            REQUIRE(session->state() == SyncSession::PublicState::Active);
            REQUIRE(session2 == session);
        }

        SECTION("transitions to Inactive if a fatal error occurs") {
            std::error_code code = std::error_code{static_cast<int>(ProtocolError::bad_syntax), realm::sync::protocol_error_category()};
            SyncSession::OnlyForTesting::handle_error(*session, {code, "Not a real error message", true});
            CHECK(sessions_are_inactive(*session));
            // The session shouldn't report fatal errors when in the dying state.
            CHECK(!error_handler_invoked);
        }

        SECTION("ignores non-fatal errors and does not transition to Inactive") {
            // Fire a simulated *non-fatal* error.
            std::error_code code = std::error_code{static_cast<int>(ProtocolError::other_error), realm::sync::protocol_error_category()};
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

    SyncServer server;
    // Disable file-related functionality and metadata functionality for testing purposes.
    TestSyncManager init_sync_manager;

    std::array<char, 64> encryption_key;
    encryption_key.fill(12);

    SECTION("open a session with realm file encryption and then open the same file directly") {
        SyncTestFile config(server, "encrypted_realm");
        std::copy_n(encryption_key.begin(), encryption_key.size(), std::back_inserter(config.encryption_key));
        config.sync_config->realm_encryption_key = encryption_key;

        // open a session and wait for it to fully download to its local realm file
        {
            std::atomic<bool> handler_called(false);
            auto session = SyncManager::shared().get_session(config.path, *config.sync_config);
            EventLoop::main().run_until([&] { return sessions_are_active(*session); });
            session->wait_for_download_completion([&](auto) {
                handler_called = true;
            });
            EventLoop::main().run_until([&] { return handler_called == true; });
            session->close();
            EventLoop::main().run_until([&] { return sessions_are_inactive(*session); });
        }

        // open a Realm with the same config, if the session didn't use the encryption key this should fail
        {
            Realm::get_shared_realm(config);
        }
    }

    SECTION("errors if encryption keys are different") {
        {
            SyncTestFile config(server, "encrypted_realm");
            config.sync_config->realm_encryption_key = encryption_key;

            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }

        {
            SyncTestFile config(server, "encrypted_realm");
            std::copy_n(encryption_key.begin(), encryption_key.size(), std::back_inserter(config.encryption_key));

            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }

        {
            SyncTestFile config(server, "encrypted_realm");
            config.sync_config->realm_encryption_key = encryption_key;
            config.encryption_key.push_back(9);

            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }
    }
}

TEST_CASE("sync: non-synced metadata table doesn't result in non-additive schema changes", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    SyncServer server;
    // Disable file-related functionality and metadata functionality for testing purposes.
    TestSyncManager init_sync_manager;

    // Create a synced Realm containing a class with two properties.
    {
        SyncTestFile config1(server, "schema-version-test");
        config1.schema_version = 1;
        config1.schema = Schema{
            {"object", {
                {"property1", PropertyType::Int},
                {"property2", PropertyType::Int}
            }},
        };

        auto realm1 = Realm::get_shared_realm(config1);
        wait_for_upload(*realm1);
    }

    // Download the existing Realm into a second local file without specifying a schema,
    // mirroring how `openAsync` works.
    SyncTestFile config2(server, "schema-version-test");
    config2.schema_version = 1;
    {
        auto realm2 = Realm::get_shared_realm(config2);
        wait_for_download(*realm2);
    }

    // Open the just-downloaded Realm while specifying a schema that contains a class with
    // only a single property. This should not result in us trying to remove `property2`,
    // and will throw an exception if it does.
    {
        SyncTestFile config3(server, "schema-version-test");
        config3.path = config2.path;
        config3.schema_version = 1;
        config3.schema = Schema{
            {"object", {
                {"property1", PropertyType::Int}
            }},
        };

        auto realm3 = Realm::get_shared_realm(config3);
    }
}


TEST_CASE("sync: stable IDs", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    SyncServer server;
    // Disable file-related functionality and metadata functionality for testing purposes.
    TestSyncManager init_sync_manager;

    SECTION("ID column isn't visible in schema read from Group") {
        SyncTestFile config(server, "schema-test");
        config.schema_version = 1;
        config.schema = Schema{
            {"object", {
                {"value", PropertyType::Int}
            }},
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

    SyncServer server;
    // Disable file-related functionality and metadata functionality for testing purposes.
    TestSyncManager init_sync_manager;


    SyncTestFile config(server, "migration-test");
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

TEST_CASE("sync: client resync") {
    using namespace std::literals::chrono_literals;
    if (!EventLoop::has_implementation())
        return;

    TestSyncManager init_sync_manager;

    SyncServer server;
    SyncTestFile config(server, "default");
    config.schema = Schema{
        {"object", {
            {"value", PropertyType::Int},
        }},
        {"link target", {
            {"value", PropertyType::Int},
        }},
        {"pk link target", {
            {"pk", PropertyType::Int, Property::IsPrimary{true}},
            {"value", PropertyType::Int},
        }},
        {"link origin", {
            {"id", PropertyType::Int},
            {"link", PropertyType::Object|PropertyType::Nullable, "link target"},
            {"pk link", PropertyType::Object|PropertyType::Nullable, "pk link target"},
            {"list", PropertyType::Object|PropertyType::Array, "link target"},
            {"pk list", PropertyType::Object|PropertyType::Array, "pk link target"},
        }},
    };
    SyncTestFile config2(server, "default");

    auto get_table = [](Realm& realm, StringData object_type) {
        return ObjectStore::table_for_object_type(realm.read_group(), object_type);
    };
    auto create_object = [&](Realm& realm, StringData object_type) -> Obj {
        auto table = get_table(realm, object_type);
        REQUIRE(table);
        return table->create_object();
    };

    auto setup = [&](auto fn) {
        auto realm = Realm::get_shared_realm(config);
        realm->begin_transaction();
        fn(*realm);
        realm->commit_transaction();
        wait_for_upload(*realm);
    };

    auto trigger_client_reset = [&](auto local, auto remote) -> std::shared_ptr<Realm> {
        auto realm = Realm::get_shared_realm(config);
        auto session = SyncManager::shared().get_session(realm->config().path, *realm->config().sync_config);
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
            auto realm2 = Realm::get_shared_realm(config2);

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
    };

    SECTION("should trigger error callback when mode is manual") {
        config.sync_config->client_resync_mode = ClientResyncMode::Manual;
        std::atomic<bool> called{false};
        config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
            REQUIRE(error.is_client_reset_requested());
            called = true;
        };

        auto realm = trigger_client_reset([](auto&){}, [](auto&){});

        EventLoop::main().run_until([&] { return called.load(); });
    }

    config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError) {
        FAIL("Error handler should not have been called");
    };

    SECTION("should discard local changeset when mode is discard") {
        config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

        auto realm = trigger_client_reset([](auto&){}, [](auto&){});
        wait_for_download(*realm);
        realm->refresh(); // FIXME: sync needs to notify

        CHECK(ObjectStore::table_for_object_type(realm->read_group(), "object")->begin()->get<Int>("value") == 6);
    }

    SECTION("should recover local changeset when mode is recover") {
        config.sync_config->client_resync_mode = ClientResyncMode::Recover;
        config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError) {
            FAIL("Error handler should not have been called");
        };

        auto realm = trigger_client_reset([](auto&){}, [](auto&){});
        wait_for_download(*realm);
        realm->refresh();

        CHECK(ObjectStore::table_for_object_type(realm->read_group(), "object")->begin()->get<Int>("value") == 4);
    }

    SECTION("should honor encryption key for downloaded Realm") {
        config.encryption_key.resize(64, 'a');
        config.sync_config->realm_encryption_key = std::array<char, 64>();
        config.sync_config->realm_encryption_key->fill('a');
        config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

        auto realm = trigger_client_reset([](auto&){}, [](auto&){});
        wait_for_download(*realm);
        realm->close();

        REQUIRE_NOTHROW(Realm::get_shared_realm(config));
    }

    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

    SECTION("add table in discarded transaction") {
        auto realm = trigger_client_reset([](auto& realm) {
            realm.update_schema({
                {"object2", {
                    {"value2", PropertyType::Int},
                }},
            }, 0, nullptr, nullptr, true);
            ObjectStore::table_for_object_type(realm.read_group(), "object2")->create_object();
        }, [](auto&){});
        wait_for_download(*realm);
        REQUIRE_THROWS(realm->refresh());
        /* FIXME: Current understanding is that local schema changes are discarded
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object2");
        REQUIRE(table);
        REQUIRE(table->size() == 0);
        */
    }

    SECTION("add column in discarded transaction") {
        auto realm = trigger_client_reset([](auto& realm) {
            realm.update_schema({
                {"object", {
                    {"value2", PropertyType::Int},
                }},
            }, 0, nullptr, nullptr, true);
            ObjectStore::table_for_object_type(realm.read_group(), "object")->begin()->set("value2", 123);
        }, [](auto&){});
        wait_for_download(*realm);
        REQUIRE_THROWS(realm->refresh());
        /* FIXME: Current understanding is that local schema changes are discarded
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object");
        REQUIRE(table->get_column_count() == 2);
        REQUIRE(table->begin()->get<Int>("value2") == 0);
        */
    }

    config.sync_config->client_resync_mode = ClientResyncMode::Recover;

    SECTION("add table without pk in recovered transaction") {
        auto realm = trigger_client_reset([](auto& realm) {
            realm.update_schema({
                {"object2", {
                    {"value2", PropertyType::Int},
                }},
            }, 0, nullptr, nullptr, true);
            ObjectStore::table_for_object_type(realm.read_group(), "object2")->create_object();
        }, [](auto&){});
        wait_for_download(*realm);
        REQUIRE_NOTHROW(realm->refresh());
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object2");
        REQUIRE(table);
        REQUIRE(table->size() == 1);
    }

    SECTION("add table pk in recovered transaction") {
        auto realm = trigger_client_reset([](auto& realm) {
            realm.update_schema({
                {"object2", {
                    {"pk", PropertyType::Int|PropertyType::Nullable, Property::IsPrimary{true}},
                }},
            }, 0, nullptr, nullptr, true);
            auto table = ObjectStore::table_for_object_type(realm.read_group(), "object2");
            table->create_object_with_primary_key(Mixed());
            table->create_object_with_primary_key(Mixed(1));
        }, [](auto&){});
        wait_for_download(*realm);
        REQUIRE_NOTHROW(realm->refresh());
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object2");
        REQUIRE(table);
        REQUIRE(table->size() == 2);
    }

    SECTION("add column in recovered transaction") {
        auto realm = trigger_client_reset([](auto& realm) {
            realm.update_schema({
                {"object", {
                    {"value2", PropertyType::Int},
                    {"array", PropertyType::Int|PropertyType::Array},
                    {"link", PropertyType::Object|PropertyType::Nullable, "object"},
                }},
            }, 0, nullptr, nullptr, true);
            auto table = ObjectStore::table_for_object_type(realm.read_group(), "object");
            table->begin()->set(table->get_column_key("value2"), 123);
        }, [](auto&){});
        wait_for_download(*realm);
        REQUIRE_NOTHROW(realm->refresh());
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object");
        REQUIRE(table->get_column_count() == 4);
        REQUIRE(table->begin()->get<Int>(table->get_column_key("value2")) == 123);
    }

    SECTION("compatible schema changes in both remote and recovered transactions") {
        auto realm = trigger_client_reset([](auto& realm) {
            realm.update_schema({
                {"object", {
                    {"value2", PropertyType::Int},
                }},
                {"object2", {
                    {"link", PropertyType::Object|PropertyType::Nullable, "object"},
                }},
            }, 0, nullptr, nullptr, true);
        }, [](auto& realm) {
            realm.update_schema({
                {"object", {
                    {"value2", PropertyType::Int},
                }},
                {"object2", {
                    {"link", PropertyType::Object|PropertyType::Nullable, "object"},
                }},
            }, 0, nullptr, nullptr, true);
        });
        wait_for_download(*realm);
        REQUIRE_NOTHROW(realm->refresh());
    }

    SECTION("incompatible schema changes in remote and recovered transactions") {
        auto realm = trigger_client_reset([](auto& realm) {
            realm.update_schema({
                {"object", {
                    {"value2", PropertyType::Float},
                }},
            }, 0, nullptr, nullptr, true);
        }, [](auto& realm) {
            realm.update_schema({
                {"object", {
                    {"value2", PropertyType::Int},
                }},
            }, 0, nullptr, nullptr, true);
        });
        wait_for_download(*realm);
        REQUIRE_THROWS_WITH(realm->refresh(),
                            Catch::Matchers::Contains("Property 'object.value2' has been changed from 'float' to 'int'"));
    }

    SECTION("add object in recovered transaction") {
        Obj obj;
        auto realm = trigger_client_reset([&](auto& realm) {
            auto table = ObjectStore::table_for_object_type(realm.read_group(), "object");
            obj = table->create_object();

            realm.update_schema({
                {"object", {
                    {"value2", PropertyType::Int},
                }},
            }, 0, nullptr, nullptr, true);
            obj.set("value2", 123);
        }, [](auto&){});
        wait_for_download(*realm);
        REQUIRE_NOTHROW(realm->refresh());
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object");
        REQUIRE(table->get_column_count() == 2);
        REQUIRE(table->begin()->get<Int>("value2") == 123);
    }

    SECTION("delete object in recovered transaction") {
        auto realm = trigger_client_reset([&](auto& realm) {
            ObjectStore::table_for_object_type(realm.read_group(), "object")->clear();
        }, [](auto&){});
        wait_for_download(*realm);
        REQUIRE_NOTHROW(realm->refresh());
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object");
        REQUIRE(table->size() == 0);
    }

    SECTION("list insertions in recovered transaction") {
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

        auto realm = trigger_client_reset([&](auto& realm) {
            auto table = get_table(realm, "link origin");
            auto list = table->begin()->get_linklist(table->get_column_key("list"));
            list.add(k0);
            list.insert(0, k2);
            list.insert(0, k1);
        }, [](auto&){});
        wait_for_download(*realm);
        REQUIRE_NOTHROW(realm->refresh());

        auto table = get_table(*realm, "link origin");
        auto list = table->begin()->get_linklist(table->get_column_key("list"));
        REQUIRE(list.size() == 6);
        REQUIRE(list.get_object(0).get<Int>("value") == 2);
        REQUIRE(list.get_object(1).get<Int>("value") == 3);
        REQUIRE(list.get_object(2).get<Int>("value") == 1);
        REQUIRE(list.get_object(3).get<Int>("value") == 2);
        REQUIRE(list.get_object(4).get<Int>("value") == 3);
        REQUIRE(list.get_object(5).get<Int>("value") == 1);
    }

    SECTION("list deletions in recovered transaction") {
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

        auto realm = trigger_client_reset([&](auto& realm) {
            auto table = get_table(realm, "link origin");
            auto list = table->begin()->get_linklist(table->get_column_key("list"));
            list.remove(1);
        }, [](auto&){});
        wait_for_download(*realm);
        REQUIRE_NOTHROW(realm->refresh());

        auto table = get_table(*realm, "link origin");
        auto list = table->begin()->get_linklist(table->get_column_key("list"));
        REQUIRE(list.size() == 2);
        REQUIRE(list.get_object(0).get<Int>("value") == 1);
        REQUIRE(list.get_object(1).get<Int>("value") == 3);
    }

    SECTION("list clear in recovered transaction") {
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

        auto realm = trigger_client_reset([&](auto& realm) {
            auto table = get_table(realm, "link origin");
            auto list = table->begin()->get_linklist(table->get_column_key("list"));
            list.clear();
        }, [&](auto& realm){
            auto key = get_table(realm, "link target")->begin()->get_key();
            auto table = get_table(realm, "link origin");
            auto list = table->begin()->get_linklist(table->get_column_key("list"));
            list.add(key);
        });
        wait_for_download(*realm);
        REQUIRE_NOTHROW(realm->refresh());

        auto table = get_table(*realm, "link origin");
        auto list = table->begin()->get_linklist(table->get_column_key("list"));
        REQUIRE(list.size() == 0);
    }

    SECTION("conflicting primary key creations") {
        auto realm = trigger_client_reset([&](auto& realm) {
            auto table = get_table(realm, "pk link target");
            table->create_object_with_primary_key(1).set("value", 1);
            table->create_object_with_primary_key(2).set("value", 2);
            table->create_object_with_primary_key(3).set("value", 3);
        }, [&](auto& realm){
            auto table = get_table(realm, "pk link target");
            table->create_object_with_primary_key(1).set("value", 1);
            table->create_object_with_primary_key(2).set("value", 4);
            table->create_object_with_primary_key(5).set("value", 5);
        });
        wait_for_download(*realm);
        REQUIRE_NOTHROW(realm->refresh());

        auto table = get_table(*realm, "pk link target");
        REQUIRE(table->size() == 4);
        auto it = table->begin();
        REQUIRE(it->get<Int>("value") == 1);
        REQUIRE((++it)->get<Int>("value") == 2);
        REQUIRE((++it)->get<Int>("value") == 3);
        REQUIRE((++it)->get<Int>("value") == 5);
    }

    SECTION("link to remotely deleted object") {
        setup([&](auto& realm) {
            auto k0 = create_object(realm, "link target").set("value", 1).get_key();
            create_object(realm, "link target").set("value", 2);
            create_object(realm, "link target").set("value", 3);

            Obj o = create_object(realm, "link origin");
            o.set("link", k0);
        });

        auto realm = trigger_client_reset([&](auto& realm) {
            auto key = get_table(realm, "link target")->get_object(1).get_key();
            auto table = get_table(realm, "link origin");
            table->begin()->set("link", key);
        }, [&](auto& realm){
            auto table = get_table(realm, "link target");
            table->get_object(1).remove();
        });
        wait_for_download(*realm);
        REQUIRE_NOTHROW(realm->refresh());

        // Link set is discarded entirely (as opposed to being set to nil)
        auto origin = get_table(*realm, "link origin");
        auto target = get_table(*realm, "link target");
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

        auto realm = trigger_client_reset([&](auto& realm) {
            auto key = get_table(realm, "link target")->get_object(1).get_key();
            auto table = get_table(realm, "link origin");
            auto list = table->begin()->get_linklist("list");
            list.add(key);
        }, [&](auto& realm){
            auto table = get_table(realm, "link target");
            table->get_object(1).remove();
        });
        wait_for_download(*realm);
        REQUIRE_NOTHROW(realm->refresh());

        auto table = get_table(*realm, "link origin");
        auto list = table->begin()->get_linklist("list");
        REQUIRE(list.size() == 1);
        REQUIRE(list.get_object(0).get<Int>("value") == 1);
    }
}
