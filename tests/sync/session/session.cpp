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

#include "catch.hpp"

#include "sync/session/session_util.hpp"

#include "object_schema.hpp"
#include "object_store.hpp"
#include "property.hpp"
#include "schema.hpp"

#include "util/event_loop.hpp"
#include "util/time.hpp"

#include <realm/util/scope_exit.hpp>

#include <atomic>
#include <chrono>
#include <unistd.h>

using namespace realm;
using namespace realm::util;

TEST_CASE("SyncSession: management by SyncUser", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    SyncServer server;
    SyncManager::shared().configure_file_system(tmp_dir(), SyncManager::MetadataMode::NoMetadata);
    const std::string realm_base_url = server.base_url();

    SECTION("a SyncUser can properly retrieve its owned sessions") {
        std::string path_1;
        std::string path_2;
        auto user = SyncManager::shared().get_user("user1a", "not_a_real_token");
        auto session1 = sync_session(server, user, "/test1a-1",
                                     [](auto&, auto&) { return s_test_token; },
                                     [](auto, auto) { },
                                     SyncSessionStopPolicy::AfterChangesUploaded,
                                     &path_1);
        auto session2 = sync_session(server, user, "/test1a-2",
                                     [](auto&, auto&) { return s_test_token; },
                                     [](auto, auto) { },
                                     SyncSessionStopPolicy::AfterChangesUploaded,
                                     &path_2);
        EventLoop::main().run_until([&] { return sessions_are_active(*session1, *session2); });

        // Check the sessions on the SyncUser.
        REQUIRE(user->all_sessions().size() == 2);
        auto s1 = user->session_for_on_disk_path(path_1);
        REQUIRE(s1);
        CHECK(s1->config().realm_url == realm_base_url + "/test1a-1");
        auto s2 = user->session_for_on_disk_path(path_2);
        REQUIRE(s2);
        CHECK(s2->config().realm_url == realm_base_url + "/test1a-2");
    }

    SECTION("a SyncUser properly unbinds its sessions upon logging out") {
        auto user = SyncManager::shared().get_user("user1b", "not_a_real_token");
        auto session1 = sync_session(server, user, "/test1b-1",
                                     [](auto&, auto&) { return s_test_token; },
                                     [](auto, auto) { });
        auto session2 = sync_session(server, user, "/test1b-2",
                                     [](auto&, auto&) { return s_test_token; },
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
        auto user = SyncManager::shared().get_user(user_id, "not_a_real_token");
        user->log_out();
        REQUIRE(user->state() == SyncUser::State::LoggedOut);
        auto session1 = sync_session(server, user, "/test1c-1",
                                     [](auto&, auto&) { return s_test_token; },
                                     [](auto, auto) { });
        auto session2 = sync_session(server, user, "/test1c-2",
                                     [](auto&, auto&) { return s_test_token; },
                                     [](auto, auto) { });
        // Run the runloop many iterations to see if the sessions spuriously bind.
        spin_runloop();
        REQUIRE(sessions_are_inactive(*session1));
        REQUIRE(sessions_are_inactive(*session2));
        REQUIRE(user->all_sessions().size() == 0);
        // Log the user back in via the sync manager.
        user = SyncManager::shared().get_user(user_id, "not_a_real_token_either");
        EventLoop::main().run_until([&] { return sessions_are_active(*session1, *session2); });
        REQUIRE(user->all_sessions().size() == 2);
    }

    SECTION("a SyncUser properly rebinds existing sessions upon logging back in") {
        const std::string user_id = "user1d";
        auto user = SyncManager::shared().get_user(user_id, "not_a_real_token");
        auto session1 = sync_session(server, user, "/test1d-1",
                                     [](auto&, auto&) { return s_test_token; },
                                     [](auto, auto) { });
        auto session2 = sync_session(server, user, "/test1d-2",
                                     [](auto&, auto&) { return s_test_token; },
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
        user = SyncManager::shared().get_user(user_id, "not_a_real_token_either");
        EventLoop::main().run_until([&] { return sessions_are_active(*session1, *session2); });
        REQUIRE(user->all_sessions().size() == 2);
    }

    SECTION("sessions that were destroyed can be properly recreated when requested again") {
        const std::string path = "/test1e";
        std::weak_ptr<SyncSession> weak_session;
        std::string on_disk_path;
        SyncConfig config;
        auto user = SyncManager::shared().get_user("user1e", "not_a_real_token");
        {
            // Create the session within a nested scope, so we can control its lifetime.
            auto session = sync_session(server, user, path,
                                        [](auto&, auto&) { return s_test_token; },
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
        auto session = SyncManager::shared().get_session(on_disk_path, config);
        CHECK(session);
        session = user->session_for_on_disk_path(on_disk_path);
        CHECK(session);
    }

    SECTION("a user can create multiple sessions for the same URL") {
        auto user = SyncManager::shared().get_user("user", "not_a_real_token");
        auto create_session = [&]() {
            // Note that this should put the sessions at different paths.
            return sync_session(server, user, "/test",
                                [](auto&, auto&) { return s_test_token; },
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

    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    SyncServer server;
    // Disable file-related functionality and metadata functionality for testing purposes.
    SyncManager::shared().configure_file_system(tmp_dir(), SyncManager::MetadataMode::NoMetadata);
    auto user = SyncManager::shared().get_user("user", "not_a_real_token");

    SECTION("Can log in") {
        std::atomic<int> error_count(0);
        auto session = sync_session(server, user, "/test",
                                    [](const std::string&, const std::string&) { return s_test_token; },
                                    [&](auto, auto) { ++error_count; });

        std::atomic<bool> download_did_complete(false);
        session->wait_for_download_completion([&](auto) { download_did_complete = true; });
        EventLoop::main().run_until([&] { return download_did_complete.load() || error_count > 0; });
        CHECK(!session->is_in_error_state());
        CHECK(error_count == 0);
    }

    SECTION("Session is invalid after invalid token") {
        std::atomic<int> error_count(0);
        auto session = sync_session(server, user, "/test",
                                    [](const std::string&, const std::string&) { return "this is not a valid access token"; },
                                    [&](auto, auto) { ++error_count; });

        EventLoop::main().run_until([&] { return error_count > 0; });
        CHECK(session->is_in_error_state());
    }

    // TODO: write a test that logs out a Realm with multiple sessions, then logs it back in?
    // TODO: write tests that check that a Session properly handles various types of errors reported via its callback.
}

TEST_CASE("sync: token refreshing", "[sync]") {
    using PublicState = realm::SyncSession::PublicState;
    using ProtocolError = realm::sync::ProtocolError;
    if (!EventLoop::has_implementation())
        return;

    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    SyncServer server;
    // Disable file-related functionality and metadata functionality for testing purposes.
    SyncManager::shared().configure_file_system(tmp_dir(), SyncManager::MetadataMode::NoMetadata);
    auto user = SyncManager::shared().get_user("user-token-refreshing", "not_a_real_token");

    SECTION("Can preemptively refresh token while session is active.") {
        auto session = sync_session(server, user, "/test-token-refreshing",
                                    [&](auto&, auto&) { return s_test_token; },
                                    [](auto, auto) { },
                                    SyncSessionStopPolicy::AfterChangesUploaded);
        EventLoop::main().run_until([&] { return sessions_are_active(*session); });
        REQUIRE(!session->is_in_error_state());

        REQUIRE(session->state() == PublicState::Active);
        session->refresh_access_token(s_test_token, none);
        REQUIRE(session->state() == PublicState::Active);
    }

    SECTION("Can refresh token when expired while session is active.") {
        std::atomic<bool> bind_function_called(false);
        auto session = sync_session(server, user, "/test-token-refreshing",
                                    [&](auto&, auto&) {
                                        bind_function_called = true;
                                        return s_test_token;
                                    },
                                    [](auto, auto) { },
                                    SyncSessionStopPolicy::AfterChangesUploaded);
        EventLoop::main().run_until([&] { return sessions_are_active(*session); });
        REQUIRE(!session->is_in_error_state());
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
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    SyncServer server;

    auto user = SyncManager::shared().get_user("close-api-tests-user", "not_a_real_token");

    SECTION("Behaves properly when called on session in the 'waiting for token' state for Immediate") {
        std::atomic<bool> bind_function_called(false);
        // Make a session that won't leave the 'waiting for token' state.
        auto server_path = "/test-close-for-waiting-token";
        auto session = sync_session_with_bind_handler(server, user, server_path,
                                                      [&](auto&, auto&, std::shared_ptr<SyncSession>) {
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
                                    [&](auto&, auto&) { return s_test_token; },
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

    SECTION("Behaves properly when called on session in the 'error' state") {
        auto session = sync_session(server, user, "/test-close-for-error",
                                    [&](auto&, auto&) { return "NOT A VALID TOKEN"; },
                                    [](auto, auto) { },
                                    SyncSessionStopPolicy::AfterChangesUploaded);
        EventLoop::main().run_until([&] { return session->state() == PublicState::Error; });
        session->close();
        REQUIRE(session->state() == PublicState::Error);
    }
}

TEST_CASE("sync: error handling", "[sync]") {
    using ProtocolError = realm::sync::ProtocolError;
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    SyncServer server;
    SyncManager::shared().configure_file_system(tmp_dir(), SyncManager::MetadataMode::NoMetadata);

    // Create a valid session.
    std::function<void(std::shared_ptr<SyncSession>, SyncError)> error_handler = [](auto, auto) { };
    const std::string user_id = "user1d";
    std::string on_disk_path;
    auto user = SyncManager::shared().get_user(user_id, "not_a_real_token");
    auto session = sync_session(server, user, "/test1e",
                                 [](auto&, auto&) { return s_test_token; },
                                 [&](auto session, SyncError error) { 
                                    error_handler(std::move(session), std::move(error));
                                 },
                                 SyncSessionStopPolicy::AfterChangesUploaded,
                                 &on_disk_path);
    // Make sure the sessions are bound.
    EventLoop::main().run_until([&] { return sessions_are_active(*session); });
    REQUIRE(!session->is_in_error_state());

    SECTION("Properly handles a client reset error") {
        int code = 0;
        SyncError final_error;
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
        std::time_t just_after_raw = std::time(nullptr);
        auto just_before = util::localtime(just_before_raw);
        auto just_after = util::localtime(just_after_raw);
        // At this point final_error should be populated.
        CHECK(final_error.is_client_reset_requested());
        // The original file path should be present.
        CHECK(final_error.user_info[SyncError::c_original_file_path_key] == on_disk_path);
        // The path to the recovery file should be present, and should contain all necessary components.
        std::string recovery_path = final_error.user_info[SyncError::c_recovery_file_path_key];
        auto idx = recovery_path.find("recovered_realm");
        CHECK(idx != std::string::npos);
        idx = recovery_path.find(SyncManager::shared().recovery_directory_path());
        CHECK(idx != std::string::npos);
        if (just_before.tm_year == just_after.tm_year) {
            idx = recovery_path.find(util::put_time(just_after_raw, "%Y"));
            CHECK(idx != std::string::npos);
        }
        if (just_before.tm_mon == just_after.tm_mon) {
            idx = recovery_path.find(util::put_time(just_after_raw, "%m"));
            CHECK(idx != std::string::npos);
        }
        if (just_before.tm_yday == just_after.tm_yday) {
            idx = recovery_path.find(util::put_time(just_after_raw, "%d"));
            CHECK(idx != std::string::npos);
        }
    }
}

TEST_CASE("sync: stop policy behavior", "[sync]") {
    using ProtocolError = realm::sync::ProtocolError;
    if (!EventLoop::has_implementation())
        return;

    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    // Server is initially stopped so we can control when the session exits the dying state.
    SyncServer server(false);
    auto schema = Schema{
        {"sync_session_object", {
            {"value 1", PropertyType::Int},
            {"value 2", PropertyType::Int},
        }},
    };

    constexpr int count = 2;
    auto add_objects = [](Realm::Config& config) {
        auto r = Realm::get_shared_realm(config);
        TableRef table = ObjectStore::table_for_object_type(r->read_group(), "sync_session_object");
        REQUIRE(table);
        r->begin_transaction();
        for (int i = 0; i < count; ++i) {
            uint64_t row_idx = table->add_empty_row();
            table->set_int(0, row_idx, i * 2);
            table->set_int(1, row_idx, (count - i) * 2);
        }
        r->commit_transaction();
    };

    SECTION("properly transitions from active directly to inactive, and nothing bad happens", "[Immediately]") {
        auto user = SyncManager::shared().get_user("user-dying-state-1", "not_a_real_token");
        Realm::Config config;
        auto session = sync_session(server, user, "/test-dying-state-1",
                                    [](auto&, auto&) { return s_test_token; },
                                    [](auto, auto) { },
                                    SyncSessionStopPolicy::Immediately,
                                    nullptr, schema, &config);
        EventLoop::main().run_until([&] { return sessions_are_active(*session); });
        // Add a couple of objects to the Realm.
        add_objects(config);
        // Now close the session, causing the state to transition directly to Inactive.
        session->close();
        REQUIRE(sessions_are_inactive(*session));
    }

    SECTION("properly transitions from active to dying to inactive if nothing bad happens", "[AfterChangesUploaded]") {
        auto user = SyncManager::shared().get_user("user-dying-state-2", "not_a_real_token");
        Realm::Config config;
        auto session = sync_session(server, user, "/test-dying-state-2",
                                    [](auto&, auto&) { return s_test_token; },
                                    [](auto, auto) { },
                                    SyncSessionStopPolicy::AfterChangesUploaded,
                                    nullptr, schema, &config);
        EventLoop::main().run_until([&] { return sessions_are_active(*session); });
        // Add a couple of objects to the Realm.
        add_objects(config);
        // Now close the session, causing the state to transition to Dying.
        // (it should remain stuck there until we start the server)
        REQUIRE(sessions_are_active(*session));
        session->close();
        REQUIRE(session->state() == SyncSession::PublicState::Dying);
        server.start();
        EventLoop::main().run_until([&] { return sessions_are_inactive(*session); });
    }

    SECTION("properly transitions from active to dying to inactive if a fatal error happens", "[AfterChangesUploaded]") {
        std::atomic<bool> error_handler_invoked(false);
        auto user = SyncManager::shared().get_user("user-dying-state-3", "not_a_real_token");
        Realm::Config config;
        auto session = sync_session(server, user, "/test-dying-state-3",
                                    [](auto&, auto&) { return s_test_token; },
                                    [&](auto, auto) { error_handler_invoked = true; },
                                    SyncSessionStopPolicy::AfterChangesUploaded,
                                    nullptr, schema, &config);
        EventLoop::main().run_until([&] { return sessions_are_active(*session); });
        // Add a couple of objects to the Realm.
        add_objects(config);
        // Now close the session, causing the state to transition to Dying.
        // (it should remain stuck there since we didn't start the server)
        session->close();
        REQUIRE(session->state() == SyncSession::PublicState::Dying);
        // Fire a simulated *fatal* error.
        std::error_code code = std::error_code{static_cast<int>(ProtocolError::bad_syntax), realm::sync::protocol_error_category()};
        SyncSession::OnlyForTesting::handle_error(*session, {code, "Not a real error message", true});
        CHECK(sessions_are_inactive(*session));
        // The session shouldn't report fatal errors when in the dying state.
        CHECK(!error_handler_invoked);
    }

    SECTION("ignores and swallows non-fatal errors if in the dying state.", "[AfterChangesUploaded]") {
        std::atomic<bool> error_handler_invoked(false);
        auto user = SyncManager::shared().get_user("user-dying-state-4", "not_a_real_token");
        Realm::Config config;
        auto session = sync_session(server, user, "/test-dying-state-4",
                                    [](auto&, auto&) { return s_test_token; },
                                    [&](auto, auto) { error_handler_invoked = true; },
                                    SyncSessionStopPolicy::AfterChangesUploaded,
                                    nullptr, schema, &config);
        EventLoop::main().run_until([&] { return sessions_are_active(*session); });
        // Add a couple of objects to the Realm.
        add_objects(config);
        // Now close the session, causing the state to transition to Dying.
        // (it should remain stuck there since we didn't start the server)
        session->close();
        REQUIRE(session->state() == SyncSession::PublicState::Dying);
        // Fire a simulated *non-fatal* error.
        std::error_code code = std::error_code{static_cast<int>(ProtocolError::other_error), realm::sync::protocol_error_category()};
        SyncSession::OnlyForTesting::handle_error(*session, {code, "Not a real error message", false});
        REQUIRE(session->state() == SyncSession::PublicState::Dying);
        CHECK(!error_handler_invoked);
    }
}
