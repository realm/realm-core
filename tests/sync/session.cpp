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

#include "catch.hpp"

#include "sync_test_utils.hpp"

#include "util/event_loop.hpp"
#include "util/test_file.hpp"
#include "util/time.hpp"

#include "sync/sync_config.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_session.hpp"
#include "sync/sync_user.hpp"

#include <realm/util/scope_exit.hpp>

#include <atomic>
#include <chrono>
#include <unistd.h>

using namespace realm;
using namespace realm::util;

template <typename FetchAccessToken, typename ErrorHandler>
std::shared_ptr<SyncSession> sync_session(SyncServer& server, std::shared_ptr<SyncUser> user, const std::string& path,
                                          FetchAccessToken&& fetch_access_token, ErrorHandler&& error_handler,
                                          SyncSessionStopPolicy stop_policy=SyncSessionStopPolicy::AfterChangesUploaded,
                                          std::string* on_disk_path=nullptr)
{
    std::string url = server.base_url() + path;
    SyncTestFile config({user, url, std::move(stop_policy),
        [&, fetch_access_token=std::forward<FetchAccessToken>(fetch_access_token)](const auto& path, const auto& config, auto session) {
            auto token = fetch_access_token(path, config.realm_url);
            session->refresh_access_token(std::move(token), config.realm_url);
        }, std::forward<ErrorHandler>(error_handler)});
    if (on_disk_path) {
        *on_disk_path = config.path;
    }

    std::shared_ptr<SyncSession> session;
    {
        auto realm = Realm::get_shared_realm(config);
        session = SyncManager::shared().get_session(config.path, *config.sync_config);
    }
    return session;
}

namespace {

bool session_is_active(const SyncSession& session)
{
    return session.state() == SyncSession::PublicState::Active;
}

bool session_is_inactive(const SyncSession& session)
{
    return session.state() == SyncSession::PublicState::Inactive;
}

}

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
        EventLoop::main().run_until([&] { return session_is_active(*session1) && session_is_active(*session2); });

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
        EventLoop::main().run_until([&] { return session_is_active(*session1) && session_is_active(*session2); });

        // Log the user out.
        user->log_out();
        // The sessions should log themselves out.
        EventLoop::main().run_until([&] { return session_is_inactive(*session1) && session_is_inactive(*session2); });
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
        std::atomic<int> run_count(0);
        EventLoop::main().run_until([&] { run_count++; return run_count >= 100; });
        REQUIRE(session_is_inactive(*session1));
        REQUIRE(session_is_inactive(*session2));
        REQUIRE(user->all_sessions().size() == 0);
        // Log the user back in via the sync manager.
        user = SyncManager::shared().get_user(user_id, "not_a_real_token_either");
        EventLoop::main().run_until([&] { return session_is_active(*session1) && session_is_active(*session2); });
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
        EventLoop::main().run_until([&] { return session_is_active(*session1) && session_is_active(*session2); });
        REQUIRE(user->all_sessions().size() == 2);
        // Log the user out.
        user->log_out();
        REQUIRE(user->state() == SyncUser::State::LoggedOut);
        // Run the runloop many iterations to see if the sessions spuriously rebind.
        std::atomic<int> run_count(0);
        EventLoop::main().run_until([&] { run_count++; return run_count >= 100; });
        REQUIRE(session_is_inactive(*session1));
        REQUIRE(session_is_inactive(*session2));
        REQUIRE(user->all_sessions().size() == 0);
        // Log the user back in via the sync manager.
        user = SyncManager::shared().get_user(user_id, "not_a_real_token_either");
        EventLoop::main().run_until([&] { return session_is_active(*session1) && session_is_active(*session2); });
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
        EventLoop::main().run_until([&] { return session_is_active(*session); });
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
        EventLoop::main().run_until([&] { return session_is_active(*session); });
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
    EventLoop::main().run_until([&] { return session_is_active(*session); });
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

TEST_CASE("sync: progress notification", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    SyncServer server;
    // Disable file-related functionality and metadata functionality for testing purposes.
    SyncManager::shared().configure_file_system(tmp_dir(), SyncManager::MetadataMode::NoMetadata);
    auto user = SyncManager::shared().get_user("user", "not_a_real_token");

    SECTION("runs at least once (initially when registered)") {
        auto user = SyncManager::shared().get_user("user-test-sync-1", "not_a_real_token");
        auto session = sync_session(server, user, "/test-sync-progress-1",
                                     [](auto&, auto&) { return s_test_token; },
                                     [](auto, auto) { },
                                     SyncSessionStopPolicy::AfterChangesUploaded);
        EventLoop::main().run_until([&] { return session_is_active(*session); });
        // Wait for uploads and downloads
        std::atomic<bool> download_did_complete(false);
        std::atomic<bool> upload_did_complete(false);
        session->wait_for_download_completion([&](auto) { download_did_complete = true; });
        session->wait_for_upload_completion([&](auto) { upload_did_complete = true; });
        EventLoop::main().run_until([&] { return download_did_complete.load() && upload_did_complete.load(); });

        REQUIRE(!session->is_in_error_state());
        std::atomic<bool> callback_was_called(false);

        SECTION("for upload notifications, with no data transfer ongoing") {
            session->register_progress_notifier([&](auto, auto) {
                callback_was_called = true;
            }, SyncSession::NotifierType::upload, false);
            EventLoop::main().run_until([&] { return callback_was_called.load(); });
        }

        SECTION("for download notifications, with no data transfer ongoing") {
            session->register_progress_notifier([&](auto, auto) {
                callback_was_called = true;
            }, SyncSession::NotifierType::download, false);
            EventLoop::main().run_until([&] { return callback_was_called.load(); });
        }

        SECTION("can register another notifier while in the initial notification without deadlock") {
            std::atomic<uint64_t> counter(0);
            session->register_progress_notifier([&](auto, auto) {
                counter++;
                session->register_progress_notifier([&](auto, auto) {
                    counter++;
                }, SyncSession::NotifierType::upload, false);
            }, SyncSession::NotifierType::download, false);
            EventLoop::main().run_until([&] { return counter.load() == 2; });
        }
    }

    SECTION("properly runs for streaming notifiers") {
        auto user = SyncManager::shared().get_user("user-test-sync-2", "not_a_real_token");
        auto session = sync_session(server, user, "/test-sync-progress-2",
                                     [](auto&, auto&) { return s_test_token; },
                                     [](auto, auto) { },
                                     SyncSessionStopPolicy::AfterChangesUploaded);
        EventLoop::main().run_until([&] { return session_is_active(*session); });
        // Wait for uploads and downloads
        std::atomic<bool> download_did_complete(false);
        std::atomic<bool> upload_did_complete(false);
        session->wait_for_download_completion([&](auto) { download_did_complete = true; });
        session->wait_for_upload_completion([&](auto) { upload_did_complete = true; });
        EventLoop::main().run_until([&] { return download_did_complete.load() && upload_did_complete.load(); });

        REQUIRE(!session->is_in_error_state());
        std::atomic<bool> callback_was_called(false);
        std::atomic<uint64_t> transferred(0);
        std::atomic<uint64_t> transferrable(0);
        uint64_t current_transferred = 0;
        uint64_t current_transferrable = 0;

        SECTION("for upload notifications") {
            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::upload, true);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 60;
            current_transferrable = 912;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 25, 26, current_transferred, current_transferrable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == current_transferrable);

            // Second callback
            callback_was_called = false;
            current_transferred = 79;
            current_transferrable = 1021;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 68, 191, current_transferred, current_transferrable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == current_transferrable);

            // Third callback
            callback_was_called = false;
            current_transferred = 150;
            current_transferrable = 1228;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 199, 591, current_transferred, current_transferrable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == current_transferrable);
        }

        SECTION("for download notifications") {
            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::download, true);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 60;
            current_transferrable = 912;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 25, 26);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == current_transferrable);

            // Second callback
            callback_was_called = false;
            current_transferred = 79;
            current_transferrable = 1021;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 68, 191);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == current_transferrable);

            // Third callback
            callback_was_called = false;
            current_transferred = 150;
            current_transferrable = 1228;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 199, 591);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == current_transferrable);
        }

        SECTION("token unregistration works") {
            uint64_t token = session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::download, true);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 60;
            current_transferrable = 912;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 25, 26);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == current_transferrable);

            // Unregister
            session->unregister_progress_notifier(token);

            // Second callback: should not actually do anything.
            callback_was_called = false;
            current_transferred = 150;
            current_transferrable = 1228;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 199, 591);
            CHECK(!callback_was_called.load());
        }

        SECTION("for multiple notifiers") {
            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::download, true);
            // Register a second notifier.
            std::atomic<bool> callback_was_called_2(false);
            std::atomic<uint64_t> transferred_2(0);
            std::atomic<uint64_t> transferrable_2(0);
            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred_2 = xferred;
                transferrable_2 = xferable;
                callback_was_called_2 = true;
            }, SyncSession::NotifierType::upload, true);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load() && callback_was_called_2.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            callback_was_called_2 = false;
            uint64_t current_uploaded = 16;
            uint64_t current_uploadable = 201;
            uint64_t current_downloaded = 68;
            uint64_t current_downloadable = 182;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_downloaded);
            CHECK(transferrable.load() == current_downloadable);
            CHECK(callback_was_called_2.load());
            CHECK(transferred_2.load() == current_uploaded);
            CHECK(transferrable_2.load() == current_uploadable);

            // Second callback
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 31;
            current_uploadable = 329;
            current_downloaded = 76;
            current_downloadable = 191;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_downloaded);
            CHECK(transferrable.load() == current_downloadable);
            CHECK(callback_was_called_2.load());
            CHECK(transferred_2.load() == current_uploaded);
            CHECK(transferrable_2.load() == current_uploadable);
        }
    }

    SECTION("properly runs for non-streaming notifiers") {
        auto user = SyncManager::shared().get_user("user-test-sync-3", "not_a_real_token");
        auto session = sync_session(server, user, "/test-sync-progress-3",
                                     [](auto&, auto&) { return s_test_token; },
                                     [](auto, auto) { },
                                     SyncSessionStopPolicy::AfterChangesUploaded);
        EventLoop::main().run_until([&] { return session_is_active(*session); });
        // Wait for uploads and downloads
        std::atomic<bool> download_did_complete(false);
        std::atomic<bool> upload_did_complete(false);
        session->wait_for_download_completion([&](auto) { download_did_complete = true; });
        session->wait_for_upload_completion([&](auto) { upload_did_complete = true; });
        EventLoop::main().run_until([&] { return download_did_complete.load() && upload_did_complete.load(); });

        REQUIRE(!session->is_in_error_state());
        std::atomic<bool> callback_was_called(false);
        std::atomic<uint64_t> transferred(0);
        std::atomic<uint64_t> transferrable(0);
        uint64_t current_transferred = 0;
        uint64_t current_transferrable = 0;

        SECTION("for upload notifications") {
            // Prime the progress updater
            current_transferred = 60;
            current_transferrable = 501;
            const uint64_t original_transferrable = current_transferrable;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 21, 26, current_transferred, current_transferrable);

            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::upload, false);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 66;
            current_transferrable = 582;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 25, 26, current_transferred, current_transferrable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == original_transferrable);

            // Second callback
            callback_was_called = false;
            current_transferred = original_transferrable + 100;
            current_transferrable = 1021;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 68, 191, current_transferred, current_transferrable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == original_transferrable);

            // The notifier should be unregistered at this point, and not fire.
            callback_was_called = false;
            current_transferred = original_transferrable + 250;
            current_transferrable = 1228;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 199, 591, current_transferred, current_transferrable);
            CHECK(!callback_was_called.load());
        }

        SECTION("for download notifications") {
            // Prime the progress updater
            current_transferred = 60;
            current_transferrable = 501;
            const uint64_t original_transferrable = current_transferrable;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 21, 26);

            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::download, false);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 66;
            current_transferrable = 582;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 25, 26);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == original_transferrable);

            // Second callback
            callback_was_called = false;
            current_transferred = original_transferrable + 100;
            current_transferrable = 1021;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 68, 191);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == original_transferrable);

            // The notifier should be unregistered at this point, and not fire.
            callback_was_called = false;
            current_transferred = original_transferrable + 250;
            current_transferrable = 1228;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 199, 591);
            CHECK(!callback_was_called.load());
        }

        SECTION("token unregistration works") {
            // Prime the progress updater
            current_transferred = 60;
            current_transferrable = 501;
            const uint64_t original_transferrable = current_transferrable;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 21, 26, current_transferred, current_transferrable);

            uint64_t token = session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::upload, false);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 66;
            current_transferrable = 912;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 25, 26, current_transferred, current_transferrable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == original_transferrable);

            // Unregister
            session->unregister_progress_notifier(token);

            // Second callback: should not actually do anything.
            callback_was_called = false;
            current_transferred = 67;
            current_transferrable = 1228;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 199, 591, current_transferred, current_transferrable);
            CHECK(!callback_was_called.load());
        }

        SECTION("for multiple notifiers, different directions") {
            // Prime the progress updater
            uint64_t current_uploaded = 16;
            uint64_t current_uploadable = 201;
            uint64_t current_downloaded = 68;
            uint64_t current_downloadable = 182;
            const uint64_t original_uploadable = current_uploadable;
            const uint64_t original_downloadable = current_downloadable;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);

            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::upload, false);
            // Register a second notifier.
            std::atomic<bool> callback_was_called_2(false);
            std::atomic<uint64_t> downloaded(0);
            std::atomic<uint64_t> downloadable(0);
            session->register_progress_notifier([&](auto xferred, auto xferable) {
                downloaded = xferred;
                downloadable = xferable;
                callback_was_called_2 = true;
            }, SyncSession::NotifierType::download, false);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load() && callback_was_called_2.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 171;
            current_downloadable = 185;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_uploaded);
            CHECK(transferrable.load() == original_uploadable);
            CHECK(callback_was_called_2.load());
            CHECK(downloaded.load() == current_downloaded);
            CHECK(downloadable.load() == original_downloadable);

            // Second callback, last one for the upload notifier
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 218;
            current_uploadable = 310;
            current_downloaded = 174;
            current_downloadable = 190;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_uploaded);
            CHECK(transferrable.load() == original_uploadable);
            CHECK(callback_was_called_2.load());
            CHECK(downloaded.load() == current_downloaded);
            CHECK(downloadable.load() == original_downloadable);

            // Third callback, last one for the download notifier
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 218;
            current_uploadable = 310;
            current_downloaded = 182;
            current_downloadable = 196;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(!callback_was_called.load());
            CHECK(callback_was_called_2.load());
            CHECK(downloaded.load() == current_downloaded);
            CHECK(downloadable.load() == original_downloadable);

            // Fourth callback, last one for the download notifier
            callback_was_called_2 = false;
            current_uploaded = 220;
            current_uploadable = 410;
            current_downloaded = 192;
            current_downloadable = 591;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(!callback_was_called.load());
            CHECK(!callback_was_called_2.load());;
        }

        SECTION("for multiple notifiers, same direction") {
            // Prime the progress updater
            uint64_t current_uploaded = 16;
            uint64_t current_uploadable = 201;
            uint64_t current_downloaded = 68;
            uint64_t current_downloadable = 182;
            const uint64_t original_downloadable = current_downloadable;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);

            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::download, false);
            EventLoop::main().run_until([&] { return callback_was_called.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 171;
            current_downloadable = 185;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_downloaded);
            CHECK(transferrable.load() == original_downloadable);

            // Register a second notifier.
            std::atomic<bool> callback_was_called_2(false);
            std::atomic<uint64_t> downloaded(0);
            std::atomic<uint64_t> downloadable(0);
            const uint64_t original_downloadable_2 = current_downloadable;
            session->register_progress_notifier([&](auto xferred, auto xferable) {
                downloaded = xferred;
                downloadable = xferable;
                callback_was_called_2 = true;
            }, SyncSession::NotifierType::download, false);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called_2.load(); });

            // Second callback, last one for first notifier
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 182;
            current_downloadable = 190;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_downloaded);
            CHECK(transferrable.load() == original_downloadable);
            CHECK(callback_was_called_2.load());
            CHECK(downloaded.load() == current_downloaded);
            CHECK(downloadable.load() == original_downloadable_2);

            // Third callback, last one for second notifier
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 189;
            current_downloadable = 250;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(!callback_was_called.load());
            CHECK(callback_was_called_2.load());
            CHECK(downloaded.load() == current_downloaded);
            CHECK(downloadable.load() == original_downloadable_2);

            // Fourth callback
            callback_was_called_2 = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 201;
            current_downloadable = 289;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(!callback_was_called_2.load());
        }
    }
}
