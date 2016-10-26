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

#include "util/event_loop.hpp"
#include "util/test_file.hpp"

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
                                          SyncSessionStopPolicy stop_policy=SyncSessionStopPolicy::AfterChangesUploaded)
{
    std::string url = server.base_url() + path;
    SyncTestFile config({user, url, std::move(stop_policy),
        [&](const std::string& path, const SyncConfig& config, std::shared_ptr<SyncSession> session) {
            auto token = fetch_access_token(path, config.realm_url);
            session->refresh_access_token(std::move(token), config.realm_url);
        }, std::forward<ErrorHandler>(error_handler)});

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
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    SyncServer server;
    SyncManager::shared().configure_file_system("/tmp/", SyncManager::MetadataMode::NoMetadata);
    const std::string realm_base_url = server.base_url();

    SECTION("a SyncUser can properly retrieve its owned sessions") {
        auto user = SyncManager::shared().get_user("user1a", "not_a_real_token");
        auto session1 = sync_session(server, user, "/test1a-1",
                                     [&](auto...) { return s_test_token; },
                                     [&](auto...) { });
        auto session2 = sync_session(server, user, "/test1a-2",
                                     [&](auto...) { return s_test_token; },
                                     [&](auto...) { });
        EventLoop::main().run_until([&] { return session_is_active(*session1) && session_is_active(*session2); });

        // Check the sessions on the SyncUser.
        REQUIRE(user->all_sessions().size() == 2);
        auto s1 = user->session_for_url(realm_base_url + "/test1a-1");
        REQUIRE(s1);
        CHECK(s1->config().realm_url == realm_base_url + "/test1a-1");
        auto s2 = user->session_for_url(realm_base_url + "/test1a-2");
        REQUIRE(s2);
        CHECK(s2->config().realm_url == realm_base_url + "/test1a-2");
    }

    SECTION("a SyncUser properly unbinds its sessions upon logging out") {
        auto user = SyncManager::shared().get_user("user1b", "not_a_real_token");
        auto session1 = sync_session(server, user, "/test1b-1",
                                     [&](auto...) { return s_test_token; },
                                     [&](auto...) { });
        auto session2 = sync_session(server, user, "/test1b-2",
                                     [&](auto...) { return s_test_token; },
                                     [&](auto...) { });
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
        auto session1 = sync_session(server, user, "/test1b-1",
                                     [&](auto...) { return s_test_token; },
                                     [&](auto...) { });
        auto session2 = sync_session(server, user, "/test1b-2",
                                     [&](auto...) { return s_test_token; },
                                     [&](auto...) { });
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
        auto session1 = sync_session(server, user, "/test1b-1",
                                     [&](auto...) { return s_test_token; },
                                     [&](auto...) { });
        auto session2 = sync_session(server, user, "/test1b-2",
                                     [&](auto...) { return s_test_token; },
                                     [&](auto...) { });
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
}

TEST_CASE("sync: log-in", "[sync]") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    SyncServer server;
    // Disable file-related functionality and metadata functionality for testing purposes.
    SyncManager::shared().configure_file_system("/tmp/", SyncManager::MetadataMode::NoMetadata);

    SECTION("Can log in") {
        std::atomic<int> error_count(0);
        auto session = sync_session(server, SyncManager::shared().get_user("user", "not_a_real_token"), "/test",
                                    [&](const std::string&, const std::string&) { return s_test_token; },
                                    [&](int, std::string, SyncSessionError) { ++error_count; });

        std::atomic<bool> download_did_complete(false);
        session->wait_for_download_completion([&] { download_did_complete = true; });
        EventLoop::main().run_until([&] { return download_did_complete.load() || error_count > 0; });
        CHECK(!session->is_in_error_state());
        CHECK(error_count == 0);
    }

    SECTION("Session is invalid after invalid token") {
        std::atomic<int> error_count(0);
        auto session = sync_session(server, SyncManager::shared().get_user("user", "not_a_real_token"), "/test",
                                    [&](const std::string&, const std::string&) { return "this is not a valid access token"; },
                                    [&](int, std::string, SyncSessionError) { ++error_count; });

        EventLoop::main().run_until([&] { return error_count > 0; });
        CHECK(session->is_in_error_state());
    }

#if 0
    // FIXME: This test currently deadlocks when SyncSession's error handler attempts to change the
    // session's state. Should be fixed by https://github.com/realm/realm-object-store/pull/181.

    SECTION("Session is invalid after invalid token while waiting on download to complete") {
        std::atomic<int> error_count(0);
        auto session = sync_session(server, nullptr, "/test",
                                    [&](const std::string&, const std::string&) { return "this is not a valid access token"; },
                                    [&](int, std::string, SyncSessionError) { ++error_count; });

        EventLoop::main().perform([&] {
            session->wait_for_download_completion([] {
                fprintf(stderr, "Download completed.\n");
            });
        });

        EventLoop::main().run_until([&] { return error_count > 0; });
        CHECK(session->is_in_error_state());
    }
#endif

    // TODO: write a test that logs out a Realm with multiple sessions, then logs it back in?
}
