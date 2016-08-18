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

#include <atomic>
#include <chrono>
#include <iostream>
#include <unistd.h>

using namespace realm;
using namespace realm::util;

// {"identity":"test", "access": ["download", "upload"]}
static const std::string s_test_token = "eyJpZGVudGl0eSI6InRlc3QiLCAiYWNjZXNzIjogWyJkb3dubG9hZCIsICJ1cGxvYWQiXX0=";

template <typename FetchAccessToken, typename ErrorHandler>
std::shared_ptr<SyncSession> sync_session(SyncServer& server, std::shared_ptr<SyncUser> user, const std::string& path,
                                          FetchAccessToken&& fetch_access_token, ErrorHandler&& error_handler)
{
    std::string url = server.base_url() + path;
    SyncTestFile config(SyncConfig(user, url, SyncSessionStopPolicy::AfterChangesUploaded,
                                   [&](const std::string& path,
                                       const SyncConfig& config,
                                       std::shared_ptr<SyncSession> session) {
        EventLoop::main().perform([&, session=std::move(session)] {
            auto token = fetch_access_token(path, config.realm_url);
            session->refresh_access_token(std::move(token), config.realm_url);
        });
    }, std::forward<ErrorHandler>(error_handler)));

    std::shared_ptr<SyncSession> session;
    {
        auto realm = Realm::get_shared_realm(config);
        session = SyncManager::shared().get_session(config.path, *config.sync_config);
    }
    return session;
}

TEST_CASE("sync: log-in", "[sync]") {
    SyncServer server;
    // Disable file-related functionality and metadata functionality for testing purposes.
    SyncManager::shared().configure_file_system("/tmp/", SyncManager::MetadataMode::NoMetadata);

    SECTION("Can log in") {
        std::atomic<int> error_count(0);
        auto session = sync_session(server, SyncManager::shared().get_user("user", "not_a_real_token"), "/test",
                                    [&](auto...) { return s_test_token; },
                                    [&](auto...) { ++error_count; });

        std::atomic<bool> download_did_complete(false);
        // FIXME: Should it be necessary to kick this wait off asynchronously?
        // Failing to do so hits an assertion failure in sync::Session.
        EventLoop::main().perform([&] {
            session->wait_for_download_completion([&] {
                download_did_complete = true;
            });
        });

        EventLoop::main().run_until([&] { return download_did_complete.load() || error_count > 0; });
        CHECK(session->is_valid());
        CHECK(error_count == 0);
    }

    SECTION("Session is invalid after invalid token") {
        std::atomic<int> error_count(0);
        auto session = sync_session(server, SyncManager::shared().get_user("user", "not_a_real_token"), "/test",
                                    [&](auto...) { return "this is not a valid access token"; },
                                    [&](auto...) { ++error_count; });

        EventLoop::main().run_until([&] { return error_count > 0; });
        CHECK(!session->is_valid());
    }

#if 0
    // FIXME: This test currently deadlocks when SyncSession's error handler attempts to change the
    // session's state. Should be fixed by https://github.com/realm/realm-object-store/pull/181.

    SECTION("Session is invalid after invalid token while waiting on download to complete") {
        std::atomic<int> error_count(0);
        auto session = sync_session(server, nullptr, "/test",
                                    [&](auto...) { return "this is not a valid access token"; },
                                    [&](auto...) { ++error_count; });

        EventLoop::main().perform([&] {
            session->wait_for_download_completion([] {
                fprintf(stderr, "Download completed.\n");
            });
        });

        EventLoop::main().run_until([&] { return error_count > 0; });
        CHECK(!session->is_valid());
    }
#endif

    // TODO: write a test that logs out a Realm with multiple sessions, then logs it back in?

    // Cleanup
    SyncManager::shared().reset_for_testing();
}

// TODO: tests investigating the interaction of SyncUser and SyncSession
