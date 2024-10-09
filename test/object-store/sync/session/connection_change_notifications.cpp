////////////////////////////////////////////////////////////////////////////
//
// Copyright 2018 Realm Inc.
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
#include <util/test_utils.hpp>
#include <util/sync/session_util.hpp>

using namespace realm;
using namespace realm::util;

TEST_CASE("sync: Connection state changes", "[sync][session][connection change]") {
    if (!EventLoop::has_implementation())
        return;

    TestSyncManager tsm;
    auto user = tsm.fake_user();

    SECTION("register connection change listener") {
        auto session = sync_session(
            user, "/connection-state-changes-1", [](auto, auto) {}, SyncSessionStopPolicy::AfterChangesUploaded);

        EventLoop::main().run_until([&] {
            return sessions_are_active(*session);
        });
        EventLoop::main().run_until([&] {
            return sessions_are_connected(*session);
        });

        std::atomic<bool> listener_called(false);
        session->register_connection_change_callback([&](SyncSession::ConnectionState, SyncSession::ConnectionState) {
            listener_called = true;
        });

        user->log_out();
        EventLoop::main().run_until([&] {
            return sessions_are_disconnected(*session);
        });
        REQUIRE(listener_called == true);
    }

    SECTION("unregister connection change listener") {
        auto session = sync_session(
            user, "/connection-state-changes-2", [](auto, auto) {}, SyncSessionStopPolicy::AfterChangesUploaded);

        EventLoop::main().run_until([&] {
            return sessions_are_active(*session);
        });
        EventLoop::main().run_until([&] {
            return sessions_are_connected(*session);
        });

        std::atomic<size_t> listener1_call_cnt(0);
        std::atomic<bool> listener2_called(false);

        auto token1 = session->register_connection_change_callback(
            [&](SyncSession::ConnectionState, SyncSession::ConnectionState) {
                ++listener1_call_cnt;
            });
        session->unregister_connection_change_callback(token1);
        // One call may have been in progress when unregistered
        REQUIRE(listener1_call_cnt <= 1);
        size_t listener1_called = listener1_call_cnt;

        session->register_connection_change_callback([&](SyncSession::ConnectionState, SyncSession::ConnectionState) {
            listener2_called = true;
        });
        user->log_out();
        REQUIRE(sessions_are_disconnected(*session));
        // ensure callback 1 was not called anymore
        REQUIRE(listener1_call_cnt == listener1_called);
        REQUIRE(listener2_called);
    }

    SECTION("unregister connection change listener during callback") {
        int listener1_call_cnt = 0;
        auto session = sync_session(
            user, "/connection-state-changes-3", [](auto, auto) {}, SyncSessionStopPolicy::AfterChangesUploaded);
        std::mutex mutex;
        std::unique_lock lock(mutex);
        uint64_t token1 = session->register_connection_change_callback(
            [&](SyncSession::ConnectionState, SyncSession::ConnectionState) {
                std::lock_guard lock(mutex);
                ++listener1_call_cnt;
                session->unregister_connection_change_callback(token1);
            });
        lock.unlock();

        EventLoop::main().run_until([&] {
            return sessions_are_active(*session);
        });
        EventLoop::main().run_until([&] {
            return sessions_are_connected(*session);
        });

        bool listener2_called = false;
        session->register_connection_change_callback([&](SyncSession::ConnectionState, SyncSession::ConnectionState) {
            listener2_called = true;
        });

        user->log_out();
        REQUIRE(sessions_are_disconnected(*session));
        REQUIRE(listener1_call_cnt == 1); // Only called once before unregister
        REQUIRE(listener2_called);
    }

    SECTION("Callback not invoked when SyncSession is detached from SyncManager") {
        auto session = sync_session(
            user, "/connection-state-changes-1", [](auto, auto) {}, SyncSessionStopPolicy::AfterChangesUploaded);

        EventLoop::main().run_until([&] {
            return sessions_are_active(*session);
        });
        EventLoop::main().run_until([&] {
            return sessions_are_connected(*session);
        });

        bool listener_called = false;
        session->register_connection_change_callback([&](SyncSession::ConnectionState, SyncSession::ConnectionState) {
            listener_called = true;
        });

        session->detach_from_sync_manager();
        REQUIRE_FALSE(listener_called);
    }
}
