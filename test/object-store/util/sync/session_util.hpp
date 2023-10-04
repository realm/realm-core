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
#pragma once

#include <util/event_loop.hpp>
#include <util/test_file.hpp>
#include <util/sync/sync_test_utils.hpp>

#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>

#include <realm/sync/config.hpp>

using namespace realm;
using namespace realm::util;

namespace Catch {
template <>
struct StringMaker<SyncSession::State> {
    static std::string convert(SyncSession::State state)
    {
        switch (state) {
            case SyncSession::State::Active:
                return "Active";
            case SyncSession::State::Dying:
                return "Dying";
            case SyncSession::State::Inactive:
                return "Inactive";
            case SyncSession::State::WaitingForAccessToken:
                return "WaitingForAccessToken";
            case SyncSession::State::Paused:
                return "Paused";
            default:
                return "Unknown";
        }
    }
};
} // namespace Catch

inline bool sessions_are_active(const SyncSession& session)
{
    return session.state() == SyncSession::State::Active;
}

inline bool sessions_are_inactive(const SyncSession& session)
{
    return session.state() == SyncSession::State::Inactive;
}

inline bool sessions_are_disconnected(const SyncSession& session)
{
    return session.connection_state() == SyncSession::ConnectionState::Disconnected;
}

inline bool sessions_are_connected(const SyncSession& session)
{
    return session.connection_state() == SyncSession::ConnectionState::Connected;
}

template <typename... S>
bool sessions_are_active(const SyncSession& session, const S&... s)
{
    return sessions_are_active(session) && sessions_are_active(s...);
}

template <typename... S>
bool sessions_are_inactive(const SyncSession& session, const S&... s)
{
    return sessions_are_inactive(session) && sessions_are_inactive(s...);
}

inline void spin_runloop(int count = 2)
{
    EventLoop::main().run_until([count, spin_count = 0]() mutable {
        spin_count++;
        return spin_count > count;
    });
}

inline std::string conn_id_for_realm(const SharedRealm& realm)
{
    return SyncSession::OnlyForTesting::get_appservices_connection_id(*realm->sync_session());
}

inline void require_no_error(std::shared_ptr<SyncSession>, SyncError error)
{
    FAIL(util::format("Unexpected sync error: %1", error.simple_message));
}

// Convenience function for creating and configuring sync sessions for test use.
// Many of the optional arguments can be used to pass information about the
// session back out to the test, or configure the session more precisely.
template <typename ErrorHandler = decltype(&require_no_error)>
std::shared_ptr<SyncSession> sync_session(
    std::shared_ptr<SyncUser> user, const std::string& path, ErrorHandler&& error_handler = &require_no_error,
    SyncSessionStopPolicy stop_policy = SyncSessionStopPolicy::AfterChangesUploaded,
    std::string* on_disk_path = nullptr, util::Optional<Schema> schema = none, Realm::Config* out_config = nullptr)
{
    SyncTestFile config(SyncConfig{user, path}, std::move(stop_policy), std::forward<ErrorHandler>(error_handler));

    // File should not be deleted when we leave this function
    config.persist();

    config.sync_config->client_resync_mode = ClientResyncMode::Manual;
    if (schema) {
        config.schema = *schema;
    }
    if (on_disk_path) {
        *on_disk_path = config.path;
    }
    if (out_config) {
        *out_config = config;
    }
    std::shared_ptr<SyncSession> session;
    {
        auto realm = Realm::get_shared_realm(config);
        session = user->sync_manager()->get_existing_session(config.path);
    }
    return session;
}
