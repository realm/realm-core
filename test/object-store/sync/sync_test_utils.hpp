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

#ifndef REALM_SYNC_TEST_UTILS_HPP
#define REALM_SYNC_TEST_UTILS_HPP

#include <catch2/catch.hpp>

#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/sync/impl/sync_file.hpp>
#include <realm/object-store/sync/impl/sync_metadata.hpp>
#include <realm/object-store/sync/sync_session.hpp>

#include "util/event_loop.hpp"
#include "util/test_utils.hpp"

// disable the tests that rely on having baas available on the network
// but allow opt-in by building with REALM_ENABLE_AUTH_TESTS=1
#ifndef REALM_ENABLE_AUTH_TESTS
#define REALM_ENABLE_AUTH_TESTS 0
#endif

namespace realm {

bool results_contains_user(SyncUserMetadataResults& results, const std::string& identity,
                           const std::string& auth_server);
bool results_contains_original_name(SyncFileActionMetadataResults& results, const std::string& original_name);

#if REALM_ENABLE_AUTH_TESTS

#ifdef REALM_MONGODB_ENDPOINT
std::string get_base_url();
#endif

struct AutoVerifiedEmailCredentials {
    AutoVerifiedEmailCredentials();
    std::string email;
    std::string password;
};

void timed_wait_for(std::function<bool()> condition,
                    std::chrono::milliseconds max_ms = std::chrono::milliseconds(5000));

void wait_for_sync_changes(std::shared_ptr<SyncSession> session);

AutoVerifiedEmailCredentials create_user_and_login(app::SharedApp app);

#endif // REALM_ENABLE_AUTH_TESTS

} // namespace realm

#endif // REALM_SYNC_TEST_UTILS_HPP
