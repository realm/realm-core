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

#pragma once

#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/impl/sync_file.hpp>
#include <realm/object-store/sync/impl/sync_metadata.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/util/bson/bson.hpp>

#include "util/event_loop.hpp"
#include "util/sync_test_utils.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"

// disable the tests that rely on having baas available on the network
// but allow opt-in by building with REALM_ENABLE_AUTH_TESTS=1
#ifndef REALM_ENABLE_AUTH_TESTS
#define REALM_ENABLE_AUTH_TESTS 0
#endif

#if REALM_ENABLE_SYNC
#if REALM_ENABLE_AUTH_TESTS

namespace realm {

#ifdef REALM_MONGODB_ENDPOINT
std::string get_base_url();
#endif

struct AutoVerifiedEmailCredentials : app::AppCredentials {
    AutoVerifiedEmailCredentials();
    std::string email;
    std::string password;
};

AutoVerifiedEmailCredentials create_user_and_log_in(app::SharedApp app);

void wait_for_advance(Realm& realm);

namespace reset_utils {

std::unique_ptr<TestClientReset> make_baas_client_reset(const Realm::Config& local_config,
                                                        const Realm::Config& remote_config,
                                                        TestAppSession& test_app_session);

std::unique_ptr<TestClientReset> make_baas_flx_client_reset(const Realm::Config& local_config,
                                                            const Realm::Config& remote_config,
                                                            const TestAppSession& test_app_session);

void wait_for_object_to_persist_to_atlas(std::shared_ptr<SyncUser> user, const AppSession& app_session,
                                         const std::string& schema_name, const bson::BsonDocument& filter_bson);

void wait_for_num_objects_in_atlas(std::shared_ptr<SyncUser> user, const AppSession& app_session,
                                   const std::string& schema_name, size_t expected_size);

void trigger_client_reset(const AppSession& app_session);
void trigger_client_reset(const AppSession& app_session, const SharedRealm& realm);

} // namespace reset_utils

} // namespace realm

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC
