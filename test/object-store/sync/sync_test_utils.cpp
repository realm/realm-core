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

#include "sync_test_utils.hpp"

namespace realm {

bool results_contains_user(SyncUserMetadataResults& results, const std::string& identity,
                           const std::string& provider_type)
{
    for (size_t i = 0; i < results.size(); i++) {
        auto this_result = results.get(i);
        if (this_result.identity() == identity && this_result.provider_type() == provider_type) {
            return true;
        }
    }
    return false;
}

bool results_contains_original_name(SyncFileActionMetadataResults& results, const std::string& original_name)
{
    for (size_t i = 0; i < results.size(); i++) {
        if (results.get(i).original_name() == original_name) {
            return true;
        }
    }
    return false;
}

#if REALM_ENABLE_AUTH_TESTS

#ifdef REALM_MONGODB_ENDPOINT
std::string get_base_url()
{
    // allows configuration with or without quotes
    std::string base_url = REALM_QUOTE(REALM_MONGODB_ENDPOINT);
    if (base_url.size() > 0 && base_url[0] == '"') {
        base_url.erase(0, 1);
    }
    if (base_url.size() > 0 && base_url[base_url.size() - 1] == '"') {
        base_url.erase(base_url.size() - 1);
    }
    return base_url;
}
#endif

AutoVerifiedEmailCredentials::AutoVerifiedEmailCredentials()
{
    // emails with this prefix will pass through the baas app due to the register function
    email = util::format("realm_tests_do_autoverify%1@%2.com", random_string(10), random_string(10));
    password = random_string(10);
}

void timed_wait_for(std::function<bool()> condition, std::chrono::milliseconds max_ms)
{
    const auto wait_start = std::chrono::steady_clock::now();
    util::EventLoop::main().run_until([&] {
        if (std::chrono::steady_clock::now() - wait_start > max_ms) {
            throw std::runtime_error(util::format("timed_wait_for exceeded %1 ms", max_ms.count()));
        }
        return condition();
    });
}

void wait_for_sync_changes(std::shared_ptr<SyncSession> session)
{
    std::atomic<bool> called{false};
    session->wait_for_upload_completion([&](std::error_code err) {
        REQUIRE(err == std::error_code{});
        called.store(true);
    });
    REQUIRE_NOTHROW(timed_wait_for([&] {
        return called.load();
    }));
    REQUIRE(called);
    called.store(false);
    session->wait_for_download_completion([&](std::error_code err) {
        REQUIRE(err == std::error_code{});
        called.store(true);
    });
    REQUIRE_NOTHROW(timed_wait_for([&] {
        return called.load();
    }));
}

AutoVerifiedEmailCredentials create_user_and_login(app::SharedApp app)
{
    REQUIRE(app);
    AutoVerifiedEmailCredentials creds;
    app->provider_client<app::App::UsernamePasswordProviderClient>().register_email(
        creds.email, creds.password, [&](util::Optional<app::AppError> error) {
            CHECK(!error);
        });
    app->log_in_with_credentials(realm::app::AppCredentials::username_password(creds.email, creds.password),
                                 [&](std::shared_ptr<realm::SyncUser> user, util::Optional<app::AppError> error) {
                                     REQUIRE(user);
                                     CHECK(!error);
                                 });
    return creds;
}

#endif // REALM_ENABLE_AUTH_TESTS


} // namespace realm
