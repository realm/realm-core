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

#include <catch2/catch.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_database.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/sync_session.hpp>

#include "util/baas_admin_api.hpp"
#include "util/event_loop.hpp"
#include "util/test_utils.hpp"
#include "util/test_file.hpp"

#include <external/json/json.hpp>
#include <realm/sync/access_token.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/uri.hpp>
#include <realm/util/websocket.hpp>

#include <chrono>
#include <condition_variable>
#include <future>
#include <thread>
#include <iostream>
#include <list>
#include <mutex>
#include <thread>

using namespace realm;
using namespace realm::app;
using util::any_cast;
using util::Optional;

using namespace std::string_view_literals;

std::ostream& operator<<(std::ostream& os, util::Optional<app::AppError> error)
{
    if (!error) {
        os << "(none)";
    }
    else {
        os << "AppError(error_code=" << error->error_code
           << ", http_status_code=" << error->http_status_code.value_or(0) << ", message=\"" << error->message
           << "\", link_to_server_logs=\"" << error->link_to_server_logs << "\")";
    }
    return os;
}

namespace {
template <typename Factory>
App::Config get_config(Factory factory)
{
    return {"app name",
            factory,
            util::none,
            util::none,
            Optional<std::string>("A Local App Version"),
            util::none,
            "Object Store Platform Tests",
            "Object Store Platform Version Blah",
            "An sdk version"};
}

std::shared_ptr<SyncUser> log_in(std::shared_ptr<App> app,
                                 app::AppCredentials credentials = app::AppCredentials::anonymous())
{
    std::shared_ptr<SyncUser> user;
    app->log_in_with_credentials(credentials,
                                 [&](std::shared_ptr<SyncUser> user_arg, util::Optional<app::AppError> error) {
                                     REQUIRE_FALSE(error);
                                     REQUIRE(user_arg);
                                     user = std::move(user_arg);
                                 });
    REQUIRE(user);
    return user;
}

app::AppError failed_log_in(std::shared_ptr<App> app,
                            app::AppCredentials credentials = app::AppCredentials::anonymous())
{
    util::Optional<app::AppError> err;
    app->log_in_with_credentials(credentials,
                                 [&](std::shared_ptr<SyncUser> user, util::Optional<app::AppError> error) {
                                     REQUIRE(error);
                                     REQUIRE_FALSE(user);
                                     err = error;
                                 });
    REQUIRE(err);
    return *err;
}

template <typename Transport>
std::unique_ptr<GenericNetworkTransport> factory()
{
    return std::unique_ptr<GenericNetworkTransport>(new Transport);
}
} // namespace


// temporarily disable these tests for now,
// but allow opt-in by building with REALM_ENABLE_AUTH_TESTS=1
#ifndef REALM_ENABLE_AUTH_TESTS
#define REALM_ENABLE_AUTH_TESTS 0
#endif

#if REALM_ENABLE_AUTH_TESTS
namespace {
// This will create a new test app in the baas server at base_url to be used in tests throughout
// tis file.
AppSession get_runtime_app_session(std::string base_url)
{
    static const AppSession cached_app_session = [&] {
        auto cached_app_session = create_app(default_app_config(base_url));
        std::cout << "found app_id: " << cached_app_session.client_app_id << " in stitch config" << std::endl;
        return cached_app_session;
    }();
    return cached_app_session;
}

class SynchronousTestTransport : public GenericNetworkTransport {
public:
    void send_request_to_server(const Request request, std::function<void(const Response)> completion_block) override
    {
        completion_block(do_http_request(request));
    }
};

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

struct AutoVerifiedEmailCredentials : AppCredentials {
    AutoVerifiedEmailCredentials()
    {
        // emails with this prefix will pass through the baas app due to the register function
        email = util::format("realm_tests_do_autoverify%1@%2.com", random_string(10), random_string(10));
        password = random_string(10);
        static_cast<AppCredentials&>(*this) = AppCredentials::username_password(email, password);
    }
    std::string email;
    std::string password;
};

void timed_wait_for(std::function<bool()> condition,
                    std::chrono::milliseconds max_ms = std::chrono::milliseconds(2000))
{
    const auto wait_start = std::chrono::steady_clock::now();
    util::EventLoop::main().run_until([&] {
        REQUIRE(std::chrono::steady_clock::now() - wait_start < max_ms);
        return condition();
    });
}

app::AppCredentials create_user_and_log_in(SharedApp app)
{
    REQUIRE(app);
    AutoVerifiedEmailCredentials creds;
    auto client = app->provider_client<App::UsernamePasswordProviderClient>();
    client.register_email(creds.email, creds.password, [&](Optional<app::AppError> error) {
        REQUIRE_FALSE(error);
    });
    log_in(app, creds);
    return std::move(creds);
}

template <typename Factory>
App::Config get_config(Factory factory, const AppSession& app_session)
{
    return {app_session.client_app_id,
            factory,
            app_session.admin_api.base_url(),
            util::none,
            Optional<std::string>("A Local App Version"),
            util::none,
            "Object Store Platform Tests",
            "Object Store Platform Version Blah",
            "An sdk version"};
}

// Get an App config suitable for integration testing against BaaS
App::Config get_integration_config()
{
    std::string base_url = get_base_url();
    REQUIRE(!base_url.empty());
    auto app_session = get_runtime_app_session(base_url);
    return get_config(factory<SynchronousTestTransport>, app_session);
}
} // namespace

// MARK: - Login with Credentials Tests

TEST_CASE("app: login_with_credentials integration", "[sync][app]") {
    SECTION("login") {
        auto config = get_integration_config();
        TestSyncManager sync_manager(config, {});
        auto app = sync_manager.app();
        bool processed = false;

        int subscribe_processed = 0;
        auto token = app->subscribe([&subscribe_processed](auto& app) {
            if (!subscribe_processed) {
                subscribe_processed++;
                REQUIRE(static_cast<bool>(app.current_user()));
            }
            else {
                subscribe_processed++;
                REQUIRE(!static_cast<bool>(app.current_user()));
            }
        });

        auto user = log_in(app);
        CHECK(!user->device_id().empty());
        CHECK(user->has_device_id());

        app->log_out([&](auto error) {
            REQUIRE_FALSE(error);
            processed = true;
        });

        CHECK(processed);
        CHECK(subscribe_processed == 2);

        app->unsubscribe(token);
    }
}

// MARK: - UsernamePasswordProviderClient Tests

TEST_CASE("app: UsernamePasswordProviderClient integration", "[sync][app]") {
    const std::string base_url = get_base_url();
    AutoVerifiedEmailCredentials creds;
    auto email = creds.email;
    auto password = creds.password;

    auto config = get_integration_config();
    TestSyncManager sync_manager(config, {});
    auto app = sync_manager.app();
    auto client = app->provider_client<App::UsernamePasswordProviderClient>();

    bool processed = false;

    client.register_email(email, password, [&](Optional<app::AppError> error) {
        CAPTURE(email);
        CAPTURE(password);
        REQUIRE_FALSE(error); // first registration success
    });

    SECTION("double registration should fail") {
        client.register_email(email, password, [&](Optional<app::AppError> error) {
            // Error returned states the account has already been created
            REQUIRE(error);
            CHECK(error->message == "name already in use");
            CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::account_name_in_use);
            CHECK(!error->link_to_server_logs.empty());
            CHECK(error->link_to_server_logs.find(base_url) != std::string::npos);
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("double registration should fail") {
        // the server registration function will reject emails that do not contain "realm_tests_do_autoverify"
        std::string email_to_reject = util::format("%1@%2.com", random_string(10), random_string(10));
        client.register_email(email_to_reject, password, [&](Optional<app::AppError> error) {
            REQUIRE(error);
            CHECK(error->message == util::format("failed to confirm user %1", email_to_reject));
            CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::bad_request);
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("can login with registered account") {
        auto user = log_in(app, creds);
        CHECK(user->user_profile().email() == email);
    }

    SECTION("cannot login with wrong password") {
        app->log_in_with_credentials(realm::app::AppCredentials::username_password(email, "boogeyman"),
                                     [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
                                         CHECK(!user);
                                         REQUIRE(error);
                                         REQUIRE(error->error_code.value() ==
                                                 int(ServiceErrorCode::invalid_email_password));
                                         processed = true;
                                     });
        CHECK(processed);
    }

    SECTION("confirm user") {
        client.confirm_user("a_token", "a_token_id", [&](Optional<app::AppError> error) {
            REQUIRE(error);
            CHECK(error->message == "invalid token data");
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("resend confirmation email") {
        client.resend_confirmation_email(email, [&](Optional<app::AppError> error) {
            REQUIRE(error);
            CHECK(error->message == "already confirmed");
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("reset password invalid tokens") {
        client.reset_password(password, "token_sample", "token_id_sample", [&](Optional<app::AppError> error) {
            REQUIRE(error);
            CHECK(error->message == "invalid token data");
            CHECK(!error->link_to_server_logs.empty());
            CHECK(error->link_to_server_logs.find(base_url) != std::string::npos);
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("reset password function success") {
        // the imported test app will accept password reset if the password contains "realm_tests_do_reset" via a
        // function
        std::string accepted_new_password = util::format("realm_tests_do_reset%1", random_string(10));
        client.call_reset_password_function(email, accepted_new_password, {}, [&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("reset password function failure") {
        std::string rejected_password = util::format("%1", random_string(10));
        client.call_reset_password_function(
            email, rejected_password, {"foo", "bar"}, [&](Optional<app::AppError> error) {
                REQUIRE(error);
                CHECK(error->message == util::format("failed to reset password for user %1", email));
                CHECK(error->is_service_error());
                processed = true;
            });
        CHECK(processed);
    }

    SECTION("reset password function for invalid user fails") {
        client.call_reset_password_function(util::format("%1@%2.com", random_string(5), random_string(5)), password,
                                            {"foo", "bar"}, [&](Optional<app::AppError> error) {
                                                REQUIRE(error);
                                                CHECK(error->message == "user not found");
                                                CHECK(error->is_service_error());
                                                CHECK(app::ServiceErrorCode(error->error_code.value()) ==
                                                      app::ServiceErrorCode::user_not_found);
                                                processed = true;
                                            });
        CHECK(processed);
    }

    SECTION("retry custom confirmation") {
        client.retry_custom_confirmation(email, [&](Optional<app::AppError> error) {
            REQUIRE(error);
            CHECK(error->message == "already confirmed");
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("retry custom confirmation for invalid user fails") {
        client.retry_custom_confirmation(
            util::format("%1@%2.com", random_string(5), random_string(5)), [&](Optional<app::AppError> error) {
                REQUIRE(error);
                CHECK(error->message == "user not found");
                CHECK(error->is_service_error());
                CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::user_not_found);
                processed = true;
            });
        CHECK(processed);
    }

    SECTION("log in, remove, log in") {
        CHECK(app->sync_manager()->all_users().size() == 0);
        CHECK(app->sync_manager()->get_current_user() == nullptr);

        auto user = log_in(app, app::AppCredentials::username_password(email, password));
        CHECK(user->user_profile().email() == email);
        CHECK(user->state() == SyncUser::State::LoggedIn);

        app->remove_user(user, [&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
        });
        CHECK(user->state() == SyncUser::State::Removed);

        log_in(app, app::AppCredentials::username_password(email, password));
        CHECK(user->state() == SyncUser::State::Removed);
        CHECK(app->current_user() != user);
        user = app->current_user();
        CHECK(user->user_profile().email() == email);
        CHECK(user->state() == SyncUser::State::LoggedIn);

        app->remove_user(user, [&](Optional<app::AppError> error) {
            REQUIRE(!error);
            CHECK(app->sync_manager()->all_users().size() == 0);
            processed = true;
        });

        CHECK(user->state() == SyncUser::State::Removed);
        CHECK(processed);
        CHECK(app->all_users().size() == 0);
    }
}

// MARK: - UserAPIKeyProviderClient Tests

TEST_CASE("app: UserAPIKeyProviderClient integration", "[sync][app]") {
    TestSyncManager sync_manager(get_integration_config(), {});
    auto app = sync_manager.app();
    auto client = app->provider_client<App::UserAPIKeyProviderClient>();

    bool processed = false;
    App::UserAPIKey api_key;

    SECTION("api-key") {
        create_user_and_log_in(app);
        std::shared_ptr<SyncUser> logged_in_user = app->current_user();
        auto api_key_name = util::format("%1", random_string(15));
        client.create_api_key(api_key_name, logged_in_user,
                              [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                                  REQUIRE_FALSE(error);
                                  CHECK(user_api_key.name == api_key_name);
                                  api_key = user_api_key;
                              });

        client.fetch_api_key(api_key.id, logged_in_user,
                             [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                                 REQUIRE_FALSE(error);
                                 CHECK(user_api_key.name == api_key_name);
                                 CHECK(user_api_key.id == api_key.id);
                             });

        client.fetch_api_keys(logged_in_user, [&](std::vector<App::UserAPIKey> api_keys, Optional<AppError> error) {
            CHECK(api_keys.size() == 1);
            for (auto key : api_keys) {
                CHECK(key.id.to_string() == api_key.id.to_string());
                CHECK(api_key.name == api_key_name);
                CHECK(key.id == api_key.id);
            }
            REQUIRE_FALSE(error);
        });

        client.enable_api_key(api_key.id, logged_in_user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        client.fetch_api_key(api_key.id, logged_in_user,
                             [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                                 REQUIRE_FALSE(error);
                                 CHECK(user_api_key.disabled == false);
                                 CHECK(user_api_key.name == api_key_name);
                                 CHECK(user_api_key.id == api_key.id);
                             });

        client.disable_api_key(api_key.id, logged_in_user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        client.fetch_api_key(api_key.id, logged_in_user,
                             [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                                 REQUIRE_FALSE(error);
                                 CHECK(user_api_key.disabled == true);
                                 CHECK(user_api_key.name == api_key_name);
                             });

        client.delete_api_key(api_key.id, logged_in_user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        client.fetch_api_key(api_key.id, logged_in_user,
                             [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                                 CHECK(user_api_key.name == "");
                                 CHECK(error);
                                 processed = true;
                             });

        CHECK(processed);
    }

    SECTION("api-key without a user") {
        std::shared_ptr<SyncUser> no_user = nullptr;
        auto api_key_name = util::format("%1", random_string(15));
        client.create_api_key(api_key_name, no_user,
                              [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                                  REQUIRE(error);
                                  CHECK(error->is_service_error());
                                  CHECK(error->message == "must authenticate first");
                                  CHECK(user_api_key.name == "");
                              });

        client.fetch_api_key(api_key.id, no_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->message == "must authenticate first");
            CHECK(user_api_key.name == "");
        });

        client.fetch_api_keys(no_user, [&](std::vector<App::UserAPIKey> api_keys, Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->message == "must authenticate first");
            CHECK(api_keys.size() == 0);
        });

        client.enable_api_key(api_key.id, no_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->message == "must authenticate first");
        });

        client.fetch_api_key(api_key.id, no_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->message == "must authenticate first");
            CHECK(user_api_key.name == "");
        });

        client.disable_api_key(api_key.id, no_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->message == "must authenticate first");
        });

        client.fetch_api_key(api_key.id, no_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->message == "must authenticate first");
            CHECK(user_api_key.name == "");
        });

        client.delete_api_key(api_key.id, no_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->message == "must authenticate first");
        });

        client.fetch_api_key(api_key.id, no_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
            CHECK(user_api_key.name == "");
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->message == "must authenticate first");
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("api-key against the wrong user") {
        create_user_and_log_in(app);
        std::shared_ptr<SyncUser> first_user = app->current_user();
        create_user_and_log_in(app);
        std::shared_ptr<SyncUser> second_user = app->current_user();
        REQUIRE(first_user != second_user);
        auto api_key_name = util::format("%1", random_string(15));
        App::UserAPIKey api_key;
        App::UserAPIKeyProviderClient provider = app->provider_client<App::UserAPIKeyProviderClient>();

        provider.create_api_key(api_key_name, first_user,
                                [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                                    REQUIRE_FALSE(error);
                                    CHECK(user_api_key.name == api_key_name);
                                    api_key = user_api_key;
                                });

        provider.fetch_api_key(api_key.id, first_user,
                               [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                                   REQUIRE_FALSE(error);
                                   CHECK(user_api_key.name == api_key_name);
                                   CHECK(user_api_key.id.to_string() == user_api_key.id.to_string());
                               });

        provider.fetch_api_key(
            api_key.id, second_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                REQUIRE(error);
                CHECK(error->message == "API key not found");
                CHECK(error->is_service_error());
                CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
                CHECK(user_api_key.name == "");
            });

        provider.fetch_api_keys(first_user, [&](std::vector<App::UserAPIKey> api_keys, Optional<AppError> error) {
            CHECK(api_keys.size() == 1);
            for (auto api_key : api_keys) {
                CHECK(api_key.name == api_key_name);
            }
            REQUIRE_FALSE(error);
        });

        provider.fetch_api_keys(second_user, [&](std::vector<App::UserAPIKey> api_keys, Optional<AppError> error) {
            CHECK(api_keys.size() == 0);
            REQUIRE_FALSE(error);
        });

        provider.enable_api_key(api_key.id, first_user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        provider.enable_api_key(api_key.id, second_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->message == "API key not found");
            CHECK(error->is_service_error());
            CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
        });

        provider.fetch_api_key(api_key.id, first_user,
                               [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                                   REQUIRE_FALSE(error);
                                   CHECK(user_api_key.disabled == false);
                                   CHECK(user_api_key.name == api_key_name);
                               });

        provider.fetch_api_key(
            api_key.id, second_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                REQUIRE(error);
                CHECK(user_api_key.name == "");
                CHECK(error->message == "API key not found");
                CHECK(error->is_service_error());
                CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
            });

        provider.disable_api_key(api_key.id, first_user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        provider.disable_api_key(api_key.id, second_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->message == "API key not found");
            CHECK(error->is_service_error());
            CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
        });

        provider.fetch_api_key(api_key.id, first_user,
                               [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                                   REQUIRE_FALSE(error);
                                   CHECK(user_api_key.disabled == true);
                                   CHECK(user_api_key.name == api_key_name);
                               });

        provider.fetch_api_key(
            api_key.id, second_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                REQUIRE(error);
                CHECK(user_api_key.name == "");
                CHECK(error->message == "API key not found");
                CHECK(error->is_service_error());
                CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
            });

        provider.delete_api_key(api_key.id, second_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->message == "API key not found");
            CHECK(error->is_service_error());
            CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
        });

        provider.delete_api_key(api_key.id, first_user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        provider.fetch_api_key(
            api_key.id, first_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                CHECK(user_api_key.name == "");
                REQUIRE(error);
                CHECK(error->message == "API key not found");
                CHECK(error->is_service_error());
                CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
                processed = true;
            });

        provider.fetch_api_key(
            api_key.id, second_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                CHECK(user_api_key.name == "");
                REQUIRE(error);
                CHECK(error->message == "API key not found");
                CHECK(error->is_service_error());
                CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
                processed = true;
            });

        CHECK(processed);
    }
}

// MARK: - Auth Providers Function Tests

TEST_CASE("app: auth providers function integration", "[sync][app]") {
    TestSyncManager sync_manager(get_integration_config(), {});
    auto app = sync_manager.app();

    SECTION("auth providers function integration") {
        bson::BsonDocument function_params{{"realmCustomAuthFuncUserId", "123456"}};
        auto credentials = app::AppCredentials::function(function_params);
        auto user = log_in(app, credentials);
        REQUIRE(user->provider_type() == IdentityProviderFunction);
    }
}

// MARK: - Link User Tests

TEST_CASE("app: link_user integration", "[sync][app]") {
    TestSyncManager sync_manager(get_integration_config(), {});
    auto app = sync_manager.app();

    SECTION("link_user intergration") {
        AutoVerifiedEmailCredentials creds;
        bool processed = false;
        std::shared_ptr<SyncUser> sync_user;

        app->provider_client<App::UsernamePasswordProviderClient>().register_email(
            creds.email, creds.password, [&](Optional<app::AppError> error) {
                CAPTURE(creds.email);
                CAPTURE(creds.password);
                REQUIRE_FALSE(error); // first registration success
            });

        sync_user = log_in(app);
        CHECK(sync_user->provider_type() == IdentityProviderAnonymous);

        app->link_user(sync_user, creds, [&](std::shared_ptr<SyncUser> user, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(user);
            CHECK(user->identity() == sync_user->identity());
            CHECK(user->identities().size() == 2);
            processed = true;
        });

        CHECK(processed);
    }
}

// MARK: - Call Function Tests

TEST_CASE("app: call function", "[sync][app]") {
    TestSyncManager sync_manager(get_integration_config(), {});
    auto app = sync_manager.app();

    create_user_and_log_in(app);

    bson::BsonArray toSum(5);
    std::iota(toSum.begin(), toSum.end(), static_cast<int64_t>(1));
    const auto checkFn = [](Optional<app::AppError> error, Optional<int64_t> sum) {
        REQUIRE(!error);
        CHECK(*sum == 15);
    };
    app->call_function<int64_t>("sumFunc", toSum, checkFn);
    app->call_function<int64_t>(app->sync_manager()->get_current_user(), "sumFunc", toSum, checkFn);
}

// MARK: - Remote Mongo Client Tests

TEST_CASE("app: remote mongo client", "[sync][app]") {
    TestSyncManager sync_manager(get_integration_config(), {});
    auto app = sync_manager.app();

    create_user_and_log_in(app);

    auto remote_client = app->current_user()->mongo_client("BackingDB");
    auto db = remote_client.db(get_runtime_app_session("").config.mongo_dbname);
    auto dog_collection = db["Dog"];
    auto person_collection = db["Person"];

    bson::BsonDocument dog_document{{"name", "fido"}, {"breed", "king charles"}};

    bson::BsonDocument dog_document2{{"name", "bob"}, {"breed", "french bulldog"}};

    auto dog3_object_id = ObjectId::gen();
    bson::BsonDocument dog_document3{
        {"_id", dog3_object_id},
        {"name", "petunia"},
        {"breed", "french bulldog"},
    };

    bson::BsonDocument person_document{
        {"firstName", "John"},
        {"lastName", "Johnson"},
        {"age", 30},
    };

    bson::BsonDocument person_document2{
        {"firstName", "Bob"},
        {"lastName", "Johnson"},
        {"age", 30},
    };

    bson::BsonDocument bad_document{{"bad", "value"}};

    dog_collection.delete_many(dog_document, [&](uint64_t, Optional<app::AppError> error) {
        REQUIRE_FALSE(error);
    });

    dog_collection.delete_many(dog_document2, [&](uint64_t, Optional<app::AppError> error) {
        REQUIRE_FALSE(error);
    });

    dog_collection.delete_many({}, [&](uint64_t, Optional<app::AppError> error) {
        REQUIRE_FALSE(error);
    });

    dog_collection.delete_many(person_document, [&](uint64_t, Optional<app::AppError> error) {
        REQUIRE_FALSE(error);
    });

    dog_collection.delete_many(person_document2, [&](uint64_t, Optional<app::AppError> error) {
        REQUIRE_FALSE(error);
    });

    SECTION("insert") {
        bool processed = false;
        ObjectId dog_object_id;
        ObjectId dog2_object_id;

        dog_collection.insert_one_bson(bad_document, [&](Optional<app::AppError> error, Optional<bson::Bson> bson) {
            CHECK(error);
            CHECK(!bson);
        });

        dog_collection.insert_one_bson(dog_document3, [&](Optional<app::AppError> error, Optional<bson::Bson> value) {
            REQUIRE_FALSE(error);
            auto bson = static_cast<bson::BsonDocument>(*value);
            CHECK(static_cast<ObjectId>(bson["insertedId"]) == dog3_object_id);
        });

        dog_collection.delete_many({}, [&](uint64_t, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
        });

        dog_collection.insert_one(bad_document, [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
            CHECK(error);
            CHECK(!object_id);
        });

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            dog_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.insert_one(dog_document2, [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            dog2_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.insert_one(dog_document3, [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(object_id->type() == bson::Bson::Type::ObjectId);
            CHECK(static_cast<ObjectId>(*object_id) == dog3_object_id);
        });

        person_document["dogs"] = bson::BsonArray({dog_object_id, dog2_object_id, dog3_object_id});
        person_collection.insert_one(person_document,
                                     [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
                                         REQUIRE_FALSE(error);
                                         CHECK((*object_id).to_string() != "");
                                     });

        dog_collection.delete_many({}, [&](uint64_t, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
        });

        bson::BsonArray documents{
            dog_document,
            dog_document2,
            dog_document3,
        };

        dog_collection.insert_many_bson(documents, [&](Optional<app::AppError> error, Optional<bson::Bson> value) {
            REQUIRE_FALSE(error);
            auto bson = static_cast<bson::BsonDocument>(*value);
            auto insertedIds = static_cast<bson::BsonArray>(bson["insertedIds"]);
        });

        dog_collection.delete_many({}, [&](uint64_t, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
        });

        dog_collection.insert_many(documents,
                                   [&](std::vector<bson::Bson> inserted_docs, Optional<app::AppError> error) {
                                       REQUIRE_FALSE(error);
                                       CHECK(inserted_docs.size() == 3);
                                       CHECK(inserted_docs[0].type() == bson::Bson::Type::ObjectId);
                                       CHECK(inserted_docs[1].type() == bson::Bson::Type::ObjectId);
                                       CHECK(inserted_docs[2].type() == bson::Bson::Type::ObjectId);
                                       CHECK(static_cast<ObjectId>(inserted_docs[2]) == dog3_object_id);
                                       processed = true;
                                   });

        CHECK(processed);
    }

    SECTION("find") {
        bool processed = false;

        dog_collection.find(dog_document,
                            [&](Optional<bson::BsonArray> document_array, Optional<app::AppError> error) {
                                REQUIRE_FALSE(error);
                                CHECK((*document_array).size() == 0);
                            });

        dog_collection.find_bson(dog_document, {}, [&](Optional<app::AppError> error, Optional<bson::Bson> bson) {
            REQUIRE_FALSE(error);
            CHECK(static_cast<bson::BsonArray>(*bson).size() == 0);
        });

        dog_collection.find_one(dog_document,
                                [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                                    REQUIRE_FALSE(error);
                                    CHECK(!document);
                                });

        dog_collection.find_one_bson(dog_document, {}, [&](Optional<app::AppError> error, Optional<bson::Bson> bson) {
            REQUIRE_FALSE(error);
            CHECK((!bson || bson::holds_alternative<util::None>(*bson)));
        });

        ObjectId dog_object_id;
        ObjectId dog2_object_id;

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            dog_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.insert_one(dog_document2, [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            dog2_object_id = static_cast<ObjectId>(*object_id);
        });

        person_document["dogs"] = bson::BsonArray({dog_object_id, dog2_object_id});
        person_collection.insert_one(person_document,
                                     [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
                                         REQUIRE_FALSE(error);
                                         CHECK((*object_id).to_string() != "");
                                     });

        dog_collection.find(dog_document, [&](Optional<bson::BsonArray> documents, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*documents).size() == 1);
        });

        dog_collection.find_bson(dog_document, {}, [&](Optional<app::AppError> error, Optional<bson::Bson> bson) {
            REQUIRE_FALSE(error);
            CHECK(static_cast<bson::BsonArray>(*bson).size() == 1);
        });

        person_collection.find(person_document,
                               [&](Optional<bson::BsonArray> documents, Optional<app::AppError> error) {
                                   REQUIRE_FALSE(error);
                                   CHECK((*documents).size() == 1);
                               });

        app::MongoCollection::FindOptions options{
            2,                                                               // document limit
            util::Optional<bson::BsonDocument>({{"name", 1}, {"breed", 1}}), // project
            util::Optional<bson::BsonDocument>({{"breed", 1}})               // sort
        };

        dog_collection.find(dog_document, options,
                            [&](Optional<bson::BsonArray> document_array, Optional<app::AppError> error) {
                                REQUIRE_FALSE(error);
                                CHECK((*document_array).size() == 1);
                            });

        dog_collection.find({{"name", "fido"}}, options,
                            [&](Optional<bson::BsonArray> document_array, Optional<app::AppError> error) {
                                REQUIRE_FALSE(error);
                                CHECK((*document_array).size() == 1);
                                auto king_charles = static_cast<bson::BsonDocument>((*document_array)[0]);
                                CHECK(king_charles["breed"] == "king charles");
                            });

        dog_collection.find_one(dog_document,
                                [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                                    REQUIRE_FALSE(error);
                                    auto name = (*document)["name"];
                                    CHECK(name == "fido");
                                });

        dog_collection.find_one(dog_document, options,
                                [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                                    REQUIRE_FALSE(error);
                                    auto name = (*document)["name"];
                                    CHECK(name == "fido");
                                });

        dog_collection.find_one_bson(dog_document, options,
                                     [&](Optional<app::AppError> error, Optional<bson::Bson> bson) {
                                         REQUIRE_FALSE(error);
                                         auto name = (static_cast<bson::BsonDocument>(*bson))["name"];
                                         CHECK(name == "fido");
                                     });

        dog_collection.find(dog_document, [&](Optional<bson::BsonArray> documents, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*documents).size() == 1);
        });

        dog_collection.find_one_and_delete(dog_document,
                                           [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                                               REQUIRE_FALSE(error);
                                               REQUIRE(document);
                                           });

        dog_collection.find_one_and_delete({{}},
                                           [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                                               REQUIRE_FALSE(error);
                                               REQUIRE(document);
                                           });

        dog_collection.find_one_and_delete({{"invalid", "key"}},
                                           [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                                               REQUIRE_FALSE(error);
                                               CHECK(!document);
                                           });

        dog_collection.find_one_and_delete_bson({{"invalid", "key"}}, {},
                                                [&](Optional<app::AppError> error, Optional<bson::Bson> bson) {
                                                    REQUIRE_FALSE(error);
                                                    CHECK((!bson || bson::holds_alternative<util::None>(*bson)));
                                                });

        dog_collection.find(dog_document, [&](Optional<bson::BsonArray> documents, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*documents).size() == 0);
            processed = true;
        });

        CHECK(processed);
    }

    SECTION("count and aggregate") {
        bool processed = false;

        ObjectId dog_object_id;
        ObjectId dog2_object_id;

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
        });

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            dog_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.insert_one(dog_document2, [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            dog2_object_id = static_cast<ObjectId>(*object_id);
        });

        person_document["dogs"] = bson::BsonArray({dog_object_id, dog2_object_id});
        person_collection.insert_one(person_document,
                                     [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
                                         REQUIRE_FALSE(error);
                                         CHECK((*object_id).to_string() != "");
                                     });

        bson::BsonDocument match{{"$match", bson::BsonDocument({{"name", "fido"}})}};

        bson::BsonDocument group{{"$group", bson::BsonDocument({{"_id", "$name"}})}};

        bson::BsonArray pipeline{match, group};

        dog_collection.aggregate(pipeline, [&](Optional<bson::BsonArray> documents, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*documents).size() == 1);
        });

        dog_collection.aggregate_bson(pipeline, [&](Optional<app::AppError> error, Optional<bson::Bson> bson) {
            REQUIRE_FALSE(error);
            CHECK(static_cast<bson::BsonArray>(*bson).size() == 1);
        });

        dog_collection.count({{"breed", "king charles"}}, [&](uint64_t count, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(count == 2);
        });

        dog_collection.count_bson({{"breed", "king charles"}}, 0,
                                  [&](Optional<app::AppError> error, Optional<bson::Bson> bson) {
                                      REQUIRE_FALSE(error);
                                      CHECK(static_cast<int64_t>(*bson) == 2);
                                  });

        dog_collection.count({{"breed", "french bulldog"}}, [&](uint64_t count, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(count == 1);
        });

        dog_collection.count({{"breed", "king charles"}}, 1, [&](uint64_t count, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(count == 1);
        });

        person_collection.count(
            {{"firstName", "John"}, {"lastName", "Johnson"}, {"age", bson::BsonDocument({{"$gt", 25}})}}, 1,
            [&](uint64_t count, Optional<app::AppError> error) {
                REQUIRE_FALSE(error);
                CHECK(count == 1);
                processed = true;
            });

        CHECK(processed);
    }

    SECTION("find and update") {
        bool processed = false;

        app::MongoCollection::FindOneAndModifyOptions find_and_modify_options{
            util::Optional<bson::BsonDocument>({{"name", 1}, {"breed", 1}}), // project
            util::Optional<bson::BsonDocument>({{"name", 1}}),               // sort,
            true,                                                            // upsert
            true                                                             // return new doc
        };

        dog_collection.find_one_and_update(dog_document, dog_document2,
                                           [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                                               REQUIRE_FALSE(error);
                                               CHECK(!document);
                                           });

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
        });

        dog_collection.find_one_and_update(dog_document, dog_document2, find_and_modify_options,
                                           [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                                               REQUIRE_FALSE(error);
                                               auto breed = static_cast<std::string>((*document)["breed"]);
                                               CHECK(breed == "french bulldog");
                                           });

        dog_collection.find_one_and_update(dog_document2, dog_document, find_and_modify_options,
                                           [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                                               REQUIRE_FALSE(error);
                                               auto breed = static_cast<std::string>((*document)["breed"]);
                                               CHECK(breed == "king charles");
                                           });

        dog_collection.find_one_and_update_bson(dog_document, dog_document2, find_and_modify_options,
                                                [&](Optional<app::AppError> error, Optional<bson::Bson> bson) {
                                                    REQUIRE_FALSE(error);
                                                    auto breed = static_cast<std::string>(
                                                        static_cast<bson::BsonDocument>(*bson)["breed"]);
                                                    CHECK(breed == "french bulldog");
                                                });

        dog_collection.find_one_and_update_bson(dog_document2, dog_document, find_and_modify_options,
                                                [&](Optional<app::AppError> error, Optional<bson::Bson> bson) {
                                                    REQUIRE_FALSE(error);
                                                    auto breed = static_cast<std::string>(
                                                        static_cast<bson::BsonDocument>(*bson)["breed"]);
                                                    CHECK(breed == "king charles");
                                                });

        dog_collection.find_one_and_update({{"name", "invalid name"}}, {{"name", "some name"}},
                                           [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                                               REQUIRE_FALSE(error);
                                               CHECK(!document);
                                               processed = true;
                                           });
        CHECK(processed);
        processed = false;

        dog_collection.find_one_and_update({{"name", "invalid name"}}, {{}}, find_and_modify_options,
                                           [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                                               REQUIRE(error);
                                               CHECK(error->message == "insert not permitted");
                                               CHECK(!document);
                                               processed = true;
                                           });
        CHECK(processed);
    }

    SECTION("update") {
        bool processed = false;
        ObjectId dog_object_id;

        dog_collection.update_one(dog_document, dog_document2, true,
                                  [&](app::MongoCollection::UpdateResult result, Optional<app::AppError> error) {
                                      REQUIRE_FALSE(error);
                                      CHECK((*result.upserted_id).to_string() != "");
                                  });

        dog_collection.update_one(dog_document2, dog_document,
                                  [&](app::MongoCollection::UpdateResult result, Optional<app::AppError> error) {
                                      REQUIRE_FALSE(error);
                                      CHECK(!result.upserted_id);
                                  });

        dog_collection.delete_many({}, [&](uint64_t, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
        });

        dog_collection.update_one_bson(dog_document, dog_document2, true,
                                       [&](Optional<app::AppError> error, Optional<bson::Bson> bson) {
                                           REQUIRE_FALSE(error);
                                           auto upserted_id = static_cast<bson::BsonDocument>(*bson)["upsertedId"];

                                           REQUIRE(upserted_id.type() == bson::Bson::Type::ObjectId);
                                       });

        dog_collection.update_one_bson(dog_document2, dog_document, true,
                                       [&](Optional<app::AppError> error, Optional<bson::Bson> bson) {
                                           REQUIRE_FALSE(error);
                                           auto document = static_cast<bson::BsonDocument>(*bson);
                                           auto foundUpsertedId = document.find("upsertedId") != document.end();
                                           REQUIRE(!foundUpsertedId);
                                       });

        person_document["dogs"] = bson::BsonArray();
        bson::BsonDocument person_document_copy = bson::BsonDocument(person_document);
        person_document_copy["dogs"] = bson::BsonArray({dog_object_id});
        person_collection.update_one(person_document, person_document, true,
                                     [&](app::MongoCollection::UpdateResult, Optional<app::AppError> error) {
                                         REQUIRE_FALSE(error);
                                         processed = true;
                                     });

        CHECK(processed);
    }

    SECTION("update many") {
        bool processed = false;

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
        });

        dog_collection.update_many(dog_document2, dog_document, true,
                                   [&](app::MongoCollection::UpdateResult result, Optional<app::AppError> error) {
                                       REQUIRE_FALSE(error);
                                       CHECK((*result.upserted_id).to_string() != "");
                                   });

        dog_collection.update_many(dog_document2, dog_document,
                                   [&](app::MongoCollection::UpdateResult result, Optional<app::AppError> error) {
                                       REQUIRE_FALSE(error);
                                       CHECK(!result.upserted_id);
                                       processed = true;
                                   });

        CHECK(processed);
    }

    SECTION("find and replace") {
        bool processed = false;
        ObjectId dog_object_id;
        ObjectId person_object_id;

        app::MongoCollection::FindOneAndModifyOptions find_and_modify_options{
            util::Optional<bson::BsonDocument>({{"name", "fido"}}), // project
            util::Optional<bson::BsonDocument>({{"name", 1}}),      // sort,
            true,                                                   // upsert
            true                                                    // return new doc
        };

        dog_collection.find_one_and_replace(
            dog_document, dog_document2, [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                REQUIRE_FALSE(error);
                CHECK(!document);
            });

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            dog_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.find_one_and_replace(
            dog_document, dog_document2, [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                REQUIRE_FALSE(error);
                auto name = static_cast<std::string>((*document)["name"]);
                CHECK(name == "fido");
            });

        dog_collection.find_one_and_replace(
            dog_document2, dog_document, find_and_modify_options,
            [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                REQUIRE_FALSE(error);
                auto name = static_cast<std::string>((*document)["name"]);
                CHECK(static_cast<std::string>(name) == "fido");
            });

        person_document["dogs"] = bson::BsonArray({dog_object_id});
        person_document2["dogs"] = bson::BsonArray({dog_object_id});
        person_collection.insert_one(person_document,
                                     [&](Optional<bson::Bson> object_id, Optional<app::AppError> error) {
                                         REQUIRE_FALSE(error);
                                         CHECK((*object_id).to_string() != "");
                                         person_object_id = static_cast<ObjectId>(*object_id);
                                     });

        app::MongoCollection::FindOneAndModifyOptions person_find_and_modify_options{
            util::Optional<bson::BsonDocument>({{"firstName", 1}}), // project
            util::Optional<bson::BsonDocument>({{"firstName", 1}}), // sort,
            false,                                                  // upsert
            true                                                    // return new doc
        };

        person_collection.find_one_and_replace(
            person_document, person_document2,
            [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                REQUIRE_FALSE(error);
                auto name = static_cast<std::string>((*document)["firstName"]);
                // Should return the old document
                CHECK(name == "John");
                processed = true;
            });

        person_collection.find_one_and_replace(
            person_document2, person_document, person_find_and_modify_options,
            [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                REQUIRE_FALSE(error);
                auto name = static_cast<std::string>((*document)["firstName"]);
                // Should return new document, Bob -> John
                CHECK(name == "John");
            });

        person_collection.find_one_and_replace(
            {{"invalid", "item"}}, {{}}, [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                // If a document is not found then null will be returned for the document and no error will be
                // returned
                REQUIRE_FALSE(error);
                CHECK(!document);
            });

        person_collection.find_one_and_replace(
            {{"invalid", "item"}}, {{}}, person_find_and_modify_options,
            [&](Optional<bson::BsonDocument> document, Optional<app::AppError> error) {
                REQUIRE_FALSE(error);
                CHECK(!document);
                processed = true;
            });

        CHECK(processed);
    }

    SECTION("delete") {

        bool processed = false;

        bson::BsonArray documents;
        documents.assign(3, dog_document);

        dog_collection.insert_many(documents,
                                   [&](std::vector<bson::Bson> inserted_docs, Optional<app::AppError> error) {
                                       REQUIRE_FALSE(error);
                                       CHECK(inserted_docs.size() == 3);
                                   });

        app::MongoCollection::FindOneAndModifyOptions find_and_modify_options{
            util::Optional<bson::BsonDocument>({{"name", "fido"}}), // project
            util::Optional<bson::BsonDocument>({{"name", 1}}),      // sort,
            true,                                                   // upsert
            true                                                    // return new doc
        };

        dog_collection.delete_one(dog_document, [&](uint64_t deleted_count, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(deleted_count >= 1);
        });

        dog_collection.delete_many(dog_document, [&](uint64_t deleted_count, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(deleted_count >= 1);
            processed = true;
        });

        person_collection.delete_many_bson(
            person_document, [&](Optional<app::AppError> error, Optional<bson::Bson> bson) {
                REQUIRE_FALSE(error);
                CHECK(static_cast<int32_t>(static_cast<bson::BsonDocument>(*bson)["deletedCount"]) >= 1);
                processed = true;
            });

        CHECK(processed);
    }
}

// MARK: - Push Notifications Tests

TEST_CASE("app: push notifications", "[sync][app]") {
    TestSyncManager sync_manager(get_integration_config(), {});
    auto app = sync_manager.app();

    create_user_and_log_in(app);
    std::shared_ptr<SyncUser> sync_user = app->current_user();

    SECTION("register") {
        bool processed;

        app->push_notification_client("gcm").register_device("hello", sync_user, [&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            processed = true;
        });

        CHECK(processed);
    }
    /*
        // FIXME: It seems this test fails when the two register_device calls are invoked too quickly,
        // The error returned will be 'Device not found' on the second register_device call.
        SECTION("register twice") {
            // registering the same device twice should not result in an error
            bool processed;

            app->push_notification_client("gcm").register_device("hello",
                                                                 sync_user,
                                                                 [&](Optional<app::AppError> error) {
                REQUIRE_FALSE(error);
            });

            app->push_notification_client("gcm").register_device("hello",
                                                                 sync_user,
                                                                 [&](Optional<app::AppError> error) {
                REQUIRE_FALSE(error);
                processed = true;
            });

            CHECK(processed);
        }
    */
    SECTION("deregister") {
        bool processed;

        app->push_notification_client("gcm").deregister_device(sync_user, [&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("register with unavailable service") {
        bool processed;

        app->push_notification_client("gcm_blah")
            .register_device("hello", sync_user, [&](Optional<app::AppError> error) {
                REQUIRE(error);
                CHECK(error->message == "service not found: 'gcm_blah'");
                processed = true;
            });
        CHECK(processed);
    }

    SECTION("register with logged out user") {
        bool processed;

        app->log_out([=](util::Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        app->push_notification_client("gcm").register_device("hello", sync_user, [&](Optional<app::AppError> error) {
            REQUIRE(error);
            processed = true;
        });

        app->push_notification_client("gcm").register_device("hello", nullptr, [&](Optional<app::AppError> error) {
            REQUIRE(error);
            processed = true;
        });

        CHECK(processed);
    }
}

// MARK: - Token refresh

TEST_CASE("app: token refresh", "[sync][app][token]") {
    TestSyncManager sync_manager(get_integration_config(), {});
    auto app = sync_manager.app();

    create_user_and_log_in(app);
    std::shared_ptr<SyncUser> sync_user = app->current_user();
    sync_user->update_access_token(ENCODE_FAKE_JWT("fake_access_token"));

    auto remote_client = app->current_user()->mongo_client("BackingDB");
    auto db = remote_client.db(get_runtime_app_session("").config.mongo_dbname);
    auto dog_collection = db["Dog"];
    bson::BsonDocument dog_document{{"name", "fido"}, {"breed", "king charles"}};

    SECTION("access token should refresh") {
        /*
         Expected sequence of events:
         - `find_one` tries to hit the server with a bad access token
         - Server returns an error because of the bad token, error should be something like:
            {\"error\":\"json: cannot unmarshal array into Go value of type map[string]interface
         {}\",\"link\":\"http://localhost:9090/groups/5f84167e776aa0f9dc27081a/apps/5f841686776aa0f9dc270876/logs?co_id=5f844c8c776aa0f9dc273db6\"}
            http_status_code = 401
            custom_status_code = 0
         - App::handle_auth_failure is then called and an attempt to refresh the access token will be peformed.
         - If the token refresh was successful, the original request will retry and we should expect no error in the
         callback of `find_one`
         */
        dog_collection.find_one(dog_document, [&](Optional<bson::BsonDocument>, Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
        });
    }
}

// MARK: - Sync Tests

TEST_CASE("app: mixed lists with object links", "[sync][app]") {
    std::string base_url = get_base_url();
    const std::string valid_pk_name = "_id";
    REQUIRE(!base_url.empty());

    Schema schema{
        {"TopLevel",
         {
             {valid_pk_name, PropertyType::ObjectId, Property::IsPrimary{true}},
             {"mixed_array", PropertyType::Mixed | PropertyType::Array | PropertyType::Nullable},
         }},
        {"Target",
         {
             {valid_pk_name, PropertyType::ObjectId, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
         }},
    };

    auto server_app_config = minimal_app_config(base_url, "set_new_embedded_object", schema);
    auto app_session = create_app(server_app_config);
    auto app_config = get_config(factory<SynchronousTestTransport>, app_session);
    auto partition = random_string(100);

    auto email = std::string("realm_tests_do_autoverify-test@example.com");
    auto password = std::string{"password"};
    auto creds = app::AppCredentials::username_password(email, password);
    auto obj_id = ObjectId::gen();
    auto target_id = ObjectId::gen();
    auto mixed_list_values = AnyVector{
        Mixed{int64_t(1234)},
        Mixed{},
        Mixed{target_id},
    };
    {
        TestSyncManager sync_manager(app_config, {});
        auto app = sync_manager.app();
        app->provider_client<App::UsernamePasswordProviderClient>().register_email(
            email, password, [&](Optional<app::AppError> error) {
                REQUIRE_FALSE(error);
            });
        log_in(app, creds);
        SyncTestFile config(app, partition, schema);
        auto realm = Realm::get_shared_realm(config);

        CppContext c(realm);
        realm->begin_transaction();
        auto target_obj =
            Object::create(c, realm, "Target",
                           util::Any(AnyDict{{valid_pk_name, target_id}, {"value", static_cast<int64_t>(1234)}}));
        mixed_list_values.push_back(Mixed(target_obj.obj().get_link()));

        Object::create(c, realm, "TopLevel",
                       util::Any(AnyDict{
                           {valid_pk_name, obj_id},
                           {"mixed_array", mixed_list_values},
                       }),
                       CreatePolicy::ForceCreate);
        realm->commit_transaction();
        CHECK(!wait_for_upload(*realm));
    }

    {
        TestSyncManager sync_manager(app_config, {});
        auto app = sync_manager.app();
        auto user = log_in(app, creds);

        SyncTestFile config(app, partition, schema);
        auto realm = Realm::get_shared_realm(config);

        CHECK(!wait_for_download(*realm));
        CppContext c(realm);
        auto obj = Object::get_for_primary_key(c, realm, "TopLevel", util::Any{obj_id});
        auto list = any_cast<List&&>(obj.get_property_value<util::Any>(c, "mixed_array"));
        for (size_t idx = 0; idx < list.size(); ++idx) {
            Mixed mixed = list.get_any(idx);
            CHECK(mixed == any_cast<Mixed>(mixed_list_values[idx]));
        }
    }
}

TEST_CASE("app: set new embedded object", "[sync][app]") {
    std::string base_url = get_base_url();
    const std::string valid_pk_name = "_id";
    REQUIRE(!base_url.empty());

    Schema schema{
        {"TopLevel",
         {
             {valid_pk_name, PropertyType::ObjectId, Property::IsPrimary{true}},
             {"array_of_objs", PropertyType::Object | PropertyType::Array, "TopLevel_array_of_objs"},
             {"embedded_obj", PropertyType::Object | PropertyType::Nullable, "TopLevel_embedded_obj"},
             {"embedded_dict", PropertyType::Object | PropertyType::Dictionary | PropertyType::Nullable,
              "TopLevel_embedded_dict"},
         }},
        {"TopLevel_array_of_objs",
         ObjectSchema::IsEmbedded{true},
         {
             {"array", PropertyType::Int | PropertyType::Array},
         }},
        {"TopLevel_embedded_obj",
         ObjectSchema::IsEmbedded{true},
         {
             {"array", PropertyType::Int | PropertyType::Array},
         }},
        {"TopLevel_embedded_dict",
         ObjectSchema::IsEmbedded{true},
         {
             {"array", PropertyType::Int | PropertyType::Array},
         }},
    };

    auto server_app_config = minimal_app_config(base_url, "set_new_embedded_object", schema);
    auto app_session = create_app(server_app_config);
    auto app_config = get_config(factory<SynchronousTestTransport>, app_session);
    auto partition = random_string(100);

    auto array_of_objs_id = ObjectId::gen();
    auto embedded_obj_id = ObjectId::gen();
    auto dict_obj_id = ObjectId::gen();
    auto email = std::string("realm_tests_do_autoverify-test@example.com");
    auto password = std::string{"password"};
    auto creds = app::AppCredentials::username_password(email, password);

    {
        TestSyncManager sync_manager(app_config, {});
        auto app = sync_manager.app();
        app->provider_client<App::UsernamePasswordProviderClient>().register_email(
            email, password, [&](Optional<app::AppError> error) {
                REQUIRE_FALSE(error);
            });
        app->log_in_with_credentials(app::AppCredentials::username_password(email, password),
                                     [&](std::shared_ptr<SyncUser> user, Optional<app::AppError> error) {
                                         REQUIRE(user);
                                         REQUIRE_FALSE(error);
                                     });

        SyncTestFile config(app, partition, schema);
        auto realm = Realm::get_shared_realm(config);

        CppContext c(realm);
        realm->begin_transaction();
        auto array_of_objs =
            Object::create(c, realm, "TopLevel",
                           util::Any(AnyDict{
                               {valid_pk_name, array_of_objs_id},
                               {"array_of_objs", AnyVector{AnyDict{{"array", AnyVector{INT64_C(1), INT64_C(2)}}}}},
                           }),
                           CreatePolicy::ForceCreate);

        auto embedded_obj =
            Object::create(c, realm, "TopLevel",
                           util::Any(AnyDict{
                               {valid_pk_name, embedded_obj_id},
                               {"embedded_obj", AnyDict{{"array", AnyVector{INT64_C(1), INT64_C(2)}}}},
                           }),
                           CreatePolicy::ForceCreate);

        auto dict_obj = Object::create(
            c, realm, "TopLevel",
            util::Any(AnyDict{
                {valid_pk_name, dict_obj_id},
                {"embedded_dict", AnyDict{{"foo", AnyDict{{"array", AnyVector{INT64_C(1), INT64_C(2)}}}}}},
            }),
            CreatePolicy::ForceCreate);

        realm->commit_transaction();
        {
            realm->begin_transaction();
            embedded_obj.set_property_value(c, "embedded_obj",
                                            util::Any(AnyDict{{
                                                "array",
                                                AnyVector{INT64_C(3), INT64_C(4)},
                                            }}),
                                            CreatePolicy::UpdateAll);
            realm->commit_transaction();
        }

        {
            realm->begin_transaction();
            List array(array_of_objs, array_of_objs.get_object_schema().property_for_name("array_of_objs"));
            CppContext c2(realm, &array.get_object_schema());
            array.set(c2, 0, util::Any{AnyDict{{"array", AnyVector{INT64_C(5), INT64_C(6)}}}});
            realm->commit_transaction();
        }

        {
            realm->begin_transaction();
            object_store::Dictionary dict(dict_obj, dict_obj.get_object_schema().property_for_name("embedded_dict"));
            CppContext c2(realm, &dict.get_object_schema());
            dict.insert(c2, "foo", util::Any{AnyDict{{"array", AnyVector{INT64_C(7), INT64_C(8)}}}});
            realm->commit_transaction();
        }
        CHECK(!wait_for_upload(*realm));
    }

    {
        TestSyncManager sync_manager(TestSyncManager::Config(app_config), {});
        auto app = sync_manager.app();
        log_in(app, creds);
        SyncTestFile config(app, partition, schema);
        auto realm = Realm::get_shared_realm(config);

        CHECK(!wait_for_download(*realm));
        CppContext c(realm);
        {
            auto obj = Object::get_for_primary_key(c, realm, "TopLevel", util::Any{embedded_obj_id});
            auto embedded_obj = any_cast<Object&&>(obj.get_property_value<util::Any>(c, "embedded_obj"));
            auto array_list = any_cast<List&&>(embedded_obj.get_property_value<util::Any>(c, "array"));
            CHECK(array_list.size() == 2);
            CHECK(array_list.get<int64_t>(0) == int64_t(3));
            CHECK(array_list.get<int64_t>(1) == int64_t(4));
        }

        {
            auto obj = Object::get_for_primary_key(c, realm, "TopLevel", util::Any{array_of_objs_id});
            auto embedded_list = any_cast<List&&>(obj.get_property_value<util::Any>(c, "array_of_objs"));
            CppContext c2(realm, &embedded_list.get_object_schema());
            auto embedded_array_obj = any_cast<Object&&>(embedded_list.get(c2, 0));
            auto array_list = any_cast<List&&>(embedded_array_obj.get_property_value<util::Any>(c2, "array"));
            CHECK(array_list.size() == 2);
            CHECK(array_list.get<int64_t>(0) == int64_t(5));
            CHECK(array_list.get<int64_t>(1) == int64_t(6));
        }

        {
            auto obj = Object::get_for_primary_key(c, realm, "TopLevel", util::Any{dict_obj_id});
            object_store::Dictionary dict(obj, obj.get_object_schema().property_for_name("embedded_dict"));
            CppContext c2(realm, &dict.get_object_schema());
            auto embedded_obj = any_cast<Object&&>(dict.get(c2, "foo"));
            auto array_list = any_cast<List&&>(embedded_obj.get_property_value<util::Any>(c2, "array"));
            CHECK(array_list.size() == 2);
            CHECK(array_list.get<int64_t>(0) == int64_t(7));
            CHECK(array_list.get<int64_t>(1) == int64_t(8));
        }
    }
}

TEST_CASE("app: sync integration", "[sync][app]") {
    const auto schema = Schema{{"Dog",
                                {{"_id", PropertyType::ObjectId | PropertyType::Nullable, true},
                                 {"breed", PropertyType::String | PropertyType::Nullable},
                                 {"name", PropertyType::String}}},
                               {"Person",
                                {{"_id", PropertyType::ObjectId | PropertyType::Nullable, true},
                                 {"age", PropertyType::Int},
                                 {"dogs", PropertyType::Object | PropertyType::Array, "Dog"},
                                 {"firstName", PropertyType::String},
                                 {"lastName", PropertyType::String}}}};

    auto get_dogs = [](SharedRealm r) -> Results {
        auto& config = r->config();
        auto session = config.sync_config->user->sync_manager()->get_existing_session(config.path);
        std::atomic<bool> called{false};
        session->wait_for_upload_completion([&](std::error_code err) {
            REQUIRE(err == std::error_code{});
            called.store(true);
        });
        timed_wait_for([&] {
            return called.load();
        });
        REQUIRE(called);
        called.store(false);
        session->wait_for_download_completion([&](std::error_code err) {
            REQUIRE(err == std::error_code{});
            called.store(true);
        });
        timed_wait_for([&] {
            return called.load();
        });
        return Results(r, r->read_group().get_table("class_Dog"));
    };

    auto create_one_dog = [](SharedRealm r) {
        r->begin_transaction();
        CppContext c;
        Object::create(c, r, "Dog",
                       util::Any(AnyDict{{"_id", util::Any(ObjectId::gen())},
                                         {"breed", std::string("bulldog")},
                                         {"name", std::string("fido")}}),
                       CreatePolicy::ForceCreate);
        r->commit_transaction();
    };

    auto app_config = get_integration_config();
    const auto partition = random_string(100);

    // MARK: Add Objects -
    SECTION("Add Objects") {
        {
            TestSyncManager sync_manager(app_config, {});
            auto app = sync_manager.app();
            create_user_and_log_in(app);
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);

            REQUIRE(get_dogs(r).size() == 0);
            create_one_dog(r);
            REQUIRE(get_dogs(r).size() == 1);
        }

        {
            TestSyncManager sync_manager(app_config, {});
            auto app = sync_manager.app();
            create_user_and_log_in(app);
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);
            Results dogs = get_dogs(r);
            REQUIRE(dogs.size() == 1);
            REQUIRE(dogs.get(0).get<String>("breed") == "bulldog");
            REQUIRE(dogs.get(0).get<String>("name") == "fido");
        }
    }

    // MARK: Expired Session Refresh -
    SECTION("Invalid Access Token is Refreshed") {
        {
            TestSyncManager sync_manager(app_config, {});
            auto app = sync_manager.app();
            create_user_and_log_in(app);
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);
            REQUIRE(get_dogs(r).size() == 0);
            create_one_dog(r);
            REQUIRE(get_dogs(r).size() == 1);
        }

        {
            TestSyncManager sync_manager(app_config, {});
            auto app = sync_manager.app();
            create_user_and_log_in(app);
            auto user = app->current_user();
            // set a bad access token. this will trigger a refresh when the sync session opens
            user->update_access_token(encode_fake_jwt("fake_access_token"));

            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);
            Results dogs = get_dogs(r);
            REQUIRE(dogs.size() == 1);
            REQUIRE(dogs.get(0).get<String>("breed") == "bulldog");
            REQUIRE(dogs.get(0).get<String>("name") == "fido");
        }
    }

    SECTION("Expired Access Token is Refreshed") {
        sync::AccessToken token;
        {
            TestSyncManager sync_manager(app_config, {});
            auto app = sync_manager.app();
            create_user_and_log_in(app);
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);

            REQUIRE(get_dogs(r).size() == 0);
            create_one_dog(r);

            REQUIRE(get_dogs(r).size() == 1);
            sync::AccessToken::ParseError error_state = realm::sync::AccessToken::ParseError::none;
            sync::AccessToken::parse(app->current_user()->access_token(), token, error_state, nullptr);
            REQUIRE(error_state == sync::AccessToken::ParseError::none);
            REQUIRE(token.timestamp);
            REQUIRE(token.expires);
            REQUIRE(token.timestamp < token.expires);
            std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
            using namespace std::chrono_literals;
            token.expires = std::chrono::system_clock::to_time_t(now - 30s);
            REQUIRE(token.expired(now));
        }

        {
            std::function<void()> hook;
            std::function<std::unique_ptr<GenericNetworkTransport>()> hooked_factory = [&hook] {
                if (hook) {
                    hook();
                }
                return std::unique_ptr<GenericNetworkTransport>(new SynchronousTestTransport);
            };
            app_config.transport_generator = hooked_factory;

            TestSyncManager sync_manager(app_config, {});
            auto app = sync_manager.app();
            create_user_and_log_in(app);
            REQUIRE(!app->current_user()->access_token_refresh_required());
            // Set a bad access token, with an expired time. This will trigger a refresh initiated by the client.
            app->current_user()->update_access_token(
                encode_fake_jwt("fake_access_token", token.expires, token.timestamp));
            REQUIRE(app->current_user()->access_token_refresh_required());

            // This assumes that we make an http request for the new token while
            // already in the WaitingForAccessToken state.
            std::vector<SyncSession::PublicState> seen_states;
            hook = [&] {
                auto user = app->current_user();
                REQUIRE(user);
                for (auto session : user->all_sessions()) {
                    seen_states.push_back(session->state());
                }
            };
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);
            REQUIRE(std::find(begin(seen_states), end(seen_states),
                              SyncSession::PublicState::WaitingForAccessToken) != end(seen_states));
            Results dogs = get_dogs(r);
            REQUIRE(dogs.size() == 1);
            REQUIRE(dogs.get(0).get<String>("breed") == "bulldog");
            REQUIRE(dogs.get(0).get<String>("name") == "fido");
        }
    }

    SECTION("Invalid refresh token") {
        auto app_session = get_runtime_app_session("");
        auto verify_error_on_sync_with_invalid_refresh_token = [&](std::shared_ptr<SyncUser> user,
                                                                   Realm::Config config) {
            REQUIRE(user);
            REQUIRE(app_session.admin_api.verify_access_token(user->access_token(), app_session.server_app_id));

            // requesting a new access token fails because the refresh token used for this request is revoked
            user->refresh_custom_data([&](util::Optional<AppError> error) {
                REQUIRE(error);
                REQUIRE(error->http_status_code == 401);
                REQUIRE(error->error_code == app::make_error_code(realm::app::ServiceErrorCode::invalid_session));
            });

            // Set a bad access token. This will force a request for a new access token when the sync session opens
            // this is only necessary because the server doesn't actually revoke previously issued access tokens
            // instead allowing their session to time out as normal. So this simulates the access token expiring.
            // see:
            // https://github.com/10gen/baas/blob/05837cc3753218dfaf89229c6930277ef1616402/api/common/auth.go#L1380-L1386
            user->update_access_token(encode_fake_jwt("fake_access_token"));
            REQUIRE(!app_session.admin_api.verify_access_token(user->access_token(), app_session.server_app_id));

            std::atomic<bool> sync_error_handler_called{false};
            config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                sync_error_handler_called.store(true);
                REQUIRE(error.error_code == sync::make_error_code(realm::sync::ProtocolError::bad_authentication));
                REQUIRE(error.message == "Unable to refresh the user access token.");
            };

            auto r = Realm::get_shared_realm(config);
            auto session = user->session_for_on_disk_path(config.path);
            REQUIRE(user->is_logged_in());
            REQUIRE(!sync_error_handler_called.load());
            {
                std::atomic<bool> called{false};
                session->wait_for_upload_completion([&](std::error_code err) {
                    called.store(true);
                    REQUIRE(err == app::make_error_code(realm::app::ServiceErrorCode::invalid_session));
                });
                timed_wait_for([&] {
                    return called.load();
                });
                REQUIRE(called);
            }
            timed_wait_for([&] {
                return sync_error_handler_called.load();
            });

            // the failed refresh logs out the user
            REQUIRE(!user->is_logged_in());
        };

        SECTION("Disabled user results in a sync error") {
            TestSyncManager sync_manager(app_config, {});
            auto app = sync_manager.app();
            auto creds = create_user_and_log_in(sync_manager.app());
            SyncTestFile config(app, partition, schema);
            auto user = app->current_user();
            REQUIRE(user);
            REQUIRE(app_session.admin_api.verify_access_token(user->access_token(), app_session.server_app_id));
            app_session.admin_api.disable_user_sessions(app->current_user()->identity(), app_session.server_app_id);

            verify_error_on_sync_with_invalid_refresh_token(user, config);

            // logging in again doesn't fix things while the account is disabled
            auto error = failed_log_in(app, creds);
            REQUIRE(error.error_code == app::make_error_code(realm::app::ServiceErrorCode::user_disabled));

            // admin enables user sessions again which should allow the session to continue
            app_session.admin_api.enable_user_sessions(user->identity(), app_session.server_app_id);

            // logging in now works properly
            log_in(app, creds);

            // still referencing the same user
            REQUIRE(user == app->current_user());
            REQUIRE(user->is_logged_in());

            {
                // check that there are no errors initiating a session now by making sure upload/download succeeds
                auto r = Realm::get_shared_realm(config);
                Results dogs = get_dogs(r);
            }
        }

        SECTION("Revoked refresh token results in a sync error") {
            TestSyncManager sync_manager(app_config, {});
            auto app = sync_manager.app();
            auto creds = create_user_and_log_in(sync_manager.app());
            SyncTestFile config(app, partition, schema);
            auto user = app->current_user();
            REQUIRE(app_session.admin_api.verify_access_token(user->access_token(), app_session.server_app_id));
            app_session.admin_api.revoke_user_sessions(user->identity(), app_session.server_app_id);
            // revoking a user session only affects the refresh token, so the access token should still continue to
            // work.
            REQUIRE(app_session.admin_api.verify_access_token(user->access_token(), app_session.server_app_id));

            verify_error_on_sync_with_invalid_refresh_token(user, config);

            // logging in again succeeds and generates a new and valid refresh token
            log_in(app, creds);

            // still referencing the same user and now the user is logged in
            REQUIRE(user == app->current_user());
            REQUIRE(user->is_logged_in());

            // new requests for an access token succeed again
            user->refresh_custom_data([&](util::Optional<AppError> error) {
                REQUIRE_FALSE(error);
            });

            {
                // check that there are no errors initiating a new sync session by making sure upload/download
                // succeeds
                auto r = Realm::get_shared_realm(config);
                Results dogs = get_dogs(r);
            }
        }
    }

    SECTION("too large sync message error handling") {
        TestSyncManager::Config test_config = app_config;
        // Too much log output seems to create problems on Evergreen CI
        test_config.verbose_sync_client_logging = false;

        TestSyncManager sync_manager(test_config, {});
        auto app = sync_manager.app();
        auto creds = create_user_and_log_in(sync_manager.app());
        SyncTestFile config(app, partition, schema);

        std::mutex sync_error_mutex;
        std::vector<SyncError> sync_errors;
        config.sync_config->error_handler = [&](auto, SyncError error) {
            std::lock_guard<std::mutex> lk(sync_error_mutex);
            sync_errors.push_back(std::move(error));
        };
        auto r = Realm::get_shared_realm(config);

        // Create 26 MB worth of dogs in a single transaction - this should all get put into one changeset
        // and get uploaded at once, which for now is an error on the server.
        r->begin_transaction();
        CppContext c;
        for (auto i = 'a'; i < 'z'; ++i) {
            Object::create(c, r, "Dog",
                           util::Any(AnyDict{{"_id", util::Any(ObjectId::gen())},
                                             {"breed", std::string("bulldog")},
                                             {"name", random_string(1024 * 1024)}}),
                           CreatePolicy::ForceCreate);
        }
        r->commit_transaction();

        auto pred = [](const SyncError& error) {
            return error.error_code.category() == util::websocket::websocket_close_status_category();
        };
        // If we haven't gotten an error in more than 5 minutes, then something has gone wrong
        // and we should fail the test.
        timed_wait_for(
            [&] {
                std::lock_guard<std::mutex> lk(sync_error_mutex);
                return std::any_of(sync_errors.begin(), sync_errors.end(), pred);
            },
            std::chrono::minutes(5));

        auto captured_error = [&] {
            std::lock_guard<std::mutex> lk(sync_error_mutex);
            const auto it = std::find_if(sync_errors.begin(), sync_errors.end(), pred);
            REQUIRE(it != sync_errors.end());
            return *it;
        }();

        REQUIRE(captured_error.error_code.category() == util::websocket::websocket_close_status_category());
        REQUIRE(captured_error.error_code.value() == 1009);
        REQUIRE(captured_error.message == "read limited at 16777217 bytes");
    }

    SECTION("validation") {
        TestSyncManager sync_manager(app_config, {});
        auto app = sync_manager.app();
        auto creds = create_user_and_log_in(sync_manager.app());
        SyncTestFile config(app, partition, schema);

        SECTION("invalid partition error handling") {
            config.sync_config->partition_value = "not a bson serialized string";
            std::atomic<bool> error_did_occur = false;
            config.sync_config->error_handler = [&error_did_occur](std::shared_ptr<SyncSession>, SyncError error) {
                CHECK(error.message.find("Illegal Realm path (BIND): serialized partition 'not a bson serialized "
                                         "string' is invalid") != std::string::npos);
                error_did_occur.store(true);
            };
            auto r = Realm::get_shared_realm(config);
            auto session = app->current_user()->session_for_on_disk_path(r->config().path);
            timed_wait_for([&] {
                return error_did_occur.load();
            });
            REQUIRE(error_did_occur.load());
        }

        SECTION("invalid pk schema error handling") {
            const std::string invalid_pk_name = "my_primary_key";
            auto it = config.schema->find("Dog");
            REQUIRE(it != config.schema->end());
            REQUIRE(it->primary_key_property());
            REQUIRE(it->primary_key_property()->name == "_id");
            it->primary_key_property()->name = invalid_pk_name;
            it->primary_key = invalid_pk_name;
            REQUIRE_THROWS_CONTAINING(Realm::get_shared_realm(config),
                                      "The primary key property on a synchronized Realm must be named '_id' but "
                                      "found 'my_primary_key' for type 'Dog'");
        }

        SECTION("missing pk schema error handling") {
            auto it = config.schema->find("Dog");
            REQUIRE(it != config.schema->end());
            REQUIRE(it->primary_key_property());
            it->primary_key_property()->is_primary = false;
            it->primary_key = "";
            REQUIRE(!it->primary_key_property());
            REQUIRE_THROWS_CONTAINING(Realm::get_shared_realm(config),
                                      "There must be a primary key property named '_id' on a synchronized "
                                      "Realm but none was found for type 'Dog'");
        }
    }
}

TEST_CASE("app: custom user data integration tests", "[sync][app]") {
    auto app_config = get_integration_config();

    SECTION("custom user data happy path") {
        TestSyncManager sync_manager(app_config);
        auto app = sync_manager.app();

        bool processed = false;

        std::shared_ptr<SyncUser> user = log_in(app);
        app->call_function(user, "updateUserData", {bson::BsonDocument({{"favorite_color", "green"}})},
                           [&](auto error, auto response) {
                               CHECK(error == none);
                               CHECK(response);
                               CHECK(*response == true);
                               processed = true;
                           });
        CHECK(processed);
        processed = false;
        app->refresh_custom_data(user, [&](auto) {
            processed = true;
        });
        CHECK(processed);
        auto data = *user->custom_data();
        CHECK(data["favorite_color"] == "green");
    }
}

#endif // REALM_ENABLE_AUTH_TESTS

TEST_CASE("app: custom error handling", "[sync][app][custom_errors]") {
    class CustomErrorTransport : public GenericNetworkTransport {
    public:
        CustomErrorTransport(int code, const std::string& message)
            : m_code(code)
            , m_message(message)
        {
        }

        void send_request_to_server(const Request, std::function<void(const Response)> completion_block) override
        {
            completion_block(Response{0, m_code, std::map<std::string, std::string>(), m_message});
        }

    private:
        int m_code;
        std::string m_message;
    };

    SECTION("custom code and message is sent back") {
        std::unique_ptr<GenericNetworkTransport> (*factory)() = [] {
            return std::unique_ptr<GenericNetworkTransport>(new CustomErrorTransport(1001, "Boom!"));
        };

        TestSyncManager tsm(get_config(factory), {});
        auto app = tsm.app();
        auto error = failed_log_in(app);
        CHECK(error.is_custom_error());
        CHECK(error.error_code.value() == 1001);
        CHECK(error.message == "Boom!");
    }
}


static const std::string profile_0_name = "Ursus americanus Ursus boeckhi";
static const std::string profile_0_first_name = "Ursus americanus";
static const std::string profile_0_last_name = "Ursus boeckhi";
static const std::string profile_0_email = "Ursus ursinus";
static const std::string profile_0_picture_url = "Ursus malayanus";
static const std::string profile_0_gender = "Ursus thibetanus";
static const std::string profile_0_birthday = "Ursus americanus";
static const std::string profile_0_min_age = "Ursus maritimus";
static const std::string profile_0_max_age = "Ursus arctos";

static const nlohmann::json profile_0 = {
    {"name", profile_0_name},         {"first_name", profile_0_first_name},   {"last_name", profile_0_last_name},
    {"email", profile_0_email},       {"picture_url", profile_0_picture_url}, {"gender", profile_0_gender},
    {"birthday", profile_0_birthday}, {"min_age", profile_0_min_age},         {"max_age", profile_0_max_age}};

static nlohmann::json user_json(std::string access_token, std::string user_id = random_string(15))
{
    return {{"access_token", access_token},
            {"refresh_token", access_token},
            {"user_id", user_id},
            {"device_id", "Panda Bear"}};
}

static nlohmann::json user_profile_json(std::string user_id = random_string(15),
                                        std::string identity_0_id = "Ursus arctos isabellinus",
                                        std::string identity_1_id = "Ursus arctos horribilis",
                                        std::string provider_type = "anon-user")
{
    return {{"user_id", user_id},
            {"identities",
             {{{"id", identity_0_id}, {"provider_type", provider_type}, {"provider_id", "lol"}},
              {{"id", identity_1_id}, {"provider_type", "lol_wut"}, {"provider_id", "nah_dawg"}}}},
            {"data", profile_0}};
}

// MARK: - Unit Tests

class UnitTestTransport : public GenericNetworkTransport {
    std::string m_provider_type;

public:
    UnitTestTransport(const std::string& provider_type = "anon-user")
        : m_provider_type(provider_type)
    {
    }

    static std::string access_token;

    static const std::string api_key;
    static const std::string api_key_id;
    static const std::string api_key_name;
    static const std::string auth_route;
    static const std::string user_id;
    static const std::string identity_0_id;
    static const std::string identity_1_id;

    void set_provider_type(const std::string& provider_type)
    {
        m_provider_type = provider_type;
    }

private:
    void handle_profile(const Request request, std::function<void(Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::get);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");
        CHECK(request.headers.at("Authorization") == "Bearer " + access_token);
        CHECK(request.body.empty());
        CHECK(request.timeout_ms == 60000);

        std::string response =
            nlohmann::json({{"user_id", user_id},
                            {"identities",
                             {{{"id", identity_0_id}, {"provider_type", m_provider_type}, {"provider_id", "lol"}},
                              {{"id", identity_1_id}, {"provider_type", "lol_wut"}, {"provider_id", "nah_dawg"}}}},
                            {"data", profile_0}})
                .dump();

        completion_block(Response{200, 0, {}, response});
    }

    void handle_login(const Request request, std::function<void(Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::post);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");
        CHECK(nlohmann::json::parse(request.body)["options"] ==
              nlohmann::json({{"device",
                               {{"appId", "app name"},
                                {"appVersion", "A Local App Version"},
                                {"platform", "Object Store Platform Tests"},
                                {"platformVersion", "Object Store Platform Version Blah"},
                                {"sdkVersion", "An sdk version"}}}}));

        CHECK(request.timeout_ms == 60000);

        std::string response = nlohmann::json({{"access_token", access_token},
                                               {"refresh_token", access_token},
                                               {"user_id", random_string(15)},
                                               {"device_id", "Panda Bear"}})
                                   .dump();

        completion_block(Response{200, 0, {}, response});
    }

    void handle_location(const Request request, std::function<void(Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::get);
        CHECK(request.timeout_ms == 60000);

        std::string response = nlohmann::json({{"deployment_model", "this"},
                                               {"hostname", "field"},
                                               {"ws_hostname", "shouldn't"},
                                               {"location", "matter"}})
                                   .dump();

        completion_block(Response{200, 0, {}, response});
    }

    void handle_create_api_key(const Request request, std::function<void(Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::post);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");
        CHECK(nlohmann::json::parse(request.body) == nlohmann::json({{"name", api_key_name}}));
        CHECK(request.timeout_ms == 60000);

        std::string response =
            nlohmann::json({{"_id", api_key_id}, {"key", api_key}, {"name", api_key_name}, {"disabled", false}})
                .dump();

        completion_block(Response{200, 0, {}, response});
    }

    void handle_fetch_api_key(const Request request, std::function<void(Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::get);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");

        CHECK(request.body == "");
        CHECK(request.timeout_ms == 60000);

        std::string response =
            nlohmann::json({{"_id", api_key_id}, {"name", api_key_name}, {"disabled", false}}).dump();

        completion_block(Response{200, 0, {}, response});
    }

    void handle_fetch_api_keys(const Request request, std::function<void(Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::get);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");

        CHECK(request.body == "");
        CHECK(request.timeout_ms == 60000);

        auto elements = std::vector<nlohmann::json>();
        for (int i = 0; i < 2; i++) {
            elements.push_back({{"_id", api_key_id}, {"name", api_key_name}, {"disabled", false}});
        }

        completion_block(Response{200, 0, {}, nlohmann::json(elements).dump()});
    }

    void handle_token_refresh(const Request request, std::function<void(Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::post);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");

        CHECK(request.body == "");
        CHECK(request.timeout_ms == 60000);

        auto elements = std::vector<nlohmann::json>();
        nlohmann::json json{{"access_token", access_token}};

        completion_block(Response{200, 0, {}, json.dump()});
    }

public:
    void send_request_to_server(const Request request, std::function<void(const Response)> completion_block) override
    {
        if (request.url.find("/login") != std::string::npos) {
            handle_login(request, completion_block);
        }
        else if (request.url.find("/profile") != std::string::npos) {
            handle_profile(request, completion_block);
        }
        else if (request.url.find("/session") != std::string::npos && request.method != HttpMethod::post) {
            completion_block(Response{200, 0, {}, ""});
        }
        else if (request.url.find("/api_keys") != std::string::npos && request.method == HttpMethod::post) {
            handle_create_api_key(request, completion_block);
        }
        else if (request.url.find(util::format("/api_keys/%1", api_key_id)) != std::string::npos &&
                 request.method == HttpMethod::get) {
            handle_fetch_api_key(request, completion_block);
        }
        else if (request.url.find("/api_keys") != std::string::npos && request.method == HttpMethod::get) {
            handle_fetch_api_keys(request, completion_block);
        }
        else if (request.url.find("/session") != std::string::npos && request.method == HttpMethod::post) {
            handle_token_refresh(request, completion_block);
        }
        else if (request.url.find("/location") != std::string::npos && request.method == HttpMethod::get) {
            handle_location(request, completion_block);
        }
        else {
            completion_block(Response{200, 0, {}, "something arbitrary"});
        }
    }
};

App::Config get_config()
{
    return get_config(factory<UnitTestTransport>);
}

static const std::string good_access_token =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJleHAiOjE1ODE1MDc3OTYsImlhdCI6MTU4MTUwNTk5NiwiaXNzIjoiNWU0M2RkY2M2MzZlZTEwNmVhYTEyYmRjIiwic3RpdGNoX2RldklkIjoi"
    "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwIiwic3RpdGNoX2RvbWFpbklkIjoiNWUxNDk5MTNjOTBiNGFmMGViZTkzNTI3Iiwic3ViIjoiNWU0M2Rk"
    "Y2M2MzZlZTEwNmVhYTEyYmRhIiwidHlwIjoiYWNjZXNzIn0.0q3y9KpFxEnbmRwahvjWU1v9y1T1s3r2eozu93vMc3s";

static const std::string good_access_token2 =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJleHAiOjE1ODkzMDE3MjAsImlhdCI6MTU4NDExODcyMCwiaXNzIjoiNWU2YmJiYzBhNmI3ZGZkM2UyNTA0OGI3Iiwic3RpdGNoX2RldklkIjoi"
    "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwIiwic3RpdGNoX2RvbWFpbklkIjoiNWUxNDk5MTNjOTBiNGFmMGViZTkzNTI3Iiwic3ViIjoiNWU2YmJi"
    "YzBhNmI3ZGZkM2UyNTA0OGIzIiwidHlwIjoiYWNjZXNzIn0.eSX4QMjIOLbdOYOPzQrD_racwLUk1HGFgxtx2a34k80";

std::string UnitTestTransport::access_token = good_access_token;

static const std::string bad_access_token = "lolwut";
static const std::string dummy_device_id = "123400000000000000000000";

const std::string UnitTestTransport::api_key = "lVRPQVYBJSIbGos2ZZn0mGaIq1SIOsGaZ5lrcp8bxlR5jg4OGuGwQq1GkektNQ3i";
const std::string UnitTestTransport::api_key_id = "5e5e6f0abe4ae2a2c2c2d329";
const std::string UnitTestTransport::api_key_name = "some_api_key_name";
const std::string UnitTestTransport::auth_route = "https://mongodb.com/unittests";
const std::string UnitTestTransport::user_id = "Ailuropoda melanoleuca";
const std::string UnitTestTransport::identity_0_id = "Ursus arctos isabellinus";
const std::string UnitTestTransport::identity_1_id = "Ursus arctos horribilis";

TEST_CASE("subscribable unit tests", "[sync][app]") {
    struct Foo : public Subscribable<Foo> {
        void event()
        {
            emit_change_to_subscribers(*this);
        }
    };

    auto foo = Foo();

    SECTION("subscriber receives events") {
        auto event_count = 0;
        auto token = foo.subscribe([&event_count](auto&) {
            event_count++;
        });

        foo.event();
        foo.event();
        foo.event();

        CHECK(event_count == 3);
    }

    SECTION("subscriber can unsubscribe") {
        auto event_count = 0;
        auto token = foo.subscribe([&event_count](auto&) {
            event_count++;
        });

        foo.event();
        CHECK(event_count == 1);

        foo.unsubscribe(token);
        foo.event();
        CHECK(event_count == 1);
    }

    SECTION("subscriber is unsubscribed on dtor") {
        auto event_count = 0;
        {
            auto token = foo.subscribe([&event_count](auto&) {
                event_count++;
            });

            foo.event();
            CHECK(event_count == 1);
        }
        foo.event();
        CHECK(event_count == 1);
    }

    SECTION("multiple subscribers receive events") {
        auto event_count = 0;
        {
            auto token1 = foo.subscribe([&event_count](auto&) {
                event_count++;
            });
            auto token2 = foo.subscribe([&event_count](auto&) {
                event_count++;
            });

            foo.event();
            CHECK(event_count == 2);
        }
        foo.event();
        CHECK(event_count == 2);
    }
}

TEST_CASE("app: login_with_credentials unit_tests", "[sync][app]") {
    SECTION("login_anonymous good") {
        UnitTestTransport::access_token = good_access_token;

        auto config = get_config();
        auto base_path = util::make_temp_dir();
        {
            auto conf = TestSyncManager::Config(config);
            conf.should_teardown_test_directory = false;
            conf.base_path = base_path;
            TestSyncManager tsm(conf, {});
            auto app = tsm.app();

            auto user = log_in(app);

            CHECK(user->identities().size() == 2);
            CHECK(user->identities()[0].id == UnitTestTransport::identity_0_id);
            CHECK(user->identities()[1].id == UnitTestTransport::identity_1_id);
            SyncUserProfile user_profile = user->user_profile();

            CHECK(user_profile.name() == profile_0_name);
            CHECK(user_profile.first_name() == profile_0_first_name);
            CHECK(user_profile.last_name() == profile_0_last_name);
            CHECK(user_profile.email() == profile_0_email);
            CHECK(user_profile.picture_url() == profile_0_picture_url);
            CHECK(user_profile.gender() == profile_0_gender);
            CHECK(user_profile.birthday() == profile_0_birthday);
            CHECK(user_profile.min_age() == profile_0_min_age);
            CHECK(user_profile.max_age() == profile_0_max_age);
        }
        App::clear_cached_apps();
        // assert everything is stored properly between runs
        {
            auto conf = TestSyncManager::Config(config);
            conf.base_path = base_path;
            TestSyncManager tsm(conf, {});
            auto app = tsm.app();
            REQUIRE(app->all_users().size() == 1);
            auto user = app->all_users()[0];
            CHECK(user->identities().size() == 2);
            CHECK(user->identities()[0].id == UnitTestTransport::identity_0_id);
            CHECK(user->identities()[1].id == UnitTestTransport::identity_1_id);
            SyncUserProfile user_profile = user->user_profile();

            CHECK(user_profile.name() == profile_0_name);
            CHECK(user_profile.first_name() == profile_0_first_name);
            CHECK(user_profile.last_name() == profile_0_last_name);
            CHECK(user_profile.email() == profile_0_email);
            CHECK(user_profile.picture_url() == profile_0_picture_url);
            CHECK(user_profile.gender() == profile_0_gender);
            CHECK(user_profile.birthday() == profile_0_birthday);
            CHECK(user_profile.min_age() == profile_0_min_age);
            CHECK(user_profile.max_age() == profile_0_max_age);
        }
    }

    SECTION("login_anonymous bad") {
        struct transport : UnitTestTransport {
            void send_request_to_server(const Request request, std::function<void(const Response)> completion_block)
            {
                if (request.url.find("/login") != std::string::npos) {
                    completion_block({200, 0, {}, user_json(bad_access_token).dump()});
                }
                else {
                    UnitTestTransport::send_request_to_server(request, completion_block);
                }
            }
        };

        TestSyncManager tsm(get_config(factory<transport>), {});
        auto app = tsm.app();

        auto error = failed_log_in(app);
        CHECK(error.message == std::string("jwt missing parts"));
        CHECK(error.error_code.message() == "bad token");
        CHECK(error.error_code.category() == app::json_error_category());
        CHECK(error.is_json_error());
        CHECK(app::JSONErrorCode(error.error_code.value()) == app::JSONErrorCode::bad_token);
    }
}

TEST_CASE("app: UserAPIKeyProviderClient unit_tests", "[sync][app]") {
    TestSyncManager sync_manager(get_config(), {});
    auto app = sync_manager.app();
    auto client = app->provider_client<App::UserAPIKeyProviderClient>();

    std::shared_ptr<SyncUser> logged_in_user = app->sync_manager()->get_user(
        UnitTestTransport::user_id, good_access_token, good_access_token, "anon-user", dummy_device_id);
    bool processed = false;
    ObjectId obj_id(UnitTestTransport::api_key_id.c_str());

    SECTION("create api key") {
        client.create_api_key(UnitTestTransport::api_key_name, logged_in_user,
                              [&](App::UserAPIKey user_api_key, util::Optional<AppError> error) {
                                  REQUIRE_FALSE(error);
                                  CHECK(user_api_key.disabled == false);
                                  CHECK(user_api_key.id.to_string() == UnitTestTransport::api_key_id);
                                  CHECK(user_api_key.key == UnitTestTransport::api_key);
                                  CHECK(user_api_key.name == UnitTestTransport::api_key_name);
                              });
    }

    SECTION("fetch api key") {
        client.fetch_api_key(obj_id, logged_in_user,
                             [&](App::UserAPIKey user_api_key, util::Optional<AppError> error) {
                                 REQUIRE_FALSE(error);
                                 CHECK(user_api_key.disabled == false);
                                 CHECK(user_api_key.id.to_string() == UnitTestTransport::api_key_id);
                                 CHECK(user_api_key.name == UnitTestTransport::api_key_name);
                             });
    }

    SECTION("fetch api keys") {
        client.fetch_api_keys(logged_in_user,
                              [&](std::vector<App::UserAPIKey> user_api_keys, util::Optional<AppError> error) {
                                  REQUIRE_FALSE(error);
                                  CHECK(user_api_keys.size() == 2);
                                  for (auto user_api_key : user_api_keys) {
                                      CHECK(user_api_key.disabled == false);
                                      CHECK(user_api_key.id.to_string() == UnitTestTransport::api_key_id);
                                      CHECK(user_api_key.name == UnitTestTransport::api_key_name);
                                  }
                                  processed = true;
                              });
        CHECK(processed);
    }
}


TEST_CASE("app: user_semantics", "[app]") {
    TestSyncManager tsm(get_config(), {});
    auto app = tsm.app();

    const auto login_user_email_pass = [=] {
        return log_in(app, app::AppCredentials::username_password("bob", "thompson"));
    };
    const auto login_user_anonymous = [=] {
        return log_in(app, app::AppCredentials::anonymous());
    };

    CHECK(!app->current_user());

    int event_processed = 0;
    auto token = app->subscribe([&event_processed](auto&) {
        event_processed++;
    });

    SECTION("current user is populated") {
        const auto user1 = login_user_anonymous();
        CHECK(app->current_user()->identity() == user1->identity());
        CHECK(event_processed == 1);
    }

    SECTION("current user is updated on login") {
        const auto user1 = login_user_anonymous();
        CHECK(app->current_user()->identity() == user1->identity());
        const auto user2 = login_user_email_pass();
        CHECK(app->current_user()->identity() == user2->identity());
        CHECK(user1->identity() != user2->identity());
        CHECK(event_processed == 2);
    }

    SECTION("current user is updated to last used user on logout") {
        const auto user1 = login_user_anonymous();
        CHECK(app->current_user()->identity() == user1->identity());
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);

        const auto user2 = login_user_email_pass();
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);
        CHECK(app->all_users()[1]->state() == SyncUser::State::LoggedIn);
        CHECK(app->current_user()->identity() == user2->identity());
        CHECK(user1->identity() != user2->identity());

        // shuold reuse existing session
        const auto user3 = login_user_anonymous();
        CHECK(user3->identity() == user1->identity());

        auto user_events_processed = 0;
        auto _ = user3->subscribe([&user_events_processed](auto&) {
            user_events_processed++;
        });

        app->log_out([&](auto) {});
        CHECK(user_events_processed == 1);

        CHECK(app->current_user()->identity() == user2->identity());

        CHECK(app->all_users().size() == 1);
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);

        CHECK(event_processed == 4);
    }

    SECTION("anon users are removed on logout") {
        const auto user1 = login_user_anonymous();
        CHECK(app->current_user()->identity() == user1->identity());
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);

        const auto user2 = login_user_anonymous();
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);
        CHECK(app->all_users().size() == 1);
        CHECK(app->current_user()->identity() == user2->identity());
        CHECK(user1->identity() == user2->identity());

        app->log_out([&](auto) {});
        CHECK(app->all_users().size() == 0);

        CHECK(event_processed == 3);
    }

    SECTION("logout user") {
        auto user1 = login_user_email_pass();
        auto user2 = login_user_anonymous();

        // Anonymous users are special
        app->log_out(user2, [](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });
        CHECK(user2->state() == SyncUser::State::Removed);

        // Other users can be LoggedOut
        app->log_out(user1, [](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });
        CHECK(user1->state() == SyncUser::State::LoggedOut);

        // Logging out already logged out users, does nothing
        app->log_out(user1, [](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });
        CHECK(user1->state() == SyncUser::State::LoggedOut);

        app->log_out(user2, [](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });
        CHECK(user2->state() == SyncUser::State::Removed);

        CHECK(event_processed == 4);
    }

    SECTION("unsubscribed observers no longer process events") {
        app->unsubscribe(token);

        const auto user1 = login_user_anonymous();
        CHECK(app->current_user()->identity() == user1->identity());
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);

        const auto user2 = login_user_anonymous();
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);
        CHECK(app->all_users().size() == 1);
        CHECK(app->current_user()->identity() == user2->identity());
        CHECK(user1->identity() == user2->identity());

        app->log_out([&](auto) {});
        CHECK(app->all_users().size() == 0);

        CHECK(event_processed == 0);
    }
}

struct ErrorCheckingTransport : public GenericNetworkTransport {
    ErrorCheckingTransport(Response r)
        : m_response(r)
    {
    }
    void send_request_to_server(const Request, std::function<void(const Response)> completion_block) override
    {
        completion_block(m_response);
    }

private:
    Response m_response;
};

TEST_CASE("app: response error handling", "[sync][app]") {
    std::string response_body = nlohmann::json({{"access_token", good_access_token},
                                                {"refresh_token", good_access_token},
                                                {"user_id", "Brown Bear"},
                                                {"device_id", "Panda Bear"}})
                                    .dump();

    Response response{200, 0, {{"Content-Type", "application/json"}}, response_body};

    std::function<std::unique_ptr<GenericNetworkTransport>()> transport_generator = [&response] {
        return std::unique_ptr<GenericNetworkTransport>(new ErrorCheckingTransport(response));
    };

    TestSyncManager tsm(get_config(transport_generator), {});
    auto app = tsm.app();

    SECTION("http 404") {
        response.http_status_code = 404;
        auto error = failed_log_in(app);
        CHECK(!error.is_json_error());
        CHECK(!error.is_custom_error());
        CHECK(!error.is_service_error());
        CHECK(error.is_http_error());
        CHECK(error.error_code.value() == 404);
        CHECK(error.message == std::string("http error code considered fatal"));
        CHECK(error.error_code.message() == "Client Error: 404");
        CHECK(error.link_to_server_logs.empty());
    }
    SECTION("http 500") {
        response.http_status_code = 500;
        auto error = failed_log_in(app);
        CHECK(!error.is_json_error());
        CHECK(!error.is_custom_error());
        CHECK(!error.is_service_error());
        CHECK(error.is_http_error());
        CHECK(error.error_code.value() == 500);
        CHECK(error.message == std::string("http error code considered fatal"));
        CHECK(error.error_code.message() == "Server Error: 500");
        CHECK(error.link_to_server_logs.empty());
    }
    SECTION("custom error code") {
        response.custom_status_code = 42;
        response.body = "Custom error message";
        auto error = failed_log_in(app);
        CHECK(!error.is_http_error());
        CHECK(!error.is_json_error());
        CHECK(!error.is_service_error());
        CHECK(error.is_custom_error());
        CHECK(error.error_code.value() == 42);
        CHECK(error.message == std::string("Custom error message"));
        CHECK(error.error_code.message() == "code 42");
        CHECK(error.link_to_server_logs.empty());
    }

    SECTION("session error code") {
        response.http_status_code = 400;
        response.body = nlohmann::json({{"error_code", "MongoDBError"},
                                        {"error", "a fake MongoDB error message!"},
                                        {"access_token", good_access_token},
                                        {"refresh_token", good_access_token},
                                        {"user_id", "Brown Bear"},
                                        {"device_id", "Panda Bear"},
                                        {"link", "http://...whatever the server passes us"}})
                            .dump();
        auto error = failed_log_in(app);
        CHECK(!error.is_http_error());
        CHECK(!error.is_json_error());
        CHECK(!error.is_custom_error());
        CHECK(error.is_service_error());
        CHECK(app::ServiceErrorCode(error.error_code.value()) == app::ServiceErrorCode::mongodb_error);
        CHECK(error.message == std::string("a fake MongoDB error message!"));
        CHECK(error.error_code.message() == "MongoDBError");
        CHECK(error.link_to_server_logs == std::string("http://...whatever the server passes us"));
    }

    SECTION("json error code") {
        response.body = "this: is not{} a valid json body!";
        auto error = failed_log_in(app);
        CHECK(!error.is_http_error());
        CHECK(error.is_json_error());
        CHECK(!error.is_custom_error());
        CHECK(!error.is_service_error());
        CHECK(app::JSONErrorCode(error.error_code.value()) == app::JSONErrorCode::malformed_json);
        CHECK(error.message ==
              std::string("[json.exception.parse_error.101] parse error at line 1, column 2: syntax error "
                          "while parsing value - invalid literal; last read: 'th'"));
        CHECK(error.error_code.message() == "malformed json");
    }
}

TEST_CASE("app: switch user", "[sync][app]") {
    TestSyncManager tsm(get_config(), {});
    auto app = tsm.app();

    bool processed = false;

    SECTION("switch user expect success") {
        CHECK(app->sync_manager()->all_users().size() == 0);

        // Log in user 1
        auto user_a = log_in(app, app::AppCredentials::username_password("test@10gen.com", "password"));
        CHECK(app->sync_manager()->get_current_user() == user_a);

        // Log in user 2
        auto user_b = log_in(app, app::AppCredentials::username_password("test2@10gen.com", "password"));
        CHECK(app->sync_manager()->get_current_user() == user_b);

        CHECK(app->sync_manager()->all_users().size() == 2);

        auto user1 = app->switch_user(user_a);
        CHECK(user1 == user_a);

        CHECK(app->sync_manager()->get_current_user() == user_a);

        auto user2 = app->switch_user(user_b);
        CHECK(user2 == user_b);

        CHECK(app->sync_manager()->get_current_user() == user_b);
        processed = true;
        CHECK(processed);
    }

    SECTION("cannot switch to a logged out but not removed user") {
        CHECK(app->sync_manager()->all_users().size() == 0);

        // Log in user 1
        auto user_a = log_in(app, app::AppCredentials::username_password("test@10gen.com", "password"));
        CHECK(app->sync_manager()->get_current_user() == user_a);

        app->log_out([&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
        });

        CHECK(app->sync_manager()->get_current_user() == nullptr);
        CHECK(user_a->state() == SyncUser::State::LoggedOut);

        // Log in user 2
        auto user_b = log_in(app, app::AppCredentials::username_password("test2@10gen.com", "password"));
        CHECK(app->sync_manager()->get_current_user() == user_b);
        CHECK(app->sync_manager()->all_users().size() == 2);

        REQUIRE_THROWS_AS(app->switch_user(user_a), AppError);
        CHECK(app->sync_manager()->get_current_user() == user_b);
    }
}

TEST_CASE("app: remove anonymous user", "[sync][app]") {
    TestSyncManager tsm(get_config(), {});
    auto app = tsm.app();

    SECTION("remove user expect success") {
        CHECK(app->sync_manager()->all_users().size() == 0);

        // Log in user 1
        auto user_a = log_in(app);
        CHECK(user_a->state() == SyncUser::State::LoggedIn);

        app->log_out(user_a, [&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            // a logged out anon user will be marked as Removed, not LoggedOut
            CHECK(user_a->state() == SyncUser::State::Removed);
        });
        CHECK(app->sync_manager()->all_users().empty());

        app->remove_user(user_a, [&](Optional<app::AppError> error) {
            CHECK(error->message == "User has already been removed");
            CHECK(app->sync_manager()->all_users().size() == 0);
        });

        // Log in user 2
        auto user_b = log_in(app);
        CHECK(app->sync_manager()->get_current_user() == user_b);
        CHECK(user_b->state() == SyncUser::State::LoggedIn);
        CHECK(app->sync_manager()->all_users().size() == 1);

        app->remove_user(user_b, [&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(app->sync_manager()->all_users().size() == 0);
        });

        CHECK(app->sync_manager()->get_current_user() == nullptr);

        // check both handles are no longer valid
        CHECK(user_a->state() == SyncUser::State::Removed);
        CHECK(user_b->state() == SyncUser::State::Removed);
    }
}

TEST_CASE("app: remove user with credentials", "[sync][app]") {
    TestSyncManager tsm(get_config(), {});
    auto app = tsm.app();

    SECTION("log in, log out and remove") {
        CHECK(app->sync_manager()->all_users().size() == 0);
        CHECK(app->sync_manager()->get_current_user() == nullptr);

        auto user = log_in(app, app::AppCredentials::username_password("email", "pass"));

        CHECK(user->state() == SyncUser::State::LoggedIn);

        app->log_out(user, [&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
        });

        CHECK(user->state() == SyncUser::State::LoggedOut);

        app->remove_user(user, [&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
        });
        CHECK(app->sync_manager()->all_users().size() == 0);

        util::Optional<app::AppError> error;
        app->remove_user(user, [&](Optional<app::AppError> err) {
            error = err;
        });
        CHECK(error->error_code.value() > 0);
        CHECK(app->sync_manager()->all_users().size() == 0);
        CHECK(user->state() == SyncUser::State::Removed);
    }
}

TEST_CASE("app: link_user", "[sync][app]") {
    TestSyncManager tsm(get_config(), {});
    auto app = tsm.app();

    auto email = util::format("realm_tests_do_autoverify%1@%2.com", random_string(10), random_string(10));
    auto password = random_string(10);

    auto custom_credentials = app::AppCredentials::facebook("a_token");
    auto email_pass_credentials = app::AppCredentials::username_password(email, password);

    auto sync_user = log_in(app, email_pass_credentials);
    CHECK(sync_user->provider_type() == IdentityProviderUsernamePassword);

    SECTION("successful link") {
        bool processed = false;
        app->link_user(sync_user, custom_credentials,
                       [&](std::shared_ptr<SyncUser> user, Optional<app::AppError> error) {
                           REQUIRE_FALSE(error);
                           REQUIRE(user);
                           CHECK(user->identity() == sync_user->identity());
                           processed = true;
                       });
        CHECK(processed);
    }

    SECTION("link_user should fail when logged out") {
        app->log_out([&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
        });

        bool processed = false;
        app->link_user(sync_user, custom_credentials,
                       [&](std::shared_ptr<SyncUser> user, Optional<app::AppError> error) {
                           CHECK(error->message == "The specified user is not logged in");
                           CHECK(!user);
                           processed = true;
                       });
        CHECK(processed);
    }
}

TEST_CASE("app: auth providers", "[sync][app]") {
    SECTION("auth providers facebook") {
        auto credentials = app::AppCredentials::facebook("a_token");
        CHECK(credentials.provider() == AuthProvider::FACEBOOK);
        CHECK(credentials.provider_as_string() == IdentityProviderFacebook);
        CHECK(credentials.serialize_as_json() == "{\"accessToken\":\"a_token\",\"provider\":\"oauth2-facebook\"}");
    }

    SECTION("auth providers anonymous") {
        auto credentials = app::AppCredentials::anonymous();
        CHECK(credentials.provider() == AuthProvider::ANONYMOUS);
        CHECK(credentials.provider_as_string() == IdentityProviderAnonymous);
        CHECK(credentials.serialize_as_json() == "{\"provider\":\"anon-user\"}");
    }

    SECTION("auth providers google authCode") {
        auto credentials = app::AppCredentials::google(AuthCode("a_token"));
        CHECK(credentials.provider() == AuthProvider::GOOGLE);
        CHECK(credentials.provider_as_string() == IdentityProviderGoogle);
        CHECK(credentials.serialize_as_json() == "{\"authCode\":\"a_token\",\"provider\":\"oauth2-google\"}");
    }

    SECTION("auth providers google idToken") {
        auto credentials = app::AppCredentials::google(IdToken("a_token"));
        CHECK(credentials.provider() == AuthProvider::GOOGLE);
        CHECK(credentials.provider_as_string() == IdentityProviderGoogle);
        CHECK(credentials.serialize_as_json() == "{\"id_token\":\"a_token\",\"provider\":\"oauth2-google\"}");
    }

    SECTION("auth providers apple") {
        auto credentials = app::AppCredentials::apple("a_token");
        CHECK(credentials.provider() == AuthProvider::APPLE);
        CHECK(credentials.provider_as_string() == IdentityProviderApple);
        CHECK(credentials.serialize_as_json() == "{\"id_token\":\"a_token\",\"provider\":\"oauth2-apple\"}");
    }

    SECTION("auth providers custom") {
        auto credentials = app::AppCredentials::custom("a_token");
        CHECK(credentials.provider() == AuthProvider::CUSTOM);
        CHECK(credentials.provider_as_string() == IdentityProviderCustom);
        CHECK(credentials.serialize_as_json() == "{\"provider\":\"custom-token\",\"token\":\"a_token\"}");
    }

    SECTION("auth providers username password") {
        auto credentials = app::AppCredentials::username_password("user", "pass");
        CHECK(credentials.provider() == AuthProvider::USERNAME_PASSWORD);
        CHECK(credentials.provider_as_string() == IdentityProviderUsernamePassword);
        CHECK(credentials.serialize_as_json() ==
              "{\"password\":\"pass\",\"provider\":\"local-userpass\",\"username\":\"user\"}");
    }

    SECTION("auth providers function") {
        bson::BsonDocument function_params{{"name", "mongo"}};
        auto credentials = app::AppCredentials::function(function_params);
        CHECK(credentials.provider() == AuthProvider::FUNCTION);
        CHECK(credentials.provider_as_string() == IdentityProviderFunction);
        CHECK(credentials.serialize_as_json() == "{\"name\":\"mongo\"}");
    }

    SECTION("auth providers user api key") {
        auto credentials = app::AppCredentials::user_api_key("a key");
        CHECK(credentials.provider() == AuthProvider::USER_API_KEY);
        CHECK(credentials.provider_as_string() == IdentityProviderUserAPIKey);
        CHECK(credentials.serialize_as_json() == "{\"key\":\"a key\",\"provider\":\"api-key\"}");
    }

    SECTION("auth providers server api key") {
        auto credentials = app::AppCredentials::server_api_key("a key");
        CHECK(credentials.provider() == AuthProvider::SERVER_API_KEY);
        CHECK(credentials.provider_as_string() == IdentityProviderServerAPIKey);
        CHECK(credentials.serialize_as_json() == "{\"key\":\"a key\",\"provider\":\"api-key\"}");
    }
}

TEST_CASE("app: refresh access token unit tests", "[sync][app]") {
    auto setup_user = [](std::shared_ptr<App> app) {
        if (app->sync_manager()->get_current_user()) {
            return;
        }
        app->sync_manager()->get_user("a_user_id", good_access_token, good_access_token, "anon-user",
                                      dummy_device_id);
    };

    SECTION("refresh custom data happy path") {
        static bool session_route_hit = false;

        struct transport : UnitTestTransport {
            void send_request_to_server(const Request request, std::function<void(const Response)> completion_block)
            {
                if (request.url.find("/session") != std::string::npos) {
                    session_route_hit = true;
                    nlohmann::json json{{"access_token", good_access_token}};
                    completion_block({200, 0, {}, json.dump()});
                }
                else {
                    UnitTestTransport::send_request_to_server(request, completion_block);
                }
            }
        };

        TestSyncManager sync_manager(get_config(factory<transport>));
        auto app = sync_manager.app();
        setup_user(app);

        bool processed = false;
        app->refresh_custom_data(app->sync_manager()->get_current_user(), [&](const Optional<AppError>& error) {
            REQUIRE_FALSE(error);
            CHECK(session_route_hit);
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("refresh custom data sad path") {
        static bool session_route_hit = false;

        struct transport : UnitTestTransport {
            void send_request_to_server(const Request request, std::function<void(const Response)> completion_block)
            {
                if (request.url.find("/session") != std::string::npos) {
                    session_route_hit = true;
                    nlohmann::json json{{"access_token", bad_access_token}};
                    completion_block({200, 0, {}, json.dump()});
                }
                else {
                    UnitTestTransport::send_request_to_server(request, completion_block);
                }
            }
        };

        TestSyncManager sync_manager(get_config(factory<transport>));
        auto app = sync_manager.app();
        setup_user(app);

        bool processed = false;
        app->refresh_custom_data(app->sync_manager()->get_current_user(), [&](const Optional<AppError>& error) {
            CHECK(error->message == "jwt missing parts");
            CHECK(error->error_code.value() == 1);
            CHECK(session_route_hit);
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("refresh token ensure flow is correct") {
        /*
         Expected flow:
         Login - this gets access and refresh tokens
         Get profile - throw back a 401 error
         Refresh token - get a new token for the user
         Get profile - get the profile with the new token
         */

            struct transport : GenericNetworkTransport {
                bool login_hit = false;
                bool get_profile_1_hit = false;
                bool get_profile_2_hit = false;
                bool refresh_hit = false;

                void send_request_to_server(const Request request,
                                            std::function<void(const Response)> completion_block)
                {
                    if (request.url.find("/login") != std::string::npos) {
                        login_hit = true;
                        completion_block({200, 0, {}, user_json(good_access_token).dump()});
                    }
                    else if (request.url.find("/profile") != std::string::npos) {
                        CHECK(login_hit);

                        auto access_token = request.headers.at("Authorization");
                        // simulated bad token request
                        if (access_token.find(good_access_token2) != std::string::npos) {
                            CHECK(login_hit);
                            CHECK(get_profile_1_hit);
                            CHECK(refresh_hit);

                            get_profile_2_hit = true;

                            completion_block({200, 0, {}, user_profile_json().dump()});
                        }
                        else if (access_token.find(good_access_token) != std::string::npos) {
                            CHECK(!get_profile_2_hit);
                            get_profile_1_hit = true;

                            completion_block({401, 0, {}});
                        }
                    }
                    else if (request.url.find("/session") != std::string::npos &&
                             request.method == HttpMethod::post) {
                        CHECK(login_hit);
                        CHECK(get_profile_1_hit);
                        CHECK(!get_profile_2_hit);
                        refresh_hit = true;

                        nlohmann::json json{{"access_token", good_access_token2}};
                        completion_block({200, 0, {}, json.dump()});
                    }
                    else if (request.url.find("/location") != std::string::npos) {
                        CHECK(request.method == HttpMethod::get);
                        completion_block({200,
                                          0,
                                          {},
                                          "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":"
                                          "\"http://localhost:9090\",\"ws_hostname\":\"ws://localhost:9090\"}"});
                    }
                }
            };

            TestSyncManager sync_manager(get_config(factory<transport>));
            auto app = sync_manager.app();
            setup_user(app);
            REQUIRE(log_in(app));
    }
}

namespace {
class AsyncMockNetworkTransport {
public:
    AsyncMockNetworkTransport()
        : transport_thread(&AsyncMockNetworkTransport::worker_routine, this)
    {
    }

    void add_work_item(Response response, std::function<void(const Response)> completion_block)
    {
        std::lock_guard<std::mutex> lk(transport_work_mutex);
        transport_work.push_front(ResponseWorkItem{std::move(response), std::move(completion_block)});
        transport_work_cond.notify_one();
    }

    void add_work_item(std::function<void()> cb)
    {
        std::lock_guard<std::mutex> lk(transport_work_mutex);
        transport_work.push_front(std::move(cb));
        transport_work_cond.notify_one();
    }

    void mark_complete()
    {
        std::unique_lock<std::mutex> lk(transport_work_mutex);
        test_complete = true;
        transport_work_cond.notify_one();
        lk.unlock();
        transport_thread.join();
    }

private:
    struct ResponseWorkItem {
        Response response;
        std::function<void(const Response)> completion_block;
    };

    void worker_routine()
    {
        std::unique_lock<std::mutex> lk(transport_work_mutex);
        for (;;) {
            transport_work_cond.wait(lk, [&] {
                return test_complete || !transport_work.empty();
            });

            if (!transport_work.empty()) {
                auto work_item = std::move(transport_work.back());
                transport_work.pop_back();
                lk.unlock();

                mpark::visit(util::overload{[](ResponseWorkItem& work_item) {
                                                work_item.completion_block(std::move(work_item.response));
                                            },
                                            [](std::function<void()>& cb) {
                                                cb();
                                            }},
                             work_item);

                lk.lock();
                continue;
            }

            if (test_complete) {
                return;
            }
        }
    }

    std::mutex transport_work_mutex;
    std::condition_variable transport_work_cond;
    bool test_complete = false;
    std::list<mpark::variant<ResponseWorkItem, std::function<void()>>> transport_work;
    JoiningThread transport_thread;
};

} // namespace

TEST_CASE("app: app destroyed during token refresh", "[sync][app]") {
    AsyncMockNetworkTransport mock_transport_worker;
    enum class TestState { unknown, location, login, profile_1, profile_2, refresh_1, refresh_2, refresh_3 };
    struct TestStateBundle {
        void advance_to(TestState new_state)
        {
            std::lock_guard<std::mutex> lk(mutex);
            state = new_state;
            cond.notify_one();
        }

        TestState get() const
        {
            std::lock_guard<std::mutex> lk(mutex);
            return state;
        }

        void wait_for(TestState new_state)
        {
            std::unique_lock<std::mutex> lk(mutex);
            cond.wait(lk, [&] {
                return state == new_state;
            });
        }

        mutable std::mutex mutex;
        std::condition_variable cond;

        TestState state = TestState::unknown;
    } state;
    std::function<std::unique_ptr<GenericNetworkTransport>()> generic_factory = [&] {
        struct transport : public GenericNetworkTransport {
            transport(AsyncMockNetworkTransport& worker, TestStateBundle& state)
                : mock_transport_worker(worker)
                , state(state)
            {
            }

            void send_request_to_server(const Request request,
                                        std::function<void(const Response)> completion_block) override

            {
                std::cerr << request.url << std::endl;
                if (request.url.find("/login") != std::string::npos) {
                    CHECK(state.get() == TestState::location);
                    state.advance_to(TestState::login);
                    mock_transport_worker.add_work_item(
                        Response{200, 0, {}, user_json(encode_fake_jwt("access token 1")).dump()},
                        std::move(completion_block));
                }
                else if (request.url.find("/profile") != std::string::npos) {
                    // simulated bad token request
                    auto cur_state = state.get();
                    CHECK((cur_state == TestState::refresh_1 || cur_state == TestState::login));
                    if (cur_state == TestState::refresh_1) {
                        state.advance_to(TestState::profile_2);
                        mock_transport_worker.add_work_item(Response{200, 0, {}, user_profile_json().dump()},
                                                            std::move(completion_block));
                    }
                    else if (cur_state == TestState::login) {
                        state.advance_to(TestState::profile_1);
                        mock_transport_worker.add_work_item(Response{401, 0, {}}, std::move(completion_block));
                    }
                }
                else if (request.url.find("/session") != std::string::npos && request.method == HttpMethod::post) {
                    if (state.get() == TestState::profile_1) {
                        state.advance_to(TestState::refresh_1);
                        nlohmann::json json{{"access_token", encode_fake_jwt("access token 1")}};
                        mock_transport_worker.add_work_item(Response{200, 0, {}, json.dump()},
                                                            std::move(completion_block));
                    }
                    else if (state.get() == TestState::profile_2) {
                        state.advance_to(TestState::refresh_2);
                        mock_transport_worker.add_work_item(Response{200, 0, {}, "{\"error\":\"too bad, buddy!\"}"},
                                                            std::move(completion_block));
                    }
                    else {
                        CHECK(state.get() == TestState::refresh_2);
                        state.advance_to(TestState::refresh_3);
                        nlohmann::json json{{"access_token", encode_fake_jwt("access token 2")}};
                        mock_transport_worker.add_work_item(Response{200, 0, {}, json.dump()},
                                                            std::move(completion_block));
                    }
                }
                else if (request.url.find("/location") != std::string::npos) {
                    CHECK(request.method == HttpMethod::get);
                    CHECK(state.get() == TestState::unknown);
                    state.advance_to(TestState::location);
                    mock_transport_worker.add_work_item(
                        Response{200,
                                 0,
                                 {},
                                 "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":"
                                 "\"http://localhost:9090\",\"ws_hostname\":\"ws://localhost:9090\"}"},
                        std::move(completion_block));
                }
            }

            AsyncMockNetworkTransport& mock_transport_worker;
            TestStateBundle& state;
        };
        return std::make_unique<transport>(mock_transport_worker, state);
    };

    TestSyncManager sync_manager(get_config(generic_factory));
    auto app = sync_manager.app();

    {
        std::mutex mutex;
        std::shared_ptr<SyncUser> cur_user;
        app->log_in_with_credentials(AppCredentials::anonymous(),
                                     [&](std::shared_ptr<SyncUser> user, Optional<app::AppError> error) {
                                         std::lock_guard lock(mutex);
                                         CHECK(user);
                                         REQUIRE_FALSE(error);
                                         cur_user = std::move(user);
                                     });

        state.wait_for(TestState::profile_2);
        util::EventLoop::main().run_until([&] {
            std::lock_guard lock(mutex);
            return cur_user != nullptr;
        });
        CHECK(cur_user);

        Realm::Config realm_config;
        realm_config.sync_config = std::make_shared<SyncConfig>(app->current_user(), bson::Bson("foo"));
        realm_config.sync_config->client_resync_mode = ClientResyncMode::Manual;
        realm_config.sync_config->error_handler = [](std::shared_ptr<SyncSession>, SyncError error) {
            std::cout << error.message << std::endl;
        };
        realm_config.schema_version = 1;
        realm_config.path = app->sync_manager()->path_for_realm(*cur_user, "default.realm");

        auto r = Realm::get_shared_realm(std::move(realm_config));
        auto session = cur_user->session_for_on_disk_path(r->config().path);
        mock_transport_worker.add_work_item([session] {
            session->initiate_access_token_refresh();
        });
    }
    for (const auto& user : app->all_users()) {
        user->log_out();
    }

    util::EventLoop::main().run_until([&] {
        return !app->sync_manager()->has_existing_sessions();
    });

    mock_transport_worker.mark_complete();
}

TEST_CASE("app: metadata is persisted between sessions", "[sync][app]") {
    static const auto test_hostname = "proto://host:1234";
    static const auto test_ws_hostname = "wsproto://host:1234";

    struct transport : UnitTestTransport {
        void send_request_to_server(const Request request,
                                    std::function<void(const Response)> completion_block) override
        {
            if (request.url.find("/location") != std::string::npos) {
                CHECK(request.method == HttpMethod::get);
                completion_block({200,
                                  0,
                                  {},
                                  nlohmann::json({{"deployment_model", "LOCAL"},
                                                  {"location", "IE"},
                                                  {"hostname", test_hostname},
                                                  {"ws_hostname", test_ws_hostname}})
                                      .dump()});
            }
            else if (request.url.find("functions/call") != std::string::npos) {
                REQUIRE(request.url.rfind(test_hostname, 0) != std::string::npos);
            }
            else {
                UnitTestTransport::send_request_to_server(request, completion_block);
            }
        }
    };

    TestSyncManager::Config config = get_config(factory<transport>);
    config.base_path = util::make_temp_dir();
    config.should_teardown_test_directory = false;

    {
        TestSyncManager sync_manager(config, {});
        auto app = sync_manager.app();
        app->log_in_with_credentials(AppCredentials::anonymous(), [](auto, auto error) {
            REQUIRE_FALSE(error);
        });
    }

    App::clear_cached_apps();
    config.override_sync_route = false;
    config.should_teardown_test_directory = true;
    {
        TestSyncManager sync_manager(config, {});
        auto app = sync_manager.app();
        REQUIRE(app->sync_manager()->sync_route().rfind(test_ws_hostname, 0) != std::string::npos);
        app->call_function("function", {}, [](auto error, auto) {
            REQUIRE_FALSE(error);
        });
    }
}

TEST_CASE("app: make_streaming_request", "[sync][app]") {
    UnitTestTransport::access_token = good_access_token;

    constexpr uint64_t timeout_ms = 60000;
    auto config = get_config();
    config.default_request_timeout_ms = timeout_ms;
    TestSyncManager tsm(TestSyncManager::Config(config), {});
    auto app = tsm.app();

    std::shared_ptr<SyncUser> user = log_in(app);

    using Headers = decltype(Request().headers);

    const auto url_prefix = "field/api/client/v2.0/app/app name/functions/call?baas_request="sv;
    const auto get_request_args = [&](const Request& req) {
        REQUIRE(req.url.substr(0, url_prefix.size()) == url_prefix);
        auto args = req.url.substr(url_prefix.size());
        if (auto amp = args.find('&'); amp != std::string::npos) {
            args.resize(amp);
        }

        auto vec = util::base64_decode_to_vector(util::uri_percent_decode(args));
        REQUIRE(!!vec);
        auto parsed = bson::parse({vec->data(), vec->size()});
        REQUIRE(parsed.type() == bson::Bson::Type::Document);
        auto out = parsed.operator const bson::BsonDocument&();
        CHECK(out.size() == 3);
        return out;
    };

    const auto make_request = [&](std::shared_ptr<SyncUser> user, auto&&... args) {
        auto req = app->make_streaming_request(user, "func", bson::BsonArray{args...}, {"svc"});
        CHECK(req.method == HttpMethod::get);
        CHECK(req.body == "");
        CHECK(req.headers == Headers{{"Accept", "text/event-stream"}});
        CHECK(req.timeout_ms == timeout_ms);
        CHECK(req.uses_refresh_token == false);

        auto req_args = get_request_args(req);
        CHECK(req_args["name"] == "func");
        CHECK(req_args["service"] == "svc");
        CHECK(req_args["arguments"] == bson::BsonArray{args...});

        return req;
    };

    SECTION("no args") {
        auto req = make_request(nullptr);
        CHECK(req.url.find('&') == std::string::npos);
    }
    SECTION("args") {
        auto req = make_request(nullptr, "arg1", "arg2");
        CHECK(req.url.find('&') == std::string::npos);
    }
    SECTION("percent encoding") {
        // These force the base64 encoding to have + and / bytes and = padding, all of which are uri encoded.
        auto req = make_request(nullptr, ">>>>>?????");

        CHECK(req.url.find('&') == std::string::npos);
        CHECK(req.url.find("%2B") != std::string::npos);   // + (from >)
        CHECK(req.url.find("%2F") != std::string::npos);   // / (from ?)
        CHECK(req.url.find("%3D") != std::string::npos);   // = (tail padding)
        CHECK(req.url.rfind("%3D") == req.url.size() - 3); // = (tail padding)
    }
    SECTION("with user") {
        auto req = make_request(user, "arg1", "arg2");

        auto amp = req.url.find('&');
        REQUIRE(amp != std::string::npos);
        auto tail = req.url.substr(amp);
        REQUIRE(tail == ("&baas_at=" + user->access_token()));
    }
}

TEST_CASE("app: sync_user_profile unit tests", "[sync][app]") {
    SECTION("with empty map") {
        auto profile = SyncUserProfile(bson::BsonDocument());
        CHECK(profile.name() == util::none);
        CHECK(profile.email() == util::none);
        CHECK(profile.picture_url() == util::none);
        CHECK(profile.first_name() == util::none);
        CHECK(profile.last_name() == util::none);
        CHECK(profile.gender() == util::none);
        CHECK(profile.birthday() == util::none);
        CHECK(profile.min_age() == util::none);
        CHECK(profile.max_age() == util::none);
    }
    SECTION("with full map") {
        auto profile = SyncUserProfile(bson::BsonDocument({
            {"first_name", "Jan"},
            {"last_name", "Jaanson"},
            {"name", "Jan Jaanson"},
            {"email", "jan.jaanson@jaanson.com"},
            {"gender", "none"},
            {"birthday", "January 1, 1970"},
            {"min_age", "0"},
            {"max_age", "100"},
            {"picture_url", "some"},
        }));
        CHECK(profile.name() == "Jan Jaanson");
        CHECK(profile.email() == "jan.jaanson@jaanson.com");
        CHECK(profile.picture_url() == "some");
        CHECK(profile.first_name() == "Jan");
        CHECK(profile.last_name() == "Jaanson");
        CHECK(profile.gender() == "none");
        CHECK(profile.birthday() == "January 1, 1970");
        CHECK(profile.min_age() == "0");
        CHECK(profile.max_age() == "100");
    }
}
