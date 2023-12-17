////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
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

#include "collection_fixtures.hpp"
#include "util/sync/baas_admin_api.hpp"
#include "util/sync/sync_test_utils.hpp"
#include "util/unit_test_transport.hpp"

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/app_utils.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/mongo_database.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/object-store/util/uuid.hpp>
#include <realm/sync/network/default_socket.hpp>
#include <realm/sync/network/websocket.hpp>
#include <realm/sync/noinst/server/access_token.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/overload.hpp>
#include <realm/util/platform_info.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/util/uri.hpp>

#include <catch2/catch_all.hpp>
#include <external/mpark/variant.hpp>

#include <condition_variable>
#include <future>
#include <iostream>
#include <list>
#include <mutex>

using namespace realm;
using namespace realm::app;
using util::any_cast;
using util::Optional;

using namespace std::string_view_literals;
using namespace std::literals::string_literals;

#if REALM_ENABLE_AUTH_TESTS

#include <realm/util/sha_crypto.hpp>

static std::string create_jwt(const std::string& appId)
{
    nlohmann::json header = {{"alg", "HS256"}, {"typ", "JWT"}};
    nlohmann::json payload = {{"aud", appId}, {"sub", "someUserId"}, {"exp", 1961896476}};

    payload["user_data"]["name"] = "Foo Bar";
    payload["user_data"]["occupation"] = "firefighter";

    payload["my_metadata"]["name"] = "Bar Foo";
    payload["my_metadata"]["occupation"] = "stock analyst";

    std::string headerStr = header.dump();
    std::string payloadStr = payload.dump();

    std::string encoded_header;
    encoded_header.resize(util::base64_encoded_size(headerStr.length()));
    util::base64_encode(headerStr.data(), headerStr.length(), encoded_header.data(), encoded_header.size());

    std::string encoded_payload;
    encoded_payload.resize(util::base64_encoded_size(payloadStr.length()));
    util::base64_encode(payloadStr.data(), payloadStr.length(), encoded_payload.data(), encoded_payload.size());

    // Remove padding characters.
    while (encoded_header.back() == '=')
        encoded_header.pop_back();
    while (encoded_payload.back() == '=')
        encoded_payload.pop_back();

    std::string jwtPayload = encoded_header + "." + encoded_payload;

    std::array<unsigned char, 32> hmac;
    unsigned char key[] = "My_very_confidential_secretttttt";
    util::hmac_sha256(util::unsafe_span_cast<unsigned char>(jwtPayload), hmac, util::Span<uint8_t, 32>(key, 32));

    std::string signature;
    signature.resize(util::base64_encoded_size(hmac.size()));
    util::base64_encode(reinterpret_cast<char*>(hmac.data()), hmac.size(), signature.data(), signature.size());
    while (signature.back() == '=')
        signature.pop_back();
    std::replace(signature.begin(), signature.end(), '+', '-');
    std::replace(signature.begin(), signature.end(), '/', '_');

    return jwtPayload + "." + signature;
}

// MARK: - Verify AppError with all error codes
TEST_CASE("app: verify app error codes", "[sync][app][local]") {
    auto error_codes = ErrorCodes::get_error_list();
    std::vector<std::pair<int, std::string>> http_status_codes = {
        {0, ""},
        {100, "http error code considered fatal: some http error. Informational: 100"},
        {200, ""},
        {300, "http error code considered fatal: some http error. Redirection: 300"},
        {400, "http error code considered fatal: some http error. Client Error: 400"},
        {500, "http error code considered fatal: some http error. Server Error: 500"},
        {600, "http error code considered fatal: some http error. Unknown HTTP Error: 600"}};

    auto make_http_error = [](std::optional<std::string_view> error_code, int http_status = 500,
                              std::optional<std::string_view> error = "some error",
                              std::optional<std::string_view> link = "http://dummy-link/") -> app::Response {
        nlohmann::json body;
        if (error_code) {
            body["error_code"] = *error_code;
        }
        if (error) {
            body["error"] = *error;
        }
        if (link) {
            body["link"] = *link;
        }

        return {
            http_status,
            0,
            {{"Content-Type", "application/json"}},
            body.empty() ? "{}" : body.dump(),
        };
    };

    // Success response
    app::Response response = {200, 0, {}, ""};
    auto app_error = AppUtils::check_for_errors(response);
    REQUIRE(!app_error);

    // Empty error code
    response = make_http_error("");
    app_error = AppUtils::check_for_errors(response);
    REQUIRE(app_error);
    REQUIRE(app_error->code() == ErrorCodes::AppUnknownError);
    REQUIRE(app_error->code_string() == "AppUnknownError");
    REQUIRE(app_error->server_error.empty());
    REQUIRE(app_error->reason() == "some error");
    REQUIRE(app_error->link_to_server_logs == "http://dummy-link/");
    REQUIRE(*app_error->additional_status_code == 500);

    // Missing error code
    response = make_http_error(std::nullopt);
    app_error = AppUtils::check_for_errors(response);
    REQUIRE(app_error);
    REQUIRE(app_error->code() == ErrorCodes::AppUnknownError);
    REQUIRE(app_error->code_string() == "AppUnknownError");
    REQUIRE(app_error->server_error.empty());
    REQUIRE(app_error->reason() == "some error");
    REQUIRE(app_error->link_to_server_logs == "http://dummy-link/");
    REQUIRE(*app_error->additional_status_code == 500);

    // Missing error code and error message with success http status
    response = make_http_error(std::nullopt, 200, std::nullopt);
    app_error = AppUtils::check_for_errors(response);
    REQUIRE(!app_error);

    for (auto [name, error] : error_codes) {
        // All error codes should not cause an exception
        if (error != ErrorCodes::HTTPError && error != ErrorCodes::OK) {
            response = make_http_error(name);
            app_error = AppUtils::check_for_errors(response);
            REQUIRE(app_error);
            if (ErrorCodes::error_categories(error).test(ErrorCategory::app_error)) {
                REQUIRE(app_error->code() == error);
                REQUIRE(app_error->code_string() == name);
            }
            else {
                REQUIRE(app_error->code() == ErrorCodes::AppServerError);
                REQUIRE(app_error->code_string() == "AppServerError");
            }
            REQUIRE(app_error->server_error == name);
            REQUIRE(app_error->reason() == "some error");
            REQUIRE(app_error->link_to_server_logs == "http://dummy-link/");
            REQUIRE(app_error->additional_status_code);
            REQUIRE(*app_error->additional_status_code == 500);
        }
    }

    response = make_http_error("AppErrorMissing", 404);
    app_error = AppUtils::check_for_errors(response);
    REQUIRE(app_error);
    REQUIRE(app_error->code() == ErrorCodes::AppServerError);
    REQUIRE(app_error->code_string() == "AppServerError");
    REQUIRE(app_error->server_error == "AppErrorMissing");
    REQUIRE(app_error->reason() == "some error");
    REQUIRE(app_error->link_to_server_logs == "http://dummy-link/");
    REQUIRE(app_error->additional_status_code);
    REQUIRE(*app_error->additional_status_code == 404);

    // HTTPError with different status values
    for (auto [status, message] : http_status_codes) {
        response = {
            status,
            0,
            {},
            "some http error",
        };
        app_error = AppUtils::check_for_errors(response);
        if (message.empty()) {
            REQUIRE(!app_error);
            continue;
        }
        REQUIRE(app_error);
        REQUIRE(app_error->code() == ErrorCodes::HTTPError);
        REQUIRE(app_error->code_string() == "HTTPError");
        REQUIRE(app_error->server_error.empty());
        REQUIRE(app_error->reason() == message);
        REQUIRE(app_error->link_to_server_logs.empty());
        REQUIRE(app_error->additional_status_code);
        REQUIRE(*app_error->additional_status_code == status);
    }

    // Missing error code and error message with fatal http status
    response = {
        501,
        0,
        {},
        "",
    };
    app_error = AppUtils::check_for_errors(response);
    REQUIRE(app_error);
    REQUIRE(app_error->code() == ErrorCodes::HTTPError);
    REQUIRE(app_error->code_string() == "HTTPError");
    REQUIRE(app_error->server_error.empty());
    REQUIRE(app_error->reason() == "http error code considered fatal. Server Error: 501");
    REQUIRE(app_error->link_to_server_logs.empty());
    REQUIRE(app_error->additional_status_code);
    REQUIRE(*app_error->additional_status_code == 501);

    // Valid client error code, with body, but no json
    app::Response client_response = {
        501,
        0,
        {},
        "Some error occurred",
        ErrorCodes::BadBsonParse, // client_error_code
    };
    app_error = AppUtils::check_for_errors(client_response);
    REQUIRE(app_error);
    REQUIRE(app_error->code() == ErrorCodes::BadBsonParse);
    REQUIRE(app_error->code_string() == "BadBsonParse");
    REQUIRE(app_error->server_error.empty());
    REQUIRE(app_error->reason() == "Some error occurred");
    REQUIRE(app_error->link_to_server_logs.empty());
    REQUIRE(app_error->additional_status_code);
    REQUIRE(*app_error->additional_status_code == 501);

    // Same response with client error code, but no body
    client_response.body = "";
    app_error = AppUtils::check_for_errors(client_response);
    REQUIRE(app_error);
    REQUIRE(app_error->reason() == "client error code value considered fatal");

    // Valid custom status code, with body, but no json
    app::Response custom_response = {501,
                                     4999, // custom_status_code
                                     {},
                                     "Some custom error occurred"};
    app_error = AppUtils::check_for_errors(custom_response);
    REQUIRE(app_error);
    REQUIRE(app_error->code() == ErrorCodes::CustomError);
    REQUIRE(app_error->code_string() == "CustomError");
    REQUIRE(app_error->server_error.empty());
    REQUIRE(app_error->reason() == "Some custom error occurred");
    REQUIRE(app_error->link_to_server_logs.empty());
    REQUIRE(app_error->additional_status_code);
    REQUIRE(*app_error->additional_status_code == 4999);

    // Same response with custom status code, but no body
    custom_response.body = "";
    app_error = AppUtils::check_for_errors(custom_response);
    REQUIRE(app_error);
    REQUIRE(app_error->reason() == "non-zero custom status code considered fatal");
}

// MARK: - Login with Credentials Tests

TEST_CASE("app: login_with_credentials integration", "[sync][app][user][baas]") {
    SECTION("login") {
        TestAppSession session;
        auto app = session.app();
        app->log_out([](auto) {});

        int subscribe_processed = 0;
        auto token = app->subscribe([&subscribe_processed](auto& app) {
            if (!subscribe_processed) {
                REQUIRE(app.current_user());
            }
            else {
                REQUIRE_FALSE(app.current_user());
            }
            subscribe_processed++;
        });

        auto user = log_in(app);
        CHECK(!user->device_id().empty());
        CHECK(user->has_device_id());

        bool processed = false;
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

TEST_CASE("app: UsernamePasswordProviderClient integration", "[sync][app][user][baas]") {
    const std::string base_url = get_base_url();
    AutoVerifiedEmailCredentials creds;
    auto email = creds.email;
    auto password = creds.password;

    TestAppSession session;
    auto app = session.app();
    auto client = app->provider_client<App::UsernamePasswordProviderClient>();

    bool processed = false;

    client.register_email(email, password, [&](Optional<AppError> error) {
        CAPTURE(email);
        CAPTURE(password);
        REQUIRE_FALSE(error); // first registration success
    });

    SECTION("double registration should fail") {
        client.register_email(email, password, [&](Optional<AppError> error) {
            // Error returned states the account has already been created
            REQUIRE(error);
            CHECK(error->reason() == "name already in use");
            CHECK(error->code() == ErrorCodes::AccountNameInUse);
            CHECK(!error->link_to_server_logs.empty());
            CHECK(error->link_to_server_logs.find(base_url) != std::string::npos);
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("double registration should fail") {
        // the server registration function will reject emails that do not contain "realm_tests_do_autoverify"
        std::string email_to_reject = util::format("%1@%2.com", random_string(10), random_string(10));
        client.register_email(email_to_reject, password, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->reason() == util::format("failed to confirm user \"%1\"", email_to_reject));
            CHECK(error->code() == ErrorCodes::BadRequest);
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("can login with registered account") {
        auto user = log_in(app, creds);
        CHECK(user->user_profile().email() == email);
    }

    SECTION("cannot login with wrong password") {
        app->log_in_with_credentials(AppCredentials::username_password(email, "boogeyman"),
                                     [&](std::shared_ptr<realm::SyncUser> user, Optional<AppError> error) {
                                         CHECK(!user);
                                         REQUIRE(error);
                                         REQUIRE(error->code() == ErrorCodes::InvalidPassword);
                                         processed = true;
                                     });
        CHECK(processed);
    }

    SECTION("confirm user") {
        client.confirm_user("a_token", "a_token_id", [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->reason() == "invalid token data");
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("resend confirmation email") {
        client.resend_confirmation_email(email, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->reason() == "already confirmed");
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("reset password invalid tokens") {
        client.reset_password(password, "token_sample", "token_id_sample", [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->reason() == "invalid token data");
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
        client.call_reset_password_function(email, accepted_new_password, {}, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("reset password function failure") {
        std::string rejected_password = util::format("%1", random_string(10));
        client.call_reset_password_function(email, rejected_password, {"foo", "bar"}, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->reason() == util::format("failed to reset password for user \"%1\"", email));
            CHECK(error->is_service_error());
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("reset password function for invalid user fails") {
        client.call_reset_password_function(util::format("%1@%2.com", random_string(5), random_string(5)), password,
                                            {"foo", "bar"}, [&](Optional<AppError> error) {
                                                REQUIRE(error);
                                                CHECK(error->reason() == "user not found");
                                                CHECK(error->is_service_error());
                                                CHECK(error->code() == ErrorCodes::UserNotFound);
                                                processed = true;
                                            });
        CHECK(processed);
    }

    SECTION("retry custom confirmation") {
        client.retry_custom_confirmation(email, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->reason() == "already confirmed");
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("retry custom confirmation for invalid user fails") {
        client.retry_custom_confirmation(util::format("%1@%2.com", random_string(5), random_string(5)),
                                         [&](Optional<AppError> error) {
                                             REQUIRE(error);
                                             CHECK(error->reason() == "user not found");
                                             CHECK(error->is_service_error());
                                             CHECK(error->code() == ErrorCodes::UserNotFound);
                                             processed = true;
                                         });
        CHECK(processed);
    }

    SECTION("log in, remove, log in") {
        app->remove_user(app->current_user(), [](auto) {});
        CHECK(app->all_users().size() == 0);
        CHECK(app->current_user() == nullptr);

        auto user = log_in(app, AppCredentials::username_password(email, password));
        CHECK(user->user_profile().email() == email);
        CHECK(user->state() == SyncUser::State::LoggedIn);

        app->remove_user(user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });
        CHECK(user->state() == SyncUser::State::Removed);

        log_in(app, AppCredentials::username_password(email, password));
        CHECK(user->state() == SyncUser::State::Removed);
        CHECK(app->current_user() != user);
        user = app->current_user();
        CHECK(user->user_profile().email() == email);
        CHECK(user->state() == SyncUser::State::LoggedIn);

        app->remove_user(user, [&](Optional<AppError> error) {
            REQUIRE(!error);
            CHECK(app->all_users().size() == 0);
            processed = true;
        });

        CHECK(user->state() == SyncUser::State::Removed);
        CHECK(processed);
        CHECK(app->all_users().size() == 0);
    }
}

// MARK: - UserAPIKeyProviderClient Tests

TEST_CASE("app: UserAPIKeyProviderClient integration", "[sync][app][api key][baas]") {
    TestAppSession session;
    auto app = session.app();
    auto client = app->provider_client<App::UserAPIKeyProviderClient>();

    bool processed = false;
    App::UserAPIKey api_key;

    SECTION("api-key") {
        std::shared_ptr<SyncUser> logged_in_user = app->current_user();
        auto api_key_name = util::format("%1", random_string(15));
        client.create_api_key(api_key_name, logged_in_user,
                              [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
                                  REQUIRE_FALSE(error);
                                  CHECK(user_api_key.name == api_key_name);
                                  api_key = user_api_key;
                              });

        client.fetch_api_key(api_key.id, logged_in_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
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

        client.fetch_api_key(api_key.id, logged_in_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(user_api_key.disabled == false);
            CHECK(user_api_key.name == api_key_name);
            CHECK(user_api_key.id == api_key.id);
        });

        client.disable_api_key(api_key.id, logged_in_user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        client.fetch_api_key(api_key.id, logged_in_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(user_api_key.disabled == true);
            CHECK(user_api_key.name == api_key_name);
        });

        client.delete_api_key(api_key.id, logged_in_user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        client.fetch_api_key(api_key.id, logged_in_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            CHECK(user_api_key.name == "");
            CHECK(error);
            processed = true;
        });

        CHECK(processed);
    }

    SECTION("api-key without a user") {
        std::shared_ptr<SyncUser> no_user = nullptr;
        auto api_key_name = util::format("%1", random_string(15));
        client.create_api_key(api_key_name, no_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->reason() == "must authenticate first");
            CHECK(user_api_key.name == "");
        });

        client.fetch_api_key(api_key.id, no_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->reason() == "must authenticate first");
            CHECK(user_api_key.name == "");
        });

        client.fetch_api_keys(no_user, [&](std::vector<App::UserAPIKey> api_keys, Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->reason() == "must authenticate first");
            CHECK(api_keys.size() == 0);
        });

        client.enable_api_key(api_key.id, no_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->reason() == "must authenticate first");
        });

        client.fetch_api_key(api_key.id, no_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->reason() == "must authenticate first");
            CHECK(user_api_key.name == "");
        });

        client.disable_api_key(api_key.id, no_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->reason() == "must authenticate first");
        });

        client.fetch_api_key(api_key.id, no_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->reason() == "must authenticate first");
            CHECK(user_api_key.name == "");
        });

        client.delete_api_key(api_key.id, no_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->reason() == "must authenticate first");
        });

        client.fetch_api_key(api_key.id, no_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            CHECK(user_api_key.name == "");
            REQUIRE(error);
            CHECK(error->is_service_error());
            CHECK(error->reason() == "must authenticate first");
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("api-key against the wrong user") {
        std::shared_ptr<SyncUser> first_user = app->current_user();
        create_user_and_log_in(app);
        std::shared_ptr<SyncUser> second_user = app->current_user();
        REQUIRE(first_user != second_user);
        auto api_key_name = util::format("%1", random_string(15));
        App::UserAPIKey api_key;
        App::UserAPIKeyProviderClient provider = app->provider_client<App::UserAPIKeyProviderClient>();

        provider.create_api_key(api_key_name, first_user,
                                [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
                                    REQUIRE_FALSE(error);
                                    CHECK(user_api_key.name == api_key_name);
                                    api_key = user_api_key;
                                });

        provider.fetch_api_key(api_key.id, first_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(user_api_key.name == api_key_name);
            CHECK(user_api_key.id.to_string() == user_api_key.id.to_string());
        });

        provider.fetch_api_key(api_key.id, second_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->reason() == "API key not found");
            CHECK(error->is_service_error());
            CHECK(error->code() == ErrorCodes::APIKeyNotFound);
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
            CHECK(error->reason() == "API key not found");
            CHECK(error->is_service_error());
            CHECK(error->code() == ErrorCodes::APIKeyNotFound);
        });

        provider.fetch_api_key(api_key.id, first_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(user_api_key.disabled == false);
            CHECK(user_api_key.name == api_key_name);
        });

        provider.fetch_api_key(api_key.id, second_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            REQUIRE(error);
            CHECK(user_api_key.name == "");
            CHECK(error->reason() == "API key not found");
            CHECK(error->is_service_error());
            CHECK(error->code() == ErrorCodes::APIKeyNotFound);
        });

        provider.disable_api_key(api_key.id, first_user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        provider.disable_api_key(api_key.id, second_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->reason() == "API key not found");
            CHECK(error->is_service_error());
            CHECK(error->code() == ErrorCodes::APIKeyNotFound);
        });

        provider.fetch_api_key(api_key.id, first_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(user_api_key.disabled == true);
            CHECK(user_api_key.name == api_key_name);
        });

        provider.fetch_api_key(api_key.id, second_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            REQUIRE(error);
            CHECK(user_api_key.name == "");
            CHECK(error->reason() == "API key not found");
            CHECK(error->is_service_error());
            CHECK(error->code() == ErrorCodes::APIKeyNotFound);
        });

        provider.delete_api_key(api_key.id, second_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->reason() == "API key not found");
            CHECK(error->is_service_error());
            CHECK(error->code() == ErrorCodes::APIKeyNotFound);
        });

        provider.delete_api_key(api_key.id, first_user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        provider.fetch_api_key(api_key.id, first_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            CHECK(user_api_key.name == "");
            REQUIRE(error);
            CHECK(error->reason() == "API key not found");
            CHECK(error->is_service_error());
            CHECK(error->code() == ErrorCodes::APIKeyNotFound);
            processed = true;
        });

        provider.fetch_api_key(api_key.id, second_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            CHECK(user_api_key.name == "");
            REQUIRE(error);
            CHECK(error->reason() == "API key not found");
            CHECK(error->is_service_error());
            CHECK(error->code() == ErrorCodes::APIKeyNotFound);
            processed = true;
        });

        CHECK(processed);
    }
}

// MARK: - Auth Providers Function Tests

TEST_CASE("app: auth providers function integration", "[sync][app][user][baas]") {
    TestAppSession session;
    auto app = session.app();

    SECTION("auth providers function integration") {
        bson::BsonDocument function_params{{"realmCustomAuthFuncUserId", "123456"}};
        auto credentials = AppCredentials::function(function_params);
        auto user = log_in(app, credentials);
        REQUIRE(user->identities()[0].provider_type == IdentityProviderFunction);
    }
}

// MARK: - Link User Tests

TEST_CASE("app: Linking user identities", "[sync][app][user][baas]") {
    TestAppSession session;
    auto app = session.app();
    auto user = log_in(app);

    AutoVerifiedEmailCredentials creds;
    app->provider_client<App::UsernamePasswordProviderClient>().register_email(creds.email, creds.password,
                                                                               [&](Optional<AppError> error) {
                                                                                   REQUIRE_FALSE(error);
                                                                               });

    SECTION("anonymous users are reused before they are linked to an identity") {
        REQUIRE(user == log_in(app));
    }

    SECTION("linking a user adds that identity to the user") {
        REQUIRE(user->identities().size() == 1);
        CHECK(user->identities()[0].provider_type == IdentityProviderAnonymous);

        app->link_user(user, creds, [&](std::shared_ptr<SyncUser> user2, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(user == user2);
            REQUIRE(user->identities().size() == 2);
            CHECK(user->identities()[0].provider_type == IdentityProviderAnonymous);
            CHECK(user->identities()[1].provider_type == IdentityProviderUsernamePassword);
        });
    }

    SECTION("linking an identity makes the user no longer returned by anonymous logins") {
        app->link_user(user, creds, [&](std::shared_ptr<SyncUser>, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });
        auto user2 = log_in(app);
        REQUIRE(user != user2);
    }

    SECTION("existing users are reused when logging in via linked identities") {
        app->link_user(user, creds, [](std::shared_ptr<SyncUser>, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });
        app->log_out([](auto error) {
            REQUIRE_FALSE(error);
        });
        REQUIRE(user->state() == SyncUser::State::LoggedOut);
        // Should give us the same user instance despite logging in with a
        // different identity
        REQUIRE(user == log_in(app, creds));
        REQUIRE(user->state() == SyncUser::State::LoggedIn);
    }
}

// MARK: - Delete User Tests

TEST_CASE("app: delete anonymous user integration", "[sync][app][user][baas]") {
    TestAppSession session;
    auto app = session.app();

    SECTION("delete user expect success") {
        CHECK(app->all_users().size() == 1);

        // Log in user 1
        auto user_a = app->current_user();
        CHECK(user_a->state() == SyncUser::State::LoggedIn);
        app->delete_user(user_a, [&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            // a logged out anon user will be marked as Removed, not LoggedOut
            CHECK(user_a->state() == SyncUser::State::Removed);
        });
        CHECK(app->all_users().empty());
        CHECK(app->current_user() == nullptr);

        app->delete_user(user_a, [&](Optional<app::AppError> error) {
            CHECK(error->reason() == "User must be logged in to be deleted.");
            CHECK(app->all_users().size() == 0);
        });

        // Log in user 2
        auto user_b = log_in(app);
        CHECK(app->current_user() == user_b);
        CHECK(user_b->state() == SyncUser::State::LoggedIn);
        CHECK(app->all_users().size() == 1);

        app->delete_user(user_b, [&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(app->all_users().size() == 0);
        });

        CHECK(app->current_user() == nullptr);

        // check both handles are no longer valid
        CHECK(user_a->state() == SyncUser::State::Removed);
        CHECK(user_b->state() == SyncUser::State::Removed);
    }
}

TEST_CASE("app: delete user with credentials integration", "[sync][app][user][baas]") {
    TestAppSession session;
    auto app = session.app();
    app->remove_user(app->current_user(), [](auto) {});

    SECTION("log in and delete") {
        CHECK(app->all_users().size() == 0);
        CHECK(app->current_user() == nullptr);

        auto credentials = create_user_and_log_in(app);
        auto user = app->current_user();

        CHECK(app->current_user() == user);
        CHECK(user->state() == SyncUser::State::LoggedIn);
        app->delete_user(user, [&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(app->all_users().size() == 0);
        });
        CHECK(user->state() == SyncUser::State::Removed);
        CHECK(app->current_user() == nullptr);

        app->log_in_with_credentials(credentials, [](std::shared_ptr<SyncUser> user, util::Optional<AppError> error) {
            CHECK(!user);
            REQUIRE(error);
            REQUIRE(error->code() == ErrorCodes::InvalidPassword);
        });
        CHECK(app->current_user() == nullptr);

        CHECK(app->all_users().size() == 0);
        app->delete_user(user, [](Optional<app::AppError> err) {
            CHECK(err->code() > 0);
        });

        CHECK(app->current_user() == nullptr);
        CHECK(app->all_users().size() == 0);
        CHECK(user->state() == SyncUser::State::Removed);
    }
}

// MARK: - Call Function Tests

TEST_CASE("app: call function", "[sync][app][function][baas]") {
    TestAppSession session;
    auto app = session.app();

    bson::BsonArray toSum(5);
    std::iota(toSum.begin(), toSum.end(), static_cast<int64_t>(1));
    const auto checkFn = [](Optional<int64_t>&& sum, Optional<AppError>&& error) {
        REQUIRE(!error);
        CHECK(*sum == 15);
    };
    app->call_function<int64_t>("sumFunc", toSum, checkFn);
    app->call_function<int64_t>(app->current_user(), "sumFunc", toSum, checkFn);
}

// MARK: - Remote Mongo Client Tests

TEST_CASE("app: remote mongo client", "[sync][app][mongo][baas]") {
    TestAppSession session;
    auto app = session.app();

    auto remote_client = app->current_user()->mongo_client("BackingDB");
    auto app_session = get_runtime_app_session();
    auto db = remote_client.db(app_session.config.mongo_dbname);
    auto dog_collection = db["Dog"];
    auto cat_collection = db["Cat"];
    auto person_collection = db["Person"];

    bson::BsonDocument dog_document{{"name", "fido"}, {"breed", "king charles"}};

    bson::BsonDocument dog_document2{{"name", "bob"}, {"breed", "french bulldog"}};

    auto dog3_object_id = ObjectId::gen();
    bson::BsonDocument dog_document3{
        {"_id", dog3_object_id},
        {"name", "petunia"},
        {"breed", "french bulldog"},
    };

    auto cat_id_string = random_string(10);
    bson::BsonDocument cat_document{
        {"_id", cat_id_string},
        {"name", "luna"},
        {"breed", "scottish fold"},
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

    dog_collection.delete_many(dog_document, [&](uint64_t, Optional<AppError> error) {
        REQUIRE_FALSE(error);
    });

    dog_collection.delete_many(dog_document2, [&](uint64_t, Optional<AppError> error) {
        REQUIRE_FALSE(error);
    });

    dog_collection.delete_many({}, [&](uint64_t, Optional<AppError> error) {
        REQUIRE_FALSE(error);
    });

    dog_collection.delete_many(person_document, [&](uint64_t, Optional<AppError> error) {
        REQUIRE_FALSE(error);
    });

    dog_collection.delete_many(person_document2, [&](uint64_t, Optional<AppError> error) {
        REQUIRE_FALSE(error);
    });

    SECTION("insert") {
        bool processed = false;
        ObjectId dog_object_id;
        ObjectId dog2_object_id;

        dog_collection.insert_one_bson(bad_document, [&](Optional<bson::Bson> bson, Optional<AppError> error) {
            CHECK(error);
            CHECK(!bson);
        });

        dog_collection.insert_one_bson(dog_document3, [&](Optional<bson::Bson> value, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            auto bson = static_cast<bson::BsonDocument>(*value);
            CHECK(static_cast<ObjectId>(bson["insertedId"]) == dog3_object_id);
        });

        cat_collection.insert_one_bson(cat_document, [&](Optional<bson::Bson> value, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            auto bson = static_cast<bson::BsonDocument>(*value);
            CHECK(static_cast<std::string>(bson["insertedId"]) == cat_id_string);
        });

        dog_collection.delete_many({}, [&](uint64_t, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        cat_collection.delete_one(cat_document, [&](uint64_t, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        dog_collection.insert_one(bad_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            CHECK(error);
            CHECK(!object_id);
        });

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            dog_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.insert_one(dog_document2, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            dog2_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.insert_one(dog_document3, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(object_id->type() == bson::Bson::Type::ObjectId);
            CHECK(static_cast<ObjectId>(*object_id) == dog3_object_id);
        });

        cat_collection.insert_one(cat_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(object_id->type() == bson::Bson::Type::String);
            CHECK(static_cast<std::string>(*object_id) == cat_id_string);
        });

        person_document["dogs"] = bson::BsonArray({dog_object_id, dog2_object_id, dog3_object_id});
        person_collection.insert_one(person_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
        });

        dog_collection.delete_many({}, [&](uint64_t, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        cat_collection.delete_one(cat_document, [&](uint64_t, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        bson::BsonArray documents{
            dog_document,
            dog_document2,
            dog_document3,
        };

        dog_collection.insert_many_bson(documents, [&](Optional<bson::Bson> value, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            auto bson = static_cast<bson::BsonDocument>(*value);
            auto insertedIds = static_cast<bson::BsonArray>(bson["insertedIds"]);
        });

        dog_collection.delete_many({}, [&](uint64_t, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        dog_collection.insert_many(documents, [&](std::vector<bson::Bson> inserted_docs, Optional<AppError> error) {
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

        dog_collection.find(dog_document, [&](Optional<bson::BsonArray> document_array, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*document_array).size() == 0);
        });

        dog_collection.find_bson(dog_document, {}, [&](Optional<bson::Bson> bson, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(static_cast<bson::BsonArray>(*bson).size() == 0);
        });

        dog_collection.find_one(dog_document, [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(!document);
        });

        dog_collection.find_one_bson(dog_document, {}, [&](Optional<bson::Bson> bson, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((!bson || bson::holds_alternative<util::None>(*bson)));
        });

        ObjectId dog_object_id;
        ObjectId dog2_object_id;

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            dog_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.insert_one(dog_document2, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            dog2_object_id = static_cast<ObjectId>(*object_id);
        });

        person_document["dogs"] = bson::BsonArray({dog_object_id, dog2_object_id});
        person_collection.insert_one(person_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
        });

        dog_collection.find(dog_document, [&](Optional<bson::BsonArray> documents, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*documents).size() == 1);
        });

        dog_collection.find_bson(dog_document, {}, [&](Optional<bson::Bson> bson, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(static_cast<bson::BsonArray>(*bson).size() == 1);
        });

        person_collection.find(person_document, [&](Optional<bson::BsonArray> documents, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*documents).size() == 1);
        });

        MongoCollection::FindOptions options{
            2,                                                         // document limit
            Optional<bson::BsonDocument>({{"name", 1}, {"breed", 1}}), // project
            Optional<bson::BsonDocument>({{"breed", 1}})               // sort
        };

        dog_collection.find(dog_document, options,
                            [&](Optional<bson::BsonArray> document_array, Optional<AppError> error) {
                                REQUIRE_FALSE(error);
                                CHECK((*document_array).size() == 1);
                            });

        dog_collection.find({{"name", "fido"}}, options,
                            [&](Optional<bson::BsonArray> document_array, Optional<AppError> error) {
                                REQUIRE_FALSE(error);
                                CHECK((*document_array).size() == 1);
                                auto king_charles = static_cast<bson::BsonDocument>((*document_array)[0]);
                                CHECK(king_charles["breed"] == "king charles");
                            });

        dog_collection.find_one(dog_document, [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            auto name = (*document)["name"];
            CHECK(name == "fido");
        });

        dog_collection.find_one(dog_document, options,
                                [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                    REQUIRE_FALSE(error);
                                    auto name = (*document)["name"];
                                    CHECK(name == "fido");
                                });

        dog_collection.find_one_bson(dog_document, options, [&](Optional<bson::Bson> bson, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            auto name = (static_cast<bson::BsonDocument>(*bson))["name"];
            CHECK(name == "fido");
        });

        dog_collection.find(dog_document, [&](Optional<bson::BsonArray> documents, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*documents).size() == 1);
        });

        dog_collection.find_one_and_delete(dog_document,
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE_FALSE(error);
                                               REQUIRE(document);
                                           });

        dog_collection.find_one_and_delete({{}},
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE_FALSE(error);
                                               REQUIRE(document);
                                           });

        dog_collection.find_one_and_delete({{"invalid", "key"}},
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE_FALSE(error);
                                               CHECK(!document);
                                           });

        dog_collection.find_one_and_delete_bson({{"invalid", "key"}}, {},
                                                [&](Optional<bson::Bson> bson, Optional<AppError> error) {
                                                    REQUIRE_FALSE(error);
                                                    CHECK((!bson || bson::holds_alternative<util::None>(*bson)));
                                                });

        dog_collection.find(dog_document, [&](Optional<bson::BsonArray> documents, Optional<AppError> error) {
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

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
        });

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            dog_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.insert_one(dog_document2, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            dog2_object_id = static_cast<ObjectId>(*object_id);
        });

        person_document["dogs"] = bson::BsonArray({dog_object_id, dog2_object_id});
        person_collection.insert_one(person_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
        });

        bson::BsonDocument match{{"$match", bson::BsonDocument({{"name", "fido"}})}};

        bson::BsonDocument group{{"$group", bson::BsonDocument({{"_id", "$name"}})}};

        bson::BsonArray pipeline{match, group};

        dog_collection.aggregate(pipeline, [&](Optional<bson::BsonArray> documents, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*documents).size() == 1);
        });

        dog_collection.aggregate_bson(pipeline, [&](Optional<bson::Bson> bson, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(static_cast<bson::BsonArray>(*bson).size() == 1);
        });

        dog_collection.count({{"breed", "king charles"}}, [&](uint64_t count, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(count == 2);
        });

        dog_collection.count_bson({{"breed", "king charles"}}, 0,
                                  [&](Optional<bson::Bson> bson, Optional<AppError> error) {
                                      REQUIRE_FALSE(error);
                                      CHECK(static_cast<int64_t>(*bson) == 2);
                                  });

        dog_collection.count({{"breed", "french bulldog"}}, [&](uint64_t count, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(count == 1);
        });

        dog_collection.count({{"breed", "king charles"}}, 1, [&](uint64_t count, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(count == 1);
        });

        person_collection.count(
            {{"firstName", "John"}, {"lastName", "Johnson"}, {"age", bson::BsonDocument({{"$gt", 25}})}}, 1,
            [&](uint64_t count, Optional<AppError> error) {
                REQUIRE_FALSE(error);
                CHECK(count == 1);
                processed = true;
            });

        CHECK(processed);
    }

    SECTION("find and update") {
        bool processed = false;

        MongoCollection::FindOneAndModifyOptions find_and_modify_options{
            Optional<bson::BsonDocument>({{"name", 1}, {"breed", 1}}), // project
            Optional<bson::BsonDocument>({{"name", 1}}),               // sort,
            true,                                                      // upsert
            true                                                       // return new doc
        };

        dog_collection.find_one_and_update(dog_document, dog_document2,
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE_FALSE(error);
                                               CHECK(!document);
                                           });

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
        });

        dog_collection.find_one_and_update(dog_document, dog_document2, find_and_modify_options,
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE_FALSE(error);
                                               auto breed = static_cast<std::string>((*document)["breed"]);
                                               CHECK(breed == "french bulldog");
                                           });

        dog_collection.find_one_and_update(dog_document2, dog_document, find_and_modify_options,
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE_FALSE(error);
                                               auto breed = static_cast<std::string>((*document)["breed"]);
                                               CHECK(breed == "king charles");
                                           });

        dog_collection.find_one_and_update_bson(dog_document, dog_document2, find_and_modify_options,
                                                [&](Optional<bson::Bson> bson, Optional<AppError> error) {
                                                    REQUIRE_FALSE(error);
                                                    auto breed = static_cast<std::string>(
                                                        static_cast<bson::BsonDocument>(*bson)["breed"]);
                                                    CHECK(breed == "french bulldog");
                                                });

        dog_collection.find_one_and_update_bson(dog_document2, dog_document, find_and_modify_options,
                                                [&](Optional<bson::Bson> bson, Optional<AppError> error) {
                                                    REQUIRE_FALSE(error);
                                                    auto breed = static_cast<std::string>(
                                                        static_cast<bson::BsonDocument>(*bson)["breed"]);
                                                    CHECK(breed == "king charles");
                                                });

        dog_collection.find_one_and_update({{"name", "invalid name"}}, {{"name", "some name"}},
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE_FALSE(error);
                                               CHECK(!document);
                                               processed = true;
                                           });
        CHECK(processed);
        processed = false;

        dog_collection.find_one_and_update({{"name", "invalid name"}}, {{}}, find_and_modify_options,
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE(error);
                                               CHECK(error->reason() == "insert not permitted");
                                               CHECK(!document);
                                               processed = true;
                                           });
        CHECK(processed);
    }

    SECTION("update") {
        bool processed = false;
        ObjectId dog_object_id;

        dog_collection.update_one(dog_document, dog_document2, true,
                                  [&](MongoCollection::UpdateResult result, Optional<AppError> error) {
                                      REQUIRE_FALSE(error);
                                      CHECK((*result.upserted_id).to_string() != "");
                                  });

        dog_collection.update_one(dog_document2, dog_document,
                                  [&](MongoCollection::UpdateResult result, Optional<AppError> error) {
                                      REQUIRE_FALSE(error);
                                      CHECK(!result.upserted_id);
                                  });

        cat_collection.update_one({}, cat_document, true,
                                  [&](MongoCollection::UpdateResult result, Optional<AppError> error) {
                                      REQUIRE_FALSE(error);
                                      CHECK(result.upserted_id->type() == bson::Bson::Type::String);
                                      CHECK(result.upserted_id == cat_id_string);
                                  });

        dog_collection.delete_many({}, [&](uint64_t, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        cat_collection.delete_many({}, [&](uint64_t, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        dog_collection.update_one_bson(dog_document, dog_document2, true,
                                       [&](Optional<bson::Bson> bson, Optional<AppError> error) {
                                           REQUIRE_FALSE(error);
                                           auto upserted_id = static_cast<bson::BsonDocument>(*bson)["upsertedId"];

                                           REQUIRE(upserted_id.type() == bson::Bson::Type::ObjectId);
                                       });

        dog_collection.update_one_bson(dog_document2, dog_document, true,
                                       [&](Optional<bson::Bson> bson, Optional<AppError> error) {
                                           REQUIRE_FALSE(error);
                                           auto document = static_cast<bson::BsonDocument>(*bson);
                                           auto foundUpsertedId = document.find("upsertedId") != document.end();
                                           REQUIRE(!foundUpsertedId);
                                       });

        cat_collection.update_one_bson({}, cat_document, true,
                                       [&](Optional<bson::Bson> bson, Optional<AppError> error) {
                                           REQUIRE_FALSE(error);
                                           auto upserted_id = static_cast<bson::BsonDocument>(*bson)["upsertedId"];
                                           REQUIRE(upserted_id.type() == bson::Bson::Type::String);
                                           REQUIRE(upserted_id == cat_id_string);
                                       });

        person_document["dogs"] = bson::BsonArray();
        bson::BsonDocument person_document_copy = bson::BsonDocument(person_document);
        person_document_copy["dogs"] = bson::BsonArray({dog_object_id});
        person_collection.update_one(person_document, person_document, true,
                                     [&](MongoCollection::UpdateResult, Optional<AppError> error) {
                                         REQUIRE_FALSE(error);
                                         processed = true;
                                     });

        CHECK(processed);
    }

    SECTION("update many") {
        bool processed = false;

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
        });

        dog_collection.update_many(dog_document2, dog_document, true,
                                   [&](MongoCollection::UpdateResult result, Optional<AppError> error) {
                                       REQUIRE_FALSE(error);
                                       CHECK((*result.upserted_id).to_string() != "");
                                   });

        dog_collection.update_many(dog_document2, dog_document,
                                   [&](MongoCollection::UpdateResult result, Optional<AppError> error) {
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

        MongoCollection::FindOneAndModifyOptions find_and_modify_options{
            Optional<bson::BsonDocument>({{"name", "fido"}}), // project
            Optional<bson::BsonDocument>({{"name", 1}}),      // sort,
            true,                                             // upsert
            true                                              // return new doc
        };

        dog_collection.find_one_and_replace(dog_document, dog_document2,
                                            [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                                REQUIRE_FALSE(error);
                                                CHECK(!document);
                                            });

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            dog_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.find_one_and_replace(dog_document, dog_document2,
                                            [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                                REQUIRE_FALSE(error);
                                                auto name = static_cast<std::string>((*document)["name"]);
                                                CHECK(name == "fido");
                                            });

        dog_collection.find_one_and_replace(dog_document2, dog_document, find_and_modify_options,
                                            [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                                REQUIRE_FALSE(error);
                                                auto name = static_cast<std::string>((*document)["name"]);
                                                CHECK(static_cast<std::string>(name) == "fido");
                                            });

        person_document["dogs"] = bson::BsonArray({dog_object_id});
        person_document2["dogs"] = bson::BsonArray({dog_object_id});
        person_collection.insert_one(person_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK((*object_id).to_string() != "");
            person_object_id = static_cast<ObjectId>(*object_id);
        });

        MongoCollection::FindOneAndModifyOptions person_find_and_modify_options{
            Optional<bson::BsonDocument>({{"firstName", 1}}), // project
            Optional<bson::BsonDocument>({{"firstName", 1}}), // sort,
            false,                                            // upsert
            true                                              // return new doc
        };

        person_collection.find_one_and_replace(person_document, person_document2,
                                               [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                                   REQUIRE_FALSE(error);
                                                   auto name = static_cast<std::string>((*document)["firstName"]);
                                                   // Should return the old document
                                                   CHECK(name == "John");
                                                   processed = true;
                                               });

        person_collection.find_one_and_replace(person_document2, person_document, person_find_and_modify_options,
                                               [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                                   REQUIRE_FALSE(error);
                                                   auto name = static_cast<std::string>((*document)["firstName"]);
                                                   // Should return new document, Bob -> John
                                                   CHECK(name == "John");
                                               });

        person_collection.find_one_and_replace({{"invalid", "item"}}, {{}},
                                               [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                                   // If a document is not found then null will be returned for the
                                                   // document and no error will be returned
                                                   REQUIRE_FALSE(error);
                                                   CHECK(!document);
                                               });

        person_collection.find_one_and_replace({{"invalid", "item"}}, {{}}, person_find_and_modify_options,
                                               [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
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

        dog_collection.insert_many(documents, [&](std::vector<bson::Bson> inserted_docs, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(inserted_docs.size() == 3);
        });

        MongoCollection::FindOneAndModifyOptions find_and_modify_options{
            Optional<bson::BsonDocument>({{"name", "fido"}}), // project
            Optional<bson::BsonDocument>({{"name", 1}}),      // sort,
            true,                                             // upsert
            true                                              // return new doc
        };

        dog_collection.delete_one(dog_document, [&](uint64_t deleted_count, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(deleted_count >= 1);
        });

        dog_collection.delete_many(dog_document, [&](uint64_t deleted_count, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(deleted_count >= 1);
            processed = true;
        });

        person_collection.delete_many_bson(person_document, [&](Optional<bson::Bson> bson, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(static_cast<int32_t>(static_cast<bson::BsonDocument>(*bson)["deletedCount"]) >= 1);
            processed = true;
        });

        CHECK(processed);
    }
}

// MARK: - Push Notifications Tests

TEST_CASE("app: push notifications", "[sync][app][notifications][baas]") {
    TestAppSession session;
    auto app = session.app();
    std::shared_ptr<SyncUser> sync_user = app->current_user();

    SECTION("register") {
        bool processed;

        app->push_notification_client("gcm").register_device("hello", sync_user, [&](Optional<AppError> error) {
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
                                                                 [&](Optional<AppError> error) {
                REQUIRE_FALSE(error);
            });

            app->push_notification_client("gcm").register_device("hello",
                                                                 sync_user,
                                                                 [&](Optional<AppError> error) {
                REQUIRE_FALSE(error);
                processed = true;
            });

            CHECK(processed);
        }
    */
    SECTION("deregister") {
        bool processed;

        app->push_notification_client("gcm").deregister_device(sync_user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("register with unavailable service") {
        bool processed;

        app->push_notification_client("gcm_blah").register_device("hello", sync_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->reason() == "service not found: 'gcm_blah'");
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("register with logged out user") {
        bool processed;

        app->log_out([=](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        app->push_notification_client("gcm").register_device("hello", sync_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            processed = true;
        });

        app->push_notification_client("gcm").register_device("hello", nullptr, [&](Optional<AppError> error) {
            REQUIRE(error);
            processed = true;
        });

        CHECK(processed);
    }
}

// MARK: - Token refresh

TEST_CASE("app: token refresh", "[sync][app][token][baas]") {
    TestAppSession session;
    auto app = session.app();
    std::shared_ptr<SyncUser> sync_user = app->current_user();
    sync_user->update_access_token(ENCODE_FAKE_JWT("fake_access_token"));

    auto remote_client = app->current_user()->mongo_client("BackingDB");
    auto app_session = get_runtime_app_session();
    auto db = remote_client.db(app_session.config.mongo_dbname);
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
        dog_collection.find_one(dog_document, [&](Optional<bson::BsonDocument>, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });
    }
}

TEST_CASE("app: custom user data integration tests", "[sync][app][user][function][baas]") {
    TestAppSession session;
    auto app = session.app();
    auto user = app->current_user();

    SECTION("custom user data happy path") {
        bool processed = false;
        app->call_function("updateUserData", {bson::BsonDocument({{"favorite_color", "green"}})},
                           [&](auto response, auto error) {
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

TEST_CASE("app: jwt login and metadata tests", "[sync][app][user][metadata][function][baas]") {
    TestAppSession session;
    auto app = session.app();
    auto jwt = create_jwt(session.app()->config().app_id);

    SECTION("jwt happy path") {
        bool processed = false;

        std::shared_ptr<SyncUser> user = log_in(app, AppCredentials::custom(jwt));

        app->call_function(user, "updateUserData", {bson::BsonDocument({{"name", "Not Foo Bar"}})},
                           [&](auto response, auto error) {
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
        auto metadata = user->user_profile();
        auto custom_data = *user->custom_data();
        CHECK(custom_data["name"] == "Not Foo Bar");
        CHECK(metadata["name"] == "Foo Bar");
    }
}


#endif // REALM_ENABLE_AUTH_TESTS

static OfflineAppSession::Config
offline_unit_test_config(std::shared_ptr<app::GenericNetworkTransport> transport = instance_of<UnitTestTransport>)
{
    return OfflineAppSession::Config(transport);
}

TEST_CASE("app: custom error handling", "[sync][app][custom errors]") {
    class CustomErrorTransport : public GenericNetworkTransport {
    public:
        CustomErrorTransport(int code, const std::string& message)
            : m_code(code)
            , m_message(message)
        {
        }

        void send_request_to_server(const Request&, util::UniqueFunction<void(const Response&)>&& completion) override
        {
            completion(Response{0, m_code, HttpHeaders(), m_message});
        }

    private:
        int m_code;
        std::string m_message;
    };

    SECTION("custom code and message is sent back") {
        OfflineAppSession session(offline_unit_test_config(std::make_shared<CustomErrorTransport>(1001, "Boom!")));
        auto error = failed_log_in(session.app());
        CHECK(error.is_custom_error());
        CHECK(*error.additional_status_code == 1001);
        CHECK(error.reason() == "Boom!");
    }
}

// MARK: - Unit Tests

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

TEST_CASE("app: login_with_credentials unit_tests", "[sync][app][user]") {
    auto config = offline_unit_test_config();
    static_cast<UnitTestTransport*>(config.transport.get())->set_profile(profile_0);

    SECTION("login_anonymous good") {
        std::string shared_storage_path = util::make_temp_dir();
        UnitTestTransport::access_token = good_access_token;
        config.storage_path = shared_storage_path;
        config.delete_storage = false;
        config.metadata_mode = SyncManager::MetadataMode::NoEncryption;
        {
            OfflineAppSession tas(std::move(config));
            auto app = tas.app();
            auto user = log_in(app);

            REQUIRE(user->identities().size() == 1);
            CHECK(user->identities()[0].id == UnitTestTransport::identity_0_id);
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
            auto config2 = offline_unit_test_config();
            config2.storage_path = shared_storage_path;
            config2.delete_storage = true;
            config2.metadata_mode = SyncManager::MetadataMode::NoEncryption;
            OfflineAppSession tas(std::move(config2));

            auto app = tas.app();
            REQUIRE(app->all_users().size() == 1);
            auto user = app->all_users()[0];
            REQUIRE(user->identities().size() == 1);
            CHECK(user->identities()[0].id == UnitTestTransport::identity_0_id);
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
            void send_request_to_server(const Request& request,
                                        util::UniqueFunction<void(const Response&)>&& completion) override
            {
                if (request.url.find("/login") != std::string::npos) {
                    completion({200, 0, {}, user_json(bad_access_token).dump()});
                }
                else {
                    UnitTestTransport::send_request_to_server(request, std::move(completion));
                }
            }
        };

        config.transport = instance_of<transport>;
        OfflineAppSession tas(std::move(config));
        auto error = failed_log_in(tas.app());
        CHECK(error.reason() == std::string("jwt missing parts"));
        CHECK(error.code_string() == "BadToken");
        CHECK(error.is_json_error());
        CHECK(error.code() == ErrorCodes::BadToken);
    }

    SECTION("login_anonynous multiple users") {
        UnitTestTransport::access_token = good_access_token;
        OfflineAppSession tas(std::move(config));
        auto app = tas.app();

        auto user1 = log_in(app);
        auto user2 = log_in(app, AppCredentials::anonymous(false));
        CHECK(user1 != user2);
    }
}

TEST_CASE("app: UserAPIKeyProviderClient unit_tests", "[sync][app][user][api key]") {
    OfflineAppSession tas(offline_unit_test_config());
    auto app = tas.app();
    auto client = app->provider_client<App::UserAPIKeyProviderClient>();

    std::shared_ptr<SyncUser> logged_in_user =
        app->backing_store()->get_user("userid", good_access_token, good_access_token, dummy_device_id);
    bool processed = false;
    ObjectId obj_id(UnitTestTransport::api_key_id.c_str());

    SECTION("create api key") {
        client.create_api_key(UnitTestTransport::api_key_name, logged_in_user,
                              [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
                                  REQUIRE_FALSE(error);
                                  CHECK(user_api_key.disabled == false);
                                  CHECK(user_api_key.id.to_string() == UnitTestTransport::api_key_id);
                                  CHECK(user_api_key.key == UnitTestTransport::api_key);
                                  CHECK(user_api_key.name == UnitTestTransport::api_key_name);
                              });
    }

    SECTION("fetch api key") {
        client.fetch_api_key(obj_id, logged_in_user, [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(user_api_key.disabled == false);
            CHECK(user_api_key.id.to_string() == UnitTestTransport::api_key_id);
            CHECK(user_api_key.name == UnitTestTransport::api_key_name);
        });
    }

    SECTION("fetch api keys") {
        client.fetch_api_keys(logged_in_user,
                              [&](std::vector<App::UserAPIKey> user_api_keys, Optional<AppError> error) {
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


TEST_CASE("app: user_semantics", "[sync][app][user]") {
    OfflineAppSession tas(offline_unit_test_config());
    auto app = tas.app();

    const auto login_user_email_pass = [=] {
        return log_in(app, AppCredentials::username_password("bob", "thompson"));
    };
    const auto login_user_anonymous = [=] {
        return log_in(app, AppCredentials::anonymous());
    };

    CHECK(!app->current_user());

    int event_processed = 0;
    auto token = app->subscribe([&event_processed](auto&) {
        event_processed++;
    });

    SECTION("current user is populated") {
        const auto user1 = login_user_anonymous();
        CHECK(app->current_user()->user_id() == user1->user_id());
        CHECK(event_processed == 1);
    }

    SECTION("current user is updated on login") {
        const auto user1 = login_user_anonymous();
        CHECK(app->current_user()->user_id() == user1->user_id());
        const auto user2 = login_user_email_pass();
        CHECK(app->current_user()->user_id() == user2->user_id());
        CHECK(user1->user_id() != user2->user_id());
        CHECK(event_processed == 2);
    }

    SECTION("current user is updated to last used user on logout") {
        const auto user1 = login_user_anonymous();
        CHECK(app->current_user()->user_id() == user1->user_id());
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);

        const auto user2 = login_user_email_pass();
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);
        CHECK(app->all_users()[1]->state() == SyncUser::State::LoggedIn);
        CHECK(app->current_user()->user_id() == user2->user_id());
        CHECK(user1 != user2);

        // should reuse existing session
        const auto user3 = login_user_anonymous();
        CHECK(user3 == user1);

        auto user_events_processed = 0;
        auto _ = user3->subscribe([&user_events_processed](auto&) {
            user_events_processed++;
        });

        app->log_out([](auto) {});
        CHECK(user_events_processed == 1);

        CHECK(app->current_user()->user_id() == user2->user_id());

        CHECK(app->all_users().size() == 1);
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);

        CHECK(event_processed == 4);
    }

    SECTION("anon users are removed on logout") {
        const auto user1 = login_user_anonymous();
        CHECK(app->current_user()->user_id() == user1->user_id());
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);

        const auto user2 = login_user_anonymous();
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);
        CHECK(app->all_users().size() == 1);
        CHECK(app->current_user()->user_id() == user2->user_id());
        CHECK(user1->user_id() == user2->user_id());

        app->log_out([](auto) {});
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
        CHECK(app->current_user()->user_id() == user1->user_id());
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);

        const auto user2 = login_user_anonymous();
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);
        CHECK(app->all_users().size() == 1);
        CHECK(app->current_user()->user_id() == user2->user_id());
        CHECK(user1->user_id() == user2->user_id());

        app->log_out([](auto) {});
        CHECK(app->all_users().size() == 0);

        CHECK(event_processed == 0);
    }
}

struct ErrorCheckingTransport : public GenericNetworkTransport {
    ErrorCheckingTransport(Response* r)
        : m_response(r)
    {
    }
    void send_request_to_server(const Request&, util::UniqueFunction<void(const Response&)>&& completion) override
    {
        completion(Response(*m_response));
    }

private:
    Response* m_response;
};

TEST_CASE("app: response error handling", "[sync][app]") {
    std::string response_body = nlohmann::json({{"access_token", good_access_token},
                                                {"refresh_token", good_access_token},
                                                {"user_id", "Brown Bear"},
                                                {"device_id", "Panda Bear"}})
                                    .dump();

    Response response{200, 0, {{"Content-Type", "text/plain"}}, response_body};
    OfflineAppSession tas(offline_unit_test_config(std::make_shared<ErrorCheckingTransport>(&response)));

    auto app = tas.app();

    SECTION("http 404") {
        response.http_status_code = 404;
        auto error = failed_log_in(app);
        CHECK(!error.is_json_error());
        CHECK(!error.is_custom_error());
        CHECK(!error.is_service_error());
        CHECK(error.is_http_error());
        CHECK(*error.additional_status_code == 404);
        CHECK(error.reason().find(std::string("http error code considered fatal")) != std::string::npos);
    }
    SECTION("http 500") {
        response.http_status_code = 500;
        auto error = failed_log_in(app);
        CHECK(!error.is_json_error());
        CHECK(!error.is_custom_error());
        CHECK(!error.is_service_error());
        CHECK(error.is_http_error());
        CHECK(*error.additional_status_code == 500);
        CHECK(error.reason().find(std::string("http error code considered fatal")) != std::string::npos);
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
        CHECK(*error.additional_status_code == 42);
        CHECK(error.reason() == std::string("Custom error message"));
        CHECK(error.link_to_server_logs.empty());
    }

    SECTION("session error code") {
        response.headers = HttpHeaders{{"Content-Type", "application/json"}};
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
        CHECK(error.code() == ErrorCodes::MongoDBError);
        CHECK(error.reason() == std::string("a fake MongoDB error message!"));
        CHECK(error.link_to_server_logs == std::string("http://...whatever the server passes us"));
    }

    SECTION("json error code") {
        response.body = "this: is not{} a valid json body!";
        auto error = failed_log_in(app);
        CHECK(!error.is_http_error());
        CHECK(error.is_json_error());
        CHECK(!error.is_custom_error());
        CHECK(!error.is_service_error());
        CHECK(error.code() == ErrorCodes::MalformedJson);
        CHECK(error.reason() ==
              std::string("[json.exception.parse_error.101] parse error at line 1, column 2: syntax error "
                          "while parsing value - invalid literal; last read: 'th'"));
        CHECK(error.code_string() == "MalformedJson");
    }
}

TEST_CASE("app: switch user", "[sync][app][user]") {
    OfflineAppSession tas(offline_unit_test_config());
    auto app = tas.app();

    bool processed = false;

    SECTION("switch user expect success") {
        CHECK(app->all_users().size() == 0);

        // Log in user 1
        auto user_a = log_in(app, AppCredentials::username_password("test@10gen.com", "password"));
        CHECK(app->current_user() == user_a);

        // Log in user 2
        auto user_b = log_in(app, AppCredentials::username_password("test2@10gen.com", "password"));
        CHECK(app->current_user() == user_b);

        CHECK(app->all_users().size() == 2);

        auto user1 = app->switch_user(user_a);
        CHECK(user1 == user_a);

        CHECK(app->current_user() == user_a);

        auto user2 = app->switch_user(user_b);
        CHECK(user2 == user_b);

        CHECK(app->current_user() == user_b);
        processed = true;
        CHECK(processed);
    }

    SECTION("cannot switch to a logged out but not removed user") {
        CHECK(app->all_users().size() == 0);

        // Log in user 1
        auto user_a = log_in(app, AppCredentials::username_password("test@10gen.com", "password"));
        CHECK(app->current_user() == user_a);

        app->log_out([&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        CHECK(app->current_user() == nullptr);
        CHECK(user_a->state() == SyncUser::State::LoggedOut);

        // Log in user 2
        auto user_b = log_in(app, AppCredentials::username_password("test2@10gen.com", "password"));
        CHECK(app->current_user() == user_b);
        CHECK(app->all_users().size() == 2);

        REQUIRE_THROWS_AS(app->switch_user(user_a), AppError);
        CHECK(app->current_user() == user_b);
    }
}

TEST_CASE("app: remove anonymous user", "[sync][app][user]") {
    OfflineAppSession tas(offline_unit_test_config());
    auto app = tas.app();

    SECTION("remove user expect success") {
        CHECK(app->all_users().size() == 0);

        // Log in user 1
        auto user_a = log_in(app);
        CHECK(user_a->state() == SyncUser::State::LoggedIn);

        app->log_out(user_a, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
            // a logged out anon user will be marked as Removed, not LoggedOut
            CHECK(user_a->state() == SyncUser::State::Removed);
        });
        CHECK(app->all_users().empty());

        app->remove_user(user_a, [&](Optional<AppError> error) {
            CHECK(error->reason() == "User has already been removed");
            CHECK(app->all_users().size() == 0);
        });

        // Log in user 2
        auto user_b = log_in(app);
        CHECK(app->current_user() == user_b);
        CHECK(user_b->state() == SyncUser::State::LoggedIn);
        CHECK(app->all_users().size() == 1);

        app->remove_user(user_b, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(app->all_users().size() == 0);
        });

        CHECK(app->current_user() == nullptr);

        // check both handles are no longer valid
        CHECK(user_a->state() == SyncUser::State::Removed);
        CHECK(user_b->state() == SyncUser::State::Removed);
    }
}

TEST_CASE("app: remove user with credentials", "[sync][app][user]") {
    OfflineAppSession tas(offline_unit_test_config());
    auto app = tas.app();

    SECTION("log in, log out and remove") {
        CHECK(app->all_users().size() == 0);
        CHECK(app->current_user() == nullptr);

        auto user = log_in(app, AppCredentials::username_password("email", "pass"));

        CHECK(user->state() == SyncUser::State::LoggedIn);

        app->log_out(user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        CHECK(user->state() == SyncUser::State::LoggedOut);

        app->remove_user(user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });
        CHECK(app->all_users().size() == 0);

        Optional<AppError> error;
        app->remove_user(user, [&](Optional<AppError> err) {
            error = err;
        });
        CHECK(error->code() > 0);
        CHECK(app->all_users().size() == 0);
        CHECK(user->state() == SyncUser::State::Removed);
    }
}

TEST_CASE("app: link_user", "[sync][app][user]") {
    OfflineAppSession tas(offline_unit_test_config());
    auto app = tas.app();

    auto email = util::format("realm_tests_do_autoverify%1@%2.com", random_string(10), random_string(10));
    auto password = random_string(10);

    auto custom_credentials = AppCredentials::facebook("a_token");
    auto email_pass_credentials = AppCredentials::username_password(email, password);

    auto sync_user = log_in(app, email_pass_credentials);
    REQUIRE(sync_user->identities().size() == 2);
    CHECK(sync_user->identities()[0].provider_type == IdentityProviderUsernamePassword);

    SECTION("successful link") {
        bool processed = false;
        app->link_user(sync_user, custom_credentials, [&](std::shared_ptr<SyncUser> user, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(user);
            CHECK(user->user_id() == sync_user->user_id());
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("link_user should fail when logged out") {
        app->log_out([&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        bool processed = false;
        app->link_user(sync_user, custom_credentials, [&](std::shared_ptr<SyncUser> user, Optional<AppError> error) {
            CHECK(error->reason() == "The specified user is not logged in.");
            CHECK(!user);
            processed = true;
        });
        CHECK(processed);
    }
}

TEST_CASE("app: auth providers", "[sync][app][user]") {
    SECTION("auth providers facebook") {
        auto credentials = AppCredentials::facebook("a_token");
        CHECK(credentials.provider() == AuthProvider::FACEBOOK);
        CHECK(credentials.provider_as_string() == IdentityProviderFacebook);
        CHECK(credentials.serialize_as_bson() ==
              bson::BsonDocument{{"provider", "oauth2-facebook"}, {"accessToken", "a_token"}});
    }

    SECTION("auth providers anonymous") {
        auto credentials = AppCredentials::anonymous();
        CHECK(credentials.provider() == AuthProvider::ANONYMOUS);
        CHECK(credentials.provider_as_string() == IdentityProviderAnonymous);
        CHECK(credentials.serialize_as_bson() == bson::BsonDocument{{"provider", "anon-user"}});
    }

    SECTION("auth providers anonymous no reuse") {
        auto credentials = AppCredentials::anonymous(false);
        CHECK(credentials.provider() == AuthProvider::ANONYMOUS_NO_REUSE);
        CHECK(credentials.provider_as_string() == IdentityProviderAnonymous);
        CHECK(credentials.serialize_as_bson() == bson::BsonDocument{{"provider", "anon-user"}});
    }

    SECTION("auth providers google authCode") {
        auto credentials = AppCredentials::google(AuthCode("a_token"));
        CHECK(credentials.provider() == AuthProvider::GOOGLE);
        CHECK(credentials.provider_as_string() == IdentityProviderGoogle);
        CHECK(credentials.serialize_as_bson() ==
              bson::BsonDocument{{"provider", "oauth2-google"}, {"authCode", "a_token"}});
    }

    SECTION("auth providers google idToken") {
        auto credentials = AppCredentials::google(IdToken("a_token"));
        CHECK(credentials.provider() == AuthProvider::GOOGLE);
        CHECK(credentials.provider_as_string() == IdentityProviderGoogle);
        CHECK(credentials.serialize_as_bson() ==
              bson::BsonDocument{{"provider", "oauth2-google"}, {"id_token", "a_token"}});
    }

    SECTION("auth providers apple") {
        auto credentials = AppCredentials::apple("a_token");
        CHECK(credentials.provider() == AuthProvider::APPLE);
        CHECK(credentials.provider_as_string() == IdentityProviderApple);
        CHECK(credentials.serialize_as_bson() ==
              bson::BsonDocument{{"provider", "oauth2-apple"}, {"id_token", "a_token"}});
    }

    SECTION("auth providers custom") {
        auto credentials = AppCredentials::custom("a_token");
        CHECK(credentials.provider() == AuthProvider::CUSTOM);
        CHECK(credentials.provider_as_string() == IdentityProviderCustom);
        CHECK(credentials.serialize_as_bson() ==
              bson::BsonDocument{{"provider", "custom-token"}, {"token", "a_token"}});
    }

    SECTION("auth providers username password") {
        auto credentials = AppCredentials::username_password("user", "pass");
        CHECK(credentials.provider() == AuthProvider::USERNAME_PASSWORD);
        CHECK(credentials.provider_as_string() == IdentityProviderUsernamePassword);
        CHECK(credentials.serialize_as_bson() ==
              bson::BsonDocument{{"provider", "local-userpass"}, {"username", "user"}, {"password", "pass"}});
    }

    SECTION("auth providers function") {
        bson::BsonDocument function_params{{"name", "mongo"}};
        auto credentials = AppCredentials::function(function_params);
        CHECK(credentials.provider() == AuthProvider::FUNCTION);
        CHECK(credentials.provider_as_string() == IdentityProviderFunction);
        CHECK(credentials.serialize_as_bson() == bson::BsonDocument{{"name", "mongo"}});
    }

    SECTION("auth providers api key") {
        auto credentials = AppCredentials::api_key("a key");
        CHECK(credentials.provider() == AuthProvider::API_KEY);
        CHECK(credentials.provider_as_string() == IdentityProviderAPIKey);
        CHECK(credentials.serialize_as_bson() == bson::BsonDocument{{"provider", "api-key"}, {"key", "a key"}});
        CHECK(enum_from_provider_type(provider_type_from_enum(AuthProvider::API_KEY)) == AuthProvider::API_KEY);
    }
}

TEST_CASE("app: refresh access token unit tests", "[sync][app][user][token]") {
    auto setup_user = [](std::shared_ptr<App> app) {
        if (app->current_user()) {
            return;
        }
        app->backing_store()->get_user("a_user_id", good_access_token, good_access_token, dummy_device_id);
    };

    SECTION("refresh custom data happy path") {
        static bool session_route_hit = false;

        struct transport : UnitTestTransport {
            void send_request_to_server(const Request& request,
                                        util::UniqueFunction<void(const Response&)>&& completion) override
            {
                if (request.url.find("/session") != std::string::npos) {
                    session_route_hit = true;
                    nlohmann::json json{{"access_token", good_access_token}};
                    completion({200, 0, {}, json.dump()});
                }
                else {
                    UnitTestTransport::send_request_to_server(request, std::move(completion));
                }
            }
        };
        OfflineAppSession tas(offline_unit_test_config(instance_of<transport>));
        auto app = tas.app();
        setup_user(app);

        bool processed = false;
        app->refresh_custom_data(app->current_user(), [&](const Optional<AppError>& error) {
            REQUIRE_FALSE(error);
            CHECK(session_route_hit);
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("refresh custom data sad path") {
        static bool session_route_hit = false;

        struct transport : UnitTestTransport {
            void send_request_to_server(const Request& request,
                                        util::UniqueFunction<void(const Response&)>&& completion) override
            {
                if (request.url.find("/session") != std::string::npos) {
                    session_route_hit = true;
                    nlohmann::json json{{"access_token", bad_access_token}};
                    completion({200, 0, {}, json.dump()});
                }
                else {
                    UnitTestTransport::send_request_to_server(request, std::move(completion));
                }
            }
        };
        OfflineAppSession tas(offline_unit_test_config(instance_of<transport>));
        auto app = tas.app();
        setup_user(app);

        bool processed = false;
        app->refresh_custom_data(app->current_user(), [&](const Optional<AppError>& error) {
            CHECK(error->reason() == "jwt missing parts");
            CHECK(error->code() == ErrorCodes::BadToken);
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

            void send_request_to_server(const Request& request,
                                        util::UniqueFunction<void(const Response&)>&& completion) override
            {
                if (request.url.find("/login") != std::string::npos) {
                    login_hit = true;
                    completion({200, 0, {}, user_json(good_access_token).dump()});
                }
                else if (request.url.find("/profile") != std::string::npos) {
                    CHECK(login_hit);

                    auto item = AppUtils::find_header("Authorization", request.headers);
                    CHECK(item);
                    auto access_token = item->second;
                    // simulated bad token request
                    if (access_token.find(good_access_token2) != std::string::npos) {
                        CHECK(login_hit);
                        CHECK(get_profile_1_hit);
                        CHECK(refresh_hit);

                        get_profile_2_hit = true;

                        completion({200, 0, {}, user_profile_json().dump()});
                    }
                    else if (access_token.find(good_access_token) != std::string::npos) {
                        CHECK(!get_profile_2_hit);
                        get_profile_1_hit = true;

                        completion({401, 0, {}});
                    }
                }
                else if (request.url.find("/session") != std::string::npos && request.method == HttpMethod::post) {
                    CHECK(login_hit);
                    CHECK(get_profile_1_hit);
                    CHECK(!get_profile_2_hit);
                    refresh_hit = true;

                    nlohmann::json json{{"access_token", good_access_token2}};
                    completion({200, 0, {}, json.dump()});
                }
                else if (request.url.find("/location") != std::string::npos) {
                    CHECK(request.method == HttpMethod::get);
                    completion({200,
                                0,
                                {},
                                "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":"
                                "\"http://localhost:9090\",\"ws_hostname\":\"ws://localhost:9090\"}"});
                }
            }
        };
        OfflineAppSession tas(offline_unit_test_config(instance_of<transport>));
        auto app = tas.app();
        setup_user(app);
        REQUIRE(log_in(app));
    }
}

TEST_CASE("app: make_streaming_request", "[sync][app][streaming]") {
    constexpr uint64_t timeout_ms = 60000; // this is the default
    UnitTestTransport::access_token = good_access_token;
    OfflineAppSession tas(offline_unit_test_config());
    auto app = tas.app();

    std::shared_ptr<SyncUser> user = log_in(app);

    using Headers = decltype(Request().headers);

    const auto url_prefix = "field/api/client/v2.0/app/app_id/functions/call?baas_request="sv;
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

TEST_CASE("app: sync_user_profile unit tests", "[sync][app][user]") {
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

TEST_CASE("app: shared instances", "[sync][app]") {
    App::Config base_config;
    set_app_config_defaults(base_config, instance_of<UnitTestTransport>);

    app::RealmBackingStoreConfig bsc;
    bsc.metadata_mode = app::RealmBackingStoreConfig::MetadataMode::NoMetadata;
    bsc.base_file_path = util::make_temp_dir();
    util::try_make_dir(bsc.base_file_path);
    auto cleanup = util::make_scope_exit([&]() noexcept {
        realm::util::try_remove_dir_recursive(bsc.base_file_path);
    });
    size_t stores_created = 0;
    auto factory = [&stores_created, &bsc](SharedApp app) -> std::shared_ptr<RealmBackingStore> {
        ++stores_created;
        return std::make_shared<RealmBackingStore>(app, bsc);
    };

    auto config1 = base_config;
    config1.app_id = "app1";

    auto config2 = base_config;
    config2.app_id = "app1";
    config2.base_url = "https://realm.mongodb.com"; // equivalent to default_base_url

    auto config3 = base_config;
    config3.app_id = "app2";

    auto config4 = base_config;
    config4.app_id = "app2";
    config4.base_url = "http://localhost:9090";

    // should all point to same underlying app
    auto app1_1 = App::get_app(app::App::CacheMode::Enabled, config1, factory);
    auto app1_2 = App::get_app(app::App::CacheMode::Enabled, config1, factory);
    auto app1_3 = App::get_cached_app(config1.app_id, config1.base_url);
    auto app1_4 = App::get_app(app::App::CacheMode::Enabled, config2, factory);
    auto app1_5 = App::get_cached_app(config1.app_id);

    CHECK(app1_1 == app1_2);
    CHECK(app1_1 == app1_3);
    CHECK(app1_1 == app1_4);
    CHECK(app1_1 == app1_5);

    // config3 and config4 should point to different apps
    auto app2_1 = App::get_app(app::App::CacheMode::Enabled, config3, factory);
    auto app2_2 = App::get_cached_app(config3.app_id, config3.base_url);
    auto app2_3 = App::get_app(app::App::CacheMode::Enabled, config4, factory);
    auto app2_4 = App::get_cached_app(config3.app_id);
    auto app2_5 = App::get_cached_app(config4.app_id, "https://some.different.url");

    CHECK(app2_1 == app2_2);
    CHECK(app2_1 != app2_3);
    CHECK(app2_4 != nullptr);
    CHECK(app2_5 == nullptr);

    CHECK(app1_1 != app2_1);
    CHECK(app1_1 != app2_3);
    CHECK(app1_1 != app2_4);
    CHECK(stores_created == 3);
}
