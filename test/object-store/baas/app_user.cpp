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

#include <catch2/catch_all.hpp>
#include <realm/object-store/object.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/app_utils.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/mongo_database.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/sync/network/default_socket.hpp>
#include <realm/sync/noinst/server/access_token.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/optional.hpp>

#include <external/json/json.hpp>
#include <external/mpark/variant.hpp>

#include "util/baas_admin_api.hpp"
#include "util/baas_test_utils.hpp"

#include <mutex>

using namespace realm;
using namespace realm::app;
using util::Optional;


namespace {
std::shared_ptr<SyncUser> log_in(std::shared_ptr<App> app, AppCredentials credentials = AppCredentials::anonymous())
{
    std::shared_ptr<SyncUser> user;
    app->log_in_with_credentials(credentials, [&](std::shared_ptr<SyncUser> user_arg, Optional<AppError> error) {
        REQUIRE_FALSE(error);
        REQUIRE(user_arg);
        user = std::move(user_arg);
    });
    REQUIRE(user);
    return user;
}

AppError failed_log_in(std::shared_ptr<App> app, AppCredentials credentials = AppCredentials::anonymous())
{
    Optional<AppError> err;
    app->log_in_with_credentials(credentials, [&](std::shared_ptr<SyncUser> user, Optional<AppError> error) {
        REQUIRE(error);
        REQUIRE_FALSE(user);
        err = error;
    });
    REQUIRE(err);
    return *err;
}

} // namespace

namespace realm {
class TestHelper {
public:
    static DBRef get_db(Realm& realm)
    {
        return Realm::Internal::get_db(realm);
    }
};
} // namespace realm

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

// MARK: - Login with Credentials Tests

TEST_CASE("app: login_with_credentials integration", "[sync][app][baas][user][new]") {
    SECTION("login") {
        TestAppSession session;
        auto app = session.app();
        app->log_out([](auto) {});

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

TEST_CASE("app: UsernamePasswordProviderClient integration", "[sync][app][baas][user][new]") {
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
        CHECK(app->sync_manager()->all_users().size() == 0);
        CHECK(app->sync_manager()->get_current_user() == nullptr);

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
            CHECK(app->sync_manager()->all_users().size() == 0);
            processed = true;
        });

        CHECK(user->state() == SyncUser::State::Removed);
        CHECK(processed);
        CHECK(app->all_users().size() == 0);
    }
}

// MARK: - UserAPIKeyProviderClient Tests

TEST_CASE("app: UserAPIKeyProviderClient integration", "[sync][app][baas][user][new]") {
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

TEST_CASE("app: auth providers function integration", "[sync][app][baas][user][new]") {
    TestAppSession session;
    auto app = session.app();

    SECTION("auth providers function integration") {
        bson::BsonDocument function_params{{"realmCustomAuthFuncUserId", "123456"}};
        auto credentials = AppCredentials::function(function_params);
        auto user = log_in(app, credentials);
        REQUIRE(user->provider_type() == IdentityProviderFunction);
    }
}

// MARK: - Link User Tests

TEST_CASE("app: link_user integration", "[sync][app][baas][user][new]") {
    TestAppSession session;
    auto app = session.app();

    SECTION("link_user integration") {
        AutoVerifiedEmailCredentials creds;
        bool processed = false;
        std::shared_ptr<SyncUser> sync_user;

        app->provider_client<App::UsernamePasswordProviderClient>().register_email(
            creds.email, creds.password, [&](Optional<AppError> error) {
                CAPTURE(creds.email);
                CAPTURE(creds.password);
                REQUIRE_FALSE(error); // first registration success
            });

        sync_user = log_in(app);
        CHECK(sync_user->provider_type() == IdentityProviderAnonymous);

        app->link_user(sync_user, creds, [&](std::shared_ptr<SyncUser> user, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(user);
            CHECK(user->identity() == sync_user->identity());
            CHECK(user->identities().size() == 2);
            processed = true;
        });

        CHECK(processed);
    }
}

// MARK: - Delete User Tests

TEST_CASE("app: delete anonymous user integration", "[sync][app][baas][user][new]") {
    TestAppSession session;
    auto app = session.app();

    SECTION("delete user expect success") {
        CHECK(app->sync_manager()->all_users().size() == 1);

        // Log in user 1
        auto user_a = app->current_user();
        CHECK(user_a->state() == SyncUser::State::LoggedIn);
        app->delete_user(user_a, [&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            // a logged out anon user will be marked as Removed, not LoggedOut
            CHECK(user_a->state() == SyncUser::State::Removed);
        });
        CHECK(app->sync_manager()->all_users().empty());
        CHECK(app->sync_manager()->get_current_user() == nullptr);

        app->delete_user(user_a, [&](Optional<app::AppError> error) {
            CHECK(error->reason() == "User must be logged in to be deleted.");
            CHECK(app->sync_manager()->all_users().size() == 0);
        });

        // Log in user 2
        auto user_b = log_in(app);
        CHECK(app->sync_manager()->get_current_user() == user_b);
        CHECK(user_b->state() == SyncUser::State::LoggedIn);
        CHECK(app->sync_manager()->all_users().size() == 1);

        app->delete_user(user_b, [&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(app->sync_manager()->all_users().size() == 0);
        });

        CHECK(app->sync_manager()->get_current_user() == nullptr);

        // check both handles are no longer valid
        CHECK(user_a->state() == SyncUser::State::Removed);
        CHECK(user_b->state() == SyncUser::State::Removed);
    }
}

TEST_CASE("app: delete user with credentials integration", "[sync][app][baas][user][new]") {
    TestAppSession session;
    auto app = session.app();
    app->remove_user(app->current_user(), [](auto) {});

    SECTION("log in and delete") {
        CHECK(app->sync_manager()->all_users().size() == 0);
        CHECK(app->sync_manager()->get_current_user() == nullptr);

        auto credentials = create_user_and_log_in(app);
        auto user = app->current_user();

        CHECK(app->sync_manager()->get_current_user() == user);
        CHECK(user->state() == SyncUser::State::LoggedIn);
        app->delete_user(user, [&](Optional<app::AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(app->sync_manager()->all_users().size() == 0);
        });
        CHECK(user->state() == SyncUser::State::Removed);
        CHECK(app->sync_manager()->get_current_user() == nullptr);

        app->log_in_with_credentials(credentials, [](std::shared_ptr<SyncUser> user, util::Optional<AppError> error) {
            CHECK(!user);
            REQUIRE(error);
            REQUIRE(error->code() == ErrorCodes::InvalidPassword);
        });
        CHECK(app->sync_manager()->get_current_user() == nullptr);

        CHECK(app->sync_manager()->all_users().size() == 0);
        app->delete_user(user, [](Optional<app::AppError> err) {
            CHECK(err->code() > 0);
        });

        CHECK(app->sync_manager()->get_current_user() == nullptr);
        CHECK(app->sync_manager()->all_users().size() == 0);
        CHECK(user->state() == SyncUser::State::Removed);
    }
}

// MARK: - Token refresh

TEST_CASE("app: token refresh", "[sync][app][baas][user][new]") {
    TestAppSession session;
    auto app = session.app();
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
        dog_collection.find_one(dog_document, [&](Optional<bson::BsonDocument>, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });
    }
}

TEST_CASE("app: sync user integration", "[sync][pbs][app][baas][user][new]") {
    auto logger = std::make_shared<util::StderrLogger>(realm::util::Logger::Level::TEST_LOGGING_LEVEL);
    const auto schema = default_app_config("").schema;

    auto get_dogs = [](SharedRealm r) -> Results {
        wait_for_upload(*r, std::chrono::seconds(10));
        wait_for_download(*r, std::chrono::seconds(10));
        return Results(r, r->read_group().get_table("class_Dog"));
    };

    auto create_one_dog = [](SharedRealm r) {
        r->begin_transaction();
        CppContext c;
        Object::create(c, r, "Dog",
                       std::any(AnyDict{{"_id", std::any(ObjectId::gen())},
                                        {"breed", std::string("bulldog")},
                                        {"name", std::string("fido")}}),
                       CreatePolicy::ForceCreate);
        r->commit_transaction();
    };

    TestAppSession session;
    auto app = session.app();
    const auto partition = random_string(100);

    // MARK: Expired Session Refresh -
    SECTION("Invalid Access Token is Refreshed") {
        {
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);
            REQUIRE(get_dogs(r).size() == 0);
            create_one_dog(r);
            REQUIRE(get_dogs(r).size() == 1);
        }

        {
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

    SECTION("Fast clock on client") {
        {
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);

            REQUIRE(get_dogs(r).size() == 0);
            create_one_dog(r);
            REQUIRE(get_dogs(r).size() == 1);
        }

        auto transport = std::make_shared<HookedTransport>();
        TestAppSession hooked_session(session.app_session(), transport, DeleteApp{false});
        auto app = hooked_session.app();
        std::shared_ptr<SyncUser> user = app->current_user();
        REQUIRE(user);
        REQUIRE(!user->access_token_refresh_required());
        // Make the SyncUser behave as if the client clock is 31 minutes fast, so the token looks expired locallaly
        // (access tokens have an lifetime of 30 mintutes today).
        user->set_seconds_to_adjust_time_for_testing(31 * 60);
        REQUIRE(user->access_token_refresh_required());

        // This assumes that we make an http request for the new token while
        // already in the WaitingForAccessToken state.
        bool seen_waiting_for_access_token = false;
        transport->request_hook = [&](const Request&) {
            auto user = app->current_user();
            REQUIRE(user);
            for (auto& session : user->all_sessions()) {
                // Prior to the fix for #4941, this callback would be called from an infinite loop, always in the
                // WaitingForAccessToken state.
                if (session->state() == SyncSession::State::WaitingForAccessToken) {
                    REQUIRE(!seen_waiting_for_access_token);
                    seen_waiting_for_access_token = true;
                }
            }
            return true;
        };
        SyncTestFile config(app, partition, schema);
        auto r = Realm::get_shared_realm(config);
        REQUIRE(seen_waiting_for_access_token);
        Results dogs = get_dogs(r);
        REQUIRE(dogs.size() == 1);
        REQUIRE(dogs.get(0).get<String>("breed") == "bulldog");
        REQUIRE(dogs.get(0).get<String>("name") == "fido");
    }

    SECTION("Expired Tokens") {
        sync::AccessToken token;
        {
            std::shared_ptr<SyncUser> user = app->current_user();
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);

            REQUIRE(get_dogs(r).size() == 0);
            create_one_dog(r);

            REQUIRE(get_dogs(r).size() == 1);
            sync::AccessToken::ParseError error_state = realm::sync::AccessToken::ParseError::none;
            sync::AccessToken::parse(user->access_token(), token, error_state, nullptr);
            REQUIRE(error_state == sync::AccessToken::ParseError::none);
            REQUIRE(token.timestamp);
            REQUIRE(token.expires);
            REQUIRE(token.timestamp < token.expires);
            std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
            using namespace std::chrono_literals;
            token.expires = std::chrono::system_clock::to_time_t(now - 30s);
            REQUIRE(token.expired(now));
        }

        auto transport = std::make_shared<HookedTransport>();
        TestAppSession hooked_session(session.app_session(), transport, DeleteApp{false});
        auto app = hooked_session.app();
        std::shared_ptr<SyncUser> user = app->current_user();
        REQUIRE(user);
        REQUIRE(!user->access_token_refresh_required());
        // Set a bad access token, with an expired time. This will trigger a refresh initiated by the client.
        user->update_access_token(encode_fake_jwt("fake_access_token", token.expires, token.timestamp));
        REQUIRE(user->access_token_refresh_required());

        SECTION("Expired Access Token is Refreshed") {
            // This assumes that we make an http request for the new token while
            // already in the WaitingForAccessToken state.
            bool seen_waiting_for_access_token = false;
            transport->request_hook = [&](const Request&) {
                auto user = app->current_user();
                REQUIRE(user);
                for (auto& session : user->all_sessions()) {
                    if (session->state() == SyncSession::State::WaitingForAccessToken) {
                        REQUIRE(!seen_waiting_for_access_token);
                        seen_waiting_for_access_token = true;
                    }
                }
            };
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);
            REQUIRE(seen_waiting_for_access_token);
            Results dogs = get_dogs(r);
            REQUIRE(dogs.size() == 1);
            REQUIRE(dogs.get(0).get<String>("breed") == "bulldog");
            REQUIRE(dogs.get(0).get<String>("name") == "fido");
        }

        SECTION("User is logged out if the refresh request is denied") {
            REQUIRE(user->is_logged_in());
            transport->response_hook = [&](const Request& request, const Response& response) {
                auto user = app->current_user();
                REQUIRE(user);
                // simulate the server denying the refresh
                if (request.url.find("/session") != std::string::npos) {
                    auto& response_ref = const_cast<Response&>(response);
                    response_ref.http_status_code = 401;
                    response_ref.body = "fake: refresh token could not be refreshed";
                }
            };
            SyncTestFile config(app, partition, schema);
            std::atomic<bool> sync_error_handler_called{false};
            config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                sync_error_handler_called.store(true);
                REQUIRE(error.get_system_error() ==
                        sync::make_error_code(realm::sync::ProtocolError::bad_authentication));
                REQUIRE(error.reason() == "Unable to refresh the user access token.");
            };
            auto r = Realm::get_shared_realm(config);
            timed_wait_for([&] {
                return sync_error_handler_called.load();
            });
            // the failed refresh logs out the user
            REQUIRE(!user->is_logged_in());
        }

        SECTION("Requests that receive an error are retried on a backoff") {
            using namespace std::chrono;
            std::vector<time_point<steady_clock>> response_times;
            std::atomic<bool> did_receive_valid_token{false};
            constexpr size_t num_error_responses = 6;

            transport->response_hook = [&](const Request& request, const Response& response) {
                // simulate the server experiencing an internal server error
                if (request.url.find("/session") != std::string::npos) {
                    if (response_times.size() >= num_error_responses) {
                        did_receive_valid_token.store(true);
                        return;
                    }
                    auto& response_ref = const_cast<Response&>(response);
                    response_ref.http_status_code = 500;
                }
            };
            transport->request_hook = [&](const Request& request) {
                if (!did_receive_valid_token.load() && request.url.find("/session") != std::string::npos) {
                    response_times.push_back(steady_clock::now());
                }
            };
            SyncTestFile config(app, partition, schema);
            auto r = Realm::get_shared_realm(config);
            create_one_dog(r);
            timed_wait_for(
                [&] {
                    return did_receive_valid_token.load();
                },
                30s);
            REQUIRE(user->is_logged_in());
            REQUIRE(response_times.size() >= num_error_responses);
            std::vector<uint64_t> delay_times;
            for (size_t i = 1; i < response_times.size(); ++i) {
                delay_times.push_back(duration_cast<milliseconds>(response_times[i] - response_times[i - 1]).count());
            }

            // sync delays start at 1000ms minus a random number of up to 25%.
            // the subsequent delay is double the previous one minus a random 25% again.
            // this calculation happens in Connection::initiate_reconnect_wait()
            bool increasing_delay = true;
            for (size_t i = 1; i < delay_times.size(); ++i) {
                if (delay_times[i - 1] >= delay_times[i]) {
                    increasing_delay = false;
                }
            }
            // fail if the first delay isn't longer than half a second
            if (delay_times.size() <= 1 || delay_times[1] < 500) {
                increasing_delay = false;
            }
            if (!increasing_delay) {
                std::string message = "delay times are not increasing: ";

                for (auto& delay : delay_times) {
                    message += delay;
                    message += ", ";
                }
                logger->error(message.c_str());
            }
            REQUIRE(increasing_delay);
        }
    }

    SECTION("Invalid refresh token") {
        auto& app_session = session.app_session();
        std::mutex mtx;
        auto verify_error_on_sync_with_invalid_refresh_token = [&](std::shared_ptr<SyncUser> user,
                                                                   Realm::Config config) {
            REQUIRE(user);
            REQUIRE(app_session.admin_api.verify_access_token(user->access_token(), app_session.server_app_id));

            // requesting a new access token fails because the refresh token used for this request is revoked
            user->refresh_custom_data([&](Optional<AppError> error) {
                REQUIRE(error);
                REQUIRE(error->additional_status_code == 401);
                REQUIRE(error->code() == ErrorCodes::InvalidSession);
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
                std::lock_guard<std::mutex> lock(mtx);
                sync_error_handler_called.store(true);
                REQUIRE(error.get_system_error() ==
                        sync::make_error_code(realm::sync::ProtocolError::bad_authentication));
                REQUIRE(error.reason() == "Unable to refresh the user access token.");
            };

            auto transport = static_cast<SynchronousTestTransport*>(session.transport());
            transport->block(); // don't let the token refresh happen until we're ready for it
            auto r = Realm::get_shared_realm(config);
            auto session = user->session_for_on_disk_path(config.path);
            REQUIRE(user->is_logged_in());
            REQUIRE(!sync_error_handler_called.load());
            {
                std::atomic<bool> called{false};
                session->wait_for_upload_completion([&](Status stat) {
                    std::lock_guard<std::mutex> lock(mtx);
                    called.store(true);
                    REQUIRE(stat.code() == ErrorCodes::InvalidSession);
                });
                transport->unblock();
                timed_wait_for([&] {
                    return called.load();
                });
                std::lock_guard<std::mutex> lock(mtx);
                REQUIRE(called);
            }
            timed_wait_for([&] {
                return sync_error_handler_called.load();
            });

            // the failed refresh logs out the user
            std::lock_guard<std::mutex> lock(mtx);
            REQUIRE(!user->is_logged_in());
        };

        SECTION("Disabled user results in a sync error") {
            auto creds = create_user_and_log_in(app);
            SyncTestFile config(app, partition, schema);
            auto user = app->current_user();
            REQUIRE(user);
            REQUIRE(app_session.admin_api.verify_access_token(user->access_token(), app_session.server_app_id));
            app_session.admin_api.disable_user_sessions(app->current_user()->identity(), app_session.server_app_id);

            verify_error_on_sync_with_invalid_refresh_token(user, config);

            // logging in again doesn't fix things while the account is disabled
            auto error = failed_log_in(app, creds);
            REQUIRE(error.code() == ErrorCodes::UserDisabled);

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
            auto creds = create_user_and_log_in(app);
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
            user->refresh_custom_data([&](Optional<AppError> error) {
                REQUIRE_FALSE(error);
            });

            {
                // check that there are no errors initiating a new sync session by making sure upload/download
                // succeeds
                auto r = Realm::get_shared_realm(config);
                Results dogs = get_dogs(r);
            }
        }

        SECTION("Revoked refresh token on an anonymous user results in a sync error") {
            app->current_user()->log_out();
            auto anon_user = log_in(app);
            REQUIRE(app->current_user() == anon_user);
            SyncTestFile config(app, partition, schema);
            REQUIRE(app_session.admin_api.verify_access_token(anon_user->access_token(), app_session.server_app_id));
            app_session.admin_api.revoke_user_sessions(anon_user->identity(), app_session.server_app_id);
            // revoking a user session only affects the refresh token, so the access token should still continue to
            // work.
            REQUIRE(app_session.admin_api.verify_access_token(anon_user->access_token(), app_session.server_app_id));

            verify_error_on_sync_with_invalid_refresh_token(anon_user, config);

            // the user has been logged out, and current user is reset
            REQUIRE(!app->current_user());
            REQUIRE(!anon_user->is_logged_in());
            REQUIRE(anon_user->state() == SyncUser::State::Removed);

            // new requests for an access token do not work for anon users
            anon_user->refresh_custom_data([&](Optional<AppError> error) {
                REQUIRE(error);
                REQUIRE(error->reason() ==
                        util::format("Cannot initiate a refresh on user '%1' because the user has been removed",
                                     anon_user->identity()));
            });

            REQUIRE_THROWS_MATCHES(
                Realm::get_shared_realm(config), std::logic_error,
                Catch::Matchers::Message(
                    util::format("Cannot start a sync session for user '%1' because this user has been removed.",
                                 anon_user->identity())));
        }

        SECTION("Opening a Realm with a removed email user results produces an exception") {
            auto creds = create_user_and_log_in(app);
            auto email_user = app->current_user();
            const std::string user_ident = email_user->identity();
            REQUIRE(email_user);
            SyncTestFile config(app, partition, schema);
            REQUIRE(email_user->is_logged_in());
            {
                // sync works on a valid user
                auto r = Realm::get_shared_realm(config);
                Results dogs = get_dogs(r);
            }
            app->sync_manager()->remove_user(user_ident);
            REQUIRE_FALSE(email_user->is_logged_in());
            REQUIRE(email_user->state() == SyncUser::State::Removed);

            // should not be able to open a sync'd Realm with an invalid user
            REQUIRE_THROWS_MATCHES(
                Realm::get_shared_realm(config), std::logic_error,
                Catch::Matchers::Message(util::format(
                    "Cannot start a sync session for user '%1' because this user has been removed.", user_ident)));

            std::shared_ptr<SyncUser> new_user_instance = log_in(app, creds);
            // the previous instance is still invalid
            REQUIRE_FALSE(email_user->is_logged_in());
            REQUIRE(email_user->state() == SyncUser::State::Removed);
            // but the new instance will work and has the same server issued ident
            REQUIRE(new_user_instance);
            REQUIRE(new_user_instance->is_logged_in());
            REQUIRE(new_user_instance->identity() == user_ident);
            {
                // sync works again if the same user is logged back in
                config.sync_config->user = new_user_instance;
                auto r = Realm::get_shared_realm(config);
                Results dogs = get_dogs(r);
            }
        }
    }
}

TEST_CASE("app: custom user data integration tests", "[sync][app][baas][user][new]") {
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

TEST_CASE("app: jwt login and metadata tests", "[sync][app][baas][user][new]") {
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
