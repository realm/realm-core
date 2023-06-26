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

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/app_utils.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/sync/noinst/server/access_token.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/overload.hpp>
#include <realm/util/future.hpp>
#include <realm/util/platform_info.hpp>
#include <realm/util/uri.hpp>

#include <external/json/json.hpp>
#include <external/mpark/variant.hpp>

#include <util/event_loop.hpp>
#include <util/sync_test_utils.hpp>
#include <util/test_utils.hpp>
#include <util/test_file.hpp>

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <list>
#include <mutex>
#include <thread>

using namespace realm;
using namespace realm::app;
using util::Optional;

using namespace std::string_view_literals;

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

TEST_CASE("app: custom error handling", "[sync][app][local][custom_errors]") {
    SECTION("custom code and message is sent back") {
        auto transport = std::make_shared<LocalTransport>();
        transport->set_custom_error(1001);
        transport->set_body("Boom!");

        TestSyncManager tsm(get_config(transport));
        auto error = failed_log_in(tsm.app());
        CHECK(error.is_custom_error());
        CHECK(*error.additional_status_code == 1001);
        CHECK(error.reason() == "Boom!");
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

class UnitTestTransport : public LocalTransport {
    std::string m_provider_type;

public:
    UnitTestTransport(const std::string& provider_type = "anon-user")
        : m_provider_type(provider_type)
    {
        request_hook = handle_request();
    }

    static std::string access_token;

    static const std::string api_key;
    static const std::string api_key_id;
    static const std::string api_key_name;
    static const std::string auth_route;
    static const std::string user_id;
    static const std::string identity_0_id;
    static const std::string identity_1_id;

    std::function<std::optional<app::Response>(const app::Request&)> custom_request_hook;

    void set_provider_type(const std::string& provider_type)
    {
        m_provider_type = provider_type;
    }

private:
    std::function<std::optional<app::Response>(const app::Request& request)> handle_request()
    {
        return [this](const app::Request& request) -> std::optional<app::Response> {
            if (custom_request_hook) {
                auto response = custom_request_hook(request);
                if (response) {
                    return response;
                }
            }
            if (request.url.find("/login") != std::string::npos) {
                return handle_login(request);
            }
            else if (request.url.find("/profile") != std::string::npos) {
                return handle_profile(request);
            }
            else if (request.url.find("/session") != std::string::npos && request.method != HttpMethod::post) {
                return make_ok_response();
            }
            else if (request.url.find("/api_keys") != std::string::npos && request.method == HttpMethod::post) {
                return handle_create_api_key(request);
            }
            else if (request.url.find(util::format("/api_keys/%1", api_key_id)) != std::string::npos &&
                     request.method == HttpMethod::get) {
                return handle_fetch_api_key(request);
            }
            else if (request.url.find("/api_keys") != std::string::npos && request.method == HttpMethod::get) {
                return handle_fetch_api_keys(request);
            }
            else if (request.url.find("/session") != std::string::npos && request.method == HttpMethod::post) {
                return handle_token_refresh(request);
            }
            else if (request.url.find("/location") != std::string::npos && request.method == HttpMethod::get) {
                return handle_location(request);
            }
            else {
                return make_ok_response("something arbitrary");
            }
        };
    }

    app::Response handle_profile(const Request& request)
    {
        CHECK(request.method == HttpMethod::get);
        auto content_type = AppUtils::find_header("Content-Type", request.headers);
        CHECK(content_type);
        CHECK(content_type->second == CONTENT_TYPE_JSON);
        auto authorization = AppUtils::find_header("Authorization", request.headers);
        CHECK(authorization);
        CHECK(authorization->second == "Bearer " + access_token);
        CHECK(request.body.empty());
        CHECK(request.timeout_ms == 60000);

        return make_json_response(
            sync::HTTPStatus::Ok,
            {{"user_id", user_id},
             {"identities",
              {{{"id", identity_0_id}, {"provider_type", m_provider_type}, {"provider_id", "lol"}},
               {{"id", identity_1_id}, {"provider_type", "lol_wut"}, {"provider_id", "nah_dawg"}}}},
             {"data", profile_0}});
    }

    app::Response handle_login(const Request& request)
    {
        CHECK(request.method == HttpMethod::post);
        auto item = AppUtils::find_header("Content-Type", request.headers);
        CHECK(item);
        CHECK(item->second == CONTENT_TYPE_JSON);
        CHECK(nlohmann::json::parse(request.body)["options"] ==
              nlohmann::json({{"device",
                               {{"appId", "app_id"},
                                {"appVersion", "A Local App Version"},
                                {"platform", util::get_library_platform()},
                                {"platformVersion", "Object Store Test Platform Version"},
                                {"sdk", "SDK Name"},
                                {"sdkVersion", "SDK Version"},
                                {"cpuArch", util::get_library_cpu_arch()},
                                {"deviceName", "Device Name"},
                                {"deviceVersion", "Device Version"},
                                {"frameworkName", "Framework Name"},
                                {"frameworkVersion", "Framework Version"},
                                {"coreVersion", REALM_VERSION_STRING},
                                {"bundleId", "Bundle Id"}}}}));

        CHECK(request.timeout_ms == 60000);

        return make_json_response(sync::HTTPStatus::Ok, {{"access_token", access_token},
                                                         {"refresh_token", access_token},
                                                         {"user_id", random_string(15)},
                                                         {"device_id", "Panda Bear"}});
    }

    app::Response handle_location(const Request& request)
    {
        CHECK(request.method == HttpMethod::get);
        CHECK(request.timeout_ms == 60000);

        return make_location_response("field", "shouldn't");
    }

    app::Response handle_create_api_key(const Request& request)
    {
        CHECK(request.method == HttpMethod::post);
        auto item = AppUtils::find_header("Content-Type", request.headers);
        CHECK(item);
        CHECK(item->second == CONTENT_TYPE_JSON);
        CHECK(nlohmann::json::parse(request.body) == nlohmann::json({{"name", api_key_name}}));
        CHECK(request.timeout_ms == 60000);

        return make_json_response(
            sync::HTTPStatus::Ok,
            {{"_id", api_key_id}, {"key", api_key}, {"name", api_key_name}, {"disabled", false}});
    }

    app::Response handle_fetch_api_key(const Request& request)
    {
        CHECK(request.method == HttpMethod::get);
        auto item = AppUtils::find_header("Content-Type", request.headers);
        CHECK(item);
        CHECK(item->second == CONTENT_TYPE_JSON);

        CHECK(request.body == "");
        CHECK(request.timeout_ms == 60000);

        return make_json_response(sync::HTTPStatus::Ok,
                                  {{"_id", api_key_id}, {"name", api_key_name}, {"disabled", false}});
    }

    app::Response handle_fetch_api_keys(const Request& request)
    {
        CHECK(request.method == HttpMethod::get);
        auto item = AppUtils::find_header("Content-Type", request.headers);
        CHECK(item);
        CHECK(item->second == CONTENT_TYPE_JSON);

        CHECK(request.body == "");
        CHECK(request.timeout_ms == 60000);

        auto elements = std::vector<nlohmann::json>();
        for (int i = 0; i < 2; i++) {
            elements.push_back({{"_id", api_key_id}, {"name", api_key_name}, {"disabled", false}});
        }

        return make_json_response(sync::HTTPStatus::Ok, nlohmann::json(elements));
    }

    app::Response handle_token_refresh(const Request& request)
    {
        CHECK(request.method == HttpMethod::post);
        auto item = AppUtils::find_header("Content-Type", request.headers);
        CHECK(item);
        CHECK(item->second == CONTENT_TYPE_JSON);

        CHECK(request.body == "");
        CHECK(request.timeout_ms == 60000);

        return make_json_response(sync::HTTPStatus::Ok, {{"access_token", access_token}});
    }
};

static TestSyncManager::Config get_config()
{
    return get_config(instance_of<UnitTestTransport>);
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

TEST_CASE("subscribable unit tests", "[sync][app][local]") {
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

TEST_CASE("app: login_with_credentials unit_tests", "[sync][app][local]") {
    auto config = get_config();

    SECTION("login_anonymous good") {
        UnitTestTransport::access_token = good_access_token;
        config.base_path = util::make_temp_dir();
        config.should_teardown_test_directory = false;
        {
            TestSyncManager tsm(config);
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
            TestSyncManager tsm(config);
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
        auto transport = std::make_shared<UnitTestTransport>();
        transport->custom_request_hook = [](const app::Request& request) -> std::optional<app::Response> {
            if (request.url.find("/login") != std::string::npos) {
                return make_json_response(sync::HTTPStatus::Ok, user_json(bad_access_token));
            }
            return std::nullopt;
        };
        config.transport = std::move(transport);
        TestSyncManager tsm(config);
        auto error = failed_log_in(tsm.app());
        CHECK(error.reason() == std::string("jwt missing parts"));
        CHECK(error.code_string() == "BadToken");
        CHECK(error.is_json_error());
        CHECK(error.code() == ErrorCodes::BadToken);
    }

    SECTION("login_anonynous multiple users") {
        UnitTestTransport::access_token = good_access_token;
        config.base_path = util::make_temp_dir();
        config.should_teardown_test_directory = false;
        TestSyncManager tsm(config);
        auto app = tsm.app();

        auto user1 = log_in(app);
        auto user2 = log_in(app, AppCredentials::anonymous(false));
        CHECK(user1 != user2);
    }
}

TEST_CASE("app: UserAPIKeyProviderClient unit_tests", "[sync][app][local]") {
    TestSyncManager sync_manager(get_config(), {});
    auto app = sync_manager.app();
    auto client = app->provider_client<App::UserAPIKeyProviderClient>();

    std::shared_ptr<SyncUser> logged_in_user = app->sync_manager()->get_user(
        UnitTestTransport::user_id, good_access_token, good_access_token, "anon-user", dummy_device_id);
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


TEST_CASE("app: user_semantics", "[app][local]") {
    TestSyncManager tsm(get_config(), {});
    auto app = tsm.app();

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

        app->log_out([](auto) {});
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
        CHECK(app->current_user()->identity() == user1->identity());
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);

        const auto user2 = login_user_anonymous();
        CHECK(app->all_users()[0]->state() == SyncUser::State::LoggedIn);
        CHECK(app->all_users().size() == 1);
        CHECK(app->current_user()->identity() == user2->identity());
        CHECK(user1->identity() == user2->identity());

        app->log_out([](auto) {});
        CHECK(app->all_users().size() == 0);

        CHECK(event_processed == 0);
    }
}

TEST_CASE("app: response error handling", "[sync][app][local]") {
    auto transport = std::make_shared<LocalTransport>();
    transport->simulated_response = make_json_response(sync::HTTPStatus::Ok, {{"access_token", good_access_token},
                                                                              {"refresh_token", good_access_token},
                                                                              {"user_id", "Brown Bear"},
                                                                              {"device_id", "Panda Bear"}});
    TestSyncManager tsm(get_config(transport));
    auto app = tsm.app();

    SECTION("http 404") {
        transport->set_http_status(sync::HTTPStatus::NotFound);
        auto error = failed_log_in(app);
        CHECK(!error.is_json_error());
        CHECK(!error.is_custom_error());
        CHECK(!error.is_service_error());
        CHECK(error.is_http_error());
        CHECK(*error.additional_status_code == 404);
        CHECK(error.reason().find(std::string("http error code considered fatal")) != std::string::npos);
    }
    SECTION("http 500") {
        transport->set_http_status(sync::HTTPStatus::InternalServerError);
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
        transport->simulated_response.custom_status_code = 42;
        transport->set_body("Custom error message");
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
        transport->simulated_response =
            make_json_response(sync::HTTPStatus::BadRequest, {{"error_code", "MongoDBError"},
                                                              {"error", "a fake MongoDB error message!"},
                                                              {"access_token", good_access_token},
                                                              {"refresh_token", good_access_token},
                                                              {"user_id", "Brown Bear"},
                                                              {"device_id", "Panda Bear"},
                                                              {"link", "http://...whatever the server passes us"}});
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
        transport->set_body("this: is not{} a valid json body!");
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

TEST_CASE("app: switch user", "[sync][app][local]") {
    TestSyncManager tsm(get_config(), {});
    auto app = tsm.app();

    bool processed = false;

    SECTION("switch user expect success") {
        CHECK(app->sync_manager()->all_users().size() == 0);

        // Log in user 1
        auto user_a = log_in(app, AppCredentials::username_password("test@10gen.com", "password"));
        CHECK(app->sync_manager()->get_current_user() == user_a);

        // Log in user 2
        auto user_b = log_in(app, AppCredentials::username_password("test2@10gen.com", "password"));
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
        auto user_a = log_in(app, AppCredentials::username_password("test@10gen.com", "password"));
        CHECK(app->sync_manager()->get_current_user() == user_a);

        app->log_out([&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        CHECK(app->sync_manager()->get_current_user() == nullptr);
        CHECK(user_a->state() == SyncUser::State::LoggedOut);

        // Log in user 2
        auto user_b = log_in(app, AppCredentials::username_password("test2@10gen.com", "password"));
        CHECK(app->sync_manager()->get_current_user() == user_b);
        CHECK(app->sync_manager()->all_users().size() == 2);

        REQUIRE_THROWS_AS(app->switch_user(user_a), AppError);
        CHECK(app->sync_manager()->get_current_user() == user_b);
    }
}

TEST_CASE("app: remove anonymous user", "[sync][app][local]") {
    TestSyncManager tsm(get_config(), {});
    auto app = tsm.app();

    SECTION("remove user expect success") {
        CHECK(app->sync_manager()->all_users().size() == 0);

        // Log in user 1
        auto user_a = log_in(app);
        CHECK(user_a->state() == SyncUser::State::LoggedIn);

        app->log_out(user_a, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
            // a logged out anon user will be marked as Removed, not LoggedOut
            CHECK(user_a->state() == SyncUser::State::Removed);
        });
        CHECK(app->sync_manager()->all_users().empty());

        app->remove_user(user_a, [&](Optional<AppError> error) {
            CHECK(error->reason() == "User has already been removed");
            CHECK(app->sync_manager()->all_users().size() == 0);
        });

        // Log in user 2
        auto user_b = log_in(app);
        CHECK(app->sync_manager()->get_current_user() == user_b);
        CHECK(user_b->state() == SyncUser::State::LoggedIn);
        CHECK(app->sync_manager()->all_users().size() == 1);

        app->remove_user(user_b, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
            CHECK(app->sync_manager()->all_users().size() == 0);
        });

        CHECK(app->sync_manager()->get_current_user() == nullptr);

        // check both handles are no longer valid
        CHECK(user_a->state() == SyncUser::State::Removed);
        CHECK(user_b->state() == SyncUser::State::Removed);
    }
}

TEST_CASE("app: remove user with credentials", "[sync][app][local]") {
    TestSyncManager tsm(get_config(), {});
    auto app = tsm.app();

    SECTION("log in, log out and remove") {
        CHECK(app->sync_manager()->all_users().size() == 0);
        CHECK(app->sync_manager()->get_current_user() == nullptr);

        auto user = log_in(app, AppCredentials::username_password("email", "pass"));

        CHECK(user->state() == SyncUser::State::LoggedIn);

        app->log_out(user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        CHECK(user->state() == SyncUser::State::LoggedOut);

        app->remove_user(user, [&](Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });
        CHECK(app->sync_manager()->all_users().size() == 0);

        Optional<AppError> error;
        app->remove_user(user, [&](Optional<AppError> err) {
            error = err;
        });
        CHECK(error->code() > 0);
        CHECK(app->sync_manager()->all_users().size() == 0);
        CHECK(user->state() == SyncUser::State::Removed);
    }
}

TEST_CASE("app: link_user", "[sync][app][local]") {
    TestSyncManager tsm(get_config(), {});
    auto app = tsm.app();

    auto email = util::format("realm_tests_do_autoverify%1@%2.com", random_string(10), random_string(10));
    auto password = random_string(10);

    auto custom_credentials = AppCredentials::facebook("a_token");
    auto email_pass_credentials = AppCredentials::username_password(email, password);

    auto sync_user = log_in(app, email_pass_credentials);
    CHECK(sync_user->provider_type() == IdentityProviderUsernamePassword);

    SECTION("successful link") {
        bool processed = false;
        app->link_user(sync_user, custom_credentials, [&](std::shared_ptr<SyncUser> user, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(user);
            CHECK(user->identity() == sync_user->identity());
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

TEST_CASE("app: auth providers", "[sync][app][local]") {
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

TEST_CASE("app: refresh access token unit tests", "[sync][app][local]") {
    auto setup_user = [](std::shared_ptr<App> app) {
        if (app->sync_manager()->get_current_user()) {
            return;
        }
        app->sync_manager()->get_user("a_user_id", good_access_token, good_access_token, "anon-user",
                                      dummy_device_id);
    };

    SECTION("refresh custom data happy path") {
        bool session_route_hit = false;

        auto transport = std::make_shared<UnitTestTransport>();
        transport->custom_request_hook =
            [&session_route_hit](const app::Request& request) -> std::optional<app::Response> {
            if (request.url.find("/session") != std::string::npos) {
                session_route_hit = true;
                return make_json_response(sync::HTTPStatus::Ok, {{"access_token", good_access_token}});
            }
            return std::nullopt;
        };

        TestSyncManager sync_manager(get_config(transport));
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
        bool session_route_hit = false;

        auto transport = std::make_shared<UnitTestTransport>();
        transport->custom_request_hook =
            [&session_route_hit](const app::Request& request) -> std::optional<app::Response> {
            if (request.url.find("/session") != std::string::npos) {
                session_route_hit = true;
                return make_json_response(sync::HTTPStatus::Ok, {{"access_token", bad_access_token}});
            }
            return std::nullopt;
        };

        TestSyncManager sync_manager(get_config(transport));
        auto app = sync_manager.app();
        setup_user(app);

        bool processed = false;
        app->refresh_custom_data(app->sync_manager()->get_current_user(), [&](const Optional<AppError>& error) {
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
        bool login_hit = false;
        bool get_profile_1_hit = false;
        bool get_profile_2_hit = false;
        bool refresh_hit = false;

        auto transport = std::make_shared<LocalTransport>();
        transport->request_hook = [&](const Request& request) -> std::optional<app::Response> {
            if (request.url.find("/login") != std::string::npos) {
                login_hit = true;
                return make_json_response(sync::HTTPStatus::Ok, user_json(good_access_token));
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

                    return make_json_response(sync::HTTPStatus::Ok, user_profile_json());
                }
                else if (access_token.find(good_access_token) != std::string::npos) {
                    CHECK(!get_profile_2_hit);
                    get_profile_1_hit = true;

                    return app::Response{401, 0, {}};
                }
            }
            else if (request.url.find("/session") != std::string::npos && request.method == HttpMethod::post) {
                CHECK(login_hit);
                CHECK(get_profile_1_hit);
                CHECK(!get_profile_2_hit);
                refresh_hit = true;

                return make_json_response(sync::HTTPStatus::Ok, {{"access_token", good_access_token2}});
            }
            else if (request.url.find("/location") != std::string::npos) {
                CHECK(request.method == HttpMethod::get);
                return make_location_response("http://localhost:9090", "ws://localhost:9090");
            }
            return std::nullopt;
        };

        TestSyncManager sync_manager(get_config(transport));
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

    void add_work_item(Response&& response, util::UniqueFunction<void(const Response&)>&& completion)
    {
        std::lock_guard<std::mutex> lk(transport_work_mutex);
        transport_work.push_front(ResponseWorkItem{std::move(response), std::move(completion)});
        transport_work_cond.notify_one();
    }

    void add_work_item(util::UniqueFunction<void()> cb)
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
        util::UniqueFunction<void(const Response&)> completion;
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
                                                work_item.completion(std::move(work_item.response));
                                            },
                                            [](util::UniqueFunction<void()>& cb) {
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
    std::list<mpark::variant<ResponseWorkItem, util::UniqueFunction<void()>>> transport_work;
    JoiningThread transport_thread;
};

template <typename State>
struct TestStateBundle {
    void advance_to(State new_state)
    {
        std::lock_guard<std::mutex> lk(mutex);
        state = new_state;
        cond.notify_one();
    }

    State get() const
    {
        std::lock_guard<std::mutex> lk(mutex);
        return state;
    }

    void wait_for(State new_state)
    {
        std::unique_lock lk(mutex);
        bool failed = !cond.wait_for(lk, std::chrono::seconds(5), [&] {
            return state == new_state;
        });
        if (failed) {
            throw std::runtime_error("wait timed out");
        }
    }

    mutable std::mutex mutex;
    std::condition_variable cond;

    State state = State::unknown;
};

} // namespace

TEST_CASE("app: app destroyed during token refresh", "[sync][app][local]") {
    AsyncMockNetworkTransport mock_transport_worker;
    enum class TestState { unknown, location, login, profile_1, profile_2, refresh_1, refresh_2, refresh_3 };
    struct TestStateBundle<TestState> state;
    auto transport = std::make_shared<LocalTransport>();
    transport->send_hook = [&state, &mock_transport_worker](
                               const Request& request, util::UniqueFunction<void(const Response&)>&& completion) {
        if (request.url.find("/login") != std::string::npos) {
            CHECK(state.get() == TestState::location);
            state.advance_to(TestState::login);
            mock_transport_worker.add_work_item(
                make_json_response(sync::HTTPStatus::Ok, user_json(encode_fake_jwt("access token 1"))),
                std::move(completion));
        }
        else if (request.url.find("/profile") != std::string::npos) {
            // simulated bad token request
            auto cur_state = state.get();
            CHECK((cur_state == TestState::refresh_1 || cur_state == TestState::login));
            if (cur_state == TestState::refresh_1) {
                state.advance_to(TestState::profile_2);
                mock_transport_worker.add_work_item(make_json_response(sync::HTTPStatus::Ok, user_profile_json()),
                                                    std::move(completion));
            }
            else if (cur_state == TestState::login) {
                state.advance_to(TestState::profile_1);
                mock_transport_worker.add_work_item(Response{401, 0, {}}, std::move(completion));
            }
        }
        else if (request.url.find("/session") != std::string::npos && request.method == HttpMethod::post) {
            if (state.get() == TestState::profile_1) {
                state.advance_to(TestState::refresh_1);
                mock_transport_worker.add_work_item(
                    make_json_response(sync::HTTPStatus::Ok, {{"access_token", encode_fake_jwt("access token 1")}}),
                    std::move(completion));
            }
            else if (state.get() == TestState::profile_2) {
                state.advance_to(TestState::refresh_2);
                mock_transport_worker.add_work_item(
                    make_json_response(sync::HTTPStatus::Ok, {{"error", "too bad, buddy!"}}), std::move(completion));
            }
            else {
                CHECK(state.get() == TestState::refresh_2);
                state.advance_to(TestState::refresh_3);
                mock_transport_worker.add_work_item(
                    make_json_response(sync::HTTPStatus::Ok, {{"access_token", encode_fake_jwt("access token 2")}}),
                    std::move(completion));
            }
        }
        else if (request.url.find("/location") != std::string::npos) {
            CHECK(request.method == HttpMethod::get);
            CHECK(state.get() == TestState::unknown);
            state.advance_to(TestState::location);
            mock_transport_worker.add_work_item(
                make_location_response("http://localhost:9090", "ws://localhost:9090"), std::move(completion));
        }
    };
    TestSyncManager sync_manager(get_config(transport));
    auto app = sync_manager.app();

    {
        auto [cur_user_promise, cur_user_future] = util::make_promise_future<std::shared_ptr<SyncUser>>();
        app->log_in_with_credentials(AppCredentials::anonymous(),
                                     [promise = std::move(cur_user_promise)](std::shared_ptr<SyncUser> user,
                                                                             util::Optional<AppError> error) mutable {
                                         REQUIRE_FALSE(error);
                                         promise.emplace_value(std::move(user));
                                     });

        auto cur_user = std::move(cur_user_future).get();
        CHECK(cur_user);

        SyncTestFile config(app->current_user(), bson::Bson("foo"));
        // Ignore websocket errors, since sometimes a websocket connection gets started during the test
        config.sync_config->error_handler = [](std::shared_ptr<SyncSession> session, SyncError error) mutable {
            // Ignore websocket errors, since there's not really an app out there...
            if (error.reason().find("Bad WebSocket") != std::string::npos ||
                error.reason().find("Connection Failed") != std::string::npos ||
                error.reason().find("user has been removed") != std::string::npos) {
                util::format(std::cerr,
                             "An expected possible WebSocket error was caught during test: 'app destroyed during "
                             "token refresh': '%1' for '%2'",
                             error.what(), session->path());
            }
            else {
                std::string err_msg(util::format("An unexpected sync error was caught during test: 'app destroyed "
                                                 "during token refresh': '%1' for '%2'",
                                                 error.what(), session->path()));
                std::cerr << err_msg << std::endl;
                throw std::runtime_error(err_msg);
            }
        };
        auto r = Realm::get_shared_realm(config);
        auto session = r->sync_session();
        mock_transport_worker.add_work_item([session] {
            session->initiate_access_token_refresh();
        });
    }
    for (const auto& user : app->all_users()) {
        user->log_out();
    }

    timed_wait_for([&] {
        return !app->sync_manager()->has_existing_sessions();
    });

    mock_transport_worker.mark_complete();
}

TEST_CASE("app: metadata is persisted between sessions", "[sync][app][local]") {
    const auto test_hostname = "proto://host:1234";
    const auto test_ws_hostname = "wsproto://host:1234";

    auto transport = std::make_shared<UnitTestTransport>();
    transport->custom_request_hook =
        [&test_hostname, &test_ws_hostname](const app::Request& request) -> std::optional<app::Response> {
        if (request.url.find("/location") != std::string::npos) {
            CHECK(request.method == HttpMethod::get);
            return make_location_response(test_hostname, test_ws_hostname, "LOCAL", "IE");
        }
        else if (request.url.find("functions/call") != std::string::npos) {
            REQUIRE(request.url.rfind(test_hostname, 0) != std::string::npos);
            return make_ok_response();
        }
        return std::nullopt;
    };

    TestSyncManager::Config config = get_config(transport);
    config.base_path = util::make_temp_dir();
    config.should_teardown_test_directory = false;

    {
        TestSyncManager sync_manager(config, {});
        auto app = sync_manager.app();
        app->log_in_with_credentials(AppCredentials::anonymous(), [](auto, auto error) {
            REQUIRE_FALSE(error);
        });
        REQUIRE(app->sync_manager()->sync_route().rfind(test_ws_hostname, 0) != std::string::npos);
    }

    App::clear_cached_apps();
    config.override_sync_route = false;
    config.should_teardown_test_directory = true;
    {
        TestSyncManager sync_manager(config);
        auto app = sync_manager.app();
        REQUIRE(app->sync_manager()->sync_route().rfind(test_ws_hostname, 0) != std::string::npos);
        app->call_function("function", {}, [](auto error, auto) {
            REQUIRE_FALSE(error);
        });
    }
}

TEST_CASE("app: make_streaming_request", "[sync][app][local]") {
    UnitTestTransport::access_token = good_access_token;

    constexpr uint64_t timeout_ms = 60000;
    auto config = get_config();
    config.app_config.default_request_timeout_ms = timeout_ms;
    TestSyncManager tsm(config);
    auto app = tsm.app();

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

TEST_CASE("app: sync_user_profile unit tests", "[sync][app][local]") {
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

#if 0
TEST_CASE("app: app cannot get deallocated during log in", "[sync][app][local]") {
    AsyncMockNetworkTransport mock_transport_worker;
    enum class TestState { unknown, location, login, app_deallocated, profile };
    struct TestStateBundle<TestState> state;
    auto transport = std::make_shared<LocalTransport>();
    transport->send_hook = [&state, &mock_transport_worker](const Request& request, util::UniqueFunction<void(const Response&)>&& completion) {
        if (request.url.find("/login") != std::string::npos) {
            state.advance_to(TestState::login);
            state.wait_for(TestState::app_deallocated);
            mock_transport_worker.add_work_item(
                make_json_response(sync::HTTPStatus::Ok, user_json(encode_fake_jwt("access token"))),
                std::move(completion));
        }
        else if (request.url.find("/profile") != std::string::npos) {
            state.advance_to(TestState::profile);
            mock_transport_worker.add_work_item(
                make_json_response(sync::HTTPStatus::Ok, user_profile_json()),
                std::move(completion));
        }
        else if (request.url.find("/location") != std::string::npos) {
            CHECK(request.method == HttpMethod::get);
            state.advance_to(TestState::location);
            mock_transport_worker.add_work_item(
                make_location_response("http://localhost:9090", "ws://localhost:9090"),
                std::move(completion));
        }
    };

    auto [cur_user_promise, cur_user_future] = util::make_promise_future<std::shared_ptr<SyncUser>>();
    {
        TestSyncManager sync_manager(get_config(transport));
        auto app = sync_manager.app();

        app->log_in_with_credentials(AppCredentials::anonymous(),
                                     [promise = std::move(cur_user_promise)](std::shared_ptr<SyncUser> user,
                                                                             util::Optional<AppError> error) mutable {
                                         REQUIRE_FALSE(error);
                                         promise.emplace_value(std::move(user));
                                     });
    }

    // At this point the test does not hold any reference to `app`.
    state.advance_to(TestState::app_deallocated);
    auto cur_user = std::move(cur_user_future).get();
    CHECK(cur_user);

    mock_transport_worker.mark_complete();
}
#endif

TEST_CASE("app: user logs out while profile is fetched", "[sync][app][local][user]") {
    AsyncMockNetworkTransport mock_transport_worker;
    enum class TestState { unknown, location, login, profile };
    struct TestStateBundle<TestState> state;
    std::shared_ptr<SyncUser> logged_in_user;

    auto transport = std::make_shared<LocalTransport>();
    transport->send_hook = [&state, &mock_transport_worker, &logged_in_user](
                               const Request& request, util::UniqueFunction<void(const Response&)>&& completion) {
        if (request.url.find("/login") != std::string::npos) {
            state.advance_to(TestState::login);
            mock_transport_worker.add_work_item(
                make_json_response(sync::HTTPStatus::Ok, user_json(encode_fake_jwt("access token"))),
                std::move(completion));
        }
        else if (request.url.find("/profile") != std::string::npos) {
            logged_in_user->log_out();
            state.advance_to(TestState::profile);
            mock_transport_worker.add_work_item(make_json_response(sync::HTTPStatus::Ok, user_profile_json()),
                                                std::move(completion));
        }
        else if (request.url.find("/location") != std::string::npos) {
            CHECK(request.method == HttpMethod::get);
            state.advance_to(TestState::location);
            mock_transport_worker.add_work_item(
                make_location_response("http://localhost:9090", "ws://localhost:9090"), std::move(completion));
        }
    };

    TestSyncManager sync_manager(get_config(transport));
    auto app = sync_manager.app();

    logged_in_user = app->sync_manager()->get_user(UnitTestTransport::user_id, good_access_token, good_access_token,
                                                   "anon-user", dummy_device_id);
    auto custom_credentials = AppCredentials::facebook("a_token");
    auto [cur_user_promise, cur_user_future] = util::make_promise_future<std::shared_ptr<SyncUser>>();

    app->link_user(logged_in_user, custom_credentials,
                   [promise = std::move(cur_user_promise)](std::shared_ptr<SyncUser> user,
                                                           util::Optional<AppError> error) mutable {
                       REQUIRE_FALSE(error);
                       promise.emplace_value(std::move(user));
                   });

    auto cur_user = std::move(cur_user_future).get();
    CHECK(state.get() == TestState::profile);
    CHECK(cur_user);
    CHECK(cur_user == logged_in_user);

    mock_transport_worker.mark_complete();
}

TEST_CASE("app: shared instances", "[sync][app][local]") {
    App::Config base_config;
    set_app_config_defaults(base_config, instance_of<UnitTestTransport>);

    SyncClientConfig sync_config;
    sync_config.metadata_mode = SyncClientConfig::MetadataMode::NoMetadata;
    sync_config.base_file_path = util::make_temp_dir() + random_string(10);
    util::try_make_dir(sync_config.base_file_path);

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
    auto app1_1 = App::get_shared_app(config1, sync_config);
    auto app1_2 = App::get_shared_app(config1, sync_config);
    auto app1_3 = App::get_cached_app(config1.app_id, config1.base_url);
    auto app1_4 = App::get_shared_app(config2, sync_config);
    auto app1_5 = App::get_cached_app(config1.app_id);

    CHECK(app1_1 == app1_2);
    CHECK(app1_1 == app1_3);
    CHECK(app1_1 == app1_4);
    CHECK(app1_1 == app1_5);

    // config3 and config4 should point to different apps
    auto app2_1 = App::get_shared_app(config3, sync_config);
    auto app2_2 = App::get_cached_app(config3.app_id, config3.base_url);
    auto app2_3 = App::get_shared_app(config4, sync_config);
    auto app2_4 = App::get_cached_app(config3.app_id);
    auto app2_5 = App::get_cached_app(config4.app_id, "https://some.different.url");

    CHECK(app2_1 == app2_2);
    CHECK(app2_1 != app2_3);
    CHECK(app2_4 != nullptr);
    CHECK(app2_5 == nullptr);

    CHECK(app1_1 != app2_1);
    CHECK(app1_1 != app2_3);
    CHECK(app1_1 != app2_4);
}
