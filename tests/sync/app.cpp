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

#include "catch2/catch.hpp"
#include "sync/app.hpp"
#include "sync/app_credentials.hpp"
#include "util/test_utils.hpp"
#include "util/test_file.hpp"

#include <curl/curl.h>
#include <json.hpp>

// temporarily disable these tests for now,
// but allow opt-in by building with REALM_ENABLE_AUTH_TESTS=1
#ifndef REALM_ENABLE_AUTH_TESTS
#define REALM_ENABLE_AUTH_TESTS 0
#endif


using namespace realm;
using namespace realm::app;

#if REALM_ENABLE_AUTH_TESTS

class IntTestTransport : public GenericNetworkTransport {
    static size_t write(char *ptr, size_t size, size_t nmemb, std::string* data) {
        size_t realsize = size * nmemb;
        data->append(ptr, realsize); // FIXME: throws std::bad_alloc when out of memory
        return realsize;
    }

    void send_request_to_server(const Request request, std::function<void (const Response)> completion_block) override
    {
        CURL *curl;
        CURLcode response_code;
        std::string response;
        std::string content_type;
        std::map<std::string, std::string> response_headers;
        std::string response_headers_raw;

        curl_global_init(CURL_GLOBAL_ALL);
        /* get a curl handle */
        curl = curl_easy_init();

        struct curl_slist *list = NULL;

        if (curl) {
            /* First set the URL that is about to receive our POST. This URL can
             just as well be a https:// URL if that is what should receive the
             data. */
            curl_easy_setopt(curl, CURLOPT_URL, request.url.c_str());

            /* Now specify the POST data */
            if (request.method == HttpMethod::post) {
                curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.body.c_str());
            } else if (request.method == HttpMethod::put) {
                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
            } else if (request.method == HttpMethod::del) {
                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
            }
                        
            curl_easy_setopt(curl, CURLOPT_TIMEOUT, request.timeout_ms);

            for (auto header : request.headers)
            {
                std::stringstream h;
                h << header.first << ": " << header.second;
                list = curl_slist_append(list, h.str().data());
            }
            
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
            curl_easy_setopt (curl, CURLOPT_HEADERDATA, &response_headers_raw);

            /* Perform the request, res will get the return code */
            response_code = curl_easy_perform(curl);
            int http_code = 0;
            curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &http_code);
            
            // Build response header map from raw string
            auto split_header_strings = split_string(response_headers_raw);
            for (auto &header_element : split_header_strings) {
                // Maybe use regex instead?
                if (header_element == "content-type: application/json") {
                    response_headers.insert({"Content-Type", "application/json"});
                }
            }
            
            double cl;
            curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
            /* Check for errors */
            if(response_code != CURLE_OK)
                fprintf(stderr, "curl_easy_perform() failed: %s\n",
                        curl_easy_strerror(response_code));
            
            /* always cleanup */
            curl_easy_cleanup(curl);
            curl_slist_free_all(list); /* free the list again */
            completion_block(Response{http_code, http_code, response_headers, response});
        }
        
        curl_global_cleanup();
    }
    
    std::vector<std::string> split_string(std::string string) {
        std::stringstream ss(string);
        std::string item;
        std::vector<std::string> split_strings;
        while (std::getline(ss, item, '\r'))
        {
            item.erase(std::remove(item.begin(),
                                   item.end(), '\n'),
                                   item.end());
            split_strings.push_back(item);
        }
        return split_strings;
    }
};

static std::string random_string(std::string::size_type length)
{
  static auto& chrs = "0123456789"
    "bcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  thread_local static std::mt19937 rg{std::random_device{}()};
  thread_local static std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(chrs) - 2);
  std::string s;
  s.reserve(length);
  while(length--)
    s += chrs[pick(rg)];
  return s;
}

TEST_CASE("app: login_with_credentials integration", "[sync][app]") {

    SECTION("login") {
        std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
            return std::unique_ptr<GenericNetworkTransport>(new IntTestTransport);
        };

        // TODO: create dummy app using Stitch CLI instead of hardcording
        auto app = App(App::Config{"translate-utwuv", factory});

        bool processed = false;

        app.login_with_credentials(AppCredentials::anonymous(),
                                   [&](std::shared_ptr<SyncUser> user, Optional<app::AppError> error) {
            CHECK(user);
            CHECK(!error);
            processed = true;
        });

        CHECK(processed);
    }
}

// MARK: - UsernamePasswordProviderClient Tests

TEST_CASE("app: UsernamePasswordProviderClient integration", "[sync][app]") {

    auto email = util::format("%1@%2.com", random_string(10), random_string(10));
    auto password = random_string(10);
    
    std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
        return std::unique_ptr<GenericNetworkTransport>(new IntTestTransport);
    };
    
    auto config = App::Config{"translate-utwuv", factory};
    auto app = App(config);
    std::string base_path = tmp_dir() + "/" + config.app_id;
    reset_test_directory(base_path);
    TestSyncManager init_sync_manager(base_path);
    
    bool processed = false;

    SECTION("register email") {
        app.provider_client<App::UsernamePasswordProviderClient>()
            .register_email("username@10gen.com",
                            "M0ng0@B2020",
                            [&](Optional<app::AppError> error) {
                // Error returned states the account has already been created
                CHECK(error->message == "name already in use");
                CHECK(error->error_code.value() == 49);
        });
        
        app.provider_client<App::UsernamePasswordProviderClient>()
            .register_email(email,
                            password,
                            [&](Optional<app::AppError> error) {
                // Error returned states the account has already been created
                CHECK(!error);
                processed = true;
        });
    }

    SECTION("confirm user") {
        app.provider_client<App::UsernamePasswordProviderClient>()
            .confirm_user("a_token",
                          "a_token_id",
                          [&](Optional<app::AppError> error) {
                CHECK(error->message == "invalid token data");
                processed = true;
        });

    }

    SECTION("resend confirmation email") {
        app.provider_client<App::UsernamePasswordProviderClient>()
            .resend_confirmation_email("username@10gen.com",
                                       [&](Optional<app::AppError> error) {
                CHECK(error->message == "already confirmed");
                processed = true;
        });
    }
    
    SECTION("send reset password") {
        app.provider_client<App::UsernamePasswordProviderClient>()
            .send_reset_password_email("username@10gen.com",
                                       [&](Optional<app::AppError> error) {
                CHECK(!error);
        });

        app.provider_client<App::UsernamePasswordProviderClient>()
            .send_reset_password_email(email,
                                       [&](Optional<app::AppError> error) {
                CHECK(error->message == "user not found");
                processed = true;
        });
    }
    
    SECTION("reset password") {
        app.provider_client<App::UsernamePasswordProviderClient>()
            .reset_password(password,
                            "token_sample",
                            "token_id_sample",
                            [&](Optional<app::AppError> error) {
                CHECK(error->message == "invalid token data");
                processed = true;
        });
    }
    
    SECTION("call reset password function") {
        app.provider_client<App::UsernamePasswordProviderClient>()
            .call_reset_password_function(email,
                                          password,
                                          "[0,1]",
                                          [&](Optional<app::AppError> error) {
                CHECK(error->message == "user not found");
                processed = true;
        });
    }

    CHECK(processed);
}

// MARK: - UserAPIKeyProviderClient Tests

TEST_CASE("app: UserAPIKeyProviderClient integration", "[sync][app]") {
        
    auto email = util::format("%1@%2.com", random_string(15), random_string(15));
    auto password = util::format("%1", random_string(15));
    auto api_key_name = util::format("%1", random_string(15));

    std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
        return std::unique_ptr<GenericNetworkTransport>(new IntTestTransport);
    };

    auto config = App::Config{"translate-utwuv", factory};
    auto app = App(config);
    std::string base_path = tmp_dir() + "/" + config.app_id;
    reset_test_directory(base_path);
    TestSyncManager init_sync_manager(base_path);
    
    bool processed = false;

    app.provider_client<App::UsernamePasswordProviderClient>().register_email(email,
                                                                              password,
                                                                              [&] (Optional<AppError> error) {
        CHECK(!error);
    });
    
    app.login_with_credentials(AppCredentials::username_password(email, password),
                           [&](std::shared_ptr<SyncUser> user, Optional<app::AppError> error) {
        CHECK(user);
        CHECK(!error);
    });

    App::UserAPIKey api_key;
    
    SECTION("api-key") {
        
        app.provider_client<App::UserAPIKeyProviderClient>()
            .create_api_key(api_key_name, [&](Optional<App::UserAPIKey> user_api_key, Optional<app::AppError> error) {
                CHECK(user_api_key->name == api_key_name);
                CHECK(user_api_key->id.to_string() == user_api_key->id.to_string());
                CHECK(!error);
                api_key = user_api_key.value();
        });
        
        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_key(api_key.id, [&](Optional<App::UserAPIKey> user_api_key, Optional<app::AppError> error) {
                CHECK(user_api_key->name == api_key_name);
                CHECK(user_api_key->id.to_string() == user_api_key->id.to_string());
                CHECK(!error);
        });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_keys([&](std::vector<App::UserAPIKey> api_keys, Optional<AppError> error) {
                CHECK(api_keys.size() == 1);
                for(auto api_key : api_keys) {
                    CHECK(api_key.id.to_string() == api_key.id.to_string());
                    CHECK(api_key.name == api_key_name);
                }
                CHECK(!error);
        });
        
        app.provider_client<App::UserAPIKeyProviderClient>()
            .enable_api_key(api_key, [&](Optional<AppError> error) {
                CHECK(!error);
        });
        
        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_key(api_key.id, [&](Optional<App::UserAPIKey> user_api_key, Optional<app::AppError> error) {
                CHECK(user_api_key->disabled == false);
                CHECK(user_api_key->name == api_key_name);
                CHECK(user_api_key->id.to_string() == user_api_key->id.to_string());
                CHECK(!error);
        });
        
        app.provider_client<App::UserAPIKeyProviderClient>()
            .disable_api_key(api_key, [&](Optional<AppError> error) {
                CHECK(!error);
        });
        
        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_key(api_key.id, [&](Optional<App::UserAPIKey> user_api_key, Optional<app::AppError> error) {
                CHECK(user_api_key->disabled == true);
                CHECK(user_api_key->name == api_key_name);
                CHECK(user_api_key->id.to_string() == user_api_key->id.to_string());
                CHECK(!error);
        });
        
        app.provider_client<App::UserAPIKeyProviderClient>()
            .delete_api_key(api_key, [&](Optional<AppError> error) {
                CHECK(!error);
        });
        
        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_key(api_key.id, [&](Optional<App::UserAPIKey> user_api_key, Optional<app::AppError> error) {
                CHECK(!user_api_key);
                CHECK(error);
                processed = true;
        });
        
        CHECK(processed);
    }
}

#endif // REALM_ENABLE_AUTH_TESTS

// MARK: - Unit Tests

class UnitTestTransport : public GenericNetworkTransport {

public:
    static std::string access_token;
    static const std::string api_key;
    static const std::string api_key_id;
    static const std::string api_key_name;
    static const std::string auth_route;
    static const std::string user_id;
    static const std::string identity_0_id;
    static const std::string identity_1_id;
    static const nlohmann::json profile_0;

private:
    void handle_profile(const Request request,
                        std::function<void (Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::get);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");
        CHECK(request.headers.at("Authorization") == "Bearer " + access_token);
        CHECK(request.body.empty());
        CHECK(request.timeout_ms == 60000);

        std::string response = nlohmann::json({
            {"user_id", user_id},
            {"identities", {
                {
                    {"id", identity_0_id},
                    {"provider_type", "anon-user"},
                    {"provider_id", "lol"}
                },
                {
                    {"id", identity_1_id},
                    {"provider_type", "lol_wut"},
                    {"provider_id", "nah_dawg"}
                }
            }},
            {"data", profile_0}
        }).dump();

        completion_block(Response{.http_status_code = 200, .custom_status_code = 0, .headers = {}, .body = response});
    }

    void handle_login(const Request request,
                      std::function<void (Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::post);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");

        CHECK(nlohmann::json::parse(request.body) == nlohmann::json({{"provider", "anon-user"}}));
        CHECK(request.timeout_ms == 60000);

        std::string response = nlohmann::json({
            {"access_token", access_token},
            {"refresh_token", access_token},
            {"user_id", "Brown Bear"},
            {"device_id", "Panda Bear"}}).dump();
        
        try {
            realm::SyncUserIdentifier identifier {
                "Brown Bear",
                "some_auth_route"
            };

            realm::SyncManager::shared().get_user(identifier,
                                                  access_token,
                                                  access_token);
            
        } catch (const AppError& err) {
            return completion_block({});
        }
        completion_block(Response { .http_status_code = 200,
                                    .custom_status_code = 0,
                                    .headers = {},
                                    .body = response });
    }
    
    void handle_create_api_key(const Request request,
                      std::function<void (Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::post);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");

        CHECK(nlohmann::json::parse(request.body) == nlohmann::json({{"name", api_key_name}}));
        CHECK(request.timeout_ms == 60000);

        std::string response = nlohmann::json({
            {"_id", api_key_id},
            {"key", api_key},
            {"name", api_key_name},
            {"disabled", false}}).dump();

        completion_block(Response { .http_status_code = 200,
                                    .custom_status_code = 0,
                                    .headers = {},
                                    .body = response });
    }
    
    void handle_fetch_api_key(const Request request,
                      std::function<void (Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::get);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");

        CHECK(request.body == "");
        CHECK(request.timeout_ms == 60000);
        
        std::string response = nlohmann::json({
            {"_id", api_key_id},
            {"name", api_key_name},
            {"disabled", false}}).dump();
        
        completion_block(Response { .http_status_code = 200,
                                    .custom_status_code = 0,
                                    .headers = {},
                                    .body = response });
    }
    
    void handle_fetch_api_keys(const Request request,
                      std::function<void (Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::get);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");

        CHECK(request.body == "");
        CHECK(request.timeout_ms == 60000);
        
        auto elements = std::vector<nlohmann::json>();
        for (int i = 0; i < 2; i++) {
            elements.push_back({
                {"_id", api_key_id},
                {"name", api_key_name},
                {"disabled", false}});
        }
        
        completion_block(Response { .http_status_code = 200,
                                    .custom_status_code = 0,
                                    .headers = {},
                                    .body = nlohmann::json(elements).dump() });

        completion_block(Response { .http_status_code = 200,
                                    .custom_status_code = 0,
                                    .headers = {},
                                    .body = nlohmann::json(elements).dump() });
    }

public:
    void send_request_to_server(const Request request, std::function<void (const Response)> completion_block) override
    {
        if (request.url.find("/login") != std::string::npos) {
            handle_login(request, completion_block);
        } else if (request.url.find("/profile") != std::string::npos) {
            handle_profile(request, completion_block);
        } else if (request.url.find("/api_keys") != std::string::npos && request.method == HttpMethod::post) {
            handle_create_api_key(request, completion_block);
        } else if (request.url.find(util::format("/api_keys/%1", api_key_id)) != std::string::npos && request.method == HttpMethod::get) {
            handle_fetch_api_key(request, completion_block);
        } else if (request.url.find("/api_keys") != std::string::npos && request.method == HttpMethod::get) {
            handle_fetch_api_keys(request, completion_block);
        }
    }
};

static const std::string good_access_token =  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1ODE1MDc3OTYsImlhdCI6MTU4MTUwNTk5NiwiaXNzIjoiNWU0M2RkY2M2MzZlZTEwNmVhYTEyYmRjIiwic3RpdGNoX2RldklkIjoiMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwIiwic3RpdGNoX2RvbWFpbklkIjoiNWUxNDk5MTNjOTBiNGFmMGViZTkzNTI3Iiwic3ViIjoiNWU0M2RkY2M2MzZlZTEwNmVhYTEyYmRhIiwidHlwIjoiYWNjZXNzIn0.0q3y9KpFxEnbmRwahvjWU1v9y1T1s3r2eozu93vMc3s";

std::string UnitTestTransport::access_token = good_access_token;

static const std::string bad_access_token = "lolwut";

const std::string UnitTestTransport::api_key = "lVRPQVYBJSIbGos2ZZn0mGaIq1SIOsGaZ5lrcp8bxlR5jg4OGuGwQq1GkektNQ3i";
const std::string UnitTestTransport::api_key_id = "5e5e6f0abe4ae2a2c2c2d329";
const std::string UnitTestTransport::api_key_name = "some_api_key_name";
const std::string UnitTestTransport::auth_route = "https://mongodb.com/unittests";
const std::string UnitTestTransport::user_id = "Ailuropoda melanoleuca";
const std::string UnitTestTransport::identity_0_id = "Ursus arctos isabellinus";
const std::string UnitTestTransport::identity_1_id = "Ursus arctos horribilis";

static const std::string profile_0_name = "Ursus americanus Ursus boeckhi";
static const std::string profile_0_first_name = "Ursus americanus";
static const std::string profile_0_last_name = "Ursus boeckhi";
static const std::string profile_0_email = "Ursus ursinus";
static const std::string profile_0_picture_url = "Ursus malayanus";
static const std::string profile_0_gender = "Ursus thibetanus";
static const std::string profile_0_birthday = "Ursus americanus";
static const std::string profile_0_min_age = "Ursus maritimus";
static const std::string profile_0_max_age = "Ursus arctos";

const nlohmann::json UnitTestTransport::profile_0 = {
    {"name", profile_0_name},
    {"first_name", profile_0_first_name},
    {"last_name", profile_0_last_name},
    {"email", profile_0_email},
    {"picture_url", profile_0_picture_url},
    {"gender", profile_0_gender},
    {"birthday", profile_0_birthday},
    {"min_age", profile_0_min_age},
    {"max_age", profile_0_max_age}
};

//FIXME: - Broken test
/*
TEST_CASE("app: login_with_credentials unit_tests", "[sync][app]") {
    std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
        return std::unique_ptr<GenericNetworkTransport>(new UnitTestTransport);
    };
    App app(App::Config{"<>", factory});

    SECTION("login_anonymous good") {
        UnitTestTransport::access_token = good_access_token;

        bool processed = false;

        app.login_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(user);
            CHECK(!error);

            CHECK(user->identities().size() == 2);
            CHECK(user->identities()[0].id == UnitTestTransport::identity_0_id);
            CHECK(user->identities()[1].id == UnitTestTransport::identity_1_id);
            SyncUserProfile user_profile = user->user_profile();

            CHECK(user_profile.name == profile_0_name);
            CHECK(user_profile.first_name == profile_0_first_name);
            CHECK(user_profile.last_name == profile_0_last_name);
            CHECK(user_profile.email == profile_0_email);
            CHECK(user_profile.picture_url == profile_0_picture_url);
            CHECK(user_profile.gender == profile_0_gender);
            CHECK(user_profile.birthday == profile_0_birthday);
            CHECK(user_profile.min_age == profile_0_min_age);
            CHECK(user_profile.max_age == profile_0_max_age);

            processed = true;
        });

        CHECK(processed);
    }

    SECTION("login_anonymous bad") {
        UnitTestTransport::access_token = bad_access_token;

        bool processed = false;

        app.login_with_credentials(AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(!user);
            CHECK(error);
            CHECK(error->message == std::string("jwt missing parts"));
            CHECK(error->error_code.message() == "bad token");
            CHECK(error->error_code.category() == app::json_error_category());
            CHECK(error->is_json_error());
            CHECK(app::JSONErrorCode(error->error_code.value()) == app::JSONErrorCode::bad_token);
            processed = true;
        });

        CHECK(processed);
    }
}
*/

TEST_CASE("app: UserAPIKeyProviderClient unit_tests", "[sync][app]") {
    
    auto setup_user = []() {
        if (realm::SyncManager::shared().get_current_user()) {
            return;
        }
        realm::SyncUserIdentifier identifier {
            UnitTestTransport::user_id,
            UnitTestTransport::auth_route
        };
        realm::SyncManager::shared().get_user(identifier,
                                              good_access_token,
                                              good_access_token);
    };
    
    std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
        return std::unique_ptr<GenericNetworkTransport>(new UnitTestTransport);
    };
    
    auto config = App::Config{"translate-utwuv", factory};
    auto app = App(config);
    std::string base_path = tmp_dir() + "/" + config.app_id;
    reset_test_directory(base_path);
    TestSyncManager init_sync_manager(base_path);
    
    bool processed = false;
    ObjectId obj_id(UnitTestTransport::api_key_id.c_str());

    SECTION("create api key") {
        setup_user();
        app.provider_client<App::UserAPIKeyProviderClient>().create_api_key(UnitTestTransport::api_key_name,
                                                                            [&](Optional<App::UserAPIKey> user_api_key, Optional<AppError> error) {
            CHECK(!error);
            CHECK(user_api_key->disabled == false);
            CHECK(user_api_key->id.to_string() == UnitTestTransport::api_key_id);
            CHECK(user_api_key->key == UnitTestTransport::api_key);
            CHECK(user_api_key->name == UnitTestTransport::api_key_name);
        });        
    }
    
    SECTION("fetch api key") {
        setup_user();
        app.provider_client<App::UserAPIKeyProviderClient>().fetch_api_key(obj_id,
                                                                           [&](Optional<App::UserAPIKey> user_api_key, Optional<AppError> error) {
            CHECK(!error);
            CHECK(user_api_key->disabled == false);
            CHECK(user_api_key->id.to_string() == UnitTestTransport::api_key_id);
            CHECK(user_api_key->name == UnitTestTransport::api_key_name);
        });
    }
    
    SECTION("fetch api keys") {
        setup_user();
        app.provider_client<App::UserAPIKeyProviderClient>().fetch_api_keys([&](std::vector<App::UserAPIKey> user_api_keys, Optional<AppError> error) {
            CHECK(!error);
            CHECK(user_api_keys.size() == 2);
            for(auto user_api_key : user_api_keys) {
                CHECK(user_api_key.disabled == false);
                CHECK(user_api_key.id.to_string() == UnitTestTransport::api_key_id);
                CHECK(user_api_key.name == UnitTestTransport::api_key_name);
            }
            processed = true;
        });
        CHECK(processed);
    }
}

struct ErrorCheckingTransport : public GenericNetworkTransport {
    ErrorCheckingTransport(Response r)
    : m_response(r)
    {
    }
    void send_request_to_server(const Request, std::function<void (const Response)> completion_block) override
    {
        completion_block(m_response);
    }
private:
    Response m_response;
};

//FIXME: - Broken tests
/*
TEST_CASE("app: response error handling", "[sync][app]") {

    std::string response_body = nlohmann::json({
        {"access_token", good_access_token},
        {"refresh_token", good_access_token},
        {"user_id", "Brown Bear"},
        {"device_id", "Panda Bear"}}).dump();

    Response response{.http_status_code = 200, .headers = {{"Content-Type", "application/json"}}, .body = response_body};

    std::function<std::unique_ptr<GenericNetworkTransport>()> transport_generator = [&response] {
        return std::unique_ptr<GenericNetworkTransport>(new ErrorCheckingTransport(response));
    };
    App app(App::Config{"<>", transport_generator});
    bool processed = false;

    SECTION("http 404") {
        response.http_status_code = 404;
        app.login_with_credentials(realm::app::AppCredentials::anonymous(),
                                   [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
                                       CHECK(!user);
                                       CHECK(error);
                                       CHECK(!error->is_json_error());
                                       CHECK(!error->is_custom_error());
                                       CHECK(!error->is_service_error());
                                       CHECK(error->is_http_error());
                                       CHECK(error->error_code.value() == 404);
                                       CHECK(error->message == std::string("http error code considered fatal"));
                                       CHECK(error->error_code.message() == "Client Error: 404");
                                       processed = true;
                                   });
        CHECK(processed);
    }
    SECTION("http 500") {
        response.http_status_code = 500;
        app.login_with_credentials(realm::app::AppCredentials::anonymous(),
                                   [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
                                       CHECK(!user);
                                       CHECK(error);
                                       CHECK(!error->is_json_error());
                                       CHECK(!error->is_custom_error());
                                       CHECK(!error->is_service_error());
                                       CHECK(error->is_http_error());
                                       CHECK(error->error_code.value() == 500);
                                       CHECK(error->message == std::string("http error code considered fatal"));
                                       CHECK(error->error_code.message() == "Server Error: 500");
                                       processed = true;
                                   });
        CHECK(processed);
    }
    SECTION("custom error code") {
        response.custom_status_code = 42;
        app.login_with_credentials(realm::app::AppCredentials::anonymous(),
                                   [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
                                       CHECK(!user);
                                       CHECK(error);
                                       CHECK(!error->is_http_error());
                                       CHECK(!error->is_json_error());
                                       CHECK(!error->is_service_error());
                                       CHECK(error->is_custom_error());
                                       CHECK(error->error_code.value() == 42);
                                       CHECK(error->message == std::string("non-zero custom status code considered fatal"));
                                       CHECK(error->error_code.message() == "code 42");
                                       processed = true;
                                   });
        CHECK(processed);
    }

    SECTION("session error code") {
        response.body = nlohmann::json({
            {"errorCode", "MongoDBError"},
            {"error", "a fake MongoDB error message!"},
            {"access_token", good_access_token},
            {"refresh_token", good_access_token},
            {"user_id", "Brown Bear"},
            {"device_id", "Panda Bear"}}).dump();
        app.login_with_credentials(realm::app::AppCredentials::anonymous(),
                                   [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
                                       CHECK(!user);
                                       CHECK(error);
                                       CHECK(!error->is_http_error());
                                       CHECK(!error->is_json_error());
                                       CHECK(!error->is_custom_error());
                                       CHECK(error->is_service_error());
                                       CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::mongodb_error);
                                       CHECK(error->message == std::string("a fake MongoDB error message!"));
                                       CHECK(error->error_code.message() == "MongoDBError");
                                       processed = true;
                                   });
        CHECK(processed);
    }

    SECTION("json error code") {
        response.body = "this: is not{} a valid json body!";
        app.login_with_credentials(realm::app::AppCredentials::anonymous(),
                                   [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
                                       CHECK(!user);
                                       CHECK(error);
                                       CHECK(!error->is_http_error());
                                       CHECK(error->is_json_error());
                                       CHECK(!error->is_custom_error());
                                       CHECK(!error->is_service_error());
                                       CHECK(app::JSONErrorCode(error->error_code.value()) == app::JSONErrorCode::malformed_json);
                                       CHECK(error->message == std::string("parse error - unexpected 't'"));
                                       CHECK(error->error_code.message() == "malformed json");
                                       processed = true;
                                   });
        CHECK(processed);
    }
}
*/

