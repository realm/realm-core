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

#include <curl/curl.h>
#include <sstream>
#include "../../external/json/json.hpp"
#include "util/test_utils.hpp"
#include "util/test_file.hpp"
#include "catch2/catch.hpp"

#include "sync/app.hpp"
#include "sync/app_credentials.hpp"

#pragma mark - Integration Tests

// temporarily disable these tests for now,
// but allow opt-in by building with REALM_ENABLE_AUTH_TESTS=1
#ifndef REALM_ENABLE_AUTH_TESTS
#define REALM_ENABLE_AUTH_TESTS 0
#endif

#if REALM_ENABLE_AUTH_TESTS

using namespace realm;
using namespace realm::app;

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

            /* Perform the request, res will get the return code */
            response_code = curl_easy_perform(curl);

            double cl;
            curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
            /* Check for errors */
            if(response_code != CURLE_OK)
                fprintf(stderr, "curl_easy_perform() failed: %s\n",
                        curl_easy_strerror(response_code));

            /* always cleanup */
            curl_easy_cleanup(curl);
            curl_slist_free_all(list); /* free the list again */
            int binding_response_code = 0;
            completion_block(Response{response_code, binding_response_code, {/*headers*/}, response});
        }
        curl_global_cleanup();
    }
};

TEST_CASE("app: login_with_credentials integration", "[sync][app]") {

    SECTION("login") {
        std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
            return std::unique_ptr<GenericNetworkTransport>(new IntTestTransport);
        };
        GenericNetworkTransport::set_network_transport_factory(factory);

        // TODO: create dummy app using Stitch CLI instead of hardcording
        auto app = App("translate-utwuv", realm::util::none);

        bool processed = false;

        static const std::string base_path = realm::tmp_dir();

        auto tsm = TestSyncManager(base_path);

        app.login_with_credentials(AppCredentials::anonymous(),
                                   [&](std::shared_ptr<SyncUser> user, std::unique_ptr<realm::app::error::AppError> error) {
            CHECK(user);
            CHECK(!error);
            processed = true;
        });

        CHECK(processed);
    }
}



#pragma mark - Unit Tests

class UnitTestTransport : public GenericNetworkTransport {

public:
    static std::string access_token;
    static const std::string user_id;
    static const std::string identity_0_id;
    static const std::string identity_1_id;
    static const nlohmann::json profile_0;
    static const nlohmann::json profile_1;

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

        completion_block(Response { .http_status_code = 200, .custom_status_code = 0, .headers = {}, .body = response });

    }

public:
    void send_request_to_server(const Request request, std::function<void (const Response)> completion_block) override
    {
        if (request.url.find("/login") != std::string::npos) {
            handle_login(request, completion_block);
        } else if (request.url.find("/profile") != std::string::npos) {
            handle_profile(request, completion_block);
        }
    }
};

static const std::string good_access_token =  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1ODE1MDc3OTYsImlhdCI6MTU4MTUwNTk5NiwiaXNzIjoiNWU0M2RkY2M2MzZlZTEwNmVhYTEyYmRjIiwic3RpdGNoX2RldklkIjoiMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwIiwic3RpdGNoX2RvbWFpbklkIjoiNWUxNDk5MTNjOTBiNGFmMGViZTkzNTI3Iiwic3ViIjoiNWU0M2RkY2M2MzZlZTEwNmVhYTEyYmRhIiwidHlwIjoiYWNjZXNzIn0.0q3y9KpFxEnbmRwahvjWU1v9y1T1s3r2eozu93vMc3s";

std::string UnitTestTransport::access_token = good_access_token;

static const std::string bad_access_token = "lolwut";

const std::string UnitTestTransport::user_id = "Ailuropoda melanoleuca";
const std::string UnitTestTransport::identity_0_id = "Ursus arctos isabellinus";
const std::string UnitTestTransport::identity_1_id = "Ursus arctos horribilis";

static const std::string profile_0_first_name = "Ursus americanus";
static const std::string profile_0_last_name = "Ursus boeckhi";
static const std::string profile_0_email = "Ursus ursinus";
static const std::string profile_0_picture_url = "Ursus malayanus";
static const std::string profile_0_gender = "Ursus thibetanus";
static const std::string profile_0_birthday = "Ursus americanus";
static const std::string profile_0_min_age = "Ursus maritimus";
static const std::string profile_0_max_age = "Ursus arctos";

const nlohmann::json UnitTestTransport::profile_0 = {
    {"first_name", profile_0_first_name},
    {"last_name", profile_0_last_name},
    {"email", profile_0_email},
    {"picture_url", profile_0_picture_url},
    {"gender", profile_0_gender},
    {"birthday", profile_0_birthday},
    {"min_age", profile_0_min_age},
    {"max_age", profile_0_max_age}
};

const nlohmann::json UnitTestTransport::profile_1 = {
};

TEST_CASE("app: login_with_credentials unit_tests", "[sync][app]") {
    std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
        return std::unique_ptr<GenericNetworkTransport>(new UnitTestTransport);
    };
    GenericNetworkTransport::set_network_transport_factory(factory);

    auto app = App::app("<>", realm::util::none);

    SECTION("login_anonymous good") {
        UnitTestTransport::access_token = good_access_token;

        bool processed = false;

        app->login_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, std::unique_ptr<realm::app::error::AppError> error) {
            CHECK(user);
            CHECK(!error);

            CHECK(user->identities().size() == 2);
            CHECK(user->identities()[0].id == UnitTestTransport::identity_0_id);
            CHECK(user->identities()[1].id == UnitTestTransport::identity_1_id);
            CHECK(user->user_profile()->first_name() == profile_0_first_name);
            CHECK(user->user_profile()->last_name() == profile_0_last_name);
            CHECK(user->user_profile()->email() == profile_0_email);
            CHECK(user->user_profile()->picture_url() == profile_0_picture_url);
            CHECK(user->user_profile()->gender() == profile_0_gender);
            CHECK(user->user_profile()->birthday() == profile_0_birthday);
            CHECK(user->user_profile()->min_age() == profile_0_min_age);
            CHECK(user->user_profile()->max_age() == profile_0_max_age);

            processed = true;
        });

        CHECK(processed);
    }

    SECTION("login_anonymous bad") {
        UnitTestTransport::access_token = bad_access_token;

        bool processed = false;

        app->login_with_credentials(AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, std::unique_ptr<realm::app::error::AppError> error) {
            CHECK(!user);
            CHECK(error);
            CHECK(error->what() == std::string("Bad Token"));
            CHECK(error->type == realm::app::error::AppError::Type::JSON);
            // knowing the type, we can expect a dynamic cast to succeed
            auto specialized_error = dynamic_cast<realm::app::error::JSONError*>(error.get());
            REQUIRE(specialized_error);
            CHECK(specialized_error->code == realm::app::error::JSONErrorCode::bad_token);
            processed = true;
        });

        CHECK(processed);
    }
}

#endif // REALM_ENABLE_AUTH_TESTS
