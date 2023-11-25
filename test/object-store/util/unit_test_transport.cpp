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

#include "util/unit_test_transport.hpp"

#include <realm/object-store/util/uuid.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/app_utils.hpp>
#include <realm/util/platform_info.hpp>

#include <catch2/catch_all.hpp>

using namespace realm;
using namespace realm::app;

std::string UnitTestTransport::access_token =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJleHAiOjE1ODE1MDc3OTYsImlhdCI6MTU4MTUwNTk5NiwiaXNzIjoiNWU0M2RkY2M2MzZlZTEwNmVhYTEyYmRjIiwic3RpdGNoX2RldklkIjoi"
    "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwIiwic3RpdGNoX2RvbWFpbklkIjoiNWUxNDk5MTNjOTBiNGFmMGViZTkzNTI3Iiwic3ViIjoiNWU0M2Rk"
    "Y2M2MzZlZTEwNmVhYTEyYmRhIiwidHlwIjoiYWNjZXNzIn0.0q3y9KpFxEnbmRwahvjWU1v9y1T1s3r2eozu93vMc3s";
const std::string UnitTestTransport::api_key = "lVRPQVYBJSIbGos2ZZn0mGaIq1SIOsGaZ5lrcp8bxlR5jg4OGuGwQq1GkektNQ3i";
const std::string UnitTestTransport::api_key_id = "5e5e6f0abe4ae2a2c2c2d329";
const std::string UnitTestTransport::api_key_name = "some_api_key_name";
const std::string UnitTestTransport::auth_route = "https://mongodb.com/unittests";
const std::string UnitTestTransport::identity_0_id = "Ursus arctos isabellinus";
const std::string UnitTestTransport::identity_1_id = "Ursus arctos horribilis";

UnitTestTransport::UnitTestTransport(const std::string& provider_type, uint64_t request_timeout)
    : m_provider_type(provider_type)
    , m_request_timeout(request_timeout)
    , m_options({{"device",
                  {{"appId", "app_id"},
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
                   {"bundleId", "Bundle Id"}}}})
{
}

void UnitTestTransport::handle_profile(const Request& request,
                                       util::UniqueFunction<void(const Response&)>&& completion)
{
    CHECK(request.method == HttpMethod::get);
    auto content_type = AppUtils::find_header("Content-Type", request.headers);
    CHECK(content_type);
    CHECK(content_type->second == "application/json;charset=utf-8");
    auto authorization = AppUtils::find_header("Authorization", request.headers);
    CHECK(authorization);
    CHECK(authorization->second == "Bearer " + access_token);
    CHECK(request.body.empty());
    CHECK(request.timeout_ms == m_request_timeout);

    std::string user_id = util::uuid_string();
    std::string response;
    if (m_provider_type == IdentityProviderAnonymous) {
        response = nlohmann::json({{"user_id", user_id},
                                   {"identities", {{{"id", identity_0_id}, {"provider_type", m_provider_type}}}},
                                   {"data", m_user_profile}})
                       .dump();
    }
    else {
        response = nlohmann::json({{"user_id", user_id},
                                   {"identities",
                                    {{{"id", identity_0_id}, {"provider_type", m_provider_type}},
                                     {{"id", identity_1_id}, {"provider_type", "lol_wut"}}}},
                                   {"data", m_user_profile}})
                       .dump();
    }

    completion(Response{200, 0, {}, response});
}

void UnitTestTransport::handle_login(const Request& request, util::UniqueFunction<void(const Response&)>&& completion)
{
    CHECK(request.method == HttpMethod::post);
    auto item = AppUtils::find_header("Content-Type", request.headers);
    CHECK(item);
    CHECK(item->second == "application/json;charset=utf-8");
    CHECK(nlohmann::json::parse(request.body)["options"] == m_options);

    CHECK(request.timeout_ms == m_request_timeout);

    std::string response = nlohmann::json({{"access_token", access_token},
                                           {"refresh_token", access_token},
                                           {"user_id", util::uuid_string()},
                                           {"device_id", "Panda Bear"}})
                               .dump();

    completion(Response{200, 0, {}, response});
}

void UnitTestTransport::handle_location(const Request& request,
                                        util::UniqueFunction<void(const Response&)>&& completion)
{
    CHECK(request.method == HttpMethod::get);
    CHECK(request.timeout_ms == m_request_timeout);

    std::string response = nlohmann::json({{"deployment_model", "this"},
                                           {"hostname", "field"},
                                           {"ws_hostname", "shouldn't"},
                                           {"location", "matter"}})
                               .dump();

    completion(Response{200, 0, {}, response});
}

void UnitTestTransport::handle_create_api_key(const Request& request,
                                              util::UniqueFunction<void(const Response&)>&& completion)
{
    CHECK(request.method == HttpMethod::post);
    auto item = AppUtils::find_header("Content-Type", request.headers);
    CHECK(item);
    CHECK(item->second == "application/json;charset=utf-8");
    CHECK(nlohmann::json::parse(request.body) == nlohmann::json({{"name", api_key_name}}));
    CHECK(request.timeout_ms == m_request_timeout);

    std::string response =
        nlohmann::json({{"_id", api_key_id}, {"key", api_key}, {"name", api_key_name}, {"disabled", false}}).dump();

    completion(Response{200, 0, {}, response});
}

void UnitTestTransport::handle_fetch_api_key(const Request& request,
                                             util::UniqueFunction<void(const Response&)>&& completion)
{
    CHECK(request.method == HttpMethod::get);
    auto item = AppUtils::find_header("Content-Type", request.headers);
    CHECK(item);
    CHECK(item->second == "application/json;charset=utf-8");

    CHECK(request.body == "");
    CHECK(request.timeout_ms == m_request_timeout);

    std::string response = nlohmann::json({{"_id", api_key_id}, {"name", api_key_name}, {"disabled", false}}).dump();

    completion(Response{200, 0, {}, response});
}

void UnitTestTransport::handle_fetch_api_keys(const Request& request,
                                              util::UniqueFunction<void(const Response&)>&& completion)
{
    CHECK(request.method == HttpMethod::get);
    auto item = AppUtils::find_header("Content-Type", request.headers);
    CHECK(item);
    CHECK(item->second == "application/json;charset=utf-8");

    CHECK(request.body == "");
    CHECK(request.timeout_ms == m_request_timeout);

    auto elements = std::vector<nlohmann::json>();
    for (int i = 0; i < 2; i++) {
        elements.push_back({{"_id", api_key_id}, {"name", api_key_name}, {"disabled", false}});
    }

    completion(Response{200, 0, {}, nlohmann::json(elements).dump()});
}

void UnitTestTransport::handle_token_refresh(const Request& request,
                                             util::UniqueFunction<void(const Response&)>&& completion)
{
    CHECK(request.method == HttpMethod::post);
    auto item = AppUtils::find_header("Content-Type", request.headers);
    CHECK(item);
    CHECK(item->second == "application/json;charset=utf-8");

    CHECK(request.body == "");
    CHECK(request.timeout_ms == m_request_timeout);

    auto elements = std::vector<nlohmann::json>();
    nlohmann::json json{{"access_token", access_token}};

    completion(Response{200, 0, {}, json.dump()});
}

void UnitTestTransport::send_request_to_server(const Request& request,
                                               util::UniqueFunction<void(const Response&)>&& completion)
{
    if (request.url.find("/login") != std::string::npos) {
        handle_login(request, std::move(completion));
    }
    else if (request.url.find("/profile") != std::string::npos) {
        handle_profile(request, std::move(completion));
    }
    else if (request.url.find("/session") != std::string::npos && request.method != HttpMethod::post) {
        completion(Response{200, 0, {}, ""});
    }
    else if (request.url.find("/api_keys") != std::string::npos && request.method == HttpMethod::post) {
        handle_create_api_key(request, std::move(completion));
    }
    else if (request.url.find(util::format("/api_keys/%1", api_key_id)) != std::string::npos &&
             request.method == HttpMethod::get) {
        handle_fetch_api_key(request, std::move(completion));
    }
    else if (request.url.find("/api_keys") != std::string::npos && request.method == HttpMethod::get) {
        handle_fetch_api_keys(request, std::move(completion));
    }
    else if (request.url.find("/session") != std::string::npos && request.method == HttpMethod::post) {
        handle_token_refresh(request, std::move(completion));
    }
    else if (request.url.find("/location") != std::string::npos && request.method == HttpMethod::get) {
        handle_location(request, std::move(completion));
    }
    else {
        completion(Response{200, 0, {}, "something arbitrary"});
    }
}
