////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or utilied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include "sync/app.hpp"

#include "sync/app_credentials.hpp"
#include "sync/generic_network_transport.hpp"
#include "sync/sync_manager.hpp"

#include <json.hpp>

namespace realm {
namespace app {

// wrap an optional json key into the Optional type
template <typename T>
Optional<T> get_optional(const nlohmann::json& json, const std::string& key)
{
    auto it = json.find(key);
    return it != json.end() ? Optional<T>(it->get<T>()) : realm::util::none;
}

const static std::string default_base_url = "https://stitch.mongodb.com";
const static std::string default_base_path = "/api/client/v2.0";
const static std::string default_app_path = "/app";
const static std::string default_auth_path = "/auth";
const static uint64_t    default_timeout_ms = 60000;

App::App(const Config& config)
: m_config(config)
, m_base_route(config.base_url.value_or(default_base_url) + default_base_path)
, m_app_route(m_base_route + default_app_path + "/" + config.app_id)
, m_auth_route(m_app_route + default_auth_path)
, m_request_timeout_ms(config.default_request_timeout_ms.value_or(default_timeout_ms))
{
    REALM_ASSERT(m_config.transport_generator);
}

static Optional<AppError> check_for_errors(const Response& response) {
    try {
        if (auto ct = response.headers.find("Content-Type"); ct != response.headers.end() && ct->second == "application/json") {
            auto body = nlohmann::json::parse(response.body);
            if (auto error_code = body.find("errorCode"); error_code != body.end() && !error_code->get<std::string>().empty()) {
                auto message = body.find("error");
                return AppError(make_error_code(service_error_code_from_string(body["errorCode"].get<std::string>())),
                                message != body.end() ? message->get<std::string>() : "no error message");
            }
        }
    } catch(const std::exception&) {
        // ignore parse errors from our attempt to read the error from json
    }

    if (response.custom_status_code != 0) {
        return AppError(make_custom_error_code(response.custom_status_code), "non-zero custom status code considered fatal");
    }

    // FIXME: our tests currently only generate codes 0 and 200,
    // but we need more robust error handling here; eg. should a 300
    // redirect really be considered fatal or should we automatically redirect?
    if (response.http_status_code >= 300
        || (response.http_status_code < 200 && response.http_status_code != 0))
    {
        return AppError(make_http_error_code(response.http_status_code), "http error code considered fatal");
    }

    return {};
}

void App::login_with_credentials(const AppCredentials& credentials,
                                 std::function<void(std::shared_ptr<SyncUser>, Optional<AppError>)> completion_block) {
    // construct the route
    std::string route = util::format("%1/providers/%2/login", m_auth_route, credentials.provider_as_string());

    auto handler = [completion_block, this](const Response& response) {
        if (auto error = check_for_errors(response)) {
            return completion_block(nullptr, error);
        }

        nlohmann::json json;
        try {
            json = nlohmann::json::parse(response.body);
        } catch(const std::exception& e) {
            return completion_block(nullptr, AppError(make_error_code(JSONErrorCode::malformed_json), e.what()));
        }

        std::shared_ptr<realm::SyncUser> sync_user;
        try {
            realm::SyncUserIdentifier identifier {
                value_from_json<std::string>(json, "user_id"),
                m_auth_route
            };

            sync_user = realm::SyncManager::shared().get_user(identifier,
                                                              value_from_json<std::string>(json, "refresh_token"),
                                                              value_from_json<std::string>(json, "access_token"));
        } catch (const AppError& err) {
            return completion_block(nullptr, err);
        }

        std::string profile_route = util::format("%1/auth/profile", m_base_route);
        std::string bearer = util::format("Bearer %1", sync_user->access_token());

        m_config.transport_generator()->send_request_to_server({
            HttpMethod::get,
            profile_route,
            m_request_timeout_ms,
            {
                { "Content-Type", "application/json;charset=utf-8" },
                { "Accept", "application/json" },
                { "Authorization", bearer}
            },
            std::string()
        }, [completion_block, &sync_user](const Response& profile_response) {
            if (auto error = check_for_errors(profile_response)) {
                return completion_block(nullptr, error);
            }

            nlohmann::json profile_json;
            try {
                profile_json = nlohmann::json::parse(profile_response.body);
            } catch(std::domain_error e) {
                return completion_block(nullptr, AppError(make_error_code(JSONErrorCode::malformed_json), e.what()));
            }

            try {
                std::vector<SyncUserIdentity> identities;
                nlohmann::json identities_json = value_from_json<nlohmann::json>(profile_json, "identities");

                for (size_t i = 0; i < identities_json.size(); i++)
                {
                    auto identity_json = identities_json[i];
                    identities.push_back(SyncUserIdentity(value_from_json<std::string>(identity_json, "id"),
                                                          value_from_json<std::string>(identity_json, "provider_type")));
                }

                sync_user->update_identities(identities);

                auto profile_data = value_from_json<nlohmann::json>(profile_json, "data");

                sync_user->update_user_profile(SyncUserProfile(get_optional<std::string>(profile_data, "name"),
                                                               get_optional<std::string>(profile_data, "email"),
                                                               get_optional<std::string>(profile_data, "picture_url"),
                                                               get_optional<std::string>(profile_data, "first_name"),
                                                               get_optional<std::string>(profile_data, "last_name"),
                                                               get_optional<std::string>(profile_data, "gender"),
                                                               get_optional<std::string>(profile_data, "birthday"),
                                                               get_optional<std::string>(profile_data, "min_age"),
                                                               get_optional<std::string>(profile_data, "max_age")));
            } catch (const AppError& err) {
                return completion_block(nullptr, err);
            }

            return completion_block(sync_user, {});
        });
    };

    std::map<std::string, std::string> headers = {
        { "Content-Type", "application/json;charset=utf-8" },
        { "Accept", "application/json" }
    };

    m_config.transport_generator()->send_request_to_server({
        HttpMethod::post,
        route,
        m_request_timeout_ms,
        headers,
        credentials.serialize_as_json()
    }, handler);
}

} // namespace app
} // namespace realm
