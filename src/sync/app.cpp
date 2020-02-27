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
#include <sstream>

// wrap an optional json key into the Optional type
#define WRAP_JSON_OPT(JSON, KEY, RET_TYPE) \
JSON.find(KEY) != JSON.end() ? Optional<RET_TYPE>(JSON[KEY].get<RET_TYPE>()) : realm::util::none

namespace realm {
namespace app {

const static std::string default_base_url = "https://stitch.mongodb.com";
const static std::string default_base_path = "/api/client/v2.0";
const static std::string default_app_path = "/app";
const static std::string default_auth_path = "/auth";
const static uint64_t    default_timeout_ms = 60000;

App::App(const std::string& app_id, const realm::util::Optional<App::Config>& config)
    : m_app_id(app_id)
    {
    std::string base_url;
    if (config) {
        base_url = config->base_url.value_or(default_base_url);
        m_request_timeout_ms = config->default_request_timeout_ms.value_or(default_timeout_ms);

        if (config->transport) {
            // TODO: Install custom transport
        }
    }
    else {
        base_url = default_base_url;
        m_request_timeout_ms = default_timeout_ms;
    }

    m_base_route = base_url + default_base_path;
    m_app_route = m_base_route + default_app_path + "/" + app_id;
    m_auth_route = m_app_route + default_auth_path;
}

static std::unique_ptr<error::AppError> handle_error(const Response& response) {
    if (response.body.empty()) {
        return {};
    }

    if (auto ct = response.headers.find("Content-Type"); ct != response.headers.end() && ct->second == "application/json") {
        auto body = nlohmann::json::parse(response.body);
        return std::make_unique<error::AppError>(app::error::ServiceError(body["errorCode"].get<std::string>(),
                              body["error"].get<std::string>()));
    }

    if (response.custom_status_code != 0) {
        return std::make_unique<error::AppError>(response.body,
                                                 response.custom_status_code,
                                                 error::AppError::Type::Custom);
    }

    return {};
}

inline bool response_code_is_fatal(Response const& response) {
    // FIXME: our tests currently only generate codes 0 and 200,
    // but we need more robust error handling here; eg. should a 300
    // redirect really be considered fatal or should we automatically redirect?
    return response.http_status_code >= 300
        || (response.http_status_code < 200 && response.http_status_code != 0)
        || response.custom_status_code != 0;
}

void App::login_with_credentials(const std::shared_ptr<AppCredentials> credentials,
                                 std::function<void(std::shared_ptr<SyncUser>, std::unique_ptr<error::AppError>)> completion_block) {
    // construct the route
    std::string route = util::format("%1/providers/%2/login", m_auth_route, provider_type_from_enum(credentials->m_provider));

    auto handler = [&](const Response& response) {
        // if there is a already an error code, pass the error upstream
        if (response_code_is_fatal(response)) { // FIXME: handle
            return completion_block(nullptr, handle_error(response));
        }

        nlohmann::json json;
        try {
            json = nlohmann::json::parse(response.body);
        } catch(std::domain_error e) {
            return completion_block(nullptr, std::make_unique<error::AppError>(app::error::JSONError(app::error::JSONErrorCode::malformed_json,
                                                                                                       e.what())));
        }

        std::shared_ptr<realm::SyncUser> sync_user;
        try {
            realm::SyncUserIdentifier identifier {
                HAS_JSON_KEY_OR_THROW(json, "user_id", std::string),
                m_auth_route
            };

            sync_user = realm::SyncManager::shared().get_user(identifier,
                                                              HAS_JSON_KEY_OR_THROW(json, "refresh_token", std::string),
                                                              HAS_JSON_KEY_OR_THROW(json, "access_token", std::string));
        } catch (const error::JSONError& err) {
            return completion_block(nullptr, std::make_unique<error::JSONError>(err));
        } catch (const error::ServiceError& err) {
            return completion_block(nullptr, std::make_unique<error::ServiceError>(err));
        } catch (const error::AppError& err) {
            return completion_block(nullptr, std::make_unique<error::AppError>(err));
        }

        std::string profile_route = util::format("%1/auth/profile", m_base_route);
        std::string bearer = util::format("Bearer %1", sync_user->access_token());

        GenericNetworkTransport::get()->send_request_to_server({
            HttpMethod::get,
            profile_route,
            m_request_timeout_ms,
            {
                { "Content-Type", "application/json;charset=utf-8" },
                { "Accept", "application/json" },
                { "Authorization", bearer}
            },
            std::string()
        }, [&](const Response& profile_response) {
            // if there is a already an error code, pass the error upstream
            if (response_code_is_fatal(profile_response)) {
                return completion_block(nullptr, handle_error(profile_response));
            }

            try {
                json = nlohmann::json::parse(profile_response.body);
            } catch(std::domain_error e) {
                return completion_block(nullptr, std::make_unique<error::AppError>(app::error::JSONError(app::error::JSONErrorCode::malformed_json,
                                                                                                           e.what())));
            }

            try {
                std::vector<SyncUserIdentity> identities;
                nlohmann::json identities_json = HAS_JSON_KEY_OR_THROW(json, "identities", nlohmann::json);

                for (size_t i = 0; i < identities_json.size(); i++)
                {
                    auto identity_json = identities_json[i];
                    identities.push_back(SyncUserIdentity(HAS_JSON_KEY_OR_THROW(identity_json, "id", std::string),
                                                          HAS_JSON_KEY_OR_THROW(identity_json, "provider_type", std::string)));
                }

                sync_user->update_identities(identities);

                auto profile_data = HAS_JSON_KEY_OR_THROW(json, "data", nlohmann::json);

                sync_user->update_user_profile(SyncUserProfile(WRAP_JSON_OPT(profile_data, "name", std::string),
                                                               WRAP_JSON_OPT(profile_data, "email", std::string),
                                                               WRAP_JSON_OPT(profile_data, "picture_url", std::string),
                                                               WRAP_JSON_OPT(profile_data, "first_name", std::string),
                                                               WRAP_JSON_OPT(profile_data, "last_name", std::string),
                                                               WRAP_JSON_OPT(profile_data, "gender", std::string),
                                                               WRAP_JSON_OPT(profile_data, "birthday", std::string),
                                                               WRAP_JSON_OPT(profile_data, "min_age", std::string),
                                                               WRAP_JSON_OPT(profile_data, "max_age", std::string)));
            }  catch (const error::JSONError& err) {
                return completion_block(nullptr, std::make_unique<error::JSONError>(err));
            } catch (const error::ServiceError& err) {
                return completion_block(nullptr, std::make_unique<error::ServiceError>(err));
            } catch (const error::AppError& err) {
                return completion_block(nullptr, std::make_unique<error::AppError>(err));
            }

            return completion_block(sync_user, {});
        });
    };

    std::map<std::string, std::string> headers = {
        { "Content-Type", "application/json;charset=utf-8" },
        { "Accept", "application/json" }
    };

    GenericNetworkTransport::get()->send_request_to_server({
        HttpMethod::post,
        route,
        m_request_timeout_ms,
        headers,
        credentials->serialize_as_json()
    }, handler);
}

} // namespace app
} // namespace realm
