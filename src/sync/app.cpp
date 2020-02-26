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

#include "app.hpp"
#include <sstream>
#include "sync_manager.hpp"
#include "generic_network_transport.hpp"
#include "app_credentials.hpp"
#include "../external/json/json.hpp"

// wrap an optional json key into the Optional type
#define WRAP_JSON_OPT(JSON, KEY, RET_TYPE) \
JSON.find(KEY) != JSON.end() ? Optional<RET_TYPE>(JSON[KEY].get<RET_TYPE>()) : realm::util::none

namespace realm {
namespace app {

std::shared_ptr<App> App::app(const std::string app_id,
                              const realm::util::Optional<App::Config> config)
{
    return std::make_shared<App>(app_id, config);
}

const std::string App::default_base_url = "https://stitch.mongodb.com";
const std::string App::base_path = "/api/client/v2.0";
const std::string App::app_path = "/app";
const std::string App::auth_path = "/auth";
const uint64_t App::default_timeout_ms = 60000;

static std::unique_ptr<error::AppError> handle_error(const Response response) {
    if (response.body.empty()) {
        return {};
    }

    if (response.headers.find("Content-Type") != response.headers.end()) {
        if (response.headers.at("Content-Type") == "application/json") {
            auto body = nlohmann::json::parse(response.body);
            return std::make_unique<error::AppError>(app::error::ServiceError(body["errorCode"].get<std::string>(),
                                  body["error"].get<std::string>()));
        }
    }

    if (response.custom_status_code != 0) {
        return std::make_unique<error::AppError>(response.body,
                                                 response.custom_status_code,
                                                 error::AppError::Type::Custom);
    }

    return {};
}

inline bool response_code_is_fatal(Response response) {
    // FIXME: our tests currently only generate codes 0 and 200,
    // but we need more robust error handling here; eg. should a 300
    // redirect really be considered fatal or should we automatically redirect?
    return response.http_status_code >= 300 || response.http_status_code < 200 || response.custom_status_code != 0;
}

void App::login_with_credentials(const std::shared_ptr<AppCredentials> credentials,
                                 std::function<void(std::shared_ptr<SyncUser>, std::unique_ptr<error::AppError>)> completion_block) {
    // construct the route
    std::string route = util::format("%1/providers/%2/login", m_auth_route, provider_type_from_enum(credentials->m_provider));

    auto handler = [&](const Response response) {
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
        }, [&](const Response profile_response) {
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

                sync_user->update_user_profile(std::make_shared<SyncUserProfile>(WRAP_JSON_OPT(profile_data, "name", std::string),
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
