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

// MARK: - Helpers
// wrap an optional json key into the Optional type
template <typename T>
Optional<T> get_optional(const nlohmann::json& json, const std::string& key)
{
    auto it = json.find(key);
    return it != json.end() ? Optional<T>(it->get<T>()) : realm::util::none;
}

static std::map<std::string, std::string> get_request_headers(bool authenticated)
{
    std::map<std::string, std::string> headers {
        { "Content-Type", "application/json;charset=utf-8" },
        { "Accept", "application/json" }
    };

    if (authenticated) {
        auto user = SyncManager::shared().get_current_user();
        REALM_ASSERT(user);
        headers.insert({ "Authorization",
            util::format("Bearer %1", user->refresh_token()) });
    }
    
    return headers;
}

const static std::string default_base_url = "https://stitch.mongodb.com";
const static std::string default_base_path = "/api/client/v2.0";
const static std::string default_app_path = "/app";
const static std::string default_auth_path = "/auth";
const static uint64_t    default_timeout_ms = 60000;
const static std::string username_password_provider_key = "local-userpass";
const static std::string user_api_key_provider_key = "api_keys";

App::App(const Config& config)
: m_config(config)
, m_base_route(config.base_url.value_or(default_base_url) + default_base_path)
, m_app_route(m_base_route + default_app_path + "/" + config.app_id)
, m_auth_route(m_app_route + default_auth_path)
, m_request_timeout_ms(config.default_request_timeout_ms.value_or(default_timeout_ms))
{
    REALM_ASSERT(m_config.transport_generator);
}

static Optional<AppError> check_for_errors(const Response& response)
{
    bool http_status_code_is_fatal = response.http_status_code >= 300 ||
        (response.http_status_code < 200 && response.http_status_code != 0);

    try {
        auto ct = response.headers.find("Content-Type");
        if (ct != response.headers.end() && ct->second == "application/json") {
            auto body = nlohmann::json::parse(response.body);
            auto message = body.find("error");
            if (auto error_code = body.find("error_code"); error_code != body.end() &&
                !error_code->get<std::string>().empty())
            {
                return AppError(make_error_code(service_error_code_from_string(body["error_code"].get<std::string>())),
                                message != body.end() ? message->get<std::string>() : "no error message");
            } else if (message != body.end()) {
                return AppError(make_error_code(ServiceErrorCode::unknown), message->get<std::string>());
            }
        }
    } catch (const std::exception&) {
        // ignore parse errors from our attempt to read the error from json
    }

    if (response.custom_status_code != 0) {
        std::string error_msg = (!response.body.empty()) ? response.body : "non-zero custom status code considered fatal";
        return AppError(make_custom_error_code(response.custom_status_code), error_msg);
    }

    if (http_status_code_is_fatal)
    {
        return AppError(make_http_error_code(response.http_status_code), "http error code considered fatal");
    }

    return {};
}

static void handle_default_response(const Response& response,
                                    std::function<void (Optional<AppError>)> completion_block)
{
    if (auto error = check_for_errors(response)) {
        return completion_block(error);
    } else {
        return completion_block({});
    }
};

//MARK: - Template specializations

template<>
App::UsernamePasswordProviderClient App::provider_client <App::UsernamePasswordProviderClient> ()
{
    return App::UsernamePasswordProviderClient(this);
}
template<>
App::UserAPIKeyProviderClient App::provider_client <App::UserAPIKeyProviderClient>()
{
    return App::UserAPIKeyProviderClient(this);
}

// MARK: - UsernamePasswordProviderClient

void App::UsernamePasswordProviderClient::register_email(const std::string &email,
                                                         const std::string &password,
                                                         std::function<void (Optional<AppError>)> completion_block)
{
    
    REALM_ASSERT(parent);
    std::string route = util::format("%1/providers/%2/register", parent->m_auth_route, username_password_provider_key);

    auto handler = [completion_block](const Response& response) {
        handle_default_response(response, completion_block);
    };

    nlohmann::json body = {
        { "email", email },
        { "password", password }
    };
    
    parent->m_config.transport_generator()->send_request_to_server({
        HttpMethod::post,
        route,
        parent->m_request_timeout_ms,
        get_request_headers(false),
        body.dump()
    }, handler);
}

void App::UsernamePasswordProviderClient::confirm_user(const std::string& token,
                                                             const std::string& token_id,
                                                             std::function<void(Optional<AppError>)> completion_block)
{
    REALM_ASSERT(parent);
    std::string route = util::format("%1/providers/%2/confirm", parent->m_auth_route, username_password_provider_key);

    auto handler = [completion_block](const Response& response) {
        handle_default_response(response, completion_block);
    };

    nlohmann::json body = {
        { "token", token },
        { "tokenId", token_id }
    };
    
    parent->m_config.transport_generator()->send_request_to_server({
        HttpMethod::post,
        route,
        parent->m_request_timeout_ms,
        get_request_headers(false),
        body.dump()
    }, handler);
}

void App::UsernamePasswordProviderClient::resend_confirmation_email(const std::string& email,
                                                                    std::function<void(Optional<AppError>)> completion_block)
{
    REALM_ASSERT(parent);
    std::string route = util::format("%1/providers/%2/confirm/send", parent->m_auth_route, username_password_provider_key);
    
    auto handler = [completion_block](const Response& response) {
        handle_default_response(response, completion_block);
    };
    
    nlohmann::json body {
        { "email", email }
    };

    parent->m_config.transport_generator()->send_request_to_server({
        HttpMethod::post,
        route,
        parent->m_request_timeout_ms,
        get_request_headers(false),
        body.dump()
    }, handler);
}

void App::UsernamePasswordProviderClient::send_reset_password_email(const std::string& email,
                                                                    std::function<void(Optional<AppError>)> completion_block)
{
    REALM_ASSERT(parent);
    std::string route = util::format("%1/providers/%2/reset/send", parent->m_auth_route, username_password_provider_key);

    auto handler = [completion_block](const Response& response) {
        handle_default_response(response, completion_block);
    };

    nlohmann::json body = {
        { "email", email }
    };

    parent->m_config.transport_generator()->send_request_to_server({
        HttpMethod::post,
        route,
        parent->m_request_timeout_ms,
        get_request_headers(false),
        body.dump()
    }, handler);
}

void App::UsernamePasswordProviderClient::reset_password(const std::string& password,
                                                         const std::string& token,
                                                         const std::string& token_id,
                                                         std::function<void(Optional<AppError>)> completion_block)
{
    REALM_ASSERT(parent);
    std::string route = util::format("%1/providers/%2/reset", parent->m_auth_route, username_password_provider_key);
    
    auto handler = [completion_block](const Response& response) {
        handle_default_response(response, completion_block);
    };
    
    nlohmann::json body = {
        { "password", password },
        { "token", token },
        { "token_id", token_id }
    };

    parent->m_config.transport_generator()->send_request_to_server({
        HttpMethod::post,
        route,
        parent->m_request_timeout_ms,
        get_request_headers(false),
        body.dump()
    }, handler);
}

void App::UsernamePasswordProviderClient::call_reset_password_function(const std::string& email,
                                                                       const std::string& password,
                                                                       const std::string& args,
                                                                       std::function<void(Optional<AppError>)> completion_block)
{
    REALM_ASSERT(parent);
    std::string route = util::format("%1/providers/%2/reset/call", parent->m_auth_route, username_password_provider_key);

    auto handler = [completion_block](const Response& response) {
        handle_default_response(response, completion_block);
    };

    nlohmann::json body = {
        { "email", email },
        { "password", password },
        { "arguments", nlohmann::json::parse(args) },
    };

    parent->m_config.transport_generator()->send_request_to_server({
        HttpMethod::post,
        route,
        parent->m_request_timeout_ms,
        get_request_headers(false),
        body.dump()
    }, handler);
}

// MARK: - UserAPIKeyProviderClient

 void App::UserAPIKeyProviderClient::create_api_key(const std::string &name,
                                                   std::function<void (Optional<UserAPIKey>, Optional<AppError>)> completion_block)
{
    REALM_ASSERT(parent);
    std::string route = util::format("%1/auth/%2", parent->m_base_route, user_api_key_provider_key);

    auto handler = [completion_block](const Response& response) {

        if (auto error = check_for_errors(response)) {
            return completion_block({}, error);
        }

        nlohmann::json json;
        try {
            json = nlohmann::json::parse(response.body);
        } catch (const std::exception& e) {
            return completion_block({}, AppError(make_error_code(JSONErrorCode::malformed_json), e.what()));
        }

        try {
            auto user_api_key = App::UserAPIKey {
                    ObjectId(value_from_json<std::string>(json, "_id").c_str()),
                    get_optional<std::string>(json, "key"),
                    value_from_json<std::string>(json, "name"),
                    value_from_json<bool>(json, "disabled")
                };
            return completion_block(user_api_key, {});
        } catch (const std::exception& e) {
            return completion_block({}, AppError(make_error_code(JSONErrorCode::malformed_json), e.what()));
        }
    };

    nlohmann::json body = {
        { "name", name }
    };

    parent->m_config.transport_generator()->send_request_to_server({
        HttpMethod::post,
        route,
        parent->m_request_timeout_ms,
        get_request_headers(true),
        body.dump()
    }, handler);
}

void App::UserAPIKeyProviderClient::fetch_api_key(const realm::ObjectId& id,
                                                   std::function<void (Optional<UserAPIKey>, Optional<AppError>)> completion_block)
{
    REALM_ASSERT(parent);
    std::string route = util::format("%1/auth/%2/%3", parent->m_base_route, user_api_key_provider_key, id.to_string());

    auto handler = [completion_block](const Response& response) {

        if (auto error = check_for_errors(response)) {
            return completion_block({}, error);
        }

        nlohmann::json json;
        try {
            json = nlohmann::json::parse(response.body);
        } catch (const std::exception& e) {
            return completion_block({}, AppError(make_error_code(JSONErrorCode::malformed_json), e.what()));
        }

        try {
            auto user_api_key = App::UserAPIKey {
                    ObjectId(value_from_json<std::string>(json, "_id").c_str()),
                    get_optional<std::string>(json, "key"),
                    value_from_json<std::string>(json, "name"),
                    value_from_json<bool>(json, "disabled")
                };
            return completion_block(user_api_key, {});
        } catch (const std::exception& e) {
            return completion_block({}, AppError(make_error_code(JSONErrorCode::malformed_json), e.what()));
        }
    };

    parent->m_config.transport_generator()->send_request_to_server({
        HttpMethod::get,
        route,
        parent->m_request_timeout_ms,
        get_request_headers(true),
    }, handler);
}

void App::UserAPIKeyProviderClient::fetch_api_keys(std::function<void(std::vector<UserAPIKey>, Optional<AppError>)> completion_block)
{
    
    REALM_ASSERT(parent);
    std::string route = util::format("%1/auth/%2", parent->m_base_route, user_api_key_provider_key);
    
    auto handler = [completion_block](const Response& response) {

        if (auto error = check_for_errors(response)) {
            return completion_block(std::vector<UserAPIKey>(), error);
        }
                    
        nlohmann::json json;
        try {
            json = nlohmann::json::parse(response.body);
        } catch (const std::exception& e) {
            return completion_block(std::vector<UserAPIKey>(), AppError(make_error_code(JSONErrorCode::malformed_json), e.what()));
        }

        try {
            auto api_key_array = std::vector<UserAPIKey>();
            auto json_array = std::vector<nlohmann::json>(json);
            for (nlohmann::json& api_key_json : json_array) {
                api_key_array.push_back(
                    App::UserAPIKey {
                        ObjectId(value_from_json<std::string>(api_key_json, "_id").c_str()),
                        get_optional<std::string>(api_key_json, "key"),
                        value_from_json<std::string>(api_key_json, "name"),
                        value_from_json<bool>(api_key_json, "disabled")
                });
            }
            return completion_block(api_key_array, {});
        } catch (const std::exception& e) {
            return completion_block(std::vector<UserAPIKey>(), AppError(make_error_code(JSONErrorCode::malformed_json), e.what()));
        }
    };

    parent->m_config.transport_generator()->send_request_to_server({
        HttpMethod::get,
        route,
        parent->m_request_timeout_ms,
        get_request_headers(true),
    }, handler);
}


void App::UserAPIKeyProviderClient::delete_api_key(const UserAPIKey& api_key,
                                                   std::function<void(Optional<AppError>)> completion_block)
{
    REALM_ASSERT(parent);
    std::string route = util::format("%1/auth/%2/%3", parent->m_base_route, user_api_key_provider_key, api_key.id.to_string());

    auto handler = [completion_block](const Response& response) {
        if (auto error = check_for_errors(response)) {
            return completion_block(error);
        } else {
            return completion_block({});
        }
    };
    
    parent->m_config.transport_generator()->send_request_to_server({
        HttpMethod::del,
        route,
        parent->m_request_timeout_ms,
        get_request_headers(true),
    }, handler);
}

void App::UserAPIKeyProviderClient::enable_api_key(const UserAPIKey& api_key,
                                                   std::function<void(Optional<AppError> error)> completion_block)
{
    REALM_ASSERT(parent);
    std::string route = util::format("%1/auth/%2/%3/enable", parent->m_base_route, user_api_key_provider_key, api_key.id.to_string());

    auto handler = [completion_block](const Response& response) {
        if (auto error = check_for_errors(response)) {
            return completion_block(error);
        } else {
            return completion_block({});
        }
    };
    
    parent->m_config.transport_generator()->send_request_to_server({
        HttpMethod::put,
        route,
        parent->m_request_timeout_ms,
        get_request_headers(true),
    }, handler);
}

void App::UserAPIKeyProviderClient::disable_api_key(const UserAPIKey& api_key,
                                                   std::function<void(Optional<AppError> error)> completion_block)
{
    REALM_ASSERT(parent);
    std::string route = util::format("%1/auth/%2/%3/disable", parent->m_base_route, user_api_key_provider_key, api_key.id.to_string());

    auto handler = [completion_block](const Response& response) {
        if (auto error = check_for_errors(response)) {
            return completion_block(error);
        } else {
            return completion_block({});
        }
    };
    
    parent->m_config.transport_generator()->send_request_to_server({
        HttpMethod::put,
        route,
        parent->m_request_timeout_ms,
        get_request_headers(true),
    }, handler);
}

// MARK: - App

std::shared_ptr<SyncUser> App::current_user() const {
    return SyncManager::shared().get_current_user();
}

std::vector<std::shared_ptr<SyncUser>> App::all_users() const {
    return SyncManager::shared().all_users();
}

void App::log_in_with_credentials(const AppCredentials& credentials,
                                  std::function<void(std::shared_ptr<SyncUser>, Optional<AppError>)> completion_block) const {
    // construct the route
    std::string route = util::format("%1/providers/%2/login", m_auth_route, credentials.provider_as_string());

    auto handler = [completion_block, credentials, this](const Response& response) {
        if (auto error = check_for_errors(response)) {
            return completion_block(nullptr, error);
        }

        nlohmann::json json;
        try {
            json = nlohmann::json::parse(response.body);
        } catch (const std::exception& e) {
            return completion_block(nullptr, AppError(make_error_code(JSONErrorCode::malformed_json), e.what()));
        }

        std::shared_ptr<realm::SyncUser> sync_user;
        try {
            sync_user = realm::SyncManager::shared().get_user(value_from_json<std::string>(json, "user_id"),
                                                              value_from_json<std::string>(json, "refresh_token"),
                                                              value_from_json<std::string>(json, "access_token"),
                                                              credentials.provider_as_string());
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
                { "Authorization", bearer }
            },
            std::string()
        }, [completion_block, sync_user](const Response& profile_response) {
            if (auto error = check_for_errors(profile_response)) {
                return completion_block(nullptr, error);
            }

            nlohmann::json profile_json;
            try {
                profile_json = nlohmann::json::parse(profile_response.body);
            } catch (const std::domain_error& e) {
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

                sync_user->set_state(SyncUser::State::Active);
                SyncManager::shared().set_current_user(sync_user->identity());
            } catch (const AppError& err) {
                return completion_block(nullptr, err);
            }

            return completion_block(sync_user, {});
        });
    };

    m_config.transport_generator()->send_request_to_server({
        HttpMethod::post,
        route,
        m_request_timeout_ms,
        get_request_headers(false),
        credentials.serialize_as_json()
    }, handler);
}

void App::log_out(std::shared_ptr<SyncUser> user, std::function<void (Optional<AppError>)> completion_block) const
{
    if (!user || user->state() != SyncUser::State::Active) {
        return completion_block(util::none);
    }
    std::string bearer = util::format("Bearer %1", current_user()->refresh_token());
    user->log_out();

    auto handler = [completion_block, user](const Response& response) {
        if (auto error = check_for_errors(response)) {
            return completion_block(error);
        }
        return completion_block(util::none);
    };

    std::string route = util::format("%1/auth/session", m_base_route);

    m_config.transport_generator()->send_request_to_server({
        HttpMethod::del,
        route,
        m_request_timeout_ms,
        {
            { "Content-Type", "application/json;charset=utf-8" },
            { "Accept", "application/json" },
            { "Authorization", bearer }
        },
        ""
    }, handler);
}

void App::log_out(std::function<void (Optional<AppError>)> completion_block) const {
    log_out(current_user(), completion_block);
}

} // namespace app
} // namespace realm
