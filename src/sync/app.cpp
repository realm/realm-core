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

#include "realm/util/base64.hpp"
#include "realm/util/uri.hpp"
#include "sync/app_credentials.hpp"
#include "sync/app_utils.hpp"
#include "sync/generic_network_transport.hpp"
#include "sync/impl/sync_client.hpp"
#include "sync/impl/sync_file.hpp"
#include "sync/impl/sync_metadata.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_user.hpp"

#include <json.hpp>
#include <string>

namespace realm {
namespace app {

using util::Optional;

// MARK: - Helpers
// wrap an optional json key into the Optional type
template <typename T>
Optional<T> get_optional(const nlohmann::json& json, const std::string& key)
{
    auto it = json.find(key);
    return it != json.end() ? Optional<T>(it->get<T>()) : realm::util::none;
}

enum class RequestTokenType {
    NoAuth,
    AccessToken,
    RefreshToken
};

// generate the request headers for a HTTP call, by default it will generate headers with a refresh token if a user is passed
static std::map<std::string, std::string> get_request_headers(std::shared_ptr<SyncUser> with_user_authorization = nullptr,
                                                              RequestTokenType token_type = RequestTokenType::RefreshToken)
{
    std::map<std::string, std::string> headers {
        { "Content-Type", "application/json;charset=utf-8" },
        { "Accept", "application/json" }
    };

    if (with_user_authorization) {
        switch (token_type) {
            case RequestTokenType::NoAuth:
                break;
            case RequestTokenType::AccessToken:
                headers.insert({ "Authorization",
                    util::format("Bearer %1", with_user_authorization->access_token())
                });
                break;
            case RequestTokenType::RefreshToken:
                headers.insert({ "Authorization",
                    util::format("Bearer %1", with_user_authorization->refresh_token())
                });
                break;
        }
    }
    return headers;
}

const static std::string default_base_url = "https://realm.mongodb.com";
const static std::string base_path = "/api/client/v2.0";
const static std::string app_path = "/app";
const static std::string auth_path = "/auth";
const static std::string sync_path = "/realm-sync";
const static uint64_t    default_timeout_ms = 60000;
const static std::string username_password_provider_key = "local-userpass";
const static std::string user_api_key_provider_key_path = "api_keys";
static std::unordered_map<std::string, std::shared_ptr<App>> s_apps_cache;
std::mutex s_apps_mutex;

SharedApp App::get_shared_app(const Config& config, const SyncClientConfig& sync_client_config)
{
    std::lock_guard<std::mutex> lock(s_apps_mutex);
    auto& app = s_apps_cache[config.app_id];
    if (!app) {
        app = std::make_shared<App>(config);
        app->configure(sync_client_config);
    }
    return app;
}

std::shared_ptr<App> App::get_cached_app(const std::string& app_id)
{
    std::lock_guard<std::mutex> lock(s_apps_mutex);
    if (auto it = s_apps_cache.find(app_id); it != s_apps_cache.end()) {
        return it->second;
    }

    return nullptr;
}

void App::clear_cached_apps()
{
    std::lock_guard<std::mutex> lock(s_apps_mutex);
    s_apps_cache.clear();
}

App::App(const Config& config)
: m_config(std::move(config))
, m_base_url(config.base_url.value_or(default_base_url))
, m_base_route(m_base_url + base_path)
, m_app_route(m_base_route + app_path + "/" + config.app_id)
, m_auth_route(m_app_route + auth_path)
, m_request_timeout_ms(config.default_request_timeout_ms.value_or(default_timeout_ms))
{
    REALM_ASSERT(m_config.transport_generator);

    if (m_config.platform.empty()) {
        throw std::runtime_error("You must specify the Platform in App::Config");
    }

    if (m_config.platform_version.empty()) {
        throw std::runtime_error("You must specify the Platform Version in App::Config");
    }

    if (m_config.sdk_version.empty()) {
        throw std::runtime_error("You must specify the SDK Version in App::Config");
    }

    // change the scheme in the base url to ws from http to satisfy the sync client
    auto sync_route = m_app_route + sync_path;
    size_t uri_scheme_start = sync_route.find("http");
    if (uri_scheme_start == 0)
        sync_route.replace(uri_scheme_start, 4, "ws");

    m_sync_manager = std::make_shared<SyncManager>();
}

void App::configure(const SyncClientConfig& sync_client_config)
{
    // change the scheme in the base url to ws from http to satisfy the sync client
    auto sync_route = m_app_route + sync_path;
    size_t uri_scheme_start = sync_route.find("http");
    if (uri_scheme_start == 0)
        sync_route.replace(uri_scheme_start, 4, "ws");

    m_sync_manager->configure(shared_from_this(), sync_route, sync_client_config);
    if (auto metadata = m_sync_manager->app_metadata()) {
        m_base_route = metadata->hostname + base_path;
        std::string this_app_path = app_path + "/" + m_config.app_id;
        m_app_route = m_base_route + this_app_path;
        m_auth_route = m_app_route + auth_path;
        m_sync_manager->set_sync_route(metadata->ws_hostname + base_path + this_app_path + sync_path);
    }
}

static void handle_default_response(const Response& response,
                                    std::function<void (Optional<AppError>)> completion_block)
{
    if (auto error = AppUtils::check_for_errors(response)) {
        return completion_block(error);
    } else {
        return completion_block({});
    }
};

//MARK: - Template specializations

template<>
App::UsernamePasswordProviderClient App::provider_client<App::UsernamePasswordProviderClient>()
{
    return App::UsernamePasswordProviderClient(shared_from_this());
}

template<>
App::UserAPIKeyProviderClient App::provider_client<App::UserAPIKeyProviderClient>()
{
    return App::UserAPIKeyProviderClient(*this);
}

// MARK: - UsernamePasswordProviderClient

void App::UsernamePasswordProviderClient::register_email(const std::string &email,
                                                         const std::string &password,
                                                         std::function<void (Optional<AppError>)> completion_block)
{
    REALM_ASSERT(m_parent);
    std::string route = util::format("%1/providers/%2/register", m_parent->m_auth_route, username_password_provider_key);

    auto handler = [completion_block](const Response& response) {
        handle_default_response(response, completion_block);
    };

    nlohmann::json body = {
        { "email", email },
        { "password", password }
    };

    m_parent->do_request(Request {
        HttpMethod::post,
        route,
        m_parent->m_request_timeout_ms,
        get_request_headers(),
        body.dump()
    }, handler);
}

void App::UsernamePasswordProviderClient::confirm_user(const std::string& token,
                                                             const std::string& token_id,
                                                             std::function<void(Optional<AppError>)> completion_block)
{
    REALM_ASSERT(m_parent);
    std::string route = util::format("%1/providers/%2/confirm", m_parent->m_auth_route, username_password_provider_key);

    auto handler = [completion_block](const Response& response) {
        handle_default_response(response, completion_block);
    };

    nlohmann::json body = {
        { "token", token },
        { "tokenId", token_id }
    };

    m_parent->do_request(Request {
        HttpMethod::post,
        route,
        m_parent->m_request_timeout_ms,
        get_request_headers(),
        body.dump()
    }, handler);
}

void App::UsernamePasswordProviderClient::resend_confirmation_email(const std::string& email,
                                                                    std::function<void(Optional<AppError>)> completion_block)
{
    REALM_ASSERT(m_parent);
    std::string route = util::format("%1/providers/%2/confirm/send", m_parent->m_auth_route, username_password_provider_key);

    auto handler = [completion_block](const Response& response) {
        handle_default_response(response, completion_block);
    };

    nlohmann::json body {
        { "email", email }
    };

    m_parent->do_request(Request {
        HttpMethod::post,
        route,
        m_parent->m_request_timeout_ms,
        get_request_headers(),
        body.dump()
    }, handler);
}

void App::UsernamePasswordProviderClient::retry_custom_confirmation(const std::string& email,
                                                                    std::function<void(Optional<AppError>)> completion_block)
{
    REALM_ASSERT(m_parent);
    std::string route = util::format("%1/providers/%2/confirm/call", m_parent->m_auth_route, username_password_provider_key);

    auto handler = [completion_block](const Response& response) {
        handle_default_response(response, completion_block);
    };

    nlohmann::json body {
        { "email", email }
    };

    m_parent->do_request(Request {
        HttpMethod::post,
        route,
        m_parent->m_request_timeout_ms,
        get_request_headers(),
        body.dump()
    }, handler);
}

void App::UsernamePasswordProviderClient::send_reset_password_email(const std::string& email,
                                                                    std::function<void(Optional<AppError>)> completion_block)
{
    REALM_ASSERT(m_parent);
    std::string route = util::format("%1/providers/%2/reset/send", m_parent->m_auth_route, username_password_provider_key);

    auto handler = [completion_block](const Response& response) {
        handle_default_response(response, completion_block);
    };

    nlohmann::json body = {
        { "email", email }
    };

    m_parent->do_request(Request {
        HttpMethod::post,
        route,
        m_parent->m_request_timeout_ms,
        get_request_headers(),
        body.dump()
    }, handler);
}

void App::UsernamePasswordProviderClient::reset_password(const std::string& password,
                                                         const std::string& token,
                                                         const std::string& token_id,
                                                         std::function<void(Optional<AppError>)> completion_block)
{
    REALM_ASSERT(m_parent);
    std::string route = util::format("%1/providers/%2/reset", m_parent->m_auth_route, username_password_provider_key);

    auto handler = [completion_block](const Response& response) {
        handle_default_response(response, completion_block);
    };

    nlohmann::json body = {
        { "password", password },
        { "token", token },
        { "tokenId", token_id }
    };

    m_parent->do_request(Request {
        HttpMethod::post,
        route,
        m_parent->m_request_timeout_ms,
        get_request_headers(),
        body.dump()
    }, handler);
}

void App::UsernamePasswordProviderClient::call_reset_password_function(const std::string& email,
                                                                       const std::string& password,
                                                                       const bson::BsonArray& args,
                                                                       std::function<void(Optional<AppError>)> completion_block)
{
    REALM_ASSERT(m_parent);
    std::string route = util::format("%1/providers/%2/reset/call", m_parent->m_auth_route, username_password_provider_key);

    auto handler = [completion_block](const Response& response) {
        handle_default_response(response, completion_block);
    };

    bson::BsonDocument arg = {
        { "email", email },
        { "password", password },
        { "arguments", args }
    };

    std::stringstream body;
    body << bson::Bson(arg);

    m_parent->do_request(Request {
        HttpMethod::post,
        route,
        m_parent->m_request_timeout_ms,
        get_request_headers(),
        body.str()
    }, handler);
}

// MARK: - UserAPIKeyProviderClient

std::string App::UserAPIKeyProviderClient::url_for_path(const std::string &path="") const
{
    if (!path.empty()) {
        return m_auth_request_client.url_for_path(util::format("%1/%2/%3",
                                                               auth_path,
                                                               user_api_key_provider_key_path,
                                                               path));
    }

    return m_auth_request_client.url_for_path(util::format("%1/%2",
                                                           auth_path,
                                                           user_api_key_provider_key_path));
}

void App::UserAPIKeyProviderClient::create_api_key(const std::string &name, std::shared_ptr<SyncUser> user,
                                                   std::function<void (UserAPIKey, Optional<AppError>)> completion_block)
{
    std::string route = url_for_path();

    auto handler = [completion_block](const Response& response) {

        if (auto error = AppUtils::check_for_errors(response)) {
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
    Request req;
    req.method = HttpMethod::post;
    req.url = route;
    req.body = body.dump();
    req.uses_refresh_token = true;

    m_auth_request_client.do_authenticated_request(req, user, handler);
}

void App::UserAPIKeyProviderClient::fetch_api_key(const realm::ObjectId& id, std::shared_ptr<SyncUser> user,
                                                   std::function<void (UserAPIKey, Optional<AppError>)> completion_block)
{
    std::string route = url_for_path(id.to_string());

    auto handler = [completion_block](const Response& response) {

        if (auto error = AppUtils::check_for_errors(response)) {
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

    Request req;
    req.method = HttpMethod::get;
    req.url = route;
    req.uses_refresh_token = true;

    m_auth_request_client.do_authenticated_request(req, user, handler);
}

void App::UserAPIKeyProviderClient::fetch_api_keys(std::shared_ptr<SyncUser> user,
                                                   std::function<void(std::vector<UserAPIKey>, Optional<AppError>)> completion_block)
{
    std::string route = url_for_path();

    auto handler = [completion_block](const Response& response) {

        if (auto error = AppUtils::check_for_errors(response)) {
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
            auto json_array = json.get<std::vector<nlohmann::json>>();
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

    Request req;
    req.method = HttpMethod::get;
    req.url = route;
    req.uses_refresh_token = true;

    m_auth_request_client.do_authenticated_request(req, user, handler);
}


void App::UserAPIKeyProviderClient::delete_api_key(const realm::ObjectId& id, std::shared_ptr<SyncUser> user,
                                                   std::function<void(util::Optional<AppError>)> completion_block)
{
    std::string route = url_for_path(id.to_string());

    auto handler = [completion_block](const Response& response) {
        if (auto error = AppUtils::check_for_errors(response)) {
            return completion_block(error);
        } else {
            return completion_block({});
        }
    };

    Request req;
    req.method = HttpMethod::del;
    req.url = route;
    req.uses_refresh_token = true;

    m_auth_request_client.do_authenticated_request(req, user, handler);
}

void App::UserAPIKeyProviderClient::enable_api_key(const realm::ObjectId& id, std::shared_ptr<SyncUser> user,
                                                   std::function<void(Optional<AppError> error)> completion_block)
{
    std::string route = url_for_path(util::format("%1/enable", id.to_string()));

    auto handler = [completion_block](const Response& response) {
        if (auto error = AppUtils::check_for_errors(response)) {
            return completion_block(error);
        } else {
            return completion_block({});
        }
    };

    Request req;
    req.method = HttpMethod::put;
    req.url = route;
    req.uses_refresh_token = true;

    m_auth_request_client.do_authenticated_request(req, user, handler);
}

void App::UserAPIKeyProviderClient::disable_api_key(const realm::ObjectId& id, std::shared_ptr<SyncUser> user,
                                                   std::function<void(Optional<AppError> error)> completion_block)
{
    std::string route = url_for_path(util::format("%1/disable", id.to_string()));

    auto handler = [completion_block](const Response& response) {
        if (auto error = AppUtils::check_for_errors(response)) {
            return completion_block(error);
        } else {
            return completion_block({});
        }
    };

    Request req;
    req.method = HttpMethod::put;
    req.url = route;
    req.uses_refresh_token = true;

    m_auth_request_client.do_authenticated_request(req, user, handler);
}
// MARK: - App

std::shared_ptr<SyncUser> App::current_user() const
{
    return m_sync_manager->get_current_user();
}

std::vector<std::shared_ptr<SyncUser>> App::all_users() const
{
    return m_sync_manager->all_users();
}

void App::get_profile(std::shared_ptr<SyncUser> sync_user,
                      std::function<void(std::shared_ptr<SyncUser>, util::Optional<AppError>)> completion_block)
{
    auto profile_handler = [completion_block, this, sync_user](const Response& profile_response) {
        if (auto error = AppUtils::check_for_errors(profile_response)) {
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

            sync_user->set_state(SyncUser::State::LoggedIn);
            m_sync_manager->set_current_user(sync_user->identity());
        } catch (const AppError& err) {
            return completion_block(nullptr, err);
        }

        return completion_block(sync_user, {});
    };

    std::string profile_route = util::format("%1/auth/profile", m_base_route);

    Request req;
    req.method = HttpMethod::get;
    req.url = profile_route;
    req.timeout_ms = m_request_timeout_ms;
    req.uses_refresh_token = false;

    do_authenticated_request(req, sync_user, profile_handler);
}

void App::attach_auth_options(bson::BsonDocument& body)
{
    bson::BsonDocument options;

    if (m_config.local_app_version) {
        options["appVersion"] = *m_config.local_app_version;
    }

    options["appId"] = m_config.app_id;
    options["platform"] = m_config.platform;
    options["platformVersion"] = m_config.platform_version;
    options["sdkVersion"] = m_config.sdk_version;

    body["options"] = bson::BsonDocument({{"device", options}});
}

void App::log_in_with_credentials(const AppCredentials& credentials,
                                  const std::shared_ptr<SyncUser> linking_user,
                                  std::function<void(std::shared_ptr<SyncUser>, Optional<AppError>)> completion_block)
{
    // construct the route
    std::string route = util::format("%1/providers/%2/login%3",
                                     m_auth_route,
                                     credentials.provider_as_string(),
                                     linking_user ? "?link=true" : "");

    auto handler = [completion_block, credentials, linking_user, this](const Response& response) {
        if (auto error = AppUtils::check_for_errors(response)) {
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
            if (linking_user) {
                linking_user->update_access_token(value_from_json<std::string>(json, "access_token"));
            } else {
                sync_user = m_sync_manager->get_user(value_from_json<std::string>(json, "user_id"),
                                                     value_from_json<std::string>(json, "refresh_token"),
                                                     value_from_json<std::string>(json, "access_token"),
                                                     credentials.provider_as_string(),
                                                     value_from_json<std::string>(json, "device_id"));
            }
        } catch (const AppError& err) {
            return completion_block(nullptr, err);
        }

        App::get_profile(linking_user ? linking_user : sync_user, completion_block);
    };

    bson::Bson credentials_as_bson = bson::parse(credentials.serialize_as_json());
    bson::BsonDocument body = static_cast<bson::BsonDocument>(credentials_as_bson);
    attach_auth_options(body);

    std::stringstream s;
    s << bson::Bson(body);

    // if we try logging in with an anonymous user while there
    // is already an anonymous session active, reuse it
    if (credentials.provider() == AuthProvider::ANONYMOUS) {
        for (auto user : m_sync_manager->all_users()) {
            if (user->provider_type() == credentials.provider_as_string() && user->is_logged_in()) {
                completion_block(switch_user(user), util::none);
                return;
            }
        }
    }

    do_request({
        HttpMethod::post,
        route,
        m_request_timeout_ms,
        get_request_headers(linking_user, RequestTokenType::AccessToken),
        s.str()
    }, handler);
}

void App::log_in_with_credentials(const AppCredentials& credentials,
                                  std::function<void(std::shared_ptr<SyncUser>, Optional<AppError>)> completion_block)
{
    App::log_in_with_credentials(credentials, nullptr, completion_block);
}

void App::log_out(std::shared_ptr<SyncUser> user, std::function<void (Optional<AppError>)> completion_block)
{
    if (!user || user->state() != SyncUser::State::LoggedIn) {
        return completion_block(util::none);
    }

    auto handler = [completion_block, user](const Response& response) {
        if (auto error = AppUtils::check_for_errors(response)) {
            return completion_block(error);
        }
        return completion_block(util::none);
    };

    auto refresh_token = user->refresh_token();
    user->log_out();

    std::string route = util::format("%1/auth/session", m_base_route);

    Request req;
    req.method = HttpMethod::del;
    req.url = route;
    req.timeout_ms = m_request_timeout_ms;
    req.uses_refresh_token = true;
    req.headers = get_request_headers();
    req.headers.insert({ "Authorization",
        util::format("Bearer %1", refresh_token)
    });

    do_request(req, [completion_block, req](Response response) {
        if (auto error = AppUtils::check_for_errors(response)) {
            // We do not care about handling auth errors on log out
            completion_block(error);
        } else {
            completion_block(util::none);
        }
    });
}

void App::log_out(std::function<void (Optional<AppError>)> completion_block) {
    log_out(current_user(), completion_block);
}

std::shared_ptr<SyncUser> App::switch_user(std::shared_ptr<SyncUser> user) const
{
    if (!user || user->state() != SyncUser::State::LoggedIn) {
        throw AppError(make_client_error_code(ClientErrorCode::user_not_logged_in),
                       "User is no longer valid or is logged out");
    }

    auto users = m_sync_manager->all_users();
    auto it = std::find(users.begin(),
                        users.end(),
                        user);

    if (it == users.end()) {
        throw AppError(make_client_error_code(ClientErrorCode::user_not_found),
                       "User does not exist");
    }

    m_sync_manager->set_current_user(user->identity());
    return current_user();
}

void App::remove_user(std::shared_ptr<SyncUser> user,
                      std::function<void(Optional<AppError>)> completion_block)
{
    if (!user || user->state() == SyncUser::State::Removed) {
        return completion_block(AppError(make_client_error_code(ClientErrorCode::user_not_found),
                                         "User has already been removed"));
    }

    auto users = m_sync_manager->all_users();

    auto it = std::find(users.begin(),
                        users.end(),
                        user);

    if (it == users.end()) {
        return completion_block(AppError(make_client_error_code(ClientErrorCode::user_not_found),
                                         "No user has been found"));
    }

    if (user->is_logged_in()) {
        log_out(user, [user, completion_block, this](const Optional<AppError>& error){
            m_sync_manager->remove_user(user->identity());
            return completion_block(error);
        });
    } else {
        m_sync_manager->remove_user(user->identity());
        return completion_block({});
    }
}

void App::link_user(std::shared_ptr<SyncUser> user,
                    const AppCredentials& credentials,
                    std::function<void(std::shared_ptr<SyncUser>, Optional<AppError>)> completion_block)
{
    if (!user || user->state() != SyncUser::State::LoggedIn) {
        return completion_block(nullptr, AppError(make_client_error_code(ClientErrorCode::user_not_found),
                                                  "The specified user is not logged in"));
    }

    auto users = m_sync_manager->all_users();

    auto it = std::find(users.begin(),
                        users.end(),
                        user);

    if (it == users.end()) {
        return completion_block(nullptr, AppError(make_client_error_code(ClientErrorCode::user_not_found),
                                                  "The specified user was not found"));
    }

    App::log_in_with_credentials(credentials, user, completion_block);
}

void App::refresh_custom_data(std::shared_ptr<SyncUser> sync_user,
                              std::function<void(Optional<AppError>)> completion_block)
{
    refresh_access_token(sync_user, completion_block);
}

std::string App::url_for_path(const std::string& path="") const
{
    return util::format("%1%2", m_base_route, path);
}

// FIXME: This passes back the response to bubble up any potential errors, making this somewhat leaky
void App::init_app_metadata(std::function<void (util::Optional<AppError>, util::Optional<Response>)> completion_block)
{
    if (m_sync_manager->app_metadata()) {
        return completion_block(util::none, util::none);
    }

    std::string route = util::format("%1/location",
                                     m_app_route);

    Request req;
    req.method = HttpMethod::get;
    req.url = route;
    req.timeout_ms = m_request_timeout_ms;

    m_config.transport_generator()->send_request_to_server(req, [this, completion_block](const Response& response) {
        nlohmann::json json;
        try {
            json = nlohmann::json::parse(response.body);
        } catch (const std::exception& e) {
            return completion_block(AppError(make_error_code(JSONErrorCode::malformed_json), e.what()),
                                    response);
        }

        try {
            auto hostname = value_from_json<std::string>(json, "hostname");
            auto ws_hostname = value_from_json<std::string>(json, "ws_hostname");
            m_sync_manager->perform_metadata_update([&](const SyncMetadataManager& manager){
                manager.set_app_metadata(value_from_json<std::string>(json, "deployment_model"),
                                         value_from_json<std::string>(json, "location"),
                                         hostname, ws_hostname);
            });

            auto metadata = m_sync_manager->app_metadata();

            m_base_route = hostname + base_path;
            std::string this_app_path = app_path + "/" + m_config.app_id;
            m_app_route = m_base_route + this_app_path;
            m_auth_route = m_app_route + auth_path;
            m_sync_manager->set_sync_route(ws_hostname + base_path + this_app_path + sync_path);
        } catch (const AppError& err) {
            return completion_block(err, response);
        }

        completion_block(util::none, util::none);
    });
}

void App::do_request(Request request,
                     std::function<void (Response)> completion_block)
{
    request.timeout_ms = default_timeout_ms;

    // if we do not have metadata yet, we need to initialize it
    if (!m_sync_manager->app_metadata()) {
        init_app_metadata([completion_block, request, this](const util::Optional<AppError> error,
                                                            const util::Optional<Response> response) mutable {
            if (error) {
                return completion_block(*response);
            }

            // if this is the first time we have received app metadata, the
            // original request will not have the correct URL hostname for
            // non global deployments.
            auto app_metadata = m_sync_manager->app_metadata();
            if (app_metadata && app_metadata->deployment_model != "GLOBAL" && request.url.rfind(m_base_url, 0) != std::string::npos) {
                request.url.replace(0, m_base_url.size(), app_metadata->hostname);
            }

            m_config.transport_generator()->send_request_to_server(request, completion_block);
        });
    } else {
        m_config.transport_generator()->send_request_to_server(request, completion_block);
    }
}

void App::do_authenticated_request(Request request,
                                   std::shared_ptr<SyncUser> sync_user,
                                   std::function<void (Response)> completion_block)
{
    request.headers = get_request_headers(sync_user,
                                          request.uses_refresh_token ?
                                          RequestTokenType::RefreshToken : RequestTokenType::AccessToken);

    do_request(request, [completion_block, request, sync_user, this](Response response) {
        if (auto error = AppUtils::check_for_errors(response)) {
            App::handle_auth_failure(error.value(), response, request, sync_user, completion_block);
        } else {
            completion_block(response);
        }
    });
}

void App::handle_auth_failure(const AppError& error,
                              const Response& response,
                              Request request,
                              std::shared_ptr<SyncUser> sync_user,
                              std::function<void (Response)> completion_block)
{
    auto access_token_handler = [this,
                                 request,
                                 completion_block,
                                 response,
                                 sync_user](const Optional<AppError>& error) {
        if (!error) {
            // assign the new access_token to the auth header
            Request newRequest = request;
            newRequest.headers = get_request_headers(sync_user, RequestTokenType::AccessToken);
            m_config.transport_generator()->send_request_to_server(newRequest, completion_block);
        } else {
            // pass the error back up the chain
            completion_block(response);
        }
    };

    // Only handle auth failures
    if (*error.http_status_code && *error.http_status_code == 401) {
        if (request.uses_refresh_token) {
            if (sync_user && sync_user->is_logged_in()) {
                sync_user->log_out();
            }
            completion_block(response);
            return;
        }

        App::refresh_access_token(sync_user, access_token_handler);
    } else {
        completion_block(response);
    }
}

/// MARK: - refresh access token
void App::refresh_access_token(std::shared_ptr<SyncUser> sync_user,
                               std::function<void(Optional<AppError>)> completion_block)
{
    if (!sync_user) {
        completion_block(AppError(make_client_error_code(ClientErrorCode::user_not_found),
                                  "No current user exists"));
        return;
    }

    if (!sync_user->is_logged_in()) {
        completion_block(AppError(make_client_error_code(ClientErrorCode::user_not_logged_in),
                                  "The user is not logged in"));
        return;
    }

    auto handler = [completion_block, sync_user](const Response& response) {

        if (auto error = AppUtils::check_for_errors(response)) {
            return completion_block(error);
        }

        try {
            nlohmann::json json = nlohmann::json::parse(response.body);
            auto access_token = value_from_json<std::string>(json, "access_token");
            sync_user->update_access_token(std::move(access_token));
        } catch (const AppError& err) {
            return completion_block(err);
        }

        return completion_block(util::none);
    };

    std::string route = util::format("%1/auth/session", m_base_route);

    do_request(Request {
        HttpMethod::post,
        route,
        m_request_timeout_ms,
        get_request_headers(sync_user,  RequestTokenType::RefreshToken)
    }, handler);
}

std::string App::function_call_url_path() const {
    return util::format("%1/app/%2/functions/call", m_base_route, m_config.app_id);
}
void App::call_function(std::shared_ptr<SyncUser> user,
                        const std::string& name,
                        const bson::BsonArray& args_bson,
                        const util::Optional<std::string>& service_name,
                        std::function<void (util::Optional<AppError>,
                                            util::Optional<bson::Bson>)> completion_block)
{
    auto handler = [completion_block](const Response& response) {
        if (auto error = AppUtils::check_for_errors(response)) {
            return completion_block(error, util::none);
        }
        completion_block(util::none, util::Optional<bson::Bson>(bson::parse(response.body)));
    };

    bson::BsonDocument args {
        { "arguments", args_bson },
        { "name", name }
    };

    if (service_name) {
        args["service"] = *service_name;
    }

    do_authenticated_request(Request{
        HttpMethod::post,
        function_call_url_path(),
        m_request_timeout_ms,
        {},
        bson::Bson(args).toJson(),
        false
    },
    user,
    handler);
}

void App::call_function(std::shared_ptr<SyncUser> user,
                        const std::string& name,
                        const bson::BsonArray& args_bson,
                        std::function<void (util::Optional<AppError>,
                                            util::Optional<bson::Bson>)> completion_block)
{
    call_function(user,
                  name,
                  args_bson,
                  util::none,
                  completion_block);
}

void App::call_function(const std::string& name,
                        const bson::BsonArray& args_bson,
                        const util::Optional<std::string>& service_name,
                        std::function<void (util::Optional<AppError>,
                                            util::Optional<bson::Bson>)> completion_block)
{
    call_function(m_sync_manager->get_current_user(),
                  name,
                  args_bson,
                  service_name,
                  completion_block);
}

void App::call_function(const std::string& name,
                        const bson::BsonArray& args_bson,
                        std::function<void (util::Optional<AppError>,
                                            util::Optional<bson::Bson>)> completion_block)
{
    call_function(m_sync_manager->get_current_user(),
                  name,
                  args_bson,
                  completion_block);
}

Request App::make_streaming_request(std::shared_ptr<SyncUser> user,
                                    const std::string &name,
                                    const bson::BsonArray &args_bson,
                                    const util::Optional<std::string> &service_name) const {
    auto args = bson::BsonDocument{
        {"arguments", args_bson},
        {"name", name},
    };
    if (service_name) {
        args["service"] = *service_name;
    }
    const auto args_json = bson::Bson(args).toJson();

    auto args_base64 = std::string(util::base64_encoded_size(args_json.size()), '\0');
    util::base64_encode(args_json.data(), args_json.size(), args_base64.data(), args_base64.size());

    auto url = function_call_url_path() + "?baas_request=" + util::uri_percent_encode(args_base64);
    if (user) {
        url += "&baas_at=";
        url += user->access_token(); // doesn't need url encoding
    }

    return Request{
        HttpMethod::get,
        url,
        m_request_timeout_ms,
        {{"Accept", "text/event-stream"}},
    };
}

PushClient App::push_notification_client(const std::string& service_name)
{
    return PushClient(service_name,
                      m_config.app_id,
                      m_request_timeout_ms,
                      shared_from_this());
}

} // namespace app
} // namespace realm
