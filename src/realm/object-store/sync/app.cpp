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

#include "external/json/json.hpp"
#include <realm/object-store/sync/app.hpp>

#include <realm/util/base64.hpp>
#include <realm/util/http.hpp>
#include <realm/util/uri.hpp>
#include <realm/object-store/sync/app_utils.hpp>
#include <realm/object-store/sync/impl/sync_metadata.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_user.hpp>

#include <sstream>
#include <string>

using namespace realm;
using namespace realm::app;
using namespace bson;
using util::Optional;
using util::UniqueFunction;

namespace {
// MARK: - Helpers

REALM_COLD
REALM_NOINLINE
REALM_NORETURN
void throw_json_error(JSONErrorCode ec, std::string_view message)
{
    throw AppError(make_error_code(ec), std::string(message));
}

template <typename T>
T as(const Bson& bson)
{
    if (holds_alternative<T>(bson)) {
        return static_cast<T>(bson);
    }
    throw_json_error(JSONErrorCode::malformed_json, "?");
}

template <typename T>
T get(const BsonDocument& doc, const std::string& key)
{
    auto& raw = doc.entries();
    if (auto it = raw.find(key); it != raw.end()) {
        return as<T>(it->second);
    }
    throw_json_error(JSONErrorCode::missing_json_key, key);
}

template <typename T>
void read_field(const BsonDocument& data, const std::string& key, T& value)
{
    auto& raw = data.entries();
    if (auto it = raw.find(key); it != raw.end()) {
        value = as<T>(it->second);
    }
    else {
        throw_json_error(JSONErrorCode::missing_json_key, key);
    }
}

template <>
void read_field(const BsonDocument& data, const std::string& key, ObjectId& value)
{
    value = ObjectId(get<std::string>(data, key).c_str());
}

template <typename T>
void read_field(const BsonDocument& data, const std::string& key, Optional<T>& value)
{
    auto& raw = data.entries();
    if (auto it = raw.find(key); it != raw.end()) {
        value = as<T>(it->second);
    }
}

template <typename T>
T parse(std::string_view str)
{
    try {
        return as<T>(bson::parse(str));
    }
    catch (const std::exception& e) {
        throw_json_error(JSONErrorCode::malformed_json, e.what());
    }
}

struct UserAPIKeyResponseHandler {
    UniqueFunction<void(App::UserAPIKey&&, Optional<AppError>)> completion;
    void operator()(const Response& response)
    {
        if (auto error = AppUtils::check_for_errors(response)) {
            return completion({}, std::move(error));
        }

        try {
            auto json = parse<BsonDocument>(response.body);
            completion(read_user_api_key(json), {});
        }
        catch (AppError& e) {
            completion({}, std::move(e));
        }
    }

    static App::UserAPIKey read_user_api_key(const BsonDocument& doc)
    {
        App::UserAPIKey user_api_key;
        read_field(doc, "_id", user_api_key.id);
        read_field(doc, "key", user_api_key.key);
        read_field(doc, "name", user_api_key.name);
        read_field(doc, "disabled", user_api_key.disabled);
        return user_api_key;
    }
};

enum class RequestTokenType { NoAuth, AccessToken, RefreshToken };

// generate the request headers for a HTTP call, by default it will generate headers with a refresh token if a user is
// passed
HttpHeaders get_request_headers(const std::shared_ptr<SyncUser>& with_user_authorization = nullptr,
                                RequestTokenType token_type = RequestTokenType::RefreshToken)
{
    HttpHeaders headers{{"Content-Type", "application/json;charset=utf-8"}, {"Accept", "application/json"}};

    if (with_user_authorization) {
        switch (token_type) {
            case RequestTokenType::NoAuth:
                break;
            case RequestTokenType::AccessToken:
                headers.insert({"Authorization", util::format("Bearer %1", with_user_authorization->access_token())});
                break;
            case RequestTokenType::RefreshToken:
                headers.insert(
                    {"Authorization", util::format("Bearer %1", with_user_authorization->refresh_token())});
                break;
        }
    }
    return headers;
}

UniqueFunction<void(const Response&)> handle_default_response(UniqueFunction<void(Optional<AppError>)>&& completion)
{
    return [completion = std::move(completion)](const Response& response) {
        completion(AppUtils::check_for_errors(response));
    };
}

const static std::string default_base_url = "https://realm.mongodb.com";
const static std::string base_path = "/api/client/v2.0";
const static std::string app_path = "/app";
const static std::string auth_path = "/auth";
const static std::string sync_path = "/realm-sync";
const static uint64_t default_timeout_ms = 60000;
const static std::string username_password_provider_key = "local-userpass";
const static std::string user_api_key_provider_key_path = "api_keys";
const static int max_http_redirects = 20;
static std::unordered_map<std::string, std::shared_ptr<App>> s_apps_cache;
std::mutex s_apps_mutex;

} // anonymous namespace

namespace realm {
namespace app {

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

SharedApp App::get_uncached_app(const Config& config, const SyncClientConfig& sync_client_config)
{
    auto app = std::make_shared<App>(config);
    app->configure(sync_client_config);
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

void App::close_all_sync_sessions()
{
    std::lock_guard<std::mutex> lock(s_apps_mutex);
    for (auto& app : s_apps_cache) {
        app.second->sync_manager()->close_all_sessions();
    }
}

App::App(const Config& config)
    : m_config(std::move(config))
    , m_base_url(config.base_url.value_or(default_base_url))
    , m_base_route(m_base_url + base_path)
    , m_app_route(m_base_route + app_path + "/" + config.app_id)
    , m_auth_route(m_app_route + auth_path)
    , m_request_timeout_ms(config.default_request_timeout_ms.value_or(default_timeout_ms))
{
    REALM_ASSERT(m_config.transport);

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
    auto sync_route = make_sync_route(m_app_route);

    m_sync_manager = std::make_shared<SyncManager>();
}

App::~App() {}

void App::configure(const SyncClientConfig& sync_client_config)
{
    auto sync_route = make_sync_route(m_app_route);
    m_sync_manager->configure(shared_from_this(), sync_route, sync_client_config);
    if (auto metadata = m_sync_manager->app_metadata()) {
        update_hostname(metadata);
    }
}

inline bool App::init_logger()
{
    // If a log function is called before configure(), a null ptr will be
    // returned by get_logger()
    if (!m_logger_ptr) {
        m_logger_ptr = m_sync_manager->get_logger();
    }
    return bool(m_logger_ptr);
}

bool App::would_log(util::Logger::Level level)
{
    init_logger();
    return m_logger_ptr && m_logger_ptr->would_log(level);
}

template <class... Params>
void App::log_debug(const char* message, Params&&... params)
{
    if (init_logger()) {
        m_logger_ptr->log(util::Logger::Level::debug, message, std::forward<Params>(params)...);
    }
}

template <class... Params>
void App::log_error(const char* message, Params&&... params)
{
    if (init_logger()) {
        m_logger_ptr->log(util::Logger::Level::error, message, std::forward<Params>(params)...);
    }
}

std::string App::make_sync_route(const std::string& http_app_route)
{
    // change the scheme in the base url to ws from http to satisfy the sync client
    auto sync_route = http_app_route + sync_path;
    size_t uri_scheme_start = sync_route.find("http");
    if (uri_scheme_start == 0)
        sync_route.replace(uri_scheme_start, 4, "ws");
    return sync_route;
}

void App::update_hostname(const util::Optional<SyncAppMetadata>& metadata)
{
    // Update url components based on new hostname value
    if (metadata) {
        update_hostname(metadata->hostname, metadata->ws_hostname);
    }
}

void App::update_hostname(const std::string& hostname, const Optional<std::string>& ws_hostname)
{
    // Update url components based on new hostname value
    log_debug("App: update_hostname: %1 | %2", hostname, ws_hostname);
    std::lock_guard<std::mutex> lock(*m_route_mutex);
    m_base_route = (hostname.length() > 0 ? hostname : default_base_url) + base_path;
    std::string this_app_path = app_path + "/" + m_config.app_id;
    m_app_route = m_base_route + this_app_path;
    m_auth_route = m_app_route + auth_path;
    if (ws_hostname && ws_hostname->length() > 0) {
        m_sync_manager->set_sync_route(*ws_hostname + base_path + this_app_path + sync_path);
    }
    else if (m_sync_manager) {
        m_sync_manager->set_sync_route(make_sync_route(m_app_route));
    }
}

// MARK: - Template specializations

template <>
App::UsernamePasswordProviderClient App::provider_client<App::UsernamePasswordProviderClient>()
{
    return App::UsernamePasswordProviderClient(shared_from_this());
}

template <>
App::UserAPIKeyProviderClient App::provider_client<App::UserAPIKeyProviderClient>()
{
    return App::UserAPIKeyProviderClient(*this);
}

// MARK: - UsernamePasswordProviderClient

void App::UsernamePasswordProviderClient::register_email(const std::string& email, const std::string& password,
                                                         UniqueFunction<void(Optional<AppError>)>&& completion)
{
    m_parent->log_debug("App: register_email: %1", email);
    m_parent->post(util::format("%1/providers/%2/register", m_parent->m_auth_route, username_password_provider_key),
                   std::move(completion), {{"email", email}, {"password", password}});
}

void App::UsernamePasswordProviderClient::confirm_user(const std::string& token, const std::string& token_id,
                                                       UniqueFunction<void(Optional<AppError>)>&& completion)
{
    m_parent->log_debug("App: confirm_user");
    m_parent->post(util::format("%1/providers/%2/confirm", m_parent->m_auth_route, username_password_provider_key),
                   std::move(completion), {{"token", token}, {"tokenId", token_id}});
}

void App::UsernamePasswordProviderClient::resend_confirmation_email(
    const std::string& email, UniqueFunction<void(Optional<AppError>)>&& completion)
{
    m_parent->log_debug("App: resend_confirmation_email: %1", email);
    m_parent->post(
        util::format("%1/providers/%2/confirm/send", m_parent->m_auth_route, username_password_provider_key),
        std::move(completion), {{"email", email}});
}

void App::UsernamePasswordProviderClient::retry_custom_confirmation(
    const std::string& email, UniqueFunction<void(Optional<AppError>)>&& completion)
{
    m_parent->log_debug("App: retry_custom_confirmation: %1", email);
    m_parent->post(
        util::format("%1/providers/%2/confirm/call", m_parent->m_auth_route, username_password_provider_key),
        std::move(completion), {{"email", email}});
}

void App::UsernamePasswordProviderClient::send_reset_password_email(
    const std::string& email, UniqueFunction<void(Optional<AppError>)>&& completion)
{
    m_parent->log_debug("App: send_reset_password_email: %1", email);
    m_parent->post(util::format("%1/providers/%2/reset/send", m_parent->m_auth_route, username_password_provider_key),
                   std::move(completion), {{"email", email}});
}

void App::UsernamePasswordProviderClient::reset_password(const std::string& password, const std::string& token,
                                                         const std::string& token_id,
                                                         UniqueFunction<void(Optional<AppError>)>&& completion)
{
    m_parent->log_debug("App: reset_password");
    m_parent->post(util::format("%1/providers/%2/reset", m_parent->m_auth_route, username_password_provider_key),
                   std::move(completion), {{"password", password}, {"token", token}, {"tokenId", token_id}});
}

void App::UsernamePasswordProviderClient::call_reset_password_function(
    const std::string& email, const std::string& password, const BsonArray& args,
    UniqueFunction<void(Optional<AppError>)>&& completion)
{
    m_parent->log_debug("App: call_reset_password_function: %1", email);
    m_parent->post(util::format("%1/providers/%2/reset/call", m_parent->m_auth_route, username_password_provider_key),
                   std::move(completion), {{"email", email}, {"password", password}, {"arguments", args}});
}

// MARK: - UserAPIKeyProviderClient

std::string App::UserAPIKeyProviderClient::url_for_path(const std::string& path = "") const
{
    if (!path.empty()) {
        return m_auth_request_client.url_for_path(
            util::format("%1/%2/%3", auth_path, user_api_key_provider_key_path, path));
    }

    return m_auth_request_client.url_for_path(util::format("%1/%2", auth_path, user_api_key_provider_key_path));
}

void App::UserAPIKeyProviderClient::create_api_key(
    const std::string& name, const std::shared_ptr<SyncUser>& user,
    UniqueFunction<void(UserAPIKey&&, Optional<AppError>)>&& completion)
{
    Request req;
    req.method = HttpMethod::post;
    req.url = url_for_path();
    req.body = Bson(BsonDocument{{"name", name}}).to_string();
    req.uses_refresh_token = true;
    m_auth_request_client.do_authenticated_request(std::move(req), user,
                                                   UserAPIKeyResponseHandler{std::move(completion)});
}

void App::UserAPIKeyProviderClient::fetch_api_key(const realm::ObjectId& id, const std::shared_ptr<SyncUser>& user,
                                                  UniqueFunction<void(UserAPIKey&&, Optional<AppError>)>&& completion)
{
    Request req;
    req.method = HttpMethod::get;
    req.url = url_for_path(id.to_string());
    req.uses_refresh_token = true;
    m_auth_request_client.do_authenticated_request(std::move(req), user,
                                                   UserAPIKeyResponseHandler{std::move(completion)});
}

void App::UserAPIKeyProviderClient::fetch_api_keys(
    const std::shared_ptr<SyncUser>& user,
    UniqueFunction<void(std::vector<UserAPIKey>&&, Optional<AppError>)>&& completion)
{
    Request req;
    req.method = HttpMethod::get;
    req.url = url_for_path();
    req.uses_refresh_token = true;

    m_auth_request_client.do_authenticated_request(
        std::move(req), user, [completion = std::move(completion)](const Response& response) {
            if (auto error = AppUtils::check_for_errors(response)) {
                return completion({}, std::move(error));
            }

            try {
                auto json = parse<BsonArray>(response.body);
                std::vector<UserAPIKey> keys;
                keys.reserve(json.size());
                for (auto&& api_key_json : json) {
                    keys.push_back(UserAPIKeyResponseHandler::read_user_api_key(as<BsonDocument>(api_key_json)));
                }
                return completion(std::move(keys), {});
            }
            catch (AppError& e) {
                completion({}, std::move(e));
            }
        });
}


void App::UserAPIKeyProviderClient::delete_api_key(const realm::ObjectId& id, const std::shared_ptr<SyncUser>& user,
                                                   UniqueFunction<void(Optional<AppError>)>&& completion)
{
    Request req;
    req.method = HttpMethod::del;
    req.url = url_for_path(id.to_string());
    req.uses_refresh_token = true;
    m_auth_request_client.do_authenticated_request(std::move(req), user,
                                                   handle_default_response(std::move(completion)));
}

void App::UserAPIKeyProviderClient::enable_api_key(const realm::ObjectId& id, const std::shared_ptr<SyncUser>& user,
                                                   UniqueFunction<void(Optional<AppError>)>&& completion)
{
    Request req;
    req.method = HttpMethod::put;
    req.url = url_for_path(util::format("%1/enable", id.to_string()));
    req.uses_refresh_token = true;
    m_auth_request_client.do_authenticated_request(std::move(req), user,
                                                   handle_default_response(std::move(completion)));
}

void App::UserAPIKeyProviderClient::disable_api_key(const realm::ObjectId& id, const std::shared_ptr<SyncUser>& user,
                                                    UniqueFunction<void(Optional<AppError>)>&& completion)
{
    Request req;
    req.method = HttpMethod::put;
    req.url = url_for_path(util::format("%1/disable", id.to_string()));
    req.uses_refresh_token = true;
    m_auth_request_client.do_authenticated_request(std::move(req), user,
                                                   handle_default_response(std::move(completion)));
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

void App::get_profile(const std::shared_ptr<SyncUser>& sync_user,
                      UniqueFunction<void(const std::shared_ptr<SyncUser>&, Optional<AppError>)>&& completion)
{
    Request req;
    req.method = HttpMethod::get;
    req.timeout_ms = m_request_timeout_ms;
    req.uses_refresh_token = false;
    {
        std::lock_guard<std::mutex> lock(*m_route_mutex);
        req.url = util::format("%1/auth/profile", m_base_route);
    }

    do_authenticated_request(
        std::move(req), sync_user,
        [completion = std::move(completion), self = shared_from_this(), sync_user](const Response& profile_response) {
            if (auto error = AppUtils::check_for_errors(profile_response)) {
                return completion(nullptr, std::move(error));
            }

            try {
                auto profile_json = parse<BsonDocument>(profile_response.body);
                auto identities_json = get<BsonArray>(profile_json, "identities");

                std::vector<SyncUserIdentity> identities;
                identities.reserve(profile_json.size());
                for (auto& identity_json : identities_json) {
                    auto doc = as<BsonDocument>(identity_json);
                    identities.push_back(
                        SyncUserIdentity(get<std::string>(doc, "id"), get<std::string>(doc, "provider_type")));
                }

                sync_user->update_identities(identities);
                sync_user->update_user_profile(SyncUserProfile(get<BsonDocument>(profile_json, "data")));
                sync_user->set_state(SyncUser::State::LoggedIn);
                self->m_sync_manager->set_current_user(sync_user->identity());
                self->emit_change_to_subscribers(*self);
            }
            catch (const AppError& err) {
                return completion(nullptr, err);
            }

            return completion(sync_user, {});
        });
}

void App::attach_auth_options(BsonDocument& body)
{
    BsonDocument options;

    if (m_config.local_app_version) {
        options["appVersion"] = *m_config.local_app_version;
    }

    log_debug("App: version info: platform: %1  version: %1 - sdk version: %3 - core version: %4", m_config.platform,
              m_config.platform_version, m_config.sdk_version, REALM_VERSION_STRING);
    options["appId"] = m_config.app_id;
    options["platform"] = m_config.platform;
    options["platformVersion"] = m_config.platform_version;
    options["sdkVersion"] = m_config.sdk_version;
    options["coreVersion"] = REALM_VERSION_STRING;

    body["options"] = BsonDocument({{"device", options}});
}

void App::log_in_with_credentials(
    const AppCredentials& credentials, const std::shared_ptr<SyncUser>& linking_user,
    UniqueFunction<void(const std::shared_ptr<SyncUser>&, Optional<AppError>)>&& completion)
{
    if (would_log(util::Logger::Level::debug)) {
        auto app_info = util::format("app_id: %1", m_config.app_id);
        if (m_config.local_app_version) {
            app_info += util::format(" - app_version: %1", *m_config.local_app_version);
        }
        log_debug("App: log_in_with_credentials: %1", app_info);
    }
    // if we try logging in with an anonymous user while there
    // is already an anonymous session active, reuse it
    if (credentials.provider() == AuthProvider::ANONYMOUS) {
        for (auto&& user : m_sync_manager->all_users()) {
            if (user->provider_type() == credentials.provider_as_string() && user->is_logged_in()) {
                completion(switch_user(user), util::none);
                return;
            }
        }
    }

    // construct the route
    std::string route = util::format("%1/providers/%2/login%3", m_auth_route, credentials.provider_as_string(),
                                     linking_user ? "?link=true" : "");

    BsonDocument body = credentials.serialize_as_bson();
    attach_auth_options(body);

    do_request({HttpMethod::post, route, m_request_timeout_ms,
                get_request_headers(linking_user, RequestTokenType::AccessToken), Bson(body).to_string()},
               [completion = std::move(completion), credentials, linking_user,
                self = shared_from_this()](const Response& response) mutable {
                   if (auto error = AppUtils::check_for_errors(response)) {
                       self->log_error("App: log_in_with_credentials failed: %1 message: %2",
                                       response.http_status_code, error->message);
                       return completion(nullptr, std::move(error));
                   }

                   std::shared_ptr<realm::SyncUser> sync_user = linking_user;
                   try {
                       auto json = parse<BsonDocument>(response.body);
                       if (linking_user) {
                           linking_user->update_access_token(get<std::string>(json, "access_token"));
                       }
                       else {
                           sync_user = self->m_sync_manager->get_user(
                               get<std::string>(json, "user_id"), get<std::string>(json, "refresh_token"),
                               get<std::string>(json, "access_token"), credentials.provider_as_string(),
                               get<std::string>(json, "device_id"));
                       }
                   }
                   catch (const AppError& e) {
                       return completion(nullptr, e);
                   }

                   self->get_profile(sync_user, std::move(completion));
               });
}

void App::log_in_with_credentials(
    const AppCredentials& credentials,
    util::UniqueFunction<void(const std::shared_ptr<SyncUser>&, Optional<AppError>)>&& completion)
{
    App::log_in_with_credentials(credentials, nullptr, std::move(completion));
}

void App::log_out(const std::shared_ptr<SyncUser>& user, UniqueFunction<void(Optional<AppError>)>&& completion)
{
    if (!user || user->state() != SyncUser::State::LoggedIn) {
        return completion(util::none);
    }

    auto refresh_token = user->refresh_token();
    user->log_out();

    std::string route = util::format("%1/auth/session", m_base_route);

    Request req;
    req.method = HttpMethod::del;
    req.url = route;
    req.timeout_ms = m_request_timeout_ms;
    req.uses_refresh_token = true;
    req.headers = get_request_headers();
    req.headers.insert({"Authorization", util::format("Bearer %1", refresh_token)});
    {
        std::lock_guard<std::mutex> lock(*m_route_mutex);
        req.url = util::format("%1/auth/session", m_base_route);
    }

    do_request(std::move(req),
               [self = shared_from_this(), completion = std::move(completion)](const Response& response) {
                   auto error = AppUtils::check_for_errors(response);
                   if (!error) {
                       self->emit_change_to_subscribers(*self);
                   }
                   completion(error);
               });
}

void App::log_out(UniqueFunction<void(Optional<AppError>)>&& completion)
{
    log_debug("App: log_out()");
    log_out(current_user(), std::move(completion));
}

bool App::verify_user_present(const std::shared_ptr<SyncUser>& user) const
{
    auto users = m_sync_manager->all_users();
    return std::any_of(users.begin(), users.end(), [&](auto&& u) {
        return u == user;
    });
}

std::shared_ptr<SyncUser> App::switch_user(const std::shared_ptr<SyncUser>& user) const
{
    if (!user || user->state() != SyncUser::State::LoggedIn) {
        throw AppError(make_client_error_code(ClientErrorCode::user_not_logged_in),
                       "User is no longer valid or is logged out");
    }

    auto users = m_sync_manager->all_users();
    auto it = std::find(users.begin(), users.end(), user);

    if (it == users.end()) {
        throw AppError(make_client_error_code(ClientErrorCode::user_not_found), "User does not exist");
    }

    m_sync_manager->set_current_user(user->identity());
    emit_change_to_subscribers(*this);
    return current_user();
}

void App::remove_user(const std::shared_ptr<SyncUser>& user, UniqueFunction<void(Optional<AppError>)>&& completion)
{
    if (!user || user->state() == SyncUser::State::Removed) {
        return completion(
            AppError(make_client_error_code(ClientErrorCode::user_not_found), "User has already been removed"));
    }
    if (!verify_user_present(user)) {
        return completion(
            AppError(make_client_error_code(ClientErrorCode::user_not_found), "No user has been found"));
    }

    if (user->is_logged_in()) {
        log_out(user, [user, completion = std::move(completion),
                       self = shared_from_this()](const Optional<AppError>& error) {
            self->m_sync_manager->remove_user(user->identity());
            return completion(error);
        });
    }
    else {
        m_sync_manager->remove_user(user->identity());
        return completion({});
    }
}

void App::delete_user(const std::shared_ptr<SyncUser>& user, UniqueFunction<void(Optional<AppError>)>&& completion)
{
    if (!user) {
        return completion(AppError(make_client_error_code(ClientErrorCode::user_not_found),
                                   "The specified user could not be found."));
    }
    if (user->state() != SyncUser::State::LoggedIn) {
        return completion(AppError(make_client_error_code(ClientErrorCode::user_not_logged_in),
                                   "User must be logged in to be deleted."));
    }

    if (!verify_user_present(user)) {
        return completion(
            AppError(make_client_error_code(ClientErrorCode::user_not_found), "No user has been found."));
    }

    Request req;
    req.method = HttpMethod::del;
    req.timeout_ms = m_request_timeout_ms;
    req.url = url_for_path("/auth/delete");
    do_authenticated_request(std::move(req), user,
                             [self = shared_from_this(), completion = std::move(completion),
                              identitiy = user->identity()](const Response& response) {
                                 auto error = AppUtils::check_for_errors(response);
                                 if (!error) {
                                     self->emit_change_to_subscribers(*self);
                                     self->m_sync_manager->delete_user(identitiy);
                                 }
                                 completion(error);
                             });
}

void App::link_user(const std::shared_ptr<SyncUser>& user, const AppCredentials& credentials,
                    UniqueFunction<void(const std::shared_ptr<SyncUser>&, Optional<AppError>)>&& completion)
{
    if (!user) {
        return completion(nullptr, AppError(make_client_error_code(ClientErrorCode::user_not_found),
                                            "The specified user could not be found."));
    }
    if (user->state() != SyncUser::State::LoggedIn) {
        return completion(nullptr, AppError(make_client_error_code(ClientErrorCode::user_not_logged_in),
                                            "The specified user is not logged in."));
    }
    if (!verify_user_present(user)) {
        return completion(nullptr, AppError(make_client_error_code(ClientErrorCode::user_not_found),
                                            "The specified user was not found."));
    }

    App::log_in_with_credentials(credentials, user, std::move(completion));
}

void App::refresh_custom_data(const std::shared_ptr<SyncUser>& user,
                              UniqueFunction<void(Optional<AppError>)>&& completion)
{
    refresh_access_token(user, std::move(completion));
}

std::string App::url_for_path(const std::string& path = "") const
{
    std::lock_guard<std::mutex> lock(*m_route_mutex);
    return util::format("%1%2", m_base_route, path);
}

std::string App::get_app_route(const Optional<std::string>& hostname) const
{
    if (hostname) {
        return *hostname + base_path + app_path + "/" + m_config.app_id;
    }
    else {
        return m_app_route;
    }
}

// FIXME: This passes back the response to bubble up any potential errors, making this somewhat leaky
void App::init_app_metadata(UniqueFunction<void(const Optional<Response>&)>&& completion,
                            const Optional<std::string>& new_hostname)
{
    std::string route;

    if (!new_hostname && m_sync_manager->app_metadata()) {
        // Skip if the app_metadata has already been initialized and a new hostname is not provided
        return completion(util::none); // early return
    }
    else {
        std::lock_guard<std::mutex> lock(*m_route_mutex);
        route = util::format("%1/location", new_hostname ? get_app_route(new_hostname) : get_app_route());
    }

    Request req;
    req.method = HttpMethod::get;
    req.url = route;
    req.timeout_ms = m_request_timeout_ms;

    m_config.transport->send_request_to_server(req, [self = shared_from_this(),
                                                     completion = std::move(completion)](const Response& response) {
        // If the response contains an error, then pass it up
        if (response.http_status_code >= 300 || (response.http_status_code < 200 && response.http_status_code != 0)) {
            return completion(response); // early return
        }

        try {
            auto json = parse<BsonDocument>(response.body);
            auto hostname = get<std::string>(json, "hostname");
            auto ws_hostname = get<std::string>(json, "ws_hostname");
            auto deployment_model = get<std::string>(json, "deployment_model");
            auto location = get<std::string>(json, "location");
            self->m_sync_manager->perform_metadata_update([&](SyncMetadataManager& manager) {
                manager.set_app_metadata(deployment_model, location, hostname, ws_hostname);
            });

            self->update_hostname(self->m_sync_manager->app_metadata());
        }
        catch (const AppError&) {
            // Pass the response back to completion
            return completion(response);
        }
        completion(util::none);
    });
}

void App::post(std::string&& route, UniqueFunction<void(Optional<AppError>)>&& completion, const BsonDocument& body)
{
    do_request(Request{HttpMethod::post, std::move(route), m_request_timeout_ms, get_request_headers(),
                       Bson(body).to_string()},
               handle_default_response(std::move(completion)));
}

void App::update_metadata_and_resend(Request&& request, UniqueFunction<void(const Response&)>&& completion,
                                     const Optional<std::string>& new_hostname)
{
    // if we do not have metadata yet, we need to initialize it and send the
    // request once that's complete; or if a new_hostname is provided, re-initialize
    // the metadata with the updated location info
    init_app_metadata(
        [completion = std::move(completion), request = std::move(request), base_url = m_base_url,
         self = shared_from_this()](const util::Optional<Response>& response) mutable {
            if (response) {
                return self->handle_possible_redirect_response(std::move(request), *response, std::move(completion));
            }

            // if this is the first time we have received app metadata, the
            // original request will not have the correct URL hostname for
            // non global deployments.
            auto app_metadata = self->m_sync_manager->app_metadata();
            if (app_metadata && request.url.rfind(base_url, 0) != std::string::npos &&
                app_metadata->hostname != base_url) {
                request.url.replace(0, base_url.size(), app_metadata->hostname);
            }
            // Retry the original request with the updated url
            self->m_config.transport->send_request_to_server(
                std::move(request), [self = std::move(self), completion = std::move(completion)](
                                        Request&& request, const Response& response) mutable {
                    self->handle_possible_redirect_response(std::move(request), response, std::move(completion));
                });
        },
        new_hostname);
}

void App::do_request(Request&& request, UniqueFunction<void(const Response&)>&& completion)
{
    request.timeout_ms = default_timeout_ms;

    // Normal do_request operation, just send the request to the server and return the response
    if (m_sync_manager->app_metadata()) {
        m_config.transport->send_request_to_server(
            std::move(request), [self = shared_from_this(), completion = std::move(completion)](
                                    Request&& request, const Response& response) mutable {
                self->handle_possible_redirect_response(std::move(request), response, std::move(completion));
            });
        return; // early return
    }

    // if we do not have metadata yet, update the metadata and resend the request
    update_metadata_and_resend(std::move(request), std::move(completion));
}

void App::handle_possible_redirect_response(Request&& request, const Response& response,
                                            UniqueFunction<void(const Response&)>&& completion)
{
    // If the response contains a redirection, then process it
    if (util::HTTPStatus(response.http_status_code) == util::HTTPStatus::MovedPermanently) {
        handle_redirect_response(std::move(request), response, std::move(completion));
    }
    else {
        completion(response);
    }
}

void App::handle_redirect_response(Request&& request, const Response& response,
                                   UniqueFunction<void(const Response&)>&& completion)
{
    // Permanent redirect - get the location and init the metadata again
    // Look for case insensitive redirect "location" in headers
    auto location = AppUtils::find_header("location", response.headers);
    if (!location || location->second.empty()) {
        // Location not found in the response, pass error response up the chain
        Response error;
        error.http_status_code = response.http_status_code;
        error.client_error_code = ClientErrorCode::redirect_error;
        error.body = "Redirect response missing location header";
        return completion(error); // early return
    }

    // Make sure we don't do too many redirects (max_http_redirects (20) is an arbitrary number)
    if (++request.redirect_count > max_http_redirects) {
        Response error;
        error.http_status_code = response.http_status_code;
        error.custom_status_code = 0;
        error.client_error_code = ClientErrorCode::too_many_redirects;
        error.body = util::format("number of redirections exceeded %1", max_http_redirects);
        return completion(error); // early return
    }

    // Update the metadata from the new location after trimming the url (limit to `scheme://host[:port]`)
    std::string_view new_url = location->second;
    // Find the end of the scheme/protocol part (e.g. 'https://', 'http://')
    auto scheme_end = new_url.find("://");
    scheme_end = scheme_end != std::string_view::npos ? scheme_end + std::char_traits<char>::length("://") : 0;
    // Trim off any trailing path/anchor/query string after the host/port
    if (auto split = new_url.find_first_of("/#?", scheme_end); split != std::string_view::npos) {
        new_url.remove_suffix(new_url.size() - split);
    }

    update_metadata_and_resend(std::move(request), std::move(completion), std::string(new_url));
}

void App::do_authenticated_request(Request&& request, const std::shared_ptr<SyncUser>& sync_user,
                                   util::UniqueFunction<void(const Response&)>&& completion)
{
    request.headers = get_request_headers(sync_user, request.uses_refresh_token ? RequestTokenType::RefreshToken
                                                                                : RequestTokenType::AccessToken);

    log_debug("App: do_authenticated_request: %1 %2", httpmethod_to_string(request.method), request.url);
    auto completion_2 = [completion = std::move(completion), request, sync_user,
                         self = shared_from_this()](const Response& response) mutable {
        if (auto error = AppUtils::check_for_errors(response)) {
            self->handle_auth_failure(std::move(*error), std::move(response), std::move(request), sync_user,
                                      std::move(completion));
        }
        else {
            completion(std::move(response));
        }
    };
    do_request(std::move(request), std::move(completion_2));
}

void App::handle_auth_failure(const AppError& error, const Response& response, Request&& request,
                              const std::shared_ptr<SyncUser>& sync_user,
                              util::UniqueFunction<void(const Response&)>&& completion)
{
    // Only handle auth failures
    if (*error.http_status_code == 401) {
        if (request.uses_refresh_token) {
            if (sync_user && sync_user->is_logged_in()) {
                sync_user->log_out();
            }
            completion(std::move(response));
            return;
        }
    }
    else {
        completion(std::move(response));
        return;
    }

    App::refresh_access_token(sync_user, [self = shared_from_this(), request = std::move(request),
                                          completion = std::move(completion), response = std::move(response),
                                          sync_user](Optional<AppError>&& error) mutable {
        if (!error) {
            // assign the new access_token to the auth header
            request.headers = get_request_headers(sync_user, RequestTokenType::AccessToken);
            self->do_request(std::move(request), std::move(completion));
        }
        else {
            // pass the error back up the chain
            completion(std::move(response));
        }
    });
}

/// MARK: - refresh access token
void App::refresh_access_token(const std::shared_ptr<SyncUser>& sync_user,
                               util::UniqueFunction<void(Optional<AppError>)>&& completion)
{
    if (!sync_user) {
        completion(AppError(make_client_error_code(ClientErrorCode::user_not_found), "No current user exists"));
        return;
    }

    if (!sync_user->is_logged_in()) {
        completion(
            AppError(make_client_error_code(ClientErrorCode::user_not_logged_in), "The user is not logged in"));
        return;
    }

    std::string route;
    {
        std::lock_guard<std::mutex> lock(*m_route_mutex);
        route = util::format("%1/auth/session", m_base_route);
    }

    log_debug("App: refresh_access_token: email: %1", sync_user->user_profile().email());

    do_request(Request{HttpMethod::post, std::move(route), m_request_timeout_ms,
                       get_request_headers(sync_user, RequestTokenType::RefreshToken)},
               [completion = std::move(completion), sync_user](const Response& response) {
                   if (auto error = AppUtils::check_for_errors(response)) {
                       return completion(std::move(error));
                   }

                   try {
                       auto json = parse<BsonDocument>(response.body);
                       sync_user->update_access_token(get<std::string>(json, "access_token"));
                   }
                   catch (AppError& err) {
                       return completion(std::move(err));
                   }

                   return completion(util::none);
               });
}

std::string App::function_call_url_path() const
{
    std::lock_guard<std::mutex> lock(*m_route_mutex);
    return util::format("%1/app/%2/functions/call", m_base_route, m_config.app_id);
}

void App::call_function(const std::shared_ptr<SyncUser>& user, const std::string& name, std::string_view args_ejson,
                        const Optional<std::string>& service_name_opt,
                        UniqueFunction<void(const std::string*, Optional<AppError>)>&& completion)
{
    auto service_name = service_name_opt ? *service_name_opt : "<none>";
    if (would_log(util::Logger::Level::debug)) {
        log_debug("App: call_function: %1 service_name: %2 args_bson: %3", name, service_name, args_ejson);
    }

    auto args = util::format("{\"arguments\":%1,\"name\":%2%3}", args_ejson, nlohmann::json(name).dump(),
                             service_name_opt ? (",\"service\":" + nlohmann::json(service_name).dump()) : "");

    do_authenticated_request(
        Request{HttpMethod::post, function_call_url_path(), m_request_timeout_ms, {}, std::move(args), false}, user,
        [self = shared_from_this(), name = name, service_name = std::move(service_name),
         completion = std::move(completion)](const Response& response) {
            if (auto error = AppUtils::check_for_errors(response)) {
                self->log_error("App: call_function: %1 service_name: %2 -> %3 ERROR: %4", name, service_name,
                                response.http_status_code, error->message);
                return completion(nullptr, error);
            }
            completion(&response.body, util::none);
        });
}

void App::call_function(const std::shared_ptr<SyncUser>& user, const std::string& name, const BsonArray& args_bson,
                        const Optional<std::string>& service_name,
                        UniqueFunction<void(Optional<Bson>&&, Optional<AppError>)>&& completion)
{
    auto service_name2 = service_name ? *service_name : "<none>";
    std::stringstream args_ejson;
    args_ejson << "[";
    for (auto&& arg : args_bson) {
        if (&arg != &args_bson.front())
            args_ejson << ',';
        args_ejson << arg.toJson();
    }
    args_ejson << "]";

    call_function(user, name, std::move(args_ejson).str(), service_name,
                  [self = shared_from_this(), name, service_name = std::move(service_name2),
                   completion = std::move(completion)](const std::string* response, util::Optional<AppError>&& err) {
                      if (err) {
                          return completion({}, err);
                      }
                      util::Optional<Bson> body_as_bson;
                      try {
                          body_as_bson = bson::parse(*response);
                          if (self->would_log(util::Logger::Level::debug)) {
                              self->log_debug("App: call_function: %1 service_name: %2 - results: %3", name,
                                              service_name, body_as_bson ? body_as_bson->to_string() : "<none>");
                          }
                      }
                      catch (const std::exception& e) {
                          self->log_error("App: call_function: %1 service_name: %2 - error parsing result: %3", name,
                                          service_name, e.what());
                          return completion(util::none,
                                            AppError(make_error_code(JSONErrorCode::bad_bson_parse), e.what()));
                      };
                      completion(std::move(body_as_bson), util::none);
                  });
}

void App::call_function(const std::shared_ptr<SyncUser>& user, const std::string& name, const BsonArray& args_bson,
                        UniqueFunction<void(Optional<bson::Bson>&&, Optional<AppError>)>&& completion)
{
    call_function(user, name, args_bson, util::none, std::move(completion));
}

void App::call_function(const std::string& name, const BsonArray& args_bson,
                        const Optional<std::string>& service_name,
                        UniqueFunction<void(Optional<bson::Bson>&&, Optional<AppError>)>&& completion)
{
    call_function(m_sync_manager->get_current_user(), name, args_bson, service_name, std::move(completion));
}

void App::call_function(const std::string& name, const BsonArray& args_bson,
                        UniqueFunction<void(Optional<bson::Bson>&&, Optional<AppError>)>&& completion)
{
    call_function(m_sync_manager->get_current_user(), name, args_bson, std::move(completion));
}

Request App::make_streaming_request(const std::shared_ptr<SyncUser>& user, const std::string& name,
                                    const BsonArray& args_bson, const Optional<std::string>& service_name) const
{
    auto args = BsonDocument{
        {"arguments", args_bson},
        {"name", name},
    };
    if (service_name) {
        args["service"] = *service_name;
    }
    const auto args_json = Bson(args).to_string();

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
    return PushClient(service_name, m_config.app_id, m_request_timeout_ms, shared_from_this());
}

} // namespace app
} // namespace realm
